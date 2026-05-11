"""End-to-end orchestrator for the DJE database update pipeline.

Runs Download -> Parse -> Split -> Optimize -> Publish -> Cleanup in a single
invocation. Designed to be called twice a year (after each half ends) with
zero arguments; sensible defaults are derived from what's already on D:.

Usage::

    python update_database.py                       # full auto-derived run
    python update_database.py --start 01/01/2025    # override start date
    python update_database.py --label 2025_Jan-Jun  # override label
    python update_database.py --dry-run             # plan only
    python update_database.py cleanup-staging --year 2024
"""
from __future__ import annotations

import argparse
import shutil
import sqlite3
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional

from poursuite.config import COURT_DOCS_DIR, DB_DIR, STAGING_DB_DIR
from poursuite.utils import setup_logging

logger = setup_logging("update_database")

# Stages, in execution order
STAGES = ("download", "parse", "split", "optimize", "publish", "cleanup")

# Pre-flight thresholds
MIN_FREE_GB_STAGING = 100
MIN_FREE_GB_PUBLISH = 50


# --------------------------------------------------------------------------
# Date / label helpers
# --------------------------------------------------------------------------

def _half_of(d: date) -> str:
    """Return 'Jan-Jun' or 'Jul-Dec' for the half-year containing ``d``."""
    return "Jan-Jun" if d.month <= 6 else "Jul-Dec"


def _half_bounds(d: date) -> tuple[date, date]:
    """Return (first_day, last_day) of the half-year containing ``d``."""
    if d.month <= 6:
        return date(d.year, 1, 1), date(d.year, 6, 30)
    return date(d.year, 7, 1), date(d.year, 12, 31)


def _next_half_start(d: date) -> date:
    """First day of the half-year after the one containing ``d``."""
    if d.month <= 6:
        return date(d.year, 7, 1)
    return date(d.year + 1, 1, 1)


def _label_for(end_date: date) -> str:
    return f"{end_date.year}_{_half_of(end_date)}"


def _latest_published_date(db_dir: Path) -> Optional[date]:
    """Read MAX(document_date) across every shard in db_dir. Returns None if empty."""
    if not db_dir.exists():
        return None
    latest: Optional[date] = None
    for db_path in sorted(db_dir.glob("*.db")):
        try:
            with sqlite3.connect(str(db_path)) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name='paragraphs'"
                )
                if not cursor.fetchone():
                    continue
                cursor.execute("SELECT MAX(document_date) FROM paragraphs")
                row = cursor.fetchone()
                if row and row[0]:
                    parsed = datetime.strptime(row[0], "%Y-%m-%d").date()
                    if latest is None or parsed > latest:
                        latest = parsed
        except Exception as e:
            logger.warning(f"Could not read date range from {db_path.name}: {e}")
    return latest


# --------------------------------------------------------------------------
# Plan
# --------------------------------------------------------------------------

@dataclass
class Plan:
    start: date
    end: date
    label: str
    staging_year_shard: Path
    staging_label_shard: Path
    optimized_dir: Path
    optimized_output: Path
    published_shard: Path
    courtdocs_dir: Path

    def describe(self) -> str:
        return (
            f"  start          {self.start}\n"
            f"  end            {self.end}\n"
            f"  label          {self.label}\n"
            f"  CourtDocs      {self.courtdocs_dir}\n"
            f"  staging year   {self.staging_year_shard}\n"
            f"  staging label  {self.staging_label_shard}\n"
            f"  optimized      {self.optimized_output}\n"
            f"  published      {self.published_shard}"
        )


def build_plan(
    start_arg: Optional[str],
    end_arg: Optional[str],
    label_arg: Optional[str],
    today: Optional[date] = None,
) -> Plan:
    today = today or date.today()
    fully_auto = not (start_arg or end_arg or label_arg)

    if start_arg:
        start = datetime.strptime(start_arg, "%d/%m/%Y").date()
    else:
        latest = _latest_published_date(DB_DIR)
        if latest is None:
            raise SystemExit(
                f"Cannot auto-derive --start: no shards found in {DB_DIR}. "
                "Pass --start DD/MM/YYYY explicitly."
            )
        start = latest + timedelta(days=1)

    if end_arg:
        end = datetime.strptime(end_arg, "%d/%m/%Y").date()
    else:
        _, end = _half_bounds(start)

    label = label_arg or _label_for(end)

    # Auto-advance past already-published halves. Only fires when no override
    # was supplied: any of --start/--end/--label disables this loop and the
    # publish-collision check at Stage 5 becomes the sole guard.
    if fully_auto:
        for _ in range(20):  # generous cap; we should never iterate this much
            target = DB_DIR / f"legal_documents_{label}.db"
            if not target.exists():
                break
            next_start = _next_half_start(start)
            _, next_end = _half_bounds(next_start)
            next_label = _label_for(next_end)
            logger.info(
                f"Half {label} already published at {target}; "
                f"advancing to {next_label} ({next_start} to {next_end})"
            )
            start, end, label = next_start, next_end, next_label
        else:
            raise SystemExit(
                "Auto-advance exceeded 20 iterations without finding an "
                "unpublished half; aborting."
            )

    if today <= end:
        raise SystemExit(
            f"Half-year ending {end} is not yet complete (today is {today}). "
            "Re-run after the half closes, or pass --end explicitly to override."
        )

    if start > end:
        raise SystemExit(f"start ({start}) is after end ({end}); nothing to do.")

    return Plan(
        start=start,
        end=end,
        label=label,
        staging_year_shard=STAGING_DB_DIR / f"legal_documents_{end.year}.db",
        staging_label_shard=STAGING_DB_DIR / f"legal_documents_{label}.db",
        optimized_dir=STAGING_DB_DIR / "Optimized",
        optimized_output=STAGING_DB_DIR / "Optimized" / f"legal_documents_{label}.db",
        published_shard=DB_DIR / f"legal_documents_{label}.db",
        courtdocs_dir=COURT_DOCS_DIR,
    )


# --------------------------------------------------------------------------
# Disk pre-flight
# --------------------------------------------------------------------------

def check_disk_space(plan: Plan) -> None:
    staging_free_gb = shutil.disk_usage(STAGING_DB_DIR.anchor).free / (1024 ** 3)
    publish_free_gb = shutil.disk_usage(DB_DIR.anchor).free / (1024 ** 3)
    logger.info(
        f"Disk free: staging {STAGING_DB_DIR.anchor} = {staging_free_gb:.1f} GB, "
        f"publish {DB_DIR.anchor} = {publish_free_gb:.1f} GB"
    )
    if staging_free_gb < MIN_FREE_GB_STAGING:
        raise SystemExit(
            f"Insufficient free space on {STAGING_DB_DIR.anchor}: "
            f"{staging_free_gb:.1f} GB < {MIN_FREE_GB_STAGING} GB required."
        )
    if publish_free_gb < MIN_FREE_GB_PUBLISH:
        raise SystemExit(
            f"Insufficient free space on {DB_DIR.anchor}: "
            f"{publish_free_gb:.1f} GB < {MIN_FREE_GB_PUBLISH} GB required."
        )


# --------------------------------------------------------------------------
# Stages
# --------------------------------------------------------------------------

def stage_download(plan: Plan, dry_run: bool) -> None:
    logger.info(f"[download] PDFs from {plan.start} to {plan.end} -> {plan.courtdocs_dir}")
    if dry_run:
        return
    # Lazy import so dry-runs don't load Selenium
    from maintenance.DownloadDJE import TJSPScraper
    scraper = TJSPScraper()
    results = scraper.download_documents(
        plan.start.strftime("%d/%m/%Y"), plan.end.strftime("%d/%m/%Y")
    )
    logger.info(
        f"[download] {len(results['successful'])} successful, "
        f"{len(results['failed'])} failed"
    )


def stage_parse(plan: Plan, dry_run: bool, force: bool) -> None:
    if plan.staging_year_shard.exists() and not force:
        logger.info(
            f"[parse] staging shard exists at {plan.staging_year_shard}; "
            "PDFtoDatabase will skip already-processed PDFs via the sidecar dedup table"
        )
    logger.info(f"[parse] {plan.courtdocs_dir} -> {plan.staging_year_shard}")
    if dry_run:
        return
    from maintenance.pdf_to_database import process_all_pdfs
    process_all_pdfs(str(plan.courtdocs_dir))


def stage_split(plan: Plan, dry_run: bool, force: bool) -> None:
    if plan.staging_label_shard.exists() and not force:
        logger.info(f"[split] {plan.staging_label_shard} exists; skipping (use --force-stage split to redo)")
        return
    logger.info(
        f"[split] {plan.staging_year_shard} -> {plan.staging_label_shard} "
        f"({plan.start} to {plan.end})"
    )
    if dry_run:
        return
    if force and plan.staging_label_shard.exists():
        plan.staging_label_shard.unlink()
    from maintenance.SplitDatabase import DatabaseSplitter
    splitter = DatabaseSplitter(
        str(plan.staging_year_shard),
        output_dir=str(STAGING_DB_DIR),
    )
    ok = splitter.split_by_date_range(
        [(plan.start.strftime("%Y-%m-%d"), plan.end.strftime("%Y-%m-%d"), plan.label)],
        db_name_pattern="legal_documents_{}.db",
    )
    if not ok:
        raise SystemExit("[split] failed; see split_database log for details.")


def stage_optimize(plan: Plan, dry_run: bool, force: bool) -> None:
    if plan.optimized_output.exists() and not force:
        logger.info(f"[optimize] {plan.optimized_output} exists; skipping")
        return
    logger.info(f"[optimize] {plan.staging_label_shard} -> {plan.optimized_output}")
    if dry_run:
        return
    if force and plan.optimized_output.exists():
        plan.optimized_output.unlink()
    plan.optimized_dir.mkdir(parents=True, exist_ok=True)
    from maintenance.static_database_optimizer import StaticDatabaseOptimizer
    optimizer = StaticDatabaseOptimizer(
        plan.staging_label_shard, output_dir=plan.optimized_dir
    )
    result = optimizer.optimize()
    archive_path = Path(result["optimized_path"])
    # Drop optimizer references and force gc so any lingering sqlite handles are
    # freed before we try to rename — Windows holds the file otherwise.
    del optimizer, result
    import gc
    gc.collect()
    if archive_path != plan.optimized_output:
        last_err = None
        for attempt in range(6):
            try:
                archive_path.rename(plan.optimized_output)
                last_err = None
                break
            except PermissionError as e:
                last_err = e
                time.sleep(2 ** attempt)
                gc.collect()
        if last_err is not None:
            raise last_err
        logger.info(f"[optimize] renamed {archive_path.name} -> {plan.optimized_output.name}")


def stage_publish(plan: Plan, dry_run: bool, force: bool) -> None:
    target_exists = plan.published_shard.exists()
    logger.info(f"[publish] {plan.optimized_output} -> {plan.published_shard}")
    if target_exists and not force:
        msg = (
            f"[publish] target {plan.published_shard} already exists. "
            "Pass --force-stage publish to overwrite."
        )
        if dry_run:
            logger.warning(msg + " (would fail in a real run)")
            return
        raise SystemExit(msg)
    if dry_run:
        return
    DB_DIR.mkdir(parents=True, exist_ok=True)
    if target_exists:
        plan.published_shard.unlink()
    shutil.move(str(plan.optimized_output), str(plan.published_shard))


def stage_cleanup(plan: Plan, dry_run: bool) -> None:
    """Delete the per-half intermediate split shard. Year shard kept."""
    logger.info(f"[cleanup] removing intermediate {plan.staging_label_shard}")
    if dry_run:
        return
    if plan.staging_label_shard.exists():
        plan.staging_label_shard.unlink()


# --------------------------------------------------------------------------
# Driver
# --------------------------------------------------------------------------

def run_pipeline(args: argparse.Namespace) -> None:
    plan = build_plan(args.start, args.end, args.label)

    skip = set(args.skip_stage or [])
    force = set(args.force_stage or [])
    bad = (skip | force) - set(STAGES)
    if bad:
        raise SystemExit(f"Unknown stage(s): {sorted(bad)}. Valid: {STAGES}")

    logger.info("=" * 60)
    logger.info("Pipeline plan:")
    for line in plan.describe().splitlines():
        logger.info(line)
    logger.info(f"  skip:   {sorted(skip) or 'none'}")
    logger.info(f"  force:  {sorted(force) or 'none'}")
    logger.info(f"  dry-run: {args.dry_run}")
    logger.info("=" * 60)

    if not args.skip_disk_check and not args.dry_run:
        check_disk_space(plan)

    started = time.time()
    for stage in STAGES:
        if stage in skip:
            logger.info(f"[{stage}] skipped via --skip-stage")
            continue
        is_force = stage in force
        if stage == "download":
            stage_download(plan, args.dry_run)
        elif stage == "parse":
            stage_parse(plan, args.dry_run, is_force)
        elif stage == "split":
            stage_split(plan, args.dry_run, is_force)
        elif stage == "optimize":
            stage_optimize(plan, args.dry_run, is_force)
        elif stage == "publish":
            stage_publish(plan, args.dry_run, is_force)
        elif stage == "cleanup":
            stage_cleanup(plan, args.dry_run)

    elapsed = time.time() - started
    logger.info(f"Pipeline complete in {elapsed:.1f}s")
    if args.dry_run:
        logger.info("(dry-run: no changes made)")
    else:
        logger.info(f"Published shard: {plan.published_shard}")


def run_cleanup_staging(args: argparse.Namespace) -> None:
    """Delete the staging year shard for ``--year``. Keeps dedup sidecar intact."""
    target = STAGING_DB_DIR / f"legal_documents_{args.year}.db"
    if not target.exists():
        logger.info(f"No staging shard at {target}; nothing to clean up.")
        return
    size_gb = target.stat().st_size / (1024 ** 3)
    logger.info(f"[cleanup-staging] deleting {target} ({size_gb:.1f} GB)")
    if args.dry_run:
        logger.info("(dry-run: no changes made)")
        return
    target.unlink()
    logger.info("[cleanup-staging] done.")


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="DJE database update pipeline (Download -> Parse -> Split -> Optimize -> Publish)."
    )
    sub = p.add_subparsers(dest="command")

    run_p = sub.add_parser("run", help="Run the full pipeline (default).")
    for parser_ in (p, run_p):
        parser_.add_argument("--start", help="Start date DD/MM/YYYY (default: day after MAX(document_date) on D:).")
        parser_.add_argument("--end", help="End date DD/MM/YYYY (default: end of half-year containing --start).")
        parser_.add_argument("--label", help="Override published filename suffix (default: derived from --end).")
        parser_.add_argument(
            "--skip-stage", action="append", choices=STAGES,
            help="Skip a stage even if its output is missing (repeatable).",
        )
        parser_.add_argument(
            "--force-stage", action="append", choices=STAGES,
            help="Re-run a stage even if its output exists (repeatable).",
        )
        parser_.add_argument("--skip-disk-check", action="store_true", help="Bypass the 100/50 GB free-space pre-flight.")
        parser_.add_argument("--dry-run", action="store_true", help="Print the plan and exit; touch nothing.")

    cleanup = sub.add_parser("cleanup-staging", help="Delete a staging year shard once the year is fully published.")
    cleanup.add_argument("--year", type=int, required=True, help="Year of the staging shard to delete (e.g. 2024).")
    cleanup.add_argument("--dry-run", action="store_true")

    return p


def main(argv: Optional[list[str]] = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == "cleanup-staging":
        run_cleanup_staging(args)
    else:
        # Default: run pipeline
        run_pipeline(args)


if __name__ == "__main__":
    main()
