"""poursuite.probes CLI — argparse entry point for diagnostic probes."""
import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

from poursuite.probes import PROBES_ROOT, latest_run_dir, make_run_dir
from poursuite.probes import datajud as datajud_probe
from poursuite.probes import esaj_inventory as esaj_inventory_probe
from poursuite.probes import layer3_probe as layer3_probe_mod
from poursuite.probes import phase3_probe as phase3_probe_mod
from poursuite.probes import receita as receita_probe
from poursuite.utils import setup_logging

PROBES_DIR = Path(__file__).parent
DEFAULT_SAMPLES = PROBES_DIR / "sample_processes.yaml"
DEFAULT_ESAJ_SAMPLES = PROBES_DIR / "esaj_inventory_samples.yaml"
DEFAULT_LAYOUT = PROBES_DIR / "cnpj_layout.txt"


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m poursuite.probes",
        description="Diagnostic probes for DataJud and Receita CNPJ.",
    )
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_dj = sub.add_parser("datajud", help="DataJud probe")
    grp = p_dj.add_mutually_exclusive_group()
    grp.add_argument("--processes", type=Path, default=DEFAULT_SAMPLES,
                     help="YAML/JSON file listing sample process numbers.")
    grp.add_argument("--process", type=str,
                     help="Single process number to probe (overrides --processes).")
    p_dj.add_argument("--known-cnpj", default="33000167000101",
                      help="CNPJ used for Experiment 2 (default: Petrobras 33000167000101).")
    p_dj.add_argument("--skip-experiments", action="store_true",
                      help="Skip Challenge Experiments 1-5 (per-process probe only).")

    p_rc = sub.add_parser("receita", help="Receita CNPJ probe")
    grp2 = p_rc.add_mutually_exclusive_group(required=True)
    grp2.add_argument("--schema-check", action="store_true",
                      help="Compare cnpj_layout.txt to PROBE_FINDINGS §2.4.")
    grp2.add_argument("--sample", action="store_true",
                      help="Download + ingest a partition slice (NOT YET WIRED).")
    p_rc.add_argument("--layout", type=Path, default=DEFAULT_LAYOUT,
                      help="Path to cnpj_layout.txt (default: bundled).")

    p_es = sub.add_parser("esaj", help="eSAJ Full Inventory probe (TJSP consulta page)")
    p_es.add_argument("--inventory", action="store_true",
                      help="Run the full-inventory walk. Required (reserved for future "
                           "subcommands).")
    grp3 = p_es.add_mutually_exclusive_group()
    grp3.add_argument("--processes", type=Path, default=DEFAULT_ESAJ_SAMPLES,
                      help="YAML/JSON file listing sample TJSP process numbers + metadata.")
    grp3.add_argument("--process", type=str,
                      help="Single CNJ-formatted process number to probe (overrides --processes).")
    p_es.add_argument("--headed", action="store_true",
                      help="Run Chrome headed (default: headless). Use for the 2-3-case "
                           "headless-detection sanity check.")

    p_p3 = sub.add_parser("phase3", help="Phase 3 probe (eSAJ doc download + extract)")
    p_p3.add_argument("--db-path", type=Path, default=None,
                      help="Override snapshot DB path (default: SNAPSHOT_DIR/esaj_snapshots.db).")

    p_l3 = sub.add_parser("layer3", help="Layer 3 probe (DataJud name-search quality)")
    p_l3.add_argument("--date-window-days", type=int, default=730,
                      help="dataAjuizamento window for 'definitely_same' classification (default 730 = 2y).")

    sub.add_parser("report", help="Consolidate latest probe runs into latest_report.md.")

    return parser


def main(argv: Optional[List[str]] = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    if args.cmd == "datajud":
        return _run_datajud(args)
    if args.cmd == "receita":
        return _run_receita(args)
    if args.cmd == "esaj":
        return _run_esaj(args)
    if args.cmd == "phase3":
        return _run_phase3(args)
    if args.cmd == "layer3":
        return _run_layer3(args)
    if args.cmd == "report":
        return _run_report()
    parser.print_help()
    return 2


def _run_datajud(args: argparse.Namespace) -> int:
    logger = setup_logging("poursuite.probes")
    run_dir = make_run_dir("datajud")
    logger.info("DataJud probe — run dir: %s", run_dir)

    if args.process:
        samples = [args.process.strip()]
    else:
        if not args.processes.exists():
            logger.error("Samples file not found: %s", args.processes)
            return 1
        samples = datajud_probe.load_samples(args.processes)

    if not samples:
        logger.error("No samples to probe.")
        return 1

    logger.info("Probing %d sample(s); known_cnpj=%s; skip_experiments=%s",
                len(samples), args.known_cnpj, args.skip_experiments)
    result = datajud_probe.run(samples, run_dir, logger,
                               known_cnpj=args.known_cnpj,
                               skip_experiments=args.skip_experiments)
    logger.info("DataJud probe complete: %s", run_dir)
    return 0 if result.get("ok") else 1


def _run_esaj(args: argparse.Namespace) -> int:
    logger = setup_logging("poursuite.probes")
    if not args.inventory:
        logger.error("eSAJ probe currently requires --inventory.")
        return 2
    run_dir = make_run_dir("esaj_inventory")
    logger.info("eSAJ inventory probe — run dir: %s", run_dir)

    if args.process:
        samples = [{
            "number": args.process.strip(),
            "source": "operator",
            "target_archetype": "ad-hoc",
        }]
    else:
        if not args.processes.exists():
            logger.error("Samples file not found: %s", args.processes)
            return 1
        samples = esaj_inventory_probe.load_samples(args.processes)

    if not samples:
        logger.error("No samples to probe.")
        return 1

    summary = esaj_inventory_probe.run(samples, run_dir, logger, headed=args.headed)
    logger.info("eSAJ inventory complete: %s", run_dir)
    logger.info("Outcomes: %s", summary.get("outcomes"))
    if summary.get("unfilled_archetypes"):
        logger.warning("Unfilled archetypes: %s",
                       ", ".join(summary["unfilled_archetypes"]))
    return 0 if summary.get("ok") else 1


def _run_phase3(args: argparse.Namespace) -> int:
    logger = setup_logging("poursuite.probes")
    run_dir = make_run_dir("phase3_probe")
    db_path = args.db_path or phase3_probe_mod.DEFAULT_DB_PATH
    result = phase3_probe_mod.run(run_dir, logger, db_path=db_path)
    logger.info("Phase 3 probe complete: %s", run_dir)
    return 0 if result.get("ok") else 1


def _run_layer3(args: argparse.Namespace) -> int:
    logger = setup_logging("poursuite.probes")
    run_dir = make_run_dir("layer3_probe")
    result = layer3_probe_mod.run(run_dir, logger,
                                  date_window_days=args.date_window_days)
    logger.info("Layer 3 probe complete: %s", run_dir)
    return 0 if result.get("ok") else 1


def _run_receita(args: argparse.Namespace) -> int:
    logger = setup_logging("poursuite.probes")
    run_dir = make_run_dir("receita")
    logger.info("Receita probe — run dir: %s", run_dir)
    if args.schema_check:
        receita_probe.schema_check(args.layout, run_dir, logger)
        return 0
    if args.sample:
        return receita_probe.sample_stub(run_dir, logger)
    return 2


def _run_report() -> int:
    logger = setup_logging("poursuite.probes")
    PROBES_ROOT.mkdir(parents=True, exist_ok=True)
    out_path = PROBES_ROOT / "latest_report.md"
    sections: List[str] = []
    sections.append("# Poursuite Probes — Latest Consolidated Report")
    sections.append(f"_Generated: {datetime.now(timezone.utc).isoformat()}_")
    sections.append("")

    dj = latest_run_dir("datajud")
    if dj and (dj / "datajud_report.md").exists():
        sections.append(f"_Source: `{dj.name}`_")
        sections.append("")
        sections.append((dj / "datajud_report.md").read_text(encoding="utf-8"))
        sections.append("")
    else:
        sections.append("## DataJud")
        sections.append("")
        sections.append("_No DataJud run found. Run `python -m poursuite.probes datajud`._")
        sections.append("")

    rc = latest_run_dir("receita")
    if rc and (rc / "schema_diff_report.md").exists():
        sections.append(f"_Source: `{rc.name}`_")
        sections.append("")
        sections.append((rc / "schema_diff_report.md").read_text(encoding="utf-8"))
        sections.append("")
    else:
        sections.append("## Receita Schema Check")
        sections.append("")
        sections.append("_No receita schema-check run found. Run "
                        "`python -m poursuite.probes receita --schema-check`._")
        sections.append("")

    out_path.write_text("\n".join(sections), encoding="utf-8")
    logger.info("Wrote %s", out_path)
    return 0


if __name__ == "__main__":
    sys.exit(main())
