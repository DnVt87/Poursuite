"""CLI for DataJud per-process enrichment — Layer 3-lite (L3L-d).

A back-office maintenance sweep: enrich process numbers with DataJud structured
fields (complementosTabelados, assuntos, grau, codigoMunicipioIBGE, freshness)
via batched `_msearch`, persisted append-on-change. Not interactive — the lawyer
consumes the filterable results, not this trigger. (An /extract-style background
job is deferred until a UI actually needs to fire enrichment.)

Pick one source:
  python -m poursuite.datajud 1002177-13.2020.8.26.0100 ...        explicit numbers
  python -m poursuite.datajud --group carteira_itau_2026_05         a carteira (ProcessGroupStore)
  python -m poursuite.datajud --backfill-all                        every un-enriched loaded snapshot
  python -m poursuite.datajud --from-shard PATH --limit 20          un-curated numbers from a DJE shard

Flags: --dry-run, --limit N, --batch-size N (default 20), --index (default api_publica_tjsp).
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from pathlib import Path
from typing import List, Optional

from poursuite.datajud.client import DEFAULT_INDEX
from poursuite.datajud.enrichment import enrich_and_store
from poursuite.db.esaj_snapshots import SnapshotStore
from poursuite.db.process_groups import ProcessGroupStore
from poursuite.utils import setup_logging


def _shard_process_numbers(shard_path: Path, limit: Optional[int]) -> List[str]:
    """Distinct process numbers from a DJE shard's `paragraphs` table — an
    un-curated, portfolio-shaped source (no pre-selection for DataJud presence,
    so the smoke's hit/miss rate is honest)."""
    uri = f"file:{shard_path.as_posix()}?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    try:
        # Dedup in rowid (insertion) order, NOT via SQL DISTINCT: DISTINCT
        # imposes a value ordering that surfaces the lexicographically-smallest
        # = oldest oddball cases (1800s/1970s), which isn't portfolio-shaped.
        # Rowid order follows the shard's gazette chronology — the active
        # docket — so it's activity-weighted and representative. Stream and
        # stop early so we don't scan the whole (large) table.
        seen: List[str] = []
        seen_set = set()
        for (pn,) in conn.execute("SELECT process_number FROM paragraphs"):
            if pn and pn not in seen_set:
                seen_set.add(pn)
                seen.append(pn)
                if limit is not None and len(seen) >= limit:
                    break
        return seen
    finally:
        conn.close()


def _resolve_source(args: argparse.Namespace, store: SnapshotStore,
                    logger) -> List[str]:
    """Resolve the chosen source flag into a list of process numbers."""
    if args.process_numbers:
        return list(args.process_numbers)
    if args.group:
        group = ProcessGroupStore().get_group(args.group)
        if group is None:
            raise SystemExit(f"group not found: {args.group!r}")
        pns = list(group.get("process_numbers", []))
        logger.info("group %r -> %d process numbers", args.group, len(pns))
        return pns
    if args.backfill_all:
        pns = store.list_unenriched_process_numbers(limit=args.limit)
        logger.info("backfill-all -> %d un-enriched loaded process numbers", len(pns))
        return pns
    if args.from_shard:
        shard = Path(args.from_shard)
        if not shard.exists():
            raise SystemExit(f"shard not found: {shard}")
        pns = _shard_process_numbers(shard, args.limit)
        logger.info("shard %s -> %d process numbers", shard.name, len(pns))
        return pns
    raise SystemExit(
        "no source given. Provide process numbers, or one of "
        "--group / --backfill-all / --from-shard. See --help."
    )


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="python -m poursuite.datajud",
        description="DataJud per-process enrichment (Layer 3-lite).",
    )
    p.add_argument("process_numbers", nargs="*",
                   help="explicit CNJ process numbers to enrich")
    p.add_argument("--group", metavar="GROUP_ID",
                   help="enrich a carteira's process numbers (ProcessGroupStore)")
    p.add_argument("--backfill-all", action="store_true",
                   help="enrich every loaded snapshot that has no enrichment yet")
    p.add_argument("--from-shard", metavar="PATH",
                   help="enrich distinct process numbers from a DJE shard's paragraphs")
    p.add_argument("--limit", type=int, default=None,
                   help="cap the number of processes (applies to --backfill-all / --from-shard)")
    p.add_argument("--batch-size", type=int, default=20,
                   help="processes per _msearch round-trip (default 20)")
    p.add_argument("--index", default=DEFAULT_INDEX,
                   help=f"DataJud index (default {DEFAULT_INDEX})")
    p.add_argument("--dry-run", action="store_true",
                   help="resolve + print the target set; fetch/write nothing")
    return p


def main(argv: Optional[List[str]] = None) -> None:
    args = build_parser().parse_args(argv if argv is not None else sys.argv[1:])
    logger = setup_logging("datajud_enrichment")

    store = SnapshotStore(logger=logger)  # applies the v5 migration on first open
    try:
        pns = _resolve_source(args, store, logger)
        # An explicit cap also applies when numbers come from a group/explicit list.
        if args.limit is not None and not (args.backfill_all or args.from_shard):
            pns = pns[:args.limit]

        if args.dry_run:
            print(json.dumps({
                "dry_run": True,
                "source": ("explicit" if args.process_numbers else
                           "group" if args.group else
                           "backfill_all" if args.backfill_all else "from_shard"),
                "target_count": len(pns),
                "sample": pns[:10],
            }, indent=2, ensure_ascii=False))
            return

        if not pns:
            print(json.dumps({"requested": 0, "note": "nothing to enrich"}, indent=2))
            return

        summary = enrich_and_store(
            process_numbers=pns, store=store, index=args.index,
            batch_size=args.batch_size, logger=logger,
        )
        print(json.dumps(summary, indent=2, ensure_ascii=False))
    finally:
        store.close()


if __name__ == "__main__":
    main()
