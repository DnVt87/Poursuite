"""DataJud capability re-inventory.

Canonical, empirically-verified reference for what the DataJud public API
can and cannot do. Replaces patchwork understanding from prior probes
(May 2026 datajud probe, May 2026 layer3 probe).

This module is investigation-only. It populates
`docs/DATAJUD_CAPABILITY_INVENTORY.md` and saves raw artifacts under
`<POURSUITE_LOG_DIR>/probes/datajud_inventory_<ts>/`.

Architecture:
- HTTP / API-key helpers reused from `poursuite.probes.datajud` (see layer3_probe.py for precedent)
- Each Part 1.1 / 1.2 / 1.3 / 2 / 3.x has its own `run_part_N()` function
- Functions return findings dicts; the orchestrator aggregates them
- The orchestrator writes the canonical doc + a side findings.md
"""
from __future__ import annotations

import logging
import re
import sqlite3
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from poursuite.config import SNAPSHOT_DIR
from poursuite.probes import write_json
from poursuite.probes.datajud import (
    PARTY_INDICATOR_KEYS,
    _has_hits,
    _hit_count,
    _http_get,
    _post,
    _resolve_api_key,
    _scan_for_party_keys,
)

URL_FOR_INDEX = "https://api-publica.datajud.cnj.jus.br/{index}/_search"
URL_BASE = "https://api-publica.datajud.cnj.jus.br/{index}"

# 5 tribunals from the Phase 3 + Layer 3 brief v1.2 (STJ + TST excluded).
TRIBUNAL_INDICES: Tuple[str, ...] = (
    "api_publica_tjsp",
    "api_publica_tjrj",
    "api_publica_tjmg",
    "api_publica_trt2",
    "api_publica_trf3",
)


# ---------------------------------------------------------------------------
# Test-case loading
# ---------------------------------------------------------------------------

def load_snapshot_test_cases(db_path: Path) -> List[Dict[str, Any]]:
    """Load latest-loaded snapshot rows that have a defendant. These are the
    ground-truth test cases for Part 1.1."""
    out: List[Dict[str, Any]] = []
    conn = sqlite3.connect(str(db_path))
    try:
        cur = conn.execute(
            """
            WITH latest AS (
              SELECT process_number, MAX(snapshot_ts) AS ts
              FROM process_snapshot WHERE scrape_outcome = 'loaded'
              GROUP BY process_number
            )
            SELECT p.process_number, p.defendant, p.plaintiff, p.foro_code,
                   p.class_type, p.initial_date, p.other_processes
            FROM process_snapshot p
            JOIN latest l
              ON l.process_number = p.process_number AND l.ts = p.snapshot_ts
            WHERE p.defendant IS NOT NULL
            ORDER BY p.process_number
            """
        )
        for r in cur.fetchall():
            out.append({
                "process_number": r[0],
                "defendant": r[1],
                "plaintiff": r[2],
                "foro_code": r[3],
                "class_type": r[4],
                "initial_date": r[5],
                "other_processes": r[6],
            })
    finally:
        conn.close()
    return out


# ---------------------------------------------------------------------------
# Part 1.1 — name search re-verification
# ---------------------------------------------------------------------------

def _query_variants_for_name(name: str) -> List[Tuple[str, Dict[str, Any]]]:
    """Brief §1.1 step 2 + Track B's nested-query hypothesis.

    The nested variant is critical: if `partes` is an Elasticsearch nested-typed
    field, the flat `match.partes.nome` query returns 0 hits even when the data
    is indexed. That would explain Probe B's negative finding without needing
    LegalSuite to be wrong. Worth testing explicitly."""
    first_two_words = " ".join(name.split()[:2]) if name else ""
    return [
        ("exact_match",
         {"query": {"match": {"partes.nome": name}}, "size": 5}),
        ("lowercase_match",
         {"query": {"match": {"partes.nome": name.lower()}}, "size": 5}),
        ("tokenized_match",
         {"query": {"match": {"partes.nome": first_two_words}}, "size": 5}),
        ("match_phrase",
         {"query": {"match_phrase": {"partes.nome": name}}, "size": 5}),
        ("nested_match",
         {"query": {"nested": {"path": "partes",
                                "query": {"match": {"partes.nome": name}}}},
          "size": 5}),
        ("nested_match_phrase",
         {"query": {"nested": {"path": "partes",
                                "query": {"match_phrase": {"partes.nome": name}}}},
          "size": 5}),
    ]


def _hit_party_signal(hit: Dict[str, Any]) -> Dict[str, Any]:
    """Inspect a single hit for any party-shaped data leaking through. Returns
    a record describing what was found vs. what was empty."""
    src = hit.get("_source") or {}
    party_keys = _scan_for_party_keys(src)
    return {
        "numeroProcesso": src.get("numeroProcesso"),
        "tribunal": src.get("tribunal"),
        "classe_codigo": (src.get("classe") or {}).get("codigo"),
        "dataAjuizamento": src.get("dataAjuizamento"),
        "_source_keys": sorted(src.keys()),
        "party_indicator_keys_found": party_keys,
        "has_fields": "fields" in hit,
        "fields_value": hit.get("fields") if "fields" in hit else None,
    }


def per_tribunal_partes_check(run_dir: Path, api_key: str,
                              logger: logging.Logger) -> Dict[str, Any]:
    """For each of the 5 tribunal indices, pull one hit via match_all and
    inspect _source keys. Tells us if `partes` appears in ANY tribunal —
    if yes, the TJSP finding is tribunal-specific not API-wide; if no,
    `partes` is genuinely absent across the public API.

    Also runs a `partes.nome` "Itaú Unibanco" canary against each tribunal
    so we can see if any tribunal returns hits (would be a definitive
    falsification of the TJSP-only finding)."""
    out: Dict[str, Any] = {"per_tribunal": []}
    for idx in TRIBUNAL_INDICES:
        url = URL_FOR_INDEX.format(index=idx)
        # Pull a sample hit to inspect _source keys
        sample_res = _post(url, {"query": {"match_all": {}}, "size": 1},
                           api_key, logger=logger)
        write_json(run_dir / f"p11_xtrib_sample__{idx}.json", sample_res)
        hits = (sample_res.get("body", {}) or {}).get("hits", {}).get("hits", []) \
               if sample_res.get("body") else []
        source_keys: List[str] = []
        party_keys: List[str] = []
        if hits:
            src = hits[0].get("_source", {}) or {}
            source_keys = sorted(src.keys())
            party_keys = _scan_for_party_keys(src)

        # Canary: does partes.nome:"Itaú Unibanco" return hits on this index?
        canary_res = _post(url, {
            "query": {"match": {"partes.nome": "Itaú Unibanco"}}, "size": 1,
        }, api_key, logger=logger)
        write_json(run_dir / f"p11_xtrib_canary__{idx}.json", canary_res)

        out["per_tribunal"].append({
            "index": idx,
            "sample_status": sample_res.get("status"),
            "sample_hit_count": _hit_count(sample_res.get("body")),
            "source_keys": source_keys,
            "partes_in_source": "partes" in source_keys,
            "party_indicator_keys_found": party_keys,
            "canary_status": canary_res.get("status"),
            "canary_total_hits": _hit_count(canary_res.get("body")),
            "canary_error": canary_res.get("error"),
        })

    out["any_tribunal_has_partes_in_source"] = any(
        t["partes_in_source"] for t in out["per_tribunal"]
    )
    out["any_tribunal_canary_returned_hits"] = any(
        t["canary_total_hits"] > 0 for t in out["per_tribunal"]
    )
    write_json(run_dir / "p11_xtrib_summary.json", out)
    return out


def existence_check(test_cases: List[Dict[str, Any]],
                    run_dir: Path, api_key: str,
                    logger: logging.Logger) -> Dict[str, Any]:
    """Precondition for Part 1.1: is each test case actually in the DataJud TJSP
    index? Without this we can't tell apart "name search broken" from
    "case not indexed."

    Per Track B: also dumps the full _source so we can see exactly which top-
    level keys come back, including verifying that `partes` is stripped."""
    tjsp_url = URL_FOR_INDEX.format(index="api_publica_tjsp")
    out: Dict[str, Any] = {"checked": [], "indexed_count": 0,
                           "all_source_keys_observed": set()}
    for case in test_cases:
        pn = case["process_number"]
        digits = pn.replace("-", "").replace(".", "")
        # Try both digits-only and formatted (the prior datajud probe found
        # digits-only is the more reliable match shape).
        payload = {"query": {"match": {"numeroProcesso": digits}}, "size": 1}
        res = _post(tjsp_url, payload, api_key, logger=logger)
        safe_pn = pn.replace("/", "_")
        write_json(run_dir / f"p11_existence__{safe_pn}.json", res)
        hits = (res.get("body", {}) or {}).get("hits", {}).get("hits", []) \
               if res.get("body") else []
        record: Dict[str, Any] = {
            "process_number": pn,
            "defendant": case["defendant"],
            "status": res.get("status"),
            "total_hits": _hit_count(res.get("body")),
            "indexed": bool(hits),
            "source_keys": [],
            "party_indicator_keys_found": [],
        }
        if hits:
            out["indexed_count"] += 1
            src = hits[0].get("_source", {}) or {}
            record["source_keys"] = sorted(src.keys())
            record["party_indicator_keys_found"] = _scan_for_party_keys(src)
            out["all_source_keys_observed"].update(src.keys())
        out["checked"].append(record)

    out["all_source_keys_observed"] = sorted(out["all_source_keys_observed"])
    write_json(run_dir / "p11_existence_summary.json", out)
    return out


def common_name_baseline(run_dir: Path, api_key: str,
                         logger: logging.Logger) -> Dict[str, Any]:
    """Track B's "if name search works at all, this returns hits" canary.

    `Itaú Unibanco` is a plaintiff in 2 of our 4 snapshot cases and a defendant
    in ten of thousands of consumer-law TJSP cases. A 0-hit result here means
    `partes.nome` is not searchable at all (or our query shape is wrong);
    a positive result means name search works and our specific 0-hit defendants
    were name-encoding / index-gap issues."""
    tjsp_url = URL_FOR_INDEX.format(index="api_publica_tjsp")
    canary_names = [
        "Itaú Unibanco",
        "Banco do Brasil",
        "Bradesco",
    ]
    out: Dict[str, Any] = {"canary_names": canary_names, "per_name": []}
    for name in canary_names:
        per_name: Dict[str, Any] = {"name": name, "variants": []}
        for vname, payload in _query_variants_for_name(name):
            res = _post(tjsp_url, payload, api_key, logger=logger)
            safe_name = re.sub(r"[^A-Za-z0-9]", "_", name)[:30]
            write_json(run_dir / f"p11_canary__{safe_name}__{vname}.json", res)
            per_name["variants"].append({
                "variant": vname,
                "ok": res.get("ok"),
                "status": res.get("status"),
                "total_hits": _hit_count(res.get("body")),
                "latency_ms": res.get("latency_ms"),
                "error": res.get("error"),
            })
        out["per_name"].append(per_name)

    out["any_variant_returned_hits"] = any(
        v["total_hits"] > 0
        for n in out["per_name"]
        for v in n["variants"]
    )
    write_json(run_dir / "p11_canary_summary.json", out)
    return out


def source_filter_bypass_on_known_case(case_pn: str,
                                       run_dir: Path, api_key: str,
                                       logger: logging.Logger) -> Dict[str, Any]:
    """Brief §1.1 step 5, but run against a case we KNOW exists (via existence
    check) rather than waiting for a name-search hit. If `fields:
    ["partes.nome"]` returns the party name despite partes being stripped from
    _source, the data IS indexed — just hidden from default responses. That's
    the empirical proof name search has data to match against."""
    tjsp_url = URL_FOR_INDEX.format(index="api_publica_tjsp")
    digits = case_pn.replace("-", "").replace(".", "")
    base_q: Dict[str, Any] = {
        "query": {"match": {"numeroProcesso": digits}},
        "size": 1,
    }
    variants: List[Tuple[str, Dict[str, Any]]] = [
        ("default_source", {**base_q}),
        ("source_true", {**base_q, "_source": True}),
        ("source_partes_list", {**base_q, "_source": ["partes"]}),
        ("source_partes_wildcard",
         {**base_q, "_source": {"includes": ["partes.*"]}}),
        ("fields_partes_nome",
         {**base_q, "fields": ["partes.nome"]}),
        ("fields_partes_star",
         {**base_q, "fields": ["partes.*"]}),
        ("docvalue_partes_keyword",
         {**base_q, "docvalue_fields": ["partes.nome.keyword"]}),
    ]
    out: Dict[str, Any] = {"case": case_pn, "results": []}
    for sv_name, payload in variants:
        res = _post(tjsp_url, payload, api_key, logger=logger)
        write_json(run_dir / f"p11_bypass__{sv_name}.json", res)
        hits = (res.get("body", {}) or {}).get("hits", {}).get("hits", []) \
               if res.get("body") else []
        first = hits[0] if hits else None
        sig = _hit_party_signal(first) if first else None
        out["results"].append({
            "variant": sv_name,
            "status": res.get("status"),
            "returned_hits": len(hits),
            "first_hit_party_signal": sig,
            "error": res.get("error"),
        })
    out["any_variant_leaked_parties"] = any(
        (r.get("first_hit_party_signal") or {}).get("party_indicator_keys_found")
        or (r.get("first_hit_party_signal") or {}).get("fields_value")
        for r in out["results"]
    )
    write_json(run_dir / "p11_bypass_summary.json", out)
    return out


def run_part_1_1_name_search(test_cases: List[Dict[str, Any]],
                             run_dir: Path, api_key: str,
                             logger: logging.Logger) -> Dict[str, Any]:
    """Name-search re-verification per brief §1.1 + Track B's methodology
    corrections.

    Order of operations:
      1. existence_check — verify each test case is in the DataJud TJSP
         index. Without this, downstream 0-hits is uninterpretable.
      2. common_name_baseline — query partes.nome for "Itaú Unibanco" and
         other high-frequency names. If THIS returns 0, partes.nome is not
         searchable in the public API and Track B's hypothesis is wrong.
      3. variant testing — 6 query shapes (4 from brief + 2 nested) per
         defendant per tribunal. Only meaningful AFTER step 1 confirms
         indexing AND step 2 establishes whether name search works at all.
      4. source_filter_bypass — always run on a known-indexed case
         (regardless of variant results); tests whether parties are
         readable via fields/docvalue_fields API.
    """
    findings: Dict[str, Any] = {
        "test_case_count": len(test_cases),
        "summary": {},
    }

    # Step 1 — existence
    logger.info("Part 1.1 step 1: existence check (numeroProcesso queries)")
    findings["existence"] = existence_check(test_cases, run_dir, api_key, logger)
    logger.info("  %d/%d cases confirmed indexed; observed _source keys: %s",
                findings["existence"]["indexed_count"], len(test_cases),
                ", ".join(findings["existence"]["all_source_keys_observed"]))

    # Step 1b — cross-tribunal `partes` field probe
    logger.info("Part 1.1 step 1b: per-tribunal partes-field check")
    findings["xtrib"] = per_tribunal_partes_check(run_dir, api_key, logger)
    logger.info("  any tribunal has partes in _source: %s; any canary hits: %s",
                findings["xtrib"]["any_tribunal_has_partes_in_source"],
                findings["xtrib"]["any_tribunal_canary_returned_hits"])

    # Step 2 — canary
    logger.info("Part 1.1 step 2: common-name baseline (canary)")
    findings["canary"] = common_name_baseline(run_dir, api_key, logger)
    logger.info("  Canary any-variant hits: %s",
                findings["canary"]["any_variant_returned_hits"])

    # Step 3 — per-case variant tests on indexed cases
    indexed_cases = [c for c, rec in zip(test_cases, findings["existence"]["checked"])
                     if rec["indexed"]]
    logger.info("Part 1.1 step 3: variant tests on %d indexed cases", len(indexed_cases))

    overall_any_hit = False
    per_case: List[Dict[str, Any]] = []
    tjsp_url = URL_FOR_INDEX.format(index="api_publica_tjsp")
    for case in indexed_cases:
        defendant = case["defendant"]
        pn = case["process_number"]
        logger.info("  case %s defendant=%r", pn, defendant)
        case_record: Dict[str, Any] = {
            "process_number": pn,
            "defendant": defendant,
            "tjsp_variants": [],
            "tjrj_variants": [],
            "tjmg_variants": [],
            "any_hit_anywhere": False,
        }

        for vname, payload in _query_variants_for_name(defendant):
            res = _post(tjsp_url, payload, api_key, logger=logger)
            safe_pn = pn.replace("/", "_")
            write_json(run_dir / f"p11_{safe_pn}__tjsp__{vname}.json", res)
            hits = (res.get("body", {}) or {}).get("hits", {}).get("hits", []) \
                   if res.get("body") else []
            record = {
                "variant": vname,
                "ok": res.get("ok"),
                "status": res.get("status"),
                "total_hits": _hit_count(res.get("body")),
                "returned_hits": len(hits),
                "latency_ms": res.get("latency_ms"),
                "hit_party_signals": [_hit_party_signal(h) for h in hits[:3]],
                "error": res.get("error"),
            }
            case_record["tjsp_variants"].append(record)
            if record["total_hits"] > 0:
                case_record["any_hit_anywhere"] = True
                overall_any_hit = True

        if not case_record["any_hit_anywhere"]:
            for idx in ("api_publica_tjrj", "api_publica_tjmg"):
                url = URL_FOR_INDEX.format(index=idx)
                for vname, payload in _query_variants_for_name(defendant):
                    res = _post(url, payload, api_key, logger=logger)
                    write_json(
                        run_dir / f"p11_{safe_pn}__{idx[-4:]}__{vname}.json", res,
                    )
                    hits = (res.get("body", {}) or {}).get("hits", {}).get(
                        "hits", []) if res.get("body") else []
                    rec = {
                        "variant": vname,
                        "ok": res.get("ok"),
                        "status": res.get("status"),
                        "total_hits": _hit_count(res.get("body")),
                        "returned_hits": len(hits),
                        "latency_ms": res.get("latency_ms"),
                        "error": res.get("error"),
                    }
                    case_record[f"{idx[-4:]}_variants"].append(rec)
                    if rec["total_hits"] > 0:
                        case_record["any_hit_anywhere"] = True
                        overall_any_hit = True

        per_case.append(case_record)

    findings["per_case"] = per_case

    # Step 4 — bypass test, always on a known-indexed case
    if indexed_cases:
        target = indexed_cases[0]["process_number"]
        logger.info("Part 1.1 step 4: source-filter bypass on %s", target)
        findings["bypass"] = source_filter_bypass_on_known_case(
            target, run_dir, api_key, logger,
        )
        logger.info("  bypass leaked parties: %s",
                    findings["bypass"]["any_variant_leaked_parties"])
    else:
        findings["bypass"] = {"skipped": "no indexed cases to test against"}

    findings["summary"] = _summarize_part_1_1(findings, overall_any_hit)

    write_json(run_dir / "part_1_1_findings.json", findings)
    return findings


def _summarize_part_1_1(findings: Dict[str, Any],
                        overall_any_hit: bool) -> Dict[str, Any]:
    """Three-axis read: cases-indexed, canary-works, defendant-hits."""
    existence = findings.get("existence", {})
    canary = findings.get("canary", {})
    bypass = findings.get("bypass", {})
    per_case = findings.get("per_case", [])

    indexed_count = existence.get("indexed_count", 0)
    total_cases = findings.get("test_case_count", 0)
    canary_works = canary.get("any_variant_returned_hits", False)
    cases_with_any_hit = sum(1 for c in per_case if c["any_hit_anywhere"])

    # Per-variant TJSP success counts (across indexed-case defendant queries)
    variants = ["exact_match", "lowercase_match", "tokenized_match",
                "match_phrase", "nested_match", "nested_match_phrase"]
    per_variant: Dict[str, int] = {v: 0 for v in variants}
    for c in per_case:
        for v in c.get("tjsp_variants", []):
            per_variant[v["variant"]] = per_variant.get(v["variant"], 0) + (
                1 if v["total_hits"] > 0 else 0
            )

    # Canary per-variant success
    canary_per_variant: Dict[str, int] = {v: 0 for v in variants}
    for n in canary.get("per_name", []):
        for v in n.get("variants", []):
            if v["total_hits"] > 0:
                canary_per_variant[v["variant"]] = \
                    canary_per_variant.get(v["variant"], 0) + 1

    bypass_leaked = bypass.get("any_variant_leaked_parties", False)

    # Verdict
    if not indexed_count:
        verdict = (
            "INCONCLUSIVE — 0 of our test cases are indexed in DataJud TJSP. "
            "We cannot tell if name search works because we have no ground "
            "truth that 'should' return our test cases. Need different test "
            "cases (known-indexed) before re-running variant tests."
        )
    elif canary_works and cases_with_any_hit > 0:
        verdict = (
            f"FALSIFIED — DataJud DOES support name search via partes.nome. "
            f"Canary names returned hits AND {cases_with_any_hit}/{indexed_count} "
            f"indexed-case defendants returned hits. Prior Probe B finding "
            f"('DataJud has no name search') is wrong, consistent with Track B."
        )
    elif canary_works and cases_with_any_hit == 0:
        verdict = (
            f"PARTIAL FALSIFICATION — name search works for common/major names "
            f"(canary returned hits) but 0 hits across all {indexed_count} of "
            f"our specific defendants × 6 variants × TJSP+TJRJ+TJMG pivot. "
            f"Specific-defendant lookup is brittle (name-normalization sensitive); "
            f"common-name aggregation may still be viable."
        )
    else:
        verdict = (
            "CONFIRMED-WITH-CAVEAT — name search returns 0 hits even on canary "
            "names that MUST appear in TJSP party data thousands of times. "
            "Either partes.nome is not searchable in the public API (contra "
            "LegalSuite's documented example), or there's a query-shape we "
            "haven't tried. Worth escalating to LegalSuite docs review before "
            "accepting."
        )

    return {
        "verdict": verdict,
        "indexed_cases": f"{indexed_count}/{total_cases}",
        "canary_works": canary_works,
        "canary_per_variant_success": canary_per_variant,
        "defendant_hits": f"{cases_with_any_hit}/{indexed_count}",
        "defendant_per_variant_success": per_variant,
        "bypass_leaked_party_data": bypass_leaked,
        "source_keys_observed": existence.get("all_source_keys_observed", []),
    }


# ---------------------------------------------------------------------------
# Part 1.2 — CPF/CNPJ search
# ---------------------------------------------------------------------------

# Banco do Brasil — a publicly-known CNPJ. Per the brief: if eSAJ surfaces a
# CNPJ for one of our snapshot defendants we'd use that, but eSAJ doesn't
# expose CNPJs in our scrape, so we use this widely-active public CNPJ.
KNOWN_CNPJ_DIGITS = "00000000000191"          # Banco do Brasil
KNOWN_CNPJ_FORMATTED = "00.000.000/0001-91"

CPF_CNPJ_FIELD_VARIANTS: Tuple[str, ...] = (
    "cpfCnpj",
    "numeroDocumentoPrincipal",
    "documento.numero",
    "partes.documento.numero",
    "partes.cpfCnpj",
)


def run_part_1_2_cpf_cnpj(run_dir: Path, api_key: str,
                          logger: logging.Logger) -> Dict[str, Any]:
    """Test CPF/CNPJ search across all 5 tribunal indices for each candidate
    field name. Both digits-only and formatted variants. Records status,
    hit count, and any error message per cell."""
    logger.info("Part 1.2: CPF/CNPJ search probe")
    results: List[Dict[str, Any]] = []
    for idx in TRIBUNAL_INDICES:
        url = URL_FOR_INDEX.format(index=idx)
        for field in CPF_CNPJ_FIELD_VARIANTS:
            for value_label, value in (("digits", KNOWN_CNPJ_DIGITS),
                                       ("formatted", KNOWN_CNPJ_FORMATTED)):
                payload = {"query": {"match": {field: value}}, "size": 1}
                res = _post(url, payload, api_key, logger=logger)
                safe_field = field.replace(".", "_")
                write_json(
                    run_dir / f"p12_{idx}__{safe_field}__{value_label}.json",
                    res,
                )
                results.append({
                    "tribunal": idx,
                    "field": field,
                    "value_label": value_label,
                    "status": res.get("status"),
                    "total_hits": _hit_count(res.get("body")),
                    "ok": res.get("ok"),
                    "error_excerpt": (res.get("raw") or "")[:200] if res.get("raw") else None,
                })

    any_hits = any(r["total_hits"] > 0 for r in results)
    field_winners = sorted({r["field"] for r in results if r["total_hits"] > 0})
    out: Dict[str, Any] = {
        "tested_cnpj_digits": KNOWN_CNPJ_DIGITS,
        "field_variants_tested": list(CPF_CNPJ_FIELD_VARIANTS),
        "per_cell": results,
        "any_cell_returned_hits": any_hits,
        "field_winners": field_winners,
        "verdict": (
            f"CPF/CNPJ search WORKS — fields {field_winners} return hits."
            if any_hits
            else "CONFIRMED — CPF/CNPJ not queryable in public API. "
                 "No field variant × tribunal cell returned hits. Consistent "
                 "with LegalSuite's LGPD framing."
        ),
    }
    write_json(run_dir / "part_1_2_findings.json", out)
    return out


# ---------------------------------------------------------------------------
# Part 1.3 — TPU movement code uniformity
# ---------------------------------------------------------------------------

# Three well-known TPU codes per the brief.
TPU_TEST_CODES: Tuple[Tuple[int, str], ...] = (
    (22, "Distribuição"),
    (26, "Conclusão"),
    (85, "Arquivamento"),
)


def run_part_1_3_tpu_uniformity(run_dir: Path, api_key: str,
                                logger: logging.Logger) -> Dict[str, Any]:
    """For each (tribunal, TPU code), query cases containing that code in
    movimentos.codigo and read back the movimento.nome strings. Tabulate the
    most-common name per (tribunal, code) to see if naming is consistent."""
    logger.info("Part 1.3: TPU code uniformity probe")
    per_cell: List[Dict[str, Any]] = []
    for idx in TRIBUNAL_INDICES:
        url = URL_FOR_INDEX.format(index=idx)
        for code, expected_label in TPU_TEST_CODES:
            payload = {
                "query": {"match": {"movimentos.codigo": code}},
                "size": 20,
            }
            res = _post(url, payload, api_key, logger=logger)
            write_json(run_dir / f"p13_{idx}__code_{code}.json", res)
            hits = (res.get("body", {}) or {}).get("hits", {}).get("hits", []) \
                   if res.get("body") else []
            # For each hit, scan its movimentos[] for the matching code and
            # collect the nome strings.
            names: List[str] = []
            for h in hits:
                movs = (h.get("_source") or {}).get("movimentos") or []
                for m in movs:
                    if m.get("codigo") == code and m.get("nome"):
                        names.append(m["nome"].strip())
                        break  # per case, count name once
            name_counts = Counter(names)
            most_common = name_counts.most_common(3)
            per_cell.append({
                "tribunal": idx,
                "code": code,
                "expected_label": expected_label,
                "status": res.get("status"),
                "total_hits": _hit_count(res.get("body")),
                "sampled_hits": len(hits),
                "distinct_names_seen": len(name_counts),
                "name_top3": [{"name": n, "count": c} for n, c in most_common],
            })

    # Cross-tribunal name consistency check
    code_consistency: Dict[int, Dict[str, Any]] = {}
    for code, _ in TPU_TEST_CODES:
        names_per_tribunal: Dict[str, Optional[str]] = {}
        for cell in per_cell:
            if cell["code"] != code:
                continue
            top = cell["name_top3"][0]["name"] if cell["name_top3"] else None
            names_per_tribunal[cell["tribunal"]] = top
        distinct_top_names = sorted({n for n in names_per_tribunal.values() if n})
        code_consistency[code] = {
            "top_name_per_tribunal": names_per_tribunal,
            "distinct_top_names": distinct_top_names,
            "uniform": len(distinct_top_names) <= 1,
        }

    all_uniform = all(c["uniform"] for c in code_consistency.values())
    out: Dict[str, Any] = {
        "per_cell": per_cell,
        "code_consistency": {str(k): v for k, v in code_consistency.items()},
        "all_uniform": all_uniform,
        "verdict": (
            "REFRESH: TPU codes appear NATIONALLY UNIFORM across the 5 "
            "tribunals we tested (top-1 nome matches for all 3 test codes). "
            "Prior 'NOT uniform' finding from May 2026 may be stale, or our "
            "3-code sample is too narrow."
            if all_uniform
            else "CONFIRMED: TPU codes NOT nationally uniform. Different "
                 "tribunals render different names for the same numeric code."
        ),
    }
    write_json(run_dir / "part_1_3_findings.json", out)
    return out


# ---------------------------------------------------------------------------
# Part 2 — field inventory (the unprobed fields)
# ---------------------------------------------------------------------------

# Process numbers we want to inspect _source for. Mix of our snapshot store +
# the sample_processes.yaml cases that prior datajud probe verified exist.
PART_2_SAMPLE_PROCESSES: Tuple[str, ...] = (
    # Our snapshot store (4)
    "1002177-13.2020.8.26.0100",
    "1033164-10.2022.8.26.0602",
    "1063559-02.2023.8.26.0100",
    "1185092-25.2023.8.26.0100",
    # sample_processes.yaml (6) — known-indexed from prior probe
    "1018045-50.2015.8.26.0506",
    "0141625-04.2009.8.26.0100",
    "1002273-51.2024.8.26.0629",
    "0700505-13.1990.8.26.0547",
    "1185179-78.2023.8.26.0100",
    "1046043-29.2020.8.26.0114",
)

# Field paths we want to probe. Brief §2 table.
UNPROBED_FIELDS: Tuple[Tuple[str, str], ...] = (
    ("assuntos", "TPU subject classification (array)"),
    ("grau", "Instance level (G1/G2/JE)"),
    ("orgaoJulgador.codigoMunicipioIBGE", "Geographic pivot via IBGE municipality codes"),
    ("valor", "Monetary value (numeric)"),
    ("movimentos.complementosTabelados", "Structured sub-codes on movements (HIGH-VALUE)"),
    ("movimentos.dataHora", "Movement timestamp format consistency"),
)


def _dig(obj: Any, dotted_path: str) -> Any:
    """Resolve dotted path into nested dict/list. For list segments, returns
    a list of values (one per element)."""
    cur: Any = obj
    parts = dotted_path.split(".")
    for i, p in enumerate(parts):
        if cur is None:
            return None
        if isinstance(cur, dict):
            cur = cur.get(p)
        elif isinstance(cur, list):
            # Apply the remaining path to each element
            rest = ".".join(parts[i:])
            return [_dig(e, rest) for e in cur]
        else:
            return None
    return cur


def _is_populated(value: Any) -> bool:
    """A field is 'populated' if it has any non-None, non-empty content
    (recursively through lists)."""
    if value is None:
        return False
    if isinstance(value, list):
        return any(_is_populated(v) for v in value)
    if isinstance(value, dict):
        return any(_is_populated(v) for v in value.values())
    if isinstance(value, str):
        return bool(value.strip())
    return True  # numbers, bools


def run_part_2_field_inventory(run_dir: Path, api_key: str,
                               logger: logging.Logger) -> Dict[str, Any]:
    """For each unprobed field × test process, record: present in _source,
    populated. Then test queryability with a representative query."""
    logger.info("Part 2: field inventory across %d processes",
                len(PART_2_SAMPLE_PROCESSES))
    tjsp_url = URL_FOR_INDEX.format(index="api_publica_tjsp")

    # First pass: pull _source for each process, build presence/population matrix.
    per_process: List[Dict[str, Any]] = []
    for pn in PART_2_SAMPLE_PROCESSES:
        digits = pn.replace("-", "").replace(".", "")
        res = _post(tjsp_url,
                    {"query": {"match": {"numeroProcesso": digits}}, "size": 1},
                    api_key, logger=logger)
        safe_pn = pn.replace("/", "_")
        write_json(run_dir / f"p2_source__{safe_pn}.json", res)
        hits = (res.get("body", {}) or {}).get("hits", {}).get("hits", []) \
               if res.get("body") else []
        if not hits:
            per_process.append({
                "process_number": pn,
                "indexed": False,
                "fields": {},
            })
            continue
        src = hits[0].get("_source") or {}
        field_state: Dict[str, Dict[str, Any]] = {}
        for field_path, _desc in UNPROBED_FIELDS:
            value = _dig(src, field_path)
            field_state[field_path] = {
                "present": value is not None,
                "populated": _is_populated(value),
                "value_sample": _sample_value_for_doc(value),
            }
        per_process.append({
            "process_number": pn,
            "indexed": True,
            "fields": field_state,
        })

    # Aggregate per-field across processes
    field_aggregates: Dict[str, Dict[str, Any]] = {}
    for field_path, desc in UNPROBED_FIELDS:
        indexed_processes = [p for p in per_process if p["indexed"]]
        present_count = sum(1 for p in indexed_processes
                            if p["fields"].get(field_path, {}).get("present"))
        populated_count = sum(1 for p in indexed_processes
                              if p["fields"].get(field_path, {}).get("populated"))
        field_aggregates[field_path] = {
            "description": desc,
            "present_rate": f"{present_count}/{len(indexed_processes)}",
            "populated_rate": f"{populated_count}/{len(indexed_processes)}",
        }

    # Second pass: test queryability per field with a representative query.
    queryability: Dict[str, Dict[str, Any]] = {}
    queryable_tests: List[Tuple[str, Dict[str, Any], str]] = [
        ("grau",
         {"query": {"match": {"grau": "G1"}}, "size": 1},
         "match grau=G1"),
        ("assuntos.codigo",
         {"query": {"match": {"assuntos.codigo": 7724}}, "size": 1},
         "match assuntos.codigo=7724 (Defesa do Consumidor / common)"),
        ("orgaoJulgador.codigoMunicipioIBGE",
         {"query": {"match": {"orgaoJulgador.codigoMunicipioIBGE": 3550308}}, "size": 1},
         "match codigoMunicipioIBGE=3550308 (São Paulo)"),
        ("valor_range_100k",
         {"query": {"range": {"valor": {"gte": 100000}}}, "size": 1},
         "range valor>=100000"),
        ("valor_exists",
         {"query": {"exists": {"field": "valor"}}, "size": 1},
         "exists query on valor (any value)"),
        ("movimentos.complementosTabelados.codigo",
         {"query": {"nested": {
             "path": "movimentos",
             "query": {"match": {"movimentos.complementosTabelados.codigo": 3}},
         }}, "size": 1},
         "nested movimentos.complementosTabelados.codigo=3 (Procedência)"),
        ("movimentos.complementosTabelados.codigo_flat",
         {"query": {"match": {"movimentos.complementosTabelados.codigo": 3}},
          "size": 1},
         "flat match movimentos.complementosTabelados.codigo=3"),
        ("movimentos.dataHora",
         {"query": {"range": {"movimentos.dataHora":
                              {"gte": "2024-01-01", "lte": "2024-12-31"}}},
          "size": 1},
         "range movimentos.dataHora in 2024"),
    ]
    for label, payload, description in queryable_tests:
        res = _post(tjsp_url, payload, api_key, logger=logger)
        safe_label = label.replace(".", "_")
        write_json(run_dir / f"p2_query__{safe_label}.json", res)
        queryability[label] = {
            "description": description,
            "status": res.get("status"),
            "total_hits": _hit_count(res.get("body")),
            "ok": res.get("ok"),
            "error_excerpt": (res.get("raw") or "")[:300] if res.get("raw") else None,
        }

    out: Dict[str, Any] = {
        "test_processes": list(PART_2_SAMPLE_PROCESSES),
        "indexed_count": sum(1 for p in per_process if p["indexed"]),
        "per_process": per_process,
        "field_aggregates": field_aggregates,
        "queryability": queryability,
    }
    # Highlight: did complementosTabelados show up?
    ct = field_aggregates.get("movimentos.complementosTabelados", {})
    out["complementos_tabelados_signal"] = {
        "present_rate": ct.get("present_rate"),
        "populated_rate": ct.get("populated_rate"),
        "nested_query_works": queryability.get(
            "movimentos.complementosTabelados.codigo", {}).get("total_hits", 0) > 0,
    }
    write_json(run_dir / "part_2_findings.json", out)
    return out


def _sample_value_for_doc(value: Any, depth: int = 0) -> Any:
    """Compact, paste-into-markdown-safe representation of a sample value."""
    if value is None or depth > 3:
        return value
    if isinstance(value, list):
        if not value:
            return []
        # Show first element only
        return [_sample_value_for_doc(value[0], depth + 1)]
    if isinstance(value, dict):
        return {k: _sample_value_for_doc(v, depth + 1) for k, v in list(value.items())[:5]}
    if isinstance(value, str) and len(value) > 80:
        return value[:80] + "…"
    return value


# ---------------------------------------------------------------------------
# Part 3.1 — rate limits (cautious)
# ---------------------------------------------------------------------------

def run_part_3_1_rate_limits(run_dir: Path, api_key: str,
                             logger: logging.Logger) -> Dict[str, Any]:
    """Cautious rate-limit probe. Stop at first 429 or 10 req/sec (whichever
    comes first); we do NOT push higher in a single session — risk of API key
    suspension would break every other part of this probe."""
    logger.info("Part 3.1: rate-limit probe (cautious: stop at 10 req/sec or first 429)")
    url = URL_FOR_INDEX.format(index="api_publica_tjsp")
    payload = {"query": {"match_all": {}}, "size": 1}

    # Sustained rates to test, one burst per rate
    rates_to_test = [1, 2, 5, 10]
    burst_seconds = 3   # short bursts, 3 seconds each

    out: Dict[str, Any] = {"bursts": [], "stopped_early": False}
    for rate in rates_to_test:
        n_requests = rate * burst_seconds
        sleep_interval = 1.0 / rate
        logger.info("  burst at %d req/sec (%d requests over ~%ds)",
                    rate, n_requests, burst_seconds)
        statuses: List[int] = []
        latencies: List[int] = []
        any_429 = False
        burst_t0 = time.perf_counter()
        for i in range(n_requests):
            req_start = time.perf_counter()
            res = _post(url, payload, api_key, logger=None)  # quieter
            statuses.append(res.get("status") or 0)
            latencies.append(res.get("latency_ms") or 0)
            if res.get("status") == 429:
                any_429 = True
                break
            # Sleep to maintain target rate
            elapsed = time.perf_counter() - req_start
            sleep_for = sleep_interval - elapsed
            if sleep_for > 0:
                time.sleep(sleep_for)
        burst_elapsed = time.perf_counter() - burst_t0
        actual_rate = len(statuses) / burst_elapsed if burst_elapsed else 0
        out["bursts"].append({
            "target_rate": rate,
            "n_requests_attempted": n_requests,
            "n_requests_completed": len(statuses),
            "actual_rate": round(actual_rate, 2),
            "burst_elapsed_s": round(burst_elapsed, 2),
            "status_counts": dict(Counter(statuses)),
            "latency_median_ms": int(sorted(latencies)[len(latencies) // 2]) if latencies else None,
            "latency_max_ms": max(latencies) if latencies else None,
            "any_429": any_429,
        })
        if any_429:
            out["stopped_early"] = True
            out["stopped_at_rate"] = rate
            logger.warning("  hit 429 at %d req/sec, stopping rate probe", rate)
            break

    out["verdict"] = (
        f"Throttled at {out.get('stopped_at_rate')} req/sec (HTTP 429 seen)."
        if out["stopped_early"]
        else f"Sustained {rates_to_test[-1]} req/sec without 429. Higher rates "
             "not probed; cautious probe stopped at cap to preserve API key."
    )
    write_json(run_dir / "part_3_1_findings.json", out)
    return out


# ---------------------------------------------------------------------------
# Part 3.2 — aggregations
# ---------------------------------------------------------------------------

def run_part_3_2_aggregations(run_dir: Path, api_key: str,
                              logger: logging.Logger) -> Dict[str, Any]:
    """Test whether Elasticsearch aggregation queries work on the public API.
    If they do, Layer 3 design simplifies substantially — server-side counting
    instead of fetch-all-and-aggregate-client-side."""
    logger.info("Part 3.2: aggregation queries")
    url = URL_FOR_INDEX.format(index="api_publica_tjsp")
    tests: List[Tuple[str, Dict[str, Any], str]] = [
        ("terms_classe",
         {"size": 0, "aggs": {"by_classe": {"terms": {"field": "classe.codigo", "size": 10}}}},
         "terms aggregation on classe.codigo"),
        ("date_histogram_year",
         {"size": 0, "aggs": {"by_year": {"date_histogram": {
             "field": "dataAjuizamento", "calendar_interval": "year"}}}},
         "date_histogram aggregation on dataAjuizamento (year)"),
        ("terms_grau",
         {"size": 0, "aggs": {"by_grau": {"terms": {"field": "grau", "size": 10}}}},
         "terms aggregation on grau"),
        ("terms_orgaoJulgador",
         {"size": 0, "aggs": {"by_orgao": {"terms": {"field": "orgaoJulgador.codigo", "size": 10}}}},
         "terms aggregation on orgaoJulgador.codigo"),
        ("cardinality_numeroProcesso",
         {"size": 0, "aggs": {"unique_procs": {"cardinality": {"field": "numeroProcesso.keyword"}}}},
         "cardinality on numeroProcesso (distinct count)"),
        ("nested_movimento_codigo",
         {"size": 0, "aggs": {"by_mov_code": {"nested": {"path": "movimentos"},
             "aggs": {"codes": {"terms": {"field": "movimentos.codigo", "size": 5}}}}}},
         "nested terms aggregation on movimentos.codigo"),
    ]
    results: List[Dict[str, Any]] = []
    for label, payload, desc in tests:
        res = _post(url, payload, api_key, logger=logger)
        write_json(run_dir / f"p32_{label}.json", res)
        body = res.get("body", {}) or {}
        aggs = body.get("aggregations") or {}
        # Did we get aggregation buckets back?
        bucket_count = 0
        sample_buckets = None
        for agg_name, agg_data in aggs.items():
            buckets = agg_data.get("buckets") if isinstance(agg_data, dict) else None
            if buckets and isinstance(buckets, list):
                bucket_count = len(buckets)
                sample_buckets = buckets[:3]
                break
            if isinstance(agg_data, dict) and "value" in agg_data:
                # cardinality returns {value: N}
                sample_buckets = [{"value": agg_data["value"]}]
                bucket_count = 1
        results.append({
            "label": label,
            "description": desc,
            "status": res.get("status"),
            "ok": res.get("ok"),
            "agg_bucket_count": bucket_count,
            "sample_buckets": sample_buckets,
            "error_excerpt": (res.get("raw") or "")[:300] if res.get("raw") else None,
        })

    any_aggs_worked = any(r["agg_bucket_count"] > 0 for r in results)
    out: Dict[str, Any] = {
        "per_aggregation": results,
        "any_aggregation_worked": any_aggs_worked,
        "verdict": (
            "Aggregations WORK on the public API. Layer 3 can use server-side "
            "counting (count cases by foro/classe/etc. in one query)."
            if any_aggs_worked
            else "Aggregations NOT supported (or restricted) on the public API. "
                 "Layer 3 needs fetch-all-and-aggregate-client-side."
        ),
    }
    write_json(run_dir / "part_3_2_findings.json", out)
    return out


# ---------------------------------------------------------------------------
# Part 3.3 — bulk / multi-process patterns
# ---------------------------------------------------------------------------

def run_part_3_3_bulk_patterns(run_dir: Path, api_key: str,
                               logger: logging.Logger) -> Dict[str, Any]:
    """Test multi-process query patterns: _msearch, terms with array,
    multi-index URL pattern."""
    logger.info("Part 3.3: bulk / multi-process query patterns")
    out: Dict[str, Any] = {"tests": []}

    sample_digits = [pn.replace("-", "").replace(".", "")
                     for pn in PART_2_SAMPLE_PROCESSES[:3]]

    # Test 1 — _msearch (NDJSON body)
    msearch_url = "https://api-publica.datajud.cnj.jus.br/_msearch"
    ndjson_lines = []
    for d in sample_digits:
        ndjson_lines.append('{"index": "api_publica_tjsp"}')
        ndjson_lines.append(json.dumps({"query": {"match": {"numeroProcesso": d}}, "size": 1}))
    ndjson_body = "\n".join(ndjson_lines) + "\n"
    msearch_res = _http_post_raw(
        msearch_url, ndjson_body.encode("utf-8"),
        api_key, content_type="application/x-ndjson", logger=logger,
    )
    write_json(run_dir / "p33_msearch.json", msearch_res)
    msearch_responses = (msearch_res.get("body", {}) or {}).get("responses") or []
    out["tests"].append({
        "label": "msearch",
        "description": "POST /_msearch with 3 sub-queries",
        "status": msearch_res.get("status"),
        "n_subresponses": len(msearch_responses),
        "subresponse_hit_counts": [_hit_count(r) for r in msearch_responses],
        "error_excerpt": (msearch_res.get("raw") or "")[:200] if msearch_res.get("raw") else None,
    })

    # Test 2 — terms query with array of process numbers
    tjsp_url = URL_FOR_INDEX.format(index="api_publica_tjsp")
    terms_payload = {
        "query": {"terms": {"numeroProcesso": sample_digits}},
        "size": 10,
    }
    terms_res = _post(tjsp_url, terms_payload, api_key, logger=logger)
    write_json(run_dir / "p33_terms.json", terms_res)
    out["tests"].append({
        "label": "terms_array",
        "description": "Single query with terms[] containing 3 process numbers",
        "status": terms_res.get("status"),
        "total_hits": _hit_count(terms_res.get("body")),
        "error_excerpt": (terms_res.get("raw") or "")[:200] if terms_res.get("raw") else None,
    })

    # Test 3 — multi-index URL pattern
    multi_url = "https://api-publica.datajud.cnj.jus.br/api_publica_tjsp,api_publica_tjrj/_search"
    multi_payload = {"query": {"match": {"numeroProcesso": sample_digits[0]}}, "size": 1}
    multi_res = _post(multi_url, multi_payload, api_key, logger=logger)
    write_json(run_dir / "p33_multi_index.json", multi_res)
    out["tests"].append({
        "label": "multi_index_url",
        "description": "GET against /api_publica_tjsp,api_publica_tjrj/_search",
        "status": multi_res.get("status"),
        "total_hits": _hit_count(multi_res.get("body")),
        "error_excerpt": (multi_res.get("raw") or "")[:200] if multi_res.get("raw") else None,
    })

    out["any_bulk_pattern_works"] = any(
        t.get("status") == 200
        and (t.get("n_subresponses", 0) > 0 or t.get("total_hits", 0) > 0)
        for t in out["tests"]
    )
    write_json(run_dir / "part_3_3_findings.json", out)
    return out


def _http_post_raw(url: str, body: bytes, api_key: str, *,
                   content_type: str, logger: logging.Logger) -> Dict[str, Any]:
    """Like _post but doesn't json-encode; lets us send NDJSON for _msearch."""
    import urllib.request, urllib.error
    headers = {
        "Authorization": f"APIKey {api_key}",
        "User-Agent": "poursuite-probes/0.1",
        "Content-Type": content_type,
    }
    req = urllib.request.Request(url, data=body, headers=headers, method="POST")
    t0 = time.perf_counter()
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
            latency = int((time.perf_counter() - t0) * 1000)
            try:
                import json as _json
                return {"ok": True, "status": resp.status,
                        "body": _json.loads(raw), "raw": None,
                        "error": None, "latency_ms": latency}
            except Exception:
                return {"ok": False, "status": resp.status, "body": None,
                        "raw": raw, "error": "non-JSON body", "latency_ms": latency}
    except urllib.error.HTTPError as e:
        latency = int((time.perf_counter() - t0) * 1000)
        try:
            err_body = e.read().decode("utf-8", errors="replace")
        except Exception:
            err_body = ""
        return {"ok": False, "status": e.code, "body": None, "raw": err_body,
                "error": f"HTTPError {e.code} {e.reason}", "latency_ms": latency}
    except Exception as e:  # noqa: BLE001
        latency = int((time.perf_counter() - t0) * 1000)
        return {"ok": False, "status": None, "body": None, "raw": None,
                "error": f"{type(e).__name__}: {e}", "latency_ms": latency}


# Lazy json import for the helper
import json  # noqa: E402


# ---------------------------------------------------------------------------
# Part 3.4 — search_after pagination
# ---------------------------------------------------------------------------

def run_part_3_4_search_after(run_dir: Path, api_key: str,
                              logger: logging.Logger) -> Dict[str, Any]:
    """Test the search_after deep-paging pattern. Two consecutive queries:
    first at size 10000, second using the last hit's sort values."""
    logger.info("Part 3.4: search_after pagination probe")
    url = URL_FOR_INDEX.format(index="api_publica_tjsp")

    # Try several sort options. id/numeroProcesso are text fields with
    # fielddata disabled — sorting on them errors. dataAjuizamento (date) and
    # @timestamp work; _doc is always available and efficient for harvesting.
    sort_variants: List[Tuple[str, List[Dict[str, Any]]]] = [
        ("doc_only", [{"_doc": "asc"}]),
        ("dataAjuizamento_desc", [{"dataAjuizamento": {"order": "desc"}}]),
        ("at_timestamp_desc", [{"@timestamp": {"order": "desc"}}]),
    ]

    out: Dict[str, Any] = {"variants": []}
    for sort_label, sort_clause in sort_variants:
        # Use size 1000 (10000 was rejected — let's see max page first)
        payload_a = {"size": 1000, "query": {"match_all": {}}, "sort": sort_clause}
        res_a = _post(url, payload_a, api_key, logger=logger)
        write_json(run_dir / f"p34_{sort_label}__page1.json", res_a)
        hits_a = (res_a.get("body", {}) or {}).get("hits", {}).get("hits", []) \
                 if res_a.get("body") else []

        record: Dict[str, Any] = {
            "sort_label": sort_label,
            "page1_status": res_a.get("status"),
            "page1_hit_count": len(hits_a),
            "page1_total": _hit_count(res_a.get("body")),
            "page1_latency_ms": res_a.get("latency_ms"),
            "page1_error_excerpt": (res_a.get("raw") or "")[:300] if res_a.get("raw") else None,
        }

        if not hits_a:
            record["verdict"] = "page 1 returned no hits"
            out["variants"].append(record)
            continue

        last_sort = hits_a[-1].get("sort")
        if not last_sort:
            record["verdict"] = "no sort values on hits"
            out["variants"].append(record)
            continue

        payload_b = {
            "size": 100, "query": {"match_all": {}},
            "sort": sort_clause, "search_after": last_sort,
        }
        res_b = _post(url, payload_b, api_key, logger=logger)
        write_json(run_dir / f"p34_{sort_label}__page2.json", res_b)
        hits_b = (res_b.get("body", {}) or {}).get("hits", {}).get("hits", []) \
                 if res_b.get("body") else []
        page2_first_sort = hits_b[0].get("sort") if hits_b else None

        record["last_sort_page1"] = last_sort
        record["page2_status"] = res_b.get("status")
        record["page2_hit_count"] = len(hits_b)
        record["page2_first_sort"] = page2_first_sort
        record["page2_latency_ms"] = res_b.get("latency_ms")
        record["verdict"] = (
            f"works — page 2 returned {len(hits_b)} hits"
            if hits_b
            else "page 2 returned 0 hits"
        )
        out["variants"].append(record)

    any_works = any(v.get("page2_hit_count", 0) > 0 for v in out["variants"])
    out["any_search_after_works"] = any_works
    working_sorts = [v["sort_label"] for v in out["variants"]
                     if v.get("page2_hit_count", 0) > 0]
    out["verdict"] = (
        f"search_after WORKS with sort(s): {working_sorts}."
        if any_works
        else "search_after did NOT work with any sort variant tested."
    )
    write_json(run_dir / "part_3_4_findings.json", out)
    return out


# ---------------------------------------------------------------------------
# Part 3.5 — _source filtering granularity
# ---------------------------------------------------------------------------

def run_part_3_5_source_filtering(run_dir: Path, api_key: str,
                                  logger: logging.Logger) -> Dict[str, Any]:
    """Test response-payload-shaping options. If we can suppress fields, large
    batch operations get cheaper."""
    logger.info("Part 3.5: _source filtering granularity")
    url = URL_FOR_INDEX.format(index="api_publica_tjsp")
    base_q: Dict[str, Any] = {"query": {"match_all": {}}, "size": 1}

    tests: List[Tuple[str, Dict[str, Any]]] = [
        ("default", {**base_q}),
        ("source_false", {**base_q, "_source": False}),
        ("source_projection",
         {**base_q, "_source": ["numeroProcesso", "classe.codigo"]}),
        ("source_excludes_movimentos",
         {**base_q, "_source": {"excludes": ["movimentos"]}}),
        ("source_includes_wildcard",
         {**base_q, "_source": {"includes": ["classe.*"]}}),
    ]
    results: List[Dict[str, Any]] = []
    for label, payload in tests:
        res = _post(url, payload, api_key, logger=logger)
        write_json(run_dir / f"p35_{label}.json", res)
        hits = (res.get("body", {}) or {}).get("hits", {}).get("hits", []) \
               if res.get("body") else []
        src = hits[0].get("_source") if hits else None
        keys_returned = sorted(src.keys()) if isinstance(src, dict) else None
        results.append({
            "label": label,
            "status": res.get("status"),
            "hit_count": len(hits),
            "source_present": src is not None,
            "source_keys_returned": keys_returned,
            "source_type": type(src).__name__ if src is not None else None,
            "response_size_chars": len(json.dumps(res.get("body") or {})),
        })

    default_size = next((r["response_size_chars"] for r in results if r["label"] == "default"), None)
    out: Dict[str, Any] = {
        "tests": results,
        "default_response_size": default_size,
    }
    # Did source filtering actually reduce payload?
    smallest = min((r["response_size_chars"] for r in results if r["label"] != "default"),
                   default=default_size)
    out["smallest_response_size"] = smallest
    out["reduction_pct"] = (
        round(100 * (default_size - smallest) / default_size, 1)
        if default_size and smallest is not None and default_size > 0
        else None
    )
    out["verdict"] = (
        f"_source filtering WORKS — payload reduced by up to {out['reduction_pct']}%."
        if out["reduction_pct"] and out["reduction_pct"] > 0
        else "_source filtering did NOT reduce payload meaningfully."
    )
    write_json(run_dir / "part_3_5_findings.json", out)
    return out


# ---------------------------------------------------------------------------
# Name-search reconciliation (LegalSuite contradiction)
# ---------------------------------------------------------------------------

def _flatten_mapping(props: Dict[str, Any], prefix: str = "") -> List[Tuple[str, str]]:
    """Flatten an Elasticsearch mapping `properties` tree into (dotted_path,
    type) leaf tuples. Object/nested fields recurse; leaf fields emit type."""
    out: List[Tuple[str, str]] = []
    for name, spec in (props or {}).items():
        path = f"{prefix}.{name}" if prefix else name
        if not isinstance(spec, dict):
            continue
        if "properties" in spec:
            # object or nested container
            container_type = spec.get("type", "object")
            out.append((path, f"[{container_type}]"))
            out.extend(_flatten_mapping(spec["properties"], path))
        else:
            out.append((path, spec.get("type", "?")))
            # multi-fields (e.g. .keyword)
            for sub_name, sub_spec in (spec.get("fields") or {}).items():
                out.append((f"{path}.{sub_name}",
                            sub_spec.get("type", "?") + " (multi-field)"))
    return out


# LegalSuite's exact published example (see docs/DATAJUD_CAPABILITY_INVENTORY.md).
LEGALSUITE_VERBATIM = {
    "query": {"match": {"partes.nome": "Empresa Exemplo Ltda"}},
    "size": 10,
    "sort": [{"dataAjuizamento": {"order": "desc"}}],
}

# Candidate party field paths from the CNJ MNI / DataJud data model. We sweep
# these as `match` queries against a guaranteed-present common name to see if
# parties live under any alternate path.
PARTY_FIELD_CANDIDATES: Tuple[str, ...] = (
    "partes.nome",
    "partes.nomeParte",
    "partes.pessoa.nome",
    "partes.nomePessoa",
    "nomeParte",
    "nome",
    "poloAtivo.parte.pessoa.nome",
    "poloPassivo.parte.pessoa.nome",
    "dadosBasicos.polo.parte.pessoa.nome",
)

CANARY_COMMON_NAME = "Itaú Unibanco"


def run_reconciliation(test_cases: List[Dict[str, Any]],
                       run_dir: Path, api_key: str,
                       logger: logging.Logger) -> Dict[str, Any]:
    """Deep reconciliation of the name-search negative against LegalSuite's
    documented working example. The decisive new test is the index mapping:
    a `match` on an unmapped field returns 200/0-hits (no error), so the
    canary alone can't distinguish 'field absent from mapping' from 'field
    present but empty'. The mapping/_field_caps settles it."""
    findings: Dict[str, Any] = {}

    # --- Test A: index mapping — does `partes` exist in the schema at all? ---
    logger.info("Reconciliation A: pulling index _mapping for all 5 tribunals")
    mapping_per_tribunal: List[Dict[str, Any]] = []
    for idx in TRIBUNAL_INDICES:
        url = URL_BASE.format(index=idx) + "/_mapping"
        res = _http_get(url, api_key, logger=logger)
        write_json(run_dir / f"recon_mapping__{idx}.json", res)
        body = res.get("body") or {}
        # body shape: {index_name: {mappings: {properties: {...}}}}
        flat: List[Tuple[str, str]] = []
        if isinstance(body, dict):
            for _index_name, index_body in body.items():
                props = (((index_body or {}).get("mappings") or {})
                         .get("properties") or {})
                flat = _flatten_mapping(props)
                break
        all_paths = [p for p, _t in flat]
        party_paths = [f"{p} ({t})" for p, t in flat
                       if "parte" in p.lower() or "pessoa" in p.lower()
                       or "nome" in p.lower() or "polo" in p.lower()
                       or "documento" in p.lower() or "cpf" in p.lower()
                       or "cnpj" in p.lower()]
        mapping_per_tribunal.append({
            "index": idx,
            "status": res.get("status"),
            "total_fields_in_mapping": len(all_paths),
            "all_field_paths": all_paths,
            "party_related_paths": party_paths,
            "partes_present_in_mapping": any(p == "partes" or p.startswith("partes.")
                                             for p in all_paths),
        })
    findings["test_A_mapping"] = {
        "per_tribunal": mapping_per_tribunal,
        "any_tribunal_has_partes_in_mapping": any(
            t["partes_present_in_mapping"] for t in mapping_per_tribunal
        ),
    }

    # --- Test B: _field_caps with CONTROL (the decisive test) ---
    # `_mapping` is 403 for the dpj_api_publica key, but `_field_caps` is 200.
    # Request KNOWN-GOOD fields alongside party fields: if the known-good fields
    # come back and party fields don't, that's airtight proof party fields are
    # absent from the mapping (and rules out "_field_caps just doesn't work for
    # our key"). Run per-tribunal so we can localize.
    logger.info("Reconciliation B: _field_caps with control fields (5 tribunals)")
    control_fields = ["numeroProcesso", "movimentos.codigo", "classe.codigo",
                      "dataAjuizamento", "grau"]
    party_fields = ["partes", "partes.*", "partes.nome", "partes.nomeParte",
                    "nomeParte", "nome", "cpfCnpj"]
    requested = ",".join(control_fields + party_fields)
    fc_per_tribunal: List[Dict[str, Any]] = []
    for idx in TRIBUNAL_INDICES:
        fc_url = URL_BASE.format(index=idx) + f"/_field_caps?fields={requested}"
        fc_res = _http_get(fc_url, api_key, logger=logger)
        write_json(run_dir / f"recon_field_caps__{idx}.json", fc_res)
        fc_body = fc_res.get("body") or {}
        returned = sorted((fc_body.get("fields") or {}).keys()) \
            if isinstance(fc_body, dict) else []
        control_present = [f for f in control_fields if f in returned]
        party_present = [f for f in returned
                         if f.startswith("partes") or f in ("nomeParte", "nome", "cpfCnpj")]
        fc_per_tribunal.append({
            "index": idx,
            "status": fc_res.get("status"),
            "fields_returned": returned,
            "control_fields_present": control_present,
            "party_fields_present": party_present,
            "endpoint_works_for_key": len(control_present) > 0,
        })
    findings["test_B_field_caps"] = {
        "control_fields_requested": control_fields,
        "party_fields_requested": party_fields,
        "per_tribunal": fc_per_tribunal,
        "field_caps_works": any(t["endpoint_works_for_key"] for t in fc_per_tribunal),
        "any_party_field_in_caps": any(t["party_fields_present"] for t in fc_per_tribunal),
    }

    # --- Test C: LegalSuite verbatim query ---
    logger.info("Reconciliation C: LegalSuite verbatim query")
    tjsp_url = URL_FOR_INDEX.format(index="api_publica_tjsp")
    ls_res = _post(tjsp_url, LEGALSUITE_VERBATIM, api_key, logger=logger)
    write_json(run_dir / "recon_legalsuite_verbatim.json", ls_res)
    findings["test_C_legalsuite_verbatim"] = {
        "query": LEGALSUITE_VERBATIM,
        "status": ls_res.get("status"),
        "total_hits": _hit_count(ls_res.get("body")),
        "error_excerpt": (ls_res.get("raw") or "")[:300] if ls_res.get("raw") else None,
        "interpretation": (
            "200 + 0 hits is EXPECTED for the placeholder name 'Empresa "
            "Exemplo Ltda' regardless of whether the field is mapped — a match "
            "on an unmapped field also returns 200/0. Diagnostic value is in "
            "the status (mapping error vs clean 200), cross-referenced with "
            "test A."
        ),
    }

    # --- Test D: alternative party field-name sweep (canary common name) ---
    logger.info("Reconciliation D: party-field-name sweep on canary '%s'",
                CANARY_COMMON_NAME)
    sweep: List[Dict[str, Any]] = []
    for field in PARTY_FIELD_CANDIDATES:
        payload = {"query": {"match": {field: CANARY_COMMON_NAME}}, "size": 1}
        res = _post(tjsp_url, payload, api_key, logger=logger)
        safe_field = field.replace(".", "_")
        write_json(run_dir / f"recon_sweep__{safe_field}.json", res)
        sweep.append({
            "field": field,
            "status": res.get("status"),
            "total_hits": _hit_count(res.get("body")),
            "error_excerpt": (res.get("raw") or "")[:200] if res.get("raw") else None,
        })
    findings["test_D_field_sweep"] = {
        "canary_name": CANARY_COMMON_NAME,
        "results": sweep,
        "any_field_returned_hits": any(s["total_hits"] > 0 for s in sweep),
    }

    # --- Test E: real defendants (PJ + PF) full variant matrix ---
    logger.info("Reconciliation E: real-defendant matrix (PJ + PF)")
    # Pick one PJ (Ltda/S.A.) and one PF defendant from test cases.
    pj = next((c for c in test_cases
               if any(t in (c["defendant"] or "").lower()
                      for t in ("ltda", "s.a", "s/a", "eireli", "me"))), None)
    pf = next((c for c in test_cases
               if c is not pj and c.get("defendant")
               and not any(t in (c["defendant"] or "").lower()
                           for t in ("ltda", "s.a", "s/a", "eireli"))), None)
    chosen = [c for c in (pj, pf) if c]
    matrix: List[Dict[str, Any]] = []
    for case in chosen:
        name = case["defendant"]
        per_variant: List[Dict[str, Any]] = []
        for vname, payload in _query_variants_for_name(name):
            # skip nested variants — test A already shows partes isn't nested
            if "nested" in vname:
                continue
            res = _post(tjsp_url, payload, api_key, logger=logger)
            safe = re.sub(r"[^A-Za-z0-9]", "_", name)[:30]
            write_json(run_dir / f"recon_matrix__{safe}__{vname}.json", res)
            per_variant.append({
                "variant": vname,
                "status": res.get("status"),
                "total_hits": _hit_count(res.get("body")),
            })
        matrix.append({
            "process_number": case["process_number"],
            "defendant": name,
            "kind": "PJ" if case is pj else "PF",
            "variants": per_variant,
            "any_hit": any(v["total_hits"] > 0 for v in per_variant),
        })
    findings["test_E_real_defendant_matrix"] = {
        "cases": matrix,
        "any_case_returned_hits": any(m["any_hit"] for m in matrix),
    }

    # --- Verdict ---
    # Evidence hierarchy (strongest first):
    #   1. _field_caps WITH CONTROL — if control fields come back but party
    #      fields don't, that's airtight proof party fields aren't in the
    #      mapping (and that _field_caps works for our key). DECISIVE.
    #   2. Any party-field query returning hits on a guaranteed-present name → N2.
    #   3. _mapping is INCONCLUSIVE here: 403 (no view_index_metadata priv),
    #      so it can NOT be used as proof of absence. Recorded, not weighted.
    fc = findings["test_B_field_caps"]
    field_caps_works = fc["field_caps_works"]
    party_in_caps = fc["any_party_field_in_caps"]
    any_party_field_hits = findings["test_D_field_sweep"]["any_field_returned_hits"]
    any_real_hits = findings["test_E_real_defendant_matrix"]["any_case_returned_hits"]
    mapping_all_403 = all(t["status"] == 403
                          for t in findings["test_A_mapping"]["per_tribunal"])

    if any_party_field_hits or any_real_hits:
        verdict = "N2"
        verdict_text = (
            "N2 — NAME SEARCH WORKS after corrected test. A party field "
            "returned hits on a guaranteed-present name. The prior negative was "
            "a test artifact. Document the working query; reopens Layer 3 proper."
        )
    elif field_caps_works and not party_in_caps:
        verdict = "N1"
        verdict_text = (
            "N1 — NEGATIVE IS REAL AND RECONCILED (decisive evidence: "
            "_field_caps with control). _field_caps returned the control fields "
            "(numeroProcesso, movimentos.codigo, etc.) but ZERO party fields "
            "across all tribunals — proving (a) the endpoint works for our key "
            "and (b) no party field (partes / partes.nome / nomeParte / nome / "
            "cpfCnpj) exists in the mapping. Corroborated by: 9-path field-name "
            "sweep all 0-hits on a guaranteed-present common name; real PJ+PF "
            "defendant matrix all 0; LegalSuite verbatim 200/0. The public index "
            "(user dpj_api_publica) simply does not index parties. "
            "LegalSuite's documented example uses the SAME public endpoint + "
            "APIKey scheme (per TRACK_B) yet cannot work as-is — reconciliation "
            "is temporal (party search removed post-LGPD) and/or a privileged "
            "key tier. See web-research section for which."
        )
    elif field_caps_works and party_in_caps:
        verdict = "N1-ambiguous"
        verdict_text = (
            "N1-AMBIGUOUS — _field_caps reports a party field EXISTS in the "
            "mapping, yet no query returned hits even on a guaranteed-present "
            "common name. Field mapped but unpopulated / non-analyzed. Escalate: "
            "odd state warranting direct CNJ inquiry."
        )
    else:
        verdict = "INCONCLUSIVE"
        verdict_text = (
            "INCONCLUSIVE — _field_caps did not return even the control fields "
            f"(field_caps_works={field_caps_works}), and _mapping is 403 "
            f"(all_403={mapping_all_403}). Cannot prove field absence via "
            "metadata. Falling back to query evidence: all party-field queries "
            "returned 0 hits, which is consistent with N1 but not airtight. "
            "Escalate for a privileged key or direct CNJ schema confirmation."
        )

    findings["verdict"] = verdict
    findings["verdict_text"] = verdict_text
    findings["mapping_note"] = (
        "GET /_mapping returns 403 for user dpj_api_publica (lacks "
        "view_index_metadata privilege). Mapping is NOT readable on this key; "
        "absence-of-data from /_mapping is permission-denial, not proof. "
        "_field_caps (200) is the usable metadata channel."
    )
    findings["legalsuite_endpoint_per_track_b"] = (
        "https://api-publica.datajud.cnj.jus.br/api_publica_tjsp/_search "
        "(SAME public endpoint, SAME APIKey auth scheme as ours)"
    )
    logger.info("Reconciliation verdict: %s", verdict_text)
    write_json(run_dir / "reconciliation_findings.json", findings)
    return findings


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

DEFAULT_DB_PATH = SNAPSHOT_DIR / "esaj_snapshots.db"

ALL_PARTS = ("1.1", "1.2", "1.3", "2", "3.1", "3.2", "3.3", "3.4", "3.5")
# `recon` is the standalone name-search/LegalSuite reconciliation pass; not in
# ALL_PARTS (run explicitly via --parts recon).


def run(run_dir: Path, logger: logging.Logger, *,
        db_path: Path = DEFAULT_DB_PATH,
        parts: Optional[List[str]] = None) -> Dict[str, Any]:
    """Run selected parts. `parts=None` runs everything in canonical order."""
    logger.info("DataJud inventory probe — run dir: %s", run_dir)
    api_key = _resolve_api_key(logger)

    if parts is None:
        parts = list(ALL_PARTS)

    test_cases = load_snapshot_test_cases(db_path)
    logger.info("Loaded %d test cases from snapshot store", len(test_cases))
    write_json(run_dir / "test_cases.json", test_cases)

    findings: Dict[str, Any] = {"parts": {}}

    if "recon" in parts:
        logger.info("=== Reconciliation — name search / LegalSuite ===")
        findings["parts"]["recon"] = run_reconciliation(
            test_cases, run_dir, api_key, logger,
        )
        logger.info("Reconciliation verdict: %s",
                    findings["parts"]["recon"]["verdict"])

    if "1.1" in parts:
        logger.info("=== Part 1.1 — name search re-verification ===")
        findings["parts"]["1.1"] = run_part_1_1_name_search(
            test_cases, run_dir, api_key, logger,
        )
        logger.info("Part 1.1 verdict: %s",
                    findings["parts"]["1.1"]["summary"]["verdict"])

    if "1.2" in parts:
        logger.info("=== Part 1.2 — CPF/CNPJ search ===")
        findings["parts"]["1.2"] = run_part_1_2_cpf_cnpj(run_dir, api_key, logger)
        logger.info("Part 1.2 verdict: %s", findings["parts"]["1.2"]["verdict"])

    if "1.3" in parts:
        logger.info("=== Part 1.3 — TPU code uniformity ===")
        findings["parts"]["1.3"] = run_part_1_3_tpu_uniformity(run_dir, api_key, logger)
        logger.info("Part 1.3 verdict: %s", findings["parts"]["1.3"]["verdict"])

    if "2" in parts:
        logger.info("=== Part 2 — field inventory ===")
        findings["parts"]["2"] = run_part_2_field_inventory(run_dir, api_key, logger)
        logger.info("Part 2 complementosTabelados signal: %s",
                    findings["parts"]["2"]["complementos_tabelados_signal"])

    if "3.1" in parts:
        logger.info("=== Part 3.1 — rate limits ===")
        findings["parts"]["3.1"] = run_part_3_1_rate_limits(run_dir, api_key, logger)
        logger.info("Part 3.1 verdict: %s", findings["parts"]["3.1"]["verdict"])

    if "3.2" in parts:
        logger.info("=== Part 3.2 — aggregations ===")
        findings["parts"]["3.2"] = run_part_3_2_aggregations(run_dir, api_key, logger)
        logger.info("Part 3.2 verdict: %s", findings["parts"]["3.2"]["verdict"])

    if "3.3" in parts:
        logger.info("=== Part 3.3 — bulk patterns ===")
        findings["parts"]["3.3"] = run_part_3_3_bulk_patterns(run_dir, api_key, logger)
        logger.info("Part 3.3 any-bulk-works: %s",
                    findings["parts"]["3.3"]["any_bulk_pattern_works"])

    if "3.4" in parts:
        logger.info("=== Part 3.4 — search_after ===")
        findings["parts"]["3.4"] = run_part_3_4_search_after(run_dir, api_key, logger)
        logger.info("Part 3.4 verdict: %s", findings["parts"]["3.4"]["verdict"])

    if "3.5" in parts:
        logger.info("=== Part 3.5 — _source filtering ===")
        findings["parts"]["3.5"] = run_part_3_5_source_filtering(run_dir, api_key, logger)
        logger.info("Part 3.5 verdict: %s", findings["parts"]["3.5"]["verdict"])

    write_json(run_dir / "all_findings.json", findings)
    return {"ok": True, "findings": findings}
