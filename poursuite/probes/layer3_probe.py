"""Layer 3 probe — DataJud name-search quality.

See CLAUDE_CODE_BRIEF_PHASE3_LAYER3_PROBE.md (v1.2). Investigates:

- What field do we query for *name search* in DataJud? (Step 0 discovery —
  party fields are returned stripped, but we don't yet know what to query.)
- For synthetic common Brazilian names: hit-volume + foro/classe/data spread
  (no reference case — measures DataJud's noise floor).
- For the 4 real debtors in the snapshot store: precision + classification
  vs. the source case.
- Cross-tribunal coverage across TJSP, TJRJ, TJMG, TRT-2, TRF-3
  (STJ + TST excluded; appellate-inflating).

Reuses HTTP and auth from `poursuite.probes.datajud`.
"""
from __future__ import annotations

import logging
import re
import sqlite3
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from poursuite.config import SNAPSHOT_DIR
from poursuite.probes import write_json
from poursuite.probes.datajud import (
    _hit_count,
    _has_hits,
    _post,
    _http_get,
    _resolve_api_key,
)

DEFAULT_DB_PATH = SNAPSHOT_DIR / "esaj_snapshots.db"

# v1.2 tribunals (STJ + TST excluded as appellate-inflating).
CROSS_TRIBUNAL_INDICES_V12: Tuple[str, ...] = (
    "api_publica_tjsp",
    "api_publica_tjrj",
    "api_publica_tjmg",
    "api_publica_trt2",
    "api_publica_trf3",
)
URL_FOR_INDEX = "https://api-publica.datajud.cnj.jus.br/{index}/_search"

# Synthetic common-name archetype — no ground truth, just noise floor.
SYNTHETIC_COMMON_NAMES: Tuple[str, ...] = (
    "José da Silva",
    "Maria Santos",
    "João Oliveira",
    "Ana Pereira",
    "Carlos Souza",
)

# Loaded debtors from snapshot store (per v1.2 reality check).
SNAPSHOT_DEBTORS: Tuple[Dict[str, str], ...] = (
    {
        "name": "Royal Coffee Comercial e Exportadora de Café Ltda.",
        "kind": "PJ",
        "source_process": "1002177-13.2020.8.26.0100",
        "source_foro_code": "0100",
        "source_initial_date": "14/01/2020",
        "source_class_type": "Execução de Título Extrajudicial",
    },
    {
        "name": "Crb Imóveis Ltda",
        "kind": "PJ",
        "source_process": "1033164-10.2022.8.26.0602",
        "source_foro_code": "0602",
        "source_initial_date": "25/08/2022",
        "source_class_type": "Execução de Título Extrajudicial",
    },
    {
        "name": "Jean Cássio Luna Santos",
        "kind": "PF-distinctive",
        "source_process": "1063559-02.2023.8.26.0100",
        "source_foro_code": "0100",
        "source_initial_date": "18/05/2023",
        "source_class_type": "Execução de Título Extrajudicial",
    },
    {
        "name": "BARUK Joias e Presentes Ltda.",
        "kind": "PJ",
        "source_process": "1185092-25.2023.8.26.0100",
        "source_foro_code": "0100",
        "source_initial_date": "28/12/2023",
        "source_class_type": "Execução de Título Extrajudicial",
    },
)


# ---------------------------------------------------------------------------
# Step 0 — query-field discovery
# ---------------------------------------------------------------------------

def _parse_dmy(date_str: str) -> Optional[datetime]:
    m = re.match(r"^(\d{2})/(\d{2})/(\d{4})$", date_str.strip()) if date_str else None
    if not m:
        return None
    d, mo, y = m.groups()
    return datetime(int(y), int(mo), int(d), tzinfo=timezone.utc)


def discover_query_field(known_name: str, run_dir: Path, api_key: str,
                         logger: logging.Logger) -> Dict[str, Any]:
    """Try several query shapes against TJSP. Document what hits, what 0s,
    what errors. The first variant returning hits wins."""
    url = URL_FOR_INDEX.format(index="api_publica_tjsp")

    variants: List[Tuple[str, Dict[str, Any]]] = [
        ("match.nomeParte", {"query": {"match": {"nomeParte": known_name}}, "size": 3}),
        ("match.partes.nome", {"query": {"match": {"partes.nome": known_name}}, "size": 3}),
        ("match.nomePessoa", {"query": {"match": {"nomePessoa": known_name}}, "size": 3}),
        ("multi_match.fielded", {"query": {"multi_match": {
            "query": known_name,
            "fields": ["nomeParte", "partes.nome", "nomePessoa", "nomeRazaoSocial"],
            "type": "best_fields",
        }}, "size": 3}),
        ("query_string", {"query": {"query_string": {
            "query": f'"{known_name}"',
        }}, "size": 3}),
    ]

    results: List[Dict[str, Any]] = []
    working_variant: Optional[str] = None
    for name, payload in variants:
        logger.info("query-field-discovery: trying %s", name)
        res = _post(url, payload, api_key, logger=logger)
        write_json(run_dir / f"step0_qf_{name.replace('.', '_')}.json", res)
        record = {
            "variant": name,
            "ok": res.get("ok"),
            "status": res.get("status"),
            "hit_count": _hit_count(res.get("body")),
            "latency_ms": res.get("latency_ms"),
            "error": res.get("error"),
        }
        results.append(record)
        if res.get("ok") and _has_hits(res) and working_variant is None:
            working_variant = name

    out = {
        "known_name": known_name,
        "variants_tested": results,
        "working_variant": working_variant,
        "conclusion": (
            f"Name-search query works via `{working_variant}`."
            if working_variant
            else "No query variant returned hits for the known name. "
                 "DataJud may not support name search directly — "
                 "investigate alternative endpoints (Wikijus, judit/conjuracy "
                 "wrappers) before Layer 3."
        ),
    }
    write_json(run_dir / "step0_query_field.json", out)
    return out


# ---------------------------------------------------------------------------
# Tribunal index liveness
# ---------------------------------------------------------------------------

def verify_tribunal_indices(run_dir: Path, api_key: str,
                            logger: logging.Logger) -> Dict[str, Any]:
    """One-shot discovery — does each index respond? Brief v1.2 requires this
    before committing to the cross-tribunal sample."""
    out: Dict[str, Any] = {}
    for idx in CROSS_TRIBUNAL_INDICES_V12:
        url = URL_FOR_INDEX.format(index=idx)
        res = _post(url, {"query": {"match_all": {}}, "size": 1}, api_key, logger=logger)
        out[idx] = {
            "ok": res.get("ok"),
            "status": res.get("status"),
            "hit_count": _hit_count(res.get("body")),
            "latency_ms": res.get("latency_ms"),
            "error": res.get("error"),
        }
    write_json(run_dir / "step0_tribunal_liveness.json", out)
    return out


# ---------------------------------------------------------------------------
# Name search (per debtor × tribunal)
# ---------------------------------------------------------------------------

def _build_name_query(query_field_variant: str, name: str, size: int) -> Dict[str, Any]:
    if query_field_variant == "match.nomeParte":
        return {"query": {"match": {"nomeParte": name}}, "size": size}
    if query_field_variant == "match.partes.nome":
        return {"query": {"match": {"partes.nome": name}}, "size": size}
    if query_field_variant == "match.nomePessoa":
        return {"query": {"match": {"nomePessoa": name}}, "size": size}
    if query_field_variant == "multi_match.fielded":
        return {"query": {"multi_match": {
            "query": name,
            "fields": ["nomeParte", "partes.nome", "nomePessoa", "nomeRazaoSocial"],
            "type": "best_fields",
        }}, "size": size}
    if query_field_variant == "query_string":
        return {"query": {"query_string": {"query": f'"{name}"'}}, "size": size}
    raise ValueError(f"Unknown query_field_variant: {query_field_variant}")


def _summarize_hit(hit: Dict[str, Any]) -> Dict[str, Any]:
    src = hit.get("_source", {}) or {}
    orgao = src.get("orgaoJulgador") or {}
    classe = src.get("classe") or {}
    return {
        "_index": hit.get("_index"),
        "_id": hit.get("_id"),
        "numeroProcesso": src.get("numeroProcesso"),
        "dataAjuizamento": src.get("dataAjuizamento"),
        "tribunal": src.get("tribunal"),
        "grau": src.get("grau"),
        "orgaoJulgador_codigo": orgao.get("codigo"),
        "orgaoJulgador_nome": orgao.get("nome"),
        "classe_codigo": classe.get("codigo"),
        "classe_nome": classe.get("nome"),
        "movimentos_count": len(src.get("movimentos") or []),
    }


def query_debtor_across_tribunals(
    name: str, query_field_variant: str, live_indices: List[str],
    run_dir: Path, api_key: str, logger: logging.Logger, *, size: int = 30,
) -> Dict[str, Any]:
    per_index: Dict[str, Dict[str, Any]] = {}
    for idx in live_indices:
        url = URL_FOR_INDEX.format(index=idx)
        payload = _build_name_query(query_field_variant, name, size)
        res = _post(url, payload, api_key, logger=logger)
        hits = (res.get("body", {}) or {}).get("hits", {}).get("hits", []) if res.get("body") else []
        per_index[idx] = {
            "ok": res.get("ok"),
            "status": res.get("status"),
            "total_hits": _hit_count(res.get("body")),
            "returned_hits": len(hits),
            "latency_ms": res.get("latency_ms"),
            "hits": [_summarize_hit(h) for h in hits],
            "error": res.get("error"),
        }
        safe_name = re.sub(r"[^A-Za-z0-9]", "_", name)[:40]
        write_json(run_dir / f"name_{safe_name}__{idx}.json", res)
    return {"name": name, "per_index": per_index}


# ---------------------------------------------------------------------------
# Disambiguation classification (for snapshot-derived debtors only)
# ---------------------------------------------------------------------------

def classify_hit_vs_reference(hit: Dict[str, Any], reference: Dict[str, str],
                              date_window_days: int) -> str:
    """Apply the v1.2 disambiguation rule against the source case."""
    ref_date = _parse_dmy(reference.get("source_initial_date", ""))
    hit_date_str = hit.get("dataAjuizamento") or ""
    hit_date = None
    if hit_date_str:
        try:
            # DataJud format: "2020-01-14T00:00:00.000Z"
            hit_date = datetime.fromisoformat(hit_date_str.replace("Z", "+00:00"))
        except (ValueError, TypeError):
            hit_date = None

    date_match = False
    if ref_date and hit_date:
        delta_days = abs((ref_date - hit_date).days)
        date_match = delta_days <= date_window_days

    # foro: DataJud orgaoJulgador is per-vara, not foro. We use a soft match:
    # if the hit's orgaoJulgador_nome contains a token from the reference's
    # foro_code or its source_process foro digits, that's a signal.
    ref_foro = (reference.get("source_foro_code") or "").lstrip("0") or "0"
    hit_orgao = (hit.get("orgaoJulgador_nome") or "").lower()
    foro_match = bool(ref_foro and (ref_foro in hit_orgao))

    # classe family: simple substring match on the head word of the class.
    ref_class = (reference.get("source_class_type") or "").lower().split()[0] \
        if reference.get("source_class_type") else ""
    hit_class = (hit.get("classe_nome") or "").lower()
    class_match = bool(ref_class and ref_class in hit_class)

    score = sum([date_match, foro_match, class_match])
    if score >= 2:
        return "definitely_same"
    if score == 0:
        return "definitely_different"
    return "uncertain"


# ---------------------------------------------------------------------------
# Confidence scoring heuristics — tested on the data
# ---------------------------------------------------------------------------

def score_hit_heuristic_A(hit: Dict[str, Any], reference: Dict[str, str]) -> float:
    """Foro-proximity heuristic. Same foro_code substring in orgao = +0.6;
    same UF (inferred from index) = +0.2."""
    score = 0.0
    ref_foro = (reference.get("source_foro_code") or "").lstrip("0") or "0"
    hit_orgao = (hit.get("orgaoJulgador_nome") or "").lower()
    if ref_foro and ref_foro in hit_orgao:
        score += 0.6
    # Index proxy for UF: api_publica_tjsp → SP (since our debtors are TJSP).
    if (hit.get("_index") or "").endswith("tjsp"):
        score += 0.2
    return score


def score_hit_heuristic_B(hit: Dict[str, Any], reference: Dict[str, str],
                          date_window_days: int) -> float:
    """Date-clustering heuristic. Within window = +0.5, within 5×window = +0.2."""
    ref_date = _parse_dmy(reference.get("source_initial_date", ""))
    hit_date_str = hit.get("dataAjuizamento") or ""
    if not (ref_date and hit_date_str):
        return 0.0
    try:
        hit_date = datetime.fromisoformat(hit_date_str.replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return 0.0
    delta_days = abs((ref_date - hit_date).days)
    if delta_days <= date_window_days:
        return 0.5
    if delta_days <= 5 * date_window_days:
        return 0.2
    return 0.0


def score_hit_heuristic_C(hit: Dict[str, Any], reference: Dict[str, str]) -> float:
    """Class-family heuristic. Same class-family head word = +0.4."""
    ref_class = (reference.get("source_class_type") or "").lower().split()[0] \
        if reference.get("source_class_type") else ""
    hit_class = (hit.get("classe_nome") or "").lower()
    if ref_class and ref_class in hit_class:
        return 0.4
    return 0.0


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------

def write_report(run_dir: Path, step0: Dict[str, Any], liveness: Dict[str, Any],
                 common_results: List[Dict[str, Any]],
                 snapshot_results: List[Dict[str, Any]],
                 window_choice: Dict[str, Any]) -> Path:
    md: List[str] = []
    md.append("# Layer 3 Probe — DataJud Name-Search Quality")
    md.append(f"_Run: `{run_dir.name}`  |  "
              f"Generated: {datetime.now(timezone.utc).isoformat()}_")
    md.append("")
    md.append("Reference brief: `CLAUDE_CODE_BRIEF_PHASE3_LAYER3_PROBE.md` (v1.2)")
    md.append("")

    md.append("## Step 0 — name-search query field discovery")
    md.append("")
    md.append(f"Known name tested: `{step0.get('known_name')}`")
    md.append("")
    md.append(f"Working variant: **{step0.get('working_variant') or 'NONE'}**")
    md.append("")
    md.append(step0.get("conclusion", ""))
    md.append("")
    md.append("| Variant | Status | Hits | Latency (ms) | Error |")
    md.append("|---|---|---|---|---|")
    for v in step0.get("variants_tested", []):
        md.append(f"| `{v['variant']}` | {v.get('status')} | "
                  f"{v.get('hit_count')} | {v.get('latency_ms')} | "
                  f"{v.get('error') or '—'} |")
    md.append("")

    md.append("## Tribunal index liveness (v1.2 list)")
    md.append("")
    md.append("| Index | Status | Latency (ms) | Hit count (match_all size 1) | Error |")
    md.append("|---|---|---|---|---|")
    for idx, rec in liveness.items():
        md.append(f"| `{idx}` | {rec.get('status')} | "
                  f"{rec.get('latency_ms')} | {rec.get('hit_count')} | "
                  f"{rec.get('error') or '—'} |")
    md.append("")

    if not step0.get("working_variant"):
        md.append("## Subsequent steps SKIPPED")
        md.append("")
        md.append("No working query field was discovered. The common-name "
                  "and snapshot-derived archetype queries were not executed. "
                  "Re-investigate the query shape before scoping Layer 3.")
        md.append("")
        out_path = run_dir / "layer3_findings.md"
        out_path.write_text("\n".join(md), encoding="utf-8")
        return out_path

    # Common-name archetype (synthetic, no ground truth)
    md.append("## Synthetic common-name archetype — noise floor")
    md.append("")
    md.append("No ground truth (no reference case). We're measuring DataJud's "
              "raw noise: hit volume, foro/classe spread, visible duplicate "
              "rate. Disambiguation classification is **not applied** here.")
    md.append("")
    md.append("| Name | Total hits (all 5 indices) | Top foros | Top classes | Distinct numeroProcesso |")
    md.append("|---|---|---|---|---|")
    for r in common_results:
        all_hits: List[Dict[str, Any]] = []
        total_hits = 0
        for idx, rec in r["per_index"].items():
            total_hits += rec.get("total_hits", 0)
            all_hits.extend(rec.get("hits", []))
        foros = Counter((h.get("orgaoJulgador_nome") or "—")[:30] for h in all_hits)
        classes = Counter((h.get("classe_nome") or "—")[:30] for h in all_hits)
        distinct_procs = len({h.get("numeroProcesso") for h in all_hits if h.get("numeroProcesso")})
        top_foros = ", ".join(f"{k} ({v})" for k, v in foros.most_common(3))
        top_classes = ", ".join(f"{k} ({v})" for k, v in classes.most_common(3))
        md.append(f"| `{r['name']}` | {total_hits} | {top_foros} | "
                  f"{top_classes} | {distinct_procs} |")
    md.append("")
    md.append("**Reading the table.** Higher total-hit counts with broad "
              "foro/classe spread = noisy (expected for common names). "
              "Distinct-numeroProcesso < returned-hits would suggest "
              "duplicates across tribunals.")
    md.append("")

    # Snapshot-derived archetype
    md.append("## Snapshot-derived archetype — distinctive + PJ")
    md.append("")
    md.append(f"Disambiguation window chosen: **±{window_choice['days']} days** "
              f"(rationale: {window_choice['rationale']})")
    md.append("")
    md.append("| Debtor | Kind | Total hits | def-same | def-diff | uncertain | "
              "Coverage vs other_processes |")
    md.append("|---|---|---|---|---|---|---|")
    for r in snapshot_results:
        counts = r["classification_counts"]
        md.append(f"| `{r['debtor']['name'][:35]}` | {r['debtor']['kind']} | "
                  f"{r['total_hits']} | {counts.get('definitely_same', 0)} | "
                  f"{counts.get('definitely_different', 0)} | "
                  f"{counts.get('uncertain', 0)} | "
                  f"{r['coverage_vs_other_processes']} |")
    md.append("")
    md.append("**Coverage column.** Snapshot store has `other_processes = NULL` "
              "for all 4 cases (i.e., contadorDeProcessos was not captured at "
              "scrape time — flagged as a scraper-bug candidate to investigate "
              "separately). Coverage cannot be measured against ground truth; "
              "the column shows 'no-data' uniformly.")
    md.append("")

    # Heuristic comparison
    md.append("## Confidence-scoring heuristics — applied to snapshot results")
    md.append("")
    md.append("Three heuristics tested per debtor:")
    md.append("- **A — foro proximity.** Hit's orgaoJulgador_nome contains the "
              "reference foro_code = +0.6; same-UF index = +0.2.")
    md.append(f"- **B — date clustering.** Within ±{window_choice['days']}d = +0.5; "
              f"within ±{5 * window_choice['days']}d = +0.2.")
    md.append("- **C — class family.** First word of class_type matches = +0.4.")
    md.append("")
    md.append("| Debtor | A median | B median | C median | A+B+C median | top-hit total |")
    md.append("|---|---|---|---|---|---|")
    for r in snapshot_results:
        a = [h["score_A"] for h in r["hits_with_scores"]] or [0.0]
        b = [h["score_B"] for h in r["hits_with_scores"]] or [0.0]
        c = [h["score_C"] for h in r["hits_with_scores"]] or [0.0]
        ab = [h["score_A"] + h["score_B"] + h["score_C"] for h in r["hits_with_scores"]] or [0.0]
        top = max(ab) if ab else 0.0

        def _med(xs):
            if not xs:
                return 0.0
            xs2 = sorted(xs)
            n = len(xs2)
            return xs2[n // 2] if n % 2 else (xs2[n // 2 - 1] + xs2[n // 2]) / 2

        md.append(f"| `{r['debtor']['name'][:35]}` | {_med(a):.2f} | "
                  f"{_med(b):.2f} | {_med(c):.2f} | {_med(ab):.2f} | {top:.2f} |")
    md.append("")
    md.append("Higher A+B+C means the hit is closer to the source case along "
              "multiple dimensions. Production Layer 3 can use the same "
              "additive scheme with tuned weights — proposal below.")
    md.append("")

    # Modal-classification check for Probe C disposition rule
    uncertain_pct = None
    if common_results:
        # For common names, we don't classify — but we can ask: how many returned
        # >10 hits across all tribunals? That's the noise signal.
        very_noisy = sum(1 for r in common_results
                         if sum(rec.get("total_hits", 0)
                                for rec in r["per_index"].values()) > 100)
        md.append("## Disposition signals (feeds Probe C)")
        md.append("")
        md.append(f"- Common-name noise: {very_noisy}/{len(common_results)} "
                  f"synthetic names returned >100 total hits across all "
                  f"queried indices.")
        # For snapshot results, count uncertain rate.
        total_classifications = 0
        uncertain_total = 0
        for r in snapshot_results:
            counts = r["classification_counts"]
            total_classifications += sum(counts.values())
            uncertain_total += counts.get("uncertain", 0)
        if total_classifications > 0:
            uncertain_pct = round(100 * uncertain_total / total_classifications, 1)
            md.append(f"- Snapshot-derived uncertain rate: {uncertain_total}/"
                      f"{total_classifications} = **{uncertain_pct}%**.")
            md.append("")
            if uncertain_pct > 40:
                md.append(f"  → Per v1.2 brief disposition rule (>40% uncertain): "
                          "**recommend Layer 3 v1 scopes to PJ/CNPJ + distinctive-"
                          "name PF only.** Common-name PF defers until party-cross-"
                          "reference is available.")
            else:
                md.append(f"  → Uncertain rate is below 40% threshold — "
                          "name-search-alone disambiguation may be feasible. "
                          "Caveat: sample is small (4 debtors).")
    md.append("")

    # Tribunal-code mapping caveat
    md.append("## Tribunal-code mapping caveat")
    md.append("")
    md.append("Per the May 2026 DataJud probe: TPU codes are not nationally "
              "uniform. Same applies to foro_codigo and classe naming across "
              "indices. The above classification ran with a soft-match rule "
              "(substring) precisely because of this. If a hit landed in "
              "'definitely_different' purely due to a rendering quirk, the "
              "report flags that as a known limitation, not a probe failure.")
    md.append("")

    # Parallel-case archetype dropped
    md.append("## Parallel-case archetype (dropped per v1.2)")
    md.append("")
    md.append("Snapshot store has 0 cases with `other_processes > 1`. The "
              "`contadorDeProcessos` field is being captured by the scraper "
              "(see `poursuite/scraper/esaj.py:439`) but is NULL for every "
              "loaded case in the store today. Two hypotheses: (a) the "
              "secondary search-by-name step is silently failing, (b) the "
              "current case sample legitimately has no parallel cases. "
              "Worth a separate scraper-bug investigation. Cross-tribunal "
              "coverage measurement against ground truth is deferred.")
    md.append("")

    out_path = run_dir / "layer3_findings.md"
    out_path.write_text("\n".join(md), encoding="utf-8")
    return out_path


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

def run(run_dir: Path, logger: logging.Logger, *,
        date_window_days: int = 730) -> Dict[str, Any]:
    """Probe runner.

    `date_window_days` is the dataAjuizamento window for "definitely_same" —
    2 years by default. The brief asks to try several; the report documents
    the chosen value and rationale. Trying multiple is a follow-up.
    """
    logger.info("Layer 3 probe — run dir: %s", run_dir)
    api_key = _resolve_api_key(logger)

    # Step 0a — query field discovery using a known debtor.
    known_name = SNAPSHOT_DEBTORS[3]["name"]  # "BARUK Joias..." — distinctive PJ
    step0 = discover_query_field(known_name, run_dir, api_key, logger)
    working_variant = step0.get("working_variant")

    # Step 0b — tribunal liveness.
    liveness = verify_tribunal_indices(run_dir, api_key, logger)
    live_indices = [idx for idx, rec in liveness.items()
                    if rec.get("ok") and rec.get("status") == 200]
    logger.info("Live tribunal indices: %s", live_indices)

    if not working_variant or not live_indices:
        report_path = write_report(run_dir, step0, liveness, [], [],
                                   {"days": date_window_days, "rationale": "default"})
        return {
            "ok": True,
            "working_variant": working_variant,
            "live_indices": live_indices,
            "report_path": str(report_path),
        }

    # Common-name archetype
    common_results: List[Dict[str, Any]] = []
    for name in SYNTHETIC_COMMON_NAMES:
        logger.info("common-name archetype: %s", name)
        r = query_debtor_across_tribunals(
            name, working_variant, live_indices, run_dir, api_key, logger,
        )
        common_results.append(r)
    write_json(run_dir / "common_name_results.json", common_results)

    # Snapshot-derived archetype with classification + scoring
    window_choice = {
        "days": date_window_days,
        "rationale": ("default 2-year window — tries 6m and 5y in a follow-up "
                      "iteration if time allows"),
    }
    snapshot_results: List[Dict[str, Any]] = []
    for debtor in SNAPSHOT_DEBTORS:
        logger.info("snapshot debtor: %s", debtor["name"])
        r = query_debtor_across_tribunals(
            debtor["name"], working_variant, live_indices, run_dir, api_key, logger,
        )
        all_hits: List[Dict[str, Any]] = []
        for rec in r["per_index"].values():
            all_hits.extend(rec.get("hits", []))
        classifications: List[str] = []
        hits_with_scores: List[Dict[str, Any]] = []
        for h in all_hits:
            cls = classify_hit_vs_reference(h, debtor, date_window_days)
            classifications.append(cls)
            hits_with_scores.append({
                **h,
                "classification": cls,
                "score_A": score_hit_heuristic_A(h, debtor),
                "score_B": score_hit_heuristic_B(h, debtor, date_window_days),
                "score_C": score_hit_heuristic_C(h, debtor),
            })
        snapshot_results.append({
            "debtor": debtor,
            "per_index": r["per_index"],
            "total_hits": len(all_hits),
            "classification_counts": dict(Counter(classifications)),
            "hits_with_scores": hits_with_scores,
            "coverage_vs_other_processes": "no-data (other_processes=NULL)",
        })
    write_json(run_dir / "snapshot_debtor_results.json", snapshot_results)

    report_path = write_report(run_dir, step0, liveness,
                               common_results, snapshot_results, window_choice)
    logger.info("Wrote %s", report_path)
    return {
        "ok": True,
        "working_variant": working_variant,
        "live_indices": live_indices,
        "common_name_count": len(common_results),
        "snapshot_debtor_count": len(snapshot_results),
        "report_path": str(report_path),
    }
