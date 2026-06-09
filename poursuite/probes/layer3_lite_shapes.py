"""Layer 3-lite · L3L-a — field-shape confirmation (read-only).

Pulls real process numbers from the snapshot store, batches them through a
single DataJud `_msearch` (the production-shaped path), and dumps the EXACT
nested shape of each of the five Layer 3-lite target fields so the v5 schema
(L3L-b) is designed against observed reality, not the inventory's prior sample.

Targets (per CLAUDE_CODE_BRIEF_LAYER3_LITE.md + docs/DATAJUD_CAPABILITY_INVENTORY.md):
  - movimentos.complementosTabelados   (the prize; nested array-of-objects)
  - assuntos                            (array-of-objects)
  - grau                               (scalar)
  - orgaoJulgador.codigoMunicipioIBGE   (scalar; conditional/sparse)
  - dataHoraUltimaAtualizacao           (timestamp string)

Investigation-only. Opens esaj_snapshots.db with `mode=ro` so it CANNOT migrate
or write the live store (the v5 migration is deliberately deferred to L3L-b).
Run:  python -m poursuite.probes.layer3_lite_shapes
"""
from __future__ import annotations

import json
import logging
import sqlite3
import urllib.error
import urllib.request
import time
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from poursuite.config import SNAPSHOT_DIR
from poursuite.probes import make_run_dir, write_json
from poursuite.probes.datajud import _hit_count, _resolve_api_key, digits_only

MSEARCH_URL = "https://api-publica.datajud.cnj.jus.br/_msearch"
TJSP_INDEX = "api_publica_tjsp"

# _source projection — only the target fields (+ join key + movement context).
SOURCE_INCLUDES: Tuple[str, ...] = (
    "numeroProcesso",
    "grau",
    "assuntos",
    "orgaoJulgador.codigoMunicipioIBGE",
    "dataHoraUltimaAtualizacao",
    "movimentos.codigo",
    "movimentos.nome",
    "movimentos.complementosTabelados",
)

# Known-indexed TJSP cases (from the inventory's Part 2 sample) used to
# supplement the store so we reliably observe complementosTabelados shapes
# even if some store cases aren't in DataJud's index.
KNOWN_INDEXED_FALLBACK: Tuple[str, ...] = (
    "1018045-50.2015.8.26.0506",
    "0141625-04.2009.8.26.0100",
    "1185179-78.2023.8.26.0100",
    "1046043-29.2020.8.26.0114",
)


# ---------------------------------------------------------------------------
# Read-only test-case loading
# ---------------------------------------------------------------------------

def load_store_process_numbers(db_path: Path, limit: int,
                               logger: logging.Logger) -> List[str]:
    """Latest-loaded process numbers from the snapshot store, READ-ONLY.

    Uses a `mode=ro` URI connection so this probe can never create, write, or
    migrate esaj_snapshots.db — the live store stays at v4 until L3L-b."""
    if not db_path.exists():
        logger.warning("snapshot store not found at %s — using fallback only", db_path)
        return []
    uri = f"file:{db_path.as_posix()}?mode=ro"
    try:
        conn = sqlite3.connect(uri, uri=True)
    except sqlite3.OperationalError as e:
        logger.warning("could not open store read-only (%s) — fallback only", e)
        return []
    try:
        cur = conn.execute(
            """
            WITH latest AS (
              SELECT process_number, MAX(snapshot_ts) AS ts
              FROM process_snapshot WHERE scrape_outcome = 'loaded'
              GROUP BY process_number
            )
            SELECT process_number FROM latest ORDER BY process_number LIMIT ?
            """,
            (limit,),
        )
        return [r[0] for r in cur.fetchall()]
    except sqlite3.OperationalError as e:
        logger.warning("process_snapshot query failed (%s) — fallback only", e)
        return []
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Batched _msearch (production-shaped path)
# ---------------------------------------------------------------------------

def msearch_by_process(process_numbers: List[str], api_key: str,
                       logger: logging.Logger) -> Dict[str, Any]:
    """One `_msearch` round-trip: one numeroProcesso subquery per process,
    `_source`-filtered to the target fields. Returns the parsed response dict."""
    lines: List[str] = []
    for pn in process_numbers:
        lines.append(json.dumps({"index": TJSP_INDEX}))
        lines.append(json.dumps({
            "query": {"match": {"numeroProcesso": digits_only(pn)}},
            "size": 1,
            "_source": {"includes": list(SOURCE_INCLUDES)},
        }))
    body = ("\n".join(lines) + "\n").encode("utf-8")
    headers = {
        "Authorization": f"APIKey {api_key}",
        "User-Agent": "poursuite-probes/0.1",
        "Content-Type": "application/x-ndjson",
    }
    req = urllib.request.Request(MSEARCH_URL, data=body, headers=headers, method="POST")
    t0 = time.perf_counter()
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
            latency = int((time.perf_counter() - t0) * 1000)
            return {"ok": True, "status": resp.status,
                    "body": json.loads(raw), "latency_ms": latency, "error": None}
    except urllib.error.HTTPError as e:
        raw = ""
        try:
            raw = e.read().decode("utf-8", errors="replace")
        except Exception:  # noqa: BLE001
            pass
        return {"ok": False, "status": e.code, "body": None,
                "latency_ms": int((time.perf_counter() - t0) * 1000),
                "error": f"HTTPError {e.code} {e.reason}", "raw": raw}
    except Exception as e:  # noqa: BLE001
        return {"ok": False, "status": None, "body": None,
                "latency_ms": int((time.perf_counter() - t0) * 1000),
                "error": f"{type(e).__name__}: {e}"}


# ---------------------------------------------------------------------------
# Shape description
# ---------------------------------------------------------------------------

def _type_name(v: Any) -> str:
    if v is None:
        return "null"
    if isinstance(v, bool):
        return "bool"
    if isinstance(v, int):
        return "int"
    if isinstance(v, float):
        return "float"
    if isinstance(v, str):
        return "str"
    if isinstance(v, list):
        return "array"
    if isinstance(v, dict):
        return "object"
    return type(v).__name__


def describe_shape(value: Any, depth: int = 0) -> Any:
    """Recursive, compact shape description: object→{key: type/shape},
    array→{element shape, len}, scalar→{type, sample}."""
    if depth > 4:
        return {"_type": _type_name(value), "_truncated": True}
    if isinstance(value, dict):
        return {"_type": "object",
                "keys": {k: describe_shape(v, depth + 1) for k, v in value.items()}}
    if isinstance(value, list):
        first = next((e for e in value if e is not None), None)
        return {"_type": "array", "len": len(value),
                "element": describe_shape(first, depth + 1) if first is not None else None}
    sample = value
    if isinstance(value, str) and len(value) > 60:
        sample = value[:60] + "…"
    return {"_type": _type_name(value), "sample": sample}


class FieldKeyProfiler:
    """Unions object keys across every instance of an array-of-objects field
    (e.g. every complementosTabelados object across every movement across every
    case). For each key records the set of value-types and up to 3 samples —
    this is what actually drives a relational schema."""

    def __init__(self) -> None:
        self.key_types: Dict[str, Counter] = {}
        self.key_samples: Dict[str, List[Any]] = {}
        self.object_count = 0
        self.objects_missing_key: Dict[str, int] = {}

    def add(self, obj: Dict[str, Any]) -> None:
        self.object_count += 1
        for k, v in obj.items():
            self.key_types.setdefault(k, Counter())[_type_name(v)] += 1
            samples = self.key_samples.setdefault(k, [])
            if len(samples) < 3 and v not in samples:
                s = v[:60] + "…" if isinstance(v, str) and len(v) > 60 else v
                if s not in samples:
                    samples.append(s)

    def finalize(self) -> Dict[str, Any]:
        # presence: how many objects had each key
        presence: Dict[str, str] = {}
        for k, types in self.key_types.items():
            present = sum(types.values())
            presence[k] = f"{present}/{self.object_count}"
        return {
            "object_count": self.object_count,
            "keys": sorted(self.key_types.keys()),
            "key_presence": presence,
            "key_types": {k: dict(c) for k, c in self.key_types.items()},
            "key_samples": self.key_samples,
        }


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

def _extract_source(subresponse: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    hits = (subresponse or {}).get("hits", {}).get("hits", [])
    return (hits[0].get("_source") or {}) if hits else None


def run(run_dir: Path, logger: logging.Logger) -> Dict[str, Any]:
    api_key = _resolve_api_key(logger)
    db_path = SNAPSHOT_DIR / "esaj_snapshots.db"

    store_pns = load_store_process_numbers(db_path, limit=10, logger=logger)
    logger.info("loaded %d process numbers from snapshot store (read-only)", len(store_pns))

    # Supplement with known-indexed fallbacks so we reliably observe shapes.
    seen = set(digits_only(p) for p in store_pns)
    supplemental = [p for p in KNOWN_INDEXED_FALLBACK if digits_only(p) not in seen]
    process_numbers = store_pns + supplemental
    provenance = ({p: "store" for p in store_pns}
                  | {p: "known_indexed_fallback" for p in supplemental})
    logger.info("querying %d processes via one _msearch (%d store + %d fallback)",
                len(process_numbers), len(store_pns), len(supplemental))

    res = msearch_by_process(process_numbers, api_key, logger)
    write_json(run_dir / "msearch_raw.json", res)
    if not res.get("ok"):
        logger.error("_msearch failed: status=%s error=%s",
                     res.get("status"), res.get("error"))
        return {"error": res.get("error"), "status": res.get("status")}

    subresponses = (res.get("body") or {}).get("responses") or []
    logger.info("_msearch OK in %d ms — %d subresponses",
                res.get("latency_ms"), len(subresponses))

    # Per-case extraction
    ct_profiler = FieldKeyProfiler()       # complementosTabelados objects
    assuntos_profiler = FieldKeyProfiler()  # assuntos objects
    grau_values: Counter = Counter()
    ibge_present = 0
    ibge_samples: List[Any] = []
    ult_atualizacao_samples: List[str] = []
    per_case: List[Dict[str, Any]] = []
    indexed = 0
    movements_with_complementos = 0
    total_movements = 0

    for pn, sub in zip(process_numbers, subresponses):
        src = _extract_source(sub)
        case: Dict[str, Any] = {
            "process_number": pn,
            "provenance": provenance.get(pn),
            "indexed": src is not None,
            "status": sub.get("status"),
        }
        if src is None:
            per_case.append(case)
            continue
        indexed += 1

        # grau
        if "grau" in src:
            case["grau"] = src.get("grau")
            if src.get("grau") is not None:
                grau_values[str(src.get("grau"))] += 1

        # assuntos
        assuntos = src.get("assuntos")
        case["assuntos_shape"] = describe_shape(assuntos)
        if isinstance(assuntos, list):
            for a in assuntos:
                if isinstance(a, dict):
                    assuntos_profiler.add(a)

        # orgaoJulgador.codigoMunicipioIBGE
        ibge = (src.get("orgaoJulgador") or {}).get("codigoMunicipioIBGE") \
            if isinstance(src.get("orgaoJulgador"), dict) else None
        case["codigoMunicipioIBGE"] = ibge
        if ibge is not None:
            ibge_present += 1
            if len(ibge_samples) < 5:
                ibge_samples.append(ibge)

        # dataHoraUltimaAtualizacao
        ult = src.get("dataHoraUltimaAtualizacao")
        case["dataHoraUltimaAtualizacao"] = ult
        if isinstance(ult, str) and len(ult_atualizacao_samples) < 5:
            ult_atualizacao_samples.append(ult)

        # movimentos.complementosTabelados — the prize
        movs = src.get("movimentos") or []
        case_ct_count = 0
        for m in movs:
            total_movements += 1
            cts = (m.get("complementosTabelados") if isinstance(m, dict) else None) or []
            if cts:
                movements_with_complementos += 1
            for ct in cts:
                if isinstance(ct, dict):
                    ct_profiler.add(ct)
                    case_ct_count += 1
        case["movimentos_count"] = len(movs)
        case["complementos_tabelados_count"] = case_ct_count
        # capture a literal example of one complemento for the report
        if case_ct_count and "complemento_example" not in case:
            for m in movs:
                for ct in (m.get("complementosTabelados") or []):
                    if isinstance(ct, dict):
                        case["complemento_example"] = ct
                        break
                if "complemento_example" in case:
                    break

        per_case.append(case)

    ct_profile = ct_profiler.finalize()
    assuntos_profile = assuntos_profiler.finalize()

    # Reconciliation vs inventory's stated shapes
    inventory_ct_keys = {"codigo", "valor", "nome", "descricao"}
    inventory_assuntos_keys = {"codigo", "nome"}
    ct_observed = set(ct_profile["keys"])
    assuntos_observed = set(assuntos_profile["keys"])

    reconciliation = {
        "complementosTabelados": {
            "inventory_keys": sorted(inventory_ct_keys),
            "observed_keys": sorted(ct_observed),
            "missing_vs_inventory": sorted(inventory_ct_keys - ct_observed),
            "extra_vs_inventory": sorted(ct_observed - inventory_ct_keys),
            "match": ct_observed == inventory_ct_keys,
        },
        "assuntos": {
            "inventory_keys": sorted(inventory_assuntos_keys),
            "observed_keys": sorted(assuntos_observed),
            "missing_vs_inventory": sorted(inventory_assuntos_keys - assuntos_observed),
            "extra_vs_inventory": sorted(assuntos_observed - inventory_assuntos_keys),
            "match": assuntos_observed == inventory_assuntos_keys,
        },
    }

    findings: Dict[str, Any] = {
        "probe": "layer3_lite_shapes (L3L-a)",
        "msearch_latency_ms": res.get("latency_ms"),
        "process_count": len(process_numbers),
        "store_case_count": len(store_pns),
        "datajud_hit_rate": f"{indexed}/{len(process_numbers)}",
        "movements_total": total_movements,
        "movements_with_complementos": movements_with_complementos,
        "grau_values": dict(grau_values),
        "codigoMunicipioIBGE_present": f"{ibge_present}/{indexed}",
        "codigoMunicipioIBGE_samples": ibge_samples,
        "dataHoraUltimaAtualizacao_samples": ult_atualizacao_samples,
        "complementosTabelados_profile": ct_profile,
        "assuntos_profile": assuntos_profile,
        "reconciliation": reconciliation,
        "per_case": per_case,
    }
    write_json(run_dir / "l3l_a_findings.json", findings)
    _write_markdown(run_dir / "l3l_a_shapes.md", findings)
    return findings


def _write_markdown(path: Path, f: Dict[str, Any]) -> None:
    ct = f["complementosTabelados_profile"]
    asn = f["assuntos_profile"]
    rec = f["reconciliation"]
    lines: List[str] = []
    lines.append("# L3L-a — DataJud field-shape confirmation\n")
    lines.append(f"- DataJud hit rate: **{f['datajud_hit_rate']}** "
                 f"({f['store_case_count']} from store + fallback supplement)")
    lines.append(f"- `_msearch` latency: {f['msearch_latency_ms']} ms (single round-trip)")
    lines.append(f"- movements carrying complementosTabelados: "
                 f"{f['movements_with_complementos']}/{f['movements_total']}\n")

    lines.append("## complementosTabelados — the prize\n")
    lines.append(f"Observed across **{ct['object_count']}** complemento objects.\n")
    lines.append("| key | presence | types | samples |")
    lines.append("|---|---|---|---|")
    for k in ct["keys"]:
        lines.append(f"| `{k}` | {ct['key_presence'].get(k)} | "
                     f"{ct['key_types'].get(k)} | "
                     f"{ct['key_samples'].get(k)} |")
    r = rec["complementosTabelados"]
    lines.append(f"\nReconciliation vs inventory `{{codigo, valor, nome, descricao}}`: "
                 f"**{'MATCH' if r['match'] else 'DIVERGES'}** — "
                 f"missing={r['missing_vs_inventory']}, extra={r['extra_vs_inventory']}\n")

    lines.append("## assuntos\n")
    lines.append(f"Observed across **{asn['object_count']}** assunto objects. "
                 f"Keys: {asn['keys']}")
    ra = rec["assuntos"]
    lines.append(f"Reconciliation vs `{{codigo, nome}}`: "
                 f"**{'MATCH' if ra['match'] else 'DIVERGES'}** — "
                 f"missing={ra['missing_vs_inventory']}, extra={ra['extra_vs_inventory']}\n")

    lines.append("## scalars\n")
    lines.append(f"- `grau` distinct values: {f['grau_values']}")
    lines.append(f"- `orgaoJulgador.codigoMunicipioIBGE` present: "
                 f"{f['codigoMunicipioIBGE_present']} (samples {f['codigoMunicipioIBGE_samples']})")
    lines.append(f"- `dataHoraUltimaAtualizacao` samples: "
                 f"{f['dataHoraUltimaAtualizacao_samples']}\n")
    path.write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s - %(levelname)s - %(message)s")
    logger = logging.getLogger("l3l_a")
    run_dir = make_run_dir("datajud_l3l_a")
    logger.info("L3L-a run dir: %s", run_dir)
    findings = run(run_dir, logger)
    print("\n==== L3L-a summary ====")
    print(json.dumps({
        "datajud_hit_rate": findings.get("datajud_hit_rate"),
        "complementosTabelados_keys": findings.get("complementosTabelados_profile", {}).get("keys"),
        "reconciliation": findings.get("reconciliation"),
        "run_dir": str(run_dir),
    }, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
