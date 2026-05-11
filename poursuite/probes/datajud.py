"""DataJud probe — public CNJ Elasticsearch API.

Verifies the documented and community-claimed behavior of the public DataJud
API against real sample queries. Outputs raw JSON dumps + a Markdown report
under <POURSUITE_LOG_DIR>/probes/datajud_<ts>/.

Reference: PROBE_FINDINGS.md §1 (the empirical questions are §1.7).
"""
import json
import logging
import os
import re
import time
import urllib.error
import urllib.request
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from poursuite.probes import write_json
from poursuite.probes.tpu import is_citacao, is_penhora

URL_TJSP = "https://api-publica.datajud.cnj.jus.br/api_publica_tjsp/_search"
URL_BASE_TJSP_INDEX = "https://api-publica.datajud.cnj.jus.br/api_publica_tjsp"

CROSS_TRIBUNAL_INDICES = (
    "api_publica_tjsp",
    "api_publica_tjrj",
    "api_publica_trf1",
    "api_publica_tst",
)

# Wiki-published key as of PROBE_FINDINGS §1.2 (May 2026). Override with env var.
DEFAULT_API_KEY = "cDZHYzlZa0JadVREZDJCendQbXY6SkJlTzNjLV9TRENyQk1RdnFKZGRQdw=="

PARTY_INDICATOR_KEYS = {
    "partes", "pessoas", "nomePessoa", "nomeParte", "documentoPrincipal",
    "cpf", "cnpj", "polo", "numeroDocumentoPrincipal",
}


# ---------------------------------------------------------------------------
# Sample loader (YAML or JSON, no PyYAML dependency)
# ---------------------------------------------------------------------------

def load_samples(path: Path) -> List[str]:
    """Load process numbers from a YAML or JSON file.

    JSON: top-level array of strings, OR `{"samples": [...]}`.
    YAML: minimal subset — `samples:` followed by `  - "..."` lines.
    """
    text = path.read_text(encoding="utf-8")
    try:
        data = json.loads(text)
        if isinstance(data, dict) and "samples" in data:
            return [str(x).strip() for x in data["samples"]]
        if isinstance(data, list):
            return [str(x).strip() for x in data]
    except json.JSONDecodeError:
        pass
    return _minimal_yaml_samples(text)


def _minimal_yaml_samples(text: str) -> List[str]:
    samples: List[str] = []
    in_block = False
    for raw in text.splitlines():
        line = raw.rstrip()
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        is_top_level = not line.startswith((" ", "\t"))
        if is_top_level:
            in_block = stripped.startswith("samples:")
            continue
        if in_block and stripped.startswith("-"):
            val = stripped[1:].strip()
            if val.startswith('"'):
                end = val.find('"', 1)
                val = val[1:end] if end > 0 else val[1:]
            elif val.startswith("'"):
                end = val.find("'", 1)
                val = val[1:end] if end > 0 else val[1:]
            else:
                val = val.split("#", 1)[0].strip()
            if val:
                samples.append(val)
    return samples


# ---------------------------------------------------------------------------
# CNJ process number transforms
# ---------------------------------------------------------------------------

def digits_only(process: str) -> str:
    return re.sub(r"\D", "", process)


def format_cnj(process_digits: str) -> str:
    d = process_digits
    if len(d) != 20 or not d.isdigit():
        return process_digits
    return f"{d[0:7]}-{d[7:9]}.{d[9:13]}.{d[13:14]}.{d[14:16]}.{d[16:20]}"


# ---------------------------------------------------------------------------
# HTTP — defensive wrappers that always return a result dict, never raise
# ---------------------------------------------------------------------------

def _post(url: str, payload: dict, api_key: str, *,
          timeout: int = 30, logger: Optional[logging.Logger] = None) -> Dict[str, Any]:
    return _http(url, api_key, method="POST",
                 body_bytes=json.dumps(payload).encode("utf-8"),
                 timeout=timeout, logger=logger)


def _http_get(url: str, api_key: str, *,
              timeout: int = 30, logger: Optional[logging.Logger] = None) -> Dict[str, Any]:
    return _http(url, api_key, method="GET", body_bytes=None,
                 timeout=timeout, logger=logger)


def _http(url: str, api_key: str, *, method: str, body_bytes: Optional[bytes],
          timeout: int, logger: Optional[logging.Logger]) -> Dict[str, Any]:
    headers = {
        "Authorization": f"APIKey {api_key}",
        "User-Agent": "poursuite-probes/0.1",
    }
    if body_bytes is not None:
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=body_bytes, headers=headers, method=method)
    t0 = time.perf_counter()
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
            latency = int((time.perf_counter() - t0) * 1000)
            try:
                return {"ok": True, "status": resp.status,
                        "body": json.loads(raw), "raw": None,
                        "error": None, "latency_ms": latency}
            except json.JSONDecodeError as e:
                return {"ok": False, "status": resp.status,
                        "body": None, "raw": raw,
                        "error": f"non-JSON body: {e}", "latency_ms": latency}
    except urllib.error.HTTPError as e:
        latency = int((time.perf_counter() - t0) * 1000)
        try:
            err_body = e.read().decode("utf-8", errors="replace")
        except Exception:
            err_body = ""
        if logger:
            logger.warning("HTTP %s for %s %s: %s", e.code, method, url, err_body[:300])
        return {"ok": False, "status": e.code, "body": None,
                "raw": err_body, "error": f"HTTPError {e.code} {e.reason}",
                "latency_ms": latency}
    except urllib.error.URLError as e:
        latency = int((time.perf_counter() - t0) * 1000)
        if logger:
            logger.error("Network error for %s %s: %s", method, url, e.reason)
        return {"ok": False, "status": None, "body": None, "raw": None,
                "error": f"URLError {e.reason}", "latency_ms": latency}
    except Exception as e:  # noqa: BLE001 — probe is intentionally defensive
        latency = int((time.perf_counter() - t0) * 1000)
        if logger:
            logger.error("Unexpected error %s %s: %s", method, url, e, exc_info=True)
        return {"ok": False, "status": None, "body": None, "raw": None,
                "error": f"{type(e).__name__}: {e}", "latency_ms": latency}


# ---------------------------------------------------------------------------
# Response inspection helpers
# ---------------------------------------------------------------------------

def _hit_count(body: Optional[dict]) -> int:
    if not body:
        return 0
    total = body.get("hits", {}).get("total", 0)
    if isinstance(total, dict):
        return int(total.get("value", 0))
    if isinstance(total, int):
        return total
    try:
        return int(total)
    except (TypeError, ValueError):
        return 0


def _has_hits(result: Dict[str, Any]) -> bool:
    if not result.get("ok"):
        return False
    body = result.get("body") or {}
    return bool(body.get("hits", {}).get("hits"))


def _summarize_response(result: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "ok": result.get("ok"),
        "status": result.get("status"),
        "latency_ms": result.get("latency_ms"),
        "error": result.get("error"),
        "hit_count": _hit_count(result.get("body")),
    }


def _scan_for_party_keys(source: dict) -> List[str]:
    found: List[str] = []
    for k, v in source.items():
        if k in PARTY_INDICATOR_KEYS:
            found.append(k)
        if isinstance(v, dict):
            for nk in v.keys():
                if nk in PARTY_INDICATOR_KEYS:
                    found.append(f"{k}.{nk}")
        elif isinstance(v, list) and v and isinstance(v[0], dict):
            for nk in v[0].keys():
                if nk in PARTY_INDICATOR_KEYS:
                    found.append(f"{k}[].{nk}")
    return sorted(set(found))


# ---------------------------------------------------------------------------
# Per-process probe
# ---------------------------------------------------------------------------

def probe_one(formatted: str, run_dir: Path, sample_index: int,
              api_key: str, logger: logging.Logger) -> Dict[str, Any]:
    digits = digits_only(formatted)
    canonical = format_cnj(digits)

    summary: Dict[str, Any] = {
        "sample_index": sample_index,
        "input": formatted,
        "digits_only": digits,
        "formatted": canonical,
        "queries": {},
        "hits": 0,
        "best_hit_fields_present": [],
        "movimentos": {"count": 0, "first": None, "last": None, "top_codes": []},
        "valor_causa": None,
        "data_ajuizamento": None,
        "data_hora_ultima_atualizacao": None,
        "tribunal": None,
        "classe": None,
        "nivel_sigilo": None,
        "complementos_tabelados_present_pct": 0.0,
        "penhora_codes_found": [],
        "citacao_codes_found": [],
        "latency_to_now_days": None,
        "errors": [],
    }

    res_digits = _post(URL_TJSP,
                       {"query": {"match": {"numeroProcesso": digits}}, "size": 5},
                       api_key, logger=logger)
    write_json(run_dir / f"sample_{sample_index:02d}_digits_match.json", res_digits)
    summary["queries"]["digits_match"] = _summarize_response(res_digits)

    res_fmt = _post(URL_TJSP,
                    {"query": {"match": {"numeroProcesso": canonical}}, "size": 5},
                    api_key, logger=logger)
    write_json(run_dir / f"sample_{sample_index:02d}_formatted_match.json", res_fmt)
    summary["queries"]["formatted_match"] = _summarize_response(res_fmt)

    primary = res_digits if _has_hits(res_digits) else res_fmt
    if not _has_hits(primary):
        res_phrase = _post(URL_TJSP,
                           {"query": {"match_phrase": {"numeroProcesso": digits}}, "size": 5},
                           api_key, logger=logger)
        write_json(run_dir / f"sample_{sample_index:02d}_phrase_match.json", res_phrase)
        summary["queries"]["phrase_match"] = _summarize_response(res_phrase)
        primary = res_phrase

    if _has_hits(primary):
        body = primary["body"]
        summary["hits"] = _hit_count(body)
        hit = body["hits"]["hits"][0]
        source = hit.get("_source", {}) or {}
        summary["best_hit_fields_present"] = sorted(source.keys())
        summary["valor_causa"] = source.get("valorCausa")
        summary["data_ajuizamento"] = source.get("dataAjuizamento")
        summary["data_hora_ultima_atualizacao"] = source.get("dataHoraUltimaAtualizacao")
        summary["tribunal"] = source.get("tribunal")
        summary["nivel_sigilo"] = source.get("nivelSigilo")
        classe = source.get("classe")
        if isinstance(classe, dict):
            summary["classe"] = f"{classe.get('codigo')} — {classe.get('nome')}"

        movs = source.get("movimentos") or []
        summary["movimentos"]["count"] = len(movs)
        if movs:
            datas = sorted(m.get("dataHora") for m in movs if m.get("dataHora"))
            summary["movimentos"]["first"] = datas[0] if datas else None
            summary["movimentos"]["last"] = datas[-1] if datas else None

            counter: Counter = Counter()
            name_for_code: Dict[int, str] = {}
            with_complementos = 0
            for m in movs:
                code = m.get("codigo")
                nome = m.get("nome", "") or ""
                if code is not None:
                    counter[int(code)] += 1
                    name_for_code.setdefault(int(code), nome)
                comp = m.get("complementosTabelados")
                if isinstance(comp, list) and len(comp) > 0:
                    with_complementos += 1
                if code is not None and is_penhora(int(code), nome):
                    summary["penhora_codes_found"].append({
                        "codigo": int(code), "nome": nome, "dataHora": m.get("dataHora"),
                    })
                if code is not None and is_citacao(int(code), nome):
                    summary["citacao_codes_found"].append({
                        "codigo": int(code), "nome": nome, "dataHora": m.get("dataHora"),
                    })
            summary["complementos_tabelados_present_pct"] = round(
                100 * with_complementos / max(len(movs), 1), 1
            )
            summary["movimentos"]["top_codes"] = [
                {"codigo": c, "nome": name_for_code.get(c, ""), "count": n}
                for c, n in counter.most_common(10)
            ]

        last_update = source.get("dataHoraUltimaAtualizacao")
        if last_update:
            try:
                dt = datetime.fromisoformat(last_update.replace("Z", "+00:00"))
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                summary["latency_to_now_days"] = (datetime.now(timezone.utc) - dt).days
            except (ValueError, TypeError) as e:
                summary["errors"].append(f"could not parse dataHoraUltimaAtualizacao: {e}")

    return summary


# ---------------------------------------------------------------------------
# Challenge experiments (PROBE_FINDINGS §1.3 / §1.7)
# ---------------------------------------------------------------------------

def experiment_1_party_stripping(samples: List[str], run_dir: Path,
                                 api_key: str, logger: logging.Logger) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "hypothesis": "Party data is stripped at the index level per Portaria 160/2020.",
        "tested": [],
        "party_indicators_found": [],
    }
    if not samples:
        out["conclusion"] = "INCONCLUSIVE — no samples provided."
        return out

    digits = digits_only(samples[0])
    base = {"query": {"match": {"numeroProcesso": digits}}, "size": 1}
    variants: List[Tuple[str, dict]] = [
        ("no_filter", base),
        ("source_true", {**base, "_source": True}),
        ("source_wildcard", {**base, "_source": ["*"]}),
        ("source_partes_pattern", {**base, "_source": ["partes*"]}),
        ("source_includes_obj", {**base, "_source": {"includes": ["*"]}}),
        ("fields_wildcard", {**base, "fields": ["*"]}),
    ]

    for name, payload in variants:
        result = _post(URL_TJSP, payload, api_key, logger=logger)
        write_json(run_dir / f"exp1_{name}.json", result)
        keys: List[str] = []
        party_keys: List[str] = []
        if _has_hits(result):
            source = result["body"]["hits"]["hits"][0].get("_source", {}) or {}
            keys = sorted(source.keys())
            party_keys = _scan_for_party_keys(source)
        out["tested"].append({
            "variant": name,
            "ok": result.get("ok"),
            "status": result.get("status"),
            "hit": _has_hits(result),
            "top_level_keys": keys,
            "party_indicator_keys_found": party_keys,
        })
        out["party_indicators_found"].extend(party_keys)

    indicators = sorted(set(out["party_indicators_found"]))
    out["conclusion"] = (
        f"REFUTED — party-shaped fields appeared: {', '.join(indicators)}"
        if indicators
        else "CONFIRMED — no party-shaped fields appeared in any tested request variant."
    )
    return out


def experiment_2_cnpj_search(known_cnpj: str, run_dir: Path,
                             api_key: str, logger: logging.Logger) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "hypothesis": "CPF/CNPJ are not indexed for search in the public API.",
        "known_cnpj": known_cnpj,
        "tested": [],
    }
    variants: List[Tuple[str, dict]] = [
        ("match_numeroDocumentoPrincipal",
         {"query": {"match": {"numeroDocumentoPrincipal": known_cnpj}}, "size": 1}),
        ("match_partes_documento",
         {"query": {"match": {"partes.numeroDocumentoPrincipal": known_cnpj}}, "size": 1}),
        ("query_string_full_text",
         {"query": {"query_string": {"query": known_cnpj}}, "size": 1}),
        ("term_all",
         {"query": {"term": {"_all": known_cnpj}}, "size": 1}),
    ]
    for name, payload in variants:
        result = _post(URL_TJSP, payload, api_key, logger=logger)
        write_json(run_dir / f"exp2_{name}.json", result)
        out["tested"].append({
            "variant": name,
            "ok": result.get("ok"),
            "status": result.get("status"),
            "hit_count": _hit_count(result.get("body")),
            "error_excerpt": (result.get("raw") or "")[:300] if not result.get("ok") else None,
        })
    any_hits = any(t["hit_count"] > 0 for t in out["tested"])
    out["conclusion"] = (
        "REFUTED — at least one variant returned hits."
        if any_hits else
        "CONFIRMED (consistent with) — all CNPJ-search variants returned 0 hits or errors. "
        "Inspect error_excerpt fields above to distinguish 'not indexed' from 'unknown field'."
    )
    return out


def experiment_3_sealed(samples: List[str], run_dir: Path,
                        api_key: str, logger: logging.Logger) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "hypothesis": "Sealed (segredo de justiça) cases either don't appear or have nulled fields.",
        "tested": [],
        "nivel_sigilo_observed": [],
    }
    for i, sample in enumerate(samples):
        digits = digits_only(sample)
        r1 = _post(URL_TJSP, {"query": {"match": {"numeroProcesso": digits}}, "size": 1},
                   api_key, logger=logger)
        r2 = _post(URL_TJSP, {"query": {"query_string": {"query": digits}}, "size": 1},
                   api_key, logger=logger)
        write_json(run_dir / f"exp3_sample_{i:02d}_match.json", r1)
        write_json(run_dir / f"exp3_sample_{i:02d}_query_string.json", r2)
        nivel = None
        if _has_hits(r1):
            nivel = r1["body"]["hits"]["hits"][0].get("_source", {}).get("nivelSigilo")
        out["tested"].append({
            "sample": sample,
            "match_hit_count": _hit_count(r1.get("body")),
            "query_string_hit_count": _hit_count(r2.get("body")),
            "nivelSigilo": nivel,
        })
        if nivel is not None:
            out["nivel_sigilo_observed"].append(nivel)

    seen = ", ".join(str(s) for s in sorted(set(out["nivel_sigilo_observed"]))) or "none"
    out["conclusion"] = (
        "INCONCLUSIVE for sealed-case behavior (no known-sealed sample in this set). "
        f"nivelSigilo values seen: {seen}."
    )
    return out


def experiment_4_undocumented(run_dir: Path, api_key: str,
                              logger: logging.Logger) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "hypothesis": "Undocumented endpoints/aliases may expose more.",
        "tested": [],
    }

    res_mapping = _http_get(f"{URL_BASE_TJSP_INDEX}/_mapping", api_key, logger=logger)
    write_json(run_dir / "exp4_mapping.json", res_mapping)
    out["tested"].append({
        "variant": "GET /_mapping",
        "ok": res_mapping.get("ok"),
        "status": res_mapping.get("status"),
        "body_excerpt": (res_mapping.get("raw") or json.dumps(res_mapping.get("body") or {})[:500])[:500],
    })

    res_agg = _post(URL_TJSP, {
        "size": 0,
        "aggs": {"distinct_fields": {"terms": {"field": "_field_names", "size": 200}}},
    }, api_key, logger=logger)
    write_json(run_dir / "exp4_aggs_field_names.json", res_agg)
    field_buckets: List[str] = []
    if res_agg.get("ok"):
        try:
            buckets = (res_agg["body"]
                       .get("aggregations", {})
                       .get("distinct_fields", {})
                       .get("buckets", []))
            field_buckets = [b.get("key") for b in buckets if b.get("key")]
        except (AttributeError, KeyError, TypeError):
            pass
    out["tested"].append({
        "variant": "POST /_search aggs _field_names",
        "ok": res_agg.get("ok"),
        "status": res_agg.get("status"),
        "distinct_fields_count": len(field_buckets),
        "distinct_fields_sample": field_buckets[:30],
    })

    res_url = _http_get(f"{URL_TJSP}?_source_includes=partes*", api_key, logger=logger)
    write_json(run_dir / "exp4_source_includes_url.json", res_url)
    out["tested"].append({
        "variant": "GET /_search?_source_includes=partes*",
        "ok": res_url.get("ok"),
        "status": res_url.get("status"),
    })

    out["conclusion"] = (
        "See per-variant results. Any unexpected 200 + party-shaped data above "
        "would refute the stripping claim."
    )
    return out


def experiment_5_cross_tribunal(samples: List[str], run_dir: Path,
                                api_key: str, logger: logging.Logger) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "hypothesis": "Field stripping is national; per-tribunal extras may differ.",
        "tested": [],
    }
    for index_name in CROSS_TRIBUNAL_INDICES:
        url = f"https://api-publica.datajud.cnj.jus.br/{index_name}/_search"
        if index_name == "api_publica_tjsp" and samples:
            payload = {"query": {"match": {"numeroProcesso": digits_only(samples[0])}}, "size": 1}
        else:
            payload = {"query": {"match_all": {}}, "size": 1}
        result = _post(url, payload, api_key, logger=logger)
        write_json(run_dir / f"exp5_{index_name}.json", result)
        keys: List[str] = []
        party_keys: List[str] = []
        if _has_hits(result):
            source = result["body"]["hits"]["hits"][0].get("_source", {}) or {}
            keys = sorted(source.keys())
            party_keys = _scan_for_party_keys(source)
        out["tested"].append({
            "tribunal": index_name,
            "ok": result.get("ok"),
            "status": result.get("status"),
            "hit_count": _hit_count(result.get("body")),
            "top_level_keys": keys,
            "party_indicator_keys_found": party_keys,
        })

    key_sets = {t["tribunal"]: set(t["top_level_keys"])
                for t in out["tested"] if t["top_level_keys"]}
    if len(key_sets) >= 2:
        common = set.intersection(*key_sets.values())
        out["common_keys"] = sorted(common)
        out["per_tribunal_extras"] = {
            t: sorted(k - common) for t, k in key_sets.items()
        }
    party_findings = {t["tribunal"]: t["party_indicator_keys_found"]
                      for t in out["tested"] if t["party_indicator_keys_found"]}
    out["per_tribunal_party_findings"] = party_findings
    out["conclusion"] = (
        f"REFUTED for nationally-uniform stripping — party fields appeared in: "
        f"{', '.join(party_findings)}"
        if party_findings
        else "CONFIRMED (consistent with) — no tribunal returned party fields. "
             "Per-tribunal extras (non-party) listed above."
    )
    return out


# ---------------------------------------------------------------------------
# Report writer
# ---------------------------------------------------------------------------

def write_datajud_report(run_dir: Path,
                         samples_summary: List[Dict[str, Any]],
                         experiments: Dict[str, Dict[str, Any]],
                         logger: logging.Logger) -> Path:
    md: List[str] = []
    md.append("# DataJud Probe Report")
    md.append(f"_Run: `{run_dir.name}`  |  Generated: {datetime.now(timezone.utc).isoformat()}_")
    md.append("")

    warnings: List[str] = []
    for s in samples_summary:
        if s.get("hits", 0) == 0:
            warnings.append(f"Sample `{s['input']}`: no hits returned by any query variant.")
        for err in s.get("errors", []):
            warnings.append(f"Sample `{s['input']}`: {err}")
    if warnings:
        md.append("## ⚠ Warnings")
        md.append("")
        for w in warnings:
            md.append(f"- {w}")
        md.append("")

    md.append("## Per-process summary")
    md.append("")
    for s in samples_summary:
        md.append(f"### `{s['input']}`")
        md.append(f"- digits-only: `{s['digits_only']}`")
        md.append(f"- canonical: `{s['formatted']}`")
        for qname, q in s["queries"].items():
            err_part = f", error={q['error']}" if q.get("error") else ""
            md.append(f"- query `{qname}`: hits={q['hit_count']}, "
                      f"status={q['status']}, latency_ms={q['latency_ms']}{err_part}")
        if s.get("hits", 0) > 0:
            md.append(f"- total hits: {s['hits']}")
            md.append(f"- tribunal: `{s.get('tribunal')}`  |  classe: {s.get('classe')}")
            md.append(f"- nivelSigilo: {s.get('nivel_sigilo')}")
            md.append(f"- dataAjuizamento: {s.get('data_ajuizamento')}")
            md.append(f"- dataHoraUltimaAtualizacao: {s.get('data_hora_ultima_atualizacao')}")
            md.append(f"- top-level fields populated: "
                      f"{', '.join('`' + k + '`' for k in s['best_hit_fields_present'])}")
            md.append(f"- `valorCausa`: "
                      f"{s['valor_causa'] if s['valor_causa'] not in (None, '') else '(absent / empty)'}")
            mv = s["movimentos"]
            md.append(f"- movimentos: {mv['count']} entries"
                      + (f", {mv['first']} → {mv['last']}" if mv['count'] else ""))
            md.append(f"- complementosTabelados populated on "
                      f"{s['complementos_tabelados_present_pct']}% of movimentos")
            if mv["top_codes"]:
                md.append("- top-10 movimento codes:")
                for tc in mv["top_codes"]:
                    md.append(f"  - `{tc['codigo']}` ({tc['count']}×) — {tc['nome']}")
            md.append(f"- penhora signals: "
                      + (
                          "; ".join(f"`{p['codigo']}` ({p['nome']}, {p['dataHora']})"
                                    for p in s["penhora_codes_found"])
                          if s["penhora_codes_found"] else "none detected"
                      ))
            md.append(f"- citação signals: "
                      + (
                          "; ".join(f"`{p['codigo']}` ({p['nome']}, {p['dataHora']})"
                                    for p in s["citacao_codes_found"])
                          if s["citacao_codes_found"] else "none detected"
                      ))
            if s.get("latency_to_now_days") is not None:
                md.append(f"- latency vs. today: {s['latency_to_now_days']} days "
                          f"since `dataHoraUltimaAtualizacao`")
        md.append("")

    md.append("## Cross-process aggregate")
    md.append("")
    total = len(samples_summary)
    with_hits = sum(1 for s in samples_summary if s.get("hits", 0) > 0)
    md.append(f"- samples processed: {total}")
    md.append(f"- samples returning ≥1 hit: {with_hits}")
    md.append(f"- samples returning no hits: {total - with_hits}")
    counts = [s["movimentos"]["count"] for s in samples_summary if s.get("hits", 0) > 0]
    if counts:
        md.append(f"- movimentos count (hit-only): "
                  f"min={min(counts)}, max={max(counts)}, mean={sum(counts)/len(counts):.1f}")

    all_codes: Counter = Counter()
    name_for_code: Dict[int, str] = {}
    for s in samples_summary:
        for tc in s["movimentos"]["top_codes"]:
            all_codes[tc["codigo"]] += tc["count"]
            name_for_code.setdefault(tc["codigo"], tc["nome"])
    if all_codes:
        md.append("- distinct movimento codes (across top-10s of all samples):")
        for code, cnt in all_codes.most_common(30):
            md.append(f"  - `{code}` ({cnt}×) — {name_for_code.get(code, '')}")
    md.append("")

    if experiments:
        md.append("## Challenge experiments")
        md.append("")
        for name, body in experiments.items():
            md.append(f"### {name}")
            md.append(f"**Hypothesis:** {body.get('hypothesis', '')}")
            md.append("")
            md.append(f"**Conclusion:** {body.get('conclusion', '')}")
            md.append("")
            for t in body.get("tested", []):
                bullet_parts = []
                for k, v in t.items():
                    if k == "top_level_keys":
                        bullet_parts.append(f"top_level_keys=[{len(v)}]")
                    elif isinstance(v, list) and len(v) > 8:
                        bullet_parts.append(f"{k}=[{len(v)}…]")
                    else:
                        bullet_parts.append(f"{k}={v}")
                md.append("- " + ", ".join(bullet_parts))
            if "common_keys" in body:
                md.append("")
                md.append(f"**Common keys across responding tribunals:** "
                          + (", ".join("`" + k + "`" for k in body["common_keys"])
                             if body["common_keys"] else "(none)"))
                md.append("**Per-tribunal extras:**")
                for tname, extras in body.get("per_tribunal_extras", {}).items():
                    md.append(f"- `{tname}`: "
                              + (", ".join("`" + k + "`" for k in extras) if extras else "(none)"))
            md.append("")

    out_path = run_dir / "datajud_report.md"
    out_path.write_text("\n".join(md), encoding="utf-8")
    logger.info("Wrote %s", out_path)
    return out_path


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

def _resolve_api_key(logger: logging.Logger) -> str:
    env_key = os.environ.get("POURSUITE_DATAJUD_API_KEY", "").strip()
    if env_key:
        logger.info("DataJud API key: from POURSUITE_DATAJUD_API_KEY env var.")
        return env_key
    logger.info("DataJud API key: PROBE_FINDINGS §1.2 wiki value (override via "
                "POURSUITE_DATAJUD_API_KEY).")
    return DEFAULT_API_KEY


def run(samples: List[str], run_dir: Path, logger: logging.Logger,
        *, known_cnpj: str, skip_experiments: bool = False) -> Dict[str, Any]:
    api_key = _resolve_api_key(logger)

    samples_summary: List[Dict[str, Any]] = []
    for i, s in enumerate(samples):
        logger.info("Probing sample %d/%d: %s", i + 1, len(samples), s)
        try:
            samples_summary.append(probe_one(s, run_dir, i, api_key, logger))
        except Exception as e:  # noqa: BLE001 — keep going on any single-sample failure
            logger.exception("Unhandled error on sample %s", s)
            samples_summary.append({
                "sample_index": i, "input": s,
                "errors": [f"{type(e).__name__}: {e}"],
                "hits": 0, "queries": {},
                "movimentos": {"count": 0, "first": None, "last": None, "top_codes": []},
                "best_hit_fields_present": [], "valor_causa": None,
                "data_ajuizamento": None, "data_hora_ultima_atualizacao": None,
                "tribunal": None, "classe": None, "nivel_sigilo": None,
                "complementos_tabelados_present_pct": 0.0,
                "penhora_codes_found": [], "citacao_codes_found": [],
                "latency_to_now_days": None,
            })
    write_json(run_dir / "samples_summary.json", samples_summary)

    experiments: Dict[str, Dict[str, Any]] = {}
    if not skip_experiments:
        logger.info("Running Challenge Experiment 1 — party stripping")
        experiments["Experiment 1 — Party stripping"] = experiment_1_party_stripping(
            samples, run_dir, api_key, logger)
        logger.info("Running Challenge Experiment 2 — CPF/CNPJ search")
        experiments["Experiment 2 — CPF/CNPJ search"] = experiment_2_cnpj_search(
            known_cnpj, run_dir, api_key, logger)
        logger.info("Running Challenge Experiment 3 — sealed cases")
        experiments["Experiment 3 — Sealed cases"] = experiment_3_sealed(
            samples, run_dir, api_key, logger)
        logger.info("Running Challenge Experiment 4 — undocumented endpoints")
        experiments["Experiment 4 — Undocumented endpoints"] = experiment_4_undocumented(
            run_dir, api_key, logger)
        logger.info("Running Challenge Experiment 5 — cross-tribunal")
        experiments["Experiment 5 — Cross-tribunal"] = experiment_5_cross_tribunal(
            samples, run_dir, api_key, logger)
        write_json(run_dir / "experiments_summary.json", experiments)

    write_datajud_report(run_dir, samples_summary, experiments, logger)
    return {"ok": True, "samples_summary": samples_summary, "experiments": experiments}
