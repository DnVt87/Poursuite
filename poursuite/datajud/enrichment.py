"""DataJud per-process enrichment — Layer 3-lite (L3L-c).

Batched `_msearch`, `_source`-filtered, SEQUENTIAL batches, per-process error
isolation (a missing/bad process number degrades to a not-found record, never
aborts the batch — same philosophy as the eSAJ scraper). Maps DataJud hits to
`EnrichmentRecord`s the snapshot store persists via append-on-change.

Target fields (shapes confirmed in L3L-a):
  - movimentos.complementosTabelados → stored in FULL (every complemento)
  - assuntos, grau, orgaoJulgador.codigoMunicipioIBGE, dataHoraUltimaAtualizacao

Run a first/ad-hoc batch:
  python -m poursuite.datajud                       # pulls N loaded from store
  python -m poursuite.datajud 1002177-13.2020.8.26.0100 ...
"""
from __future__ import annotations

import json
import logging
import re
import sqlite3
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Sequence

from poursuite.datajud.client import (
    DEFAULT_INDEX,
    msearch_by_process,
    resolve_api_key,
)
from poursuite.db.esaj_snapshots import SnapshotStore
from poursuite.utils import setup_logging

# `_source` projection — only the target fields (+ join key + movement context
# so complementos are interpretable). ~90% payload reduction vs full docs.
SOURCE_INCLUDES: Sequence[str] = (
    "numeroProcesso",
    "grau",
    "assuntos",
    "orgaoJulgador.codigoMunicipioIBGE",
    "dataHoraUltimaAtualizacao",
    "movimentos.codigo",
    "movimentos.nome",
    "movimentos.complementosTabelados",
)


@dataclass
class Complemento:
    """One complementoTabelado, with its parent movement's code/name for context."""
    movimento_codigo: Optional[int]
    movimento_nome: Optional[str]
    codigo: Optional[int]
    valor: Optional[int]
    nome: Optional[str]
    descricao: Optional[str]


@dataclass
class EnrichmentRecord:
    """The DataJud enrichment for one process — what the store persists."""
    process_number: str
    datajud_found: bool
    tribunal: Optional[str] = None
    grau: Optional[str] = None
    assuntos: Optional[List[Dict[str, Any]]] = None
    codigo_municipio_ibge: Optional[int] = None
    data_hora_ultima_atualizacao: Optional[str] = None
    data_hora_ultima_atualizacao_iso: Optional[str] = None
    movimentos_count: int = 0
    complementos: List[Complemento] = field(default_factory=list)
    raw_source_json: Optional[str] = None
    error: Optional[str] = None

    def canonical_payload(self) -> Dict[str, Any]:
        """Order-independent structure the store hashes for append-on-change.

        Includes dataHoraUltimaAtualizacao: a change there means DataJud
        re-ingested the case, which IS a meaningful enrichment to record."""
        comps = sorted(
            (
                [c.movimento_codigo, c.codigo, c.valor, c.nome, c.descricao]
                for c in self.complementos
            ),
            key=lambda x: [("" if v is None else str(v)) for v in x],
        )
        assuntos = sorted(
            ([a.get("codigo"), a.get("nome")] for a in (self.assuntos or [])),
            key=lambda x: [("" if v is None else str(v)) for v in x],
        )
        return {
            "found": self.datajud_found,
            "grau": self.grau,
            "ibge": self.codigo_municipio_ibge,
            "ultima_atualizacao": self.data_hora_ultima_atualizacao,
            "assuntos": assuntos,
            "complementos": comps,
        }


_FRAC_DIGITS = re.compile(r"(\d+)")


def parse_datajud_ts(s: Optional[str]) -> Optional[str]:
    """Parse DataJud's dataHoraUltimaAtualizacao into a normalized ISO string.

    Tolerates the variable fractional precision observed in L3L-a
    (`...687000Z` 6-digit vs `...734Z` 3-digit) and a missing fraction.
    Returns `YYYY-MM-DDTHH:MM:SS.ffffff+00:00`, or None if unparseable."""
    if not s or not isinstance(s, str):
        return None
    t = s.strip()
    if t.endswith("Z"):
        t = t[:-1]
    if "." in t:
        base, frac = t.split(".", 1)
        m = _FRAC_DIGITS.match(frac)
        frac_digits = (m.group(1) if m else "")[:6].ljust(6, "0")
        norm, fmt = f"{base}.{frac_digits}", "%Y-%m-%dT%H:%M:%S.%f"
    else:
        norm, fmt = t, "%Y-%m-%dT%H:%M:%S"
    try:
        dt = datetime.strptime(norm, fmt).replace(tzinfo=timezone.utc)
    except ValueError:
        return None
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%f+00:00")


def map_source_to_record(process_number: str, src: Dict[str, Any],
                         tribunal: str) -> EnrichmentRecord:
    """Map a DataJud `_source` (already `_source`-projected) to a record."""
    assuntos_raw = src.get("assuntos")
    assuntos: Optional[List[Dict[str, Any]]] = None
    if isinstance(assuntos_raw, list):
        assuntos = [
            {"codigo": a.get("codigo"), "nome": a.get("nome")}
            for a in assuntos_raw if isinstance(a, dict)
        ]

    orgao = src.get("orgaoJulgador")
    ibge = orgao.get("codigoMunicipioIBGE") if isinstance(orgao, dict) else None

    ult = src.get("dataHoraUltimaAtualizacao")

    complementos: List[Complemento] = []
    movimentos = src.get("movimentos") or []
    for m in movimentos:
        if not isinstance(m, dict):
            continue
        for ct in (m.get("complementosTabelados") or []):
            if isinstance(ct, dict):
                complementos.append(Complemento(
                    movimento_codigo=m.get("codigo"),
                    movimento_nome=m.get("nome"),
                    codigo=ct.get("codigo"),
                    valor=ct.get("valor"),
                    nome=ct.get("nome"),
                    descricao=ct.get("descricao"),
                ))

    return EnrichmentRecord(
        process_number=process_number,
        datajud_found=True,
        tribunal=tribunal,
        grau=src.get("grau"),
        assuntos=assuntos,
        codigo_municipio_ibge=ibge,
        data_hora_ultima_atualizacao=ult if isinstance(ult, str) else None,
        data_hora_ultima_atualizacao_iso=parse_datajud_ts(ult),
        movimentos_count=len(movimentos),
        complementos=complementos,
        raw_source_json=json.dumps(src, ensure_ascii=False, separators=(",", ":")),
    )


class DataJudEnricher:
    """Sequentially batches process numbers through `_msearch` and returns
    `EnrichmentRecord`s. No concurrent connections in v1 (the inventory's
    rate-limit caveat — parallel behavior is unprobed)."""

    def __init__(self, *, api_key: Optional[str] = None, index: str = DEFAULT_INDEX,
                 batch_size: int = 20, logger: Optional[logging.Logger] = None) -> None:
        self.logger = logger or setup_logging("datajud_enrichment")
        self.api_key = api_key or resolve_api_key(self.logger)
        self.index = index
        self.batch_size = max(1, batch_size)

    def enrich(self, process_numbers: Sequence[str]) -> List[EnrichmentRecord]:
        records: List[EnrichmentRecord] = []
        pns = list(process_numbers)
        for start in range(0, len(pns), self.batch_size):
            batch = pns[start:start + self.batch_size]
            res = msearch_by_process(
                batch, index=self.index, source_includes=SOURCE_INCLUDES,
                api_key=self.api_key, logger=self.logger,
            )
            responses = res.get("responses") or []
            if not res.get("ok") or len(responses) != len(batch):
                err = res.get("error") or (
                    f"_msearch returned {len(responses)} of {len(batch)} responses"
                )
                self.logger.warning("batch @%d failed: %s", start, err)
                for pn in batch:
                    records.append(EnrichmentRecord(
                        process_number=pn, datajud_found=False,
                        tribunal=self.index, error=err))
                continue
            for pn, sub in zip(batch, responses):
                records.append(self._map_subresponse(pn, sub))
            self.logger.info("batch @%d: %d processes in %d ms",
                             start, len(batch), res.get("latency_ms"))
        return records

    def _map_subresponse(self, pn: str, sub: Dict[str, Any]) -> EnrichmentRecord:
        try:
            status = sub.get("status")
            if status is not None and status != 200:
                return EnrichmentRecord(process_number=pn, datajud_found=False,
                                        tribunal=self.index,
                                        error=f"subresponse status {status}")
            hits = ((sub.get("hits") or {}).get("hits")) or []
            if not hits:
                return EnrichmentRecord(process_number=pn, datajud_found=False,
                                        tribunal=self.index)
            src = hits[0].get("_source") or {}
            return map_source_to_record(pn, src, tribunal=self.index)
        except Exception as e:  # noqa: BLE001 — per-process isolation
            return EnrichmentRecord(process_number=pn, datajud_found=False,
                                    tribunal=self.index,
                                    error=f"{type(e).__name__}: {e}")


def _loaded_process_numbers(db_path, limit: int) -> List[str]:
    """Read-only pull of latest-loaded process numbers (separate ro connection
    — WAL lets us read alongside the store's write connection)."""
    uri = f"file:{db_path.as_posix()}?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
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
    except sqlite3.OperationalError:
        return []
    finally:
        conn.close()


def enrich_and_store(
    process_numbers: Optional[Sequence[str]] = None,
    store: Optional[SnapshotStore] = None,
    *,
    index: str = DEFAULT_INDEX,
    batch_size: int = 20,
    limit_from_store: int = 8,
    api_key: Optional[str] = None,
    logger: Optional[logging.Logger] = None,
) -> Dict[str, Any]:
    """Fetch DataJud enrichment for a set of processes and persist each via the
    store's append-on-change path. Constructing a default store applies the v5
    migration to the live DB on first open.

    With no process_numbers, pulls `limit_from_store` loaded processes from the
    store — convenient for a first/ad-hoc batch.
    """
    logger = logger or setup_logging("datajud_enrichment")
    own_store = store is None
    if store is None:
        store = SnapshotStore(logger=logger)
    try:
        pns = list(process_numbers) if process_numbers else \
            _loaded_process_numbers(store.db_path, limit_from_store)
        if not process_numbers:
            logger.info("no process numbers given; pulled %d loaded from store", len(pns))

        enricher = DataJudEnricher(api_key=api_key, index=index,
                                   batch_size=batch_size, logger=logger)
        records = enricher.enrich(pns)

        summary: Dict[str, Any] = {
            "requested": len(pns), "found": 0, "not_found": 0, "errors": 0,
            "inserted": 0, "unchanged": 0, "complementos_written": 0,
            "schema_version": store.schema_version(), "per_process": [],
        }
        for rec in records:
            if rec.error:
                summary["errors"] += 1
            if rec.datajud_found:
                summary["found"] += 1
            else:
                summary["not_found"] += 1
            save = store.save_datajud_enrichment(rec)
            if save["inserted"]:
                summary["inserted"] += 1
                summary["complementos_written"] += len(rec.complementos)
            elif save["reason"] == "unchanged":
                summary["unchanged"] += 1
            summary["per_process"].append({
                "process_number": rec.process_number,
                "found": rec.datajud_found,
                "complementos": len(rec.complementos),
                "save": save["reason"],
                "error": rec.error,
            })
        summary["hit_rate"] = f"{summary['found']}/{summary['requested']}"
        return summary
    finally:
        if own_store:
            store.close()


def main() -> None:
    logger = setup_logging("datajud_enrichment")
    pns = [a for a in sys.argv[1:] if not a.startswith("-")]
    summary = enrich_and_store(process_numbers=pns or None, logger=logger)
    print(json.dumps(summary, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
