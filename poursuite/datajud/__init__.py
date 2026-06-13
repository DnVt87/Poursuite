"""poursuite.datajud — production DataJud public-API client + enrichment.

The reusable primitives here were lifted out of `poursuite.probes.datajud`
(investigation code) into the package so production paths don't import from
`probes/`. Scope is the public CNJ Elasticsearch API at
`api-publica.datajud.cnj.jus.br`.
"""
from poursuite.datajud.client import (
    DEFAULT_INDEX,
    digits_only,
    format_cnj,
    msearch_by_process,
    resolve_api_key,
)
from poursuite.datajud.enrichment import (
    SOURCE_INCLUDES,
    Complemento,
    DataJudEnricher,
    EnrichmentRecord,
    enrich_and_store,
)

__all__ = [
    "DEFAULT_INDEX",
    "digits_only",
    "format_cnj",
    "msearch_by_process",
    "resolve_api_key",
    "SOURCE_INCLUDES",
    "Complemento",
    "DataJudEnricher",
    "EnrichmentRecord",
    "enrich_and_store",
]
