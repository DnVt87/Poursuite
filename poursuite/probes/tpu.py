"""Tabela Processual Unificada (TPU) — embedded subset.

DataJud responses include `nome` alongside `codigo`, so this lookup is used
only as a fallback for code-only references and to define families
(e.g. "is this a penhora event?"). Codes encountered in real responses are
captured directly from those responses.
"""
from typing import Dict, FrozenSet

# Per PROBE_FINDINGS.md §1.4
EMBEDDED_TPU: Dict[int, str] = {
    26: "Distribuição",
    51: "Recebimento",
    60: "Expedição de documento",
    11383: "Penhora",
    11382: "Bloqueio/penhora on line",
    12284: "Citação",
    22: "Baixa",
    246: "Arquivamento definitivo",
    11373: "Anulação de sentença/acórdão",
    11025: "Suspensão ou Sobrestamento",
    14702: "Incidente ou Cautelar — Procedimento Resolvido",
    12735: "Extinção da punibilidade",
}

PENHORA_CODES: FrozenSet[int] = frozenset({11382, 11383})
CITACAO_CODES: FrozenSet[int] = frozenset({12284})

PENHORA_NAME_PATTERNS = (
    "penhora",
    "bloqueio",
    "sisbajud",
    "bacenjud",
    "bacen-jud",
    "constrição",
    "constricao",
)


def lookup(codigo: int) -> str:
    return EMBEDDED_TPU.get(codigo, f"desconhecido ({codigo})")


def is_penhora(codigo: int, nome: str = "") -> bool:
    if codigo in PENHORA_CODES:
        return True
    if nome:
        nome_lc = nome.lower()
        return any(p in nome_lc for p in PENHORA_NAME_PATTERNS)
    return False


def is_citacao(codigo: int, nome: str = "") -> bool:
    if codigo in CITACAO_CODES:
        return True
    if nome:
        return "cita" in nome.lower()
    return False
