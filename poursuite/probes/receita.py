"""Receita CNPJ probe.

v0 capabilities:
  - schema_check: parse poursuite/probes/cnpj_layout.txt and diff against
    EXPECTED_SCHEMA below (which encodes PROBE_FINDINGS.md §2.4). Produces a
    Markdown drift report. No download required.
  - sample_stub: placeholder for the bulk download path. Intentionally not
    wired to rictom/cnpj-sqlite yet — that integration is gated on a
    separate vendoring + license review step.

Reference: PROBE_FINDINGS.md §2 (the empirical questions are §2.8).
"""
import logging
import re
from collections import OrderedDict
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List

# Encoding of PROBE_FINDINGS.md §2.4 — what the schema-check expects to see.
EXPECTED_SCHEMA: "OrderedDict[str, List[str]]" = OrderedDict([
    ("empresas", [
        "cnpj_basico",
        "razao_social",
        "natureza_juridica",
        "qualificacao_responsavel",
        "capital_social",
        "porte_empresa",
        "ente_federativo_responsavel",
    ]),
    ("estabelecimentos", [
        "cnpj_basico",
        "cnpj_ordem",
        "cnpj_dv",
        "identificador_matriz_filial",
        "nome_fantasia",
        "situacao_cadastral",
        "data_situacao_cadastral",
        "motivo_situacao_cadastral",
        "nome_cidade_exterior",
        "pais",
        "data_inicio_atividade",
        "cnae_fiscal_principal",
        "cnae_fiscal_secundaria",
        "tipo_logradouro",
        "logradouro",
        "numero",
        "complemento",
        "bairro",
        "cep",
        "uf",
        "municipio",
        "ddd_1",
        "telefone_1",
        "ddd_2",
        "telefone_2",
        "ddd_fax",
        "fax",
        "correio_eletronico",
        "situacao_especial",
        "data_situacao_especial",
    ]),
    ("socios", [
        "cnpj_basico",
        "identificador_socio",
        "nome_socio",
        "cnpj_cpf_socio",
        "qualificacao_socio",
        "data_entrada_sociedade",
        "pais",
        "cpf_representante_legal",
        "nome_representante_legal",
        "qualificacao_representante_legal",
        "faixa_etaria",
    ]),
    ("dados_simples", [
        "cnpj_basico",
        "opcao_simples",
        "data_opcao_simples",
        "data_exclusao_simples",
        "opcao_mei",
        "data_opcao_mei",
        "data_exclusao_mei",
    ]),
])


def parse_layout_file(path: Path) -> Dict[str, List[str]]:
    """Parse cnpj_layout.txt into {table_name: [columns]}.

    Format:
        # comments
        [table_name]
        column_name
        ...
    Whitespace and blank lines ignored. Inline comment after a column name is dropped.
    """
    if not path.exists():
        return {}
    schema: Dict[str, List[str]] = {}
    current = None
    table_re = re.compile(r"^\[([a-zA-Z0-9_]+)\]$")
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        m = table_re.match(line)
        if m:
            current = m.group(1)
            schema[current] = []
            continue
        if current is None:
            continue
        # First whitespace-or-comment-separated token is the column name
        col = re.split(r"\s+|#", line, maxsplit=1)[0]
        if col:
            schema[current].append(col)
    return schema


def schema_check(layout_path: Path, run_dir: Path,
                 logger: logging.Logger) -> Path:
    observed = parse_layout_file(layout_path)
    md: List[str] = []
    md.append("# Receita CNPJ Schema Check")
    md.append(f"_Run: `{run_dir.name}`  |  Generated: {datetime.now(timezone.utc).isoformat()}_")
    md.append(f"_Layout source: `{layout_path}`_")
    md.append("")

    if not observed:
        md.append("⚠ **Layout file not found or empty.** Schema-check cannot run.")
        md.append("")
        md.append("Populate `poursuite/probes/cnpj_layout.txt` with the canonical "
                  "column lists from the latest `cnpj-metadados.pdf`, then re-run.")
        out = run_dir / "schema_diff_report.md"
        out.write_text("\n".join(md), encoding="utf-8")
        logger.warning("Layout file missing or empty: %s", layout_path)
        return out

    expected_tables = set(EXPECTED_SCHEMA.keys())
    observed_tables = set(observed.keys())

    md.append("## Tables")
    md.append(f"- expected: {', '.join('`' + t + '`' for t in EXPECTED_SCHEMA)}")
    md.append(f"- observed: {', '.join('`' + t + '`' for t in observed) or '(none)'}")
    missing_tables = sorted(expected_tables - observed_tables)
    extra_tables = sorted(observed_tables - expected_tables)
    if missing_tables:
        md.append(f"- ⚠ missing: {', '.join('`' + t + '`' for t in missing_tables)}")
    if extra_tables:
        md.append(f"- ⚠ extra (not in PROBE_FINDINGS §2.4): "
                  f"{', '.join('`' + t + '`' for t in extra_tables)}")
    md.append("")

    overall_clean = not missing_tables and not extra_tables

    for table, expected_cols in EXPECTED_SCHEMA.items():
        md.append(f"### `{table}`")
        if table not in observed:
            md.append("⚠ table not in observed layout.")
            md.append("")
            overall_clean = False
            continue
        obs_cols = observed[table]
        exp_set = set(expected_cols)
        obs_set = set(obs_cols)
        missing = [c for c in expected_cols if c not in obs_set]
        extra = [c for c in obs_cols if c not in exp_set]
        order_ok = (
            exp_set.issubset(obs_set)
            and len(obs_cols) >= len(expected_cols)
            and obs_cols[:len(expected_cols)] == expected_cols
        )
        md.append(f"- expected columns: {len(expected_cols)} | observed: {len(obs_cols)}")
        if missing:
            md.append(f"- ⚠ missing: {', '.join('`' + c + '`' for c in missing)}")
            overall_clean = False
        if extra:
            md.append(f"- ⚠ extra: {', '.join('`' + c + '`' for c in extra)}")
            overall_clean = False
        if not missing and not extra:
            order_note = " and order matches" if order_ok else " (column order differs)"
            md.append(f"- ✓ columns match exactly{order_note}")
        md.append("")

    md.append("## Summary")
    if overall_clean:
        md.append("✓ Layout file is consistent with PROBE_FINDINGS.md §2.4.")
        md.append("")
        md.append("**Note:** the bundled `cnpj_layout.txt` is a v0 baseline that "
                  "mirrors §2.4. To detect real drift between PROBE_FINDINGS and "
                  "the upstream `cnpj-metadados.pdf`, re-transcribe the layout "
                  "file from the PDF and re-run.")
    else:
        md.append("⚠ Drift detected. Reconcile `cnpj_layout.txt` with PROBE_FINDINGS §2.4 "
                  "(or update §2.4 if the PDF is now authoritative).")

    out = run_dir / "schema_diff_report.md"
    out.write_text("\n".join(md), encoding="utf-8")
    logger.info("Wrote %s", out)
    return out


def sample_stub(run_dir: Path, logger: logging.Logger) -> int:
    """Placeholder for the bulk-sample download. Returns non-zero exit code."""
    msg_path = run_dir / "sample_NOT_WIRED.md"
    msg_path.write_text(
        "# Receita Sample — Not Yet Wired\n\n"
        "The `receita sample` command requires integration with "
        "[`rictom/cnpj-sqlite`](https://github.com/rictom/cnpj-sqlite) "
        "(or `caiopizzol/cnpj-data-pipeline`) to fetch a 1-of-10 partition slice "
        "of the bulk Receita CNPJ dataset. This integration is intentionally "
        "deferred — it depends on:\n\n"
        "- vendoring (or pip-installing) the upstream tool with attribution + license review\n"
        "- a disk-space preflight (>=10 GB free at the run dir's filesystem)\n"
        "- choice of source snapshot (latest available)\n"
        "- resumable download wiring (Receita's WebDAV servers are flaky)\n\n"
        "Run `python -m poursuite.probes receita --schema-check` for the no-download "
        "portion of the probe.\n",
        encoding="utf-8",
    )
    logger.warning("Receita sample command is not yet wired; see %s", msg_path)
    return 2
