-- eSAJ snapshot store schema (v1, Phase 2)
-- Owned by poursuite.db.esaj_snapshots.SnapshotStore.
--
-- Append-on-change semantics: each scrape computes a hash over
-- (header + movimentos + linked + peticoes). When the hash matches
-- the most-recent stored snapshot, the scrape is a no-op.
--
-- Hybrid storage on process_snapshot: 21 Phase-1 fields promoted to
-- dedicated columns for fast SQL filtering; full header_json kept
-- alongside for rare/conditional fields and forward compatibility.

-- ──────────────────────────────────────────────────────────────────────
-- process_snapshot: one row per (process_number, snapshot_ts).
-- ──────────────────────────────────────────────────────────────────────

CREATE TABLE process_snapshot (
    process_number     TEXT NOT NULL,
    snapshot_ts        TEXT NOT NULL,       -- ISO 8601 UTC, microsecond precision
    snapshot_hash      TEXT NOT NULL,       -- SHA-256 hex of canonical payload
    scraped_at         TEXT NOT NULL,       -- same as snapshot_ts at insert time
    scrape_outcome     TEXT NOT NULL,       -- 'loaded' | 'sealed' | 'not_found' | 'error'

    -- 21 promoted columns from ProcessData (excluding `number` -> process_number,
    -- and `error` -> scrape_error). Names match the dataclass field names.
    initial_date       TEXT,
    class_type         TEXT,
    subject            TEXT,
    value              TEXT,
    last_movement      TEXT,
    status             TEXT,
    plaintiff          TEXT,
    defendant          TEXT,
    other_processes    INTEGER,
    foro               TEXT,
    vara               TEXT,
    juiz               TEXT,
    controle           TEXT,
    outros_assuntos    TEXT,
    outros_numeros     TEXT,
    local_fisico       TEXT,
    area               TEXT,
    foro_code          TEXT,
    tribunal_code      TEXT,
    distribution_year  TEXT,
    scrape_error       TEXT,

    -- Raw JSON of the full ProcessData payload. Kept for rare/conditional
    -- fields and to future-proof against schema additions without losing data.
    header_json        TEXT NOT NULL,

    PRIMARY KEY (process_number, snapshot_ts)
);

CREATE INDEX idx_process_snapshot_latest
    ON process_snapshot(process_number, snapshot_ts DESC);

CREATE INDEX idx_process_snapshot_hash
    ON process_snapshot(process_number, snapshot_hash);

-- Common-filter indexes for the search interface (light, can grow).
CREATE INDEX idx_process_snapshot_class_type
    ON process_snapshot(class_type) WHERE class_type IS NOT NULL;

CREATE INDEX idx_process_snapshot_foro_code
    ON process_snapshot(foro_code) WHERE foro_code IS NOT NULL;

CREATE INDEX idx_process_snapshot_distribution_year
    ON process_snapshot(distribution_year) WHERE distribution_year IS NOT NULL;


-- ──────────────────────────────────────────────────────────────────────
-- movimento: one row per movimento per snapshot.
-- ──────────────────────────────────────────────────────────────────────

CREATE TABLE movimento (
    process_number     TEXT NOT NULL,
    snapshot_ts        TEXT NOT NULL,
    ordem              INTEGER NOT NULL,    -- 0-based position in the timeline as scraped
    data_hora          TEXT,                -- ISO 8601 if parseable; raw text otherwise
    codigo             INTEGER,             -- TPU code if visible (NULL allowed; many cases lack it)
    nome               TEXT NOT NULL,       -- movimento label as displayed
    complementos_json  TEXT,                -- structured complementos (parsed nested rows)
    complementos_text  TEXT,                -- flattened text for FTS

    PRIMARY KEY (process_number, snapshot_ts, ordem),
    FOREIGN KEY (process_number, snapshot_ts)
        REFERENCES process_snapshot(process_number, snapshot_ts)
        ON DELETE CASCADE
);

CREATE INDEX idx_movimento_lookup
    ON movimento(process_number, snapshot_ts);

CREATE INDEX idx_movimento_codigo
    ON movimento(codigo) WHERE codigo IS NOT NULL;

CREATE INDEX idx_movimento_data
    ON movimento(data_hora) WHERE data_hora IS NOT NULL;

-- FTS5 contentless-ish index — uses external content so we don't duplicate
-- the data. Triggers below keep it in sync with INSERT/UPDATE/DELETE on movimento.
CREATE VIRTUAL TABLE movimento_fts USING fts5(
    nome,
    complementos_text,
    content='movimento',
    content_rowid='rowid'
);

CREATE TRIGGER movimento_fts_after_insert AFTER INSERT ON movimento BEGIN
    INSERT INTO movimento_fts(rowid, nome, complementos_text)
    VALUES (new.rowid, new.nome, new.complementos_text);
END;

CREATE TRIGGER movimento_fts_after_delete AFTER DELETE ON movimento BEGIN
    INSERT INTO movimento_fts(movimento_fts, rowid, nome, complementos_text)
    VALUES ('delete', old.rowid, old.nome, old.complementos_text);
END;

CREATE TRIGGER movimento_fts_after_update AFTER UPDATE ON movimento BEGIN
    INSERT INTO movimento_fts(movimento_fts, rowid, nome, complementos_text)
    VALUES ('delete', old.rowid, old.nome, old.complementos_text);
    INSERT INTO movimento_fts(rowid, nome, complementos_text)
    VALUES (new.rowid, new.nome, new.complementos_text);
END;


-- ──────────────────────────────────────────────────────────────────────
-- linked_process: apensos, incidentes, dependentes, embargos.
-- relationship_type is free text; canonical values surfaced from eSAJ
-- section headers during scraping. Documented values:
--   'apenso', 'entranhado', 'unificado'  (from "Apensos, Entranhados e Unificados")
--   'incidente', 'recurso', 'execucao_sentenca'  (from "Incidentes, ações
--    incidentais, recursos e execuções de sentenças")
-- 2d will refine; column accepts any TEXT for flexibility.
-- ──────────────────────────────────────────────────────────────────────

CREATE TABLE linked_process (
    process_number     TEXT NOT NULL,
    snapshot_ts        TEXT NOT NULL,
    linked_number      TEXT NOT NULL,
    relationship_type  TEXT NOT NULL,

    PRIMARY KEY (process_number, snapshot_ts, linked_number, relationship_type),
    FOREIGN KEY (process_number, snapshot_ts)
        REFERENCES process_snapshot(process_number, snapshot_ts)
        ON DELETE CASCADE
);

CREATE INDEX idx_linked_process_lookup
    ON linked_process(process_number, snapshot_ts);

-- Reverse-lookup index: "what processes link to X?"
CREATE INDEX idx_linked_process_reverse
    ON linked_process(linked_number);


-- ──────────────────────────────────────────────────────────────────────
-- peticao: shallow petição metadata + cdDocumento reference for deep-search.
-- ──────────────────────────────────────────────────────────────────────

CREATE TABLE peticao (
    process_number     TEXT NOT NULL,
    snapshot_ts        TEXT NOT NULL,
    ordem              INTEGER NOT NULL,
    data               TEXT,                -- raw date as displayed
    tipo               TEXT,                -- petição type/category
    cd_documento       TEXT,                -- eSAJ document ID for deep-search

    PRIMARY KEY (process_number, snapshot_ts, ordem),
    FOREIGN KEY (process_number, snapshot_ts)
        REFERENCES process_snapshot(process_number, snapshot_ts)
        ON DELETE CASCADE
);

CREATE INDEX idx_peticao_lookup
    ON peticao(process_number, snapshot_ts);

CREATE INDEX idx_peticao_cd_documento
    ON peticao(cd_documento) WHERE cd_documento IS NOT NULL;


-- ──────────────────────────────────────────────────────────────────────
-- documents: stub for Phase 3 deep-search.
-- Empty by default. Schema present so Phase 2 callers can reference it
-- (e.g. presence checks). Populated only when a lawyer triggers deep-search.
-- last_accessed_at supports eviction of cold rows.
-- ──────────────────────────────────────────────────────────────────────

CREATE TABLE documents (
    process_number     TEXT NOT NULL,
    cd_documento       TEXT NOT NULL,
    nome_recurso       TEXT,                -- human-readable name as captured at deep-search time
    text_compressed    BLOB,                -- zlib-compressed extracted text
    text_length        INTEGER,             -- uncompressed text length, in bytes
    ingested_at        TEXT,
    last_accessed_at   TEXT,                -- updated on read; supports LRU eviction
    PRIMARY KEY (process_number, cd_documento)
);

CREATE INDEX idx_documents_lru
    ON documents(last_accessed_at) WHERE text_compressed IS NOT NULL;
