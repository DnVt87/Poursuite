"""Concurrent-read regression for the snapshot store.

The store holds ONE SQLite connection; FastAPI runs sync endpoints in a
threadpool, so several read requests (e.g. Detalhe's Promise.all fan-out, or two
lawyers querying at once) hit that connection concurrently. Pre-fix this raised
`sqlite3.InterfaceError: bad parameter or other API misuse`; the RLock-serialized
read paths fix it.

This MUST be concurrent — sequential calls all return 200 and miss the bug
entirely (that is the nature of the race). Many threads start together on a
barrier and hammer a mix of reads; we assert zero worker exceptions.

Run:  .venv/Scripts/python.exe tests/smoke_concurrent_reads.py
"""
from __future__ import annotations

import sys
import tempfile
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

PN = "10000000020208260100"
THREADS = 16
ITERS = 40


def main() -> int:
    tmp = Path(tempfile.mkdtemp(prefix="poursuite_conc_"))
    import poursuite.db.esaj_snapshots as snap
    from poursuite.models import ProcessData
    from poursuite.datajud.enrichment import Complemento, EnrichmentRecord

    store = snap.SnapshotStore(tmp / "esaj_snapshots.db")
    # Seed a snapshot with child rows + a DataJud enrichment with a complemento,
    # so every read method has something to return (and a real cursor to open).
    store.save_snapshot(
        ProcessData(number=PN, class_type="Execução de Título Extrajudicial",
                    foro_code="0100", distribution_year="2020"),
        movimentos=[{"ordem": 1, "data_hora": "2020-01-01", "codigo": 26,
                     "nome": "Distribuição", "complementos_json": None,
                     "complementos_text": "x", "cd_documento": None}],
        linked=[{"linked_number": "20000000020208260100", "relationship_type": "apenso"}],
        peticoes=[{"ordem": 1, "data": "2020-01-02", "tipo": "Petição", "cd_documento": None}],
    )
    store.save_datajud_enrichment(EnrichmentRecord(
        process_number=PN, datajud_found=True, grau="G1",
        assuntos=[{"codigo": 1, "nome": "X"}], movimentos_count=1,
        complementos=[Complemento(0, "2020-01-01T00:00:00Z", 26, "Distribuição",
                                  2, 2, "sorteio", "tipo_de_distribuicao")],
    ))

    barrier = threading.Barrier(THREADS)
    errors: list = []

    def work():
        try:
            barrier.wait(timeout=10)
        except threading.BrokenBarrierError:
            pass
        for _ in range(ITERS):
            store.get_latest(PN)
            store.list_snapshots(PN)
            store.count_snapshots()
            store.get_movimentos(PN)
            store.get_linked(PN)
            store.get_peticoes(PN)
            store.query({"select": ["process_number"],
                         "where": {"field": "class_type", "op": "is_not_null"}})
            store.get_latest_enrichment(PN)
            store.get_complementos(PN)
            store.complemento_catalog()
            store.enrichment_status([PN, "0" * 20])
            store.snapshot_status([PN, "0" * 20])

    with ThreadPoolExecutor(max_workers=THREADS) as ex:
        futs = [ex.submit(work) for _ in range(THREADS)]
        for f in as_completed(futs):
            try:
                f.result()
            except Exception as e:  # noqa: BLE001
                errors.append(repr(e))
    store.close()

    if errors:
        print("FAIL: %d/%d workers raised. First: %s" % (len(errors), THREADS, errors[0]))
        return 1
    print("OK — %d threads x %d iters x 10 reads concurrent, no errors" % (THREADS, ITERS))
    return 0


if __name__ == "__main__":
    sys.exit(main())
