"""Entry point: `python -m poursuite.datajud [process_number ...]`.

Runs a DataJud enrichment batch and prints a JSON summary. With no process
numbers, pulls a handful of loaded processes from the snapshot store.
"""
from poursuite.datajud.cli import main

if __name__ == "__main__":
    main()
