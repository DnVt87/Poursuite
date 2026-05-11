"""Entry shim so `python -m poursuite.probes ...` works."""
import sys

from poursuite.probes.cli import main

if __name__ == "__main__":
    sys.exit(main())
