#!/usr/bin/env python3
from __future__ import annotations

import sys

from dataprocessor.cli import main as cli_main
from dataprocessor.interactive import main as interactive_main


def main() -> int:
    if len(sys.argv) == 1:
        interactive_main()
        return 0
    return cli_main(sys.argv[1:])


if __name__ == "__main__":
    raise SystemExit(main())
