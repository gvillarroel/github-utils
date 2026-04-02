#!/usr/bin/env python
"""Backward-compatible wrapper for the hybrid exporter."""

from exporters.hybrid_exporter import main


if __name__ == "__main__":
    raise SystemExit(main())
