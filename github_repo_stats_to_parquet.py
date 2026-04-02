#!/usr/bin/env python
"""Backward-compatible wrapper for the REST-first exporter."""

from exporters.core import main


if __name__ == "__main__":
    raise SystemExit(main())
