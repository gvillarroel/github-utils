#!/usr/bin/env python
"""Backward-compatible wrapper for the GraphQL-first exporter."""

from exporters.graphql_exporter import main


if __name__ == "__main__":
    raise SystemExit(main())
