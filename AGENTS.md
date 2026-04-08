# Repository Instructions

## User Preferences

- Documents and comments must be written only in English.
- If there is another step that can improve progress, validation, testing, coverage, manual verification, or research, do it without asking for confirmation.
- Store experiments, research spikes, and their results in `spikes/*.md`.
- Store architecture decisions, primary strategy choices, and execution defaults in `SPECS.md`.
- Default product direction:
  - primary strategy: `strategies/incremental_refresh/exporter.py`
  - default profile: `--inventory-mode tree-only`
  - preferred fallback for exact line counts and full snapshot fidelity: `strategies/shallow_clone/exporter.py`
