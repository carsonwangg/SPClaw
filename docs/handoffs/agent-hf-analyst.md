# Agent Handoff - Hedge Fund Analyst

## Branch
- `codex/agent-hf-analyst`

## Suggested Worktree
- `/Users/carsonwang/worktrees/spclaw/hf-analyst`

## Ownership Scope
- New HF analyst module(s) under `src/spclaw/`
- HF routing in `src/spclaw/slack_bot.py`
- HF command surface in `src/spclaw/cli.py`
- HF tests under `tests/`

## Validation
- Run targeted HF tests once created.
- Run routing regression tests when `slack_bot.py` changes.

## Merge Notes
- Rebase onto `origin/main` before merge.
- Do not restart runtime from this branch; integrator handles deploy on `main`.

## Current Ship Status (2026-02-24)
- HFA V1 implementation is complete in this branch:
  - `src/spclaw/hf_document_extract.py`
  - `src/spclaw/hf_prompt_contract.py`
  - `src/spclaw/hf_store.py`
  - `src/spclaw/hf_analyst.py`
  - CLI wiring in `src/spclaw/cli.py`
  - Slack wiring in `src/spclaw/slack_bot.py`
  - memory helper extension in `src/spclaw/memory_runtime.py`
- Tests added:
  - `tests/test_hf_document_extract.py`
  - `tests/test_hf_prompt_contract.py`
  - `tests/test_hf_analyst.py`
- Validation baseline:
  - `PYTHONPATH=src python3 -m pytest -q` -> `240 passed`

## Immediate Integrator Checklist
1. Merge `codex/agent-hf-analyst` into `main`.
2. Restart OpenClaw and verify Slack command path:
   - `hfa analyze` in a thread with uploaded docs.
3. Verify DM auto-run dedupe:
   - first upload auto-runs once; repeated events for same file set do not rerun.

## Current Ship Status (2026-02-26)
- HFA Podcast V1 is implemented on this branch:
  - Added `src/spclaw/hf_youtube_transcript.py` (YouTube parsing, captions-first transcript, ASR fallback orchestration).
  - Added `src/spclaw/hf_podcast.py` (summary + top-quote extraction/validation + markdown renderers).
  - Extended `src/spclaw/hf_store.py`:
    - `hf_runs.run_kind` (`thread_docs` / `podcast_youtube`)
    - `hf_podcast_inputs`
    - `hf_dm_podcast_autoruns` dedupe table.
  - Extended `src/spclaw/hf_analyst.py` with `analyze_podcast_url(...)`, YouTube URL extraction, and podcast DM dedupe helpers.
  - Extended `src/spclaw/cli.py` with `claw hfa podcast --url ... [--question ...] [--dry-run]`.
  - Extended `src/spclaw/slack_bot.py` with:
    - `hfa podcast <url> [question]` command path
    - DM YouTube auto-run + dedupe.
  - Extended `src/spclaw/memory_runtime.py` HFA writeback to accept source tags (podcast uses `hfa-podcast-analysis`).
- Tests added:
  - `tests/test_hf_podcast.py`
  - `tests/test_hf_youtube_transcript.py`
- Validation run:
  - `PYTHONPATH=src python3 -m pytest -q tests/test_hf_analyst.py tests/test_hf_podcast.py tests/test_hf_youtube_transcript.py tests/test_hf_document_extract.py tests/test_slack_routing.py` -> `26 passed`
  - `PYTHONPATH=src python3 -m compileall -q src` -> pass

## Patch Status (2026-02-26)
- ASR fallback compatibility patch added for podcast transcription:
  - `src/spclaw/hf_youtube_transcript.py` retries transcription without `response_format` when model/API rejects `verbose_json`.
  - fixes production failure mode on mini where ASR model rejects verbose response format.
- Regression coverage:
  - `tests/test_hf_youtube_transcript.py::test_asr_transcript_retries_without_response_format_on_incompatible_model`

## Patch Status (2026-02-27)
- HFA thread-doc mode now fails closed when model output is unavailable/unparseable.
- `src/spclaw/hf_analyst.py` no longer falls back to `_fallback_draft` in `analyze_thread`.
- Failure reason is propagated as `analysis_generation_failed:<reason>` to Slack/CLI paths.

## Patch Status (2026-02-27)
- Added memory-backed runtime HFA output controls (Slack configurable): strict/freeform mode + instruction text.
- Control commands are handled in `src/spclaw/slack_bot.py` via `hfa control ...`.

## Patch Status (2026-02-27)
- Simplified HFA output controls to one mode (`freeform`) with memory-backed instruction tuning.
- Strict mode is no longer an operator path.

## Patch Status (2026-02-27)
- Added implicit Slack parsing for HFA output-format change requests, with runtime memory writeback.

## Patch Status (2026-02-27)
- Added durable KB source-file copying for `hfa analyze` inputs.
