# Coatue Claw - Current Plan (OpenClaw Native)

## Objective
Build a 24/7 equity research bot (Slack-first) that runs natively on OpenClaw as the primary runtime and control plane.

## V1 Scope
- SEC + transcript + macro ingestion
- Diligence packets (bull/bear + peer comp + charts)
- Weekly idea scan
- X-only digest (digest-first)
- Memory layer (SQLite + LanceDB + thesis notes)

## Platform Target
- Repo: GitHub (`CoatueClaw`)
- Runtime: OpenClaw-native workflows and agents
- Dev machine: Mac mini (local dev + fallback runtime only)
- Control: laptop via OpenClaw
- Runtime data dirs: `/opt/coatue-claw-data/{db,cache,logs,artifacts,backups}`

## Delivery Phases
1. OpenClaw Foundation
- Define OpenClaw execution model (entrypoints, long-running jobs, scheduled jobs)
- Define secrets model for Slack/OpenAI keys in OpenClaw
- Define logging/alerts and incident visibility in OpenClaw
- Define artifact persistence paths and retention

2. Runtime Integration
- Wire Slack bot into OpenClaw process model
- Validate mention events, replies, and retries end-to-end
- Add health checks and restart policy

3. Product Core
- Implement real diligence pipeline (replace template output)
- Implement ingestion jobs (SEC/transcripts/macro)
- Implement memory layer writes/reads

4. Product Loops
- Weekly idea scan automation
- X-only digest generation + posting path
- Operator workflows for review/approval

## Current Status
- Chart-day splitter fix shipped on role branch:
  - removed sentence-splitting behavior from `_extract_first_sentence` in `src/coatue_claw/x_chart_daily.py`.
  - goal: avoid abbreviation truncation (`U.S.`) that produced fragment headlines/takeaways.
  - validation: `PYTHONPATH=src python3 -m pytest -q tests/test_x_chart_daily.py` -> `85 passed`.
- Market Daily LLM grounding upgrade shipped on role branch:
  - simple-synthesis relevance selection + one-line drafting now use richer article-body context for top candidates (not headline/snippet only).
  - added per-run controls for context enrichment:
    - `COATUE_CLAW_MD_ARTICLE_CONTEXT_ENABLED`
    - `COATUE_CLAW_MD_ARTICLE_CONTEXT_TIMEOUT_MS`
    - `COATUE_CLAW_MD_ARTICLE_CONTEXT_MAX_CHARS`
    - `COATUE_CLAW_MD_ARTICLE_CONTEXT_LIMIT`
  - coverage:
    - `tests/test_market_daily.py::test_extract_article_context_from_html_prefers_body_text`
    - `tests/test_market_daily.py::test_evidence_context_for_llm_includes_article_body`
- Market Daily LLM-first relevance mode shipped on role branch:
  - new `COATUE_CLAW_MD_RELEVANCE_MODE` control (`llm_first` default, `deterministic` fallback).
  - in simple-synthesis mode, model now selects anchor/support evidence IDs before writing the catalyst sentence.
  - deterministic consensus/anchor logic remains as automatic fallback if relevance selection fails.
  - regression coverage:
    - `tests/test_market_daily.py::test_relevance_mode_defaults_to_llm_first`
    - `tests/test_market_daily.py::test_select_anchor_support_llm_parses_json`
    - `tests/test_market_daily.py::test_llm_first_relevance_anchor_is_used`
- Market Daily catalyst ranking quality hardening shipped on role branch:
  - added price-action-only text detector to penalize non-causal “intraday momentum/trading” blurbs.
  - anchor chooser now prefers true causal explainers, including analyst upgrade/downgrade narratives.
  - deterministic fallback gate now excludes price-action-only candidates from specific-line generation.
  - regression coverage:
    - `tests/test_market_daily.py::test_price_action_only_penalty_lowers_rank_vs_causal_upgrade`
    - `tests/test_market_daily.py::test_anchor_picker_prefers_upgrade_explainer_over_price_action`
- Market Daily earnings recap dedupe fix shipped on role branch:
  - manual recap runs outside the scheduled window now record under `earnings_recap_manual`.
  - scheduled nightly recap keeps `earnings_recap`.
  - this prevents daytime manual/test runs from blocking the 7:00 PM scheduled recap via same-slot dedupe.
  - regression test:
    - `tests/test_market_daily.py::test_run_earnings_recap_manual_daytime_does_not_block_scheduled_slot`
- Board Seat has been reset to a scaffold baseline to restart from scratch:
  - `src/coatue_claw/board_seat_daily.py` no longer runs legacy drafting/quality logic.
  - default behavior is hard skip with `feature_reset_in_progress`.
  - CLI/ops commands are still present so launchd and make targets remain stable.
  - funding subcommands are temporary `not_implemented` placeholders during rebuild.
  - target memory/ledger primitives remain available for continuity.
- Rebuild target for next phase:
  - implement a clean v1 candidate-first architecture with minimal mandatory gates.
  - keep reset mode on until new writer pipeline passes canary quality checks.
- Board-seat writing fix v3 shipped in `src/coatue_claw/board_seat_daily.py`:
  - passthrough-biased drafting replaced by strict synthesis defaults.
  - defaults now prioritize quality-safe output:
    - `writing_mode=synthetic_strict`
    - `quality_mode=strict`
    - `delivery_mode=diagnostic_fallback`
    - tighter copy guard threshold (`quote_overlap_max=0.28`)
  - strict-mode hard-fail now includes:
    - quote-like overlap
    - near-duplicate thesis sections
    - selected-target/final-target inconsistency
  - candidate extraction noise rejection added (all-caps/slogan fragments blocked before selection pool).
  - added synthesis observability fields in run payload:
    - `synthesis_enforced`
    - `copy_guard_triggered_fields`
    - `candidate_noise_rejections`
    - `target_selection_consistent`
  - new env knobs:
    - `COATUE_CLAW_BOARD_SEAT_SYNTH_MIN_FIELD_SCORE=0.72`
    - `COATUE_CLAW_BOARD_SEAT_SYNTH_REWRITE_MAX=3`
  - validation:
    - `PYTHONPATH=src python3 -m pytest -q tests/test_board_seat_daily.py` -> `79 passed`
    - `PYTHONPATH=src python3 -m pytest -q tests/test_launchd_runtime.py` -> `8 passed`
- Board-seat recovery v2 shipped in `src/coatue_claw/board_seat_daily.py`:
  - candidate-first selection flow added (multi-target pool, per-target evidence, LLM winner selection with deterministic fallback).
  - default delivery now posts best cleaned draft (`delivery_mode=post`) with light, advisory quality warnings.
  - hard gates now centered on:
    - company-only target validity
    - strict cooldown repeat prevention via target memory lock window
  - confidence/new-target gate defaults relaxed (`COATUE_CLAW_BOARD_SEAT_REQUIRE_HIGH_CONF_NEW_TARGET=0`) while cooldown hard-block remains enforced.
  - payload diagnostics now include candidate selection details + quality warnings + hard gate metadata.
  - validation:
    - `PYTHONPATH=src python3 -m pytest -q tests/test_board_seat_daily.py` -> `79 passed`
    - `PYTHONPATH=src python3 -m pytest -q tests/test_launchd_runtime.py` -> `8 passed`
- Board-seat why-now relaxation shipped in `src/coatue_claw/board_seat_daily.py`:
  - `why_now` is now thematic and non-blocking by default:
    - `COATUE_CLAW_BOARD_SEAT_WHY_NOW_MODE=thematic_non_blocking`
    - `COATUE_CLAW_BOARD_SEAT_WHY_NOW_THEME_WINDOW_DAYS=120`
  - sparse dated evidence for `why_now` now produces soft notes + optional fallback synthesis instead of blocking the post path.
  - payload additions now emitted on sent/skipped rows:
    - `why_now_mode`
    - `why_now_theme_window_days`
    - `why_now_generated_fallback`
    - `why_now_soft_notes`
  - quality metrics/status now include `why_now_soft_notes_count_7d`.
  - target/acquisition retrieval now merges Brave + Google rows for broader context before drafting.
- Board-seat diagnostic fallback observability patch shipped in `src/coatue_claw/board_seat_daily.py`:
  - fixed field propagation across retarget/repitch copy paths so quality observability survives draft rewrites.
  - ensured payload defaults always include full maps for:
    - `fact_cards_count_by_field`
    - `quote_overlap_by_field`
  - quality gate payload and target gate payload now consistently return non-empty keyed maps (`target_does`, `why_now`, `whats_different`, `mos_risks`).
  - validation:
    - `PYTHONPATH=src python3 -m pytest -q tests/test_board_seat_daily.py` -> `75 passed`
    - `PYTHONPATH=src python3 -m pytest -q tests/test_launchd_runtime.py` -> `8 passed`
  - deployed on Mac mini (`/opt/coatue-claw`) at commit `67934bf`; canary runs verified payload maps are populated in diagnostic outputs.
- Board-seat output recovery shipped in `src/coatue_claw/board_seat_daily.py` with fact-cards + diagnostic fallback:
  - added delivery and anti-copy controls:
    - `COATUE_CLAW_BOARD_SEAT_DELIVERY_MODE=diagnostic_fallback`
    - `COATUE_CLAW_BOARD_SEAT_FACT_CARD_MODE=always`
    - `COATUE_CLAW_BOARD_SEAT_QUOTE_OVERLAP_MAX=0.22`
    - `COATUE_CLAW_BOARD_SEAT_DIAGNOSTIC_MAX_REASONS=4`
    - `COATUE_CLAW_BOARD_SEAT_DIAGNOSTIC_INCLUDE_URLS=1`
  - writer now receives fact-card evidence bundles (not raw snippet-heavy context) and enforces synthesized wording.
  - deterministic quality checks now include quote-overlap by field; high-overlap fields are blocked.
  - run payload now includes:
    - `delivery_mode_applied`
    - `quality_blocked`
    - `quality_failure_codes`
    - `fact_cards_count_by_field`
    - `quote_overlap_by_field`
  - quality failure routing:
    - `skip` mode: skip with `reason=quality_gate_failed`
    - `diagnostic_fallback` mode: post compact diagnostic in same channel, no bad thesis prose
    - `post` mode: legacy compatibility path
  - diagnostic posts are persisted in pitch history with `source=quality_diagnostic_post`.
  - status/quality metrics now include:
    - `diagnostic_fallback_count_7d`
    - `top_quality_failure_codes_7d`
    - `quote_overlap_violations_7d`
    - `fact_card_coverage_7d`
  - validation:
    - `PYTHONPATH=src python3 -m pytest -q tests/test_board_seat_daily.py` -> `73 passed`
    - `PYTHONPATH=src python3 -m pytest -q tests/test_launchd_runtime.py` -> `8 passed`
- Board-seat Research+Critic strict fail-closed pipeline shipped in `src/coatue_claw/board_seat_daily.py`:
  - source policy default is now `tiered_trusted_first`.
  - section-aware evidence bundles are generated and passed to writer/critic:
    - `target_does_evidence`
    - `why_now_evidence`
    - `whats_different_evidence`
    - `mos_risks_evidence`
  - semantic Why-now recency gate added (`COATUE_CLAW_BOARD_SEAT_WHY_NOW_RECENCY_DAYS=45` default) with deterministic evidence alignment checks.
  - critic thresholds and loop added:
    - `COATUE_CLAW_BOARD_SEAT_CRITIC_MIN_FIELD_SCORE=0.70`
    - `COATUE_CLAW_BOARD_SEAT_CRITIC_MIN_OVERALL_SCORE=0.78`
    - reviewer uses `COATUE_CLAW_BOARD_SEAT_REVIEW_MODEL` (same top model default).
  - evidence fetch knobs added:
    - `COATUE_CLAW_BOARD_SEAT_EVIDENCE_FETCH_ENABLED=1`
    - `COATUE_CLAW_BOARD_SEAT_EVIDENCE_FETCH_TIMEOUT_MS=2500`
    - `COATUE_CLAW_BOARD_SEAT_EVIDENCE_MAX_URLS=12`
  - fail-closed posting remains enforced with `COATUE_CLAW_BOARD_SEAT_QUALITY_FAIL_POLICY=skip`.
  - run payload contract now includes:
    - `quality_field_scores`
    - `quality_failed_fields`
    - `quality_required_evidence`
    - `evidence_tier_mix`
    - `why_now_recency_passed`
  - status contract now includes:
    - `quality_pass_rate_7d`
    - `top_failed_fields_7d`
    - `avg_rewrite_attempts_7d`
  - validation:
    - `PYTHONPATH=src python3 -m pytest -q tests/test_board_seat_daily.py` -> `71 passed`
    - `PYTHONPATH=src python3 -m pytest -q tests/test_launchd_runtime.py` -> `8 passed`
- Board-seat fail-closed quality gate + auto-revision is now implemented in `src/coatue_claw/board_seat_daily.py`:
  - new env controls (defaulted on):
    - `COATUE_CLAW_BOARD_SEAT_QUALITY_GATE_ENABLED=1`
    - `COATUE_CLAW_BOARD_SEAT_REWRITE_MAX_RETRIES=4`
    - `COATUE_CLAW_BOARD_SEAT_SOURCE_GATE_MODE=soft_block`
    - `COATUE_CLAW_BOARD_SEAT_QUALITY_FAIL_POLICY=skip`
    - `COATUE_CLAW_BOARD_SEAT_REVIEW_MODEL` (defaults to generation model)
  - source hygiene expanded with low-signal CTA/menu filtering and soft demotion in evidence selection.
  - draft lifecycle now uses quality assess + reviewer rewrite loop before any post.
  - fallback drafts are quality-gated as well; no fail-open path remains.
  - on unrecoverable quality failure, run skips post with `reason=quality_gate_failed`.
  - run/status observability now includes:
    - `quality_gate_passed`, `quality_score`, `quality_reasons`, `rewrite_attempts`, `quality_fail_stage`
    - `last_quality_run_metrics` in `status`.
  - validation:
    - `PYTHONPATH=src python3 -m pytest -q tests/test_board_seat_daily.py` -> `68 passed`
    - `PYTHONPATH=src python3 -m pytest -q tests/test_launchd_runtime.py` -> `8 passed`
- Board-seat candidate quality recovery shipped on `codex/agent-board-seat`:
  - confidence model now defaults to `broad_weighted_v1` (deterministic weighted scoring over top target evidence), replacing allowlist-heavy gating behavior.
  - new confidence env knobs:
    - `COATUE_CLAW_BOARD_SEAT_CONFIDENCE_MODEL=broad_weighted_v1`
    - `COATUE_CLAW_BOARD_SEAT_CONFIDENCE_HIGH_MIN=2.40`
    - `COATUE_CLAW_BOARD_SEAT_CONFIDENCE_MEDIUM_MIN=1.35`
    - `COATUE_CLAW_BOARD_SEAT_ALLOW_MEDIUM_NEW_TARGET=1`
  - gate behavior now allows **new + Medium/High** target confidence (still requires new target; 14-day no-repeat unchanged).
  - conceptual target filtering expanded (`LLMs`, `ROI`, `workflow`, `platform`, etc.) and applied across:
    - `_is_valid_target_name`
    - `_is_valid_acquisition_idea_line`
    - `_target_candidates_from_seed`
  - run payload observability expanded for target gating diagnostics:
    - `target_confidence_score`
    - `target_confidence_reasons`
    - `target_validation_reason`
  - no Slack command/interface changes.
- Board-seat API-key compatibility + health diagnosis update:
  - `_brave_search_api_key()` now reads both `COATUE_CLAW_BRAVE_API_KEY` and `BRAVE_SEARCH_API_KEY`.
  - `.env.example` now documents `COATUE_CLAW_BRAVE_API_KEY`.
  - regression test added:
    - `tests/test_board_seat_daily.py::test_brave_api_key_accepts_coatue_claw_alias`
  - environment diagnosis during live checks:
    - Brave path now active with provided key alias
    - SerpAPI currently returns HTTP `429` (rate-limited), so Google rows are empty until quota resets/plan changes
  - validation:
    - `PYTHONPATH=src python3 -m pytest -q tests/test_board_seat_daily.py` -> `46 passed`
    - `PYTHONPATH=src python3 -m pytest -q` -> `310 passed`
- Board-seat posting now enforces strict new-target quality gate across all active portcos:
  - skip post unless target is both **new** (not already in target memory for that company) and **High confidence** from source scoring.
  - skip reason contract:
    - `reason=no_high_confidence_new_target`
    - `gate_reason` in `{invalid_target,target_not_new,target_confidence_not_high}`
  - env control added (default enabled):
    - `COATUE_CLAW_BOARD_SEAT_REQUIRE_HIGH_CONF_NEW_TARGET=1`
  - status payload now includes:
    - `require_high_conf_new_target`
  - validation:
    - `PYTHONPATH=src python3 -m pytest -q tests/test_board_seat_daily.py` -> `45 passed`
    - `PYTHONPATH=src python3 -m pytest -q` -> `309 passed`
- Board-seat target hardening shipped to block conceptual/non-company targets (for example `AI-first`, `ROI`) and possessive/pluralized self-target leakage (for example `OpenAIs` for OpenAI):
  - updated target filters in `src/coatue_claw/board_seat_daily.py` (`ACQ_PLACEHOLDER_TARGETS`, `ACQ_INVALID_TARGET_TERMS`, `TARGET_TOKEN_STOPWORDS`, `_canonical_target_key(...)`).
  - regression tests added in `tests/test_board_seat_daily.py`:
    - `test_is_valid_target_name_rejects_ai_first_placeholder`
    - `test_is_valid_target_name_rejects_possessive_company_variant`
    - `test_is_valid_target_name_rejects_metric_token`
  - validation:
    - `PYTHONPATH=src python3 -m pytest -q tests/test_board_seat_daily.py` -> `42 passed`
    - `PYTHONPATH=src python3 -m pytest -q` -> `306 passed`
- Board-seat sqlite connection lifecycle fix shipped for ledger FD stability:
  - `BoardSeatStore._connect()` now commits/rolls back and always closes sqlite connections.
  - this resolves descriptor accumulation that surfaced as `[Errno 24] Too many open files` in board-seat ledger export path.
  - regression test added:
    - `tests/test_board_seat_daily.py::test_store_connect_context_closes_connection`
  - validation:
    - `PYTHONPATH=src python3 -m pytest -q tests/test_board_seat_daily.py` -> `38 passed`
    - `PYTHONPATH=src python3 -m pytest -q` -> `257 passed`
  - deployed/verified on Mac mini:
    - `/opt/coatue-claw` updated to `6a62458`
    - board-seat dry-run no longer emits `[Errno 24] Too many open files` in `ledger`
    - ledger payload includes expected CSV/JSON/mirror output paths
    - Slack probe healthy after restart (`ok=true`)
- Board Seat funding hardening + strict repitch governance merged/deployed:
  - merged commit: `fd8a942` (`Merge board-seat funding hardening and strict repitch governance`)
  - merged-tree validation:
    - `PYTHONPATH=src python3 -m pytest -q` -> `256 passed`
  - Mac mini runtime verification:
    - `make openclaw-restart`
    - `make openclaw-slack-status` (`probe.ok=true`)
    - `make openclaw-board-seat-status` shows `target_lock_days=14` and funding verification payloads
    - `make openclaw-board-seat-refresh-funding` refreshed 23 entities
    - `make openclaw-board-seat-funding-report` wrote `/opt/coatue-claw-data/artifacts/board-seat/funding-quality-report-2026-02-24.md`
    - `make openclaw-board-seat-run-once DRY_RUN=1 FORCE=1` confirms strict repitch skip paths are active
  - follow-up note:
    - dry-run payload surfaced non-fatal ledger write warning (`[Errno 24] Too many open files`) in board-seat ledger export path.
- launchd 24x7 enable path hardened for transient bootstrap errors:
  - `src/coatue_claw/launchd_runtime.py` now retries transient launchctl bootstrap `Input/output error` failures during `enable`.
  - retry control env added: `COATUE_CLAW_LAUNCHCTL_BOOTSTRAP_RETRIES` (default `3`).
  - enable failure messages now include the exact failing label (`failed enabling <label>: ...`) for faster operator triage.
  - regression coverage added in `tests/test_launchd_runtime.py` for retry behavior + label-specific error context.
  - validation:
    - `PYTHONPATH=src python3 -m pytest -q tests/test_launchd_runtime.py` -> `8 passed`
    - `PYTHONPATH=src python3 -m pytest -q tests/test_market_daily.py tests/test_launchd_runtime.py` -> `44 passed`
  - deployed/verified on Mac mini:
    - `/opt/coatue-claw` updated to `da144cd`
    - `make openclaw-24x7-enable` now succeeds
    - `make openclaw-24x7-status` confirms `com.coatueclaw.market-daily` + `com.coatueclaw.market-daily-earnings-recap` loaded
    - `make openclaw-slack-status` probe healthy (`ok=true`)
- HFA V1 (docs-first hedge fund analyst workflow) is implemented on `codex/agent-hf-analyst`:
  - new modules:
    - `src/coatue_claw/hf_document_extract.py`
    - `src/coatue_claw/hf_prompt_contract.py`
    - `src/coatue_claw/hf_store.py`
    - `src/coatue_claw/hf_analyst.py`
  - CLI:
    - `claw hfa analyze --channel <id> --thread-ts <ts> [--question "..."] [--dry-run]`
    - `claw hfa status [--channel <id>] [--thread-ts <ts>]`
  - Slack:
    - explicit `hfa analyze` and `hfa status`
    - DM auto-run for new file sets only (`hf_dm_autoruns` dedupe)
  - runtime persistence:
    - DB: `/opt/coatue-claw-data/db/hf_analyst.sqlite`
    - artifact dir: `/opt/coatue-claw-data/artifacts/hf-analyst`
  - memory writeback:
    - structured thesis/catalyst/risk/score/artifact pointer via `MemoryRuntime.ingest_hfa_facts(...)`
  - dependency adds:
    - `pypdf`, `python-docx`, `python-pptx`
  - validation:
    - `PYTHONPATH=src python3 -m pytest -q` -> `240 passed`
- MD catalyst + link relevance hardening shipped (AMD/INTC regression fix):
  - X evidence now rejects low-signal promo/cashtag spam before candidate scoring.
  - Catalyst selection now supports deterministic direct-evidence fallback (quality Yahoo/Web) when cluster confirmation fails but headline is clearly causal.
  - Link rendering is cause-aware:
    - fallback line suppresses `[X]`
    - fallback retains only quality `[News]/[Web]`
    - specific lines include `[X]` only when stricter relevance checks pass.
  - `CatalystEvidence` now records cause trace fields:
    - `cause_mode` (`cluster_confirmed|decisive_primary|direct_evidence|fallback`)
    - `cause_source_type`
    - `cause_source_url`
  - debug/artifact output now exposes cause tracing fields for quicker diagnosis.
  - validation:
    - `PYTHONPATH=src python3 -m pytest -q tests/test_market_daily.py` -> `40 passed`
    - `PYTHONPATH=src python3 -m pytest -q` -> `238 passed`
- Market Daily earnings expansion shipped in `codex/agent-market-daily` worktree:
  - Morning `open` MD post now appends `Earnings After Close Today` for same-day after-close names detected from final MD universe (`top-K + Coatue overlay + overrides`).
  - New nightly MD recap path added at `19:00` local runtime by default:
    - `python -m coatue_claw.market_daily run-earnings-recap`
    - deduped per local day via `slot_name=earnings_recap`
    - scope: any final-universe name with same-day earnings report signal
    - ranking: top 4 by absolute move since regular close
    - output: 2-4 bullets per ticker (LLM synthesis with deterministic fallback)
  - Slack/CLI/runtime wiring:
    - Slack: `md earnings now`, `md earnings now force`
    - CLI: `claw market-daily run-earnings-recap --manual|--force|--dry-run`
    - launchd label: `com.coatueclaw.market-daily-earnings-recap`
    - env: `COATUE_CLAW_MD_EARNINGS_RECAP_TIME` (default `19:00`)
    - Make target: `openclaw-market-daily-earnings-recap-run-once`
  - validation:
    - `PYTHONPATH=src python3 -m pytest -q tests/test_market_daily.py tests/test_launchd_runtime.py` -> `42 passed`
- Board Seat repitch governance is now strict and Spencer-aligned:
  - hard no-repeat rule for same target within 14 days (non-bypassable).
  - default target lock is now 14 days, with minimum enforced at 14.
  - repeated idea resurfacing after lock now requires exceptional, event-backed significance thresholds (critical bias against repeats).
  - when resurfacing is allowed, outbound message must include explicit:
    - `Repitch note`
    - `New evidence`
  - new audit/storage tables:
    - `board_seat_target_events` (continuous promising-target event tracking)
    - `board_seat_repitch_assessments` (allow/reject decisions + evidence snapshots)
  - `board_seat_pitches` now persists repitch linkage metadata (`is_repitch`, prior pitch pointer, evidence summary).
  - validation:
    - `PYTHONPATH=src python3 -m pytest -q tests/test_board_seat_daily.py` -> `37 passed`
    - `PYTHONPATH=src python3 -m pytest -q` -> `236 passed`
- Board Seat funding accuracy hardening is now shipped on `codex/agent-board-seat` (web-first, warning-mode, all active portcos):
  - funding evidence model now normalizes/dedupes URLs, rejects low-signal rows, scores evidence, and persists metadata in `board_seat_funding_cache`:
    - `evidence_count`
    - `distinct_domains`
    - `conflict_flags`
    - `verification_status`
  - message contract keeps posting behavior unchanged while appending an explicit warning line for low-confidence funding:
    - `Warning: Funding data is low-confidence; verify before action.`
  - new controls:
    - `COATUE_CLAW_BOARD_SEAT_FUNDING_MIN_DOMAINS` (default `2`)
    - `COATUE_CLAW_BOARD_SEAT_FUNDING_LOW_CONF_THRESHOLD` (default `0.55`)
    - `COATUE_CLAW_BOARD_SEAT_FUNDING_WARNING_MODE` (default `1`)
  - operator tools added:
    - `board_seat_daily refresh-funding --all-portcos`
    - `board_seat_daily funding-quality-report --all-portcos`
    - artifact: `funding-quality-report-YYYY-MM-DD.md`
  - status telemetry expanded with `funding_verification_by_company` and aggregate `funding_quality_metrics` (`verified_pct`, `low_confidence_pct`, `oldest_cache_age_days`)
  - validation:
    - `python3 -m compileall -q src/coatue_claw/board_seat_daily.py` -> pass
    - `PYTHONPATH=src python3 -m pytest -q tests/test_board_seat_daily.py` -> `35 passed`
- Parallel Codex multi-agent branch model is now codified for this repo:
  - branch naming standard added in `AGENTS.md`:
    - `codex/agent-board-seat`
    - `codex/agent-chart-day`
    - `codex/agent-hf-analyst`
    - `codex/agent-market-daily`
  - per-agent handoff docs added:
    - `docs/handoffs/agent-board-seat.md`
    - `docs/handoffs/agent-chart-day.md`
    - `docs/handoffs/agent-hf-analyst.md`
    - `docs/handoffs/agent-market-daily.md`
  - worktrees created and verified:
    - `/Users/carsonwang/worktrees/coatue-claw/board-seat`
    - `/Users/carsonwang/worktrees/coatue-claw/chart-day`
    - `/Users/carsonwang/worktrees/coatue-claw/hf-analyst`
    - `/Users/carsonwang/worktrees/coatue-claw/market-daily`
  - remote role branches pushed:
    - `origin/codex/agent-board-seat`
    - `origin/codex/agent-chart-day`
    - `origin/codex/agent-hf-analyst`
    - `origin/codex/agent-market-daily`
  - deploy gate remains integrator-only (`main` merge then runtime restart/verification).
- X-chart slot posting reliability fix shipped (Morning/Afternoon/Evening not posting):
  - root cause: `_slot_key` required runtime minute within ±20 of configured windows while launchd scout runs on drifting `StartInterval=3600`.
  - fix: `_slot_key` now maps each scheduled run to the most recent elapsed configured window (`09:00/12:00/18:00` by default), with existing slot dedupe preserved.
  - result: offset runs (for example `09:34`) still post the `09:00` slot once instead of skipping all day.
  - tests:
    - `PYTHONPATH=src python3 -m pytest -q tests/test_x_chart_daily.py` -> `74 passed`
    - `PYTHONPATH=src python3 -m pytest -q tests/test_launchd_runtime.py` -> `5 passed`
  - deployed on Mac mini (`/opt/coatue-claw`), restarted runtime, and validated live scheduled post for slot `2026-02-24-09:00` (`Coatue Chart of the Morning`)
- Board Seat V6 formatting + content contract shipped (supersedes V5 output shape):
  - `BOARD_SEAT_FORMAT_VERSION = v6_richtext_target_does_monthly_theme`
  - thesis now requires:
    - `Idea`
    - `Target does` (new)
    - `Why now` (past-month thematic framing; no 24h phrasing)
    - `What's different`
    - `MOS/risks`
    - `Bottom line`
  - removed `Idea confidence` line from rendered output.
  - Slack post path now supports rich-text section headers with bold+underline styling and automatic plaintext fallback if blocks are rejected.
  - specificity guardrails enabled (`COATUE_CLAW_BOARD_SEAT_SPECIFICITY_MODE=moderate`): at most one generic filler line across thesis/context core lines.
  - funding scope now defaults to target company (`COATUE_CLAW_BOARD_SEAT_FUNDING_SCOPE=target`) with Crunchbase primary + web fallback.
  - target memory + no-repeat lock remains active:
    - table: `board_seat_target_memory`
    - lock window: `COATUE_CLAW_BOARD_SEAT_TARGET_LOCK_DAYS` (default `30`)
    - ledger artifacts:
      - `/opt/coatue-claw-data/artifacts/board-seat/board-seat-target-ledger.csv`
      - `/opt/coatue-claw-data/artifacts/board-seat/board-seat-target-ledger.json`
    - mirror:
      - `/Users/spclaw/Documents/SPClaw Database/Companies/Board-Seat`
  - validation:
    - `PYTHONPATH=src python3 -m pytest -q tests/test_board_seat_daily.py` -> `28 passed`
- Premium model policy is now defaulted for all OpenAI-backed tasks:
  - board-seat synthesis default: `gpt-5.2-chat-latest`
  - x-chart title/copy synthesis default: `gpt-5.2-chat-latest`
  - x-chart vision extraction default: `gpt-4.1` (high-quality multimodal compatibility on chat endpoint)
  - memory embeddings default: `text-embedding-3-large`
  - market-daily model default upgraded to `gpt-5.2-chat-latest`
  - runtime on Mac mini now pinned to premium model env values (same as above)
- MD BKNG/Google-visible cause reliability fix shipped:
  - web evidence path is now `google_serp` primary with automatic `ddg_html` fallback
  - Google evidence ingestion now parses title + snippet + answer-box text (not title-only)
  - web retrieval depth increased (`COATUE_CLAW_MD_WEB_MAX_RESULTS` default `20`)
  - BKNG-specific query templates and aliases added:
    - `why is BKNG stock down`
    - `BKNG stock down reason today`
    - `Booking Holdings selloff cause`
    - `BKNG AI threat travel OTA`
  - new BKNG/OTA cause clusters added:
    - `ota_ai_disruption`
    - `travel_demand_outlook`
  - scoring now boosts snippet-level causal language and suppresses generic wrappers unless a specific causal narrative is present
  - decisive defaults are now more assertive:
    - `COATUE_CLAW_MD_DECISIVE_PRIMARY_REASON_MIN_SCORE=0.60`
    - `COATUE_CLAW_MD_DECISIVE_PRIMARY_REASON_MIN_MARGIN=0.03`
  - debug transparency expanded:
    - top 5 evidence rows
    - selected cluster + cluster scoring diagnostics
    - web backend used (`google_serp` vs `ddg_html`)
  - regression validation:
    - `PYTHONPATH=src pytest -q` => `162 passed`
- MD is now tuned for decisive “primary reason” output:
  - when one high-quality source clearly dominates cluster evidence, MD states the reason directly instead of defaulting to uncertainty
  - generic wrapper blocking remains enforced to prevent tautological headlines
  - fallback line is still used only for genuinely weak/ambiguous evidence
  - env controls:
    - `COATUE_CLAW_MD_DECISIVE_PRIMARY_REASON_ENABLED=1`
    - `COATUE_CLAW_MD_DECISIVE_PRIMARY_REASON_MIN_SCORE=0.60`
    - `COATUE_CLAW_MD_DECISIVE_PRIMARY_REASON_MIN_MARGIN=0.03`
  - added cybersecurity basket coherence pass:
    - when CRWD/NET-style names sell off together and one confirms Anthropic/Claude cyber catalyst, peers reuse that cause phrasing in the same MD run
  - directional ranking now penalizes positive partnership-only headlines on down moves
  - removed generic `deal_contract` canned phrase; MD now uses concrete event text for ORCL-like contract/financing/litigation moves
  - fixed Yahoo evidence relevance bug by requiring ticker/alias mention in headlines before scoring
  - decisive-primary override now permits strong explicit event headlines even when cluster score margin is narrow
  - fixed cluster-reuse scope: only high-specific clusters are reusable; generic `deal_contract` phrases no longer bleed across tickers
- MD specific-cause enforcement for selloffs is now implemented (NET/CRWD Anthropic miss class):
  - final catalyst lines can name a specific event only when corroborated by:
    - >=2 independent sources
    - >=2 distinct domains
    - >=1 quality domain
  - evidence normalization/dedupe added:
    - canonical URL normalization + DDG redirect unwrap
    - de-duplication by canonical URL/title fingerprint
  - generic wrappers are blocked from final reasons:
    - `why ... stock down today`, `news today`, and ticker-only wrappers
  - new explicit cause cluster:
    - `anthropic_claude_cyber` -> `Anthropic launched Claude Code Security.`
  - final line contract is deterministic:
    - `Shares fell after <specific event>.` / `Shares rose after <specific event>.`
    - fallback: `Likely positioning/flow; no single confirmed catalyst.`
  - cross-mover cluster reuse now keeps basket-event lines consistent across affected movers in one run
  - debug output includes corroboration fields and confirmed cluster metadata
  - tests updated and passing:
    - `PYTHONPATH=src pytest -q` => `155 passed`
- MD catalyst reliability fix (NET / Anthropic miss class) is now implemented:
  - evidence stack upgraded from X+Yahoo to X + Yahoo + Google SERP primary + DDG fallback (`COATUE_CLAW_MD_WEB_SEARCH_ENABLED=1`)
  - Yahoo ingestion now supports both legacy and nested yfinance schemas
  - catalyst lookback now uses session anchors (prev close / same-day open) with configurable cap
  - X retrieval depth and query quality improved for ambiguous tickers (`COATUE_CLAW_MD_X_MAX_RESULTS`, alias-aware query + filters)
  - evidence scoring/clustering now drives confidence and chosen catalyst, with directional ranking for up/down movers
  - markdown artifacts now include per-mover evidence diagnostics and reject reasons
  - debug interfaces shipped:
    - CLI `claw market-daily debug-catalyst <TICKER> [--slot open|close]`
    - Slack `md debug <TICKER> [open|close]`
  - tests updated (`tests/test_market_daily.py`) and passing
- Post-fix tuning added:
  - web fallback now also triggers when directional evidence is weak or source diversity is narrow
  - catalyst ranking now applies move-direction bonuses (up/down-aware)
  - final one-line reason now prefers the selected evidence source path
  - regression tests added for negative-move selection and fallback trigger behavior
- MD output copy contract refined:
  - no universe line in Slack post
  - slot copy says `3 biggest movers this morning/afternoon`
  - mover rows use only `📈`/`📉` directional emoji
  - catalyst lines are sanitized (no hashtags/cashtags/handles/URLs/extra emoji) and forced to causal explanation style
  - source links remain as `[X]` / `[News]` only
  - X evidence relevance guard added for ambiguous tickers (short symbols require cashtag + finance-keyword match)
  - vague X-only catalyst snippets now auto-fallback to a coherent company-specific-driver sentence
- MD (Market Daily) is now shipped in code and wired across CLI + Slack + launchd:
  - module: `src/coatue_claw/market_daily.py`
  - schedule: weekdays `07:00` and `14:15` local via `com.coatueclaw.market-daily`
  - Slack commands: `md now|status|holdings refresh|holdings show|include|exclude`
  - artifacts: `/opt/coatue-claw-data/artifacts/market-daily/md-<slot>-<timestamp>.md`
  - DB: `/opt/coatue-claw-data/db/market_daily.sqlite`
  - seed universe: `config/md_tmt_seed_universe.csv`
  - Make targets: `openclaw-market-daily-status|run-once|refresh-holdings`
  - test coverage added: `tests/test_market_daily.py` + launchd runtime expectations
- Change-request governance now tracks both Spencer and Carson with explicit attribution:
  - captured items include requester identity in command output + daily digest
  - command aliases now include `change requests` / `tracked changes` in addition to `spencer changes`
  - optional requester mapping env: `COATUE_CLAW_CHANGE_TRACKER_USERS=user_id:label,...`
- Board Seat-as-a-Service daily loop is now implemented:
  - service: `com.coatueclaw.board-seat-daily` via `launchd`
  - runtime module: `src/coatue_claw/board_seat_daily.py`
  - cadence: daily at `COATUE_CLAW_BOARD_SEAT_TIME` (default `08:30`, local tz)
  - default channel set: anduril/anthropic/cursor/neuralink/openai/physical-intelligence/ramp/spacex/stripe/sunday-robotics
  - behavior: one post per company per local day (duplicate-protected in SQLite ledger)
  - per-channel output uses board-seat frame (`Signal`, `Board lens`, `Watchlist`, `Team ask`) and incorporates last-24h channel context when available
  - `missing_scope` fallback is built in for Slack channel lookup: posts can still deliver by channel name even without `conversations:read`
  - config supports custom company:channel map with `COATUE_CLAW_BOARD_SEAT_PORTCOS`
- X chart flow is now scout-first and slot-posted:
  - launchd runs `x_chart_daily run-once` hourly (`StartInterval=3600`; configurable via `COATUE_CLAW_X_CHART_SCOUT_INTERVAL_SECONDS`)
  - each hourly run stores candidates in `observed_candidates` and updates source trust signals
  - when a post window is active, winner ranking is drawn from candidates observed since the last scheduled slot post
  - this improves post quality by ranking across the full inter-slot pool instead of one fetch snapshot
- X chart naming convention is now time-of-day based:
  - `Coatue Chart of the Morning`, `...Afternoon`, `...Evening`
  - applied in Slack upload title and initial message format
- New pool controls are live:
  - `COATUE_CLAW_X_CHART_POOL_KEEP_DAYS` (retention, default `10`)
  - `COATUE_CLAW_X_CHART_POOL_LIMIT` (ranking pool size, default `600`)
- X chart candidate selection now balances score + variety:
  - keeps highest-score behavior as baseline
  - when alternatives are close (within score floor), prefers less-recently-used source to avoid repeat posters
  - defaults:
    - `COATUE_CLAW_X_CHART_SOURCE_VARIETY_LOOKBACK=6`
    - `COATUE_CLAW_X_CHART_SOURCE_VARIETY_SCORE_FLOOR=0.90`
  - if alternatives are not close enough, top scorer still wins
  - tests:
    - `test_pick_winner_prefers_variety_within_score_floor`
    - `test_pick_winner_keeps_top_when_alternative_too_low`
- X chart posting mode reverted to source-snip:
  - Slack chart output now uses source-snip-card mode:
    - source X chart image embedded in Coatue-branded output card
    - no numeric reconstruction/redraw
  - URL chart requests (`run-post-url`) no longer enforce numeric reconstruction prechecks
  - explicit error is returned only when source image cannot be fetched
  - Slack copy retains title discipline:
    - narrative `Title`
    - technical `Chart label`
    - concise `Key takeaway`
  - runtime verification aid:
    - `x chart status` now returns `render_mode: source-snip`
  - title clipping fix in source-snip-card renderer:
    - auto-fit loop for headline/subheading width
    - hard fail-safe shortening when text still overflows
  - low-signal copy rewrite guard:
    - filters generic lead-ins and forces concise trend phrasing
    - keyword override for tariff/customs posts
    - trailing-stopword trimming to prevent awkward cutoffs
    - vision chart-title hint fallback when tweet text is generic/opening-heavy
    - applies independently to headline/chart-label/takeaway (not headline-only)
  - Slack summary now mirrors sanitized chart takeaway copy (no raw-fragment fallback)
  - modules touched: `src/coatue_claw/x_chart_daily.py`
  - tests updated: `tests/test_x_chart_daily.py`
- Spencer-request governance is now end-to-end:
  - auto-capture + status tracking for Spencer bot-change asks
  - on-demand Slack review commands (`spencer changes`, `spencer changes open`, `spencer changes last 50`)
  - scheduled daily DM digest at 6:00 PM local time via launchd service `com.coatueclaw.spencer-change-digest`
  - DM recipients configured via `COATUE_CLAW_SPENCER_CHANGE_DIGEST_DM_USER_IDS`
  - runtime module: `src/coatue_claw/spencer_change_digest.py`
  - tests: `tests/test_spencer_change_log.py`, `tests/test_spencer_change_digest.py`
  - runtime validation:
    - Mac mini scheduler service `com.coatueclaw.spencer-change-digest` is loaded via launchd
    - one-time forced send succeeded to configured recipient (`U0AGD28QSQG`)
  - delivery fallback hardening:
    - digest sender falls back across Slack token sources (env token -> OpenClaw config token)
    - if IM scope is unavailable on `conversations.open`, sender posts to App Home DM channel (`channel=<user_id>`)
- Spencer change-request tracker shipped:
  - captures Spencer-requested bot changes from Slack (`spcoatue` + `spencermpeter` user IDs by default; env-overridable)
  - persists requests to `/opt/coatue-claw-data/db/spencer_changes.sqlite`
  - auto-updates request status through execution path (`captured`, `handled`, `implemented`, `blocked`, `needs_followup`)
  - Slack retrieval commands:
    - `spencer changes`
    - `spencer changes open`
    - `spencer changes last 50`
  - module: `src/coatue_claw/spencer_change_log.py`
  - tests: `tests/test_spencer_change_log.py`
- Slack channel access hardening shipped:
  - bot auto-joins newly created public channels (`channel_created` -> `conversations.join`)
  - bot runs startup public-channel bootstrap to join existing public channels it is missing
  - feature toggle: `COATUE_CLAW_SLACK_AUTOJOIN_PUBLIC_CHANNELS=1` (default on)
  - private channels remain invite-only by Slack design
  - tests: `tests/test_slack_channel_access.py`
- X Chart posting target moved to `#charting`:
  - runtime env uses `COATUE_CLAW_X_CHART_SLACK_CHANNEL=C0AFXM2MWAV` (`#charting`) instead of `#general`
- X Chart post-publish self-review loop shipped:
  - every posted chart now writes checklist audit to SQLite table `post_reviews` in `/opt/coatue-claw-data/db/x_chart_daily.sqlite`
  - checklist includes US relevance, copy constraints, axis-label presence, grouped-series validity (when required), and artifact integrity
  - automatic self-learning feedback updates source `priority/trust_score` after each post (low-quality outputs are downranked for future picks)
- Drive + file-ingest taxonomy simplification shipped:
  - Drive root renamed/configured to `/Users/spclaw/Documents/SPClaw Database`
  - category taxonomy reduced to three folders: `Universes`, `Companies`, `Industries`
  - ingest classifier now maps legacy labels (`filings`, `themes`, `macro`, `sectors`, etc.) into the new three-folder scheme
- X chart QA hardening for grouped bar charts (employees vs robots) shipped:
  - two-series requirement enforced for employee/robot charts (single-series outputs now fail closed)
  - grouped charts must use unit values (not normalized index), monotonic year x-axis labels, and non-placeholder labels
  - grouped series metadata normalized before rendering:
    - `Employees` (dark navy) + `Robots` (purple)
    - y-axis defaults to `Number (thousands)` when source units are unclear
  - fixed grouped metadata normalization for immutable dataclasses (copy/replace instead of in-place mutation)
  - added CV fallback extractor for employee/robot charts when vision is unavailable/quota-limited:
    - reconstructs two series from chart colors (dark employees + purple robots)
    - calibrates to absolute unit values using latest employee/robot figures in post text
  - style copy QA added pre-render (headline/chart-label/takeaway length + no ellipsis)
  - y-axis ticks are now always generated for non-normalized bar charts (unit-readable values with numeric labels)
  - pre-post guardrail now fails if reconstructed bar charts are missing y-axis tick labels
  - employee/robot takeaway now uses a short complete sentence to avoid clipped wording
  - new tests:
    - vision extraction rejects single-series payloads for employee/robot charts
    - renderer rejects employee/robot charts without both series
  - validation: `PYTHONPATH=src pytest -q` => `105 passed`
- X URL chart requests now have a deterministic CLI entrypoint for OpenClaw gateway routing:
  - added `run-post-url` command to `coatue_claw.x_chart_daily` CLI
  - command routes to `run_chart_for_post_url(...)` (the strict rebuild-only pipeline)
  - this avoids freeform screenshot-style replies for tweet URL chart requests
  - runtime guardrail mirrored into `~/.openclaw/workspace/AGENTS.md` on Mac mini so gateway sessions follow the deterministic command path
  - test added: `test_cli_run_post_url_command`
  - validation: `PYTHONPATH=src pytest -q` => `100 passed`
- X chart output now better matches requested behavior:
  - shortened takeaway strings across style draft + Slack summary + chart footer to avoid visual truncation
  - screenshot fallback disabled in renderer (hard-off), so posted charts must be reconstructed
  - grouped/two-series bar reconstruction added for tweet charts with paired bars
  - vision extraction parser now supports multi-series JSON (`series`) and maps to native grouped-bar rendering
  - reconstructability gate updated to respect inferred chart mode (bar vs line) for consistency
  - added regression test to prevent screenshot fallback even when env fallback is disabled
  - validation: `PYTHONPATH=src pytest -q` => `99 passed`
- X chart URL workflow now enforces true reconstruction over screenshot fallback:
  - `run_chart_for_post_url` validates extracted numeric series before post
  - if extraction is not reliable, it fails cleanly and asks for another post instead of posting a screenshot chart
  - added `vxtwitter` fallback candidate fetch for URL-specific requests when X API payload lacks media
  - renderer now tries vision bar extraction first to better rebuild bar charts from tweet images
  - global Slack post guard now defaults to rebuild-required mode (`COATUE_CLAW_X_CHART_REQUIRE_REBUILD=1` behavior) so unreadable screenshot-style outputs are blocked
- X chart title/subheading quality improved:
  - optional LLM style synthesis generates:
    - narrative headline (theme)
    - technical chart label (what graph shows)
    - concise takeaway
  - constrained formatting: no handles, no `BREAKING`, no ellipsis
  - fallback heuristics remain if LLM path fails
  - added deterministic narrative rule for employees-vs-robots posts so headline/subheading remain Coatue-like when LLM synthesis is unavailable
- tests added/updated:
  - URL-run fallback coverage
  - URL-run reconstruction gate coverage
  - total suite validation: `PYTHONPATH=src pytest -q` => `98 passed`
- Slack bot now supports combined natural-language X-post requests:
  - detects X post URL + “add to twitter/x source list” phrasing + “make chart” phrasing in one message
  - executes both actions in sequence:
    - `add_source(handle)`
    - `run_chart_for_post_url(post_url)` with Coatue-style output
  - prevents fallback to generic executor for this workflow
  - new parser module: `src/coatue_claw/slack_x_chart_intent.py`
  - new runner entrypoint: `run_chart_for_post_url` in `src/coatue_claw/x_chart_daily.py`
  - tests added:
    - `tests/test_slack_x_chart_intent.py`
    - `tests/test_x_chart_daily.py::test_run_chart_for_post_url_posts_specific_tweet`
  - validation: `PYTHONPATH=src pytest -q` => `95 passed`
- X Chart readability hardening shipped:
  - removed top-left generated timestamp from card header
  - enforced x-axis labels on reconstructed bar charts
  - fallback labeling now prefers real/inferred years before generic placeholders
  - added readability fail-safe: if reconstructed bar chart lacks sufficient labels, auto-fallback to source image to avoid unreadable output
  - vision extraction now sends inline image bytes (data URL) for higher reliability
  - validation: `PYTHONPATH=src pytest -q` => `91 passed`
- X Chart-of-the-Day refinement shipped for Coatue-style framing and safer chart rebuilds:
  - synthesized two-level titles:
    - big narrative headline from tweet/chart takeaway
    - small chart label describing chart content/units/timeframe
  - removed generic placeholder bar labels (`G1..G10`)
  - tightened bar rebuild quality gates so low-confidence parses fail closed instead of posting misleading reconstructed bars
  - added optional OpenAI vision bar extraction path (env-gated) to pull concrete labels/values from source chart images
  - bar renderer now handles non-normalized values and negative bars when extracted
  - validation: `PYTHONPATH=src pytest -q` => `90 passed`
- AGENTS and initial scaffold are complete
- Basic CLI + Slack bot skeleton are implemented
- Bot mention delivery is working with open Slack access policy
- Slack default routing is now enabled:
  - plain messages are treated as OpenClaw requests by default
  - messages with explicit `@user` mentions are not default-routed
  - deployed/validated on Mac mini (`86bce9d`): Slack probe healthy after restart
  - runtime transport config on Mac mini now explicitly disables mention gating:
    - `~/.openclaw/openclaw.json` -> `channels.slack.requireMention=false`
    - channel override for `#general` (`C0AFGMRFWP8`) also set to `requireMention=false`
- Natural-language chart requests now route into valuation charting with configurable axes
- CSV-backed universe management is implemented for Slack-driven create/edit/reuse flows
- Missing-ticker chart prompts now ask for source choice (`online` discovery vs saved universe CSV)
- Post-chart feedback loop is implemented for include/exclude reruns
- Post-chart feedback prompt delivery now uses resilient thread posting (retry + fallback) for higher Slack reliability
- OpenClaw valuation-charting skill now requires a post-chart adjustments follow-up question after each successful chart response
- Chart headline context now follows prompt theme; citation/footer is left-aligned for cleaner layout
- Category guide placement now defaults to adaptive in-plot whitespace positioning to reduce wasted space while avoiding key chart overlays
- Laptop/Codex/OpenClaw runbook now exists in-repo (`docs/laptop-codex-openclaw-workflow.md`) and AGENTS includes explicit canonical-path + ship/restart workflow rules
- OpenClaw runtime contract is now codified in `docs/openclaw-runtime.md` (execution model, job classes, ops + triage checklist)
- Make targets now include explicit `dev`, `bot`, and `schedulers` runtime controls for operator workflows
- Makefile OpenClaw targets now prepend `/opt/homebrew/bin` to PATH and use binary fallback detection so remote non-login SSH sessions can restart/status without manual PATH export
- Plain-English Slack settings controls are now implemented (`show settings`, conversational default updates, promote-to-main, undo last promotion)
- Runtime settings now persist under `/opt/coatue-claw-data/db/runtime-settings.json` with markdown audit logs in `/opt/coatue-claw-data/artifacts/config-audit/`
- Slack deploy pipeline controls are implemented (`deploy latest`, `undo last deploy`, `run checks`, `show pipeline status`, `show deploy history`, `build: ...`) with one-job-at-a-time locking and admin gating
- Deploy history now persists to `/opt/coatue-claw-data/db/deploy-history.json`
- Diligence command now generates a structured neutral investment memo (deep data pull from company profile, financials, valuation, balance sheet, and recent reporting headlines) instead of template placeholders
- Diligence now runs a local database-first report lookup before external research:
  - checks `/opt/coatue-claw-data/db/file_ingest.sqlite` and prior packet markdowns in `/opt/coatue-claw-data/artifacts/packets/`
  - includes local match references directly in memo output for continuity and auditability
- Hybrid memory system is implemented:
  - SQLite + FTS5 structured memory store in `/opt/coatue-claw-data/db/memory.sqlite`
  - auto extraction of profile facts, decisions, and conventions from Slack messages
  - decay tiers (`permanent`, `stable`, `active`, `session`, `checkpoint`) with TTL refresh-on-access
  - pre-flight pipeline checkpoints for deploy/build/undo operations
  - optional LanceDB/OpenAI semantic fallback
  - CLI ops: `claw memory status|query|prune|extract-daily|checkpoint`
- File management bridge is implemented:
  - local-first canonical storage in `/opt/coatue-claw-data/files/{working,archive,published,incoming}`
  - share mirror sync to configurable Drive root via `config/file-bridge.json`
  - Drive mirror root is configured on Mac mini as `/Users/spclaw/Documents/SPClaw Database`
  - category subfolders are simplified for Spencer-facing workflows under `01_DROP_HERE_Incoming/02_READ_ONLY_Latest_AUTO/03_READ_ONLY_Archive_AUTO`: `Universes`, `Companies`, `Industries`
  - `01_DROP_HERE_Incoming/_Latest_Reference_READ_ONLY` auto-mirrors Latest and is excluded from pull ingestion
  - Slack file uploads now auto-ingest into knowledge folders with SQLite audit tracking (`/opt/coatue-claw-data/db/file_ingest.sqlite`) via `message` + `file_shared` + `app_mention` event handlers
  - operations via `make openclaw-files-{init,status,sync-pull,sync-push,sync,index}`
  - published index artifacts generated to `published/index.{json,md}`
- Email channel integration is implemented (optional):
  - IMAP poll + SMTP reply runtime in `src/coatue_claw/email_gateway.py`
  - email commands: diligence, memory status/query, files status, help
  - context-aware diligence email parsing now prioritizes body intent and filters filler tokens so ticker extraction is robust in natural phrasing
  - diligence email response format is now consumer-friendly (executive summary in body + full memo attached as readable `.pdf`, with summary citation tails removed for readability)
  - local filesystem paths are removed from user-facing diligence email output
  - PDF rendering now escapes literal `$` symbols so finance values render reliably
  - diligence attachment PDF now renders as a sectioned, consumer-readable brief (not raw memo text)
  - professional PDF styling now uses clean section headers, readable bullet spacing, and page footers for Spencer-facing consumption
  - report title is generic to the diligence topic/company (no third-party/borrowed brand title text)
  - latest template upgrade adds centered title + metadata row + backdrop callout to align with professional memo aesthetics
  - email attachments auto-ingest to knowledge folders with audit DB (`/opt/coatue-claw-data/db/email_gateway.sqlite`)
  - operations via `make openclaw-email-{status,run-once,serve}`
  - Mac mini validation confirms `Testing Dilligence` + `Diligence SNOW please` resolves to ticker `SNOW`
  - Mac mini validation confirms summary citation tails are removed in email body while full-citation memo remains attached
  - Mac mini validation confirms diligence attachment is now readable PDF (`application/pdf`) and local paths are removed from user-facing email output
- X digest (official API path) is implemented for on-demand use:
  - Slack commands:
    - `x digest <query> [last Nh] [limit N]`
    - `x status`
  - CLI command:
    - `claw x-digest "QUERY" --hours 24 --limit 50`
  - digest artifact output:
    - `/opt/coatue-claw-data/artifacts/x-digest` (override with `COATUE_CLAW_X_DIGEST_DIR`)
  - runtime env contract:
    - `COATUE_CLAW_X_BEARER_TOKEN` required
    - `COATUE_CLAW_X_API_BASE` optional (default `https://api.x.com`)
  - tests:
    - `tests/test_slack_x_intent.py`
    - `tests/test_x_digest.py`
  - Mac mini runtime status:
    - deployed on `/opt/coatue-claw` at commit `5dfdd03`
    - bearer token configured in `.env.prod`
    - Slack probe healthy after restart (`make openclaw-slack-status` => `ok=true`)
    - live digest smoke test succeeded and wrote artifact to `/opt/coatue-claw-data/artifacts/x-digest/`
- X chart scout is now implemented for daily winner posting:
  - prioritized source list seeded with `@fiscal_AI` and other high-signal accounts
  - auto-discovery/promotion of new sources based on engagement
  - supplemental ingestion from Visual Capitalist feed (`https://www.visualcapitalist.com/feed/`)
  - Slack commands:
    - `x chart now`
    - `x chart status`
    - `x chart sources`
    - `x chart add @handle priority 1.2`
  - CLI commands:
    - `claw x-chart run-once --manual`
    - `claw x-chart status`
    - `claw x-chart list-sources`
    - `claw x-chart add-source HANDLE --priority 1.2`
  - scheduled runtime service:
    - `com.coatueclaw.x-chart-daily` via launchd
    - windows default to `09:00,12:00,18:00` (timezone default `America/Los_Angeles`)
  - artifacts and state:
    - sqlite store: `/opt/coatue-claw-data/db/x_chart_daily.sqlite`
    - markdown artifacts: `/opt/coatue-claw-data/artifacts/x-chart-daily`
  - tests:
    - `tests/test_x_chart_daily.py`
    - `tests/test_launchd_runtime.py` (updated for new service)
  - resilience:
    - invalid/renamed X handles are skipped without failing the full scout run
    - Slack posting can use `~/.openclaw/openclaw.json` token fallback if env token is unavailable
    - Slack posting automatically retries against fallback token when primary env token is rejected
  - Mac mini runtime status:
    - deployed and validated at commit `c3f64d0`
    - scheduler service `com.coatueclaw.x-chart-daily` loaded via launchd
    - proof-of-life manual run posted successfully to `#charting`
  - presentation layer:
    - winners are now rendered into a Coatue-style “Chart of the Day” visual card before Slack upload
    - style cues align with C:\\Takes design language and valuation-chart skill guidance
  - quality gate:
    - candidate selection now enforces chart-like text/data signals to suppress non-chart image picks
    - candidate selection now enforces US relevance and blocks non-US forex-only chart trends
    - post copy/style now goes through iterative style-audit checks before final render/post
    - output text is normalized to prevent unsupported glyph/missing-character artifacts
  - presentation update:
    - card layout shifted to graph-first style for Chart of the Day:
      - no left-side narrative column
      - concise headline at top
      - chart/image carries the core story
      - minimal bottom footer (takeaway + source)
    - Slack post packaging now sends chart file in the initial channel message (not in a thread)
    - Chart output now attempts source-chart reconstruction (line extraction + redraw) so final output is a rebuilt Coatue chart, not a screenshot frame
    - renderer now supports bar-mode reconstruction when bar cues are detected in text/image (bar chart output instead of line output)
    - guardrail: bar-cue posts no longer degrade into fake line reconstructions; they rebuild as bars or fall back to source image
    - chart image now omits source-handle overlay and score corner marker
    - pre-save layout checks enforce no overlapping header/chart/footer text
    - Headline/takeaway formatter now enforces no-ellipsis titles (`...` removed and phrasing shortened)
    - title generation now follows Coatue two-level framing:
      - small chart label = what the graph is showing
      - big headline = thematic narrative takeaway
      - raw news prefixes (for example `BREAKING:`) are rewritten into narrative title language
- 24/7 runtime supervision is implemented:
  - launchd-managed services in `src/coatue_claw/launchd_runtime.py`
  - services: `com.coatueclaw.email-gateway` (always-on poller), `com.coatueclaw.memory-prune` (hourly prune)
  - launchctl domain fallback (`gui/<uid>` then `user/<uid>`) for reliable control over SSH and local sessions
  - operations via `make openclaw-24x7-{enable,status,disable}`
  - scheduler status target now reports real launchd state (`make openclaw-schedulers-status`)
  - deployed and validated on Mac mini (`a49f887` + `95fb26d`): email poller is running; memory-prune service is loaded with clean `last_exit_code=0` between hourly runs
- Git shipping protocol is now explicit: every Codex change ships to `origin` with handoff updates

## Immediate Next Actions
1. Validate Slack deploy pipeline commands in `#claw-lab` (`deploy latest`, `undo last deploy`, `run checks`, `build: ...`)
2. Configure `SLACK_PIPELINE_ADMINS` on runtime host and validate permission boundaries
3. Ensure Slack app permissions for cross-channel posting are enabled:
   - bot scopes: `channels:read`, `channels:join`, `chat:write`, `chat:write.public`
   - bot event subscription: `channel_created`
   - reinstall app after scope/event updates
4. Validate hybrid memory behavior in Slack:
   - `remember ...` capture
   - `what is my ...` retrieval
   - `memory status`
   - `memory checkpoint`
5. Confirm Google Drive desktop client is syncing `/Users/spclaw/Documents/SPClaw Database` to Spencer-shared Drive
6. Validate category-based file flow with Spencer (`01_DROP_HERE_Incoming/{Universes|Companies|Industries}` -> local incoming mirror -> `02_READ_ONLY_Latest_AUTO/{Universes|Companies|Industries}`)
7. Validate Slack file upload auto-ingest (`Slack upload` -> categorized `incoming/{Universes|Companies|Industries}` + DB record in `file_ingest.sqlite`)
8. Validate launchd service persistence after next Mac mini reboot (`make openclaw-24x7-status`)
9. Validate daily backfill flow (`claw memory extract-daily --dry-run --days 14`)
10. Validate new diligence memo output in Slack (`diligence TICKER`) and confirm section completeness/citations + local database-first precheck behavior
11. Configure email env vars in `/opt/coatue-claw/.env.prod` and validate `make openclaw-email-status` + `make openclaw-email-run-once`
12. Deploy and enable `com.coatueclaw.x-chart-daily` on Mac mini with:
    - `COATUE_CLAW_X_CHART_SLACK_CHANNEL`
    - `COATUE_CLAW_X_CHART_WINDOWS=09:00,12:00,18:00`
    - `COATUE_CLAW_X_CHART_TIMEZONE=America/Los_Angeles`
13. Validate three scheduled daily posts in Slack (9am/12pm/6pm PT) and tune source priority list after first day.
14. Pull latest on Mac mini, restart runtime, and verify `x chart now` posts a rebuilt graph-first chart (not source screenshot framing) with no-ellipsis title.
15. Observe 1-2 days of live scheduled posts and tune reconstruction thresholds/source priorities if rebuild fallback rate is high.
16. On Mac mini, verify `OPENAI_API_KEY` is set in `/opt/coatue-claw/.env.prod` so vision-assisted bar extraction is active for X chart rebuild quality.

## 2026-02-19 Update - Build Request Runtime Robustness
- Added a near-term reliability guard for Slack `build:` execution:
  - `codex exec` prompt now instructs fallback to `grep -R` when `rg` is missing.
- Added test coverage:
  - `tests/test_slack_pipeline.py::test_run_build_request_prompt_includes_rg_fallback`.
- Operational recommendation remains to install ripgrep on runtime host for speed and consistency.

## 2026-02-19 Ship Status
- Shipping prompt fallback + test to `main` to prevent Slack build-request failures when `rg` is missing.

## 2026-02-21 Plan Update - Chart Title Coherence

### Completed
- Hardened X chart title synthesis to prevent ungrammatical English in final chart header output.
- Added grammar-aware cleanup + fallback rewrite path in `src/coatue_claw/x_chart_daily.py`.
- Added regression coverage for the observed Slack failure pattern.

### In Progress
- Runtime verification on Mac mini after deploy/restart:
  - check manual post-url path + scheduled slot behavior.

### Next
1. Verify chart title quality in `#charting` for noisy source posts.
2. If any title still fails coherence checks, capture artifact + source URL and add targeted override/test.

## 2026-02-21 Plan Update - Source-Snip Copy Simplification

### Completed
- Removed user-facing chart-label from X source-snip output (image subtitle + Slack comment line).
- Preserved internal chart-label generation for style QA/scoring.

### Next
1. Verify live `run-post-url` post in `#charting` has no chart-label line.
2. Continue monitoring title coherence + truncation in scheduled posts.

## 2026-02-23 Plan Update - MD APP Cause Specificity

### Completed
- Added explicit `regulatory_probe` cause cluster in Market Daily so SEC/probe selloffs can be named directly.
- Added APP alias coverage and APP-specific web query expansion for `SEC probe` / `short seller report` narratives.
- Promoted `barrons.com` to MD quality-source set and domain-weight map so Barron's evidence can satisfy corroboration/decisive gates.
- Expanded directional and decisive event-term matching for `probe` / `investigation` / `regulatory`.
- Added APP regression coverage in `tests/test_market_daily.py`.
- Validation: `PYTHONPATH=src python3 -m pytest tests/test_market_daily.py` (`31 passed`).

### In Progress
- Deploying this patch to Mac mini runtime and verifying live `md now` output for APP-like selloff cases.

### Next
1. Confirm `md debug APP` resolves to `selected_cluster=regulatory_probe` with a specific line.
2. Monitor next 3 MD posts and verify fallback usage rate declines for clearly news-driven selloffs.
3. If generic fallback still appears for obvious cases, expand quality-domain allowlist and cluster keywords for the miss pattern.

## 2026-02-23 Plan Update - X Chart Copy Quality Hardening

### Completed
- Enforced complete-sentence takeaway output in `src/coatue_claw/x_chart_daily.py` using deterministic validators/finalizers.
- Added fallback copy rewrite path from candidate context when synthesized takeaway is clipped or low quality.
- Added degenerate-copy guards for headline/chart label (for example single-token `U.S` outputs).
- Added scout-run candidate fallback selection:
  - if top-ranked candidate fails copy quality, system now chooses next valid candidate.
- Preserved strict explicit-URL behavior:
  - no candidate swap in `run_chart_for_post_url`;
  - requested URL is retained and copy is rewritten if needed.
- Added diagnostics to `run_chart_scout_once` and `run_chart_for_post_url` results:
  - `copy_rewrite_applied`
  - `copy_rewrite_reason`
  - `candidate_fallback_used`
- Expanded post-review checks with:
  - `takeaway_complete_sentence`
  - `headline_non_degenerate`
  - `chart_label_non_degenerate`
- Added regression coverage in `tests/test_x_chart_daily.py` for:
  - fragment rejection + sentence finalization
  - degenerate field rewrites
  - scout candidate fallback
  - explicit URL no-swap behavior with rewrite.
- Validation:
  - `PYTHONPATH=/opt/coatue-claw/src /opt/coatue-claw/.venv/bin/python -m pytest -q /opt/coatue-claw/tests/test_x_chart_daily.py` -> `49 passed`.

### In Progress
- Runtime verification on Mac mini after pull/restart to confirm live Slack output quality for both `x chart now` and explicit URL requests.

### Next
1. Deploy latest `main` on Mac mini and restart OpenClaw runtime.
2. Validate one scout post and one explicit URL post in `#charting` for sentence completeness and source URL retention.
3. Review next scheduled chart window for any residual low-signal rewrites (`copy_rewrite_reason=safe_fallback`) and tune synthesis prompts only if needed.
4. Handle unrelated full-suite failing tests (`tests/test_spencer_change_digest.py`, `tests/test_spencer_change_log.py`) in a separate patch.

## 2026-02-23 Plan Update - X Chart Headline Completeness Hardening

### Completed
- Added deterministic headline completeness guardrails in `src/coatue_claw/x_chart_daily.py`:
  - `_is_complete_headline_phrase`
  - `_finalize_headline_phrase`
  - `_rewrite_headline_from_candidate`
- Added headline dangling-ending filters so malformed endings (for example `...now is`) are blocked.
- Fixed first-sentence extraction for abbreviation-leading text (`U.S.`), preventing subject truncation.
- Integrated headline enforcement into `_sanitize_style_copy(...)`:
  - rewrite attempted if headline is invalid;
  - unrecoverable headlines now set `copy_rewrite_reason=headline_unrecoverable`;
  - no generic fallback headline is force-posted in unrecoverable cases.
- Extended publish-critical checks:
  - style errors now include `headline incomplete phrase`
  - publish issues now include `headline_incomplete_phrase`
  - style/post-review checks now include `headline_complete_phrase`.
- Updated scout failure behavior:
  - if no candidate is publishable after copy-quality gates, return non-post result with explicit reason `no_publishable_candidate_available` and `publish_issues`.
- Explicit URL mode remains strict:
  - URL never swapped
  - request errors if headline remains invalid after rewrite.
- Added/updated regression coverage in `tests/test_x_chart_daily.py` for headline validator/finalizer, style rewrite behavior, scout fallback/skip behavior, and explicit-URL unrecoverable-title error.
- Validation:
  - `PYTHONPATH=/opt/coatue-claw/src /opt/coatue-claw/.venv/bin/python -m pytest -q /opt/coatue-claw/tests/test_x_chart_daily.py` -> `55 passed`.

### In Progress
- Runtime verification in live Slack usage for malformed title class (`...now is`) after deploy/restart.

### Next
1. Deploy latest `main` on Mac mini and restart OpenClaw runtime.
2. Validate one scout run and one explicit URL run in `#charting` for title completeness and strict URL behavior.
3. Track occurrences of `copy_rewrite_reason=headline_unrecoverable`; if frequent for specific source families, add targeted synthesis heuristics.
4. Resolve unrelated Spencer-change identity default test failures in a dedicated patch.

## 2026-02-23 Plan Update - Board Seat Idea De-dup + History Memory

### Completed
- Implemented persistent board-seat pitch memory in `src/coatue_claw/board_seat_daily.py` (`board_seat_pitches` table).
- Added automatic migration seed from historical `board_seat_runs` into pitch memory (`legacy_run_seed`) so prior posts are included immediately.
- Added repeat-idea guardrail for each portco:
  - blocks repeated investment theses unless significant context change is detected.
- Added significant-change detector based on context novelty/event tokens/numeric deltas.
- Added one-pass novel rewrite fallback; if still repeated and no significant change, post is skipped with explicit reason.
- Added best-effort Slack channel-history backfill routine for pitch memory ingestion.
- Extended status output with pitch-memory counts by company.
- Added board-seat tests for extraction, dedupe skip, significant-change allow, and backfill parsing.
- Validation:
  - `PYTHONPATH=/opt/coatue-claw/src /opt/coatue-claw/.venv/bin/python -m pytest -q /opt/coatue-claw/tests/test_board_seat_daily.py` -> `9 passed`.
  - full smoke unchanged except pre-existing Spencer-change identity failures.

### In Progress
- Deep historical Slack message backfill for `#anduril` is scope-limited under current Slack token permissions (`conversations_history` returns empty under present configuration).

### Next
1. Enable Slack history scopes for bot/app in production workspace and reinstall app.
2. Re-run board-seat history backfill for `#anduril` and confirm `board_seat_pitches` contains all legacy channel posts (not only run-table seed).
3. Add explicit named-investment entity extraction (e.g., Epirus) to strengthen semantic repeat blocking beyond lexical similarity.
4. Continue monitoring skip reasons in runtime logs to tune novelty thresholds without suppressing truly new ideas.

## 2026-02-23 Plan Update - Board Seat V4 Acquisition + Named Citations

### Completed
- Upgraded board-seat output contract to acquisition/acquihire-first in `src/coatue_claw/board_seat_daily.py`.
- Bumped format version to `v4_acq_acquihire_named_sources`.
- Added explicit `Idea` line to thesis rendering and repeat-signal parsing.
- Added structured source reference model (`SourceRef`) and replaced numeric source labels with named citation lines:
  - `Publisher/Source — Article title: <url>`.
- Added acquisition-idea validation + deterministic best-effort rewrite path for invalid/non-acquisition idea lines.
- Added acquisition evidence retrieval helper (`_acquisition_search_rows`) and source-reference merge/fallback logic.
- Updated board-seat test suite to V4 behaviors and regression coverage.
- Validation:
  - `PYTHONPATH=/opt/coatue-claw/src /opt/coatue-claw/.venv/bin/python -m pytest -q /opt/coatue-claw/tests/test_board_seat_daily.py` -> `15 passed`.
  - Full suite unchanged except pre-existing Spencer identity failures.

### In Progress
- Live interactive validation in `#openai` for conversational board-seat replies using V4 format.

### Next
1. Trigger a fresh `#openai` prompt (`give me a new board seat idea`) and verify:
   - first thesis line is `Idea: Acquire/Acquihire ...`
   - citations are named title lines (no `Source 1/2/3`).
2. If any conversational path still emits generic/non-M&A framing, tighten that path’s prompt/routing rule in OpenClaw workspace config.
3. Resolve unrelated Spencer identity default test failures in a dedicated patch.

## 2026-02-23 Plan Update - V4 Placeholder Target Guardrail

### Completed
- Fixed low-signal fallback bug that produced placeholder target text (`Acquire No ...`) in board-seat idea lines.
- Implementation in `src/coatue_claw/board_seat_daily.py`:
  - `ACQ_PLACEHOLDER_TARGETS` extended to include `no`.
  - `_target_candidates_from_seed(...)` now skips short candidate tokens (`len < 3`).
- Added regression test:
  - `tests/test_board_seat_daily.py::test_best_effort_idea_line_avoids_placeholder_no_target`.
- Validation:
  - targeted board-seat tests: `16 passed`.
  - full suite: unchanged unrelated Spencer identity failures only.

### In Progress
- Live validation of V4 board-seat outputs in `#openai` after runtime restart.

### Next
1. Prompt in `#openai`: `give me a new board seat idea` and verify the `Idea` target is concrete and non-placeholder.
2. If fallback quality is still weak in sparse-signal conditions, add deterministic company-specific fallback target maps.

## 2026-02-24 Plan Update - Memory-to-Git Reconciliation Policy v1

### Completed
- Implemented explicit `git-memory:` capture path in `src/coatue_claw/slack_bot.py`:
  - prefixed requests are captured as `request_kind=memory_git`, `trigger_mode=git_memory_prefix`
  - bot replies in-thread with queue id/status
  - runtime memory ingestion still occurs for the same message.
- Extended tracker schema + APIs in `src/coatue_claw/spencer_change_log.py`:
  - new columns: `request_kind`, `trigger_mode`, `source_ref`, `related_commit`
  - backward-compatible migration on startup for existing DBs
  - filtering support for `memory_git` queue views.
- Added deterministic reconciliation artifacts + commands:
  - `claw memory reconcile-status`
  - `claw memory reconcile-export --limit N` -> writes `docs/memory-inbox/queue.md`
  - `claw memory reconcile-link --ids ... --commit <hash>` -> updates status/commit and appends `docs/memory-inbox/reconciliation-ledger.csv`
- Added repo-tracked memory inbox artifacts:
  - `docs/memory-inbox/queue.md`
  - `docs/memory-inbox/reconciliation-ledger.csv`
- Updated operator docs:
  - `AGENTS.md`
  - `docs/laptop-codex-openclaw-workflow.md`
- Test coverage added/updated in:
  - `tests/test_spencer_change_log.py`
  - `tests/test_spencer_change_digest.py` (still green with new schema)

### Validation
- Targeted:
  - `PYTHONPATH=/opt/coatue-claw/src /opt/coatue-claw/.venv/bin/python -m pytest -q /opt/coatue-claw/tests/test_spencer_change_log.py /opt/coatue-claw/tests/test_spencer_change_digest.py` -> `14 passed`

### In Progress
- Runtime restart + Slack health verification on Mac mini after policy ship.

### Next
1. Post a live Slack message prefixed with `git-memory:` and confirm queue capture acknowledgment.
2. Run `spencer changes memory` to verify filtered queue listing.
3. Run `claw memory reconcile-export --limit 200` and confirm queue snapshot refresh in repo.

## 2026-02-24 Plan Update - Scheduled Memory Reconcile Export

### Completed
- Added launchd scheduler service in `src/coatue_claw/launchd_runtime.py`:
  - label: `com.coatueclaw.memory-reconcile-export`
  - command: `python -m coatue_claw.cli memory reconcile-export --limit <N>`
  - default interval: every 900 seconds (15 min)
  - env knobs:
    - `COATUE_CLAW_MEMORY_RECONCILE_INTERVAL_SECONDS` (default `900`, min `300`, max `86400`)
    - `COATUE_CLAW_MEMORY_RECONCILE_EXPORT_LIMIT` (default `200`, min `1`, max `1000`)
- Added explicit service selector support:
  - `launchd_runtime enable|status|disable --service memoryreconcile`
- Added operator Make targets:
  - `openclaw-memory-reconcile-status`
  - `openclaw-memory-reconcile-export`
- Updated runtime docs and tests:
  - `docs/openclaw-runtime.md`
  - `tests/test_launchd_runtime.py`

### Validation
- Targeted tests:
  - `PYTHONPATH=/opt/coatue-claw/src /opt/coatue-claw/.venv/bin/python -m pytest -q /opt/coatue-claw/tests/test_launchd_runtime.py /opt/coatue-claw/tests/test_spencer_change_log.py` -> `14 passed`
- Full suite:
  - `PYTHONPATH=/opt/coatue-claw/src /opt/coatue-claw/.venv/bin/python -m pytest -q` -> `222 passed`
- Runtime:
  - enabled new service: `python -m coatue_claw.launchd_runtime enable --service memoryreconcile`
  - status confirms loaded: `com.coatueclaw.memory-reconcile-export` (`last_exit_code=0`)
  - manual command checks:
    - `make openclaw-memory-reconcile-status`
    - `make openclaw-memory-reconcile-export`

### Next
1. Post one live `git-memory:` request in Slack and confirm queue snapshot updates within 15 minutes without manual export.
2. If you want tighter cadence, set `COATUE_CLAW_MEMORY_RECONCILE_INTERVAL_SECONDS` in `/opt/coatue-claw/.env.prod`, then re-run `make openclaw-24x7-enable`.

## 2026-02-24 Plan Update - Auto DM + Auto Capture for Behavior Change Requests

### Completed
- Implemented automatic memory-git capture for behavior-change requests in `src/coatue_claw/slack_bot.py`:
  - explicit prefix `git-memory: ...` (existing)
  - auto-detected natural-language behavior-change asks (new)
- On each captured request, bot now automatically:
  - appends entry to workspace `MEMORY.md` (configurable path)
  - refreshes reconciliation queue snapshot (`docs/memory-inbox/queue.md`)
  - sends immediate DM notification to Carson (configurable notify list)
- Added new trigger mode support in tracker schema code:
  - `auto_behavior_request` (alongside `manual`, `git_memory_prefix`)
- Added env knobs in docs:
  - `COATUE_CLAW_CHANGE_NOTIFY_USER_IDS`
  - `COATUE_CLAW_CHANGE_MEMORY_MD_PATH`

### Validation
- Targeted tests:
  - `PYTHONPATH=/opt/coatue-claw/src /opt/coatue-claw/.venv/bin/python -m pytest -q /opt/coatue-claw/tests/test_spencer_change_log.py /opt/coatue-claw/tests/test_spencer_change_digest.py /opt/coatue-claw/tests/test_launchd_runtime.py` -> `20 passed`
- Full suite:
  - `PYTHONPATH=/opt/coatue-claw/src /opt/coatue-claw/.venv/bin/python -m pytest -q` -> `223 passed`

### Next
1. Live-test in Slack with a plain-language behavior change request (without `git-memory:`) and verify:
   - in-thread queue acknowledgment
   - Carson DM
   - `spencer changes memory` shows new row.

## 2026-02-24 Plan Update - X Chart Source Repeat Cooldown (3 Days)

### Completed
- Updated winner selection in `src/coatue_claw/x_chart_daily.py` to enforce source-level repeat cooldown:
  - same account can be selected again only if its most recent posted chart is older than 3 days.
  - env knob added: `COATUE_CLAW_X_CHART_SOURCE_REPEAT_DAYS` (default `3`, clamped `0..30`).
  - cooldown is applied before source-variety ranking.
  - starvation guard: if all candidates are within cooldown, selection falls back to normal score ordering (post still proceeds).
- Added regression tests in `tests/test_x_chart_daily.py`:
  - `test_pick_winner_enforces_source_repeat_cooldown_with_alternative`
  - `test_pick_winner_allows_recent_source_when_no_alternative`

### Validation
- `PYTHONPATH=src python3 -m pytest -q tests/test_x_chart_daily.py -k "pick_winner"` -> `4 passed`
- `PYTHONPATH=src python3 -m pytest -q tests/test_x_chart_daily.py` -> `76 passed`

### Next
1. Monitor next scheduled posts and confirm same-source repetition does not occur within 3 days when alternatives exist.
2. If feed quality drops due to cooldown pressure, tune `COATUE_CLAW_X_CHART_SOURCE_REPEAT_DAYS` to `2` and re-check.

## 2026-02-24 Plan Update - X Chart Preferred Sources + Style-Quality Scoring

### Completed
- Updated default prioritized X sources in `src/coatue_claw/x_chart_daily.py`:
  - added `stock_unlock` (1.45)
  - added `stripe` (1.4)
- Added deterministic style-quality scoring to candidate ranking:
  - institutional/data-dense chart language gets a positive boost
  - promo CTA patterns and cashtag-heavy spam patterns get penalties
  - integrated into `_score_candidate(...)` as `style_component`
- Added tests in `tests/test_x_chart_daily.py`:
  - `test_store_seeds_priority_sources`
  - `test_score_candidate_boosts_institutional_chart_language`
  - `test_score_candidate_penalizes_cashtag_spam_with_cta`

### Validation
- `PYTHONPATH=src python3 -m pytest -q tests/test_x_chart_daily.py` -> `78 passed`

### Next
1. Monitor the next 1-2 scheduled chart windows and confirm preferred-source quality uplift appears in winner set without overfitting to account priority.
2. If needed, tune source priorities for `stock_unlock` and `stripe` by ±0.1 based on observed hit rate.

## 2026-02-24 Plan Update - X Chart Topic Tags + Additional Preferred Handles

### Completed
- Expanded chart-of-day topic tag coverage in `src/coatue_claw/x_chart_daily.py` for Spencer-preferred content themes:
  - fundamental inflection signals (`backlog`)
  - AI infra second-order effects (`data center power`, `power demand`)
  - market internals/regime (`breadth`, `rotation`, `dispersion`, `regime`)
  - positioning language (`positioning`, `underallocated`, `stock pickers`)
- Added referenced accounts to default source consideration list:
  - `MikeZaccardi` (`1.3`)
  - `oguzerkan` (`1.25`)
  - (existing preferred list already includes `fiscal_AI`, `stock_unlock`, `stripe`)
- Updated tests in `tests/test_x_chart_daily.py`:
  - extended source-seed assertions for `mikezaccardi` and `oguzerkan`
  - added `test_score_candidate_boosts_preferred_topic_tags`

### Validation
- `PYTHONPATH=src python3 -m pytest -q tests/test_x_chart_daily.py` -> `79 passed`

### Next
1. Observe next 2-3 scheduled posts and confirm more winners contain preferred topic tags without reducing diversity too aggressively.
2. If too concentrated, lower `MikeZaccardi`/`oguzerkan` priority by `0.05-0.1` while keeping tag scoring intact.

- Integrator deploy completed for Market Daily no-X patch on `main` (merge: `d6c2bdd`):
  - merged role-branch commit `f0e13f1` (`market-daily: remove X evidence and links from MD outputs`).
  - MD production behavior now excludes X evidence/links from MD outputs:
    - mover lines render `[News]/[Web]` only
    - MD footer text: `Yahoo fast_info + Yahoo news + web search`
    - debug links payload excludes `x`
    - earnings recap path remains no-X and handles `no_reporters` cleanly
  - post-merge runtime checks passed on Mac mini:
    - `tests/test_market_daily.py` (`40 passed`)
    - `tests/test_launchd_runtime.py` (`8 passed`)
    - `openclaw` Slack probe healthy after restart
  - immediate next patch: Intel headline-quality hardening to reject generic quote-wrapper titles as catalyst phrases.

- Market Daily Intel headline-quality hardening shipped on `main` (post no-X deploy):
  - deterministic wrapper-title rejection added for quote-directory strings (e.g., stock price/quote-history wrappers)
  - applied in candidate normalization, direct-cause selection, cluster phrase generation, and reason-line rendering
  - INTC-like wrapper phrase path now resolves to specific causal phrase or fallback (never wrapper text in rendered line)
  - validation:
    - `tests/test_market_daily.py` -> `43 passed`
    - `tests/test_launchd_runtime.py` -> `8 passed`
  - runtime check:
    - forced MD close run now shows INTC fallback line instead of wrapper-title phrase
    - MD footer remains no-X (`Yahoo fast_info + Yahoo news + web search`)

- Integrator merged latest Market Daily role branch to `main` (`c5f6eca`), including `e9ae7d8`:
  - no-X MD output policy remains enforced (`[X]` removed from mover lines, no X in source footer)
  - quote-directory wrapper hardening merged for catalyst phrase rejection
- Post-merge runtime verification (Mac mini):
  - Slack probe healthy after restart
  - forced MD close run artifact confirms footer and INTC fallback line quality
  - earnings recap force-run returns clean `no_reporters`
- Additional main follow-up hardening (same deploy cycle):
  - reject `why ... stock/shares ... today|now` phrasing as wrapper-like cause text to avoid weak reason lines in direct-evidence path
- Environment note:
  - checklist `python3 -m pytest` commands fail on mini due missing global pytest; tests must run via `/opt/coatue-claw/.venv/bin/python -m pytest`.

- Integrator deployed latest Market Daily role-branch updates to `main` (merge `6c5f51d`, includes `94b8180`):
  - no-X policy enforced in MD post lines/footer/debug link map
  - quote-directory wrapper catalyst rejection enforced (INTC quality hardening)
  - grammar hardening active with render diagnostics (`cause_render_mode`, raw/final cause phrase fields)
- Verification state on Mac mini:
  - forced MD run shows no `[X]` and footer `Yahoo fast_info + Yahoo news + web search`
  - INTC rendered as fallback instead of quote-directory text
  - earnings recap force-run returned clean `no_reporters`
- Ops note:
  - system `python3` lacks pytest; validation on mini must use `/opt/coatue-claw/.venv/bin/python -m pytest`.

- Integrator deployed Market Daily `simple_synthesis` from role branch (`f71eaa1`) to `main` via merge `c862aaf`.
- Active MD catalyst policy on main:
  - default `simple_synthesis` generation path
  - no-X output/link policy preserved
  - quote-directory wrapper rejection preserved
  - hybrid polish + aggressive fallback diagnostics exposed in debug payloads
- Production checks:
  - status reports synthesis defaults (`mode=max_results/source_mode/domain_gate/force_best_guess`)
  - forced close run outputs clean causal/fallback lines with no X links
  - INTC no longer emits quote-directory catalyst text
- Ops note:
  - mini system `python3` is not the runtime interpreter for this module/tests; use `/opt/coatue-claw/.venv/bin/python` for validation/CLI.

- Market Daily time-integrity guardrails are deployed on `main` (`d59ca97`, includes `43c84ed`):
  - strict in-window publish-time filtering enabled by default
  - stale historical callback narratives rejected from synthesis candidates
  - publish-time enrichment enabled with bounded timeout (1200ms)
  - MD links now constrained to time-valid evidence
- Operational verification:
  - INTC debug confirms stale Morgan callback rejection and non-empty time-integrity diagnostics
  - selected synthesis URLs include in-window Yahoo Intel "why stock soaring" candidate (or equivalent in-window replacement)
  - close artifact footer remains no-X: `Yahoo fast_info + Yahoo news + web search`
- Integrator compatibility note:
  - mini system `python3` should not be used for this module/tests; use `/opt/coatue-claw/.venv/bin/python`.

- Market Daily catalyst quality recovery deployed on `main`:
  - merge `bee2d68` from `origin/codex/agent-market-daily` (includes `c8736fa`)
  - quality behavior confirmed:
    - no-X links/footer in close + recap outputs
    - stale historical callback links rejected under time-integrity gates
    - simple synthesis requires Google SERP key and does not DDG-fallback when missing
    - weak-evidence path can cleanly return fallback line when LLM/source support is unavailable
- Integrator follow-up improvement on `main`:
  - prefer top selected in-window source URL for rendered link selection in simple synthesis to avoid weak TA/roundup link wins over stronger explainers.
- Operational note:
  - use `/opt/coatue-claw/.venv/bin/python` for tests/CLI on mini (system `python3` is not runtime-compatible for this module).

- Market Daily anchor-first free-sentence catalyst labeling is live on `main` (`d8bf379`, includes `8193c58`).
- Current verified behavior:
  - close mover lines render as free sentences (`post_as_is`), not forced wrapper phrasing.
  - no `[X]` links and no X footer mention in close/recap outputs.
  - INTC debug exposes anchor/support diagnostics and strict time-integrity rejections (including stale historical callback rejection).
  - production defaults active: `reason_output_mode=free_sentence`, `synth_support_count=2`, `post_as_is=1`.
- Operational guardrail:
  - on mini, use `/opt/coatue-claw/.venv/bin/python -m pytest` for Market Daily validation; host `python3` lacks pytest.
- Next quality follow-up (if needed):
  - enforce rendered link preference to exactly the selected anchor URL when an in-window anchor is present, rather than allowing support URL substitution in `links.web`.

- Market Daily INTC catalyst correction (consensus event + no attribution) is now deployed on `main`:
  - consensus-first winner replaces anchor-first for simple synthesis sentence selection.
  - publisher attribution stripping is enforced in final mover sentence output.
  - sentence-family consistency guard prevents outlier narrative drift.
  - support links are filtered/aligned to the consensus event family.
- Debug contract additions on `debug-catalyst` payload:
  - `consensus_event_family`, `consensus_winner_url`, `attribution_stripped`
- Current acceptance status:
  - INTC close line now surfaces partnership catalyst (SambaNova) without Reuters attribution.
  - footer stays no-X (`Yahoo fast_info + Yahoo news + web search`).
  - market daily + launchd runtime test suites pass on mini venv (`72 passed`, `8 passed`).

- Deployed on `main` via cherry-pick (market-daily-only) from role branch:
  - commit `0f78583` (`market-daily: rewrite earnings recap to anchor-first end-to-end`)
- Verification state:
  - recap service force-run posts successfully when reporters are present.
  - recap bullet count contract (2–4) validated on latest run.
  - no-X policy remains intact in recap output/footer.
  - deterministic backup path remains coherent when LLM is unavailable.
- Remaining runtime nuance to monitor:
  - when recap evidence hydration yields `none`, citation-handle bullets (`[S1]/[S2]/[S3]`) cannot be emitted for those rows in that run.

## Board Seat - Company-Only Target Enforcement (2026-02-25)
- Status: implemented on `codex/agent-board-seat`, validated.
- Completed scope:
  - strict company-target resolution layer added before target gating/rendering.
  - default alias map includes `next.js -> Vercel`; env JSON override/extension supported.
  - non-company target shapes (product/framework forms) are deterministically retargeted via alias first, then fallback rotation/default.
  - run payload observability now includes `target_original` and `target_resolution_reason`.
  - `.env.example` updated with:
    - `COATUE_CLAW_BOARD_SEAT_REQUIRE_COMPANY_TARGET=1`
    - `COATUE_CLAW_BOARD_SEAT_TARGET_COMPANY_ALIAS_JSON={"next.js":"Vercel"}`
- Governance unchanged:
  - high/medium confidence policy, new-target requirement, hard 14-day no-repeat, repitch significance.
- Validation:
  - `PYTHONPATH=src python3 -m pytest -q tests/test_board_seat_daily.py` -> `55 passed`.

## Board Seat - Truncation Readability Fix (2026-02-25)
- Status: implemented on `codex/agent-board-seat`, validated.
- Completed scope:
  - line normalization now trims incomplete short trailing sentence fragments introduced by strict word caps.
  - prevents clipped outputs like `... Vercel. Migrate` while keeping existing 18-word cap and format contract.
- Validation:
  - `PYTHONPATH=src python3 -m pytest -q tests/test_board_seat_daily.py` -> `57 passed`.

## Board Seat - Writing Quality Recovery (2026-02-25)
- Status: implemented on `codex/agent-board-seat`, validated.
- Completed scope:
  - default no-cap line policy via `COATUE_CLAW_BOARD_SEAT_MAX_LINE_WORDS=0` (legacy capped behavior still available with positive values).
  - writing mode/env controls added: passthrough mode + obvious artifact stripping.
  - LLM draft path now receives a concrete evidence pack from target/acquisition/funding retrieval before generation.
  - prompt updated to request field-specific, non-duplicative thesis writing and removed hard 18-word instruction.
  - sanitize path now:
    - strips obvious artifacts from LLM fields
    - applies exact duplicate-field guard across thesis lines
    - records `writing_artifact_cleanups` + `writing_field_dedup_fixes`.
  - run payload rows (`sent` and gate-based `skipped`) now include writing observability fields.
- Governance unchanged:
  - company-only target enforcement, confidence/new-target gate, hard 14-day no-repeat, and repitch significance.

## Board Seat - Target Extractor Hardening (2026-02-25)
- Status: in progress on `codex/agent-board-seat` (shipped tests), pending integrator merge.
- Completed in this update:
  - expanded non-company target rejection for leaked placeholders and business-model acronyms (`there`, `d2c`, `director`, etc.).
  - added conceptual adjective rejection for `ai/llm/model` + `focused/first/native/driven`.
  - added regression tests covering these failure patterns.
- Validation:
  - `PYTHONPATH=src python3 -m pytest -q tests/test_board_seat_daily.py` -> `63 passed`.
- Next:
  - add a post-sanitize target-lock re-check so retargeted outputs cannot bypass cooldown/new-target governance when final target differs from initial extraction.

## Market Daily - Earnings Recap Manual Slot Dedupe Verification (2026-02-26)
- Verified on main in /opt/coatue-claw.
- Behavior for 95ddd47 is present: daytime manual runs use earnings_recap_manual and do not consume scheduled recap slot.
- Validation: tests/test_market_daily.py 79 passed; tests/test_launchd_runtime.py 10 passed.
- Runtime: recap scheduler service com.coatueclaw.market-daily-earnings-recap loaded.
- DB checks confirmed slot separation between earnings_recap_manual and earnings_recap.
- Scheduled-path dry-run result in this window was no_reporters.

## Market Daily - Article Context Grounding Deploy (2026-02-27)
- Deployed commit d817abc (main commit d0b440f) to add richer article context for LLM relevance + drafting.
- Test results: market_daily 81 passed; launchd_runtime 10 passed (venv).
- Runtime health: openclaw restart complete; slack probe healthy.
- Status keys present: article_context_enabled, article_context_timeout_ms, article_context_max_chars, article_context_limit; relevance_mode remains llm_first.
- Dry-run + live smoke executed; artifact generated and posted.
- NFLX debug close shows contextual synthesis fields active, with remaining improvement opportunity in source-anchor quality selection.

## HFA Update (2026-02-27)
- `hfa analyze` is now fail-closed when model output cannot be produced/parsed.
- Removed fallback-draft rendering path from `analyze_thread` in `src/coatue_claw/hf_analyst.py`.
- Operator-visible behavior:
  - returns explicit failure reasons (`analysis_generation_failed:<reason>`) instead of fallback memo content.
- Validation:
  - `PYTHONPATH=src python3 -m pytest -q tests/test_hf_analyst.py tests/test_hf_document_extract.py tests/test_slack_routing.py tests/test_hf_youtube_transcript.py` -> `23 passed`
  - `PYTHONPATH=src python3 -m compileall -q src` -> pass

## HFA Runtime-Control Update (2026-02-27)
- HFA output style is now controllable from Slack via runtime memory.
- New commands:
  - `hfa control show`
  - `hfa control mode strict|freeform`
  - `hfa control instruction <text>`
  - `hfa control clear`
- Thread-doc analyzer now reads these controls at run time:
  - `strict` mode preserves structured HFA contract.
  - `freeform` mode allows operator-instructed markdown output.
- Validation:
  - `PYTHONPATH=src python3 -m pytest -q tests/test_hf_analyst.py tests/test_memory_runtime.py tests/test_hf_podcast.py tests/test_slack_routing.py tests/test_hf_youtube_transcript.py` -> `25 passed`
  - `PYTHONPATH=src python3 -m compileall -q src` -> pass

## HFA Runtime-Control Simplification (2026-02-27)
- HFA output mode now has a single mode: `freeform`.
- Mode switching removed from practical behavior; operator control is now via memory-backed instruction text only.
- Slack control commands retained for operations:
  - `hfa control show`
  - `hfa control instruction <text>`
  - `hfa control clear`
  - `hfa control mode freeform` (idempotent)
- validation:
  - `PYTHONPATH=src python3 -m pytest -q tests/test_hf_analyst.py tests/test_memory_runtime.py tests/test_hf_podcast.py tests/test_slack_routing.py tests/test_hf_youtube_transcript.py` -> `26 passed`
  - `PYTHONPATH=src python3 -m compileall -q src` -> pass

## HFA Implicit-Control Update (2026-02-27)
- HFA now captures natural-language format-change requests in Slack and writes them to runtime memory for future `hfa analyze` runs.
- Explicit command still supported: `hfa control instruction <text>`.
- Implicit examples now supported:
  - `for hfa analyze, use this format: ...`
  - `going forward format as: ...` (in an HFA thread context)
- validation:
  - `PYTHONPATH=src python3 -m pytest -q tests/test_hf_analyst.py tests/test_memory_runtime.py tests/test_hf_podcast.py tests/test_slack_routing.py tests/test_hf_youtube_transcript.py` -> `27 passed`
  - `PYTHONPATH=src python3 -m compileall -q src` -> pass
