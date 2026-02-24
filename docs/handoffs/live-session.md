# Live Session Handoff (Coatue Claw)

## Objective
Ship valuation charting into the OpenClaw-native Slack workflow.

## Update (2026-02-24, Board Seat Readability V3 labeled hierarchy)
- Implemented Board Seat readability V3 in `/opt/coatue-claw/src/coatue_claw/board_seat_daily.py`.
- Format is now deterministic labeled-line hierarchy (no bullet subheaders):
  - `*Thesis*` with fixed lines:
    - `*Why now:* ...`
    - `*What's different:* ...`
    - `*MOS/risks:* ...`
    - `*Bottom line:* ...`
  - `*{Company} context*` with fixed lines:
    - `*Current efforts:* ...`
    - `*Domain fit/gaps:* ...`
  - `*Funding snapshot*` with fixed lines:
    - `*History:* ...`
    - `*Latest round/backers:* ...`
- `format_version` bumped from `v2_thesis_context_funding` to `v3_labeled_hierarchy`.
- `BoardSeatDraft` moved from list-based bullet fields to fixed labeled fields:
  - `why_now`, `whats_different`, `mos_risks`, `bottom_line`
  - `context_current_efforts`, `context_domain_fit_gaps`
  - `funding_history`, `funding_latest_round_backers`
- Added per-line sanitization and validation:
  - each required line must be present and non-empty
  - each line is capped to `MAX_LINE_WORDS=18`
  - strips accidental leading bullet markers (`-`, `•`) and trims noisy suffix punctuation
- Extended parsing for repeat-guardrail compatibility:
  - supports V3 labeled lines
  - still supports V2 bullet sections and legacy 5-line posts (`Signal`, `Board lens`, `Watchlist`)
  - repeat signature continues to focus on thesis/context core signal
- Funding resolver/cache behavior kept unchanged (manual seed -> fresh cache -> web refresh -> unknown fallback).
- Unknown funding now renders explicitly in both funding lines:
  - `History: Funding details are currently unavailable.`
  - `Latest round/backers: Funding details are currently unavailable.`
- Runtime bugfix retained: Slack `SlackResponse` parsing in channel/history fetch paths.

### Validation (this session)
- Targeted board-seat tests:
  - `PYTHONPATH=/opt/coatue-claw/src /opt/coatue-claw/.venv/bin/python -m pytest -q /opt/coatue-claw/tests/test_board_seat_daily.py`
  - Result: `12 passed`
- Full suite smoke:
  - `PYTHONPATH=/opt/coatue-claw/src /opt/coatue-claw/.venv/bin/python -m pytest -q`
  - Result: `3` unrelated/pre-existing failures in Spencer change label defaults:
    - `tests/test_spencer_change_digest.py::test_run_once_dry_run_includes_carson_label`
    - `tests/test_spencer_change_log.py::test_is_spencer_user_defaults`
    - `tests/test_spencer_change_log.py::test_requester_label_defaults`
- Dry-run check for hierarchy output:
  - `COATUE_CLAW_BOARD_SEAT_PORTCOS='OpenAI:openai' ... run-once --force --dry-run`
  - Confirmed labeled-line hierarchy and spacing in preview output.
- Live Anduril run check:
  - `COATUE_CLAW_BOARD_SEAT_PORTCOS='Anduril:anduril' ... run-once --force`
  - Expected skip preserved: `repeat_investment_without_significant_change`.

## Update (2026-02-24, Board Seat V2 format + funding snapshot)
- Implemented Board Seat V2 in `/opt/coatue-claw/src/coatue_claw/board_seat_daily.py`:
  - deterministic sectioned output for all portcos:
    - `*Board Seat as a Service — {Company}*`
    - `*Thesis*` (max 2 bullets)
    - `*{Company} context*` (max 2 bullets)
    - `*Funding snapshot*` (max 2 bullets)
  - hard formatting guardrails:
    - max 6 bullets total
    - max 20 words per bullet
    - legacy labels (`Signal`, `Board lens`, `Watchlist`, `Team ask`) removed from generation/fallback paths
- Added structured generation and rendering:
  - new dataclasses: `FundingSnapshot`, `BoardSeatDraft`
  - model path now generates JSON draft bullets, then deterministic sanitizer/renderer enforces final shape
- Added funding data pipeline with additive DB cache table:
  - new SQLite table: `board_seat_funding_cache`
  - resolver order:
    1) manual seed JSON (`COATUE_CLAW_BOARD_SEAT_FUNDING_MANUAL_PATH`)
    2) fresh cache (`COATUE_CLAW_BOARD_SEAT_FUNDING_TTL_DAYS`, default 14)
    3) Brave web refresh + extraction
    4) explicit unknown snapshot fallback
  - source tracking emits `manual_seed` / `cache` / `web_refresh` / `unknown`
- Preserved and extended repeat guardrail behavior:
  - repeat detection still blocks stale repeats (`repeat_investment_without_significant_change`)
  - investment parsing now supports V2 sections; core repeat signal remains thesis/context-focused
- Added run/status diagnostics:
  - `run_once(...).sent[*]` now includes:
    - `format_version`
    - `funding_source_type`
    - `funding_as_of_utc`
    - `funding_unknown`
  - `status()` now includes:
    - `format_version`
    - `funding_cache_age_days_by_company`
    - `funding_data_source_by_company`

### Validation (this session)
- Targeted:
  - `PYTHONPATH=/opt/coatue-claw/src /opt/coatue-claw/.venv/bin/python -m pytest -q /opt/coatue-claw/tests/test_board_seat_daily.py`
  - Result: `13 passed`
- Full smoke:
  - `PYTHONPATH=/opt/coatue-claw/src /opt/coatue-claw/.venv/bin/python -m pytest -q`
  - Result: `3` failures outside board-seat module (pre-existing Spencer label defaults):
    - `tests/test_spencer_change_digest.py::test_run_once_dry_run_includes_carson_label`
    - `tests/test_spencer_change_log.py::test_is_spencer_user_defaults`
    - `tests/test_spencer_change_log.py::test_requester_label_defaults`

### Next Steps
1. Run live `board seat run-once --force` and verify Slack output in `#anduril` matches V2 structure and skim budget.
2. Add/seed `COATUE_CLAW_BOARD_SEAT_FUNDING_MANUAL_PATH` JSON for top portcos to improve funding quality and reduce web variance.
3. Separately fix Spencer change-tracker default user mapping to restore full-suite green.

## Update (2026-02-24, runtime follow-up)
- Runtime rollout executed:
  - `make openclaw-restart`
  - `make openclaw-slack-status` (probe `ok`, Slack account healthy)
  - `board_seat_daily status` confirms `format_version: v2_thesis_context_funding`
- Fixed a live-runtime bug in `board_seat_daily` Slack pagination/parsing:
  - `SlackResponse` objects were being treated as plain `dict` in channel/history fetch paths.
  - channel resolution now works with `channel_ref=anduril` and resolves `channel_id` correctly.
- Live Anduril run currently skips posting due to preserved repeat guardrail:
  - reason: `repeat_investment_without_significant_change`
  - this is expected behavior with unchanged recent context.

## Update (2026-02-23, title/takeaway role + sentence integrity)
- Fixed title/takeaway inversion class in X chart posts:
  - title is now enforced as the concise sentence
  - takeaway is enforced as the fuller contextual sentence
  - deterministic role correction added (`title_takeaway_role_swapped`)
- Removed post/render-time takeaway clipping that was stripping punctuation and causing fragments.
- Added single-sentence takeaway guardrails:
  - new validator check for one complete sentence (`takeaway_single_sentence`)
  - malformed comparative fragments (`to lowest`/`to highest`) are rejected and rewritten
- Added renderer takeaway fit behavior:
  - wrap up to 2 lines
  - shrink font to floor
  - one semantic shorten pass if needed
  - fail publish if still not fit
- Added diagnostics in outputs/reviews:
  - `title_takeaway_role_swapped`
  - `takeaway_single_sentence`
  - `takeaway_wrapped_line_count`
- Explicit URL flow improved:
  - if strict candidate parse filters out the post, fallback now builds a candidate directly from the fetched tweet payload before using vxtwitter fallback

### Validation (this session)
- Targeted tests:
  - `PYTHONPATH=/opt/coatue-claw/src /opt/coatue-claw/.venv/bin/python -m pytest -q /opt/coatue-claw/tests/test_x_chart_daily.py /opt/coatue-claw/tests/test_slack_x_chart_intent.py`
  - Result: `74 passed`
- Live post check:
  - `run-post-url https://x.com/KobeissiLetter/status/2026040229535047769`
  - posted successfully with `copy_rewrite_reason=title_takeaway_role_swapped` and review checks passed (`takeaway_single_sentence=true`, `title_takeaway_role_ok=true`)
- Full suite smoke:
  - `PYTHONPATH=/opt/coatue-claw/src /opt/coatue-claw/.venv/bin/python -m pytest -q`
  - Result: `3` pre-existing Spencer change-tracker failures (unchanged):
    - `tests/test_spencer_change_digest.py::test_run_once_dry_run_includes_carson_label`
    - `tests/test_spencer_change_log.py::test_is_spencer_user_defaults`
    - `tests/test_spencer_change_log.py::test_requester_label_defaults`

## Update (2026-02-23, headline truncation guardrails)
- Implemented headline truncation fix for X chart posts by removing hard character clipping from headline generation and shifting to layout-based fitting in render paths.
- Headline policy is now a complete sentence (terminal punctuation + action verb), with locked finance term integrity checks (for example `market cap`, `enterprise value`, `free cash flow`).
- Renderers now fit headlines with wrap + font-size adaptation (up to 3 lines) and perform one semantic rewrite pass before marking overflow unpublishable.
- Added explicit title override support for URL-post flows:
  - CLI: `run-post-url <url> --title "<full sentence>"`
  - Slack compound URL flow: supports `title: ...`
- Added/updated diagnostics and review checks in chart run outputs:
  - `copy_rewrite_applied`
  - `copy_rewrite_reason`
  - `candidate_fallback_used`
  - `headline_complete_sentence`
  - `headline_wrapped_line_count`
  - `headline_complete_phrase` retained for backward compatibility.
- Quality gates now reject incomplete headline sentences and missing locked terms before publishing.

### Validation (this session)
- Targeted tests:
  - `PYTHONPATH=/opt/coatue-claw/src /opt/coatue-claw/.venv/bin/python -m pytest -q /opt/coatue-claw/tests/test_x_chart_daily.py /opt/coatue-claw/tests/test_slack_x_chart_intent.py`
  - Result: `70 passed`.
- Full suite smoke:
  - `PYTHONPATH=/opt/coatue-claw/src /opt/coatue-claw/.venv/bin/python -m pytest -q`
  - Result: `3` pre-existing failures in Spencer change tracker modules:
    - `tests/test_spencer_change_digest.py::test_run_once_dry_run_includes_carson_label`
    - `tests/test_spencer_change_log.py::test_is_spencer_user_defaults`
    - `tests/test_spencer_change_log.py::test_requester_label_defaults`
  - These failures are outside the X chart files touched in this patch.

### Next Steps
1. Run a live Slack post test in `#charting` with a known long headline containing locked terms (`market cap`) and confirm no truncation.
2. Run explicit URL post with `title:` override in Slack and verify override passes/fails deterministically under sentence + locked-term validation.
3. If desired, normalize Spencer change-tracker defaults in a separate patch to restore full-suite green.

## Current Status (2026-02-23)
- OpenAI model policy updated to premium defaults (no frugal mode):
  - `COATUE_CLAW_BOARD_SEAT_MODEL` default -> `gpt-5.2-chat-latest`
  - `COATUE_CLAW_X_CHART_TITLE_MODEL` default -> `gpt-5.2-chat-latest`
  - `COATUE_CLAW_X_CHART_VISION_MODEL` default -> `gpt-4.1`
  - `COATUE_CLAW_MEMORY_EMBED_MODEL` default -> `text-embedding-3-large`
  - `COATUE_CLAW_MD_MODEL` default -> `gpt-5.2-chat-latest`
  - Mac mini `.env.prod` set to these premium runtime values and OpenClaw restarted.
- MD BKNG catalyst miss fix is now shipped with Google-first evidence:
  - web retrieval is now `google_serp` primary with `ddg_html` fallback (instead of DDG-only)
  - Google payload parsing now includes `organic_results`, `news_results`, and `answer_box` snippets
  - retrieval depth increased (`COATUE_CLAW_MD_WEB_MAX_RESULTS=20` default)
  - BKNG-specific query templates + aliases added (`Booking Holdings`, `Booking.com`, `OTA`)
  - new BKNG causal clusters:
    - `ota_ai_disruption`
    - `travel_demand_outlook`
  - scoring now upweights snippet-level causal phrases and only blocks generic wrappers when they lack specific event content
  - debug output now shows:
    - top 5 evidence candidates
    - selected cluster + cluster scoring diagnostics
    - web backend used (`google_serp` vs `ddg_html`)
  - decisive defaults tuned to reduce over-fallback:
    - `COATUE_CLAW_MD_DECISIVE_PRIMARY_REASON_MIN_SCORE=0.60`
    - `COATUE_CLAW_MD_DECISIVE_PRIMARY_REASON_MIN_MARGIN=0.03`
  - tests:
    - `PYTHONPATH=src pytest -q tests/test_market_daily.py` => `28 passed`
    - `PYTHONPATH=src pytest -q` => `162 passed`
- MD cause wording is now less conservative by default (decisive-primary mode):
  - if one high-quality source clearly dominates the evidence cluster, MD now states the primary reason directly
  - strict generic-wrapper blocklist still applies (no `stock down today` / `news today` style lines)
  - fallback line remains only when evidence is weak/ambiguous:
    - `Likely positioning/flow; no single confirmed catalyst.`
  - new knobs:
    - `COATUE_CLAW_MD_DECISIVE_PRIMARY_REASON_ENABLED` (default `1`)
    - `COATUE_CLAW_MD_DECISIVE_PRIMARY_REASON_MIN_SCORE` (default `0.60`)
    - `COATUE_CLAW_MD_DECISIVE_PRIMARY_REASON_MIN_MARGIN` (default `0.03`)
  - test added:
    - `test_single_strong_quality_source_can_drive_decisive_primary_reason`
- MD basket-cause coherence now handles the NET/CRWD Anthropic case:
  - if one cybersecurity mover has confirmed `anthropic_claude_cyber`, peer cybersecurity selloff movers in the same run inherit that same cause phrase
  - directional scoring now penalizes upbeat partnership headlines for down-move cause selection
  - tests added:
    - `test_cyber_basket_carries_anthropic_cause_to_net`
- ORCL/generic-cause wording fix:
  - removed generic `deal_contract` fallback phrase (`a major deal or contract update changed sentiment`)
  - deal/contract lines now use the concrete selected headline event text
  - Yahoo headline relevance filter now requires ticker/alias mention, preventing unrelated cross-company headlines in ORCL/NET lines
  - decisive-primary logic now allows strong explicit event headlines (for example lawsuits) even when the top-vs-second cluster score gap is small
  - fixed cross-mover phrase contamination bug:
    - generic clusters like `deal_contract` no longer reuse phrases across movers
    - cluster reuse is now limited to high-specific clusters (for example `anthropic_claude_cyber`)
  - expanded wrapper blocklist for weak templates like:
    - `why ... shares ... today`
    - `shares ... trading lower today`
- MD specific-cause enforcement is now shipped for selloffs (NET/CRWD Anthropic case class):
  - cause naming now requires corroboration gate: at least 2 independent sources + 2 distinct domains + at least one quality domain
  - evidence is now normalized/deduped by canonical URL + title fingerprint, including DDG absolute redirect unwrapping
  - generic wrappers are blocked from final lines (`why ... stock down today`, `news today`, ticker-only fragments)
  - added explicit cause cluster `anthropic_claude_cyber` with deterministic event phrase:
    - `Anthropic launched Claude Code Security.`
  - final reason lines now use deterministic template when corroborated:
    - negative: `Shares fell after <event>.`
    - positive: `Shares rose after <event>.`
  - when corroboration fails, fallback is now:
    - `Likely positioning/flow; no single confirmed catalyst.`
  - shared basket-event reuse is enabled across movers in the same run (for example NET + CRWD share the same Anthropic cause phrase)
  - debug payload now includes:
    - `confirmed_cluster`, `confirmed_cause_phrase`, `corroborated_sources`, `corroborated_domains`
  - tests added/updated in `tests/test_market_daily.py`:
    - generic wrapper blocking
    - Anthropic cluster extraction mapping
    - corroboration gate behavior
    - NET/CRWD shared-cluster reason reuse
    - single-source fallback behavior
  - local validation:
    - `PYTHONPATH=src pytest -q` => `155 passed`
- MD catalyst reliability fix for NET/Anthropic-class misses is now implemented in repo:
  - source coverage expanded from X+Yahoo-only to X + Yahoo + Google SERP primary + DDG fallback (when confidence is weak)
  - Yahoo parser now supports both legacy yfinance fields and nested `content.*` schema (`pubDate`, `title`, `clickThroughUrl.url`/`canonicalUrl.url`)
  - evidence windows are now session-anchored instead of fixed-hour:
    - open slot starts at previous regular-session close
    - close slot starts at same-day session open
    - lookback cap enforced by `COATUE_CLAW_MD_MAX_LOOKBACK_HOURS` (default `96`)
  - X retrieval now uses richer ticker+alias query and configurable depth (`COATUE_CLAW_MD_X_MAX_RESULTS`, default `50`)
  - evidence scoring now includes source quality, recency, mention strength, driver keywords, and directional move-aware ranking
  - fallback reason line now uses concise default:
    - `Likely positioning/flow; no single confirmed catalyst.`
  - Slack output style remains unchanged (📈/📉 + concise reason + source links), with optional `[Web]` link when web fallback is used
  - markdown artifact now includes evidence diagnostics per mover:
    - confidence, chosen source, driver keywords, top evidence considered, and reject reasons
  - new debug surfaces shipped:
    - CLI: `claw market-daily debug-catalyst <TICKER> [--slot open|close]`
    - Slack: `md debug <TICKER> [open|close]`
  - tests added/updated in `tests/test_market_daily.py`:
    - nested Yahoo schema parsing
    - Monday-open previous-close session window behavior
    - DDG fallback parsing
    - short-ticker alias relevance behavior
    - debug output shape
  - local validation:
    - `PYTHONPATH=src pytest -q tests/test_market_daily.py tests/test_launchd_runtime.py tests/test_slack_channel_access.py tests/test_slack_pipeline.py tests/test_slack_routing.py`
    - `26 passed`
  - local dry-run to `/opt/coatue-claw-data` cannot execute from laptop sandbox due path permission; runtime verification is required on Mac mini.
- Follow-on MD tuning shipped after initial reliability patch:
  - fallback now triggers on weak directional signal and low source diversity (not only low score)
  - catalyst selection now applies move-direction-aware ranking (down movers prefer down-cause evidence)
  - reason-line fallback now respects the chosen evidence source instead of blindly preferring Yahoo headline order
  - added regression tests for direction-aware fallback and negative-move evidence selection
  - full suite validation after tuning: `PYTHONPATH=src pytest -q` => `149 passed`

## Current Status (2026-02-20)
- MD Slack output style tightened for readability and user-facing copy:
  - mover lines now include only directional emoji:
    - up movers: `📈`
    - down movers: `📉`
  - removed universe line from Slack post body
  - replaced with slot-aware opener:
    - `3 biggest movers this morning:`
    - `3 biggest movers this afternoon:`
  - catalyst line sanitization now strips hashtags, cashtags, handles, URLs, and non-MD emoji
  - catalyst generation now enforces causal wording (`after/on/as/amid`) so each line explains why the stock moved
  - retained source links only (`[X]` / `[News]`) per mover
  - added relevance filtering for X evidence on ambiguous tickers:
    - short symbols (3 chars or less) now require cashtag usage (for example `$NET`)
    - finance-keyword gate blocks non-market chatter
    - noise-term guard filters sports/context collisions (for example `run rate`, `match`)
  - catalyst coherence guard added:
    - truncates to a single readable sentence
    - for vague X-only snippets, falls back to a clean company-specific-driver fallback line
  - tests updated:
    - `tests/test_market_daily.py::test_build_message_format`
    - `tests/test_market_daily.py::test_catalyst_sanitization_removes_tags_urls_and_extra_emoji`
    - `tests/test_market_daily.py::test_relevant_ticker_post_filters_ambiguous_short_tickers`

- MD (Market Daily) 2x/day feature is now implemented in repo (`main`) with Slack/CLI/runtime wiring:
  - new module: `src/coatue_claw/market_daily.py`
  - SQLite store: `/opt/coatue-claw-data/db/market_daily.sqlite`
    - tables: `md_runs`, `md_universe_snapshots`, `md_coatue_holdings`, `md_overrides`, `md_cusip_ticker_cache`
  - universe flow:
    - seed file: `config/md_tmt_seed_universe.csv`
    - top-40 ranking by yfinance market cap
    - Coatue 13F overlay + manual include/exclude overrides
  - move ranking:
    - `% move = (last - prev_close) / prev_close`
    - top movers by abs % move with market-cap tie-breaker
  - catalyst flow:
    - X recent search + Yahoo news evidence
    - per-mover concise one-line catalyst with fallback when no clear signal
  - Slack command surface added:
    - `md now`, `md now force`
    - `md status`
    - `md holdings refresh`
    - `md holdings show`
    - `md include <TICKER>`
    - `md exclude <TICKER>`
  - CLI command surface added:
    - `claw market-daily run-once [--manual] [--force] [--dry-run] [--channel ...]`
    - `claw market-daily status`
    - `claw market-daily holdings`
    - `claw market-daily refresh-coatue-holdings`
    - `claw market-daily include <TICKER>` / `exclude <TICKER>`
  - launchd service added:
    - label: `com.coatueclaw.market-daily`
    - schedule: weekdays at `07:00` and `14:15` local time
    - program: `python -m coatue_claw.market_daily run-once`
  - Make targets added:
    - `openclaw-market-daily-status`
    - `openclaw-market-daily-run-once`
    - `openclaw-market-daily-refresh-holdings`
  - docs updated:
    - `docs/openclaw-runtime.md` includes MD runtime + env contract
  - tests added:
    - `tests/test_market_daily.py`
    - `tests/test_launchd_runtime.py` updated for market-daily service/schedule
  - validation:
    - `PYTHONPATH=src pytest -q` => `140 passed`
- Immediate runtime verification required on Mac mini:
  - `make openclaw-market-daily-status`
  - `make openclaw-market-daily-run-once DRY_RUN=1`
  - `make openclaw-24x7-enable` (loads new market-daily launchd plist)
  - `make openclaw-24x7-status` (confirm `com.coatueclaw.market-daily` loaded)

- Change tracker now captures both Spencer + Carson requests with requester attribution:
  - default tracked users include `Spencer Peterson` + `Carson Wang`
  - list output now labels each item with requester name
  - daily digest now labels each open request with requester name
  - tracker commands accept:
    - `spencer changes`
    - `change requests`
    - `tracked changes`
  - optional override via env:
    - `COATUE_CLAW_CHANGE_TRACKER_USERS` (format `user_id:label,...`)
- Board Seat-as-a-Service daily scheduler shipped for portco channels:
  - new runtime module: `src/coatue_claw/board_seat_daily.py`
  - daily post per company/channel with duplicate guard (one post per company per local day)
  - default portco channel map:
    - `anduril`, `anthropic`, `cursor`, `neuralink`, `openai`, `physical-intelligence`, `ramp`, `spacex`, `stripe`, `sunday-robotics`
  - post format follows the Anduril board-seat frame and is tailored by company:
    - `Signal`, `Board lens`, `Watchlist`, `Team ask`
  - channel context source: recent Slack history in each channel (default 24h lookback); fallback template when no high-signal context exists
  - Slack scope fallback: if `conversations:read` is unavailable (`missing_scope`), service posts by channel name directly and skips history-context enrichment
  - launchd service added: `com.coatueclaw.board-seat-daily` (`COATUE_CLAW_BOARD_SEAT_TIME`, default `08:30`)
  - Make targets added:
    - `openclaw-board-seat-status`
    - `openclaw-board-seat-run-once`
  - tests added:
    - `tests/test_board_seat_daily.py`
    - `tests/test_launchd_runtime.py` updated for new service
- X chart pipeline now runs as hourly scout + windowed post:
  - every run ingests candidates into a persistent `observed_candidates` pool in SQLite
  - scheduled posting windows still use `COATUE_CLAW_X_CHART_WINDOWS` (`09:00,12:00,18:00` default)
  - at post time, winner is ranked from pooled candidates observed since the previous scheduled post (not just the current fetch)
  - non-window runs now return `reason: scouted_pool_updated` after refreshing the pool
  - pool retention/pruning controls:
    - `COATUE_CLAW_X_CHART_POOL_KEEP_DAYS` (default `10`)
    - `COATUE_CLAW_X_CHART_POOL_LIMIT` (default `600`)
- Naming convention updated from “Chart of the Day” to slot naming:
  - `Coatue Chart of the Morning`
  - `Coatue Chart of the Afternoon`
  - `Coatue Chart of the Evening`
  - naming is applied to Slack initial comment + uploaded file title
- launchd scheduler updated for hourly scout cadence:
  - `com.coatueclaw.x-chart-daily` now runs every `3600s` by default (`COATUE_CLAW_X_CHART_SCOUT_INTERVAL_SECONDS`)
  - posting still occurs only when the hourly run lands in an allowed window
- tests added/updated:
  - `test_run_chart_scout_outside_window_updates_pool`
  - `test_run_chart_scout_window_uses_hourly_pool_since_last_slot`
  - `test_convention_name_uses_morning_afternoon_evening_windows`
  - `tests/test_launchd_runtime.py` expectations updated for hourly scheduler

## Previous Status Snapshot (2026-02-19)
- X chart source variety control shipped:
  - winner selection still starts from highest scores, but now applies source diversity in a near-top-score pool
  - default behavior:
    - lookback: last `6` posted charts
    - diversity pool: candidates within `90%` of top score
    - pick least-recently-used source from that pool; otherwise keep top score winner
  - env controls:
    - `COATUE_CLAW_X_CHART_SOURCE_VARIETY_LOOKBACK` (default `6`)
    - `COATUE_CLAW_X_CHART_SOURCE_VARIETY_SCORE_FLOOR` (default `0.90`)
  - tests added:
    - `test_pick_winner_prefers_variety_within_score_floor`
    - `test_pick_winner_keeps_top_when_alternative_too_low`
- X chart rendering reverted to source-snip mode (latest):
  - Slack chart posts now use source-snip-card mode:
    - raw X chart image is embedded as-is inside a Coatue header/footer card
    - no numeric reconstruction or chart redraw
  - URL-triggered chart runs (`run-post-url`) no longer fail on numeric reconstruction checks
  - if no source image is available, bot returns explicit Slack error
  - retained title quality instructions in Slack output:
    - `Title` (narrative headline)
    - `Chart label` (what the chart shows)
    - `Key takeaway` (concise readout)
  - source-snip-card title readability guard added:
    - headline/subheading now auto-fit to card width
    - if text is too long, renderer force-shortens to concise one-line copy (no clipping)
  - style-copy sanitizer added for low-signal phrasing:
    - rewrites generic/opening phrases (for example “It’s official…”) into concise trend language
    - applies keyword override for tariff/customs charts
    - trims trailing stopwords to avoid broken headline endings
  - added chart-image hint fallback for low-signal tweet copy:
    - reads chart title cue from image (vision) to synthesize concise headline
    - prevents broken fragments like “It’s official: In ...”
  - takeaway rewrite guard now runs independently of headline quality
    - prevents generic/clipped takeaway copy even when headline is already concise
  - Slack post summary now uses the sanitized style takeaway (same as chart card), not raw excerpt fallback
  - `x chart status` now shows `render_mode: source-snip` for runtime verification
  - updated tests cover source-snip posting path and no-rebuild requirement
- Spencer change-review + daily digest shipped:
  - Spencer requests are auto-captured from Slack (tracked user IDs: `spcoatue` + `spencermpeter` by default)
  - persisted DB: `/opt/coatue-claw-data/db/spencer_changes.sqlite`
  - statuses: `captured`, `handled`, `implemented`, `blocked`, `needs_followup`
  - review commands in Slack:
    - `spencer changes`
    - `spencer changes open`
    - `spencer changes last 50`
  - daily DM digest runtime added:
    - service label: `com.coatueclaw.spencer-change-digest`
    - schedule env: `COATUE_CLAW_SPENCER_CHANGE_DIGEST_TIME` (default `18:00`)
    - recipients env: `COATUE_CLAW_SPENCER_CHANGE_DIGEST_DM_USER_IDS`
  - Make targets added:
    - `openclaw-spencer-digest-status`
    - `openclaw-spencer-digest-run-once`
  - tests added:
    - `tests/test_spencer_change_log.py`
    - `tests/test_spencer_change_digest.py`
  - Mac mini runtime validation:
    - pulled latest (`5b26ea9`), restarted OpenClaw, and enabled scheduler service
    - `make openclaw-24x7-status` confirms `com.coatueclaw.spencer-change-digest` is loaded
    - forced send test succeeded:
      - `/opt/coatue-claw/.venv/bin/python -m coatue_claw.spencer_change_digest run-once --force`
      - Slack post delivered to user channel `U0AGD28QSQG` (`ts=1771557014.620259`)
  - reliability hardening:
    - digest sender now retries across Slack token sources (env + `~/.openclaw/openclaw.json`)
    - if `conversations.open` lacks scope, it falls back to App Home DM posting (`channel=<user_id>`)
- Spencer-request governance log shipped:
  - bot now auto-captures change requests from Spencer accounts (`spcoatue`/`spencermpeter` user IDs) when messages look like bot-change asks
  - each request is persisted in SQLite (`/opt/coatue-claw-data/db/spencer_changes.sqlite`) with status lifecycle:
    - `captured`, `handled`, `implemented`, `blocked`, `needs_followup`
  - request status is auto-updated inline as bot workflows execute (settings/pipeline/chart/x digest/universe/diligence paths)
  - new Slack review commands:
    - `spencer changes`
    - `spencer changes open`
    - `spencer changes last 50`
  - env override for tracked Spencer IDs: `COATUE_CLAW_SPENCER_USER_IDS`
  - tests added: `tests/test_spencer_change_log.py`
- Slack channel access hardening shipped for new channels:
  - bot now auto-joins newly created public channels via `channel_created` handler + `conversations.join`
  - bot also performs startup bootstrap over existing public channels and joins channels where it is not yet a member
  - env flag: `COATUE_CLAW_SLACK_AUTOJOIN_PUBLIC_CHANNELS` (default `1`, set `0` to disable)
  - private channels still require manual invite (Slack platform limitation)
  - tests added: `tests/test_slack_channel_access.py`
- X Chart destination channel updated to `#charting` for scheduled/manual chart posts.
  - Mac mini runtime env set: `COATUE_CLAW_X_CHART_SLACK_CHANNEL=C0AFXM2MWAV` (`#charting`)
- X Chart post-publish checklist loop shipped:
  - each Slack-posted chart is now reviewed against a persisted checklist (`post_reviews` in `x_chart_daily.sqlite`)
  - checklist coverage includes copy limits, US relevance, axis labels, grouped-series validity, and artifact-size integrity
  - learning feedback now downranks sources when checklist failures occur
  - Mac mini validation: `make openclaw-x-chart-run-once` posted to `C0AFXM2MWAV` and recorded review pass (`review_summary.pass_count=1`)
- Drive/share taxonomy simplification prepared for deployment:
  - Drive root path changed to `/Users/spclaw/Documents/SPClaw Database`
  - category taxonomy simplified to `Universes`, `Companies`, `Industries`
  - ingest classifier now maps legacy category words (for example `filings`, `themes`, `macro`) into the simplified three-folder set
- Grouped-bar QA + rendering correctness update shipped (`main`):
  - employee/robot tweet charts now require two reconstructed bar series before posting
  - rejected cases:
    - single-series grouped output
    - normalized/index-only grouped output
    - non-year / placeholder x-axis labels for grouped employee/robot charts
  - grouped bar semantics normalized before render:
    - labels: `Employees` + `Robots`
    - colors: navy + purple
    - y-axis unit fallback: `Number (thousands)`
  - fixed immutable dataclass handling in grouped-series normalization (no in-place mutation)
  - added CV fallback for employee/robot charts when vision extraction is unavailable:
    - detects dark/purple bar pairs
    - scales to unit values using latest employee/robot figures from post text
  - style-copy QA enforced pre-render (headline/chart label/takeaway constraints)
  - non-normalized bar charts now always render y-axis numeric tick labels
  - added hard fail if reconstructed bar chart has missing y-axis tick labels
  - employee/robot takeaway copy now emits a complete short sentence (avoids clipped line endings)
  - validation: `PYTHONPATH=src pytest -q` => `105 passed`
- OpenClaw gateway handoff hardening shipped (`main`):
  - added deterministic CLI entrypoint for tweet URL chart requests:
    - `/opt/coatue-claw/.venv/bin/python -m coatue_claw.x_chart_daily run-post-url <x-url> [--channel <id>]`
  - this allows gateway-side natural-language Slack handling to call the strict rebuild-only chart path instead of ad hoc screenshot replies
  - runtime note: Mac mini workspace guidance at `~/.openclaw/workspace/AGENTS.md` now explicitly instructs the gateway agent to use `run-post-url` for tweet URL chart requests and avoid relative media paths
  - test added: `test_cli_run_post_url_command`
  - validation: `PYTHONPATH=src pytest -q` => `100 passed`
- X Chart readability + reconstruction fidelity update shipped (`main`):
  - takeaway line is now much shorter by default (feed-safe and chart-safe) to prevent clipping in Slack/file preview
  - renderer screenshot fallback is now hard-disabled (independent of env flag)
  - bar reconstruction now supports grouped/two-series bar structures (for example employees vs robots) and renders native Coatue-style bars with legend
  - vision extractor schema now accepts multi-series bar outputs (`series`) in addition to single-series (`values`)
  - reconstruction gate logic now mirrors rendering mode:
    - bar-mode requires bar reconstruction
    - line-mode requires line reconstruction
  - regression test added to guarantee no screenshot fallback path
  - validation: `PYTHONPATH=src pytest -q` => `99 passed`
- X Chart semantic-title + true-rebuild enforcement shipped (`main`):
  - style copy generation improved for title/subheading fidelity:
    - optional LLM title synthesis (`gpt-4o-mini`) now generates:
      - narrative headline (big title)
      - technical chart label (small subheading)
      - concise takeaway
    - fallback heuristic still applies if LLM synthesis is unavailable
  - URL-specific chart requests now require numeric reconstruction before posting:
    - `run_chart_for_post_url(...)` now pre-validates extracted bars/series
    - if reconstruction fails, bot returns a clear failure instead of posting a screenshot-style fallback
  - added fallback candidate source for URL requests when X API misses media:
    - `api.vxtwitter.com` fallback fetch for text/media on specific post URLs
  - renderer now attempts vision bar extraction first (before mode heuristics), improving reconstruction rate on tweet charts that lack explicit bar keywords
  - title heuristics now include explicit employees-vs-robots narrative handling (for posts like `$AMZN ... employees ... robots`) to avoid raw/fragmented headline/subheading text
  - global post gate added (`COATUE_CLAW_X_CHART_REQUIRE_REBUILD`, default on):
    - if numeric reconstruction is not reliable, Slack posting is skipped with explicit error (no screenshot fallback output)
  - validation: `PYTHONPATH=src pytest -q` => `98 passed`
- Slack natural-language compound X-post handling shipped (`main`):
  - single message with X post URL can now do both:
    - add poster handle to X scout source list
    - generate/post Coatue-style chart from that exact post URL
  - this closes the gap where non-command phrasing fell through and triggered generic executor/browser-relay behavior
  - examples now supported:
    - `Please make a chart of the day from this post <x-url> and add this guy to our twitter list`
    - `Output a coatue style chart from this post <x-url>`
  - implemented in:
    - `src/coatue_claw/slack_x_chart_intent.py`
    - `src/coatue_claw/slack_bot.py` (compound handler wired before strict `x chart ...` parser)
    - `src/coatue_claw/x_chart_daily.py` (`run_chart_for_post_url`)
  - validation: `PYTHONPATH=src pytest -q` => `95 passed`
- X Chart readability pass shipped (`main`):
  - removed generated timestamp line from top-left chart header (less clutter)
  - bar reconstructions now always render x-axis labels (no blank x-axis)
  - added fallback label strategy:
    - use parsed years when available
    - otherwise infer year range ending at post year
    - finally fall back to `P1..Pn`
  - added pre-save readability guardrail: if reconstructed bar chart has insufficient x-axis labels, renderer falls back to source image instead of posting an unreadable rebuild
  - vision extractor now uses inline image bytes (data URL) to improve extraction reliability vs remote URL fetch edge cases
  - validation: `PYTHONPATH=src pytest -q` => `91 passed`
- X Chart-of-the-Day title and bar reconstruction pass shipped (`main`):
  - title framing now explicitly follows Coatue pattern:
    - big narrative headline (theme/takeaway)
    - small chart label (what the graph shows)
  - bar reconstruction no longer emits placeholder-style `G1..G10` labels
  - stricter reconstruction gates now reject low-quality bar parses instead of rendering misleading synthetic bars
  - optional OpenAI vision extraction added for bar charts (`OPENAI_API_KEY` + `COATUE_CLAW_X_CHART_VISION_ENABLED=1`) to recover real bar labels/values when possible
  - renderer now supports non-normalized bar values (including negatives) and dynamic y-axis labels when vision extraction returns concrete units
  - test suite updated and green (`PYTHONPATH=src pytest -q` => `90 passed`)
- Repo is synced on `main` and used as the cross-device source of truth.
- Slack channel/user policy is open in OpenClaw (`groupPolicy=open`, `dmPolicy=open`, `allowFrom=["*"]`).
- Natural-language chart intent parsing is implemented:
  - detects plot/chart/graph requests
  - defaults y-axis to YoY revenue growth unless user specifies otherwise
  - supports configurable axis metrics (EV/LTM multiple, YoY growth, LTM revenue, market cap, enterprise value, debt, cash, latest quarter revenue)
- Chart footer branding text (`COATUE CLAW`) has been removed; only footnote/citation text remains.
- CSV-backed universe workflow is implemented:
  - storage path: `/opt/coatue-claw-data/db/universes/*.csv`
  - Slack natural commands: create/list/show/add/remove universes
  - chart requests with missing or underspecified tickers now prompt for source:
    - `online` discovery
    - saved `universe` CSV
  - post-chart feedback loop asks for include/exclude tickers and can rerun chart in-thread
- Chart pre-output follow-ups are now only asked when strictly necessary:
  - bot first tries auto universe match and online discovery
  - if a valid ticker set is found, it renders immediately with no extra question
  - if not, it asks for `online` vs `use universe NAME`
- Chart titles now infer context from user prompt/source (example: `Defense Stocks`) and use that as headline.
- Footer citation/footnote text now sits at the left corner since logo text was removed.
- Coatue-style median dotted line + callout has been restored for chart outputs after configurable-axis refactor.
- Category guide/key is now auto-placed inside plot whitespace (dotted `Category Guide` box) using a density-aware heuristic.
- Default guide placement behavior now optimizes for low point density and distance from datapoints while avoiding `R^2` and median-callout zones (and de-prioritizing trendline overlap).
- Post-chart Slack follow-up prompt is now sent via resilient thread posting (`chat_postMessage` with retry on rate limits, fallback to `say`) so the adjustments question is consistently delivered.
- OpenClaw charting skill contract now explicitly requires a final post-chart follow-up question (stock screen/data/formatting adjustments) after successful chart output.
- Added a dedicated laptop/Codex/OpenClaw operations runbook at `docs/laptop-codex-openclaw-workflow.md` and mirrored key guardrails into `AGENTS.md` (canonical repo path, ship loop, restart/verify loop).
- Expanded runtime spec in `docs/openclaw-runtime.md` with execution model, job classes, artifact contract, and incident triage runbook.
- Added explicit OpenClaw operator targets in `Makefile` for `openclaw-dev`, `openclaw-bot-status`, `openclaw-bot-logs`, and `openclaw-schedulers-status`.
- Hardened `Makefile` OpenClaw targets for non-login SSH shells:
  - prepends `/opt/homebrew/bin` to PATH (so `node` + `openclaw` resolve)
  - auto-resolves OpenClaw binary path (`openclaw` on PATH or `/opt/homebrew/bin/openclaw`)
- Added plain-English Slack settings workflow:
  - `show my settings` / `how are you configured`
  - conversational setting updates (peer count target, default x/y axes, post-chart follow-up wording)
  - `promote current settings` to auto-commit/push runtime defaults to `main`
  - `undo last promotion` to auto-`git revert` the last settings promotion commit
- Added runtime settings persistence and audit modules:
  - `src/coatue_claw/runtime_settings.py`
  - `src/coatue_claw/slack_config_intent.py`
  - defaults file tracked in git: `config/runtime-defaults.json`
- Added Slack deploy pipeline workflow (admin-gated, single-job lock):
  - `deploy latest` -> pull + restart + Slack probe status
  - `undo last deploy` -> revert last deploy target + push + restart + probe
  - `run checks` -> `PYTHONPATH=src pytest -q`
  - `show pipeline status` / `show deploy history`
  - `build: <request>` -> runs Codex CLI on runtime host by default (or custom command via env)
- Added deploy pipeline modules:
  - `src/coatue_claw/slack_pipeline.py`
  - `src/coatue_claw/slack_pipeline_intent.py`
  - deploy history file: `/opt/coatue-claw-data/db/deploy-history.json`
- Replaced diligence template output with deep neutral memo generation:
  - new module: `src/coatue_claw/diligence_report.py`
  - `claw diligence TICKER` now outputs the 8-section neutral investment memo format with evidence-based citations
  - data sources in memo: company profile, statements, valuation/balance sheet metrics, and recent reporting metadata (via Yahoo Finance/yfinance)
  - Slack mention parsing now accepts both `diligence` and common typo `dilligence`
  - diligence now performs a database-first local report check before online fetch:
    - local sources: `/opt/coatue-claw-data/db/file_ingest.sqlite` + `/opt/coatue-claw-data/artifacts/packets/*.md`
    - memo now includes matched local report references and local-check timestamp in sources
- Added hybrid memory subsystem (structured-first + semantic fallback):
  - SQLite + FTS5 memory DB: `/opt/coatue-claw-data/db/memory.sqlite`
  - auto fact extraction from Slack mentions (`profile`, `relationship`, `decision`, `convention`)
  - decay tiers with TTL refresh-on-access (`permanent`, `stable`, `active`, `session`, `checkpoint`)
  - memory CLI commands: `claw memory status|query|prune|extract-daily|checkpoint`
  - Slack memory interactions:
    - `remember ...`
    - `memory status`
    - `memory prune`
    - `memory extract daily [days N]`
    - natural lookup (`what is my ...`, `when is my ...`, `do you remember ...`)
  - pre-flight pipeline checkpoints now auto-write before `deploy_latest`, `undo_last_deploy`, and `build_request`
  - optional semantic retrieval path via LanceDB/OpenAI embeddings when configured
- Added file management bridge (local-first + shared mirror):
  - module: `src/coatue_claw/file_bridge.py`
  - config: `config/file-bridge.json`
  - runbook: `docs/file-management-system.md`
  - Make targets:
    - `openclaw-files-init`
    - `openclaw-files-status`
    - `openclaw-files-sync-pull`
    - `openclaw-files-sync-push`
    - `openclaw-files-sync`
    - `openclaw-files-index`
  - published index artifacts generated to `published/index.json` and `published/index.md`
  - Drive mirror root is now set to `/Users/spclaw/Documents/SPClaw Database` for Mac mini sync
  - Spencer-facing category subfolders are provisioned under `01_DROP_HERE_Incoming`, `02_READ_ONLY_Latest_AUTO`, and `03_READ_ONLY_Archive_AUTO` (`Universes`, `Companies`, `Industries`)
  - `01_DROP_HERE_Incoming/_Latest_Reference_READ_ONLY` auto-mirrors Latest for visibility and is excluded from pull ingestion
  - Slack file uploads are now auto-ingested (download + category routing + SQLite audit + Drive mirror):
    - module: `src/coatue_claw/slack_file_ingest.py`
    - DB: `/opt/coatue-claw-data/db/file_ingest.sqlite`
    - wired in `src/coatue_claw/slack_bot.py` (`message`, `file_shared`, and `app_mention` with file attachments)
- Added email interaction channel (optional poller):
  - module: `src/coatue_claw/email_gateway.py`
  - runbook: `docs/email-integration.md`
  - Make targets:
    - `openclaw-email-status`
    - `openclaw-email-run-once`
    - `openclaw-email-serve`
  - command support via email subject/body:
    - `diligence TICKER` / `dilligence TICKER`
    - `memory status`
    - `memory query <phrase>`
    - `files status`
    - `help`
  - email attachments auto-ingest into local + Drive category folders with audit DB:
    - `/opt/coatue-claw-data/db/email_gateway.sqlite`
  - diligence parsing now uses context-aware ticker extraction (body-first, stopword filtering) so phrases like `Testing Dilligence` + `Diligence SNOW please` resolve to ticker `SNOW` instead of `DILIGENCE`
- Added 24/7 runtime supervision for email + memory pruning:
  - module: `src/coatue_claw/launchd_runtime.py`
  - launchd services:
    - `com.coatueclaw.email-gateway` (always-on `email_gateway serve` with KeepAlive)
    - `com.coatueclaw.memory-prune` (hourly `claw memory prune` via StartInterval)
  - launchctl domain fallback support for remote SSH operations (`gui/<uid>` then `user/<uid>`)
  - Make targets:
    - `openclaw-24x7-enable`
    - `openclaw-24x7-status`
    - `openclaw-24x7-disable`
  - `openclaw-schedulers-status` now reports actual launchd service status instead of placeholder text
- Chart outputs remain PNG + CSV + JSON + raw provider payload.
- Session shipping protocol is codified in `AGENTS.md` and templated in `docs/handoffs/ship-template.md`.

## What Was Implemented
- Added valuation chart engine in `/opt/coatue-claw/src/coatue_claw/valuation_chart.py`.
- Added CLI command in `/opt/coatue-claw/src/coatue_claw/cli.py`:
  - `claw valuation-chart SNOW,MDB,DDOG`
- Added Slack command handling in `/opt/coatue-claw/src/coatue_claw/slack_bot.py`:
  - mention pattern for graph/chart + EV/LTM/growth tickers
  - uploads PNG + CSV + JSON + raw provider JSON artifacts
- Added unit tests in `/opt/coatue-claw/tests/test_valuation_chart.py`.
- Added workspace skill for OpenClaw runtime:
  - `/Users/spclaw/.openclaw/workspace/skills/valuation-charting/SKILL.md`

## Current Data Behavior
- Default metric orientation is **EV/LTM revenue on x-axis** and **YoY revenue growth on y-axis**.
- User can override x/y metrics in natural language (`x axis ...`, `y axis ...`, or `A vs B` phrasing).
- LTM revenue is **sum of last 4 reported quarters**.
- Provider preference is `google` then `yahoo`.
- In this build, Google adapter is unavailable for required EV + LTM inputs, so run falls back to Yahoo.
- Quality gates include:
  - `missing_ltm_revenue`
  - `missing_debt`
  - `currency_mismatch`
  - `stale_market_data`
  - `stale_fundamentals`

## Artifacts
- Charts/data write to `/opt/coatue-claw-data/artifacts/charts/`:
  - `valuation-scatter-*.png`
  - `valuation-scatter-*.csv`
  - `valuation-scatter-*.json`
  - `valuation-scatter-*-raw.json`

## Validation Completed
- `pytest`: `20 passed` (valuation chart + chart-intent parser tests).
- CLI smoke run (latest):
  - provider used: `yahoo`
  - included/excluded counts returned
  - PNG/CSV/JSON/raw generated
- OpenClaw skill recognized:
  - `openclaw skills info valuation-charting` => ready, source `openclaw-workspace`
- Repo-session validation (this session):
  - `PYTHONPATH=src pytest -q` => `58 passed`
  - `PYTHONPATH=src pytest -q tests/test_launchd_runtime.py tests/test_email_gateway.py` => `6 passed`
  - `make openclaw-restart` failed locally with `openclaw: No such file or directory`; runtime restart/status validation must be executed on Mac mini runtime host.
  - Mac mini runtime validation succeeded after pull (`d5099bb`): gateway running, Slack probe `ok=true`; root cause of earlier SSH failure was minimal PATH in non-login shell.
  - Makefile PATH hardening validated on Mac mini after pull (`0862aa0`): non-login SSH `make openclaw-restart` and `make openclaw-slack-status` now execute with resolved `openclaw` + `node`.
  - Hybrid memory runtime deployed on Mac mini (`33650f2`): structured memory active; semantic fallback currently disabled until `OPENAI_API_KEY` is set in runtime env.
  - File bridge Drive root configured + validated on Mac mini (`9db4643` + latest pull): `make openclaw-files-init`, `make openclaw-files-sync`, and `make openclaw-files-status` pass using `/Users/spclaw/Documents/SPClaw Database`.
  - Recursive subfolder sync validation passed on Mac mini: file dropped into `01_DROP_HERE_Incoming/Companies` mirrored to local `incoming/Companies`, then cleaned up.
- Slack file ingest tests added/validated: classification, routing, dedupe, and SQLite write path.
- Diligence tests extended/validated for local-report precheck behavior before external research.
- Email gateway tests added/validated: command parsing, disabled-mode safety, and attachment ingest routing.
- 24/7 runtime service tests added/validated:
  - local: `PYTHONPATH=src pytest -q tests/test_launchd_runtime.py` => `4 passed`
  - Mac mini: `/opt/coatue-claw/.venv/bin/python -m pytest -q tests/test_launchd_runtime.py tests/test_email_gateway.py` => `7 passed`
- Mac mini runtime validation (2026-02-19):
  - pulled `a49f887` then `95fb26d` in `/opt/coatue-claw`
  - `make openclaw-24x7-enable` succeeded for:
    - `com.coatueclaw.email-gateway`
    - `com.coatueclaw.memory-prune`
  - `make openclaw-24x7-status` reports:
    - email service: `loaded=true`, `state=running`
    - memory prune service: `loaded=true`, `last_exit_code=0` (idle between scheduled runs is expected)
  - `make openclaw-slack-status` probe returns `ok=true`
  - `make openclaw-email-status` confirms email channel enabled with expected allowlist senders
- Email diligence parsing hardening validation (commit `19b2099`):
  - local tests: `PYTHONPATH=src pytest -q tests/test_email_gateway.py` => `3 passed`
  - Mac mini tests: `/opt/coatue-claw/.venv/bin/python -m pytest -q tests/test_email_gateway.py` => `3 passed`
  - Mac mini direct parser check:
    - `parse_email_command('Testing Dilligence', 'Diligence SNOW please')`
    - result: `EmailCommand(kind='diligence', arg='SNOW')`
  - runtime health after deploy:
    - `make openclaw-24x7-status`: email service loaded/running; memory-prune loaded with clean `last_exit_code=0`
    - `make openclaw-slack-status`: probe `ok=true`
- Email diligence response formatting upgrade (`c862c48` + `a58c0f8`):
  - replaced raw markdown preview dump with readable executive summary body
  - added HTML email alternative for cleaner rendering in Gmail
  - attached full memo as readable `.pdf` for consumer-facing delivery
  - removed local filesystem path lines from user-facing email output
  - stripped long `[Source: ...]` tails from summary bullets for readability while preserving full citations in the attachment
  - escaped literal `$` values during PDF rendering to avoid matplotlib mathtext parse failures on financial figures
  - replaced raw full-memo PDF text dump with a professionally sectioned diligence brief layout (clean headings, wrapped bullets, readable spacing)
  - tests added for readable summary + attachment contract (`tests/test_email_gateway.py`)
  - Mac mini validation:
    - `/opt/coatue-claw/.venv/bin/python -m pytest -q tests/test_email_gateway.py` => `4 passed`
    - direct runtime check confirms:
      - `has_local_path=False`
      - attachment filename ends with `.pdf`
      - attachment content type `application/pdf`
      - attachment payload starts with `%PDF`
- Professional PDF brief rendering upgrade (`fb3692c`):
  - replaced dense raw-memo PDF pages with a clean sectioned brief format:
    - Key Takeaways
    - Business Overview
    - Financial Snapshot
    - Company Strengths
    - Key Risks
    - Open Questions
  - improved typography, bullet wrapping, spacing, and page footer layout for readability
  - Mac mini validation:
    - `tests/test_email_gateway.py` => `4 passed`
    - generated diligence attachment remains valid PDF (`%PDF`) with consumer-facing summary body
    - services healthy: `make openclaw-24x7-status`, `make openclaw-slack-status`
- PDF layout style refinement (this session):
  - upgraded to an executive-style template with centered report title, metadata row, blue divider, and backdrop callout box
  - improved section hierarchy and typography to match a professional investment brief feel
  - report title now stays generic to diligence topic/company (example: `SNOW Diligence Report`) and avoids third-party naming
  - Mac mini deployment validation (`26fc61d`):
    - `tests/test_email_gateway.py` => `4 passed`
    - generated attachment metadata:
      - filename: `SNOW-...pdf`
      - content_type: `application/pdf`
      - payload begins with `%PDF`
    - user-facing summary title now reports `Title: SNOW Diligence Report`
- Slack default routing upgrade (this session):
  - added routing helper module: `src/coatue_claw/slack_routing.py`
  - plain Slack messages now run through OpenClaw request handling by default
  - explicit `@user` mentions are excluded from default routing
  - `app_mention` flow remains supported and now shares common request handler logic with default-routed messages
  - tests: `tests/test_slack_routing.py`
  - Mac mini deployment validation (`86bce9d`):
    - `/opt/coatue-claw/.venv/bin/python -m pytest -q tests/test_slack_routing.py` => `3 passed`
    - `make openclaw-restart` + `make openclaw-slack-status` => Slack probe `ok=true`
    - `make openclaw-24x7-status` => email/memory launchd services still healthy
  - deployed on Mac mini via `0173404` and validated:
    - `tests/test_email_gateway.py` => `4 passed` on runtime host
    - direct runtime check confirms `has_source_tail=False` in diligence summary body
    - email 24/7 service restored and running (`make openclaw-24x7-status`)
- X digest feature (this session):
  - implemented official X API digest path (no browser scraping dependency):
    - new digest engine: `src/coatue_claw/x_digest.py`
    - new Slack intent parser: `src/coatue_claw/slack_x_intent.py`
    - Slack commands wired:
      - `x digest <query> [last Nh] [limit N]`
      - `x status`
    - CLI command wired:
      - `claw x-digest "QUERY" --hours 24 --limit 50`
    - artifact contract:
      - writes markdown digest to `/opt/coatue-claw-data/artifacts/x-digest` (override via `COATUE_CLAW_X_DIGEST_DIR`)
    - env contract:
      - `COATUE_CLAW_X_BEARER_TOKEN` (required; runtime only, never in git)
      - optional `COATUE_CLAW_X_API_BASE`
  - tests added:
    - `tests/test_slack_x_intent.py`
    - `tests/test_x_digest.py`
  - local validation:
    - `PYTHONPATH=src pytest -q` => `69 passed`
  - operator note:
    - token was provided in chat; rotate/regenerate it in X Developer portal and set the new token in runtime `.env.prod` only
  - Mac mini deploy + runtime verification completed:
    - pulled `main` to `/opt/coatue-claw` at commit `5dfdd03`
    - set `COATUE_CLAW_X_BEARER_TOKEN` in `/opt/coatue-claw/.env.prod`
    - restarted runtime: `make openclaw-restart`
    - verified Slack health: `make openclaw-slack-status` probe `ok=true`
    - verified live X API path using runtime env:
      - `build_x_digest("snowflake", hours=24, max_results=10)` succeeded
      - output written to `/opt/coatue-claw-data/artifacts/x-digest/snowflake-20260219-061237.md`
- Slack no-mention routing hotfix on Mac mini:
  - root cause from runtime logs:
    - `slack-auto-reply ... reason=\"no-mention\" skipping channel message`
  - config fix applied in `~/.openclaw/openclaw.json`:
    - `channels.slack.requireMention=false`
    - `channels.slack.channels.C0AFGMRFWP8.requireMention=false`
  - restarted runtime and verified Slack probe health:
    - `make openclaw-restart`
    - `make openclaw-slack-status` => probe `ok=true`
  - expected behavior after fix:
    - plain channel messages (e.g., `x status`) are routed to OpenClaw without requiring `@Coatue Claw`
- X chart scout daily winner feature (this session):
  - implemented automated chart scouting + posting engine:
    - module: `src/coatue_claw/x_chart_daily.py`
    - persists source trust/ranking + post history in SQLite:
      - `/opt/coatue-claw-data/db/x_chart_daily.sqlite`
    - writes per-slot markdown artifacts:
      - `/opt/coatue-claw-data/artifacts/x-chart-daily/*.md`
  - source strategy:
    - seeded prioritized source list with `@fiscal_AI` and additional high-signal accounts
    - auto-discovers and promotes new X sources based on engagement
    - ingests supplemental candidates from Visual Capitalist feed (`https://www.visualcapitalist.com/feed/`)
  - scheduling:
    - added launchd service `com.coatueclaw.x-chart-daily`
    - default windows: `09:00,12:00,18:00` with configurable timezone (default `America/Los_Angeles`)
  - Slack controls added:
    - `x chart now`
    - `x chart status`
    - `x chart sources`
    - `x chart add @handle priority 1.2`
  - CLI controls added:
    - `claw x-chart run-once --manual`
    - `claw x-chart status`
    - `claw x-chart list-sources`
    - `claw x-chart add-source HANDLE --priority 1.2`
  - Makefile controls added:
    - `make openclaw-x-chart-status`
    - `make openclaw-x-chart-run-once`
    - `make openclaw-x-chart-sources`
    - `make openclaw-x-chart-add-source HANDLE=fiscal_AI PRIORITY=1.6`
  - tests added/updated:
    - `tests/test_x_chart_daily.py`
    - `tests/test_launchd_runtime.py`
  - local validation:
    - `PYTHONPATH=src pytest -q` => `72 passed`
  - robustness patch:
    - X source scanning now handles invalid/renamed usernames gracefully (skips invalid handles instead of failing the entire run)
    - Slack post token resolution now falls back to `~/.openclaw/openclaw.json` (`channels.slack.botToken`) when `SLACK_BOT_TOKEN` env is missing/inactive
    - Slack posting retries with fallback token when env token is rejected (`account_inactive`, `invalid_auth`, `token_revoked`)
  - Mac mini deploy/verification:
    - pulled `main` to `/opt/coatue-claw` at commit `c3f64d0`
    - runtime env configured:
      - `COATUE_CLAW_X_CHART_SLACK_CHANNEL=#charting`
      - `COATUE_CLAW_X_CHART_TIMEZONE=America/Los_Angeles`
      - `COATUE_CLAW_X_CHART_WINDOWS=09:00,12:00,18:00`
    - launchd scheduler enabled and healthy:
      - `make openclaw-24x7-enable`
      - `make openclaw-24x7-status` shows `com.coatueclaw.x-chart-daily` loaded
    - manual proof-of-life post succeeded:
      - `make openclaw-x-chart-run-once` posted winner to `#charting`
      - sample winner: `@Barchart` post `https://x.com/Barchart/status/2024274150118859021`
      - artifact: `/opt/coatue-claw-data/artifacts/x-chart-daily/manual-20260218-224336-x.md`
  - Chart-of-the-day style formatter (this session):
    - integrated a new Coatue-style rendered image card into x-chart posting flow
    - renderer uses design cues from Coatue `C:\\Takes` + valuation-chart skill style language:
      - navy headline, metadata row, blue divider
      - light gray canvas with bordered content rails
      - executive summary bullets and backdrop context callout
      - source footer + score marker
      - Avenir/Avenir Next/Helvetica Neue fallback typography
    - Slack delivery now posts:
      - summary text message in channel
      - threaded `Coatue Chart of the Day` styled PNG upload
    - output artifact path:
      - `/opt/coatue-claw-data/artifacts/x-chart-daily/<slot>-styled.png`
  - chart-quality filter update:
    - X candidates now must pass chart-like text/data signal checks before ranking
    - reduces false positives (e.g., headline photos without chart context)
  - US scope + readability/style refinement (this session):
    - added US relevance classifier gate for both X and Visual Capitalist candidates (blocks non-US FX-only trends such as Turkish lira posts)
    - added render-safe normalization for outbound chart text (`_normalize_render_text`) to remove unsupported glyph/emoji artifacts
    - simplified chart card format to a graph-first layout (no left text panel; headline + large chart + minimal footer only)
    - added iterative post-generation style audit (`StyleDraft`) with checks for:
      - US relevance
      - explicit trend signal
      - concise headline/takeaway
      - plain language (non slide-jargon)
      - clean characters
      - graph-first copy density (short, feed-readable framing)
    - style audit metadata now persists in:
      - markdown artifact (`style_iteration`, `style_score`, checks)
      - Slack post payload (`style_audit`)
    - Slack delivery now uploads the styled graph as the initial message attachment (`files_upload_v2` with `initial_comment`) instead of posting image in a thread reply
  - chart reconstruction upgrade (this session):
    - chart pipeline now attempts to reconstruct line-series directly from source chart images (color/dark-line extraction + normalization) and re-plots the chart in Coatue style rather than screenshot framing by default
    - when reconstruction succeeds, output is graph-first with normalized axes and redrawn series; original image embed is fallback-only
    - bar-mode support added:
      - if the post references bar/histogram language, or bar-like structure is detected in source chart image, renderer outputs rebuilt bar chart instead of line chart
      - if bar mode is selected but bar reconstruction confidence is insufficient, renderer now falls back to source image (never fake line output for bar-cue posts)
    - chart headlines are auto-shortened with no `...`; long titles are rephrased to concise header text
    - title/takeaway builders and Slack summary text now avoid ellipsis output
    - two-level Coatue-style title synthesis added:
      - small chart label describes what is being measured
      - big narrative headline expresses thematic takeaway
    - headline synthesis now strips raw news prefixes (for example `BREAKING:`) and rewrites into narrative language
    - removed source-handle callout and score badge from chart image output
    - render includes pre-save overlap checks that adjust header/plot/footer spacing to avoid text/graphics collisions
  - local validation for this refinement:
    - `PYTHONPATH=src pytest -q tests/test_x_chart_daily.py tests/test_launchd_runtime.py` => `22 passed`
    - `PYTHONPATH=src pytest -q` => `88 passed`

## Next Step to Validate in Slack
Send in `#charting`:
- `@Coatue Claw plot EV/Revenue multiples and revenue growth for SNOW,MDB,DDOG,NOW,CRWD`
- `@Coatue Claw graph SNOW,MDB,DDOG with x axis market cap and y axis ltm revenue`
- `@Coatue Claw create universe defense with PLTR,LMT,RTX,NOC,GD,LDOS`
- `@Coatue Claw make me a valuation chart for defense stocks` then reply `@Coatue Claw use universe defense` or `@Coatue Claw online`
- Confirm rendered title headline is prompt-relevant (`Defense Stocks` / similar) and footnote is left-aligned.
- Confirm category guide appears inside unused plot whitespace and does not consume a dedicated right gutter.
- Confirm each successful chart post includes the in-thread adjustments follow-up prompt right after artifact upload.

Then confirm bot returns:
- as-of timestamps
- provider used + fallback reason
- chart image with line of best fit
- CSV/JSON/raw attachments

## Immediate Next Steps
1. Ensure Slack app permissions for cross-channel behavior:
   - bot scopes: `channels:read`, `channels:join`, `chat:write`, `chat:write.public`
   - event subscriptions: add `channel_created`
   - reinstall app after scope/event changes
2. Run all Slack validation prompts above in `#charting`.
3. Validate plain-English settings commands in Slack:
   - `@Coatue Claw show my settings`
   - `@Coatue Claw going forward look for 12 peers`
   - `@Coatue Claw use market cap as the default x-axis`
   - `@Coatue Claw when you finish a chart, ask us if we want ticker changes`
4. Validate `@Coatue Claw promote current settings` commits/pushes to `main` and reports commit hash in-thread.
5. Validate `@Coatue Claw undo last promotion` produces a revert commit and restarts runtime.
6. Validate Slack deploy pipeline in `#claw-lab`:
   - `@Coatue Claw deploy latest`
   - `@Coatue Claw run checks`
   - `@Coatue Claw show pipeline status`
   - `@Coatue Claw show deploy history`
7. Validate diligence memo output in Slack:
    - `@Coatue Claw diligence SNOW`
    - `@Coatue Claw dilligence MDB` (typo alias path)
    - confirm memo includes all required neutral sections plus source/timestamp attribution
    - confirm memo includes local database precheck summary and local report references when available
8. Validate memory flows in Slack:
   - `@Coatue Claw remember my daughter's birthday is June 3rd`
   - `@Coatue Claw what is my daughter's birthday?`
   - `@Coatue Claw memory status`
   - `@Coatue Claw memory checkpoint`
9. Configure `SLACK_PIPELINE_ADMINS` and optional `COATUE_CLAW_SLACK_BUILD_COMMAND` in runtime env for production permissions/runner control.
9. Validate 24/7 persistence after next Mac mini reboot:
   - run `make openclaw-24x7-status`
   - confirm both services auto-restart as `loaded=true` and `state=running`
10. Confirm Spencer has Drive access to `/Users/spclaw/Documents/SPClaw Database` and can drag/drop by category under `01_DROP_HERE_Incoming/{Universes|Companies|Industries}`.
11. Validate end-to-end category workflow with Spencer:
    - Spencer drops a file into `01_DROP_HERE_Incoming/{Universes|Companies|Industries}`
    - run `make openclaw-files-sync-pull`
    - confirm file appears in `/opt/coatue-claw-data/files/incoming/{Universes|Companies|Industries}`
12. Validate Slack upload ingestion with Spencer:
    - Spencer uploads a file in Slack (without bot mention)
    - confirm bot thread ack shows routed category
    - confirm file appears in `/opt/coatue-claw-data/files/incoming/{Universes|Companies|Industries}`
    - confirm record exists in `/opt/coatue-claw-data/db/file_ingest.sqlite`
13. Configure email env vars in `/opt/coatue-claw/.env.prod` and validate:
    - `make openclaw-email-status`
    - `make openclaw-email-run-once`
    - send test email `diligence SNOW` and confirm reply
14. On Mac mini, set chart scout env in `/opt/coatue-claw/.env.prod`:
    - `COATUE_CLAW_X_CHART_SLACK_CHANNEL=<channel-id>`
    - `COATUE_CLAW_X_CHART_TIMEZONE=America/Los_Angeles`
    - `COATUE_CLAW_X_CHART_WINDOWS=09:00,12:00,18:00`
15. Run:
    - `make openclaw-24x7-enable`
    - `make openclaw-24x7-status` (confirm `com.coatueclaw.x-chart-daily` loaded)
16. Dry-run source quality and posting:
    - `make openclaw-x-chart-sources`
    - `make openclaw-x-chart-run-once`
17. Tune style details after first day of live posts (font sizing, backdrop length, and summary density).
15. If response fails, capture first failing line with `openclaw channels logs --channel slack --lines 300`.

## 2026-02-19 - Slack Build Runner rg Fallback
- Issue observed: Slack `build:`/behavior-refine flow failed inside `codex exec` when generated command used `rg` on hosts without ripgrep (`zsh: command not found: rg`).
- Code update: `src/coatue_claw/slack_pipeline.py`
  - Added explicit build-runner prompt guidance: if `rg` is unavailable, use `grep -R` (or install ripgrep) instead of failing.
- Tests:
  - Added `test_run_build_request_prompt_includes_rg_fallback` in `tests/test_slack_pipeline.py`.
  - Validation run: `PYTHONPATH=src pytest -q tests/test_slack_pipeline.py` (pass).

### Immediate Next Steps
1. On Mac mini runtime host, install ripgrep for best performance: `brew install ripgrep`.
2. Pull latest `main`, restart runtime, and retry the same Slack behavior-refine request.
3. If still failing, capture `make openclaw-slack-logs` output and confirm `PATH` seen by the runtime process includes `/opt/homebrew/bin`.

### 2026-02-19 Ship Status
- rg fallback prompt patch is ready to ship on `main` in this session.
- Next operator action after pull: `make openclaw-restart` then re-run Slack refine request.

## 2026-02-21 - X Chart Title Coherence Hardening
- Issue fixed: Chart-of-the-* titles could become ungrammatical from noisy/truncated source copy (example observed in Slack: `Institutional investors sold a are at an extreme`).
- Root cause:
  - subject extraction occasionally preserved a partial action phrase (`sold a`), then narrative template appended `are at an extreme`.
- Code updates in `/Users/carsonwang/CoatueClaw/src/coatue_claw/x_chart_daily.py`:
  - added subject cleanup before narrative synthesis (`_clean_subject_for_headline`)
  - added singular/plural copula selection (`_subject_is_plural`) so templates use `is/are` correctly
  - added explicit institutional net-seller/net-buyer narrative rules
  - added incoherent-headline detector (`_has_incoherent_headline`)
  - added final grammar-repair fallback inside `_sanitize_style_copy`
  - added style quality/checklist guardrails so ungrammatical headlines fail QA
- Tests:
  - added regression test `test_style_draft_rewrites_incoherent_institutional_selling_headline`
  - validation run: `PYTHONPATH=src pytest -q tests/test_x_chart_daily.py` -> `44 passed`

### Immediate Next Steps
1. Deploy latest `main` to `/opt/coatue-claw` and restart runtime (`make openclaw-restart`).
2. Run manual post-url smoke tests with known noisy titles and verify coherent final headline in Slack.
3. Monitor next scheduled morning/afternoon/evening chart posts for title quality regressions.

## 2026-02-21 - Remove Chart Label From Source-Snip Output
- User request: remove chart-label line from chart-of-the-* output now that final render is a direct X source snip card.
- Changes:
  - removed chart-label subtitle line from source snip card renderer (`_render_source_snip_card`)
  - removed `- Chart label: ...` from Slack initial comment payload in `_post_winner_to_slack`
  - retained internal style-draft `chart_label` field for QA/scoring only (not user-facing in snip mode)
- Tests:
  - updated `tests/test_x_chart_daily.py` to assert Slack initial comment no longer includes `Chart label:`
  - validation run: `PYTHONPATH=src pytest -q tests/test_x_chart_daily.py` -> `44 passed`

### Immediate Next Steps
1. Deploy/restart on mini and run one `run-post-url` smoke test.
2. Confirm Slack output in `#charting` includes title + takeaway + source link, with no chart-label line.

## 2026-02-23 - MD Catalyst Fix For APP SEC-Probe Attribution
- Issue observed: APP mover line defaulted to generic/fallback wording and missed clear SEC-probe narrative visible in top web coverage.
- Root cause:
  - no dedicated `regulatory/probe` cause cluster;
  - Barron's (`barrons.com`) was not treated as a quality cause domain;
  - APP-specific web retrieval queries were too generic.
- Code updates in `/Users/carsonwang/CoatueClaw/src/coatue_claw/market_daily.py`:
  - added APP alias overrides (`AppLovin`, `AppLovin Corporation`, `AppLovin Corp`);
  - added `regulatory_probe` driver cluster + event phrase (`reports of an active SEC probe.`);
  - added cluster priority bonus for `regulatory_probe`;
  - added `barrons.com` to both domain weights and quality-domain allowlist;
  - expanded APP web queries (`sec probe`, `regulatory probe`, `short seller report`);
  - boosted directional/decisive recognition for probe/investigation/regulatory wording.
- Tests added in `/Users/carsonwang/CoatueClaw/tests/test_market_daily.py`:
  - `test_regulatory_probe_cluster_extraction_maps_keywords`
  - `test_barrons_domain_counts_as_quality_source`
  - `test_app_regulatory_probe_cluster_outputs_specific_reason`
- Validation run:
  - `PYTHONPATH=src python3 -m pytest tests/test_market_daily.py` -> `31 passed`.

### Immediate Next Steps
1. Pull latest `main` on Mac mini and restart runtime (`make openclaw-restart`).
2. Run `md debug APP` (or CLI `claw market-daily debug-catalyst APP --slot open`) and confirm selected cluster `regulatory_probe`.
3. Run `md now --force` in Slack and verify APP reason line explicitly names SEC-probe overhang when corroborated evidence is present.

## 2026-02-23 - X Chart Takeaway Sentence Completeness + Candidate Fallback
- Issue fixed: X chart posts could emit incomplete takeaway fragments (example class: `...fell to lowest`) due to hard clipping without sentence validation.
- Root cause:
  - takeaway text was length-constrained but not validated for sentence completeness;
  - degenerate one-token LLM outputs (`U.S`) could survive as headline/chart label;
  - scout winner selection did not switch candidates when top copy quality was poor.
- Code updates in `/opt/coatue-claw/src/coatue_claw/x_chart_daily.py`:
  - added deterministic copy-quality helpers:
    - `_is_complete_sentence`
    - `_finalize_takeaway_sentence`
    - `_rewrite_takeaway_from_candidate`
    - `_is_degenerate_copy_value`
  - upgraded `_sanitize_style_copy(...)` to:
    - enforce complete takeaway sentences;
    - rewrite low-quality/fragment takeaways from candidate context;
    - rebuild degenerate headline/chart-label values;
    - return rewrite diagnostics (`copy_rewrite_applied`, `copy_rewrite_reason`).
  - added style checks:
    - `takeaway_complete_sentence`
    - `headline_non_degenerate`
    - `chart_label_non_degenerate`
  - added scout fallback selection in `run_chart_scout_once(...)`:
    - evaluates candidate pool in score order;
    - skips top candidate when copy quality fails and picks next valid candidate;
    - returns `candidate_fallback_used` + rewrite diagnostics in result payload.
  - kept explicit URL behavior strict in `run_chart_for_post_url(...)`:
    - preserves requested URL;
    - rewrites copy as needed;
    - returns diagnostics (`copy_rewrite_applied`, `copy_rewrite_reason`, `candidate_fallback_used=false`).
  - expanded post-publish review checklist and persisted these checks in review payload.
- Tests added in `/opt/coatue-claw/tests/test_x_chart_daily.py`:
  - `test_takeaway_sentence_validator_rejects_fragment_fell_to_lowest`
  - `test_takeaway_sentence_finalizer_returns_complete_sentence`
  - `test_style_draft_rewrites_degenerate_fields_and_fragment_takeaway`
  - `test_run_chart_scout_falls_back_when_top_candidate_copy_is_bad`
  - `test_run_chart_for_post_url_rewrites_takeaway_but_keeps_requested_url`
- Validation:
  - `PYTHONPATH=/opt/coatue-claw/src /opt/coatue-claw/.venv/bin/python -m pytest -q /opt/coatue-claw/tests/test_x_chart_daily.py` -> `49 passed`.
  - full suite smoke: `PYTHONPATH=/opt/coatue-claw/src /opt/coatue-claw/.venv/bin/python -m pytest -q` -> 3 pre-existing unrelated failures in `test_spencer_change_digest` / `test_spencer_change_log`.
- Runtime smoke checks completed:
  - `make -C /opt/coatue-claw openclaw-x-chart-run-once`
  - `/opt/coatue-claw/.venv/bin/python -m coatue_claw.x_chart_daily run-post-url https://x.com/Barchart/status/2025715989384663396`
  - both posted successfully and emitted rewrite diagnostics; review checks reported complete-sentence and non-degenerate copy.

### Immediate Next Steps
1. Pull latest `main` on Mac mini runtime and restart (`make openclaw-restart`).
2. In Slack `#charting`, run `@Coatue Claw x chart now` and confirm takeaway is a complete sentence with no clipped ending.
3. Run one explicit URL request and confirm posted source URL is unchanged while takeaway is rewritten when needed.
4. Triage unrelated full-suite failures in `tests/test_spencer_change_digest.py` and `tests/test_spencer_change_log.py` separately from X-chart pipeline.

## 2026-02-23 - X Chart Headline Completeness Guardrail
- Issue fixed: source-snip chart posts could still ship malformed titles (example observed in Slack: `U.S. Housing Market Home Sellers now is`).
- Root cause:
  - title pipeline had length + degenerate checks but no deterministic headline-phrase completeness gate;
  - `U.S.` abbreviation splitting in first-sentence extraction could collapse subject parsing and degrade headline quality.
- Code updates in `/opt/coatue-claw/src/coatue_claw/x_chart_daily.py`:
  - added headline-quality helpers:
    - `_is_complete_headline_phrase`
    - `_finalize_headline_phrase`
    - `_rewrite_headline_from_candidate`
    - `_strip_trailing_headline_dangling_endings`
  - added headline dangling ending rules (copulas/prepositions and fragment tails like `now is`).
  - integrated headline finalization + rewrite into `_sanitize_style_copy(...)`:
    - no generic forced fallback title on unrecoverable headline;
    - sets `copy_rewrite_reason=headline_unrecoverable` when title cannot be repaired.
  - fixed `_extract_first_sentence(...)` to avoid `U.S.` abbreviation truncation when the first segment is too short.
  - made headline completeness publish-critical:
    - `_style_copy_quality_errors` includes `headline incomplete phrase`
    - `_style_copy_publish_issues` includes `headline_incomplete_phrase`
    - style checks include `headline_complete_phrase`
    - post-review checks include `headline_complete_phrase`
  - scout behavior update:
    - if all candidates fail publish copy quality (including headline completeness), scout run returns non-post result:
      - `reason=no_publishable_candidate_available`
      - `publish_issues=[...]`
    - scout no longer force-posts top invalid candidate in this failure mode.
  - explicit URL behavior remains strict:
    - requested URL is preserved
    - run errors if headline remains invalid after rewrite.
- Tests added/updated in `/opt/coatue-claw/tests/test_x_chart_daily.py`:
  - `test_headline_phrase_validator_rejects_fragment_home_sellers_now_is`
  - `test_headline_phrase_finalizer_returns_empty_for_dangling_copula`
  - `test_style_draft_rewrites_broken_headline_phrase`
  - `test_run_chart_scout_falls_back_when_top_candidate_headline_is_incomplete`
  - `test_run_chart_scout_returns_non_post_when_no_publishable_candidate`
  - `test_run_chart_for_post_url_errors_when_headline_unrecoverable`
  - expanded explicit-URL rewrite regression to assert headline validity.
- Validation:
  - `PYTHONPATH=/opt/coatue-claw/src /opt/coatue-claw/.venv/bin/python -m pytest -q /opt/coatue-claw/tests/test_x_chart_daily.py` -> `55 passed`.
  - full smoke: `PYTHONPATH=/opt/coatue-claw/src /opt/coatue-claw/.venv/bin/python -m pytest -q` -> unchanged unrelated failures:
    - `tests/test_spencer_change_digest.py::test_run_once_dry_run_includes_carson_label`
    - `tests/test_spencer_change_log.py::test_is_spencer_user_defaults`
    - `tests/test_spencer_change_log.py::test_requester_label_defaults`

### Immediate Next Steps
1. Pull latest `main` on Mac mini and restart runtime (`make openclaw-restart`).
2. In Slack `#charting`, run one `x chart now` and confirm title is a complete grammatical phrase (no clipped ending).
3. Run one explicit URL command for the same source class and confirm:
   - URL is unchanged
   - title is either repaired or request fails with publish-copy error (no invalid title post).
4. Triage the unrelated Spencer-change identity defaults failures in a separate patch.

## 2026-02-23 - Board Seat Non-Repeat Guardrail (Anduril)
- User signal captured from `#anduril`: Spencer explicitly flagged repeated investment pitches (example callout: repeated Epirus idea) and requested net-new ideas unless material change.
- Runtime module updated: `/opt/coatue-claw/src/coatue_claw/board_seat_daily.py`
  - added persistent pitch-history store table: `board_seat_pitches` in `/opt/coatue-claw-data/db/board_seat_daily.sqlite`
  - pitch-history schema stores:
    - company/channel metadata
    - message text + extracted investment text
    - investment signature/hash
    - context signature/snippets
    - significant-change flag
  - existing `board_seat_runs` rows are auto-seeded into `board_seat_pitches` on startup (`legacy_run_seed`) to bootstrap historical memory.
  - added best-effort Slack history backfill path for each channel (`slack_history_backfill`) when history scope is available.
  - added deterministic repeat detection:
    - exact hash repeat block
    - high signal-line similarity repeat block
    - special repeat block for repeated “no high signal updates” theme
  - added significant-change gating:
    - allows prior idea only when context changed materially (token novelty/event-term or numeric delta)
  - if repeated idea detected without significant change:
    - tries one net-new fallback rewrite
    - if still repeated, skips posting with explicit reason `repeat_investment_without_significant_change`
  - LLM prompt now receives prior investment theses and is instructed not to repeat absent clear context change.
  - status payload now includes `pitch_counts` by company.
- Tests added/updated in `/opt/coatue-claw/tests/test_board_seat_daily.py`:
  - extraction test for structured investment text
  - repeat-without-change skip behavior
  - allow-post when significant change exists
  - history-backfill parsing test
- Validation:
  - `PYTHONPATH=/opt/coatue-claw/src /opt/coatue-claw/.venv/bin/python -m pytest -q /opt/coatue-claw/tests/test_board_seat_daily.py` -> `9 passed`
  - full smoke remains unchanged with 3 unrelated pre-existing failures in Spencer-change tests.
- Live runtime verification:
  - forced dry-run for Anduril now returns skip reason `repeat_investment_without_significant_change`
  - forced live run also skipped repeat post (no duplicate idea pushed)
  - current seeded Anduril pitch history count: `3`
- Slack history scope note:
  - direct `conversations_history` backfill for `#anduril` currently scans `0` messages under present bot scopes/token permissions.
  - existing historical posts are still captured via `legacy_run_seed` from `board_seat_runs`.

### Immediate Next Steps
1. Add/confirm Slack scopes for deep backfill (`channels:history` and private-channel equivalent as needed), reinstall app, then re-run history backfill.
2. Re-run board-seat force run in `#anduril` after new Anduril signals appear and confirm post is allowed only when significant change is detected.
3. Expand repeat detector from textual similarity to named-investment entity tracking (e.g., Epirus/Shield AI/Saronic) if repeated-name risk remains high.

## 2026-02-23 - X Chart Fragment-Tail Guardrails (Title + Takeaway)
- Runtime module updated: `/opt/coatue-claw/src/coatue_claw/x_chart_daily.py`
  - added deterministic tail-fragment dictionaries:
    - `TRAILING_DETERMINERS`
    - `TRAILING_QUALIFIERS`
  - added new validators:
    - `_has_fragment_tail(words)`
    - `_tail_complete(text)`
  - title/takeaway completeness now rejects clipped tails like:
    - `... in their`
    - `... in their initial`
    - `... in early`
  - title/takeaway finalizers now fail fast on fragment tails after clipping and force rewrite path instead of posting incomplete phrases.
  - headline/takeaway rewrite flow updated to apply fragment-tail rewrites and diagnostic reasons:
    - `headline_tail_fragment_rewritten`
    - `takeaway_tail_fragment_rewritten`
    - `headline_unrecoverable` (unchanged for hard failures)
  - style quality and publish gates now include tail-fragment checks:
    - `_style_copy_quality_errors(...)`
    - `_style_copy_publish_issues(...)`
  - style draft checks include:
    - `headline_tail_complete`
    - `takeaway_tail_complete`
  - post-review metadata now includes:
    - `_post_publish_checklist(...).checks.headline_tail_complete`
    - `_post_publish_checklist(...).checks.takeaway_tail_complete`
    - `_post_winner_to_slack(...).review.checks.headline_tail_complete`
    - `_post_winner_to_slack(...).review.checks.takeaway_tail_complete`
- Tests updated in `/opt/coatue-claw/tests/test_x_chart_daily.py`:
  - `test_headline_validator_rejects_trailing_possessive_fragment`
  - `test_takeaway_validator_rejects_trailing_possessive_fragment`
  - `test_headline_finalizer_returns_empty_for_clipped_their_fragment`
  - `test_takeaway_finalizer_returns_empty_for_clipped_initial_fragment`
  - `test_style_draft_rewrites_fragmented_kobeissi_copy`
  - `test_run_chart_scout_falls_back_when_top_candidate_has_fragment_tail`
  - updated explicit URL rewrite regression to assert fragment-tail rewrite behavior and diagnostics.
- Validation:
  - `PYTHONPATH=/opt/coatue-claw/src /opt/coatue-claw/.venv/bin/python -m pytest -q /opt/coatue-claw/tests/test_x_chart_daily.py` -> `61 passed`
  - full smoke: `PYTHONPATH=/opt/coatue-claw/src /opt/coatue-claw/.venv/bin/python -m pytest -q` -> unchanged unrelated failures:
    - `tests/test_spencer_change_digest.py::test_run_once_dry_run_includes_carson_label`
    - `tests/test_spencer_change_log.py::test_is_spencer_user_defaults`
    - `tests/test_spencer_change_log.py::test_requester_label_defaults`

### Immediate Next Steps
1. Pull latest `main` on the Mac mini and restart runtime (`make openclaw-restart`).
2. Trigger `x chart now` in Slack and confirm title/takeaway no longer end in clipped tails.
3. Trigger one explicit URL post for a fragment-prone source and confirm URL is preserved with rewritten coherent copy.

## 2026-02-23 - X Chart Run-On Clause Fix (Takeaway + Role Stability)
- Runtime module updated: `/opt/coatue-claw/src/coatue_claw/x_chart_daily.py`
  - added deterministic unjoined-clause detection for takeaway copy:
    - `_has_unjoined_clause_boundary(text)`
    - `_first_unjoined_clause_boundary_index(text)`
    - `_tokenize_clause_words(text)`
  - added deterministic repair path:
    - `_repair_takeaway_clause_boundary(text)` rewrites run-ons into one coherent sentence with connector (`while`) and re-validates.
  - takeaway validation/finalization tightened:
    - `_is_single_sentence_takeaway(...)` now rejects unjoined clause boundaries.
    - `_finalize_takeaway_sentence(...)` now repairs run-on clause boundaries before accepting.
  - sanitize flow updated:
    - `_sanitize_style_copy(...)` marks rewrite diagnostic `takeaway_clause_rewritten` when this recovery path is applied.
  - publish gates tightened:
    - `_style_copy_quality_errors(...)` now adds `takeaway clause boundary invalid`.
    - `_style_copy_publish_issues(...)` now adds `takeaway_clause_boundary_invalid`.
  - review/style checks expanded:
    - `takeaway_clause_boundary_ok` added to style/review check payloads.
  - role-stability hardening:
    - `_enforce_title_takeaway_roles(...)` now compacts run-on headlines to concise locked-term-safe core sentence when role order is otherwise valid.
- Tests updated in `/opt/coatue-claw/tests/test_x_chart_daily.py`:
  - `test_takeaway_validator_rejects_market_cap_ai_runon`
  - `test_takeaway_finalizer_repairs_unjoined_clause_boundary`
  - `test_role_enforcement_compacts_runon_headline_when_role_order_is_valid`
  - updated Kobeissi regression expected takeaway to coherent one-sentence form:
    - `US stocks erase nearly -$800 billion in market cap while AI disruption fears spread and trade war headlines return.`
- Validation:
  - targeted:
    - `PYTHONPATH=/opt/coatue-claw/src /opt/coatue-claw/.venv/bin/python -m pytest -q /opt/coatue-claw/tests/test_x_chart_daily.py /opt/coatue-claw/tests/test_slack_x_chart_intent.py` -> `77 passed`
  - full smoke:
    - `PYTHONPATH=/opt/coatue-claw/src /opt/coatue-claw/.venv/bin/python -m pytest -q` -> unchanged unrelated failures:
      - `tests/test_spencer_change_digest.py::test_run_once_dry_run_includes_carson_label`
      - `tests/test_spencer_change_log.py::test_is_spencer_user_defaults`
      - `tests/test_spencer_change_log.py::test_requester_label_defaults`
- Live validation:
  - explicit URL reposted:
    - `https://x.com/KobeissiLetter/status/2026040229535047769`
  - result payload showed:
    - `copy_rewrite_reason = takeaway_clause_rewritten`
    - `takeaway_clause_boundary_ok = true`
    - `takeaway_single_sentence = true`
    - `title_takeaway_role_ok = true`
    - all post-review checks passed.

### Immediate Next Steps
1. In Slack `#charting`, visually confirm the latest Kobeissi repost shows:
   - concise title ending with `market cap.`
   - one-sentence takeaway using connector (`while`) and terminal punctuation.
2. Monitor the next 3 manual `run-post-url` uses for `takeaway_clause_rewritten` rate; if frequent, add pre-LLM prompt guidance to avoid unjoined clause patterns earlier.
3. Keep Spencer-change identity failures out of this charting branch and patch separately.

## 2026-02-23 - X Chart Post Copy Cleanup + #charting Purge Attempt
- Runtime module updated: `/opt/coatue-claw/src/coatue_claw/x_chart_daily.py`
  - removed `- Render: ...` line from Slack chart post `initial_comment` output.
  - post summary now includes only:
    - Source
    - Title
    - Key takeaway
    - Link
- Test updated: `/opt/coatue-claw/tests/test_x_chart_daily.py`
  - `test_post_winner_preserves_takeaway_punctuation_in_slack_comment` now asserts `- Render:` is absent.
- Validation:
  - targeted tests:
    - `PYTHONPATH=/opt/coatue-claw/src /opt/coatue-claw/.venv/bin/python -m pytest -q /opt/coatue-claw/tests/test_x_chart_daily.py /opt/coatue-claw/tests/test_slack_x_chart_intent.py` -> `77 passed`
- Slack channel cleanup run (`#charting`, channel id `C0AFXM2MWAV`):
  - API delete pass results:
    - `messages_seen: 48`
    - `deleted: 36`
    - `cant_delete_message: 12`
  - follow-up pass:
    - `messages_seen: 11`
    - `deleted_now: 0`
    - `cant_delete_now: 11`
  - remaining messages are non-bot/user or channel-system events (join/rename/user posts), which the current bot token cannot delete.

### Immediate Next Steps
1. If full channel wipe is still required, use a user/admin token with delete rights (or manual Slack admin UI deletion for non-bot messages).
2. Keep current bot deletion helper for future bot-authored cleanup runs.

## 2026-02-23 - DM Routing Fix for SPClaw
- Runtime modules updated:
  - `/opt/coatue-claw/src/coatue_claw/slack_routing.py`
    - added `should_route_message_event(text, channel_type)`:
      - always routes non-empty direct-message events (`channel_type=im`), even if the DM includes `<@SPClaw>` mention markup.
      - preserves existing mention-based default routing behavior for channel messages.
  - `/opt/coatue-claw/src/coatue_claw/slack_bot.py`
    - `handle_message` now uses `should_route_message_event(...)` instead of only `should_default_route_message(...)`.
    - DM events are tagged as `source_event=slack-message-dm` for logging/memory source traceability.
- Tests updated:
  - `/opt/coatue-claw/tests/test_slack_routing.py`
    - added `test_should_route_message_event_im_always_routes_nonempty`
    - added `test_should_route_message_event_non_im_follows_default_rules`
- Validation:
  - `PYTHONPATH=/opt/coatue-claw/src /opt/coatue-claw/.venv/bin/python -m pytest -q /opt/coatue-claw/tests/test_slack_routing.py /opt/coatue-claw/tests/test_x_chart_daily.py /opt/coatue-claw/tests/test_slack_x_chart_intent.py` -> `82 passed`
  - runtime restart:
    - `make openclaw-restart` (gateway restarted)
    - `make openclaw-slack-status` -> Slack probe `ok=true`, bot `coatue_claw` connected.

### Immediate Next Steps
1. DM `SPClaw` a plain message and a second message including `@SPClaw` to verify both trigger replies.
2. If DM still does not trigger, check Slack app Event Subscriptions include DM `message` events (`message.im`) and reinstall app.
