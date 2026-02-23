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
- MD is now tuned for decisive “primary reason” output:
  - when one high-quality source clearly dominates cluster evidence, MD states the reason directly instead of defaulting to uncertainty
  - generic wrapper blocking remains enforced to prevent tautological headlines
  - fallback line is still used only for genuinely weak/ambiguous evidence
  - env controls:
    - `COATUE_CLAW_MD_DECISIVE_PRIMARY_REASON_ENABLED=1`
    - `COATUE_CLAW_MD_DECISIVE_PRIMARY_REASON_MIN_SCORE=0.64`
    - `COATUE_CLAW_MD_DECISIVE_PRIMARY_REASON_MIN_MARGIN=0.06`
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
  - evidence stack upgraded from X+Yahoo to X + Yahoo + DDG fallback (`COATUE_CLAW_MD_WEB_SEARCH_ENABLED=1`)
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
