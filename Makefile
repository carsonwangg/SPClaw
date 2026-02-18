.PHONY: openclaw-status openclaw-restart openclaw-logs openclaw-dev openclaw-bot-status openclaw-bot-logs openclaw-schedulers-status openclaw-slack-status openclaw-slack-logs openclaw-slack-probe openclaw-slack-audit valuation-chart

export PATH := /opt/homebrew/bin:$(PATH)
OPENCLAW ?= $(shell command -v openclaw 2>/dev/null || echo /opt/homebrew/bin/openclaw)

openclaw-status:
	$(OPENCLAW) gateway status

openclaw-restart:
	$(OPENCLAW) gateway restart

openclaw-logs:
	$(OPENCLAW) logs --follow

openclaw-dev:
	$(MAKE) openclaw-status
	$(MAKE) openclaw-slack-status

openclaw-bot-status:
	$(MAKE) openclaw-slack-status

openclaw-bot-logs:
	$(MAKE) openclaw-slack-logs

openclaw-schedulers-status:
	@echo "No scheduled OpenClaw jobs are wired yet (weekly idea scan and X digest are pending implementation)."

openclaw-slack-status:
	$(OPENCLAW) channels status --probe --json

openclaw-slack-logs:
	$(OPENCLAW) channels logs --channel slack --lines 300

openclaw-slack-probe:
	python3 scripts/openclaw_slack_probe.py

openclaw-slack-audit:
	python3 scripts/openclaw_slack_audit.py --limit 20

valuation-chart:
	/opt/coatue-claw/.venv/bin/python -m coatue_claw.cli valuation-chart $(TICKERS)
