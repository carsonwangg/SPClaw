.PHONY: openclaw-status openclaw-restart openclaw-logs openclaw-dev openclaw-bot-status openclaw-bot-logs openclaw-schedulers-status openclaw-slack-status openclaw-slack-logs openclaw-slack-probe openclaw-slack-audit openclaw-memory-status openclaw-memory-prune openclaw-memory-extract-daily openclaw-files-init openclaw-files-status openclaw-files-index openclaw-files-sync-pull openclaw-files-sync-push openclaw-files-sync valuation-chart

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
	@echo "Scheduler hooks:"
	@echo "- memory prune (hourly target): make openclaw-memory-prune"
	@echo "- memory extract backfill: make openclaw-memory-extract-daily DAYS=14"
	@echo "- files sync pull/push: make openclaw-files-sync"

openclaw-slack-status:
	$(OPENCLAW) channels status --probe --json

openclaw-slack-logs:
	$(OPENCLAW) channels logs --channel slack --lines 300

openclaw-slack-probe:
	python3 scripts/openclaw_slack_probe.py

openclaw-slack-audit:
	python3 scripts/openclaw_slack_audit.py --limit 20

openclaw-memory-status:
	/opt/coatue-claw/.venv/bin/python -m coatue_claw.cli memory status

openclaw-memory-prune:
	/opt/coatue-claw/.venv/bin/python -m coatue_claw.cli memory prune

openclaw-memory-extract-daily:
	/opt/coatue-claw/.venv/bin/python -m coatue_claw.cli memory extract-daily --days $(or $(DAYS),14)

openclaw-files-init:
	/opt/coatue-claw/.venv/bin/python -m coatue_claw.file_bridge init-layout

openclaw-files-status:
	/opt/coatue-claw/.venv/bin/python -m coatue_claw.file_bridge status

openclaw-files-index:
	/opt/coatue-claw/.venv/bin/python -m coatue_claw.file_bridge index

openclaw-files-sync-pull:
	/opt/coatue-claw/.venv/bin/python -m coatue_claw.file_bridge pull

openclaw-files-sync-push:
	/opt/coatue-claw/.venv/bin/python -m coatue_claw.file_bridge push

openclaw-files-sync:
	/opt/coatue-claw/.venv/bin/python -m coatue_claw.file_bridge sync

valuation-chart:
	/opt/coatue-claw/.venv/bin/python -m coatue_claw.cli valuation-chart $(TICKERS)
