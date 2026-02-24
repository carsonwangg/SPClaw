.PHONY: openclaw-status openclaw-restart openclaw-logs openclaw-dev openclaw-bot-status openclaw-bot-logs openclaw-schedulers-status openclaw-slack-status openclaw-slack-logs openclaw-slack-probe openclaw-slack-audit openclaw-memory-status openclaw-memory-prune openclaw-memory-extract-daily openclaw-memory-reconcile-status openclaw-memory-reconcile-export openclaw-files-init openclaw-files-status openclaw-files-index openclaw-files-sync-pull openclaw-files-sync-push openclaw-files-sync openclaw-email-status openclaw-email-run-once openclaw-email-serve openclaw-x-chart-status openclaw-x-chart-run-once openclaw-x-chart-sources openclaw-x-chart-add-source openclaw-spencer-digest-status openclaw-spencer-digest-run-once openclaw-board-seat-status openclaw-board-seat-run-once openclaw-market-daily-status openclaw-market-daily-run-once openclaw-market-daily-refresh-holdings openclaw-market-daily-earnings-recap-run-once openclaw-24x7-enable openclaw-24x7-status openclaw-24x7-disable valuation-chart

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
	/opt/coatue-claw/.venv/bin/python -m coatue_claw.launchd_runtime status

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

openclaw-memory-reconcile-status:
	/opt/coatue-claw/.venv/bin/python -m coatue_claw.cli memory reconcile-status

openclaw-memory-reconcile-export:
	/opt/coatue-claw/.venv/bin/python -m coatue_claw.cli memory reconcile-export --limit $(or $(LIMIT),200)

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

openclaw-email-status:
	/opt/coatue-claw/.venv/bin/python -m coatue_claw.email_gateway status

openclaw-email-run-once:
	/opt/coatue-claw/.venv/bin/python -m coatue_claw.email_gateway run-once

openclaw-email-serve:
	/opt/coatue-claw/.venv/bin/python -m coatue_claw.email_gateway serve

openclaw-x-chart-status:
	/opt/coatue-claw/.venv/bin/python -m coatue_claw.x_chart_daily status

openclaw-x-chart-run-once:
	/opt/coatue-claw/.venv/bin/python -m coatue_claw.x_chart_daily run-once --manual

openclaw-x-chart-sources:
	/opt/coatue-claw/.venv/bin/python -m coatue_claw.x_chart_daily list-sources --limit $(or $(LIMIT),50)

openclaw-x-chart-add-source:
	/opt/coatue-claw/.venv/bin/python -m coatue_claw.x_chart_daily add-source $(HANDLE) --priority $(or $(PRIORITY),1.0)

openclaw-spencer-digest-status:
	/opt/coatue-claw/.venv/bin/python -m coatue_claw.spencer_change_digest status

openclaw-spencer-digest-run-once:
	/opt/coatue-claw/.venv/bin/python -m coatue_claw.spencer_change_digest run-once $(if $(FORCE),--force,)

openclaw-board-seat-status:
	/opt/coatue-claw/.venv/bin/python -m coatue_claw.board_seat_daily status

openclaw-board-seat-run-once:
	/opt/coatue-claw/.venv/bin/python -m coatue_claw.board_seat_daily run-once $(if $(FORCE),--force,) $(if $(DRY_RUN),--dry-run,)

openclaw-market-daily-status:
	/opt/coatue-claw/.venv/bin/python -m coatue_claw.market_daily status

openclaw-market-daily-run-once:
	/opt/coatue-claw/.venv/bin/python -m coatue_claw.market_daily run-once --manual $(if $(FORCE),--force,) $(if $(DRY_RUN),--dry-run,)

openclaw-market-daily-refresh-holdings:
	/opt/coatue-claw/.venv/bin/python -m coatue_claw.market_daily refresh-coatue-holdings

openclaw-market-daily-earnings-recap-run-once:
	/opt/coatue-claw/.venv/bin/python -m coatue_claw.market_daily run-earnings-recap --manual $(if $(FORCE),--force,) $(if $(DRY_RUN),--dry-run,)

openclaw-24x7-enable:
	/opt/coatue-claw/.venv/bin/python -m coatue_claw.launchd_runtime enable

openclaw-24x7-status:
	/opt/coatue-claw/.venv/bin/python -m coatue_claw.launchd_runtime status

openclaw-24x7-disable:
	/opt/coatue-claw/.venv/bin/python -m coatue_claw.launchd_runtime disable --remove-plists

valuation-chart:
	/opt/coatue-claw/.venv/bin/python -m coatue_claw.cli valuation-chart $(TICKERS)
