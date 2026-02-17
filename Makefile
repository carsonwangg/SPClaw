.PHONY: openclaw-status openclaw-restart openclaw-logs openclaw-slack-status openclaw-slack-logs openclaw-slack-probe openclaw-slack-audit valuation-chart

openclaw-status:
	openclaw gateway status

openclaw-restart:
	openclaw gateway restart

openclaw-logs:
	openclaw logs --follow

openclaw-slack-status:
	openclaw channels status --probe --json

openclaw-slack-logs:
	openclaw channels logs --channel slack --lines 300

openclaw-slack-probe:
	python3 scripts/openclaw_slack_probe.py

openclaw-slack-audit:
	python3 scripts/openclaw_slack_audit.py --limit 20

valuation-chart:
	/opt/coatue-claw/.venv/bin/python -m coatue_claw.cli valuation-chart $(TICKERS)
