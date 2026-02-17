#!/usr/bin/env python3
import json
import subprocess
import sys

cmd = ["openclaw", "channels", "status", "--probe", "--json"]
proc = subprocess.run(cmd, capture_output=True, text=True)
if proc.returncode != 0:
    print(proc.stderr.strip() or proc.stdout.strip())
    sys.exit(proc.returncode)

try:
    payload = json.loads(proc.stdout)
except json.JSONDecodeError as exc:
    print(f"Invalid JSON from openclaw: {exc}")
    print(proc.stdout)
    sys.exit(2)

accounts = payload.get("channelAccounts", {}).get("slack", [])
if not accounts:
    print("Slack channel account not configured")
    sys.exit(3)

acct = accounts[0]
probe = acct.get("probe", {})
print("Slack account:", acct.get("accountId"))
print("Running:", acct.get("running"))
print("Probe OK:", probe.get("ok"), f"status={probe.get('status')}", f"elapsedMs={probe.get('elapsedMs')}")
print("Bot:", (probe.get("bot") or {}).get("name"), (probe.get("bot") or {}).get("id"))
print("Team:", (probe.get("team") or {}).get("name"), (probe.get("team") or {}).get("id"))
print("Last inbound:", acct.get("lastInboundAt"))
print("Last outbound:", acct.get("lastOutboundAt"))

if not probe.get("ok"):
    sys.exit(4)
