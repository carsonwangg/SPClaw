# Email Integration (Spencer <-> Coatue Claw)

## Objective
Allow Spencer to interact with Coatue Claw via email using the same core workflows (diligence, memory queries, file ingest).

## How It Works
- Inbound email is polled from IMAP.
- Message subject/body is parsed into a command.
- Bot executes command and replies by SMTP.
- Any email attachments are auto-ingested into the knowledge folders:
  - local: `/opt/coatue-claw-data/files/incoming/<Category>/...`
  - shared Drive: `01_DROP_HERE_Incoming/<Category>/...`
- Ingestion and message processing are logged in:
  - `/opt/coatue-claw-data/db/email_gateway.sqlite`

## Supported Commands (email body or subject)
- `diligence TICKER`
- `dilligence TICKER` (typo alias)
- `memory status`
- `memory query <phrase>`
- `files status`
- `help`

For diligence replies:
- Email body now returns a readable executive summary (quick takeaways + top risks), with citation tails removed for readability.
- Full memo is attached as a `.md` file so the complete report is preserved without inline wall-of-text formatting.

## Required Environment Variables (Mac mini `.env.prod`)
- `COATUE_CLAW_EMAIL_ENABLED=true`
- `COATUE_CLAW_EMAIL_IMAP_HOST=<imap host>`
- `COATUE_CLAW_EMAIL_IMAP_PORT=993`
- `COATUE_CLAW_EMAIL_IMAP_USER=<email username>`
- `COATUE_CLAW_EMAIL_IMAP_PASSWORD=<email password or app password>`
- `COATUE_CLAW_EMAIL_IMAP_MAILBOX=INBOX`
- `COATUE_CLAW_EMAIL_SMTP_HOST=<smtp host>`
- `COATUE_CLAW_EMAIL_SMTP_PORT=587`
- `COATUE_CLAW_EMAIL_SMTP_USER=<email username>`
- `COATUE_CLAW_EMAIL_SMTP_PASSWORD=<email password or app password>`
- `COATUE_CLAW_EMAIL_FROM=<bot sender email>`

Optional:
- `COATUE_CLAW_EMAIL_ALLOWED_SENDERS=spencer@domain.com,carson@domain.com`
- `COATUE_CLAW_EMAIL_POLL_SECONDS=60`
- `COATUE_CLAW_EMAIL_MAX_ATTACHMENT_MB=25`
- `COATUE_CLAW_EMAIL_DB_PATH=/opt/coatue-claw-data/db/email_gateway.sqlite`

## Operations
Status:
```bash
make openclaw-email-status
```

Poll once:
```bash
make openclaw-email-run-once
```

Run continuous poller:
```bash
make openclaw-email-serve
```

## 24/7 Production Mode (Recommended)
Enable launchd supervision so email polling survives logouts/reboots:

```bash
cd /opt/coatue-claw
make openclaw-24x7-enable
make openclaw-24x7-status
```

This installs:
- `~/Library/LaunchAgents/com.coatueclaw.email-gateway.plist`
- `~/Library/LaunchAgents/com.coatueclaw.memory-prune.plist`

Disable/remove:

```bash
make openclaw-24x7-disable
```

## Recommended Initial Validation
1. Send an email with subject `diligence SNOW`.
2. Confirm reply email arrives with packet preview.
3. Send an email with attachment `AAPL-10Q.pdf`.
4. Confirm attachment lands in `incoming/Filings` and appears in Drive `01_DROP_HERE_Incoming/Filings`.
