# File Management System (Local-First + Drive Share)

## Objective
Keep file operations local on Mac mini (so OpenClaw can freely rearrange) while publishing stable, referenceable outputs to a shared Google Drive folder for Spencer.

## Architecture

### Local canonical paths (bot-owned)
- `/opt/coatue-claw-data/files/working`
- `/opt/coatue-claw-data/files/archive`
- `/opt/coatue-claw-data/files/published`
- `/opt/coatue-claw-data/files/incoming`

### Shared Drive mirror (human-facing)
Configured under `drive.root` in `config/file-bridge.json`:
- `Latest`
- `Archive`
- `Incoming`

Default fallback in repo is local-only:
- `/opt/coatue-claw-data/files/drive-share`

Replace this with the actual Google Drive folder path on Mac mini (Google Drive Desktop mount) to publish for Spencer.

## Ownership Rules
- Bot writes locally (`working`, `archive`, `published`).
- Bot publishes to Drive `Latest` and `Archive`.
- Humans drop files into Drive `Incoming`.
- Bot pulls Drive `Incoming` into local `incoming`.
- Subfolders are preserved recursively (Drive -> local and local -> Drive), so humans can organize by topic and the bot keeps the same paths.

Avoid direct human edits inside bot-owned local folders to prevent sync conflicts.

## Commands

### Initialize layout
```bash
make openclaw-files-init
```

### Status snapshot
```bash
make openclaw-files-status
```

### Pull incoming files from Drive -> local incoming
```bash
make openclaw-files-sync-pull
```

### Push local published/archive -> Drive Latest/Archive
```bash
make openclaw-files-sync-push
```

### Full sync (pull + push + index)
```bash
make openclaw-files-sync
```

## Recommended Shared Folder Map (Spencer-facing)
Create and use the same category folders under all three Drive folders: `Incoming`, `Latest`, and `Archive`.

- `Companies`
- `Sectors`
- `Themes`
- `Earnings`
- `Filings`
- `Transcripts`
- `Decks`
- `Models`
- `Notes`
- `Calls`
- `Macro`
- `Admin`
- `Misc`

Example human workflow:
- Spencer drops source material into `Incoming/Companies` or `Incoming/Earnings`.
- OpenClaw pulls files to matching local paths (for example `.../incoming/Companies/...`).
- Generated outputs are published to `Latest/<same category>`.
- Older outputs are moved to `Archive/<same category>`.

### Rebuild published index files
```bash
make openclaw-files-index
```

## Index Artifacts
Written to local published root:
- `index.json`
- `index.md`

These provide stable references for Spencer and operational auditability.

## Config

File: `config/file-bridge.json`

Key fields:
- `local.working|archive|published|incoming`
- `drive.root|latest|archive|incoming`
- `rclone.enabled` (optional alternate transport)

## Optional rclone mode
If `rclone.enabled=true` and `remote_root` is set, sync commands use rclone instead of local folder copy.

## Safety Notes
- Default sync is non-destructive (copy semantics).
- Destructive mirror (`--delete`) is available only through direct CLI usage and should be used cautiously.
- Generated insight files should include source + timestamp metadata in content or sidecar artifacts.
