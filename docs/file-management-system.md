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
- `/Users/spclaw/Documents/SPClaw Database`
- `01_DROP_HERE_Incoming`
- `02_READ_ONLY_Latest_AUTO`
- `03_READ_ONLY_Archive_AUTO`

Incoming contains an auto-generated read-only reference mirror:
- `01_DROP_HERE_Incoming/_Latest_Reference_READ_ONLY`

Default fallback in repo is local-only:
- `/opt/coatue-claw-data/files/drive-share`

Replace this with the actual Google Drive folder path on Mac mini (Google Drive Desktop mount) to publish for Spencer.

## Ownership Rules
- Bot writes locally (`working`, `archive`, `published`).
- Bot publishes to Drive `02_READ_ONLY_Latest_AUTO` and `03_READ_ONLY_Archive_AUTO`.
- Humans drop files into Drive `01_DROP_HERE_Incoming` (category subfolders).
- Bot pulls Drive `01_DROP_HERE_Incoming` into local `incoming`.
- Subfolders are preserved recursively (Drive -> local and local -> Drive), so humans can organize by topic and the bot keeps the same paths.
- `01_DROP_HERE_Incoming/_Latest_Reference_READ_ONLY` is a carbon copy of Latest for visibility and is ignored during pull ingestion.
- Slack uploads are also ingested automatically:
  - user uploads a file in Slack
  - bot downloads, classifies, and stores under local `incoming/{Universes|Companies|Industries}`
  - bot mirrors the same file to Drive `01_DROP_HERE_Incoming/{Universes|Companies|Industries}`
  - ingest metadata is written to SQLite (`/opt/coatue-claw-data/db/file_ingest.sqlite`)

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
Use the same simplified category folders under all three Drive folders: `01_DROP_HERE_Incoming`, `02_READ_ONLY_Latest_AUTO`, and `03_READ_ONLY_Archive_AUTO`.

- `Universes`
- `Companies`
- `Industries`

Example human workflow:
- Spencer drops source material into `01_DROP_HERE_Incoming/Companies`, `01_DROP_HERE_Incoming/Industries`, or `01_DROP_HERE_Incoming/Universes`.
- OpenClaw pulls files to matching local paths (for example `.../incoming/Companies/...`).
- Generated outputs are published to `02_READ_ONLY_Latest_AUTO/<same category>`.
- Older outputs are moved to `03_READ_ONLY_Archive_AUTO/<same category>`.

Slack upload workflow:
- Spencer uploads files directly in Slack.
- Bot auto-acks with category routing summary in thread.
- Files become available in the same category structure as Drive drop-offs.

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
