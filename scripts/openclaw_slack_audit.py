#!/usr/bin/env python3
import argparse
import json
import re
from pathlib import Path

SESSIONS_INDEX = Path('/Users/spclaw/.openclaw/agents/main/sessions/sessions.json')
DEFAULT_LIMIT = 20
SYSTEM_RE = re.compile(
    r'^System: \[(?P<ts>[^\]]+)\] Slack message in #(?P<channel>[^\s]+) from (?P<user>[^:]+): (?P<text>.*)$'
)


def _extract_text(content):
    if isinstance(content, str):
        return content
    if not isinstance(content, list):
        return ''
    parts = []
    for part in content:
        if isinstance(part, dict) and part.get('type') == 'text':
            txt = part.get('text')
            if isinstance(txt, str):
                parts.append(txt)
    return '\n'.join(parts).strip()


def _iter_slack_session_files(index_path):
    if not index_path.exists():
        return []
    data = json.loads(index_path.read_text())
    files = []
    for session in data.values():
        delivery = session.get('deliveryContext') or {}
        if delivery.get('channel') != 'slack':
            continue
        session_file = session.get('sessionFile')
        if not session_file:
            continue
        p = Path(session_file)
        if p.exists():
            files.append(p)
    files.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return files


def _parse_events(session_file):
    events = []
    pending = None
    for line in session_file.read_text().splitlines():
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue
        if obj.get('type') != 'message':
            continue

        msg = obj.get('message') or {}
        role = msg.get('role')
        text = _extract_text(msg.get('content'))

        if role == 'user':
            first_line = text.splitlines()[0].strip() if text else ''
            match = SYSTEM_RE.match(first_line)
            if not match:
                continue
            pending = {
                'at': match.group('ts'),
                'channel': match.group('channel'),
                'user': match.group('user'),
                'request': match.group('text').strip(),
                'reply': '',
                'session_file': str(session_file),
            }
            events.append(pending)
            continue

        if role == 'assistant' and pending is not None:
            if text:
                pending['reply'] = text
                pending = None

    return events


def main():
    parser = argparse.ArgumentParser(description='Show Slack-triggered OpenClaw activity')
    parser.add_argument('--limit', type=int, default=DEFAULT_LIMIT)
    args = parser.parse_args()

    files = _iter_slack_session_files(SESSIONS_INDEX)
    all_events = []
    for file in files:
        all_events.extend(_parse_events(file))

    all_events.sort(key=lambda e: e.get('at', ''), reverse=True)
    rows = all_events[: max(args.limit, 1)]

    if not rows:
        print('No Slack audit events found.')
        return

    print('Slack audit trail (latest first)')
    print('=' * 80)
    for i, event in enumerate(rows, start=1):
        reply_preview = event['reply'].replace('\n', ' ').strip()
        if len(reply_preview) > 160:
            reply_preview = reply_preview[:157] + '...'
        print(f"{i}. at={event['at']} channel=#{event['channel']} user={event['user']}")
        print(f"   request: {event['request']}")
        print(f"   reply:   {reply_preview or '[assistant reply omitted in event stream]'}")


if __name__ == '__main__':
    main()
