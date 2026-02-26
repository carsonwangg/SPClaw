from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys

import pytest

from coatue_claw import board_seat_daily


@pytest.fixture
def board_seat_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    data_root = tmp_path / "data"
    monkeypatch.setenv("COATUE_CLAW_DATA_ROOT", str(data_root))
    monkeypatch.setenv("COATUE_CLAW_BOARD_SEAT_DB_PATH", str(data_root / "db" / "board_seat_daily.sqlite"))
    monkeypatch.delenv("COATUE_CLAW_BOARD_SEAT_PORTCOS", raising=False)
    monkeypatch.delenv("COATUE_CLAW_BOARD_SEAT_ENABLED", raising=False)
    monkeypatch.delenv("COATUE_CLAW_BOARD_SEAT_RESET_MODE", raising=False)
    return data_root


def test_parse_portcos_default() -> None:
    parsed = board_seat_daily._parse_portcos("")
    assert len(parsed) == 10
    assert parsed[0] == ("Anduril", "anduril")


def test_parse_portcos_custom() -> None:
    parsed = board_seat_daily._parse_portcos("Anduril:anduril,OpenAI:#openai")
    assert parsed == [("Anduril", "anduril"), ("OpenAI", "openai")]


def test_run_once_reset_mode_skips_all(board_seat_env: Path) -> None:
    payload = board_seat_daily.run_once(force=True, dry_run=False)
    assert payload["ok"] is True
    assert payload["reset_mode"] is True
    assert payload["sent"] == []
    assert len(payload["skipped"]) == len(payload["portcos"])
    assert {row["reason"] for row in payload["skipped"]} == {"feature_reset_in_progress"}


def test_run_once_disabled_when_reset_off(board_seat_env: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("COATUE_CLAW_BOARD_SEAT_RESET_MODE", "0")
    monkeypatch.setenv("COATUE_CLAW_BOARD_SEAT_ENABLED", "0")
    payload = board_seat_daily.run_once(force=True, dry_run=False)
    assert payload["ok"] is True
    assert payload["reset_mode"] is False
    assert payload["sent"] == []
    assert {row["reason"] for row in payload["skipped"]} == {"board_seat_disabled"}


def test_run_once_dry_run_preview_when_enabled(board_seat_env: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("COATUE_CLAW_BOARD_SEAT_RESET_MODE", "0")
    monkeypatch.setenv("COATUE_CLAW_BOARD_SEAT_ENABLED", "1")
    payload = board_seat_daily.run_once(force=True, dry_run=True)
    assert payload["ok"] is True
    assert len(payload["sent"]) == len(payload["portcos"])
    assert payload["skipped"] == []
    assert payload["sent"][0]["delivery_mode_applied"] == "dry_run_preview"


def test_seed_target_and_target_memory(board_seat_env: Path) -> None:
    seeded = board_seat_daily._seed_target(company="Anduril", target="Saronic", channel_ref="anduril")
    assert seeded.inserted is True
    assert seeded.target_key == "saronic"

    store = board_seat_daily.BoardSeatStore()
    rows = store.target_ledger_rows(company="Anduril", limit=10)
    assert len(rows) == 1
    assert rows[0]["target"] == "Saronic"


def test_status_reports_reset_scaffold(board_seat_env: Path) -> None:
    payload = board_seat_daily.status()
    assert payload["ok"] is True
    assert payload["status"] == "reset_scaffold"
    assert payload["reset_mode"] is True
    assert payload["hard_gates"] == ["company_only_target", "cooldown_repeat_block"]


def test_refresh_funding_returns_not_implemented(board_seat_env: Path) -> None:
    payload = board_seat_daily._refresh_funding_payload(entities=["Anduril"], report=False)
    assert payload["ok"] is True
    assert payload["status"] == "not_implemented"


def test_cli_status_json(board_seat_env: Path) -> None:
    env = os.environ.copy()
    cmd = [sys.executable, "-m", "coatue_claw.board_seat_daily", "status"]
    out = subprocess.check_output(cmd, text=True, env=env)
    payload = json.loads(out)
    assert payload["ok"] is True
    assert payload["status"] == "reset_scaffold"


def test_cli_run_once_json(board_seat_env: Path) -> None:
    env = os.environ.copy()
    cmd = [sys.executable, "-m", "coatue_claw.board_seat_daily", "run-once", "--dry-run"]
    out = subprocess.check_output(cmd, text=True, env=env)
    payload = json.loads(out)
    assert payload["ok"] is True
    assert payload["format_version"] == board_seat_daily.BOARD_SEAT_FORMAT_VERSION
