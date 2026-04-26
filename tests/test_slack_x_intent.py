from __future__ import annotations

from spclaw.slack_x_intent import parse_x_digest_intent


def test_parse_basic_x_digest() -> None:
    intent = parse_x_digest_intent("x digest SNOW")
    assert intent is not None
    assert intent.kind == "digest"
    assert intent.query == "SNOW"
    assert intent.hours == 24
    assert intent.limit == 50


def test_parse_x_digest_with_window_and_limit() -> None:
    intent = parse_x_digest_intent("@SPClaw x digest snowflake ai last 48h limit 80")
    assert intent is not None
    assert intent.kind == "digest"
    assert intent.query == "snowflake ai"
    assert intent.hours == 48
    assert intent.limit == 80


def test_parse_x_status_and_help() -> None:
    status = parse_x_digest_intent("x status")
    assert status is not None
    assert status.kind == "status"

    help_intent = parse_x_digest_intent("twitter digest help")
    assert help_intent is not None
    assert help_intent.kind == "help"


def test_non_x_text_returns_none() -> None:
    assert parse_x_digest_intent("diligence SNOW") is None
