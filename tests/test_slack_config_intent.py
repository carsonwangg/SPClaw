from __future__ import annotations

from spclaw.slack_config_intent import parse_config_intent


def test_parse_show_settings_intent():
    intent = parse_config_intent("@SPClaw show my settings")
    assert intent is not None
    assert intent.kind == "show"


def test_parse_peer_limit_from_layman_phrase():
    intent = parse_config_intent("going forward please look for about 12 peers instead of 8")
    assert intent is not None
    assert intent.kind == "set"
    assert intent.key == "peer_discovery_limit"
    assert intent.value == 12


def test_parse_default_x_axis_from_layman_phrase():
    intent = parse_config_intent("Use market cap as the default x-axis going forward")
    assert intent is not None
    assert intent.kind == "set"
    assert intent.key == "default_x_metric"
    assert intent.value == "market_cap"


def test_parse_followup_prompt_from_layman_phrase():
    intent = parse_config_intent("When you finish a chart, ask us if we want ticker changes")
    assert intent is not None
    assert intent.kind == "set"
    assert intent.key == "followup_prompt"
    assert intent.value == "us if we want ticker changes?"


def test_parse_promote_and_undo_intents():
    promote = parse_config_intent("promote current settings")
    assert promote is not None
    assert promote.kind == "promote"

    undo = parse_config_intent("undo last promotion")
    assert undo is not None
    assert undo.kind == "undo_promotion"
