from __future__ import annotations

from spclaw.slack_pipeline_intent import parse_pipeline_intent


def test_parse_deploy_latest_intent():
    intent = parse_pipeline_intent("@SPClaw deploy latest")
    assert intent is not None
    assert intent.kind == "deploy_latest"


def test_parse_undo_last_deploy_intent():
    intent = parse_pipeline_intent("undo last deploy")
    assert intent is not None
    assert intent.kind == "undo_last_deploy"


def test_parse_run_checks_intent():
    intent = parse_pipeline_intent("please run checks")
    assert intent is not None
    assert intent.kind == "run_checks"


def test_parse_status_and_history_intent():
    status = parse_pipeline_intent("show pipeline status")
    assert status is not None
    assert status.kind == "status"

    history = parse_pipeline_intent("show deploy history")
    assert history is not None
    assert history.kind == "history"


def test_parse_build_request_intent_from_colon_syntax():
    intent = parse_pipeline_intent("build: add a command that posts chart summary to thread")
    assert intent is not None
    assert intent.kind == "build_request"
    assert intent.request == "add a command that posts chart summary to thread"


def test_chart_phrase_does_not_trigger_pipeline_build():
    intent = parse_pipeline_intent("please build a chart for SNOW and MDB")
    assert intent is None
