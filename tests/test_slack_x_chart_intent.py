from __future__ import annotations

from coatue_claw.slack_x_chart_intent import parse_x_chart_post_intent


def test_parse_compound_intent_add_source_and_chart() -> None:
    text = (
        "Please make a chart of the day from this post https://x.com/oguzerkan/status/2024447368137994460 "
        "and also add this guy to our twitter list"
    )
    intent = parse_x_chart_post_intent(text)
    assert intent is not None
    assert intent.run_chart is True
    assert intent.add_source is True
    assert intent.handle == "oguzerkan"
    assert intent.tweet_id == "2024447368137994460"
    assert intent.title_override is None


def test_parse_compound_intent_chart_only() -> None:
    text = "Output a coatue style chart from this post: https://x.com/fiscal_AI/status/1234567890"
    intent = parse_x_chart_post_intent(text)
    assert intent is not None
    assert intent.run_chart is True
    assert intent.add_source is False
    assert intent.title_override is None


def test_parse_compound_intent_with_title_override() -> None:
    text = (
        "x chart from https://x.com/KobeissiLetter/status/2026040229535047769 "
        "title: US stocks erase nearly $800 billion in market cap."
    )
    intent = parse_x_chart_post_intent(text)
    assert intent is not None
    assert intent.run_chart is True
    assert intent.add_source is False
    assert intent.title_override == "US stocks erase nearly $800 billion in market cap."


def test_parse_compound_intent_none_without_action_phrase() -> None:
    text = "https://x.com/fiscal_AI/status/1234567890"
    intent = parse_x_chart_post_intent(text)
    assert intent is None
