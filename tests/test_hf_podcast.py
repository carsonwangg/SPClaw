from __future__ import annotations

from coatue_claw.hf_podcast import (
    PodcastQuote,
    _validate_quotes,
    build_podcast_analysis,
    format_timestamp,
    render_full_transcript_markdown,
    render_podcast_summary_markdown,
)
from coatue_claw.hf_youtube_transcript import PodcastTranscript, TranscriptSegment


def _sample_transcript() -> PodcastTranscript:
    return PodcastTranscript(
        url="https://youtu.be/abcDEF12345",
        video_id="abcDEF12345",
        title="Sample Episode",
        channel_name="Sample Channel",
        duration_sec=600,
        transcript_source="captions",
        segments=(
            TranscriptSegment(start_sec=12.0, end_sec=20.0, text="We think demand is accelerating this quarter.", source_type="captions"),
            TranscriptSegment(start_sec=45.0, end_sec=60.0, text="The biggest risk is margin pressure from pricing.", source_type="captions"),
            TranscriptSegment(start_sec=80.0, end_sec=95.0, text="A catalyst could be the enterprise launch in Q3.", source_type="captions"),
        ),
    )


def test_format_timestamp() -> None:
    assert format_timestamp(65) == "01:05"
    assert format_timestamp(3661) == "01:01:01"


def test_validate_quotes_drops_non_verbatim() -> None:
    transcript = _sample_transcript()
    quotes = [
        PodcastQuote(
            quote="We think demand is accelerating this quarter.",
            timestamp_sec=12.0,
            why_it_matters="Demand callout.",
        ),
        PodcastQuote(
            quote="This line does not exist in transcript.",
            timestamp_sec=20.0,
            why_it_matters="Should be dropped.",
        ),
    ]

    validated, warnings = _validate_quotes(quotes, transcript)

    assert len(validated) == 1
    assert validated[0].quote == "We think demand is accelerating this quarter."
    assert "quote_not_verbatim_dropped" in warnings


def test_build_podcast_analysis_fallback(monkeypatch) -> None:
    transcript = _sample_transcript()
    monkeypatch.setattr("coatue_claw.hf_podcast._model_analysis", lambda *args, **kwargs: None)

    analysis = build_podcast_analysis(transcript, question="focus on catalysts")

    assert analysis.executive_summary
    assert analysis.key_themes
    assert 1 <= len(analysis.quotes) <= 5


def test_render_summary_and_transcript_markdown() -> None:
    transcript = _sample_transcript()
    analysis = build_podcast_analysis(transcript, question=None)

    summary_md = render_podcast_summary_markdown(
        transcript=transcript,
        analysis=analysis,
        generated_at_utc="2026-02-26T00:00:00+00:00",
    )
    transcript_md = render_full_transcript_markdown(
        transcript=transcript,
        generated_at_utc="2026-02-26T00:00:00+00:00",
    )

    assert "## Executive Summary" in summary_md
    assert "## Top 5 Interesting Quotes" in summary_md
    assert "## Sources" in summary_md
    assert "## Transcript" in transcript_md
    assert "[00:12]" in transcript_md
