from __future__ import annotations

import pytest

from coatue_claw.hf_youtube_transcript import (
    TranscriptSegment,
    YouTubeTranscriptError,
    _asr_transcript,
    fetch_youtube_transcript,
    parse_youtube_video_id,
)


def test_parse_youtube_video_id_variants() -> None:
    assert parse_youtube_video_id("https://youtu.be/abcDEF12345") == "abcDEF12345"
    assert parse_youtube_video_id("https://www.youtube.com/watch?v=abcDEF12345") == "abcDEF12345"
    assert parse_youtube_video_id("https://youtube.com/shorts/abcDEF12345") == "abcDEF12345"
    assert parse_youtube_video_id("https://youtube.com/embed/abcDEF12345") == "abcDEF12345"
    assert parse_youtube_video_id("https://example.com/watch?v=abcDEF12345") is None


def test_fetch_youtube_transcript_captions_path(monkeypatch) -> None:
    monkeypatch.setattr(
        "coatue_claw.hf_youtube_transcript._fetch_video_metadata",
        lambda url, video_id: ("Title", "Channel", 123),
    )
    monkeypatch.setattr(
        "coatue_claw.hf_youtube_transcript._captions_transcript",
        lambda video_id: [
            TranscriptSegment(start_sec=0.0, end_sec=2.0, text="Hello world", source_type="captions")
        ],
    )

    transcript = fetch_youtube_transcript("https://youtu.be/abcDEF12345")

    assert transcript.video_id == "abcDEF12345"
    assert transcript.transcript_source == "captions"
    assert transcript.title == "Title"
    assert transcript.full_text == "Hello world"


def test_fetch_youtube_transcript_asr_fallback(monkeypatch) -> None:
    monkeypatch.setattr(
        "coatue_claw.hf_youtube_transcript._fetch_video_metadata",
        lambda url, video_id: ("Title", "Channel", 123),
    )

    def _raise(_video_id: str):
        raise YouTubeTranscriptError("captions_unavailable")

    monkeypatch.setattr("coatue_claw.hf_youtube_transcript._captions_transcript", _raise)
    monkeypatch.setattr(
        "coatue_claw.hf_youtube_transcript._asr_transcript",
        lambda url: [TranscriptSegment(start_sec=1.0, end_sec=3.0, text="ASR text", source_type="asr")],
    )

    transcript = fetch_youtube_transcript("https://youtu.be/abcDEF12345")
    assert transcript.transcript_source == "asr"
    assert transcript.full_text == "ASR text"


def test_fetch_youtube_transcript_raises_when_both_paths_fail(monkeypatch) -> None:
    monkeypatch.setattr(
        "coatue_claw.hf_youtube_transcript._fetch_video_metadata",
        lambda url, video_id: ("Title", "Channel", 123),
    )

    def _raise_caps(_video_id: str):
        raise YouTubeTranscriptError("captions_unavailable")

    def _raise_asr(_url: str):
        raise YouTubeTranscriptError("asr_unavailable")

    monkeypatch.setattr("coatue_claw.hf_youtube_transcript._captions_transcript", _raise_caps)
    monkeypatch.setattr("coatue_claw.hf_youtube_transcript._asr_transcript", _raise_asr)

    with pytest.raises(YouTubeTranscriptError, match="transcript_unavailable"):
        fetch_youtube_transcript("https://youtu.be/abcDEF12345")


def test_asr_transcript_retries_without_response_format_on_incompatible_model(monkeypatch, tmp_path) -> None:
    class _Transcriptions:
        def __init__(self) -> None:
            self.calls = 0

        def create(self, **kwargs):
            self.calls += 1
            if self.calls == 1:
                raise RuntimeError("response_format 'verbose_json' is not compatible with model 'x'")
            assert "response_format" not in kwargs
            return {"text": "Recovered ASR text"}

    class _Audio:
        def __init__(self) -> None:
            self.transcriptions = _Transcriptions()

    class _Client:
        def __init__(self, **kwargs) -> None:  # noqa: ANN003
            self.audio = _Audio()

    audio_path = tmp_path / "audio.mp3"
    audio_path.write_bytes(b"fake-audio")
    monkeypatch.setattr("coatue_claw.hf_youtube_transcript.OpenAI", _Client)
    monkeypatch.setattr("coatue_claw.hf_youtube_transcript._download_audio", lambda url, tmp_dir: audio_path)
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")

    segments = _asr_transcript("https://youtu.be/abcDEF12345")

    assert len(segments) == 1
    assert segments[0].text == "Recovered ASR text"
    assert segments[0].source_type == "asr"
