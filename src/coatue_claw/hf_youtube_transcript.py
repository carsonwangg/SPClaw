from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path
import re
import tempfile
from typing import Any
from urllib.parse import parse_qs, urlparse

try:
    from youtube_transcript_api import YouTubeTranscriptApi
except Exception:  # pragma: no cover - optional dependency
    YouTubeTranscriptApi = None  # type: ignore[assignment]

try:
    import yt_dlp
except Exception:  # pragma: no cover - optional dependency
    yt_dlp = None  # type: ignore[assignment]

try:
    from openai import OpenAI
except Exception:  # pragma: no cover - optional dependency
    OpenAI = None  # type: ignore[assignment]


class YouTubeTranscriptError(RuntimeError):
    pass


def _venv_install_hint() -> str:
    return "/opt/coatue-claw/.venv/bin/python -m pip install -U youtube-transcript-api yt-dlp"


@dataclass(frozen=True)
class TranscriptSegment:
    start_sec: float
    end_sec: float
    text: str
    source_type: str


@dataclass(frozen=True)
class PodcastTranscript:
    url: str
    video_id: str
    title: str
    channel_name: str
    duration_sec: int | None
    transcript_source: str
    segments: tuple[TranscriptSegment, ...]

    @property
    def full_text(self) -> str:
        return "\n".join(segment.text for segment in self.segments if segment.text.strip())


def parse_youtube_video_id(url: str) -> str | None:
    raw = (url or "").strip()
    if not raw:
        return None
    parsed = urlparse(raw)
    host = (parsed.netloc or "").lower()
    path = parsed.path or ""
    if "youtu.be" in host:
        value = path.strip("/").split("/")[0]
        return value if re.fullmatch(r"[A-Za-z0-9_-]{6,32}", value or "") else None
    if "youtube.com" in host or "music.youtube.com" in host:
        if path.startswith("/watch"):
            qs = parse_qs(parsed.query or "")
            value = (qs.get("v") or [""])[0]
            return value if re.fullmatch(r"[A-Za-z0-9_-]{6,32}", value or "") else None
        if path.startswith("/shorts/") or path.startswith("/embed/"):
            value = path.strip("/").split("/")[1] if len(path.strip("/").split("/")) > 1 else ""
            return value if re.fullmatch(r"[A-Za-z0-9_-]{6,32}", value or "") else None
    return None


def is_youtube_url(url: str) -> bool:
    return parse_youtube_video_id(url) is not None


def _fetch_video_metadata(url: str, video_id: str) -> tuple[str, str, int | None]:
    if yt_dlp is None:
        return (f"YouTube Video {video_id}", "Unknown", None)
    opts = {
        "quiet": True,
        "skip_download": True,
        "no_warnings": True,
    }
    try:
        with yt_dlp.YoutubeDL(opts) as ydl:  # type: ignore[attr-defined]
            info = ydl.extract_info(url, download=False)
    except Exception:
        return (f"YouTube Video {video_id}", "Unknown", None)
    if not isinstance(info, dict):
        return (f"YouTube Video {video_id}", "Unknown", None)
    title = str(info.get("title") or f"YouTube Video {video_id}").strip()
    channel_name = str(info.get("uploader") or info.get("channel") or "Unknown").strip()
    duration_raw = info.get("duration")
    duration_sec: int | None = None
    try:
        if duration_raw is not None:
            duration_sec = int(duration_raw)
    except Exception:
        duration_sec = None
    return (title, channel_name, duration_sec)


def _captions_transcript(video_id: str) -> list[TranscriptSegment]:
    if YouTubeTranscriptApi is None:
        raise YouTubeTranscriptError(f"youtube-transcript-api not installed; install with `{_venv_install_hint()}`")
    try:
        # Support both v1 instance-style API and older class/static method API.
        if hasattr(YouTubeTranscriptApi, "fetch"):
            data = None
            try:
                data = YouTubeTranscriptApi.fetch(video_id, languages=["en", "en-US", "en-GB"])  # type: ignore[attr-defined]
            except TypeError:
                client = YouTubeTranscriptApi()  # type: ignore[call-arg]
                data = client.fetch(video_id, languages=["en", "en-US", "en-GB"])  # type: ignore[attr-defined]
        else:
            data = YouTubeTranscriptApi.get_transcript(video_id, languages=["en", "en-US", "en-GB"])  # type: ignore[attr-defined]
    except Exception as exc:
        raise YouTubeTranscriptError(f"captions_unavailable:{type(exc).__name__}") from exc

    rows = list(data) if isinstance(data, list) else list(data or [])
    out: list[TranscriptSegment] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        text = str(row.get("text") or "").strip()
        if not text:
            continue
        start = float(row.get("start") or 0.0)
        duration = float(row.get("duration") or 0.0)
        out.append(
            TranscriptSegment(
                start_sec=max(0.0, start),
                end_sec=max(start, start + max(0.0, duration)),
                text=text,
                source_type="captions",
            )
        )
    if not out:
        raise YouTubeTranscriptError("captions_empty")
    return out


def _download_audio(url: str, *, tmp_dir: Path) -> Path:
    if yt_dlp is None:
        raise YouTubeTranscriptError(f"yt_dlp_not_installed; install with `{_venv_install_hint()}`")
    outtmpl = str(tmp_dir / "audio.%(ext)s")
    opts = {
        "format": "bestaudio/best",
        "outtmpl": outtmpl,
        "quiet": True,
        "no_warnings": True,
        "noplaylist": True,
    }
    try:
        with yt_dlp.YoutubeDL(opts) as ydl:  # type: ignore[attr-defined]
            ydl.download([url])
    except Exception as exc:
        raise YouTubeTranscriptError(f"audio_download_failed:{type(exc).__name__}") from exc

    candidates = sorted(tmp_dir.glob("audio.*"))
    for path in candidates:
        if path.is_file() and path.stat().st_size > 0:
            return path
    raise YouTubeTranscriptError("audio_file_missing_after_download")


def _is_response_format_incompatible_error(exc: Exception) -> bool:
    text = str(exc or "").lower()
    return "response_format" in text and "compatible" in text


def _asr_transcript(url: str) -> list[TranscriptSegment]:
    if OpenAI is None:
        raise YouTubeTranscriptError("openai_client_unavailable_for_asr")
    api_key = os.environ.get("OPENAI_API_KEY", "").strip()
    if not api_key:
        raise YouTubeTranscriptError("openai_api_key_missing_for_asr")
    model = (os.environ.get("COATUE_CLAW_HFA_PODCAST_ASR_MODEL", "gpt-4o-mini-transcribe") or "gpt-4o-mini-transcribe").strip()
    client = OpenAI(api_key=api_key)
    with tempfile.TemporaryDirectory(prefix="hfa-podcast-") as tmp:
        audio_path = _download_audio(url, tmp_dir=Path(tmp))
        try:
            with audio_path.open("rb") as handle:
                response = client.audio.transcriptions.create(
                    model=model,
                    file=handle,
                    response_format="verbose_json",
                )
        except Exception as exc:
            if _is_response_format_incompatible_error(exc):
                try:
                    with audio_path.open("rb") as handle:
                        response = client.audio.transcriptions.create(
                            model=model,
                            file=handle,
                        )
                except Exception as retry_exc:
                    raise YouTubeTranscriptError(f"asr_transcription_failed:{type(retry_exc).__name__}") from retry_exc
            else:
                raise YouTubeTranscriptError(f"asr_transcription_failed:{type(exc).__name__}") from exc

    segments_raw = getattr(response, "segments", None)
    if segments_raw is None and isinstance(response, dict):
        segments_raw = response.get("segments")
    out: list[TranscriptSegment] = []
    if isinstance(segments_raw, list):
        for row in segments_raw:
            if isinstance(row, dict):
                text = str(row.get("text") or "").strip()
                if not text:
                    continue
                start = float(row.get("start") or 0.0)
                end = float(row.get("end") or start)
                out.append(TranscriptSegment(start_sec=max(0.0, start), end_sec=max(start, end), text=text, source_type="asr"))
            else:
                text = str(getattr(row, "text", "") or "").strip()
                if not text:
                    continue
                start = float(getattr(row, "start", 0.0) or 0.0)
                end = float(getattr(row, "end", start) or start)
                out.append(TranscriptSegment(start_sec=max(0.0, start), end_sec=max(start, end), text=text, source_type="asr"))
    if out:
        return out

    text = ""
    if isinstance(response, dict):
        text = str(response.get("text") or "").strip()
    else:
        text = str(getattr(response, "text", "") or "").strip()
    if not text:
        raise YouTubeTranscriptError("asr_no_text")
    return [TranscriptSegment(start_sec=0.0, end_sec=0.0, text=text, source_type="asr")]


def fetch_youtube_transcript(url: str) -> PodcastTranscript:
    raw = (url or "").strip()
    video_id = parse_youtube_video_id(raw)
    if not video_id:
        raise YouTubeTranscriptError("invalid_youtube_url")
    title, channel_name, duration_sec = _fetch_video_metadata(raw, video_id)

    errors: list[str] = []
    try:
        segments = _captions_transcript(video_id)
        source = "captions"
    except Exception as exc:
        errors.append(str(exc))
        try:
            segments = _asr_transcript(raw)
            source = "asr"
        except Exception as asr_exc:
            errors.append(str(asr_exc))
            raise YouTubeTranscriptError("transcript_unavailable:" + " | ".join(errors)) from asr_exc

    return PodcastTranscript(
        url=raw,
        video_id=video_id,
        title=title,
        channel_name=channel_name,
        duration_sec=duration_sec,
        transcript_source=source,
        segments=tuple(segments),
    )
