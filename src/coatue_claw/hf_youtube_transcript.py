from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path
import re
import shutil
import subprocess
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
    extraction_warnings: tuple[str, ...] = ()

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


def _transcribe_audio_file(*, client: Any, model: str, audio_path: Path, source_type: str) -> list[TranscriptSegment]:
    try:
        with audio_path.open("rb") as handle:
            response = client.audio.transcriptions.create(
                model=model,
                file=handle,
                response_format="verbose_json",
            )
    except Exception as exc:
        if _is_response_format_incompatible_error(exc):
            with audio_path.open("rb") as handle:
                response = client.audio.transcriptions.create(
                    model=model,
                    file=handle,
                )
        else:
            raise

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
                out.append(TranscriptSegment(start_sec=max(0.0, start), end_sec=max(start, end), text=text, source_type=source_type))
            else:
                text = str(getattr(row, "text", "") or "").strip()
                if not text:
                    continue
                start = float(getattr(row, "start", 0.0) or 0.0)
                end = float(getattr(row, "end", start) or start)
                out.append(TranscriptSegment(start_sec=max(0.0, start), end_sec=max(start, end), text=text, source_type=source_type))
    if out:
        return out

    text = ""
    if isinstance(response, dict):
        text = str(response.get("text") or "").strip()
    else:
        text = str(getattr(response, "text", "") or "").strip()
    if not text:
        raise YouTubeTranscriptError("asr_no_text")
    return [TranscriptSegment(start_sec=0.0, end_sec=0.0, text=text, source_type=source_type)]


def _ffmpeg_available() -> bool:
    return shutil.which("ffmpeg") is not None


def _transcode_to_wav(*, src: Path, dst: Path) -> None:
    cmd = ["ffmpeg", "-y", "-i", str(src), "-ac", "1", "-ar", "16000", str(dst)]
    proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if proc.returncode != 0:
        raise YouTubeTranscriptError("ffmpeg_transcode_failed")


def _chunk_wav(*, src: Path, out_dir: Path, chunk_seconds: int) -> list[Path]:
    pattern = out_dir / "chunk-%03d.wav"
    cmd = [
        "ffmpeg",
        "-y",
        "-i",
        str(src),
        "-f",
        "segment",
        "-segment_time",
        str(chunk_seconds),
        str(pattern),
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if proc.returncode != 0:
        raise YouTubeTranscriptError("ffmpeg_chunk_failed")
    return sorted(path for path in out_dir.glob("chunk-*.wav") if path.is_file() and path.stat().st_size > 0)


def _asr_transcript(url: str) -> tuple[list[TranscriptSegment], str, list[str]]:
    if OpenAI is None:
        raise YouTubeTranscriptError("openai_client_unavailable_for_asr")
    api_key = os.environ.get("OPENAI_API_KEY", "").strip()
    if not api_key:
        raise YouTubeTranscriptError("openai_api_key_missing_for_asr")
    model = (os.environ.get("COATUE_CLAW_HFA_PODCAST_ASR_MODEL", "gpt-4o-mini-transcribe") or "gpt-4o-mini-transcribe").strip()
    client = OpenAI(api_key=api_key)
    warnings: list[str] = []

    with tempfile.TemporaryDirectory(prefix="hfa-podcast-") as tmp:
        tmp_dir = Path(tmp)
        audio_path = _download_audio(url, tmp_dir=tmp_dir)

        try:
            return _transcribe_audio_file(client=client, model=model, audio_path=audio_path, source_type="asr"), "asr", warnings
        except Exception as primary_exc:
            warnings.append(f"asr_primary_failed:{type(primary_exc).__name__}")

        if _ffmpeg_available():
            wav_path = tmp_dir / "audio-16k.wav"
            try:
                _transcode_to_wav(src=audio_path, dst=wav_path)
                segments = _transcribe_audio_file(client=client, model=model, audio_path=wav_path, source_type="fallback_transcode_asr")
                warnings.append("transcript_fallback_used:fallback_transcode_asr")
                return segments, "fallback_transcode_asr", warnings
            except Exception as transcode_exc:
                warnings.append(f"fallback_transcode_failed:{type(transcode_exc).__name__}")

                try:
                    chunk_seconds = 600
                    chunks_dir = tmp_dir / "chunks"
                    chunks_dir.mkdir(parents=True, exist_ok=True)
                    chunk_paths = _chunk_wav(src=wav_path, out_dir=chunks_dir, chunk_seconds=chunk_seconds)
                    if not chunk_paths:
                        raise YouTubeTranscriptError("chunked_asr_no_chunks")
                    stitched: list[TranscriptSegment] = []
                    offset = 0.0
                    for chunk in chunk_paths:
                        chunk_segments = _transcribe_audio_file(
                            client=client,
                            model=model,
                            audio_path=chunk,
                            source_type="fallback_chunked_asr",
                        )
                        for seg in chunk_segments:
                            stitched.append(
                                TranscriptSegment(
                                    start_sec=seg.start_sec + offset,
                                    end_sec=seg.end_sec + offset,
                                    text=seg.text,
                                    source_type="fallback_chunked_asr",
                                )
                            )
                        offset += float(chunk_seconds)
                    warnings.append("transcript_fallback_used:fallback_chunked_asr")
                    return stitched, "fallback_chunked_asr", warnings
                except Exception as chunk_exc:
                    warnings.append(f"fallback_chunked_failed:{type(chunk_exc).__name__}")

        raise YouTubeTranscriptError("asr_transcription_failed_all_fallbacks")


def fetch_youtube_transcript(url: str) -> PodcastTranscript:
    raw = (url or "").strip()
    video_id = parse_youtube_video_id(raw)
    if not video_id:
        raise YouTubeTranscriptError("invalid_youtube_url")
    title, channel_name, duration_sec = _fetch_video_metadata(raw, video_id)

    errors: list[str] = []
    extraction_warnings: list[str] = []
    try:
        segments = _captions_transcript(video_id)
        source = "captions"
    except Exception as exc:
        errors.append(str(exc))
        try:
            segments, source, asr_warnings = _asr_transcript(raw)
            extraction_warnings.extend(asr_warnings)
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
        extraction_warnings=tuple(extraction_warnings),
    )
