from __future__ import annotations

from dataclasses import dataclass
import re


@dataclass(frozen=True)
class PipelineIntent:
    kind: str
    request: str | None = None


def _strip_slack_mentions(text: str) -> str:
    return re.sub(r"<@[^>]+>", " ", text or "").strip()


def parse_pipeline_intent(text: str) -> PipelineIntent | None:
    stripped = _strip_slack_mentions(text)
    lower = stripped.lower()

    if re.search(r"\b(undo|rollback|roll back|revert)\s+(?:the\s+)?last\s+deploy\b", lower):
        return PipelineIntent(kind="undo_last_deploy")

    if re.search(r"\b(deploy\s+latest|deploy\s+now|ship\s+latest|pull\s+latest\s+and\s+restart)\b", lower):
        return PipelineIntent(kind="deploy_latest")

    if re.search(r"\b(restart\s+(?:the\s+)?bot|restart\s+openclaw)\b", lower):
        return PipelineIntent(kind="deploy_latest")

    if re.search(r"\b(run\s+checks|run\s+tests|test\s+suite)\b", lower):
        return PipelineIntent(kind="run_checks")

    if re.search(r"\b(show|what(?:'s| is)|check)\s+(?:the\s+)?(?:pipeline|deploy|release)\s+status\b", lower):
        return PipelineIntent(kind="status")

    if re.search(r"\b(show|list)\s+deploy\s+history\b", lower):
        return PipelineIntent(kind="history")

    explicit = re.search(r"^\s*(?:please\s+)?(?:build|implement|task)\s*[:\-]\s*(.+)$", stripped, re.IGNORECASE)
    if explicit:
        request = explicit.group(1).strip()
        if request:
            return PipelineIntent(kind="build_request", request=request)

    natural = re.search(r"^\s*(?:please\s+)?(?:build|implement)\s+(?!a\s+chart\b|chart\b)(.+)$", stripped, re.IGNORECASE)
    if natural:
        request = natural.group(1).strip()
        if request:
            return PipelineIntent(kind="build_request", request=request)

    if re.search(r"\b(pipeline|deploy|release|build\s+pipeline)\b", lower):
        return PipelineIntent(kind="help")

    return None
