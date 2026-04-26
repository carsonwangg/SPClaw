from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
import json
import os
from pathlib import Path
import re
from typing import Any, Callable
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


class XDigestError(RuntimeError):
    pass


@dataclass(frozen=True)
class XPost:
    tweet_id: str
    text: str
    created_at: str | None
    author_username: str | None
    author_name: str | None
    metrics: dict[str, int]

    @property
    def url(self) -> str:
        if self.author_username:
            return f"https://x.com/{self.author_username}/status/{self.tweet_id}"
        return f"https://x.com/i/web/status/{self.tweet_id}"

    @property
    def engagement(self) -> int:
        return int(self.metrics.get("like_count", 0)) + int(self.metrics.get("retweet_count", 0)) + int(
            self.metrics.get("reply_count", 0)
        ) + int(self.metrics.get("quote_count", 0))


@dataclass(frozen=True)
class XDigestResult:
    query: str
    hours: int
    generated_at_utc: str
    post_count: int
    top_post_url: str | None
    output_path: Path


def _data_root() -> Path:
    return Path(os.environ.get("SPCLAW_DATA_ROOT", "/opt/spclaw-data"))


def _digest_dir() -> Path:
    return Path(
        os.environ.get(
            "SPCLAW_X_DIGEST_DIR",
            str(_data_root() / "artifacts/x-digest"),
        )
    )


def _x_api_base() -> str:
    return (os.environ.get("SPCLAW_X_API_BASE", "https://api.x.com").strip() or "https://api.x.com").rstrip("/")


def _resolve_bearer_token() -> str:
    for key in ("SPCLAW_X_BEARER_TOKEN", "X_BEARER_TOKEN", "SPCLAW_TWITTER_BEARER_TOKEN"):
        value = os.environ.get(key, "").strip()
        if value:
            return value
    raise XDigestError("X bearer token missing. Set SPCLAW_X_BEARER_TOKEN in .env.prod.")


def _slugify(value: str) -> str:
    slug = re.sub(r"[^a-zA-Z0-9]+", "-", value.lower()).strip("-")
    return slug[:48] or "x-digest"


def _excerpt(text: str, limit: int = 180) -> str:
    cleaned = re.sub(r"\s+", " ", text or "").strip()
    if len(cleaned) <= limit:
        return cleaned
    return cleaned[: limit - 1].rstrip() + "…"


def _normalize_query(query: str) -> str:
    base = re.sub(r"\s+", " ", query).strip()
    if not base:
        raise XDigestError("Query cannot be empty.")
    lower = base.lower()
    additions: list[str] = []
    if "-is:retweet" not in lower:
        additions.append("-is:retweet")
    if "-is:reply" not in lower:
        additions.append("-is:reply")
    return f"{base} {' '.join(additions)}".strip()


def _default_fetch_json(
    *,
    url: str,
    headers: dict[str, str],
    params: dict[str, str],
) -> dict[str, Any]:
    full_url = f"{url}?{urlencode(params)}"
    request = Request(full_url, headers=headers, method="GET")
    try:
        with urlopen(request, timeout=30) as response:
            payload = response.read()
    except HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="ignore")
        raise XDigestError(f"X API request failed ({exc.code}): {detail[:500]}") from exc
    except URLError as exc:
        raise XDigestError(f"X API request failed: {exc.reason}") from exc
    try:
        parsed = json.loads(payload.decode("utf-8"))
    except json.JSONDecodeError as exc:
        raise XDigestError("Failed to parse X API response JSON.") from exc
    if isinstance(parsed, dict) and parsed.get("errors"):
        raise XDigestError(f"X API returned errors: {parsed['errors']}")
    return parsed if isinstance(parsed, dict) else {}


def _parse_posts(payload: dict[str, Any]) -> list[XPost]:
    users_by_id: dict[str, dict[str, Any]] = {}
    includes = payload.get("includes")
    if isinstance(includes, dict):
        users = includes.get("users")
        if isinstance(users, list):
            for user in users:
                if not isinstance(user, dict):
                    continue
                user_id = str(user.get("id") or "").strip()
                if user_id:
                    users_by_id[user_id] = user

    out: list[XPost] = []
    data = payload.get("data")
    if not isinstance(data, list):
        return out

    for row in data:
        if not isinstance(row, dict):
            continue
        tweet_id = str(row.get("id") or "").strip()
        text = str(row.get("text") or "").strip()
        if not tweet_id or not text:
            continue
        metrics_raw = row.get("public_metrics")
        metrics: dict[str, int] = {}
        if isinstance(metrics_raw, dict):
            for key in ("like_count", "retweet_count", "reply_count", "quote_count"):
                value = metrics_raw.get(key, 0)
                try:
                    metrics[key] = int(value)
                except (TypeError, ValueError):
                    metrics[key] = 0
        author_id = str(row.get("author_id") or "").strip()
        user = users_by_id.get(author_id, {})
        out.append(
            XPost(
                tweet_id=tweet_id,
                text=text,
                created_at=(str(row.get("created_at")) if row.get("created_at") else None),
                author_username=(str(user.get("username")) if user.get("username") else None),
                author_name=(str(user.get("name")) if user.get("name") else None),
                metrics=metrics,
            )
        )
    out.sort(key=lambda item: item.engagement, reverse=True)
    return out


def _top_terms(posts: list[XPost], *, limit: int = 8) -> list[tuple[str, int]]:
    stop = {
        "the",
        "and",
        "for",
        "with",
        "this",
        "that",
        "from",
        "into",
        "about",
        "https",
        "http",
        "your",
        "have",
        "will",
        "just",
        "what",
        "when",
        "where",
        "they",
        "their",
        "there",
        "is",
        "are",
        "was",
        "were",
        "you",
        "our",
        "its",
        "but",
        "not",
        "all",
        "can",
        "has",
        "had",
        "who",
        "new",
        "out",
        "now",
    }
    counts: dict[str, int] = {}
    for post in posts:
        for token in re.findall(r"[a-zA-Z][a-zA-Z0-9_-]{2,}", post.text.lower()):
            if token in stop:
                continue
            counts[token] = counts.get(token, 0) + 1
    ranked = sorted(counts.items(), key=lambda pair: (-pair[1], pair[0]))
    return ranked[:limit]


def _render_markdown(
    *,
    query: str,
    hours: int,
    generated_at_utc: str,
    posts: list[XPost],
) -> str:
    lines: list[str] = []
    lines.append(f"# X Digest: {query}")
    lines.append("")
    lines.append(f"Generated UTC: `{generated_at_utc}`")
    lines.append("Source: `X Recent Search API v2`")
    lines.append(f"Window: last `{hours}h`")
    lines.append(f"Posts analyzed: `{len(posts)}`")
    lines.append("")

    lines.append("## Key Takeaways")
    if not posts:
        lines.append("- No matching posts were returned in this window.")
        lines.append("")
        return "\n".join(lines)

    top = posts[0]
    lines.append(f"- Highest-engagement post: {top.url} (engagement `{top.engagement}`).")
    avg_engagement = int(sum(post.engagement for post in posts) / max(1, len(posts)))
    lines.append(f"- Average engagement across returned posts: `{avg_engagement}`.")
    top_terms = _top_terms(posts)
    if top_terms:
        terms = ", ".join(f"`{term}` ({count})" for term, count in top_terms[:6])
        lines.append(f"- Recurring terms in discussion: {terms}.")
    unique_authors = len({post.author_username or post.author_name or post.tweet_id for post in posts})
    lines.append(f"- Unique authors captured: `{unique_authors}`.")
    lines.append("")

    lines.append("## Top Posts")
    for idx, post in enumerate(posts[:12], start=1):
        author = post.author_username or post.author_name or "unknown"
        lines.append(
            (
                f"{idx}. [{author}]({post.url}) "
                f"(engagement `{post.engagement}`, likes `{post.metrics.get('like_count', 0)}`, "
                f"retweets `{post.metrics.get('retweet_count', 0)}`, replies `{post.metrics.get('reply_count', 0)}`)"
            )
        )
        lines.append(f"   - {_excerpt(post.text)}")
    lines.append("")
    lines.append("## Notes")
    lines.append("- Query includes `-is:retweet -is:reply` by default to reduce duplicate/noise.")
    lines.append("- This digest is ranking-based, not endorsement-based.")
    return "\n".join(lines)


def build_x_digest(
    query: str,
    *,
    hours: int = 24,
    max_results: int = 50,
    now_utc: datetime | None = None,
    fetch_json: Callable[..., dict[str, Any]] | None = None,
) -> XDigestResult:
    if hours < 1 or hours > 168:
        raise XDigestError("Hours must be between 1 and 168.")
    if max_results < 10 or max_results > 100:
        raise XDigestError("max_results must be between 10 and 100.")

    token = _resolve_bearer_token()
    now = now_utc or datetime.now(UTC)
    start_time = (now - timedelta(hours=hours)).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    generated_at_utc = now.replace(microsecond=0).isoformat().replace("+00:00", "Z")
    normalized_query = _normalize_query(query)

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    params = {
        "query": normalized_query,
        "max_results": str(max_results),
        "start_time": start_time,
        "tweet.fields": "author_id,created_at,public_metrics,lang",
        "expansions": "author_id",
        "user.fields": "name,username,verified",
    }
    fetch = fetch_json or _default_fetch_json
    payload = fetch(
        url=f"{_x_api_base()}/2/tweets/search/recent",
        headers=headers,
        params=params,
    )
    posts = _parse_posts(payload)

    digest_dir = _digest_dir()
    digest_dir.mkdir(parents=True, exist_ok=True)
    filename = f"{_slugify(query)}-{now.strftime('%Y%m%d-%H%M%S')}.md"
    output_path = digest_dir / filename
    markdown = _render_markdown(
        query=query.strip(),
        hours=hours,
        generated_at_utc=generated_at_utc,
        posts=posts,
    )
    output_path.write_text(markdown, encoding="utf-8")

    return XDigestResult(
        query=query.strip(),
        hours=hours,
        generated_at_utc=generated_at_utc,
        post_count=len(posts),
        top_post_url=(posts[0].url if posts else None),
        output_path=output_path,
    )


def format_x_digest_summary(result: XDigestResult) -> str:
    lines = [
        "X digest complete:",
        f"- Query: `{result.query}`",
        f"- Window: last `{result.hours}h`",
        f"- Posts analyzed: `{result.post_count}`",
    ]
    if result.top_post_url:
        lines.append(f"- Top post: {result.top_post_url}")
    lines.append(f"- Full report: `{result.output_path}`")
    return "\n".join(lines)
