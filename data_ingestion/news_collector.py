"""
data_ingestion/news_collector.py

Real-time news collector. Three sources in priority order:
  1. Twitter/X Filtered Stream API  (lowest latency, streaming)
  2. NewsAPI / GNews                (polling every 30s)
  3. RSS feeds                      (polling every 60s)

All events are normalized to NewsEvent and published to stream:news.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any

import aiohttp
import feedparser  # type: ignore[import]
import structlog

from data_ingestion.base import BaseCollector

logger = structlog.get_logger(__name__)


@dataclass
class NewsEvent:
    id: str                         # SHA256(source + headline + timestamp)
    source: str                     # e.g. "twitter", "newsapi", "rss:bbc"
    timestamp: datetime             # when the event occurred (UTC)
    received_at: datetime           # when we received it (UTC)
    latency_ms: int                 # received_at - timestamp in ms
    headline: str
    body: str
    url: str
    category: str                   # politics/sports/crypto/economy/other
    entities: list[str]             # extracted named entities
    raw_data: dict[str, Any]        # original source payload

    def to_stream_dict(self) -> dict[str, str]:
        """Serialize for Redis Streams (all values must be strings)."""
        d = asdict(self)
        d["timestamp"] = self.timestamp.isoformat()
        d["received_at"] = self.received_at.isoformat()
        d["entities"] = json.dumps(self.entities)
        d["raw_data"] = json.dumps(self.raw_data)
        return {k: str(v) for k, v in d.items()}

    @staticmethod
    def make_id(source: str, headline: str, timestamp: str) -> str:
        raw = f"{source}:{headline}:{timestamp}"
        return hashlib.sha256(raw.encode()).hexdigest()[:16]


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _parse_dt(value: Any) -> datetime:
    """Parse various timestamp formats into UTC datetime."""
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc)
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value, tz=timezone.utc)
    if isinstance(value, str):
        try:
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
            return dt.astimezone(timezone.utc)
        except ValueError:
            pass
    return _now_utc()


def _guess_category(text: str) -> str:
    text_lower = text.lower()
    if any(w in text_lower for w in ["election", "president", "congress", "senate", "vote", "government", "minister"]):
        return "politics"
    if any(w in text_lower for w in ["bitcoin", "crypto", "ethereum", "blockchain", "defi", "nft"]):
        return "crypto"
    if any(w in text_lower for w in ["goal", "score", "match", "game", "nba", "nfl", "fifa", "champion", "league", "sport"]):
        return "sports"
    if any(w in text_lower for w in ["fed", "rate", "gdp", "inflation", "market", "stock", "recession", "economy"]):
        return "economy"
    return "other"


def _extract_entities(text: str) -> list[str]:
    """
    Lightweight rule-based entity extraction.
    In the signal processing layer, spaCy/LLM gives richer results.
    Here we just pull capitalized tokens as a fast pre-filter.
    """
    import re
    tokens = re.findall(r"\b[A-Z][a-zA-Z]{2,}\b", text)
    # deduplicate preserving order
    seen: set[str] = set()
    result: list[str] = []
    for t in tokens:
        if t not in seen:
            seen.add(t)
            result.append(t)
    return result[:20]


# ─── Twitter/X Filtered Stream ────────────────────────────────────────────────

class TwitterStreamCollector(BaseCollector):
    """
    Connects to the Twitter v2 Filtered Stream API.
    Applies rules based on watched accounts and market keywords.

    Docs: https://developer.twitter.com/en/docs/twitter-api/tweets/filtered-stream
    """

    STREAM_KEY = "stream:news"
    COLLECTOR_NAME = "twitter_stream"
    STREAM_URL = "https://api.twitter.com/2/tweets/search/stream"
    RULES_URL = "https://api.twitter.com/2/tweets/search/stream/rules"

    TWEET_FIELDS = "created_at,author_id,entities,referenced_tweets,text"
    EXPANSIONS = "author_id,referenced_tweets.id"
    USER_FIELDS = "username,name,verified"

    def __init__(self, redis_manager: Any, bearer_token: str, watchlist: list[str]) -> None:
        super().__init__(redis_manager)
        self._bearer_token = bearer_token
        self._watchlist = watchlist
        self._session: aiohttp.ClientSession | None = None

    def _auth_headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self._bearer_token}"}

    async def _ensure_rules(self, session: aiohttp.ClientSession) -> None:
        """Upsert stream filter rules for our watchlist."""
        # Delete existing rules first
        async with session.get(self.RULES_URL, headers=self._auth_headers()) as resp:
            body = await resp.json()
            existing = body.get("data", [])

        if existing:
            ids = [r["id"] for r in existing]
            await session.post(
                self.RULES_URL,
                headers=self._auth_headers(),
                json={"delete": {"ids": ids}},
            )

        # Build new rules: one per watched account + breaking news keywords
        rules = [{"value": f"from:{handle}", "tag": f"watch:{handle}"}
                 for handle in self._watchlist]
        rules.append({
            "value": "(breaking OR urgent OR alert) -is:retweet lang:en",
            "tag": "breaking_news",
        })

        async with session.post(
            self.RULES_URL,
            headers=self._auth_headers(),
            json={"add": rules},
        ) as resp:
            result = await resp.json()
            self._log.info("twitter_rules_set", count=len(rules), result=result.get("meta"))

    async def _run_loop(self) -> None:
        self._session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=None, sock_read=90),
        )
        try:
            await self._ensure_rules(self._session)

            params = {
                "tweet.fields": self.TWEET_FIELDS,
                "expansions": self.EXPANSIONS,
                "user.fields": self.USER_FIELDS,
            }

            async with self._session.get(
                self.STREAM_URL,
                headers=self._auth_headers(),
                params=params,
            ) as resp:
                if resp.status != 200:
                    body = await resp.text()
                    raise ConnectionError(f"Twitter stream HTTP {resp.status}: {body}")

                self._log.info("twitter_stream_connected")

                async for raw_line in resp.content:
                    if self._stop_event.is_set():
                        break

                    line = raw_line.strip()
                    if not line:
                        continue  # keep-alive newline

                    try:
                        payload = json.loads(line)
                    except json.JSONDecodeError:
                        continue

                    event = self._parse_tweet(payload)
                    if event:
                        await self._publish(event.to_stream_dict())

        finally:
            if self._session:
                await self._session.close()
                self._session = None

    def _parse_tweet(self, payload: dict) -> NewsEvent | None:
        try:
            tweet = payload.get("data", {})
            text = tweet.get("text", "")
            if not text:
                return None

            # Try to get author username from includes
            author_username = "unknown"
            includes = payload.get("includes", {})
            users = {u["id"]: u for u in includes.get("users", [])}
            author_id = tweet.get("author_id", "")
            if author_id in users:
                author_username = users[author_id].get("username", "unknown")

            created_str = tweet.get("created_at", "")
            event_time = _parse_dt(created_str)
            received_at = _now_utc()
            latency_ms = int((received_at - event_time).total_seconds() * 1000)

            return NewsEvent(
                id=NewsEvent.make_id("twitter", text, created_str),
                source=f"twitter:{author_username}",
                timestamp=event_time,
                received_at=received_at,
                latency_ms=max(0, latency_ms),
                headline=text[:280],
                body=text,
                url=f"https://twitter.com/i/web/status/{tweet.get('id', '')}",
                category=_guess_category(text),
                entities=_extract_entities(text),
                raw_data=payload,
            )
        except Exception as exc:
            self._log.warning("tweet_parse_error", error=str(exc))
            return None

    async def _on_stop(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()


# ─── NewsAPI Polling ──────────────────────────────────────────────────────────

class NewsAPICollector(BaseCollector):
    """
    Polls NewsAPI /v2/top-headlines every N seconds.
    Deduplicates by article URL.

    Docs: https://newsapi.org/docs/endpoints/top-headlines
    """

    STREAM_KEY = "stream:news"
    COLLECTOR_NAME = "newsapi"
    BASE_URL = "https://newsapi.org/v2/top-headlines"

    def __init__(
        self,
        redis_manager: Any,
        api_key: str,
        poll_interval_s: int = 30,
        categories: list[str] | None = None,
    ) -> None:
        super().__init__(redis_manager)
        self._api_key = api_key
        self._poll_interval_s = poll_interval_s
        self._categories = categories or ["general", "politics", "business", "technology", "sports"]
        self._seen_urls: set[str] = set()

    async def _run_loop(self) -> None:
        async with aiohttp.ClientSession() as session:
            while not self._stop_event.is_set():
                for category in self._categories:
                    if self._stop_event.is_set():
                        break
                    await self._poll_category(session, category)

                try:
                    await asyncio.wait_for(
                        self._stop_event.wait(),
                        timeout=self._poll_interval_s,
                    )
                except asyncio.TimeoutError:
                    pass

    async def _poll_category(self, session: aiohttp.ClientSession, category: str) -> None:
        params = {
            "apiKey": self._api_key,
            "language": "en",
            "pageSize": 20,
            "sortBy": "publishedAt",
        }
        # NewsAPI doesn't have a "politics" category, map it
        api_cat = "general" if category == "politics" else category
        params["category"] = api_cat

        try:
            async with session.get(self.BASE_URL, params=params, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status == 429:
                    self._log.warning("newsapi_rate_limited")
                    await asyncio.sleep(60)
                    return
                if resp.status != 200:
                    self._log.warning("newsapi_error", status=resp.status)
                    return

                data = await resp.json()
                articles = data.get("articles", [])

                new_count = 0
                for article in articles:
                    url = article.get("url", "")
                    if url in self._seen_urls:
                        continue
                    self._seen_urls.add(url)
                    # Prevent unbounded growth
                    if len(self._seen_urls) > 10_000:
                        self._seen_urls = set(list(self._seen_urls)[-5_000:])

                    event = self._parse_article(article, category)
                    if event:
                        await self._publish(event.to_stream_dict())
                        new_count += 1

                if new_count:
                    self._log.debug("newsapi_new_articles", category=category, count=new_count)

        except asyncio.TimeoutError:
            self._log.warning("newsapi_timeout", category=category)
        except Exception as exc:
            self._log.error("newsapi_poll_error", category=category, error=str(exc))

    def _parse_article(self, article: dict, category: str) -> NewsEvent | None:
        try:
            title = article.get("title") or ""
            description = article.get("description") or ""
            published_at = article.get("publishedAt") or ""
            source_name = article.get("source", {}).get("name", "newsapi")
            url = article.get("url") or ""

            if not title or title == "[Removed]":
                return None

            event_time = _parse_dt(published_at)
            received_at = _now_utc()
            combined_text = f"{title} {description}"

            return NewsEvent(
                id=NewsEvent.make_id(source_name, title, published_at),
                source=f"newsapi:{source_name}",
                timestamp=event_time,
                received_at=received_at,
                latency_ms=max(0, int((received_at - event_time).total_seconds() * 1000)),
                headline=title,
                body=description,
                url=url,
                category=_guess_category(combined_text) if category == "general" else category,
                entities=_extract_entities(combined_text),
                raw_data=article,
            )
        except Exception as exc:
            self._log.warning("article_parse_error", error=str(exc))
            return None


# ─── RSS Feed Polling ─────────────────────────────────────────────────────────

class RSSCollector(BaseCollector):
    """
    Polls a list of RSS feeds every N seconds using feedparser.
    Deduplicates by entry link/id.
    """

    STREAM_KEY = "stream:news"
    COLLECTOR_NAME = "rss"

    def __init__(
        self,
        redis_manager: Any,
        feed_urls: list[str],
        poll_interval_s: int = 60,
    ) -> None:
        super().__init__(redis_manager)
        self._feed_urls = feed_urls
        self._poll_interval_s = poll_interval_s
        self._seen_ids: set[str] = set()

    async def _run_loop(self) -> None:
        loop = asyncio.get_event_loop()
        while not self._stop_event.is_set():
            for url in self._feed_urls:
                if self._stop_event.is_set():
                    break
                await self._poll_feed(loop, url)

            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=self._poll_interval_s)
            except asyncio.TimeoutError:
                pass

    async def _poll_feed(self, loop: asyncio.AbstractEventLoop, url: str) -> None:
        try:
            # feedparser is synchronous; run in executor to avoid blocking
            feed = await loop.run_in_executor(None, feedparser.parse, url)
        except Exception as exc:
            self._log.warning("rss_fetch_error", url=url, error=str(exc))
            return

        source_name = f"rss:{feed.feed.get('title', url)}"
        new_count = 0

        for entry in feed.entries:
            entry_id = entry.get("id") or entry.get("link") or ""
            if not entry_id or entry_id in self._seen_ids:
                continue
            self._seen_ids.add(entry_id)
            if len(self._seen_ids) > 20_000:
                self._seen_ids = set(list(self._seen_ids)[-10_000:])

            event = self._parse_entry(entry, source_name)
            if event:
                await self._publish(event.to_stream_dict())
                new_count += 1

        if new_count:
            self._log.debug("rss_new_entries", source=source_name, count=new_count)

    def _parse_entry(self, entry: Any, source_name: str) -> NewsEvent | None:
        try:
            title = entry.get("title", "").strip()
            summary = entry.get("summary", "").strip()
            link = entry.get("link", "")
            published_parsed = entry.get("published_parsed")

            if not title:
                return None

            if published_parsed:
                import calendar
                ts = calendar.timegm(published_parsed)
                event_time = datetime.fromtimestamp(ts, tz=timezone.utc)
            else:
                event_time = _now_utc()

            received_at = _now_utc()
            combined = f"{title} {summary}"

            return NewsEvent(
                id=NewsEvent.make_id(source_name, title, event_time.isoformat()),
                source=source_name,
                timestamp=event_time,
                received_at=received_at,
                latency_ms=max(0, int((received_at - event_time).total_seconds() * 1000)),
                headline=title,
                body=summary[:1000],
                url=link,
                category=_guess_category(combined),
                entities=_extract_entities(combined),
                raw_data={"title": title, "summary": summary, "link": link},
            )
        except Exception as exc:
            self._log.warning("rss_entry_parse_error", error=str(exc))
            return None


# ─── Composite NewsCollector (façade) ────────────────────────────────────────

class NewsCollector(BaseCollector):
    """
    Façade that manages all three news sub-collectors as a unit.
    start()/stop() delegates to each child.
    """

    STREAM_KEY = "stream:news"
    COLLECTOR_NAME = "news"

    def __init__(
        self,
        redis_manager: Any,
        twitter_bearer_token: str,
        newsapi_key: str,
        rss_feeds: list[str],
        twitter_watchlist: list[str],
        newsapi_poll_interval_s: int = 30,
        rss_poll_interval_s: int = 60,
    ) -> None:
        super().__init__(redis_manager)
        self._children: list[BaseCollector] = []

        if twitter_bearer_token:
            self._children.append(
                TwitterStreamCollector(redis_manager, twitter_bearer_token, twitter_watchlist)
            )

        if newsapi_key:
            self._children.append(
                NewsAPICollector(redis_manager, newsapi_key, newsapi_poll_interval_s)
            )

        if rss_feeds:
            self._children.append(
                RSSCollector(redis_manager, rss_feeds, rss_poll_interval_s)
            )

        if not self._children:
            self._log.warning("news_collector_no_sources_configured")

    async def start(self) -> None:
        for child in self._children:
            await child.start()
        self._health.status.__class__  # keep type checker happy
        self._health.status = self._health.status.__class__.RUNNING

    async def stop(self) -> None:
        for child in self._children:
            await child.stop()

    async def _run_loop(self) -> None:
        # Not called directly; children manage their own loops
        pass

    @property
    def health(self):  # type: ignore[override]
        from data_ingestion.base import CollectorHealth, CollectorStatus
        h = CollectorHealth(name="news")
        h.status = CollectorStatus.RUNNING if any(c.health.is_healthy for c in self._children) else CollectorStatus.ERROR
        h.messages_total = sum(c.health.messages_total for c in self._children)
        h.errors_total = sum(c.health.errors_total for c in self._children)
        return h
