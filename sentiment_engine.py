"""Market sentiment engine — StockTwits + Reddit + FinBERT.

Install dependencies once:
    pip install transformers torch praw

Reddit credentials: fill REDDIT_CLIENT_ID / REDDIT_CLIENT_SECRET in config.py
  → create a free app at https://www.reddit.com/prefs/apps (script type)
"""
import re
import time
import threading
from datetime import datetime, timezone, timedelta

import requests

from config import (
    _NYSE_TZ,
    SENTIMENT_STOCKTWITS_WEIGHT,
    SENTIMENT_REDDIT_WEIGHT,
    SENTIMENT_VOLUME_WEIGHT,
    SENTIMENT_MOMENTUM_WEIGHT,
    SENTIMENT_LOOKBACK_DAYS,
    SENTIMENT_SUBREDDITS,
    SENTIMENT_MIN_POST_LEN,
    SENTIMENT_REFRESH_HOUR,
    REDDIT_CLIENT_ID,
    REDDIT_CLIENT_SECRET,
)

# ── FinBERT (lazy-loaded on first use, thread-safe) ───────────────────────────

_finbert_pipe = None
_finbert_lock = threading.Lock()


def _get_finbert():
    global _finbert_pipe
    if _finbert_pipe is None:
        with _finbert_lock:
            if _finbert_pipe is None:
                try:
                    from transformers import pipeline
                    print("[sentiment] Loading FinBERT (first run — downloads ~500 MB once)…")
                    _finbert_pipe = pipeline(
                        "text-classification",
                        model="ProsusAI/finbert",
                        device=-1,       # CPU
                        truncation=True,
                        max_length=512,
                    )
                    print("[sentiment] FinBERT ready.")
                except ImportError:
                    print("[sentiment] WARNING: transformers/torch not installed. "
                          "Run: pip install transformers torch")
                    _finbert_pipe = None
    return _finbert_pipe


# ── Shared session (proxy-compatible, mirrors yfinance session) ───────────────

def _session() -> requests.Session:
    s = requests.Session()
    s.verify = False
    s.headers.update({"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"})
    return s


# ── StockTwits ────────────────────────────────────────────────────────────────

_ST_URL = "https://api.stocktwits.com/api/2/streams/symbol/{ticker}.json"


def _fetch_stocktwits(ticker: str) -> list[dict]:
    """Fetch StockTwits posts. Falls back to Yahoo Finance RSS if blocked."""
    try:
        r = _session().get(_ST_URL.format(ticker=ticker), params={"limit": 30}, timeout=10)
        if r.status_code != 200 or not r.text.strip():
            print(f"[sentiment] StockTwits {ticker}: HTTP {r.status_code}, "
                  f"body_len={len(r.text)} — falling back to Yahoo RSS")
            return _fetch_yahoo_rss(ticker)
        payload = r.json()
        if payload.get("response", {}).get("status") not in (None, 200):
            print(f"[sentiment] StockTwits {ticker}: API error {payload.get('response')} — Yahoo RSS fallback")
            return _fetch_yahoo_rss(ticker)
        messages = payload.get("messages", [])
        print(f"[sentiment] StockTwits {ticker}: {len(messages)} raw messages")
    except Exception as e:
        print(f"[sentiment] StockTwits {ticker}: {type(e).__name__}: {e} — Yahoo RSS fallback")
        return _fetch_yahoo_rss(ticker)

    cutoff = datetime.now(timezone.utc) - timedelta(days=SENTIMENT_LOOKBACK_DAYS)
    seen, posts = set(), []
    for m in messages:
        try:
            created = datetime.fromisoformat(m["created_at"].replace("Z", "+00:00"))
        except Exception:
            continue
        if created < cutoff:
            continue
        mid = m.get("id")
        if mid in seen:
            continue
        seen.add(mid)
        body = (m.get("body") or "").strip()
        if len(body) < SENTIMENT_MIN_POST_LEN:
            continue
        if re.fullmatch(r'[\$#@\w\s]+', body) and len(body.split()) <= 3:
            continue
        posts.append({"id": str(mid), "text": body, "source": "stocktwits"})
    return posts


# ── Yahoo Finance RSS (proxy-safe fallback for social sentiment) ───────────────

_YF_RSS_URL = "https://finance.yahoo.com/rss/headline?s={ticker}"


def _fetch_yahoo_rss(ticker: str) -> list[dict]:
    """Parse Yahoo Finance RSS headlines as a sentiment text source."""
    import xml.etree.ElementTree as ET
    try:
        r = _session().get(_YF_RSS_URL.format(ticker=ticker), timeout=10)
        if r.status_code != 200 or not r.text.strip():
            print(f"[sentiment] Yahoo RSS {ticker}: HTTP {r.status_code}, body empty")
            return []
        root = ET.fromstring(r.text)
        cutoff = datetime.now(timezone.utc) - timedelta(days=SENTIMENT_LOOKBACK_DAYS)
        posts = []
        for item in root.iter("item"):
            title = (item.findtext("title") or "").strip()
            desc  = (item.findtext("description") or "").strip()
            pub   = item.findtext("pubDate") or ""
            try:
                from email.utils import parsedate_to_datetime
                ts = parsedate_to_datetime(pub).astimezone(timezone.utc)
                if ts < cutoff:
                    continue
            except Exception:
                pass
            text = (title + ". " + desc).strip()
            if len(text) < SENTIMENT_MIN_POST_LEN:
                continue
            uid = hashlib.md5(text.encode()).hexdigest()
            posts.append({"id": uid, "text": text[:512], "source": "yahoo_rss"})
        print(f"[sentiment] Yahoo RSS {ticker}: {len(posts)} headlines")
        return posts
    except Exception as e:
        print(f"[sentiment] Yahoo RSS {ticker}: {type(e).__name__}: {e}")
        return []


# ── Reddit ────────────────────────────────────────────────────────────────────

_reddit_client = None


def _get_reddit():
    global _reddit_client
    if _reddit_client is None:
        if not REDDIT_CLIENT_ID or not REDDIT_CLIENT_SECRET:
            return None
        try:
            import praw
            _reddit_client = praw.Reddit(
                client_id=REDDIT_CLIENT_ID,
                client_secret=REDDIT_CLIENT_SECRET,
                user_agent="SP500Sentiment/1.0 (by kishor77@gmail.com)",
            )
        except Exception as e:
            print(f"[sentiment] Reddit init error: {e}")
    return _reddit_client


def _fetch_reddit(ticker: str) -> list[dict]:
    """Return recent Reddit posts mentioning ticker from finance subreddits."""
    reddit = _get_reddit()
    if reddit is None:
        return []

    cutoff = time.time() - (SENTIMENT_LOOKBACK_DAYS * 86400)
    seen, posts = set(), []

    for sub_name in SENTIMENT_SUBREDDITS:
        try:
            sub = reddit.subreddit(sub_name)
            query = f'"{ticker}" OR "${ticker}"'
            for post in sub.search(query, limit=20, sort="new", time_filter="week"):
                if post.created_utc < cutoff:
                    continue
                if post.id in seen:
                    continue
                seen.add(post.id)
                text = (post.title + " " + (post.selftext or ""))[:512].strip()
                if len(text) < SENTIMENT_MIN_POST_LEN:
                    continue
                posts.append({"id": post.id, "text": text, "source": "reddit"})
        except Exception as e:
            print(f"[sentiment] Reddit r/{sub_name} {ticker}: {e}")

    return posts


# ── FinBERT scoring ───────────────────────────────────────────────────────────

_POLARITY = {"positive": 1.0, "negative": -1.0, "neutral": 0.0}


def _score_texts(texts: list[str]) -> float:
    """Run FinBERT on texts; return aggregate sentiment in [-1, +1].
    Falls back to 0.0 (neutral) if FinBERT unavailable."""
    if not texts:
        return 0.0
    pipe = _get_finbert()
    if pipe is None:
        return 0.0
    try:
        results = pipe(texts, batch_size=16, truncation=True)
        scores = [
            _POLARITY.get(r["label"].lower(), 0.0) * r["score"]
            for r in results
        ]
        return sum(scores) / len(scores)
    except Exception as e:
        print(f"[sentiment] FinBERT scoring error: {e}")
        return 0.0


def _to_100(raw: float) -> float:
    """Map [-1, +1] → [0, 100]."""
    return round((raw + 1.0) / 2.0 * 100.0, 1)


# ── Sub-signal helpers ────────────────────────────────────────────────────────

_VOLUME_BASELINE = 10.0   # expected posts per ticker per lookback window


def _volume_spike_score(count: int) -> float:
    """No data → 50 (neutral). Baseline → 60. 3× baseline → 100. Zero real posts → 50."""
    if count == 0:
        return 50.0   # no posts means no signal, not bearish
    ratio = count / _VOLUME_BASELINE
    return round(min(100.0, 40.0 + ratio / 3.0 * 60.0), 1)


def _momentum_score(current: float, previous: float | None) -> float:
    """Score improvement vs yesterday: flat → 50, +100pt swing → 100, −100pt → 0."""
    if previous is None:
        return 50.0
    delta = current - previous   # range: [-100, +100]
    return round(max(0.0, min(100.0, 50.0 + delta / 2.0)), 1)


def _classify(score: float) -> str:
    if score >= 80: return "Strong Bullish"
    if score >= 60: return "Bullish"
    if score >= 40: return "Neutral"
    if score >= 20: return "Bearish"
    return "Strong Bearish"


# ── DB helpers ────────────────────────────────────────────────────────────────

def _prev_score(ticker: str, engine) -> float | None:
    from sqlalchemy import text as _t
    try:
        with engine.connect() as conn:
            row = conn.execute(_t("""
                SELECT score FROM sentiment_scores
                WHERE ticker = :tk
                  AND score_date = CURDATE() - INTERVAL 1 DAY
                ORDER BY computed_at DESC LIMIT 1
            """), {"tk": ticker}).fetchone()
        return float(row.score) if row else None
    except Exception:
        return None


def _save(ticker: str, data: dict, engine):
    from sqlalchemy import text as _t
    try:
        with engine.connect() as conn:
            conn.execute(_t("""
                INSERT INTO sentiment_scores
                    (ticker, score_date, score,
                     stocktwits_score, reddit_score, volume_spike, momentum,
                     post_count, classification, computed_at)
                VALUES
                    (:tk, CURDATE(), :score,
                     :st, :rd, :vs, :mom,
                     :pc, :cls, NOW())
                ON DUPLICATE KEY UPDATE
                    score=VALUES(score),
                    stocktwits_score=VALUES(stocktwits_score),
                    reddit_score=VALUES(reddit_score),
                    volume_spike=VALUES(volume_spike),
                    momentum=VALUES(momentum),
                    post_count=VALUES(post_count),
                    classification=VALUES(classification),
                    computed_at=NOW()
            """), {
                "tk": ticker,        "score": data["score"],
                "st": data["st"],    "rd":    data["rd"],
                "vs": data["vol"],   "mom":   data["mom"],
                "pc": data["posts"], "cls":   data["classification"],
            })
            conn.commit()
    except Exception as e:
        print(f"[sentiment] DB save {ticker}: {e}")


def load_all_scores(engine) -> dict[str, dict]:
    """Load today's sentiment scores from DB → {ticker: {...}}."""
    from sqlalchemy import text as _t
    try:
        with engine.connect() as conn:
            rows = conn.execute(_t("""
                SELECT ticker, score, stocktwits_score, reddit_score,
                       volume_spike, momentum, post_count, classification,
                       computed_at
                FROM sentiment_scores
                WHERE score_date = CURDATE()
                ORDER BY computed_at DESC
            """)).fetchall()
        result: dict[str, dict] = {}
        for r in rows:
            if r.ticker not in result:
                result[r.ticker] = {
                    "score":            float(r.score),
                    "stocktwits_score": r.stocktwits_score,
                    "reddit_score":     r.reddit_score,
                    "volume_spike":     r.volume_spike,
                    "momentum":         r.momentum,
                    "post_count":       r.post_count,
                    "classification":   r.classification,
                    "computed_at":      str(r.computed_at),
                }
        return result
    except Exception as e:
        print(f"[sentiment] load_all_scores: {e}")
        return {}


# ── Per-ticker computation ────────────────────────────────────────────────────

def compute_ticker_sentiment(ticker: str, engine) -> dict:
    st_posts = _fetch_stocktwits(ticker)
    rd_posts  = _fetch_reddit(ticker)

    st_raw = _score_texts([p["text"] for p in st_posts])
    rd_raw = _score_texts([p["text"] for p in rd_posts])

    st_score  = _to_100(st_raw)  if st_posts else None
    rd_score  = _to_100(rd_raw)  if rd_posts else None
    vol_score = _volume_spike_score(len(st_posts) + len(rd_posts))

    # Combined raw score (only from sources that have data)
    sources = [(st_score, SENTIMENT_STOCKTWITS_WEIGHT)] if st_score is not None else []
    sources += [(rd_score, SENTIMENT_REDDIT_WEIGHT)]     if rd_score is not None else []
    if sources:
        total_w    = sum(w for _, w in sources)
        combined   = sum(s * w for s, w in sources) / total_w
    else:
        combined   = 50.0

    prev      = _prev_score(ticker, engine)
    mom_score = _momentum_score(combined, prev)

    # If no posts at all from any source, skip scoring entirely — return neutral
    total_posts = len(st_posts) + len(rd_posts)
    if total_posts == 0:
        final = 50.0
    else:
        # Composite weighted by configured sub-weights; normalize for missing sources
        parts = {
            "vol": (vol_score, SENTIMENT_VOLUME_WEIGHT),
            "mom": (mom_score, SENTIMENT_MOMENTUM_WEIGHT),
        }
        if st_score is not None:
            parts["st"] = (st_score, SENTIMENT_STOCKTWITS_WEIGHT)
        if rd_score is not None:
            parts["rd"] = (rd_score, SENTIMENT_REDDIT_WEIGHT)

        total_w = sum(w for _, w in parts.values())
        final   = sum(s * w for s, w in parts.values()) / total_w if total_w else 50.0
        final   = round(max(0.0, min(100.0, final)), 1)

    data = {
        "score":          final,
        "st":             st_score if st_score is not None else 50.0,
        "rd":             rd_score if rd_score is not None else 50.0,
        "vol":            vol_score,
        "mom":            mom_score,
        "posts":          len(st_posts) + len(rd_posts),
        "classification": _classify(final),
    }
    _save(ticker, data, engine)
    return data


# ── Scheduling ────────────────────────────────────────────────────────────────

_sentiment_running = False


def should_run_sentiment(existing_scores: dict, engine) -> bool:
    """True if a fresh sentiment pass is warranted right now."""
    if not existing_scores:
        return True                           # no data today at all → run
    now_et = datetime.now(_NYSE_TZ)
    if now_et.hour < SENTIMENT_REFRESH_HOUR:
        return False                          # before noon, skip second run
    # After noon: check whether we already ran post-noon today
    from sqlalchemy import text as _t
    try:
        with engine.connect() as conn:
            row = conn.execute(_t("""
                SELECT COUNT(*) AS cnt FROM sentiment_scores
                WHERE score_date = CURDATE()
                  AND HOUR(computed_at) >= :hr
            """), {"hr": SENTIMENT_REFRESH_HOUR}).fetchone()
        return row.cnt == 0                   # True = no post-noon run yet
    except Exception:
        return False


def run_sentiment_pass(tickers: list[str], engine, on_complete=None):
    """Spawn a background thread to score the given tickers."""
    global _sentiment_running
    if _sentiment_running:
        return

    def _worker():
        global _sentiment_running
        _sentiment_running = True
        print(f"[sentiment] Pass started — {len(tickers)} tickers")
        for ticker in tickers:
            try:
                r = compute_ticker_sentiment(ticker, engine)
                print(f"[sentiment] {ticker}: {r['score']:.1f} "
                      f"({r['classification']}) {r['posts']} posts")
            except Exception as e:
                print(f"[sentiment] {ticker} error: {e}")
        _sentiment_running = False
        print("[sentiment] Pass complete.")
        if on_complete:
            try:
                on_complete()
            except Exception:
                pass

    threading.Thread(target=_worker, daemon=True, name="sentiment-pass").start()


def sentiment_status(engine) -> dict:
    """Return a status dict for the /api/sentiment/status endpoint."""
    from sqlalchemy import text as _t
    try:
        with engine.connect() as conn:
            rows = conn.execute(_t("""
                SELECT ticker, score, classification, computed_at
                FROM sentiment_scores
                WHERE score_date = CURDATE()
                ORDER BY computed_at DESC
            """)).fetchall()
        tickers = [r.ticker for r in rows]
        last_ts = str(rows[0].computed_at) if rows else None
        return {
            "running":        _sentiment_running,
            "scored_today":   len(tickers),
            "last_run":       last_ts,
            "covered_tickers": tickers,
        }
    except Exception as e:
        return {"running": _sentiment_running, "scored_today": 0, "error": str(e)}
