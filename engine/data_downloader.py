"""
engine/data_downloader.py — CryptoForge Async Data Engineering Module
Downloads 5 years of 1m, 3m, 5m OHLCV candles from Delta Exchange / Binance.
Handles: 2000-candle pagination, rate limits, gap detection, optional TimescaleDB storage.

Usage:
    python -m engine.data_downloader --symbols BTCUSDT ETHUSDT --resolutions 1m 3m 5m --years 5
    python -m engine.data_downloader --symbols BTCUSDT --resolutions 5m --years 1 --to-db

Architecture:
    aiohttp.ClientSession → Semaphore(8) → Paginator → DataFrame / PostgreSQL
"""

import argparse
import asyncio
import os
import sys
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple

import aiohttp
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

# ── Constants ──────────────────────────────────────────────────────
MAX_CANDLES_PER_REQUEST = 2000  # Delta Exchange limit
BINANCE_MAX_PER_REQUEST = 1000  # Binance limit
MAX_CONCURRENT_REQUESTS = 8  # aiohttp concurrency cap
REQUEST_DELAY_SEC = 0.12  # ~8 req/sec safe rate for Delta
MAX_RETRIES = 5
BACKOFF_BASE = 2.0
BACKOFF_CAP = 60.0

RESOLUTION_SECONDS = {
    "1m": 60,
    "3m": 180,
    "5m": 300,
    "15m": 900,
    "30m": 1800,
    "1h": 3600,
    "2h": 7200,
    "4h": 14400,
    "6h": 21600,
    "1d": 86400,
    "1w": 604800,
}

# Symbols available on Delta Exchange (rest go to Binance fallback)
DELTA_SYMBOLS = {"BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "DOGEUSDT", "PAXGUSDT"}


class AsyncDataDownloader:
    """
    Enterprise-grade async candle downloader.
    - aiohttp for true async I/O (no thread pools)
    - Semaphore-based concurrency limiting
    - Exponential backoff on 429/5xx
    - Gap detection and fill
    - Optional TimescaleDB persistence
    """

    def __init__(self, use_db: bool = False, db_url: str = None):
        self.use_db = use_db
        self.db_url = db_url or os.getenv("DATABASE_URL", "")
        self._semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
        self._session: Optional[aiohttp.ClientSession] = None
        self._db_pool = None  # asyncpg connection pool
        self._request_count = 0
        self._error_count = 0

        # Determine Delta base URL
        testnet = os.getenv("DELTA_TESTNET", "false").lower() == "true"
        if testnet:
            self._delta_base = "https://testnet-api.delta.exchange/v2"
        else:
            region = os.getenv("DELTA_REGION", "india").lower()
            self._delta_base = (
                "https://api.india.delta.exchange/v2" if region == "india" else "https://api.delta.exchange/v2"
            )
        self._binance_base = "https://api.binance.com/api/v3"

    # ── Session Management ──────────────────────────────────────
    async def _ensure_session(self):
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=60, connect=15)
            connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT_REQUESTS + 2, enable_cleanup_closed=True)
            self._session = aiohttp.ClientSession(timeout=timeout, connector=connector)

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()
        if self._db_pool:
            await self._db_pool.close()

    # ── Database Setup ──────────────────────────────────────────
    async def _ensure_db(self):
        """Create asyncpg pool and ensure schema exists."""
        if not self.use_db or self._db_pool:
            return
        try:
            import asyncpg
        except ImportError:
            print("[DATA] asyncpg not installed — pip install asyncpg")
            print("[DATA] Falling back to in-memory mode")
            self.use_db = False
            return

        try:
            self._db_pool = await asyncpg.create_pool(self.db_url, min_size=2, max_size=10)
            async with self._db_pool.acquire() as conn:
                # Create table if not exists
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS candles (
                        time        TIMESTAMPTZ      NOT NULL,
                        symbol      TEXT             NOT NULL,
                        resolution  TEXT             NOT NULL,
                        open        DOUBLE PRECISION,
                        high        DOUBLE PRECISION,
                        low         DOUBLE PRECISION,
                        close       DOUBLE PRECISION,
                        volume      DOUBLE PRECISION,
                        UNIQUE (time, symbol, resolution)
                    );
                """)
                # Try to create hypertable (only works if TimescaleDB extension is present)
                try:
                    await conn.execute("SELECT create_hypertable('candles', 'time', if_not_exists => true);")
                    print("[DATA] TimescaleDB hypertable ready")
                except Exception:
                    print("[DATA] Standard PostgreSQL table (no TimescaleDB extension)")

                # Create index
                await conn.execute("""
                    CREATE INDEX IF NOT EXISTS idx_candles_sym_res
                    ON candles (symbol, resolution, time DESC);
                """)
            print("[DATA] Database schema ready")
        except Exception as e:
            print(f"[DATA] Database connection failed: {e}")
            print("[DATA] Falling back to in-memory mode")
            self.use_db = False

    # ── HTTP Request with Retry ─────────────────────────────────
    async def _fetch_with_retry(self, url: str, params: dict, source: str = "delta") -> Optional[dict]:
        """
        Fetch JSON with exponential backoff on failure.
        Returns parsed JSON or None on exhausted retries.
        """
        await self._ensure_session()

        for attempt in range(MAX_RETRIES):
            async with self._semaphore:
                try:
                    async with self._session.get(url, params=params) as resp:
                        self._request_count += 1

                        if resp.status == 200:
                            return await resp.json()

                        if resp.status == 429:
                            # Rate limited — respect Retry-After or backoff
                            retry_after = resp.headers.get("Retry-After")
                            wait = (
                                float(retry_after) if retry_after else min(BACKOFF_BASE ** (attempt + 1), BACKOFF_CAP)
                            )
                            print(f"[DATA] 429 Rate Limited ({source}) — waiting {wait:.1f}s")
                            await asyncio.sleep(wait)
                            continue

                        if resp.status >= 500:
                            wait = min(BACKOFF_BASE ** (attempt + 1), BACKOFF_CAP)
                            print(f"[DATA] {resp.status} Server Error ({source}) — retry in {wait:.1f}s")
                            await asyncio.sleep(wait)
                            continue

                        # Client error (400, 404, etc.) — don't retry
                        body = await resp.text()
                        print(f"[DATA] {resp.status} Error ({source}): {body[:200]}")
                        self._error_count += 1
                        return None

                except asyncio.TimeoutError:
                    wait = min(BACKOFF_BASE ** (attempt + 1), BACKOFF_CAP)
                    print(f"[DATA] Timeout ({source}) — retry in {wait:.1f}s")
                    await asyncio.sleep(wait)

                except aiohttp.ClientError as e:
                    wait = min(BACKOFF_BASE ** (attempt + 1), BACKOFF_CAP)
                    print(f"[DATA] Client error ({source}): {e} — retry in {wait:.1f}s")
                    await asyncio.sleep(wait)

            # Small delay between requests to avoid burst
            await asyncio.sleep(REQUEST_DELAY_SEC)

        self._error_count += 1
        return None

    # ── Delta Exchange Candle Fetcher ───────────────────────────
    async def _fetch_delta_chunk(self, symbol: str, resolution: str, start_ts: int, end_ts: int) -> List[dict]:
        """Fetch one chunk of candles from Delta Exchange."""
        url = f"{self._delta_base}/history/candles"
        params = {
            "symbol": symbol,
            "resolution": resolution,
            "start": start_ts,
            "end": end_ts,
        }
        data = await self._fetch_with_retry(url, params, source="delta")
        if data and "result" in data:
            return data["result"]
        return []

    # ── Binance Candle Fetcher ──────────────────────────────────
    async def _fetch_binance_chunk(self, symbol: str, interval: str, start_ms: int, end_ms: int) -> List[list]:
        """Fetch one chunk of candles from Binance public API."""
        url = f"{self._binance_base}/klines"
        params = {
            "symbol": symbol,
            "interval": interval,
            "startTime": start_ms,
            "endTime": end_ms,
            "limit": BINANCE_MAX_PER_REQUEST,
        }
        data = await self._fetch_with_retry(url, params, source="binance")
        return data if isinstance(data, list) else []

    # ── Paginated Download — Delta Exchange ─────────────────────
    async def download_delta(self, symbol: str, resolution: str, start_date: str, end_date: str) -> pd.DataFrame:
        """
        Download candles from Delta Exchange with 2000-candle pagination.

        Args:
            symbol: e.g. 'BTCUSDT'
            resolution: '1m', '3m', '5m', '15m', '1h', '1d'
            start_date: 'YYYY-MM-DD'
            end_date: 'YYYY-MM-DD'

        Returns:
            DataFrame with datetime index, OHLCV columns
        """
        start_ts = int(datetime.strptime(start_date, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp())
        end_ts = int(datetime.strptime(end_date, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp())

        secs_per_candle = RESOLUTION_SECONDS.get(resolution, 300)
        chunk_seconds = secs_per_candle * MAX_CANDLES_PER_REQUEST

        # Build chunk ranges
        chunks = []
        cursor = start_ts
        while cursor < end_ts:
            chunk_end = min(cursor + chunk_seconds, end_ts)
            chunks.append((cursor, chunk_end))
            cursor = chunk_end

        total_chunks = len(chunks)
        print(f"[DELTA] Downloading {symbol} {resolution}: {start_date} → {end_date} ({total_chunks} chunks)")

        # Fetch all chunks concurrently (bounded by semaphore)
        all_candles = []

        async def fetch_chunk(idx: int, s: int, e: int):
            candles = await self._fetch_delta_chunk(symbol, resolution, s, e)
            if candles:
                all_candles.extend(candles)
            if (idx + 1) % 20 == 0 or idx + 1 == total_chunks:
                print(f"[DELTA] Progress: {idx + 1}/{total_chunks} chunks ({len(all_candles)} candles)")

        # Process in batches to control memory and rate
        batch_size = MAX_CONCURRENT_REQUESTS
        for batch_start in range(0, total_chunks, batch_size):
            batch = chunks[batch_start : batch_start + batch_size]
            tasks = [fetch_chunk(batch_start + i, s, e) for i, (s, e) in enumerate(batch)]
            await asyncio.gather(*tasks)
            # Small inter-batch delay
            await asyncio.sleep(0.1)

        if not all_candles:
            print(f"[DELTA] No candle data for {symbol} ({resolution})")
            return pd.DataFrame()

        return self._candles_to_df(all_candles, source="delta")

    # ── Paginated Download — Binance ────────────────────────────
    async def download_binance(self, symbol: str, resolution: str, start_date: str, end_date: str) -> pd.DataFrame:
        """
        Download candles from Binance public API with 1000-candle pagination.
        No API key required.
        """
        start_ms = int(datetime.strptime(start_date, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp() * 1000)
        end_ms = int(datetime.strptime(end_date, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp() * 1000)

        secs_per_candle = RESOLUTION_SECONDS.get(resolution, 300)
        chunk_ms = secs_per_candle * BINANCE_MAX_PER_REQUEST * 1000

        chunks = []
        cursor = start_ms
        while cursor < end_ms:
            chunk_end = min(cursor + chunk_ms, end_ms)
            chunks.append((cursor, chunk_end))
            cursor = chunk_end

        total_chunks = len(chunks)
        print(f"[BINANCE] Downloading {symbol} {resolution}: {start_date} → {end_date} ({total_chunks} chunks)")

        all_candles = []

        async def fetch_chunk(idx: int, s: int, e: int):
            candles = await self._fetch_binance_chunk(symbol, resolution, s, e)
            if candles:
                all_candles.extend(candles)
            if (idx + 1) % 20 == 0 or idx + 1 == total_chunks:
                print(f"[BINANCE] Progress: {idx + 1}/{total_chunks} chunks ({len(all_candles)} candles)")

        batch_size = MAX_CONCURRENT_REQUESTS
        for batch_start in range(0, total_chunks, batch_size):
            batch = chunks[batch_start : batch_start + batch_size]
            tasks = [fetch_chunk(batch_start + i, s, e) for i, (s, e) in enumerate(batch)]
            await asyncio.gather(*tasks)
            await asyncio.sleep(0.1)

        if not all_candles:
            print(f"[BINANCE] No candle data for {symbol} ({resolution})")
            return pd.DataFrame()

        return self._candles_to_df(all_candles, source="binance")

    # ── Smart Download (Delta first, Binance fallback) ──────────
    async def download(self, symbol: str, resolution: str, start_date: str, end_date: str) -> pd.DataFrame:
        """
        Smart download: try Delta Exchange first for supported symbols,
        fall back to Binance public API.
        """
        if symbol in DELTA_SYMBOLS:
            df = await self.download_delta(symbol, resolution, start_date, end_date)
            if not df.empty:
                print(f"[DATA] {symbol} {resolution}: {len(df)} candles from Delta")
                if self.use_db:
                    await self._save_to_db(df, symbol, resolution)
                return df
            print(f"[DATA] Delta returned no data for {symbol}, trying Binance...")

        df = await self.download_binance(symbol, resolution, start_date, end_date)
        if not df.empty:
            print(f"[DATA] {symbol} {resolution}: {len(df)} candles from Binance")
            if self.use_db:
                await self._save_to_db(df, symbol, resolution)
        return df

    # ── Bulk Download (multiple symbols × resolutions) ──────────
    async def bulk_download(
        self, symbols: List[str], resolutions: List[str], start_date: str, end_date: str
    ) -> Dict[str, pd.DataFrame]:
        """
        Download candles for multiple symbol × resolution combinations.
        Returns dict keyed by 'SYMBOL_RESOLUTION'.
        """
        await self._ensure_session()
        if self.use_db:
            await self._ensure_db()

        results = {}
        total = len(symbols) * len(resolutions)
        completed = 0

        for symbol in symbols:
            for resolution in resolutions:
                key = f"{symbol}_{resolution}"
                print(f"\n{'─' * 50}")
                print(f"[BULK] [{completed + 1}/{total}] {key}")
                print(f"{'─' * 50}")

                try:
                    df = await self.download(symbol, resolution, start_date, end_date)
                    results[key] = df
                    if not df.empty:
                        print(f"[BULK] ✓ {key}: {len(df)} candles ({df.index[0]} → {df.index[-1]})")
                    else:
                        print(f"[BULK] ✗ {key}: No data")
                except Exception as e:
                    print(f"[BULK] ✗ {key}: Error — {e}")
                    results[key] = pd.DataFrame()

                completed += 1

        print(f"\n{'=' * 50}")
        print(f"[BULK] Download complete: {self._request_count} requests, {self._error_count} errors")
        for key, df in results.items():
            status = f"{len(df)} candles" if not df.empty else "EMPTY"
            print(f"  {key}: {status}")
        print(f"{'=' * 50}")

        return results

    # ── DataFrame Conversion ────────────────────────────────────
    def _candles_to_df(self, candles: list, source: str = "delta") -> pd.DataFrame:
        """Convert raw candle data to standardized OHLCV DataFrame."""
        if not candles:
            return pd.DataFrame()

        if source == "binance":
            # Binance: [[openTime, O, H, L, C, V, closeTime, ...], ...]
            df = pd.DataFrame(
                candles,
                columns=[
                    "open_time",
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume",
                    "close_time",
                    "quote_volume",
                    "trades",
                    "taker_buy_base",
                    "taker_buy_quote",
                    "ignore",
                ],
            )
            df["datetime"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
        else:
            # Delta: [{time, open, high, low, close, volume}, ...]
            df = pd.DataFrame(candles)
            if "time" in df.columns:
                df["datetime"] = pd.to_datetime(df["time"], unit="s", utc=True)
            elif "t" in df.columns:
                df["datetime"] = pd.to_datetime(df["t"], unit="s", utc=True)
            # Handle short-form column names
            rename_map = {}
            for orig, target in [("o", "open"), ("h", "high"), ("l", "low"), ("c", "close"), ("v", "volume")]:
                if orig in df.columns:
                    rename_map[orig] = target
            if rename_map:
                df.rename(columns=rename_map, inplace=True)

        for col in ["open", "high", "low", "close", "volume"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        if "datetime" in df.columns:
            df.set_index("datetime", inplace=True)
            df.sort_index(inplace=True)
            df = df[~df.index.duplicated(keep="first")]

        # Keep only OHLCV
        keep = [c for c in ["open", "high", "low", "close", "volume"] if c in df.columns]
        return df[keep]

    # ── Database Storage ────────────────────────────────────────
    async def _save_to_db(self, df: pd.DataFrame, symbol: str, resolution: str):
        """Bulk upsert candles into PostgreSQL/TimescaleDB."""
        if not self._db_pool or df.empty:
            return

        rows = []
        for ts, row in df.iterrows():
            rows.append(
                (
                    ts.to_pydatetime(),
                    symbol,
                    resolution,
                    float(row["open"]),
                    float(row["high"]),
                    float(row["low"]),
                    float(row["close"]),
                    float(row.get("volume", 0)),
                )
            )

        try:
            async with self._db_pool.acquire() as conn:
                # Use COPY for bulk insert (fastest), with conflict handling
                await conn.executemany(
                    """
                    INSERT INTO candles (time, symbol, resolution, open, high, low, close, volume)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                    ON CONFLICT (time, symbol, resolution) DO UPDATE
                    SET open = EXCLUDED.open, high = EXCLUDED.high,
                        low = EXCLUDED.low, close = EXCLUDED.close,
                        volume = EXCLUDED.volume
                """,
                    rows,
                )
            print(f"[DB] Saved {len(rows)} candles for {symbol} {resolution}")
        except Exception as e:
            print(f"[DB] Save error: {e}")

    async def load_from_db(self, symbol: str, resolution: str, start_date: str, end_date: str) -> pd.DataFrame:
        """Load candles from database."""
        if not self._db_pool:
            await self._ensure_db()
        if not self._db_pool:
            return pd.DataFrame()

        try:
            async with self._db_pool.acquire() as conn:
                rows = await conn.fetch(
                    """
                    SELECT time, open, high, low, close, volume
                    FROM candles
                    WHERE symbol = $1 AND resolution = $2
                      AND time >= $3 AND time <= $4
                    ORDER BY time ASC
                """,
                    symbol,
                    resolution,
                    datetime.strptime(start_date, "%Y-%m-%d").replace(tzinfo=timezone.utc),
                    datetime.strptime(end_date, "%Y-%m-%d").replace(tzinfo=timezone.utc),
                )

            if not rows:
                return pd.DataFrame()

            df = pd.DataFrame([dict(r) for r in rows])
            df.set_index("time", inplace=True)
            df.index.name = "datetime"
            return df
        except Exception as e:
            print(f"[DB] Load error: {e}")
            return pd.DataFrame()

    # ── Gap Detection ───────────────────────────────────────────
    def detect_gaps(self, df: pd.DataFrame, resolution: str, tolerance: float = 1.5) -> List[Tuple[datetime, datetime]]:
        """
        Detect gaps in candle data where the time difference between
        consecutive candles exceeds tolerance × expected interval.

        Returns list of (gap_start, gap_end) tuples.
        """
        if df.empty or len(df) < 2:
            return []

        expected_delta = timedelta(seconds=RESOLUTION_SECONDS.get(resolution, 300))
        max_delta = expected_delta * tolerance

        gaps = []
        timestamps = df.index
        for i in range(1, len(timestamps)):
            diff = timestamps[i] - timestamps[i - 1]
            if diff > max_delta:
                gaps.append((timestamps[i - 1], timestamps[i]))

        if gaps:
            print(f"[DATA] Found {len(gaps)} gaps in {resolution} data:")
            for start, end in gaps[:5]:
                duration = end - start
                print(f"  {start} → {end} ({duration})")
            if len(gaps) > 5:
                print(f"  ... and {len(gaps) - 5} more")

        return gaps

    async def fill_gaps(self, df: pd.DataFrame, symbol: str, resolution: str) -> pd.DataFrame:
        """Detect and fill gaps by fetching missing chunks."""
        gaps = self.detect_gaps(df, resolution)
        if not gaps:
            return df

        print(f"[DATA] Filling {len(gaps)} gaps for {symbol} {resolution}...")
        for gap_start, gap_end in gaps:
            start_str = gap_start.strftime("%Y-%m-%d")
            end_str = gap_end.strftime("%Y-%m-%d")
            chunk = await self.download(symbol, resolution, start_str, end_str)
            if not chunk.empty:
                df = pd.concat([df, chunk])
                df = df[~df.index.duplicated(keep="first")]
                df.sort_index(inplace=True)

        print(f"[DATA] After gap-fill: {len(df)} candles")
        return df

    # ── Incremental Sync ────────────────────────────────────────
    async def incremental_sync(self, symbol: str, resolution: str, existing_df: pd.DataFrame = None) -> pd.DataFrame:
        """
        Only fetch new candles since the last timestamp in existing data.
        Useful for keeping a local cache up-to-date.
        """
        if existing_df is not None and not existing_df.empty:
            last_ts = existing_df.index[-1]
            start_date = last_ts.strftime("%Y-%m-%d")
        else:
            # Default: last 30 days
            start_date = (datetime.utcnow() - timedelta(days=30)).strftime("%Y-%m-%d")

        end_date = datetime.utcnow().strftime("%Y-%m-%d")
        new_df = await self.download(symbol, resolution, start_date, end_date)

        if existing_df is not None and not existing_df.empty and not new_df.empty:
            combined = pd.concat([existing_df, new_df])
            combined = combined[~combined.index.duplicated(keep="last")]
            combined.sort_index(inplace=True)
            return combined

        return new_df if not new_df.empty else (existing_df or pd.DataFrame())


# ── CLI Entry Point ─────────────────────────────────────────────
async def main():
    parser = argparse.ArgumentParser(description="CryptoForge Data Downloader — Async bulk candle download")
    parser.add_argument(
        "--symbols",
        nargs="+",
        default=["BTCUSDT", "ETHUSDT"],
        help="Symbols to download (e.g. BTCUSDT ETHUSDT SOLUSDT)",
    )
    parser.add_argument("--resolutions", nargs="+", default=["5m"], help="Candle resolutions (e.g. 1m 3m 5m)")
    parser.add_argument("--years", type=float, default=1, help="Years of history to download (default: 1)")
    parser.add_argument("--start", type=str, default=None, help="Start date YYYY-MM-DD (overrides --years)")
    parser.add_argument("--end", type=str, default=None, help="End date YYYY-MM-DD (default: today)")
    parser.add_argument("--to-db", action="store_true", help="Store results in PostgreSQL/TimescaleDB")
    parser.add_argument("--to-csv", action="store_true", help="Save results as CSV files")
    parser.add_argument("--output-dir", type=str, default="data", help="Directory for CSV output (default: data/)")
    args = parser.parse_args()

    end_date = args.end or datetime.utcnow().strftime("%Y-%m-%d")
    if args.start:
        start_date = args.start
    else:
        start_dt = datetime.strptime(end_date, "%Y-%m-%d") - timedelta(days=int(args.years * 365.25))
        start_date = start_dt.strftime("%Y-%m-%d")

    print(f"{'=' * 60}")
    print("  CryptoForge Data Downloader")
    print(f"  Symbols:     {', '.join(args.symbols)}")
    print(f"  Resolutions: {', '.join(args.resolutions)}")
    print(f"  Period:      {start_date} → {end_date}")
    print(f"  Database:    {'Yes' if args.to_db else 'No'}")
    print(f"  CSV Output:  {'Yes' if args.to_csv else 'No'}")
    print(f"{'=' * 60}\n")

    downloader = AsyncDataDownloader(use_db=args.to_db)
    try:
        results = await downloader.bulk_download(
            symbols=args.symbols,
            resolutions=args.resolutions,
            start_date=start_date,
            end_date=end_date,
        )

        if args.to_csv:
            os.makedirs(args.output_dir, exist_ok=True)
            for key, df in results.items():
                if not df.empty:
                    path = os.path.join(args.output_dir, f"{key}.csv")
                    df.to_csv(path)
                    print(f"[CSV] Saved {path} ({len(df)} rows)")

        # Print summary statistics
        print(f"\n{'=' * 60}")
        print("  Download Summary")
        print(f"{'=' * 60}")
        total_candles = sum(len(df) for df in results.values() if not df.empty)
        print(f"  Total candles: {total_candles:,}")
        print(f"  API requests:  {downloader._request_count}")
        print(f"  Errors:        {downloader._error_count}")
        for key, df in results.items():
            if not df.empty:
                span = df.index[-1] - df.index[0]
                print(f"  {key}: {len(df):,} candles ({span.days} days)")

    finally:
        await downloader.close()


if __name__ == "__main__":
    asyncio.run(main())
