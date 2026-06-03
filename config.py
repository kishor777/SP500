"""Application configuration — all tuneable constants and static lookup tables."""
import re
from datetime import date as _date
from zoneinfo import ZoneInfo

# ── Timezone & Market Calendar ────────────────────────────────────────────────

_NYSE_TZ = ZoneInfo("America/New_York")

# NYSE holidays 2025-2026 (early closings excluded — full closes only)
_NYSE_HOLIDAYS = {
    _date(2025, 1, 1),   # New Year's Day
    _date(2025, 1, 20),  # MLK Day
    _date(2025, 2, 17),  # Presidents' Day
    _date(2025, 4, 18),  # Good Friday
    _date(2025, 5, 26),  # Memorial Day
    _date(2025, 6, 19),  # Juneteenth
    _date(2025, 7, 4),   # Independence Day
    _date(2025, 9, 1),   # Labor Day
    _date(2025, 11, 27), # Thanksgiving
    _date(2025, 12, 25), # Christmas
    _date(2026, 1, 1),   # New Year's Day
    _date(2026, 1, 19),  # MLK Day
    _date(2026, 2, 16),  # Presidents' Day
    _date(2026, 4, 3),   # Good Friday
    _date(2026, 5, 25),  # Memorial Day
    _date(2026, 6, 19),  # Juneteenth
    _date(2026, 7, 3),   # Independence Day (observed)
    _date(2026, 9, 7),   # Labor Day
    _date(2026, 11, 26), # Thanksgiving
    _date(2026, 12, 25), # Christmas
}

# ── Scheduler ─────────────────────────────────────────────────────────────────

SCHEDULER_CONFIG = {
    "refresh_interval_sec":    300,   # price-refresh cadence (seconds)
    "vol_trade_buy_hour":        9,   # ET hour   to trigger paper buy  (>=)
    "vol_trade_buy_minute":     50,   # ET minute to trigger paper buy  (>=)
    "vol_trade_sell_hour":      15,   # ET hour   to trigger paper sell (>=)
    "vol_trade_sell_minute":    55,   # ET minute to trigger paper sell (>=)
    "intraday_batch_size":      50,   # tickers per yfinance batch
    "intraday_backfill_days":   60,   # how far back to fetch 5-min bars
    "intraday_skip_threshold": 1000,  # skip backfill if DB already has this many rows
    "hist_batch_size":          50,   # tickers per daily-history batch
    "hist_backfill_days":      730,   # 2 years of daily history to back-fill
}

# ── Admin Settings Schema ─────────────────────────────────────────────────────
# Single source of truth for every DB-backed setting.
# Each entry: key (DB key_name), label, type (int|float|str), default, and
# optional unit/hint strings shown in the admin UI.

SETTINGS_SCHEMA: dict[str, list[dict]] = {
    "Scheduler": [
        {"key": "refresh_interval_sec",  "label": "Price Refresh Interval", "type": "int",   "default": 300,  "unit": "sec",     "hint": "How often live prices are fetched during market hours"},
        {"key": "vol_trade_buy_hour",    "label": "Vol Trade Buy Hour (ET)", "type": "int",   "default": 9,    "unit": "hour"},
        {"key": "vol_trade_buy_minute",  "label": "Vol Trade Buy Minute",    "type": "int",   "default": 50,   "unit": "min"},
        {"key": "vol_trade_sell_hour",   "label": "Vol Trade Sell Hour (ET)","type": "int",   "default": 15,   "unit": "hour"},
        {"key": "vol_trade_sell_minute", "label": "Vol Trade Sell Minute",   "type": "int",   "default": 55,   "unit": "min"},
    ],
    "Vol Trade Strategy": [
        {"key": "vol_trade_quality_min",  "label": "Min Quality Score",   "type": "int",   "default": 70,   "unit": "0–100", "hint": "Fundamental quality percentile threshold — stocks below this are skipped"},
        {"key": "vol_trade_vol_ratio_min","label": "Min Volume Ratio",    "type": "float", "default": 2.0,  "unit": "×",     "hint": "Minimum today_vol ÷ expected_vol to qualify as abnormal"},
        {"key": "vol_trade_amount_usd",   "label": "Trade Amount",        "type": "int",   "default": 1000, "unit": "USD",   "hint": "Dollar amount invested per paper trade"},
    ],
    "Recommendation Engine — Signal Weights": [
        {"key": "reco_w_momentum",    "label": "Momentum",   "type": "float", "default": 0.15, "hint": "3M/6M/1Y price return blend"},
        {"key": "reco_w_fundamental", "label": "Quality",    "type": "float", "default": 0.25, "hint": "ROE, margins, revenue growth"},
        {"key": "reco_w_valuation",   "label": "Valuation",  "type": "float", "default": 0.15, "hint": "P/E, P/B, EV/EBITDA (inverted)"},
        {"key": "reco_w_guru",        "label": "Guru",       "type": "float", "default": 0.15, "hint": "13F conviction score"},
        {"key": "reco_w_analyst",     "label": "Analyst",    "type": "float", "default": 0.15, "hint": "Yahoo Finance recommendation"},
        {"key": "reco_w_sentiment",   "label": "Sentiment",  "type": "float", "default": 0.15, "hint": "StockTwits + Reddit FinBERT"},
    ],
    "Recommendation Engine — Label Thresholds": [
        {"key": "reco_thr_strong_buy", "label": "Strong Buy ≥",   "type": "int", "default": 68, "unit": "score"},
        {"key": "reco_thr_buy",        "label": "Buy ≥",          "type": "int", "default": 57, "unit": "score"},
        {"key": "reco_thr_hold",       "label": "Hold ≥",         "type": "int", "default": 44, "unit": "score"},
        {"key": "reco_thr_under",      "label": "Underperform ≥", "type": "int", "default": 32, "unit": "score"},
    ],
    "Sentiment Engine": [
        {"key": "sent_w_stocktwits", "label": "StockTwits Weight",    "type": "float", "default": 0.40},
        {"key": "sent_w_reddit",     "label": "Reddit Weight",        "type": "float", "default": 0.30},
        {"key": "sent_w_volume",     "label": "Volume Spike Weight",  "type": "float", "default": 0.20},
        {"key": "sent_w_momentum",   "label": "Sentiment Momentum",   "type": "float", "default": 0.10},
        {"key": "sent_lookback",     "label": "Lookback Days",        "type": "int",   "default": 3,    "unit": "days", "hint": "Only posts from the last N days are scored"},
        {"key": "sent_refresh_hour", "label": "Daily Refresh Hour",   "type": "int",   "default": 12,   "unit": "ET",   "hint": "Allow a second daily sentiment run after this hour"},
    ],
    "Cache / TTL": [
        {"key": "ttl_reco",  "label": "Recommendations TTL", "type": "int", "default": 3600,  "unit": "sec", "hint": "Recompute recommendation scores after this interval"},
        {"key": "ttl_guru",  "label": "Guru Holdings TTL",   "type": "int", "default": 86400, "unit": "sec", "hint": "Re-fetch 13F guru data after this interval"},
        {"key": "ttl_optim", "label": "Optimised Rules TTL", "type": "int", "default": 3600,  "unit": "sec"},
    ],
}

# ── TTL / Cache Timeouts ──────────────────────────────────────────────────────

_DB_GURU_TTL    = 86400  # seconds before DB guru cache is considered stale (24h)
_OPTIMIZED_TTL  = 3600   # recompute optimized rules once per hour
_RECO_TTL       = 3600   # recompute recommendation engine once per hour

# ── Reddit API credentials (set in .env — never hardcode here) ────────────────
import os as _os
try:
    from dotenv import load_dotenv as _load_dotenv
    _load_dotenv()
except ImportError:
    pass
REDDIT_CLIENT_ID     = _os.getenv("REDDIT_CLIENT_ID",     "")
REDDIT_CLIENT_SECRET = _os.getenv("REDDIT_CLIENT_SECRET", "")

# ── Sentiment Engine ───────────────────────────────────────────────────────────

# Sub-signal weights within the sentiment composite (must sum to 1.0)
SENTIMENT_STOCKTWITS_WEIGHT = 0.40   # StockTwits sentiment score
SENTIMENT_REDDIT_WEIGHT     = 0.30   # Reddit sentiment score
SENTIMENT_VOLUME_WEIGHT     = 0.20   # mention volume spike vs baseline
SENTIMENT_MOMENTUM_WEIGHT   = 0.10   # change vs yesterday's sentiment

SENTIMENT_LOOKBACK_DAYS  = 3         # only use posts from last N days
SENTIMENT_MIN_POST_LEN   = 20        # chars — shorter posts treated as noise
SENTIMENT_REFRESH_HOUR   = 12        # ET hour — allow a second daily run after this
SENTIMENT_SUBREDDITS     = [         # subreddits to search for ticker mentions
    "wallstreetbets", "stocks", "investing",
    "SecurityAnalysis", "options",
]

# ── Recommendation Engine Weights ─────────────────────────────────────────────

# Momentum signal: weighted blend of price-return periods before percentile-ranking
RECO_MOMENTUM_WEIGHTS = {"m3": 0.30, "m6": 0.40, "y1": 0.30}

# Fundamental quality signal: metrics and their intra-signal weights
RECO_FUNDAMENTAL_FIELDS = {
    "returnOnEquity":   0.30,
    "grossMargins":     0.20,
    "operatingMargins": 0.20,
    "revenueGrowth":    0.15,
    "earningsGrowth":   0.15,
}

# Valuation signal: multiples and their intra-signal weights (lower = better)
RECO_VALUATION_FIELDS = {
    "trailingPE":                   0.30,
    "priceToBook":                  0.25,
    "priceToSalesTrailing12Months": 0.25,
    "enterpriseToEbitda":           0.20,
}

# Guru conviction signal: points awarded per 13F change tag
RECO_GURU_TAG_WEIGHT = {"new": 3.0, "added": 2.0, "held": 1.0, "reduced": -0.5, "": 1.0}

# Analyst consensus signal: fixed score per Yahoo Finance recommendationKey
RECO_ANALYST_SCORE_MAP = {
    "strongbuy": 100, "strong_buy": 100, "buy": 80,
    "hold": 50, "underperform": 25, "sell": 5,
}

# Composite: weight of each signal in the final 0-100 score
RECO_SIGNAL_WEIGHTS = {
    "momentum":    0.15,   # reduced from 0.25 to make room for sentiment
    "fundamental": 0.25,
    "valuation":   0.15,
    "guru":        0.15,   # reduced from 0.20 to make room for sentiment
    "analyst":     0.15,
    "sentiment":   0.15,   # new — Market Sentiment Engine
}

# Score → label thresholds (upper-inclusive)
RECO_LABEL_THRESHOLDS = [
    (68, "Strong Buy"),
    (57, "Buy"),
    (44, "Hold"),
    (32, "Underperform"),
    (0,  "Sell"),
]

# Optimized-guru screener: period weights used to score candidate rule sets
RECO_OPT_PERIOD_WEIGHTS = {"w1": 0.05, "m1": 0.10, "m3": 0.20, "m6": 0.25, "y1": 0.40}

# ── SEC EDGAR ─────────────────────────────────────────────────────────────────

_EDGAR_UA = {"User-Agent": "SP500Viewer/1.0 kishor77@gmail.com"}

# ── Main Table Columns ────────────────────────────────────────────────────────

TABLE_COLS = [
    "longName", "sector", "industry", "currentPrice", "marketCap",
    "trailingPE", "forwardPE", "dividendYield", "beta",
    "returnOnEquity", "profitMargins", "revenueGrowth", "recommendationKey",
    "52WeekChange",
]

# ── Company Detail Sections ───────────────────────────────────────────────────

DETAIL_SECTIONS = {
    "Company Profile & Address": [
        "longName", "shortName", "displayName", "prevName", "symbol",
        "address1", "address2", "city", "state", "zip", "country",
        "phone", "fax", "website", "irWebsite",
        "industry", "industryDisp", "industrySymbol", "sector", "sectorDisp",
        "longBusinessSummary", "fullTimeEmployees", "executiveTeam",
        "nameChangeDate", "prevTicker", "tickerChangeDate", "ipoExpectedDate",
    ],
    "Governance & Risk": [
        "auditRisk", "boardRisk", "compensationRisk",
        "shareHolderRightsRisk", "overallRisk",
        "governanceEpochDate", "compensationAsOfEpochDate",
    ],
    "Market Pricing & Trading": [
        "currentPrice", "previousClose", "open", "dayLow", "dayHigh",
        "regularMarketPrice", "regularMarketPreviousClose", "regularMarketOpen",
        "regularMarketDayLow", "regularMarketDayHigh",
        "regularMarketChange", "regularMarketChangePercent", "regularMarketDayRange",
        "marketState", "marketCap", "nonDilutedMarketCap",
        "volume", "regularMarketVolume", "averageVolume",
        "averageVolume10days", "averageDailyVolume10Day", "averageDailyVolume3Month",
        "bid", "ask", "bidSize", "askSize",
        "fiftyTwoWeekLow", "fiftyTwoWeekHigh", "fiftyTwoWeekRange", "allTimeHigh", "allTimeLow",
        "fiftyDayAverage", "twoHundredDayAverage",
        "fiftyTwoWeekLowChange", "fiftyTwoWeekLowChangePercent",
        "fiftyTwoWeekHighChange", "fiftyTwoWeekHighChangePercent",
        "52WeekChange", "fiftyTwoWeekChangePercent", "SandP52WeekChange",
        "fiftyDayAverageChange", "fiftyDayAverageChangePercent",
        "twoHundredDayAverageChange", "twoHundredDayAverageChangePercent",
    ],
    "Dividends & Splits": [
        "dividendRate", "dividendYield", "exDividendDate", "payoutRatio",
        "fiveYearAvgDividendYield", "trailingAnnualDividendRate", "trailingAnnualDividendYield",
        "dividendDate", "lastDividendValue", "lastDividendDate",
        "lastSplitFactor", "lastSplitDate",
    ],
    "Valuation Metrics": [
        "beta", "trailingPE", "forwardPE", "pegRatio", "trailingPegRatio",
        "priceToSalesTrailing12Months", "enterpriseValue",
        "enterpriseToRevenue", "enterpriseToEbitda", "bookValue", "priceToBook",
        "targetHighPrice", "targetLowPrice", "targetMeanPrice", "targetMedianPrice",
        "recommendationMean", "recommendationKey", "averageAnalystRating",
        "numberOfAnalystOpinions", "priceEpsCurrentYear",
    ],
    "Shares & Ownership": [
        "floatShares", "sharesOutstanding", "impliedSharesOutstanding",
        "sharesShort", "sharesShortPriorMonth", "sharesShortPreviousMonthDate",
        "dateShortInterest", "sharesPercentSharesOut",
        "heldPercentInsiders", "heldPercentInstitutions",
        "shortRatio", "shortPercentOfFloat",
    ],
    "Financial Statements & Performance": [
        "totalCash", "totalCashPerShare", "ebitda", "totalDebt",
        "quickRatio", "currentRatio", "totalRevenue", "debtToEquity",
        "revenuePerShare", "returnOnAssets", "returnOnEquity",
        "grossProfits", "freeCashflow", "operatingCashflow", "netIncomeToCommon",
        "profitMargins", "grossMargins", "ebitdaMargins", "operatingMargins",
        "earningsGrowth", "revenueGrowth", "earningsQuarterlyGrowth",
        "trailingEps", "forwardEps", "epsTrailingTwelveMonths", "epsForward",
        "epsCurrentYear", "financialCurrency",
    ],
    "Fiscal & Earnings Dates": [
        "lastFiscalYearEnd", "nextFiscalYearEnd", "mostRecentQuarter",
        "earningsTimestamp", "earningsTimestampStart", "earningsTimestampEnd",
        "earningsCallTimestampStart", "earningsCallTimestampEnd",
        "isEarningsDateEstimate", "firstTradeDateMilliseconds",
    ],
    "Market & Exchange Information": [
        "currency", "tradeable", "cryptoTradeable", "quoteType", "typeDisp",
        "quoteSourceName", "exchange", "fullExchangeName",
        "exchangeTimezoneName", "exchangeTimezoneShortName", "gmtOffSetMilliseconds",
        "market", "sourceInterval", "exchangeDataDelayedBy",
        "hasPrePostMarketData", "triggerable", "customPriceAlertConfidence",
        "corporateActions", "postMarketTime", "regularMarketTime",
        "postMarketPrice", "postMarketChange", "postMarketChangePercent",
        "language", "region", "messageBoardId", "esgPopulated",
    ],
}

# ── Screener Field Definitions ────────────────────────────────────────────────
# fmt: "num" | "dollar" | "cap" | "pct_frac" (×100 for display) | "pct_val" (already %)

SCREENER_NUM_GROUPS = {
    "Governance & Risk": [
        {"id": "auditRisk",             "label": "Audit Risk"},
        {"id": "boardRisk",             "label": "Board Risk"},
        {"id": "compensationRisk",      "label": "Compensation Risk"},
        {"id": "shareHolderRightsRisk", "label": "Shareholder Rights Risk"},
        {"id": "overallRisk",           "label": "Overall Risk"},
    ],
    "Market Pricing": [
        {"id": "currentPrice",          "label": "Price",              "fmt": "dollar"},
        {"id": "marketCap",             "label": "Market Cap",         "fmt": "cap"},
        {"id": "beta",                  "label": "Beta"},
        {"id": "52WeekChange",           "label": "52-Week Return",     "fmt": "pct_frac"},
        {"id": "averageVolume",         "label": "Avg Volume",         "fmt": "cap"},
        {"id": "fiftyTwoWeekLow",             "label": "52W Low",                "fmt": "dollar"},
        {"id": "fiftyTwoWeekHigh",            "label": "52W High",               "fmt": "dollar"},
        {"id": "fiftyTwoWeekLowChangePercent","label": "% Above 52W Low",        "fmt": "pct_frac"},
    ],
    "Valuation": [
        {"id": "trailingPE",                    "label": "Trailing P/E"},
        {"id": "forwardPE",                     "label": "Forward P/E"},
        {"id": "pegRatio",                      "label": "PEG Ratio"},
        {"id": "trailingPegRatio",              "label": "Trailing PEG Ratio"},
        {"id": "priceToBook",                   "label": "Price / Book"},
        {"id": "priceToSalesTrailing12Months",  "label": "Price / Sales"},
        {"id": "enterpriseToRevenue",           "label": "EV / Revenue"},
        {"id": "enterpriseToEbitda",            "label": "EV / EBITDA"},
        {"id": "recommendationMean",            "label": "Analyst Score (1=Buy 5=Sell)"},
        {"id": "numberOfAnalystOpinions",       "label": "# Analyst Opinions"},
    ],
    "Dividends": [
        {"id": "dividendYield",              "label": "Dividend Yield",        "fmt": "pct_val"},
        {"id": "dividendRate",               "label": "Dividend Rate",         "fmt": "dollar"},
        {"id": "payoutRatio",                "label": "Payout Ratio",          "fmt": "pct_frac"},
        {"id": "fiveYearAvgDividendYield",   "label": "5-Year Avg Yield",      "fmt": "pct_val"},
        {"id": "trailingAnnualDividendYield","label": "Trailing Annual Yield", "fmt": "pct_frac"},
    ],
    "Shares & Ownership": [
        {"id": "heldPercentInsiders",     "label": "Insider Holding",       "fmt": "pct_frac"},
        {"id": "heldPercentInstitutions", "label": "Institutional Holding", "fmt": "pct_frac"},
        {"id": "shortRatio",              "label": "Short Ratio"},
        {"id": "shortPercentOfFloat",     "label": "Short % of Float",      "fmt": "pct_frac"},
        {"id": "sharesPercentSharesOut",  "label": "Short % of Shares Out", "fmt": "pct_frac"},
    ],
    "Financial Performance": [
        {"id": "returnOnAssets",         "label": "Return on Assets",     "fmt": "pct_frac"},
        {"id": "returnOnEquity",         "label": "Return on Equity",     "fmt": "pct_frac"},
        {"id": "profitMargins",          "label": "Profit Margin",        "fmt": "pct_frac"},
        {"id": "grossMargins",           "label": "Gross Margin",         "fmt": "pct_frac"},
        {"id": "ebitdaMargins",          "label": "EBITDA Margin",        "fmt": "pct_frac"},
        {"id": "operatingMargins",       "label": "Operating Margin",     "fmt": "pct_frac"},
        {"id": "revenueGrowth",          "label": "Revenue Growth",       "fmt": "pct_frac"},
        {"id": "earningsGrowth",         "label": "Earnings Growth",      "fmt": "pct_frac"},
        {"id": "earningsQuarterlyGrowth","label": "Quarterly EPS Growth", "fmt": "pct_frac"},
        {"id": "debtToEquity",           "label": "Debt / Equity"},
        {"id": "currentRatio",           "label": "Current Ratio"},
        {"id": "quickRatio",             "label": "Quick Ratio"},
    ],
    "Financials (Size)": [
        {"id": "totalRevenue",      "label": "Total Revenue",      "fmt": "cap"},
        {"id": "ebitda",            "label": "EBITDA",             "fmt": "cap"},
        {"id": "netIncomeToCommon", "label": "Net Income",         "fmt": "cap"},
        {"id": "totalCash",         "label": "Total Cash",         "fmt": "cap"},
        {"id": "totalDebt",         "label": "Total Debt",         "fmt": "cap"},
        {"id": "freeCashflow",      "label": "Free Cash Flow",     "fmt": "cap"},
        {"id": "operatingCashflow", "label": "Operating Cash Flow","fmt": "cap"},
        {"id": "fullTimeEmployees", "label": "Employees",          "fmt": "num"},
    ],
}

SCREENER_CAT_FIELDS = [
    {"id": "sector",           "label": "Sector"},
    {"id": "industry",         "label": "Industry"},
    {"id": "country",          "label": "Country"},
    {"id": "exchange",         "label": "Exchange"},
    {"id": "recommendationKey","label": "Analyst Recommendation"},
]

# ── Optimized-Screener Skip / Scale Sets ──────────────────────────────────────

# Fields to skip — absolute price levels that don't scale across stocks
_OPT_SKIP = {
    "currentPrice", "fiftyTwoWeekHigh", "fiftyTwoWeekLow",
    "fiftyTwoWeekHighChange", "fiftyTwoWeekLowChange",
    "fiftyDayAverage", "twoHundredDayAverage",
    "fiftyDayAverageChange", "twoHundredDayAverageChange",
}

_OPT_PCT_FRAC = {
    "returnOnEquity", "profitMargins", "grossMargins", "operatingMargins",
    "ebitdaMargins", "revenueGrowth", "earningsGrowth", "earningsQuarterlyGrowth",
    "returnOnAssets", "dividendYield", "52WeekChange",
    "fiftyTwoWeekLowChangePercent", "payoutRatio", "shortPercentOfFloat",
}

# ── Gurus ─────────────────────────────────────────────────────────────────────

GURUS = {
    "buffett": {
        "name": "Warren Buffett", "fund": "Berkshire Hathaway",
        "cik": "1067983", "style": "Quality Value", "color": "#1a4e8a",
        "quote": "It's far better to buy a wonderful company at a fair price than a fair company at a wonderful price.",
        "description": "Buffett seeks durable competitive moats, consistent earnings power, and high returns on equity — purchased at sensible prices and held forever.",
        "rules": {"numeric": {
            "returnOnEquity":   {"min": 15},
            "grossMargins":     {"min": 40},
            "profitMargins":    {"min": 10},
            "operatingMargins": {"min": 15},
            "debtToEquity":     {"max": 80},
            "trailingPE":       {"max": 25},
            "revenueGrowth":    {"min": 3},
            "earningsGrowth":   {"min": 5},
            "freeCashflow":     {"min": 1},
        }, "categorical": {}},
    },
    "ackman": {
        "name": "Bill Ackman", "fund": "Pershing Square Capital Management",
        "cik": "1336528", "style": "Concentrated Quality", "color": "#0a5c36",
        "quote": "We look for simple, predictable, free-cash-flow-generative businesses with dominant market positions.",
        "description": "Ackman runs a concentrated portfolio of large-cap businesses with pricing power, high margins, and strong free cash flow. He occasionally takes activist positions to unlock value.",
        "rules": {"numeric": {
            "marketCap":        {"min": 10_000_000_000},
            "returnOnEquity":   {"min": 20},
            "returnOnAssets":   {"min": 10},
            "grossMargins":     {"min": 40},
            "operatingMargins": {"min": 20},
            "freeCashflow":     {"min": 1_000_000_000},
            "revenueGrowth":    {"min": 5},
            "beta":             {"max": 1.5},
        }, "categorical": {}},
    },
    "einhorn": {
        "name": "David Einhorn", "fund": "Greenlight Capital",
        "cik": "1079114", "style": "Deep Value", "color": "#7b2d00",
        "quote": "We try to buy things that are cheap relative to our assessment of intrinsic value.",
        "description": "Einhorn combines long deep-value positions with short ideas. He targets cheap stocks with strong fundamentals and shorts expensive or misleading companies.",
        "rules": {"numeric": {
            "trailingPE":                   {"max": 15},
            "priceToBook":                  {"max": 2.0},
            "priceToSalesTrailing12Months": {"max": 2.0},
            "enterpriseToEbitda":           {"max": 10},
            "debtToEquity":                 {"max": 80},
            "currentRatio":                 {"min": 1.0},
            "returnOnEquity":               {"min": 8},
            "profitMargins":                {"min": 5},
            "operatingMargins":             {"min": 8},
        }, "categorical": {}},
    },
    "burry": {
        "name": "Michael Burry", "fund": "Scion Asset Management",
        "cik": "1649339", "style": "Deep Value / Contrarian", "color": "#4a0080",
        "quote": "My metric for everything I look at is the 10-year treasury — everything is present value.",
        "description": "Burry is a contrarian deep-value investor famous for 'The Big Short'. He hunts for severely undervalued companies with strong free cash flow, often ignored or shorted by the market.",
        "rules": {"numeric": {
            "trailingPE":                   {"max": 12},
            "priceToBook":                  {"max": 1.5},
            "priceToSalesTrailing12Months": {"max": 1.5},
            "enterpriseToEbitda":           {"max": 8},
            "freeCashflow":                 {"min": 1},
            "debtToEquity":                 {"max": 60},
            "operatingMargins":             {"min": 5},
            "shortRatio":                   {"min": 1},
        }, "categorical": {}},
    },
    "druckenmiller": {
        "name": "Stanley Druckenmiller", "fund": "Duquesne Family Office",
        "cik": "1536411", "style": "Macro Growth", "color": "#8b4513",
        "quote": "Earnings don't move the overall market; it's the Federal Reserve Board — focus on liquidity.",
        "description": "Druckenmiller is a macro investor who focuses on earnings momentum, revenue growth trends, and liquidity cycles. He looks for asymmetric risk/reward opportunities.",
        "rules": {"numeric": {
            "revenueGrowth":           {"min": 15},
            "earningsGrowth":          {"min": 10},
            "earningsQuarterlyGrowth": {"min": 10},
            "returnOnEquity":          {"min": 15},
            "grossMargins":            {"min": 35},
            "operatingMargins":        {"min": 15},
            "forwardPE":               {"max": 40},
            "marketCap":               {"min": 1_000_000_000},
            "averageVolume":           {"min": 500_000},
        }, "categorical": {}},
    },
    "icahn": {
        "name": "Carl Icahn", "fund": "Icahn Capital Management",
        "cik": "921669", "style": "Activist Value", "color": "#b8860b",
        "quote": "In life and business, there are two cardinal sins: the first is to act precipitously without thought, and the second is to not act at all.",
        "description": "Icahn is a corporate activist who takes large stakes in undervalued companies and pushes for buybacks, cost cuts, spinoffs, or strategic sales to unlock hidden value.",
        "rules": {"numeric": {
            "priceToBook":        {"max": 2.5},
            "debtToEquity":       {"max": 100},
            "currentRatio":       {"min": 1.0},
            "trailingPE":         {"max": 20},
            "profitMargins":      {"min": 5},
            "enterpriseToEbitda": {"max": 12},
            "freeCashflow":       {"min": 1},
            "marketCap":          {"min": 500_000_000},
        }, "categorical": {}},
    },
    "dalio": {
        "name": "Ray Dalio", "fund": "Bridgewater Associates",
        "cik": "1350694", "style": "All Weather / Risk Parity", "color": "#1a5276",
        "quote": "He who lives by the crystal ball will eat shattered glass.",
        "description": "Dalio's All Weather strategy balances assets across economic environments. His equity holdings favour large-cap, defensive names with consistent dividends and low volatility.",
        "rules": {"numeric": {
            "beta":             {"max": 1.0},
            "dividendYield":    {"min": 1.0},
            "marketCap":        {"min": 10_000_000_000},
            "currentRatio":     {"min": 1.0},
            "debtToEquity":     {"max": 60},
            "returnOnAssets":   {"min": 5},
            "profitMargins":    {"min": 10},
            "operatingMargins": {"min": 12},
        }, "categorical": {}},
    },
    "loeb": {
        "name": "Dan Loeb", "fund": "Third Point LLC",
        "cik": "1040273", "style": "Event-Driven Value", "color": "#6e2594",
        "quote": "The best investments come from companies in transition — new management, spinoffs, restructurings.",
        "description": "Loeb is an event-driven activist who targets companies undergoing strategic change — spinoffs, CEO transitions, or restructurings — where the market has mispriced the outcome.",
        "rules": {"numeric": {
            "trailingPE":         {"max": 20},
            "returnOnEquity":     {"min": 12},
            "grossMargins":       {"min": 30},
            "operatingMargins":   {"min": 12},
            "profitMargins":      {"min": 8},
            "revenueGrowth":      {"min": 5},
            "enterpriseToEbitda": {"max": 15},
            "marketCap":          {"min": 1_000_000_000},
        }, "categorical": {}},
    },
    "coleman": {
        "name": "Chase Coleman", "fund": "Tiger Global Management",
        "cik": "1167483", "style": "High-Growth Tech", "color": "#0e6655",
        "quote": "We're looking for businesses that are reshaping entire industries with durable network effects.",
        "description": "Coleman focuses on high-growth technology and internet businesses with strong secular tailwinds, large total addressable markets, and defensible network effects.",
        "rules": {
            "numeric": {
                "revenueGrowth":           {"min": 20},
                "earningsQuarterlyGrowth": {"min": 15},
                "grossMargins":            {"min": 50},
                "operatingMargins":        {"min": 10},
                "returnOnEquity":          {"min": 15},
                "forwardPE":               {"max": 60},
                "marketCap":               {"min": 1_000_000_000},
            },
            "categorical": {"sector": ["Technology", "Communication Services"]},
        },
    },
    "tomlee": {
        "name": "Tom Lee", "fund": "Fundstrat Capital (GRNY ETF)",
        "cik": "1722388", "series_id": "S000088227", "source": "nport",
        "style": "Growth & Momentum", "color": "#c0392b",
        "quote": "The stocks that keep showing up across multiple themes — that's the signal.",
        "description": "Tom Lee's 'Granny Shots' methodology selects large-cap stocks that appear in at least two of Fundstrat's seven investment themes. The resulting equal-weighted basket favours earnings-revision momentum, strong sector tailwinds, and improving fundamentals. Holdings are pulled from the GRNY ETF's SEC NPORT-P filings.",
        "rules": {"numeric": {
            "earningsGrowth":   {"min": 10},
            "revenueGrowth":    {"min": 5},
            "returnOnEquity":   {"min": 15},
            "returnOnAssets":   {"min": 8},
            "grossMargins":     {"min": 30},
            "forwardPE":        {"max": 45},
            "marketCap":        {"min": 10_000_000_000},
            "52WeekChange":     {"min": 0},
            "averageVolume":    {"min": 1_000_000},
        }, "categorical": {}},
    },
    "tepper": {
        "name": "David Tepper", "fund": "Appaloosa Management",
        "cik": "1656456", "style": "Distressed & Macro", "color": "#1abc9c",
        "quote": "Be open-minded. The market is telling you something and you need to listen to it.",
        "description": "Tepper is a macro-oriented value investor who built his reputation buying distressed debt and beaten-down equities during financial crises. He pairs top-down macro conviction with fundamental analysis, concentrating in high-conviction ideas where he sees asymmetric risk-reward — heavy in tech, financials, and cyclicals.",
        "rules": {"numeric": {
            "trailingPE":       {"max": 20},
            "priceToBook":      {"max": 4},
            "returnOnEquity":   {"min": 12},
            "debtToEquity":     {"max": 150},
            "revenueGrowth":    {"min": 3},
            "currentRatio":     {"min": 0.8},
            "profitMargins":    {"min": 5},
            "operatingMargins": {"min": 8},
            "marketCap":        {"min": 2_000_000_000},
            "averageVolume":    {"min": 1_000_000},
        }, "categorical": {}},
    },
    "ptjones": {
        "name": "Paul Tudor Jones", "fund": "Tudor Investment Corp",
        "cik": "923093", "max_holdings": 300, "style": "Global Macro & Momentum", "color": "#e67e22",
        "quote": "The secret to being successful from a trading perspective is to have an indefatigable and an undying and unquenchable thirst for information and knowledge.",
        "description": "Paul Tudor Jones is a legendary macro trader known for predicting Black Monday 1987. He blends top-down macro analysis with technical momentum signals, trading across equities, commodities, currencies and fixed income. His equity book reflects cyclical and momentum themes within his broader macro views.",
        "rules": {"numeric": {
            "beta":             {"min": 0.8},
            "52WeekChange":     {"min": 0},
            "revenueGrowth":    {"min": 5},
            "earningsGrowth":   {"min": 5},
            "grossMargins":     {"min": 30},
            "operatingMargins": {"min": 10},
            "returnOnEquity":   {"min": 10},
            "averageVolume":    {"min": 1_000_000},
            "marketCap":        {"min": 5_000_000_000},
        }, "categorical": {}},
    },
    "cathiewood": {
        "name": "Cathie Wood", "fund": "ARK Investment Management",
        "cik": "1697748", "style": "Disruptive Innovation", "color": "#8e44ad",
        "quote": "We focus on companies that are leading disruptive innovation and that have the potential to be worth much more in 5 years than they are today.",
        "description": "Cathie Wood concentrates on five major innovation platforms: genomics, robotics, energy storage, AI, and blockchain. ARK takes 5-year time horizons and accepts high near-term volatility in exchange for exponential long-term upside. Portfolios are typically small in number (~30–50 names) with very high active share.",
        "rules": {"numeric": {
            "revenueGrowth":  {"min": 20},
            "grossMargins":   {"min": 40},
            "marketCap":      {"min": 500_000_000},
            "beta":           {"min": 1.2},
            "forwardPE":      {"max": 100},
            "averageVolume":  {"min": 500_000},
        }, "categorical": {"sector": ["Technology", "Health Care", "Communication Services"]}},
    },
    "griffin": {
        "name": "Ken Griffin", "fund": "Citadel Advisors",
        "cik": "1423053", "max_holdings": 300, "style": "Multi-Strategy Quantitative", "color": "#2c3e50",
        "quote": "Great trading is really a synthesis of quantitative techniques and a sound understanding of market dynamics.",
        "description": "Citadel runs one of the world's largest multi-strategy hedge funds, combining quantitative systematic strategies, fundamental equity long/short, fixed income, and macro. The 13F reflects its massive long equity book — diversified across thousands of positions — driven by quantitative factor models emphasising quality, momentum, and value.",
        "rules": {"numeric": {
            "marketCap":        {"min": 2_000_000_000},
            "averageVolume":    {"min": 500_000},
            "returnOnEquity":   {"min": 10},
            "returnOnAssets":   {"min": 5},
            "profitMargins":    {"min": 5},
            "grossMargins":     {"min": 20},
            "trailingPE":       {"max": 35},
            "revenueGrowth":    {"min": 0},
            "beta":             {"min": 0.5, "max": 2.5},
            "currentRatio":     {"min": 0.8},
        }, "categorical": {}},
    },
    "simons": {
        "name": "Jim Simons", "fund": "Renaissance Technologies",
        "cik": "1037389", "max_holdings": 300, "style": "Quantitative Systematic", "color": "#3498db",
        "quote": "The best way to conduct research is to be systematic — let the data speak, not your intuitions.",
        "description": "Simons built the most successful quant fund in history using mathematical models and pattern recognition. Renaissance's public RIEF portfolio targets highly liquid large-cap equities exhibiting momentum, quality, and mean-reversion signals identified through statistical analysis of vast historical datasets.",
        "rules": {"numeric": {
            "marketCap":      {"min": 2_000_000_000},
            "averageVolume":  {"min": 1_000_000},
            "52WeekChange":   {"min": 0},
            "returnOnEquity": {"min": 10},
            "profitMargins":  {"min": 5},
            "beta":           {"min": 0.5, "max": 2.0},
        }, "categorical": {}},
    },
    "klarman": {
        "name": "Seth Klarman", "fund": "Baupost Group",
        "cik": "1061768", "style": "Deep Value / Margin of Safety", "color": "#7f8c8d",
        "quote": "Value investing is at its core the marriage of a contrarian streak and a calculator.",
        "description": "Klarman wrote the definitive text on margin of safety and is among the most respected deep value investors alive. Baupost buys out-of-favour assets at steep discounts to intrinsic value, prioritising downside protection above all. He holds cash patiently until compelling opportunities arise.",
        "rules": {"numeric": {
            "trailingPE":                   {"max": 15},
            "priceToBook":                  {"max": 1.5},
            "priceToSalesTrailing12Months": {"max": 2.0},
            "debtToEquity":                 {"max": 50},
            "currentRatio":                 {"min": 1.5},
            "freeCashflow":                 {"min": 1},
            "profitMargins":                {"min": 5},
            "marketCap":                    {"min": 500_000_000},
        }, "categorical": {}},
    },
    "hmarks": {
        "name": "Howard Marks", "fund": "Oaktree Capital",
        "cik": "949509", "style": "Risk-Aware Value / Credit", "color": "#27ae60",
        "quote": "The most important thing is being aware of where we stand in the cycle — and acting accordingly.",
        "description": "Howard Marks is famous for his memos on market cycles and risk management. While Oaktree is primarily a credit investor, equity positions favour companies with durable cash flows, conservative balance sheets, and limited downside — bought when the cycle has driven them to unjustifiably low prices.",
        "rules": {"numeric": {
            "trailingPE":       {"max": 18},
            "priceToBook":      {"max": 2.0},
            "debtToEquity":     {"max": 80},
            "currentRatio":     {"min": 1.2},
            "profitMargins":    {"min": 8},
            "operatingMargins": {"min": 10},
            "beta":             {"max": 1.2},
            "freeCashflow":     {"min": 1},
        }, "categorical": {}},
    },
    "soros": {
        "name": "George Soros", "fund": "Soros Fund Management",
        "cik": "1029160", "style": "Global Macro / Reflexivity", "color": "#d35400",
        "quote": "It's not whether you're right or wrong — it's how much you make when right and how little you lose when wrong.",
        "description": "Soros pioneered the theory of reflexivity — market participants' biases feed back into fundamentals, creating self-reinforcing boom-bust cycles. His equity long book reflects large macro themes, favouring large-cap companies riding powerful global tailwinds with strong momentum characteristics.",
        "rules": {"numeric": {
            "marketCap":      {"min": 5_000_000_000},
            "revenueGrowth":  {"min": 5},
            "52WeekChange":   {"min": 0},
            "averageVolume":  {"min": 500_000},
            "beta":           {"min": 0.8},
            "returnOnEquity": {"min": 10},
        }, "categorical": {}},
    },
    "laffont": {
        "name": "Philippe Laffont", "fund": "Coatue Management",
        "cik": "1135730", "style": "Growth Technology", "color": "#16a085",
        "quote": "Technology is not just a sector — it is the operating system of the modern economy.",
        "description": "Laffont founded Coatue as a long/short technology-focused fund. He concentrates in platform businesses with powerful network effects, high gross margins, and dominant positions in large addressable markets — particularly software, internet, and semiconductors.",
        "rules": {"numeric": {
            "grossMargins":   {"min": 40},
            "revenueGrowth":  {"min": 15},
            "marketCap":      {"min": 1_000_000_000},
            "averageVolume":  {"min": 500_000},
            "returnOnEquity": {"min": 10},
            "forwardPE":      {"max": 60},
        }, "categorical": {"sector": ["Technology", "Communication Services"]}},
    },
    "kfisher": {
        "name": "Ken Fisher", "fund": "Fisher Asset Management",
        "cik": "850529", "max_holdings": 300, "style": "GARP / Global Large-Cap", "color": "#34495e",
        "quote": "The stock market is a discounting mechanism — it prices in what most people aren't yet thinking about.",
        "description": "Ken Fisher manages one of the world's largest independent RIA firms. His GARP approach (Growth at a Reasonable Price) targets large-cap global companies with strong earnings visibility, positive analyst revisions, and valuations reasonable relative to their growth rates.",
        "rules": {"numeric": {
            "trailingPE":     {"max": 30},
            "earningsGrowth": {"min": 5},
            "revenueGrowth":  {"min": 5},
            "marketCap":      {"min": 5_000_000_000},
            "returnOnEquity": {"min": 10},
            "profitMargins":  {"min": 8},
            "averageVolume":  {"min": 1_000_000},
        }, "categorical": {}},
    },
    "gerstner": {
        "name": "Brad Gerstner", "fund": "Altimeter Capital",
        "cik": "1543160", "style": "AI & Software Growth", "color": "#0097b2",
        "quote": "We are in the early innings of the AI revolution — the companies building the foundational layer will be the most valuable in history.",
        "description": "Gerstner runs high-conviction technology bets at Altimeter, known for early investments in Snowflake, ByteDance, and AI infrastructure. He focuses on software and cloud companies with strong gross margins, rapid revenue growth, and durable competitive moats driven by AI and network effects.",
        "rules": {"numeric": {
            "grossMargins":     {"min": 50},
            "revenueGrowth":    {"min": 20},
            "marketCap":        {"min": 1_000_000_000},
            "operatingMargins": {"min": 5},
            "returnOnEquity":   {"min": 15},
            "forwardPE":        {"max": 80},
            "averageVolume":    {"min": 500_000},
        }, "categorical": {"sector": ["Technology", "Communication Services"]}},
    },
}

# ── 13F Name-Matching Helpers ─────────────────────────────────────────────────

# Words to strip from company names (replaced with space so surrounding words don't merge)
_strip_words_re = re.compile(
    r'\b(INCORPORATED|INC|CORPORATION|CORP|LIMITED|LTD|COMPANY|COMPANIES|'
    r'CO|LLC|PLC|LP|NV|SA|AG|SE|SPA|'
    r'THE|OF|AND|'
    r'CL\s*[A-C]|CLASS\s+[A-C]|SHS|SHARES|ADR|ADS|'
    r'HOLDINGS?|HLDGS?|GROUP|'
    r'TECHNOLOGIES|TECHNOLOGY|TECH|'
    r'SYSTEMS|SOLUTIONS|SERVICES|SVCS|'
    r'COMMUNICATIONS?|'
    r'INTERNATIONAL|INTL|GLOBAL|'
    r'ENTERPRISES?|INDUSTRIES|PARTNERS|ASSOCIATES|'
    r'PHARMACEUTICALS?|PHARMA|'
    r'SWITZ|SWISS|BERMUDA|CAYMAN|IRELAND|MTN|NOTE|DEBENTURE)\b',
    re.IGNORECASE,
)
# Punctuation stripped with '' (empty) so "MOODY'S" → "MOODYS", not "MOODY S"
_strip_punct_re = re.compile(r'[^\w\s]')

# Expand common 13F abbreviations to full words used in standard company names
_13F_ABBREV = {
    'PETE':     'PETROLEUM',
    'PETROL':   'PETROLEUM',
    'MFG':      'MANUFACTURING',
    'NATL':     'NATIONAL',
    'AMER':     'AMERICAN',
    'FINL':     'FINANCIAL',
    'HLTH':     'HEALTH',
    'HLTHCARE': 'HEALTHCARE',
    'SEMICOND': 'SEMICONDUCTOR',
    'COMMUN':   'COMMUNICATIONS',
    'MGMT':     'MANAGEMENT',
    'SVCS':     'SERVICES',
    'SVC':      'SERVICE',
    'BANCORP':  'BANCORPORATION',
    'BANCSHS':  'BANCSHARES',
    'RES':      'RESOURCES',
    'ENGY':     'ENERGY',
    'ENGR':     'ENGINEERING',
    'ELEC':     'ELECTRIC',
    'GENL':     'GENERAL',
    'INTL':     '',
    'PHRM':     'PHARMACEUTICALS',
}

# Hard overrides for names that systematically differ between 13F and standard form
# Key = normalised 13F name, Value = ticker
_TICKER_OVERRIDES: dict[str, str] = {
    'SIRIUSXM': 'SIRI',
    'SIRIUS XM': 'SIRI',
    'MACYS': 'M',
    'MOODYS': 'MCO',
    'CHUBB': 'CB',
    'LOUISIANA PAC': 'LPX',
    'LOUISIANA PACIFIC': 'LPX',
    'NEW YORK TIMES': 'NYT',
    'ALLY FINANCIAL': 'ALLY',
    'ALLY FINL': 'ALLY',
    'JPMORGAN CHASE': 'JPM',
    'AMAZON COM': 'AMZN',
    'AMAZON': 'AMZN',
    'ALPHABET': 'GOOGL',
    'BERKSHIRE HATHAWAY': 'BRK-B',
    'TAIWAN SEMICONDUCTOR MANUFACTURING': 'TSM',
    'TAIWAN SEMICONDUCTR MANUFACTURING': 'TSM',
    # Additional overrides for Tepper / Tudor / Wood / Griffin holdings
    'META PLATFORMS': 'META',
    'NETFLIX': 'NFLX',
    'UBER': 'UBER',
    'AIRBNB': 'ABNB',
    'COINBASE': 'COIN',
    'ROKU': 'ROKU',
    'PALANTIR': 'PLTR',
    'PINTEREST': 'PINS',
    'ZOOM VIDEO': 'ZM',
    'ZOOM': 'ZM',
    'SHOPIFY': 'SHOP',
    'DOCUSIGN': 'DOCU',
    'TELADOC': 'TDOC',
    'UIPATH': 'PATH',
    'UNITY SOFTWARE': 'U',
    'TWILIO': 'TWLO',
    'SQUARE': 'SQ',
    'BLOCK': 'SQ',
    'ROBINHOOD': 'HOOD',
    'DRAFTKINGS': 'DKNG',
    'MICROSOFT': 'MSFT',
    'APPLE': 'AAPL',
    'NVIDIA': 'NVDA',
    'TESLA': 'TSLA',
    'BROADCOM': 'AVGO',
    'ELI LILLY': 'LLY',
    'UNITEDHEALTH': 'UNH',
    'JOHNSON JOHNSON': 'JNJ',
    'PROCTER GAMBLE': 'PG',
    'EXXON MOBIL': 'XOM',
    'CHEVRON': 'CVX',
    'WELLS FARGO': 'WFC',
    'BANK OF AMERICA': 'BAC',
    'CITIGROUP': 'C',
    'MORGAN STANLEY': 'MS',
    'VISA': 'V',
    'MASTERCARD': 'MA',
    'ASML HLDG': 'ASML',
    'WHIRLPOOL': 'WHR',
    'PALO ALTO': 'PANW',
    'SERVICENOW': 'NOW',
    'INTUIT': 'INTU',
    'SALESFORCE': 'CRM',
    'CROWDSTRIKE': 'CRWD',
    'DATADOG': 'DDOG',
    'SNOWFLAKE': 'SNOW',
    'WORKDAY': 'WDAY',
    'FORTINET': 'FTNT',
    'VEEVA': 'VEEV',
    'TWILIO': 'TWLO',
    'HUBSPOT': 'HUBS',
    'MONGODB': 'MDB',
    'GITLAB': 'GTLB',
    'CRISPR': 'CRSP',
    'EXACT SCIENCES': 'EXAS',
    'REGENERON': 'REGN',
    'MODERNA': 'MRNA',
    'GILEAD': 'GILD',
    'BIOGEN': 'BIIB',
    'VERTEX': 'VRTX',
    'ABBVIE': 'ABBV',
    'DEXCOM': 'DXCM',
    'INTUITIVE SURGICAL': 'ISRG',
}


def _norm(s: str) -> str:
    """Normalise a company name: uppercase, dots/hyphens→space,
    strip legal suffixes (→space) and punctuation (→''), expand abbreviations."""
    s = s.upper().replace('-', ' ').replace('.', ' ')   # dots & hyphens → spaces first
    s = _strip_words_re.sub(' ', s)    # legal suffixes & country codes → space
    s = _strip_punct_re.sub('', s)     # punctuation → '' (keeps MOODY'S as MOODYS)
    words = [_13F_ABBREV.get(w, w) for w in s.split()]
    return re.sub(r'\s+', ' ', ' '.join(w for w in words if w)).strip()


# Pre-compute normalized override keys so _match_ticker avoids re-normalizing on every call
_TICKER_OVERRIDES_NORM: dict[str, str] = {_norm(k): v for k, v in _TICKER_OVERRIDES.items()}
