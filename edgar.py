"""EDGAR fetching and all guru DB helper functions."""
import re
import time
import requests
import xml.etree.ElementTree as ET
import pandas as pd

import state
from helpers import clean
from db_config import get_engine
from config import (
    GURUS, _norm, _TICKER_OVERRIDES_NORM, _EDGAR_UA, _DB_GURU_TTL,
    _13F_ABBREV,
)

# ── Ticker name resolution ────────────────────────────────────────────────────
def build_name_to_ticker() -> dict:
    result: dict = {}
    for tk, row in state.df.iterrows():
        for f in ('shortName', 'longName'):
            v = row.get(f)
            if pd.notna(v):
                n = _norm(str(v))
                if n:
                    result[n] = tk
    return result


def _match_ticker(issuer: str) -> str | None:
    n = _norm(issuer)
    for ok, ticker in _TICKER_OVERRIDES_NORM.items():
        if n == ok or n.startswith(ok):
            return ticker
    if n in state._name_to_ticker:
        return state._name_to_ticker[n]
    for k, v in state._name_to_ticker.items():
        plen = min(len(n), len(k))
        if plen >= 6 and n[:plen] == k[:plen]:
            return v
    n_words = set(n.split())
    if len(n_words) >= 1:
        best_score, best_tk = 0.0, None
        for k, v in state._name_to_ticker.items():
            k_words = set(k.split())
            if not k_words:
                continue
            inter = len(n_words & k_words)
            if inter == 0:
                continue
            score = inter / max(len(n_words), len(k_words))
            if score > best_score:
                best_score, best_tk = score, v
        if best_score >= 0.60:
            return best_tk
    return None

# ── Guru rules DB ─────────────────────────────────────────────────────────────
def save_guru_rules_to_db():
    import json as _json
    from sqlalchemy import text as sql_text
    try:
        engine = get_engine()
        with engine.connect() as conn:
            for slug, g in GURUS.items():
                rules = g.get("rules", {})
                for field_id, bounds in rules.get("numeric", {}).items():
                    for rule_type, val in bounds.items():
                        conn.execute(sql_text("""
                            INSERT INTO guru_rules (slug, field_id, rule_type, num_value)
                            VALUES (:slug, :fid, :rt, :val)
                            ON DUPLICATE KEY UPDATE num_value=VALUES(num_value), is_active=1,
                                                    updated_at=CURRENT_TIMESTAMP
                        """), {"slug": slug, "fid": field_id, "rt": rule_type, "val": float(val)})
                for field_id, vals in rules.get("categorical", {}).items():
                    conn.execute(sql_text("""
                        INSERT INTO guru_rules (slug, field_id, rule_type, cat_values)
                        VALUES (:slug, :fid, 'categorical', :cv)
                        ON DUPLICATE KEY UPDATE cat_values=VALUES(cat_values), is_active=1,
                                                updated_at=CURRENT_TIMESTAMP
                    """), {"slug": slug, "fid": field_id, "cv": _json.dumps(vals)})
            conn.commit()
    except Exception as e:
        print(f"Could not seed guru rules to DB: {e}")


def load_guru_rules_from_db() -> dict:
    import json as _json
    from sqlalchemy import text as sql_text
    result: dict = {}
    try:
        engine = get_engine()
        with engine.connect() as conn:
            rows = conn.execute(sql_text(
                "SELECT slug, field_id, rule_type, num_value, cat_values "
                "FROM guru_rules WHERE is_active=1"
            )).fetchall()
        for r in rows:
            slug = r.slug
            if slug not in result:
                result[slug] = {"numeric": {}, "categorical": {}}
            if r.rule_type in ("min", "max"):
                existing = result[slug]["numeric"].get(r.field_id, {})
                existing[r.rule_type] = r.num_value
                result[slug]["numeric"][r.field_id] = existing
            elif r.rule_type == "categorical":
                try:
                    result[slug]["categorical"][r.field_id] = _json.loads(r.cat_values or "[]")
                except Exception:
                    pass
    except Exception as e:
        print(f"Could not load guru rules from DB: {e}")
    return result

# ── Holdings DB ───────────────────────────────────────────────────────────────
def _db_save_holdings(slug: str, filing_date: str, holdings: list, source: str = '13f'):
    from sqlalchemy import text as sql_text
    from datetime import datetime, timezone
    try:
        engine = get_engine()
        with engine.connect() as conn:
            conn.execute(sql_text("""
                INSERT INTO guru_filing_meta (slug, filing_date, fetched_at, source)
                VALUES (:slug, :fd, :now, :src)
                ON DUPLICATE KEY UPDATE fetched_at=VALUES(fetched_at), source=VALUES(source)
            """), {"slug": slug, "fd": filing_date,
                   "now": datetime.now(timezone.utc).replace(tzinfo=None), "src": source})
            conn.execute(sql_text(
                "DELETE FROM guru_holdings WHERE slug=:slug AND filing_date=:fd"
            ), {"slug": slug, "fd": filing_date})
            for h in holdings:
                conn.execute(sql_text("""
                    INSERT INTO guru_holdings
                        (slug, filing_date, name, cusip, ticker, put_call, value, shares, weight, change_tag)
                    VALUES (:slug, :fd, :name, :cusip, :ticker, :pc, :val, :shr, :wgt, :chg)
                """), {
                    "slug": slug, "fd": filing_date,
                    "name":   h.get("name", ""),    "cusip":  h.get("cusip") or None,
                    "ticker": h.get("ticker") or None, "pc":  h.get("put_call", ""),
                    "val":    int(h.get("value", 0)), "shr":  int(h.get("shares", 0)),
                    "wgt":    h.get("weight"),        "chg":  h.get("change") or None,
                })
            all_dates = conn.execute(sql_text("""
                SELECT filing_date FROM guru_filing_meta
                WHERE slug=:slug ORDER BY filing_date DESC
            """), {"slug": slug}).fetchall()
            for row in all_dates[2:]:
                old_fd = str(row[0])
                conn.execute(sql_text(
                    "DELETE FROM guru_holdings WHERE slug=:slug AND filing_date=:fd"
                ), {"slug": slug, "fd": old_fd})
                conn.execute(sql_text(
                    "DELETE FROM guru_filing_meta WHERE slug=:slug AND filing_date=:fd"
                ), {"slug": slug, "fd": old_fd})
            conn.commit()
    except Exception as e:
        print(f"DB save holdings failed [{slug}]: {e}")


def _db_load_holdings(slug: str) -> tuple[list | None, str | None]:
    from sqlalchemy import text as sql_text
    from datetime import datetime, timezone
    try:
        engine = get_engine()
        with engine.connect() as conn:
            meta = conn.execute(sql_text("""
                SELECT filing_date, fetched_at FROM guru_filing_meta
                WHERE slug=:slug ORDER BY filing_date DESC LIMIT 1
            """), {"slug": slug}).fetchone()
            if not meta:
                return None, None
            filing_date, fetched_at = meta
            if (datetime.now(timezone.utc).replace(tzinfo=None) - fetched_at).total_seconds() > _DB_GURU_TTL:
                return None, None
            rows = conn.execute(sql_text("""
                SELECT name, cusip, ticker, put_call, value, shares, weight, change_tag
                FROM guru_holdings WHERE slug=:slug AND filing_date=:fd ORDER BY value DESC
            """), {"slug": slug, "fd": str(filing_date)}).fetchall()
        holdings = [
            {"name": r.name, "cusip": r.cusip or "", "ticker": r.ticker or "",
             "put_call": r.put_call or "", "value": r.value, "shares": r.shares,
             "weight": r.weight, "change": r.change_tag or ""}
            for r in rows
        ]
        return holdings, str(filing_date)
    except Exception as e:
        print(f"DB load holdings failed [{slug}]: {e}")
        return None, None


def _db_load_prev_for_diff(slug: str, current_date: str) -> dict:
    from sqlalchemy import text as sql_text
    try:
        engine = get_engine()
        with engine.connect() as conn:
            prev = conn.execute(sql_text("""
                SELECT filing_date FROM guru_filing_meta
                WHERE slug=:slug AND filing_date < :cd ORDER BY filing_date DESC LIMIT 1
            """), {"slug": slug, "cd": current_date}).fetchone()
            if not prev:
                return {}
            rows = conn.execute(sql_text("""
                SELECT ticker, put_call, value FROM guru_holdings
                WHERE slug=:slug AND filing_date=:fd
            """), {"slug": slug, "fd": str(prev[0])}).fetchall()
        return {f"{r.ticker or ''}|{r.put_call or ''}": r.value for r in rows}
    except Exception as e:
        print(f"DB load prev holdings failed [{slug}]: {e}")
        return {}


def _enrich_holdings(raw: list, filing_date: str | None) -> list:
    total_val = sum(h["value"] for h in raw) or 1
    enriched = []
    for h in raw:
        tk = h.get("ticker") or None
        extra: dict = {}
        if tk and tk in state.df.index:
            row = state.df.loc[tk]
            extra = {
                "longName":          clean(row.get("longName")),
                "sector":            clean(row.get("sector")),
                "currentPrice":      clean(row.get("currentPrice")),
                "marketCap":         clean(row.get("marketCap")),
                "trailingPE":        clean(row.get("trailingPE")),
                "returnOnEquity":    clean(row.get("returnOnEquity")),
                "profitMargins":     clean(row.get("profitMargins")),
                "revenueGrowth":     clean(row.get("revenueGrowth")),
                "52WeekChange":      clean(row.get("52WeekChange")),
                "recommendationKey": clean(row.get("recommendationKey")),
            }
        weight = h.get("weight") if h.get("weight") is not None else round(h["value"] / total_val * 100, 2)
        enriched.append({**h, "weight": weight, **extra})
    return enriched

# ── EDGAR HTTP ─────────────────────────────────────────────────────────────────
def _edgar_get(url: str, timeout: int = 90) -> requests.Response:
    for attempt in range(3):
        try:
            return requests.get(url, headers=_EDGAR_UA, timeout=timeout, verify=False)
        except Exception:
            if attempt == 2:
                raise
            time.sleep(2 ** attempt)


def _dedup_rows(rows: list) -> list:
    merged: dict = {}
    for r in rows:
        key = (_norm(r['name']), r.get('put_call', ''))
        if key in merged:
            merged[key]['value']  += r['value']
            merged[key]['shares'] += r['shares']
        else:
            merged[key] = dict(r)
    return sorted(merged.values(), key=lambda x: x['value'], reverse=True)


def _parse_info_table(text: str) -> list:
    def _apply_multiplier(rows):
        if not rows:
            return rows
        max_raw = max(r['value'] for r in rows)
        mult = 1 if max_raw >= 100_000_000 else 1000
        for r in rows:
            r['value'] = r['value'] * mult
        return rows

    try:
        clean_xml = re.sub(r'<\?xml[^?]*\?>', '', text).strip()
        root = ET.fromstring(clean_xml)
        ns = (root.tag.split('}')[0] + '}') if root.tag.startswith('{') else ''
        items = list(root.iter(f'{ns}infoTable')) or list(root.iter('infoTable'))
        rows = []
        for info in items:
            def _gt(tag):
                return (info.findtext(f'{ns}{tag}') or info.findtext(tag) or '').strip()
            val_str = _gt('value').replace(',', '')
            try:
                raw_value = int(float(val_str))
            except ValueError:
                raw_value = 0
            shrs_el = info.find(f'{ns}shrsOrPrnAmt')
            if shrs_el is None:
                shrs_el = info.find('shrsOrPrnAmt')
            shares = 0
            if shrs_el is not None:
                try:
                    shares = int(float((shrs_el.findtext(f'{ns}sshPrnamt') or
                                        shrs_el.findtext('sshPrnamt') or '0').replace(',', '')))
                except ValueError:
                    shares = 0
            name = _gt('nameOfIssuer')
            put_call = _gt('putCall').upper()
            if name and raw_value > 0:
                rows.append({'name': name, 'cusip': _gt('cusip'),
                             'value': raw_value, 'shares': shares, 'put_call': put_call})
        if rows:
            return _dedup_rows(_apply_multiplier(rows))
    except Exception:
        pass

    pat = re.compile(
        r'<nameOfIssuer[^>]*>([^<]+)</nameOfIssuer>.*?'
        r'<cusip[^>]*>([^<]+)</cusip>.*?<value[^>]*>([^<]+)</value>.*?'
        r'<sshPrnamt[^>]*>([^<]+)</sshPrnamt>', re.DOTALL | re.IGNORECASE)
    rows = []
    for m in pat.finditer(text):
        try:
            raw_value = int(float(m.group(3).replace(',', '')))
            shares = int(float(m.group(4).replace(',', '')))
        except ValueError:
            continue
        if raw_value > 0:
            ctx = text[max(0, m.start() - 300): m.end() + 300]
            pc_m = re.search(r'<putCall[^>]*>([^<]*)</putCall>', ctx, re.IGNORECASE)
            put_call = pc_m.group(1).strip().upper() if pc_m else ''
            rows.append({'name': m.group(1).strip(), 'cusip': m.group(2).strip(),
                         'value': raw_value, 'shares': shares, 'put_call': put_call})
    return _dedup_rows(_apply_multiplier(rows))

# ── 13F / NPORT fetchers ───────────────────────────────────────────────────────
def _fetch_13f(slug: str) -> tuple[list, str | None]:
    cached = state._holdings_cache.get(slug)
    if cached and (time.time() - cached[2]) < 3600:
        return cached[0], cached[1]
    raw_db, filing_date_db = _db_load_holdings(slug)
    if raw_db is not None:
        enriched = _enrich_holdings(raw_db, filing_date_db)
        state._holdings_cache[slug] = (enriched, filing_date_db, time.time())
        return enriched, filing_date_db

    guru = GURUS[slug]
    cik = guru['cik']
    cik_padded = cik.zfill(10)
    try:
        sub = _edgar_get(f"https://data.sec.gov/submissions/CIK{cik_padded}.json").json()
        recent = sub['filings']['recent']
        accession = filing_date = None
        for i, form in enumerate(recent['form']):
            if form in ('13F-HR', '13F-HR/A'):
                accession = recent['accessionNumber'][i]
                filing_date = recent['filingDate'][i]
                break
        if not accession:
            return [], None

        acc_nd = accession.replace('-', '')
        base_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{acc_nd}"
        dir_html = _edgar_get(f"{base_url}/").text
        all_links = re.findall(r'href="(/Archives/edgar/data/[^"]+\.(xml|htm|html))"',
                               dir_html, re.I)
        doc_name = None
        xml_links = [l[0].split('/')[-1] for l in all_links if l[0].endswith('.xml')]
        for nm in xml_links:
            if nm.lower() != 'primary_doc.xml':
                doc_name = nm
                break
        if not doc_name and xml_links:
            doc_name = xml_links[0]
        if not doc_name:
            return [], filing_date

        doc_text = _edgar_get(f"{base_url}/{doc_name}").text
        raw = _dedup_rows(_parse_info_table(doc_text))
        raw = raw[:guru.get('max_holdings', 500)]
        total_val = sum(h['value'] for h in raw) or 1
        prev_vals = _db_load_prev_for_diff(slug, filing_date)

        raw_with_meta = []
        for h in raw:
            tk = _match_ticker(h['name'])
            put_call = h.get('put_call', '')
            prev_key = f"{tk or ''}|{put_call}"
            if not prev_vals:
                change = ''
            elif prev_key not in prev_vals:
                change = 'new'
            elif h['value'] > prev_vals[prev_key] * 1.005:
                change = 'added'
            elif h['value'] < prev_vals[prev_key] * 0.995:
                change = 'reduced'
            else:
                change = 'held'
            raw_with_meta.append({**h, 'ticker': tk, 'put_call': put_call, 'change': change,
                                   'weight': round(h['value'] / total_val * 100, 2)})

        _db_save_holdings(slug, filing_date, raw_with_meta)
        enriched = _enrich_holdings(raw_with_meta, filing_date)
        state._holdings_cache[slug] = (enriched, filing_date, time.time())
        return enriched, filing_date
    except Exception as ex:
        print(f"13F fetch error [{slug}]: {ex}")
        return [], None


def _fetch_nport(slug: str) -> tuple[list, str | None]:
    cached = state._holdings_cache.get(slug)
    if cached and (time.time() - cached[2]) < 3600:
        return cached[0], cached[1]
    raw_db, filing_date_db = _db_load_holdings(slug)
    if raw_db is not None:
        enriched = _enrich_holdings(raw_db, filing_date_db)
        state._holdings_cache[slug] = (enriched, filing_date_db, time.time())
        return enriched, filing_date_db

    guru = GURUS[slug]
    cik = guru['cik']
    series_id = guru['series_id']
    cik_padded = cik.zfill(10)
    try:
        sub = _edgar_get(f"https://data.sec.gov/submissions/CIK{cik_padded}.json").json()
        recent = sub['filings']['recent']
        accession = filing_date = doc_text = None
        for i, form in enumerate(recent['form']):
            if form != 'NPORT-P':
                continue
            acc = recent['accessionNumber'][i]
            acc_nd = acc.replace('-', '')
            r = _edgar_get(f"https://www.sec.gov/Archives/edgar/data/{cik}/{acc_nd}/primary_doc.xml")
            if series_id in r.text:
                accession = acc
                filing_date = recent['filingDate'][i]
                doc_text = r.text
                break
        if not accession:
            return [], None

        ns = {'n': 'http://www.sec.gov/edgar/nport'}
        root = ET.fromstring(doc_text)
        rd = root.find('.//n:repPdDate', ns)
        rep_date = rd.text if rd is not None else None

        raw_holdings = []
        for sec in root.findall('.//n:invstOrSec', ns):
            name_el   = sec.find('n:name', ns)
            val_el    = sec.find('n:valUSD', ns)
            pct_el    = sec.find('n:pctVal', ns)
            shares_el = sec.find('n:balance', ns)
            ticker_el = sec.find('.//n:ticker', ns)
            name   = name_el.text          if name_el   is not None else ''
            val    = float(val_el.text)    if val_el    is not None else 0.0
            pct    = float(pct_el.text)    if pct_el    is not None else 0.0
            shares = float(shares_el.text) if shares_el is not None else 0.0
            tk     = ticker_el.attrib.get('value') if ticker_el is not None else None
            raw_holdings.append({'name': name, 'value': int(val), 'shares': int(shares),
                                  'weight': round(pct, 2), 'ticker': tk or '', 'put_call': ''})

        final_date = rep_date or filing_date
        raw_sorted = sorted(raw_holdings, key=lambda x: x['value'], reverse=True)
        _db_save_holdings(slug, final_date, raw_sorted, source='nport')
        enriched = _enrich_holdings(raw_sorted, final_date)
        state._holdings_cache[slug] = (enriched, final_date, time.time())
        return enriched, final_date
    except Exception as ex:
        print(f"NPORT fetch error [{slug}]: {ex}")
        return [], None
