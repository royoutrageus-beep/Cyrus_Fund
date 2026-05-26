import ccxt
import pandas as pd
import streamlit as st
import requests
import numpy as np
import pytz
import time
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

# ════════════════════════════════════════════════════
#  CONFIG
# ════════════════════════════════════════════════════
TOKEN   = st.secrets.get("TELEGRAM_TOKEN", "")
CHAT_ID = st.secrets.get("TELEGRAM_CHAT_ID", "")
WIB     = pytz.timezone("Asia/Jakarta")

for _k, _v in [
    ("scan_results", []), ("wl_results", []),
    ("last_scan_time", None), ("last_scan_mode", "Scalping ⚡"),
    ("active_scan_mode", "Scalping ⚡"), ("active_tf", "15m"),
    ("bt_results_data", None),
]:
    if _k not in st.session_state: st.session_state[_k] = _v

st.set_page_config(layout="wide", page_title="Crypto Turbo", page_icon="🚀",
                   initial_sidebar_state="collapsed")

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Space+Mono:wght@400;700&family=Syne:wght@400;600;800&display=swap');
:root{
  --bg:#080c10;--surface:#0d1117;--border:#1c2533;
  --accent:#00e5ff;--green:#00ff88;--red:#ff3d5a;
  --amber:#ffb700;--purple:#bf5fff;--orange:#ff7b00;
  --muted:#4a5568;--text:#c9d1d9;--heading:#e6edf3;
  --btc:#f7931a;--eth:#627eea;
}
html,body,[data-testid="stAppViewContainer"]{background:var(--bg)!important;color:var(--text)!important;font-family:'Syne',sans-serif;}
#MainMenu,footer,header{visibility:hidden;}
[data-testid="stSidebar"]{display:none!important;}
[data-testid="stExpander"]{background:var(--surface)!important;border:1px solid var(--border)!important;border-radius:8px!important;margin-bottom:12px!important;}
[data-testid="stExpander"] summary{font-family:'Space Mono',monospace!important;font-size:12px!important;color:var(--accent)!important;}
.ct-header{display:flex;align-items:center;padding:16px 0 12px;border-bottom:1px solid var(--border);margin-bottom:16px;}
.ct-logo{font-family:'Space Mono',monospace;font-size:22px;font-weight:700;color:var(--btc);letter-spacing:-1px;}
.ct-sub{font-size:11px;color:var(--muted);letter-spacing:2px;text-transform:uppercase;}
.live-badge{display:inline-flex;align-items:center;gap:6px;padding:4px 12px;background:rgba(0,229,255,.08);border:1px solid rgba(0,229,255,.3);border-radius:20px;font-family:'Space Mono',monospace;font-size:10px;color:var(--accent);letter-spacing:1px;margin-left:auto;}
.live-dot{width:6px;height:6px;background:var(--green);border-radius:50%;animation:blink 1s infinite;}
@keyframes blink{0%,100%{opacity:1;}50%{opacity:.2;}}
.metric-row{display:flex;gap:10px;margin-bottom:18px;flex-wrap:wrap;}
.metric-card{flex:1;min-width:110px;background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:12px 14px;position:relative;overflow:hidden;}
.metric-card::before{content:'';position:absolute;top:0;left:0;right:0;height:2px;background:var(--accent);}
.metric-card.green::before{background:var(--green);}
.metric-card.red::before{background:var(--red);}
.metric-card.amber::before{background:var(--amber);}
.metric-card.orange::before{background:var(--orange);}
.metric-card.purple::before{background:var(--purple);}
.metric-card.btc::before{background:var(--btc);}
.metric-label{font-size:10px;color:var(--muted);letter-spacing:1.5px;text-transform:uppercase;margin-bottom:4px;}
.metric-value{font-family:'Space Mono',monospace;font-size:22px;font-weight:700;color:var(--heading);line-height:1;}
.metric-sub{font-size:10px;color:var(--muted);margin-top:3px;}
.signal-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(260px,1fr));gap:12px;margin-bottom:20px;}
.signal-card{background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:16px;position:relative;overflow:hidden;transition:border-color .2s;}
.signal-card.gacor{border-color:rgba(0,255,136,.4);background:rgba(0,255,136,.03);}
.signal-card.potensial{border-color:rgba(255,183,0,.3);background:rgba(255,183,0,.03);}
.signal-card.watch{border-color:rgba(0,229,255,.2);}
.signal-card.bagger{border-color:rgba(191,95,255,.6);background:rgba(191,95,255,.05);box-shadow:0 0 20px rgba(191,95,255,.15);}
.signal-card::after{content:'';position:absolute;top:0;left:0;width:4px;height:100%;}
.signal-card.gacor::after{background:var(--green);}
.signal-card.potensial::after{background:var(--amber);}
.signal-card.watch::after{background:var(--accent);}
.signal-card.bagger::after{background:var(--purple);}
.sc-ticker{font-family:'Space Mono',monospace;font-size:16px;font-weight:700;color:var(--heading);}
.sc-price{font-family:'Space Mono',monospace;font-size:12px;color:var(--muted);}
.sc-signal{font-size:13px;font-weight:700;margin:6px 0;}
.sc-bars{display:flex;gap:3px;margin:8px 0;}
.sc-bar{height:14px;border-radius:2px;}
.sc-bar.filled{background:var(--green);}
.sc-bar.filled-purple{background:var(--purple);}
.sc-bar.empty{background:var(--border);}
.sc-stats{display:flex;gap:10px;flex-wrap:wrap;margin-top:8px;}
.sc-stat{font-family:'Space Mono',monospace;font-size:10px;color:var(--muted);}
.sc-stat span{color:var(--text);}
.alert-box{background:rgba(255,61,90,.06);border:1px solid rgba(255,61,90,.4);border-radius:8px;padding:14px 18px;margin-bottom:16px;animation:pulse-r 2s infinite;}
.bagger-alert-box{background:rgba(191,95,255,.06);border:1px solid rgba(191,95,255,.5);border-radius:8px;padding:14px 18px;margin-bottom:16px;animation:pulse-p 2s infinite;}
@keyframes pulse-r{0%,100%{border-color:rgba(255,61,90,.4);}50%{border-color:rgba(255,61,90,.9);}}
@keyframes pulse-p{0%,100%{border-color:rgba(191,95,255,.4);}50%{border-color:rgba(191,95,255,.9);}}
.alert-title{color:var(--red);font-family:'Space Mono',monospace;font-size:12px;font-weight:700;letter-spacing:2px;}
.bagger-title{color:var(--purple);font-family:'Space Mono',monospace;font-size:12px;font-weight:700;letter-spacing:2px;}
.tape-wrap{overflow:hidden;white-space:nowrap;border-top:1px solid var(--border);border-bottom:1px solid var(--border);padding:5px 0;margin-bottom:16px;background:var(--surface);}
.tape-inner{display:inline-block;animation:marquee 40s linear infinite;}
@keyframes marquee{0%{transform:translateX(0)}100%{transform:translateX(-50%)}}
.tape-item{display:inline-block;margin:0 18px;font-family:'Space Mono',monospace;font-size:10px;}
.tape-item.up{color:var(--green);}.tape-item.down{color:var(--red);}.tape-item.flat{color:var(--muted);}.tape-item.bagger{color:var(--purple);}
.section-title{font-family:'Space Mono',monospace;font-size:11px;color:var(--muted);letter-spacing:2px;text-transform:uppercase;border-left:3px solid var(--orange);padding-left:10px;margin:20px 0 10px 0;}
.settings-label{font-family:'Space Mono',monospace;font-size:10px;color:var(--muted);letter-spacing:2px;margin-bottom:10px;padding-bottom:6px;border-bottom:1px solid var(--border);}
.fr-long{color:var(--green);font-family:'Space Mono',monospace;font-size:11px;}
.fr-short{color:var(--red);font-family:'Space Mono',monospace;font-size:11px;}
[data-testid="stDataFrame"]{border:1px solid var(--border)!important;border-radius:8px!important;}
button[data-testid="baseButton-primary"]{background:var(--orange)!important;color:var(--bg)!important;font-family:'Space Mono',monospace!important;font-weight:700!important;border:none!important;}
::-webkit-scrollbar{width:4px;height:4px;}::-webkit-scrollbar-track{background:var(--bg);}::-webkit-scrollbar-thumb{background:var(--border);border-radius:2px;}
@media(max-width:768px){.main .block-container{padding-left:.75rem!important;padding-right:.75rem!important;}.signal-grid{grid-template-columns:1fr;}}
</style>
""", unsafe_allow_html=True)

# ════════════════════════════════════════════════════
#  INDODAX — CCXT CLIENT (singleton di session)
# ════════════════════════════════════════════════════
@st.cache_resource
def get_exchange():
    ex = ccxt.indodax({
        "enableRateLimit": True,
        "timeout": 15000,
    })
    return ex

@st.cache_resource
def get_binance():
    return ccxt.binance({"enableRateLimit": True, "timeout": 10000})

# ════════════════════════════════════════════════════
#  PAIR LIST — semua IDR pairs dari Indodax
# ════════════════════════════════════════════════════
@st.cache_data(ttl=3600)
def get_idr_pairs():
    try:
        ex = get_exchange()
        markets = ex.load_markets()
        pairs = sorted([s for s in markets if s.endswith("/IDR")])
        # Filter: minimum ada base currency IDR
        return pairs
    except Exception as e:
        # Fallback list kalau API gagal
        return [
            "BTC/IDR","ETH/IDR","BNB/IDR","SOL/IDR","XRP/IDR","ADA/IDR",
            "DOGE/IDR","AVAX/IDR","DOT/IDR","MATIC/IDR","LINK/IDR","UNI/IDR",
            "LTC/IDR","BCH/IDR","ATOM/IDR","FIL/IDR","ETC/IDR","XLM/IDR",
            "VET/IDR","ALGO/IDR","ICP/IDR","SAND/IDR","MANA/IDR","AXS/IDR",
            "NEAR/IDR","FTM/IDR","ONE/IDR","ENJ/IDR","CHZ/IDR","THETA/IDR",
            "TRX/IDR","EOS/IDR","ZEC/IDR","DASH/IDR","XMR/IDR","WAVES/IDR",
            "AAVE/IDR","COMP/IDR","MKR/IDR","SNX/IDR","YFI/IDR","SUSHI/IDR",
            "CRV/IDR","1INCH/IDR","BAL/IDR","REN/IDR","UMA/IDR","ZRX/IDR",
            "OMG/IDR","BAT/IDR","KNC/IDR","BAND/IDR","SKL/IDR","STORJ/IDR",
        ]

# ════════════════════════════════════════════════════
#  OHLCV FETCH — Indodax via CCXT
# ════════════════════════════════════════════════════
@st.cache_data(ttl=300)
def fetch_ohlcv_pair(symbol: str, timeframe: str = "15m", limit: int = 200):
    """Fetch OHLCV satu pair dari Indodax."""
    try:
        ex = get_exchange()
        raw = ex.fetch_ohlcv(symbol, timeframe, limit=limit)
        if not raw or len(raw) < 10:
            return None
        df = pd.DataFrame(raw, columns=["ts","Open","High","Low","Close","Volume"])
        df["ts"] = pd.to_datetime(df["ts"], unit="ms", utc=True).dt.tz_convert(WIB)
        df.set_index("ts", inplace=True)
        df = df.astype(float)
        return df
    except:
        return None

@st.cache_data(ttl=300)
def fetch_batch_ohlcv(pairs: tuple, timeframe: str = "15m", limit: int = 200):
    """Batch fetch semua pairs — rate-limited."""
    result = {}
    ex = get_exchange()
    for symbol in pairs:
        try:
            raw = ex.fetch_ohlcv(symbol, timeframe, limit=limit)
            if raw and len(raw) >= 20:
                df = pd.DataFrame(raw, columns=["ts","Open","High","Low","Close","Volume"])
                df["ts"] = pd.to_datetime(df["ts"], unit="ms", utc=True).dt.tz_convert(WIB)
                df.set_index("ts", inplace=True)
                df = df.astype(float)
                result[symbol] = df
        except:
            pass
        time.sleep(ex.rateLimit / 1000)
    return result

# ════════════════════════════════════════════════════
#  BTC REGIME — ganti IHSG
# ════════════════════════════════════════════════════
@st.cache_data(ttl=300)
def get_btc_regime():
    """
    Regime detector berbasis BTC/IDR daily.
    GREEN  = BTC > EMA20 & EMA55 → Altseason bullish
    SIDEWAYS = BTC antara EMA20/55 → Selektif
    RED    = BTC < EMA20 → Risk-off, altcoin bleeding
    """
    try:
        df = fetch_ohlcv_pair("BTC/IDR", "1d", limit=80)
        if df is None or len(df) < 20:
            return ("UNKNOWN", 0, 0, 0, "Data kurang", 0.0)
        close = df["Close"]
        ema20 = float(close.ewm(span=20, adjust=False).mean().iloc[-1])
        ema55 = float(close.ewm(span=min(55, len(close)-1), adjust=False).mean().iloc[-1])
        price = float(close.iloc[-1])
        chg   = float((close.iloc[-1] - close.iloc[-2]) / close.iloc[-2] * 100)

        band         = 0.015   # ±1.5% (crypto lebih volatile dari saham)
        pct_vs_e20   = (price - ema20) / ema20 * 100
        above_e20    = price > ema20 * (1 - band)
        above_e20_cl = price > ema20 * (1 + band)
        above_e55    = price > ema55
        recovering   = chg > 0.5

        if above_e20_cl and above_e55:
            regime = "GREEN"
            detail = f"BTC {price:,.0f} > EMA20 & EMA55 → Bullish ✅ ({pct_vs_e20:+.1f}%)"
        elif above_e20 and above_e55:
            regime = "GREEN"
            detail = f"BTC {price:,.0f} dekat EMA20 & > EMA55 → Bullish"
        elif above_e20 and not above_e55:
            regime = "SIDEWAYS"
            detail = f"BTC > EMA20 tapi < EMA55 → Sideways"
        elif not above_e20 and recovering:
            regime = "SIDEWAYS"
            detail = f"BTC recovery {chg:+.2f}% (EMA20={ema20:,.0f}, gap {pct_vs_e20:+.1f}%)"
        elif chg < -0.5 and not above_e20:
            regime = "RED"
            detail = f"BTC {price:,.0f} < EMA20 + turun {chg:.2f}% → Bearish"
        else:
            regime = "SIDEWAYS"
            detail = f"BTC {price:,.0f} sedikit < EMA20 → Sideways"

        return (regime, price, ema20, ema55, detail, chg)
    except Exception as e:
        return ("UNKNOWN", 0, 0, 0, f"BTC error: {str(e)[:40]}", 0.0)

def get_regime_config(regime):
    return {
        "GREEN": {
            "mode":"Momentum 🚀","min_score":4,"min_rvol":1.5,"sl_mult":0.8,
            "label":"🟢 BTC BULLISH — Momentum & Bagger Hunt",
            "color":"#00ff88",
            "desc":"BTC trending up. Altcoin momentum & akumulasi Wyckoff optimal."
        },
        "RED": {
            "mode":"Reversal 🎯","min_score":5,"min_rvol":2.0,"sl_mult":0.6,
            "label":"🔴 BTC BEARISH — Reversal Oversold Only",
            "color":"#ff3d5a",
            "desc":"BTC downtrend. Crypto SPOT: filter ketat, hanya reversal extreme OS."
        },
        "SIDEWAYS": {
            "mode":"Scalping ⚡","min_score":4,"min_rvol":2.0,"sl_mult":0.7,
            "label":"🟡 BTC SIDEWAYS — Scalping RVOL ≥ 2x",
            "color":"#ffb700",
            "desc":"BTC konsolidasi. Fokus scalping dengan volume surge."
        },
        "UNKNOWN": {
            "mode":"Scalping ⚡","min_score":4,"min_rvol":1.5,"sl_mult":0.8,
            "label":"⚪ REGIME UNKNOWN — Manual Mode",
            "color":"#4a5568","desc":"Data BTC tidak tersedia."
        },
    }.get(regime, {"mode":"Scalping ⚡","min_score":4,"min_rvol":1.5,"sl_mult":0.8,
                   "label":"⚪ UNKNOWN","color":"#4a5568","desc":""})

# ════════════════════════════════════════════════════
#  FUNDING RATE — Binance (referensi sentiment saja)
#  SPOT only → funding rate = market sentiment, bukan biaya
# ════════════════════════════════════════════════════
@st.cache_data(ttl=600)
def get_funding_rates(symbols=("BTC/USDT:USDT","ETH/USDT:USDT","SOL/USDT:USDT",
                               "BNB/USDT:USDT","XRP/USDT:USDT")):
    """
    Funding rate dari Binance Perpetual.
    Untuk SPOT: dipakai sebagai SENTIMENT indicator.
    FR tinggi (+) = market terlalu long → potensi flush → hati-hati beli
    FR negatif (-) = banyak yang short → potential short squeeze → peluang long
    """
    try:
        bn = get_binance()
        rates = {}
        for sym in symbols:
            try:
                fr = bn.fetch_funding_rate(sym)
                coin = sym.split("/")[0]
                rates[coin] = {
                    "rate": float(fr.get("fundingRate", 0)) * 100,
                    "next": fr.get("nextFundingTime", 0),
                }
            except:
                pass
        return rates
    except:
        return {}

def fr_label(rate_pct):
    """Interpret funding rate untuk SPOT trader."""
    if rate_pct > 0.1:    return "🔴 Longs dominant — HATI-HATI beli", "#ff3d5a"
    elif rate_pct > 0.05: return "🟡 Sedikit bias long", "#ffb700"
    elif rate_pct < -0.05:return "🟢 Short squeeze potensi — peluang long!", "#00ff88"
    else:                  return "⚪ Netral", "#4a5568"

# ════════════════════════════════════════════════════
#  BTC DOMINANCE — altcoin season indicator
# ════════════════════════════════════════════════════
@st.cache_data(ttl=1800)
def get_btc_dominance():
    """BTC Dominance dari CoinGecko (free, no API key)."""
    try:
        r = requests.get(
            "https://api.coingecko.com/api/v3/global",
            timeout=8, headers={"Accept":"application/json"}
        )
        data = r.json()["data"]
        dom  = data["market_cap_percentage"]["btc"]
        alt  = 100 - dom
        if dom < 42:   label = "🔥 ALTSEASON — Altcoin outperform BTC"
        elif dom < 50: label = "⚡ Transisi — Altcoin mulai naik"
        elif dom < 58: label = "⚪ Netral — BTC dominan"
        else:          label = "🔴 BTC Season — Altcoin underperform"
        return round(dom, 1), label
    except:
        return None, "Data tidak tersedia"

# ════════════════════════════════════════════════════
#  INDICATORS
# ════════════════════════════════════════════════════
def ema(s, n): return s.ewm(span=n, adjust=False).mean()

def rsi_smooth(s, p=14, smooth=3):
    d = s.diff()
    g = d.clip(lower=0).rolling(p).mean()
    l = (-d.clip(upper=0)).rolling(p).mean()
    rs = g / l.replace(0, np.nan)
    raw = 100 - 100/(1+rs)
    return raw, ema(raw, smooth)

def stochastic(h, l, c, k=14, d=3):
    ll = l.rolling(k).min(); hh = h.rolling(k).max()
    K  = 100*(c-ll)/(hh-ll).replace(0,np.nan)
    D  = K.rolling(d).mean()
    return K.fillna(50), D.fillna(50)

def macd_calc(s, f=12, sl=26, sg=9):
    ml = ema(s,f)-ema(s,sl); sig = ema(ml,sg)
    return ml, sig, ml-sig

def apply_indicators(df):
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.droplevel(-1)
    df["EMA9"]   = ema(df["Close"],9)
    df["EMA21"]  = ema(df["Close"],21)
    df["EMA50"]  = ema(df["Close"],50)
    df["EMA200"] = ema(df["Close"],200)
    df["RSI"], df["RSI_EMA"] = rsi_smooth(df["Close"],14,3)
    df["STOCH_K"], df["STOCH_D"] = stochastic(df["High"],df["Low"],df["Close"],14,3)
    df["MACD"], df["MACD_Sig"], df["MACD_Hist"] = macd_calc(df["Close"])
    tp = (df["High"]+df["Low"]+df["Close"])/3
    df["VWAP"] = (tp*df["Volume"]).cumsum()/df["Volume"].cumsum()
    df["BB_mid"]   = df["Close"].rolling(20).mean()
    df["BB_std"]   = df["Close"].rolling(20).std()
    df["BB_upper"] = df["BB_mid"]+2*df["BB_std"]
    df["BB_lower"] = df["BB_mid"]-2*df["BB_std"]
    df["BB_pct"]   = (df["Close"]-df["BB_lower"])/(df["BB_upper"]-df["BB_lower"])
    df["AvgVol"]   = df["Volume"].rolling(20).mean()
    df["RVOL"]     = df["Volume"]/df["AvgVol"].replace(0,np.nan)
    df["NetVol"]   = np.where(df["Close"]>=df["Open"],df["Volume"],-df["Volume"])
    df["NetVol3"]  = df["NetVol"].rolling(3).sum()
    df["NetVol8"]  = df["NetVol"].rolling(8).sum()
    df["VolSpike"] = df["RVOL"]>2.5
    df["Body"]     = (df["Close"]-df["Open"]).abs()
    df["BodyRatio"]= df["Body"]/(df["High"]-df["Low"]).replace(0,np.nan)
    df["BullBar"]  = (df["Close"]>df["Open"])&(df["BodyRatio"]>0.5)
    df["ROC3"]     = df["Close"].pct_change(3)
    df["HH"] = df["High"]>df["High"].shift(1)
    df["HL"] = df["Low"]>df["Low"].shift(1)
    tr = pd.concat([df["High"]-df["Low"],
                    (df["High"]-df["Close"].shift()).abs(),
                    (df["Low"] -df["Close"].shift()).abs()],axis=1).max(axis=1)
    df["ATR"] = tr.rolling(14).mean()
    return df

# ════════════════════════════════════════════════════
#  SCORING — Crypto SPOT (LONG ONLY)
#  Crypto vs Saham IDX:
#  - Volatilitas 3-5x lebih tinggi → threshold RVOL lebih longgar
#  - 24/7 market → tidak ada overnight bias
#  - Korelasi BTC → consider BTC trend per bar
# ════════════════════════════════════════════════════
def score_scalping(r, p, p2):
    score=0; reasons=[]
    if r["EMA9"]>r["EMA21"]>r["EMA50"]:  score+=1.5; reasons.append("EMA stack ▲")
    elif r["EMA9"]>r["EMA21"]:            score+=0.8; reasons.append("EMA9>21")
    if r["Close"]>r["VWAP"]:             score+=1.0; reasons.append("Above VWAP")
    mh=float(r["MACD_Hist"]); mhp=float(p["MACD_Hist"])
    if mh>0 and mh>mhp:  score+=1.5; reasons.append("MACD expanding ✦")
    elif mh>0:            score+=0.5; reasons.append("MACD +")
    rsi_e=float(r["RSI_EMA"])
    if 50<rsi_e<70:  score+=0.8; reasons.append(f"RSI-EMA={rsi_e:.1f}")
    elif rsi_e>=70:  score-=0.3  # crypto OB lebih toleran
    rvol=float(r["RVOL"])
    if rvol>2.5:   score+=1.2; reasons.append(f"RVOL={rvol:.1f}x surge 🔥")
    elif rvol>1.5: score+=0.7; reasons.append(f"RVOL={rvol:.1f}x")
    elif rvol>1.2: score+=0.3
    if bool(r["BullBar"]):    score+=0.5; reasons.append("Bull bar")
    if float(r["NetVol3"])>0: score+=0.4; reasons.append("Net vol +")
    if r["Close"]<r["EMA200"]*0.95: score-=0.5
    return max(0,min(6,round(score,1))), reasons, {}

def score_momentum(r, p, p2):
    score=0; reasons=[]
    if bool(r["HH"]) and bool(r["HL"]): score+=1.5; reasons.append("HH+HL ▲")
    elif bool(r["HH"]):                  score+=0.8
    rvol=float(r["RVOL"])
    if rvol>3.0:   score+=1.5; reasons.append(f"RVOL={rvol:.1f}x SURGE 🔥")
    elif rvol>2.0: score+=1.0; reasons.append(f"RVOL={rvol:.1f}x")
    elif rvol>1.5: score+=0.5; reasons.append(f"RVOL={rvol:.1f}x")
    roc=float(r["ROC3"])*100
    if roc>3.0:   score+=1.5; reasons.append(f"ROC3={roc:.1f}%")
    elif roc>1.5: score+=0.8; reasons.append(f"ROC3={roc:.1f}%")
    elif roc<0:   score-=0.5
    rsi_e=float(r["RSI_EMA"])
    if 55<rsi_e<78: score+=0.8; reasons.append(f"RSI-EMA={rsi_e:.1f}")
    if rsi_e>82:    score-=0.8; reasons.append("⚠️ RSI OB extreme")
    sk=float(r["STOCH_K"]); sd=float(r["STOCH_D"])
    if sk>60 and sk>sd: score+=0.8; reasons.append("STOCH bullish")
    mh=float(r["MACD_Hist"]); mhp=float(p["MACD_Hist"])
    if mh>0 and mh>mhp: score+=0.8; reasons.append("MACD expanding")
    if r["Close"]>r["VWAP"]: score+=0.5; reasons.append("Above VWAP")
    return max(0,min(6,round(score,1))), reasons, {}

def score_reversal(r, p, p2):
    """
    SPOT only: reversal = beli di oversold extreme.
    Tidak ada short. Fokus V-bottom dan capitulation volume.
    """
    score=0; reasons=[]; os_count=0
    rsi_e=float(r["RSI_EMA"])
    # Crypto RSI threshold lebih rendah (lebih sering OS extreme)
    if rsi_e<25:   os_count+=1; score+=2.0; reasons.append(f"RSI-EMA={rsi_e:.1f} EXTREME OS 🔥")
    elif rsi_e<32: os_count+=1; score+=1.2; reasons.append(f"RSI-EMA={rsi_e:.1f} OS")
    elif rsi_e<40: os_count+=1; score+=0.6
    sk=float(r["STOCH_K"]); sd=float(r["STOCH_D"])
    if sk<15:   os_count+=1; score+=1.2; reasons.append(f"STOCH={sk:.0f} extreme OS")
    elif sk<25: os_count+=1; score+=0.6
    bp=float(r["BB_pct"])
    if bp<0.02:   os_count+=1; score+=1.2; reasons.append("BB lower extreme")
    elif bp<0.10: os_count+=1; score+=0.6; reasons.append("BB lower touch")
    if os_count < 1: return 0,[],{}
    # Reversal confirmation
    rev=0
    pk=float(p["STOCH_K"]); pd_=float(p["STOCH_D"])
    if sk<25 and sk>sd and pk<=pd_: rev+=1; score+=2.0; reasons.append("STOCH cross ↑ OS ✦✦")
    elif sk<20 and sk>sd:           rev+=1; score+=1.2; reasons.append("STOCH K>D extreme OS")
    rsi_p=float(p["RSI_EMA"])
    if rsi_e>rsi_p and rsi_e<38: rev+=1; score+=1.2; reasons.append("RSI pivot ↑")
    mh=float(r["MACD_Hist"]); mhp=float(p["MACD_Hist"])
    if mh>mhp and mh<0: rev+=1; score+=1.0; reasons.append("MACD diverge ↑")
    # Capitulation candle (big vol + lower wick)
    rvol=float(r["RVOL"])
    lo_wick=(float(r["Open"])-float(r["Low"]))/max(float(r["High"])-float(r["Low"]),1)
    if rvol>2.0 and lo_wick>0.4: score+=1.5; reasons.append(f"Capitulation vol RVOL={rvol:.1f}x 🔥")
    elif rvol>1.5:               score+=0.5; reasons.append(f"Vol surge {rvol:.1f}x")
    if float(r["NetVol3"])>0: score+=0.5; reasons.append("Net vol turning +")
    if rev==0: score*=0.4
    return max(0,min(6,round(score,1))), reasons, {}

def score_bagger(r, p, p2, df_full):
    """
    Wyckoff Accumulation — Daily TF.
    Crypto: pola Wyckoff 10-30 hari vs saham 1-3 bulan.
    """
    score=0; reasons=[]
    close=float(r["Close"]); e9=float(r["EMA9"]); e21=float(r["EMA21"])
    e50=float(r["EMA50"]); e200=float(r["EMA200"])
    rvol=float(r["RVOL"]); rsi_e=float(r["RSI_EMA"])
    wyckoff_phase = "SCANNING"

    # ① PHASE A-B SIDEWAYS (Crypto: 10-20 hari)
    is_sideways=False
    range_high=close*1.05; range_low=close*0.95
    sb=min(20, len(df_full)-2)
    try:
        rh=df_full["High"].iloc[-sb-1:-1]; rl=df_full["Low"].iloc[-sb-1:-1]
        range_high=float(rh.max()); range_low=float(rl.min())
        rng=(range_high-range_low)/max(range_low,0.01)*100
        # Crypto: threshold 12% (lebih longgar dari saham 8-10%)
        is_sideways=rng<12.0
        if is_sideways:
            tb=max(0,(12.0-rng)/12.0)
            score+=1.0+tb*0.5; reasons.append(f"Sideways {rng:.1f}% ({sb}D) ✦")
            wyckoff_phase="A-B"
    except: pass

    # Dry Volume
    try:
        vm20=float(df_full["AvgVol"].iloc[-1])
        vl5=float(df_full["Volume"].iloc[-6:-1].mean())
        dr=vl5/max(vm20,1)
        if dr<0.5 and is_sideways:
            score+=2.0; reasons.append(f"Dry vol {dr:.2f}x — stealth accum ✦✦")
            wyckoff_phase="A-B AKUMULASI"
        elif dr<0.7 and is_sideways:
            score+=1.2; reasons.append(f"Vol drying {dr:.2f}x ✦")
            wyckoff_phase="A-B AKUMULASI"
        elif dr<0.85 and is_sideways:
            score+=0.5
    except: pass

    # Stealth Net Buy
    try:
        if len(df_full)>=12:
            nv=[float(df_full["NetVol"].iloc[i]) for i in range(-11,-1)]
            np_=sum(1 for v in nv if v>0); nr=np_/10
            if nr>=0.7 and is_sideways:
                score+=1.5; reasons.append(f"Stealth net buy {np_}/10 hari ✦✦")
            elif nr>=0.6: score+=0.8; reasons.append(f"Net buy {np_}/10 hari")
            elif nr>=0.5: score+=0.4
    except:
        nv3=float(r["NetVol3"]); nv8=float(r["NetVol8"])
        if nv3>0 and nv8>0: score+=0.8; reasons.append("Net buyer sustained ✦")

    # BB Squeeze
    try:
        bc=float(r["BB_std"])
        ba=float(df_full["BB_std"].iloc[-11:-1].mean())
        sq=bc/max(ba,0.0001)
        if sq<0.7 and is_sideways: score+=1.5; reasons.append(f"BB squeeze {sq:.2f}x ✦✦")
        elif sq<0.85:              score+=0.8; reasons.append(f"BB squeeze {sq:.2f}x")
    except: pass

    # ② SPRING
    spring_detected=False
    try:
        lp=min(15,len(df_full)-3)
        supp=float(df_full["Low"].iloc[-lp-2:-2].min())
        bl=float(r["Low"]); bc2=float(r["Close"]); bh=float(r["High"])
        if bl<supp and bc2>supp:
            rs=(bc2-bl)/max(bh-bl,0.0001)
            if rs>0.7 and rvol>1.2:
                score+=3.0; reasons.append(f"🔥 SPRING! {rs:.0%} rebound ✦✦✦")
                wyckoff_phase="SPRING ⚡"; spring_detected=True
            elif rs>0.5:
                score+=1.8; reasons.append(f"Spring ({rs:.0%} recovery) ✦✦")
                wyckoff_phase="SPRING"; spring_detected=True
        psp=(float(p["Low"])<supp and float(p["Close"])>supp and bc2>float(p["Close"]))
        if psp and not spring_detected:
            score+=2.0; reasons.append("Post-spring ✦✦"); wyckoff_phase="POST-SPRING"; spring_detected=True
    except: pass

    # ③ PHASE D BREAKOUT
    try:
        ab_res=close>range_high*0.998; tb2=float(r["BodyRatio"])>0.55; bull=float(r["Close"])>float(r["Open"])
        if rvol>3.0 and ab_res and tb2 and bull:
            score+=3.0; reasons.append(f"🚀 PHASE D! RVOL={rvol:.1f}x breakout ✦✦✦"); wyckoff_phase="PHASE D 🚀"
        elif rvol>2.0 and ab_res and bull:
            score+=2.2; reasons.append(f"Breakout RVOL={rvol:.1f}x ✦✦"); wyckoff_phase="BREAKOUT ✦"
        elif rvol>1.5 and ab_res:
            score+=1.5; reasons.append(f"Breakout attempt {rvol:.1f}x")
        elif ab_res: score+=0.8; reasons.append("Above resistance")
        else:
            if rvol>3.0: score+=1.2; reasons.append(f"RVOL={rvol:.1f}x SURGE")
            elif rvol>2.0: score+=0.8; reasons.append(f"RVOL={rvol:.1f}x")
    except:
        if rvol>3.0: score+=1.2
        elif rvol>2.0: score+=0.8

    if e9>e21>e50>e200: score+=1.5; reasons.append("EMA golden stack ✦✦")
    elif e9>e21>e50:     score+=1.0; reasons.append("EMA stack ▲")
    elif e9>e21:         score+=0.4

    if wyckoff_phase in ["A-B","A-B AKUMULASI","SPRING","POST-SPRING"]:
        if 20<=rsi_e<=52: score+=1.0; reasons.append(f"RSI={rsi_e:.1f} accum zone ✓")
        elif rsi_e<20:    score+=0.8; reasons.append(f"RSI={rsi_e:.1f} extreme OS")
        elif rsi_e>65:    score-=0.3
    else:
        if 52<rsi_e<75:  score+=1.0; reasons.append(f"RSI={rsi_e:.1f} momentum")
        elif rsi_e>=75:  score-=0.5; reasons.append(f"⚠️ RSI OB {rsi_e:.1f}")

    if close>float(r["VWAP"]): score+=0.5; reasons.append("Above VWAP")
    if e200>0 and close<e200*0.85: score-=1.0

    try:
        bc3=sum(1 for i in range(-3,0) if float(df_full["Close"].iloc[i])>float(df_full["Open"].iloc[i]))
        if bc3==3:   score+=0.8; reasons.append("3x bull bar")
        elif bc3==2: score+=0.3
    except: pass

    if wyckoff_phase!="SCANNING": reasons.insert(0,f"⚙️ Wyckoff: {wyckoff_phase}")
    return max(0,min(6,round(score,1))), reasons, {"wyckoff_phase":wyckoff_phase}

# ════════════════════════════════════════════════════
#  SIGNAL & CARD
# ════════════════════════════════════════════════════
def get_signal(score, mode):
    t = {
        "Scalping ⚡": {5:"GACOR ⚡",    4:"POTENSIAL 🔥",3:"WATCH 👀"},
        "Momentum 🚀": {5:"GACOR 🚀",    4:"POTENSIAL 🔥",3:"WATCH 👀"},
        "Reversal 🎯": {5:"REVERSAL 🎯", 4:"POTENSIAL 🔥",3:"WATCH 👀"},
        "Bagger 💎":   {5:"BAGGER 💎",   4:"KANDIDAT 🚀", 3:"WATCH 👀"},
    }.get(mode,{})
    for th in sorted(t.keys(),reverse=True):
        if score>=th: return t[th]
    return "WAIT"

def get_card_class(sig):
    if "BAGGER" in sig or "KANDIDAT" in sig: return "bagger"
    if "GACOR"  in sig or "REVERSAL" in sig: return "gacor"
    if "POTENSIAL" in sig:                   return "potensial"
    if "WATCH"  in sig:                      return "watch"
    return ""

def fmt_price(p):
    """Format harga IDR crypto — bisa dari 1 Rupiah sampai 1 Miliar."""
    if p >= 1_000_000:   return f"Rp{p/1_000_000:.2f}M"
    elif p >= 1_000:     return f"Rp{p:,.0f}"
    elif p >= 1:         return f"Rp{p:.2f}"
    else:                return f"Rp{p:.6f}"

# ════════════════════════════════════════════════════
#  TELEGRAM
# ════════════════════════════════════════════════════
def send_telegram(results_top, source="Scanner"):
    if not TOKEN or not CHAT_ID: return
    now=datetime.now(WIB); sep="━"*28
    hdr=(f"🚀 *CRYPTO TURBO {'WATCHLIST' if source=='Watchlist' else 'ALERT'}*\n"
         f"⏰ `{now.strftime('%H:%M:%S')} WIB` · `{now.strftime('%d %b %Y')}`\n"
         f"🌐 Indodax · SPOT IDR\n{sep}\n")
    body=""
    for r in results_top[:5]:
        sig=r.get("Signal","-")
        em="💎" if "BAGGER" in sig else("🏆" if "GACOR" in sig or "REVERSAL" in sig else("🔥" if "POTENSIAL" in sig else "👀"))
        bar="█"*int(r["Score"])+"░"*(6-int(r["Score"]))
        body+=(f"\n{em} *{r['Pair']}*  `{sig}`\n"
               f"   💰 Price: `{fmt_price(r['Price'])}`\n"
               f"   📊 Score: `[{bar}] {r['Score']}/6`\n"
               f"   📈 RSI: `{r.get('RSI-EMA',0)}` | RVOL: `{r.get('RVOL',0)}x`\n"
               f"   🎯 TP: `{fmt_price(r['TP'])}` | SL: `{fmt_price(r['SL'])}` | R:R `{r['R:R']}`\n"
               f"   💡 _{r.get('Reasons','')[:60]}_\n")
    footer=f"\n{sep}\n🔥 _Crypto Turbo · Wyckoff Bagger · SPOT IDR_\n⚠️ _BUKAN saran investasi. DYOR!_"
    try:
        requests.post(f"https://api.telegram.org/bot{TOKEN}/sendMessage",
                      data={"chat_id":CHAT_ID,"text":hdr+body+footer,"parse_mode":"Markdown"},
                      timeout=10)
    except: pass


# ════════════════════════════════════════════════════
#  HEADER
# ════════════════════════════════════════════════════
regime,btc_price,ema20,ema55,regime_detail,btc_chg = get_btc_regime()
rcfg   = get_regime_config(regime)
rcolor = rcfg["color"]
chg_col= "#00ff88" if btc_chg>=0 else "#ff3d5a"
chg_sym= "▲" if btc_chg>=0 else "▼"
now_wib= datetime.now(WIB)
btc_dom, dom_label = get_btc_dominance()
fr_data= get_funding_rates()
btc_fr = fr_data.get("BTC",{}).get("rate",0)
fr_lbl, fr_col = fr_label(btc_fr)

st.markdown(f"""
<div class="ct-header">
  <div>
    <div class="ct-logo">🚀 CRYPTO TURBO</div>
    <div class="ct-sub">SPOT IDR · Indodax · Wyckoff Bagger · Auto BTC Regime</div>
  </div>
  <div class="live-badge"><div class="live-dot"></div>LIVE {now_wib.strftime("%H:%M:%S")} WIB</div>
</div>""", unsafe_allow_html=True)

# BTC Regime Banner
st.markdown(f"""
<div style="background:rgba(0,0,0,.4);border:1px solid {rcolor}44;border-radius:8px;
     padding:12px 16px;margin-bottom:10px;border-left:4px solid {rcolor};">
  <div style="display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:8px;">
    <div>
      <div style="font-family:Space Mono,monospace;font-size:12px;font-weight:700;color:{rcolor};">{rcfg["label"]}</div>
      <div style="font-family:Space Mono,monospace;font-size:10px;color:#4a5568;margin-top:2px;">{rcfg["desc"]}</div>
    </div>
    <div style="text-align:right;font-family:Space Mono,monospace;">
      <div style="font-size:16px;font-weight:700;color:{rcolor};">BTC {fmt_price(btc_price)} <span style="font-size:11px;color:{chg_col}">{chg_sym}{abs(btc_chg):.2f}%</span></div>
      <div style="font-size:9px;color:#4a5568;">EMA20 {fmt_price(ema20)} · EMA55 {fmt_price(ema55)}</div>
    </div>
  </div>
</div>""", unsafe_allow_html=True)

# Info strip: BTC Dom + Funding Rate
btc_dom_str = f"{btc_dom:.1f}%" if btc_dom else "N/A"
st.markdown(f"""
<div style="display:flex;gap:10px;margin-bottom:14px;flex-wrap:wrap;">
  <div style="flex:1;background:var(--surface);border:1px solid var(--border);border-radius:6px;padding:8px 12px;">
    <div style="font-size:9px;color:var(--muted);letter-spacing:1px;">₿ BTC DOMINANCE</div>
    <div style="font-family:Space Mono,monospace;font-size:13px;font-weight:700;color:var(--btc);">{btc_dom_str}</div>
    <div style="font-size:9px;color:#4a5568;">{dom_label}</div>
  </div>
  <div style="flex:2;background:var(--surface);border:1px solid var(--border);border-radius:6px;padding:8px 12px;">
    <div style="font-size:9px;color:var(--muted);letter-spacing:1px;">📊 FUNDING RATE (Binance Ref · SPOT Sentiment)</div>
    <div style="display:flex;gap:16px;margin-top:4px;flex-wrap:wrap;">
      {"".join([f'<div><span style="font-size:9px;color:#4a5568;">{c}</span> <span style="font-family:Space Mono,monospace;font-size:11px;color:{"#ff3d5a" if d["rate"]>0.05 else "#00ff88" if d["rate"]<-0.01 else "#ffb700"};">{d["rate"]:+.4f}%</span></div>' for c,d in fr_data.items()])}
    </div>
    <div style="font-size:9px;color:#4a5568;margin-top:2px;">{fr_lbl}</div>
  </div>
</div>""", unsafe_allow_html=True)

# ════════════════════════════════════════════════════
#  TABS
# ════════════════════════════════════════════════════
tab_scanner, tab_watchlist, tab_market, tab_backtest = st.tabs(
    ["🔥 Scanner","👁️ Watchlist","🌐 Market Overview","📊 Backtest"]
)

# ════════════════════════════════════════════════════
#  TAB 1: SCANNER
# ════════════════════════════════════════════════════
with tab_scanner:
    with st.expander("⚙️ Scanner Settings", expanded=False):
        sc1,sc2,sc3 = st.columns(3)
        with sc1:
            st.markdown('<div class="settings-label">MODE SIGNAL</div>',unsafe_allow_html=True)
            auto_regime_tog = st.toggle("🤖 Auto BTC Regime",value=True,key="auto_reg")
            if auto_regime_tog:
                scan_mode = rcfg["mode"]
                st.markdown(f'<div style="font-family:Space Mono,monospace;font-size:10px;padding:6px;background:rgba(0,0,0,.3);border-radius:4px;color:{rcolor}">Auto: {scan_mode}</div>',unsafe_allow_html=True)
            else:
                _opts=["Scalping ⚡","Momentum 🚀","Reversal 🎯","Bagger 💎"]
                _prev=st.session_state.get("active_scan_mode","Scalping ⚡")
                _idx=_opts.index(_prev) if _prev in _opts else 0
                scan_mode=st.radio("Mode",_opts,index=_idx,label_visibility="collapsed",key="scan_mode_radio")
            st.session_state.active_scan_mode=scan_mode
            tele_on=st.toggle("📡 Telegram Alert",value=True,key="tele_on")
        with sc2:
            st.markdown('<div class="settings-label">TIMEFRAME & FILTER</div>',unsafe_allow_html=True)
            tf_options = ["15m","1h","4h"] if scan_mode!="Bagger 💎" else ["1d"]
            is_bagger  = (scan_mode=="Bagger 💎")
            if is_bagger:
                scan_tf="1d"
                st.markdown('<div style="font-size:10px;color:#bf5fff;padding:6px;background:rgba(191,95,255,.1);border-radius:4px;">📅 Bagger: Daily TF otomatis</div>',unsafe_allow_html=True)
            else:
                _prev_tf=st.session_state.get("active_tf","15m")
                _tf_opts=["15m","1h","4h"]
                _tf_idx=_tf_opts.index(_prev_tf) if _prev_tf in _tf_opts else 0
                scan_tf=st.radio("Timeframe",_tf_opts,index=_tf_idx,horizontal=True,key="scan_tf")
            # Persist tf ke session_state
            st.session_state.active_tf=scan_tf
            auto_thresh=st.toggle("🤖 Auto-Threshold",value=True,key="auto_thr")
            if auto_thresh:
                min_score=rcfg["min_score"]; vol_thresh=rcfg["min_rvol"]
                st.caption(f"Auto: Score≥{min_score} · RVOL≥{vol_thresh}x")
            else:
                min_score=st.slider("Min Score",0,6,4,key="msc")
                vol_thresh=st.slider("Min RVOL",1.0,5.0,1.5,0.1,key="vol")
        with sc3:
            st.markdown('<div class="settings-label">PASANGAN & TAMPILAN</div>',unsafe_allow_html=True)
            view_mode=st.radio("View",["Card View 🃏","Table View 📊"],label_visibility="collapsed",key="view_mode")
            min_vol_idr=st.number_input("Min Vol 24h (Juta IDR)",value=100,step=50,key="min_vol")*1_000_000
            quick_mode=st.toggle("⚡ Quick Scan (Top 50)",value=False,key="quick_mode")
            if is_bagger:
                st.markdown('<div style="font-size:9px;color:#bf5fff;">📅 Bagger pakai Daily TF · Wyckoff 20 hari</div>',unsafe_allow_html=True)

    do_scan=st.button("🔥 SCAN SEKARANG",type="primary",use_container_width=True,key="btn_scan")

    # Auto-refresh 5 menit
    _now_chk=now_wib.timestamp()
    auto_triggered=False
    if st.session_state.last_scan_time and not do_scan:
        _el=_now_chk-st.session_state.last_scan_time
        if _el>=300 and st.session_state.scan_results:
            do_scan=True; auto_triggered=True
            # Restore mode & tf dari session_state — CRITICAL untuk auto refresh
            scan_mode=st.session_state.get("active_scan_mode", scan_mode)
            scan_tf  =st.session_state.get("active_tf", "15m")

    if do_scan:
        all_pairs=get_idr_pairs()
        # Quick mode: top 50 by name (BTC, ETH, BNB biasanya di atas)
        scan_pairs=all_pairs[:50] if quick_mode else all_pairs
        n_pairs=len(scan_pairs)
        is_bagger_scan=(scan_mode=="Bagger 💎")
        tf_scan="1d" if is_bagger_scan else scan_tf
        min_bars=20 if is_bagger_scan else 55

        prog_ph=st.empty(); pb=st.progress(0)
        label="🔄 AUTO-REFRESH" if auto_triggered else "🔥 SCANNING"
        prog_ph.markdown(
            f'<div style="color:#f7931a;font-family:Space Mono,monospace;font-size:12px;">'
            f'{label} {n_pairs} pairs · {scan_mode} · {tf_scan} · Indodax IDR</div>',
            unsafe_allow_html=True)

        # ══ PARALLEL BATCH FETCH — 5 threads, partisi 10 pair ══
        # Indodax rate limit: 10 req/s → 5 threads aman
        results   = []
        done_count= [0]
        WORKERS   = 5
        CHUNK     = 10   # partisi per batch

        def _fetch_one(pair):
            """Fetch + parse satu pair — thread safe."""
            try:
                # Tiap thread buat exchange instance sendiri — thread safe
                _ex = ccxt.indodax({"enableRateLimit": True, "timeout": 12000})
                raw = _ex.fetch_ohlcv(pair, tf_scan, limit=220)
                if not raw or len(raw) < min_bars:
                    return pair, None
                df = pd.DataFrame(raw, columns=["ts","Open","High","Low","Close","Volume"])
                df["ts"] = pd.to_datetime(df["ts"], unit="ms", utc=True).dt.tz_convert(WIB)
                df.set_index("ts", inplace=True)
                df = df.astype(float)
                return pair, df
            except:
                return pair, None

        # Proses per chunk — update progress setiap chunk selesai
        for chunk_start in range(0, n_pairs, CHUNK):
            chunk = scan_pairs[chunk_start:chunk_start + CHUNK]
            prog_ph.markdown(
                f'<div style="color:#f7931a;font-family:Space Mono,monospace;font-size:11px;">' 
                f'⚡ Fetching {chunk_start+1}-{min(chunk_start+CHUNK, n_pairs)}'
                f' dari {n_pairs} pairs · {scan_mode} · {tf_scan}...</div>',
                unsafe_allow_html=True)

            with ThreadPoolExecutor(max_workers=WORKERS) as ex_pool:
                futs = {ex_pool.submit(_fetch_one, p): p for p in chunk}
                for fut in as_completed(futs):
                    try:
                        pair, df = fut.result(timeout=15)
                        done_count[0] += 1
                        if df is None: continue

                        df = apply_indicators(df)
                        r = df.iloc[-1]; p_r = df.iloc[-2]
                        p2_r = df.iloc[-3] if len(df) >= 3 else p_r
                        close = float(r["Close"]); vol = float(r["Volume"])
                        vol_idr = close * vol; rvol = float(r["RVOL"])
                        if vol_idr < min_vol_idr or rvol < vol_thresh: continue

                        if scan_mode=="Scalping ⚡":   sc,reasons,_=score_scalping(r,p_r,p2_r)
                        elif scan_mode=="Momentum 🚀": sc,reasons,_=score_momentum(r,p_r,p2_r)
                        elif scan_mode=="Bagger 💎":   sc,reasons,_=score_bagger(r,p_r,p2_r,df)
                        else:                          sc,reasons,_=score_reversal(r,p_r,p2_r)
                        if sc < min_score: continue
                        sig = get_signal(sc, scan_mode)
                        if sig == "WAIT": continue

                        atr = float(r["ATR"]) if not np.isnan(float(r["ATR"])) else close*0.02
                        slm = rcfg.get("sl_mult", 0.8)
                        if scan_mode=="Scalping ⚡":   tp=close+1.5*atr; sl=close-slm*atr
                        elif scan_mode=="Momentum 🚀": tp=close+2.0*atr; sl=close-slm*atr
                        elif scan_mode=="Bagger 💎":   tp=close+3.5*atr; sl=close-1.2*atr
                        else:                          tp=close+2.5*atr; sl=close-slm*atr
                        rr = (tp-close) / max(close-sl, 0.0001)

                        coin = pair.split("/")[0]
                        e9=float(r["EMA9"]); e21=float(r["EMA21"]); e50=float(r["EMA50"])
                        trend = "▲ UP" if e9>e21>e50 else ("▼ DOWN" if e9<e21<e50 else "◆ SIDE")
                        results.append({
                            "Pair":pair,"Coin":coin,"Price":close,"Score":sc,"Signal":sig,
                            "Trend":trend,"TF":tf_scan,"RSI-EMA":round(float(r["RSI_EMA"]),1),
                            "Stoch K":round(float(r["STOCH_K"]),1),"RVOL":round(rvol,2),
                            "BB%":round(float(r["BB_pct"]),2),"ROC 3%":round(float(r["ROC3"])*100,2),
                            "MACD Hist":round(float(r["MACD_Hist"]),6),
                            "Vol IDR(M)":round(vol_idr/1e6,1),
                            "TP":tp,"SL":sl,"R:R":round(rr,1),
                            "Reasons":" · ".join(reasons),
                            "_class":get_card_class(sig),
                        })
                    except: done_count[0] += 1

            # Update progress bar per chunk
            pb.progress(min((chunk_start + CHUNK) / max(n_pairs, 1), 1.0))
            prog_ph.markdown(
                f'<div style="color:#00ff88;font-family:Space Mono,monospace;font-size:11px;">' 
                f'✅ {done_count[0]}/{n_pairs} selesai · {len(results)} signal ditemukan...</div>',
                unsafe_allow_html=True)

        prog_ph.empty(); pb.empty()
        st.session_state.scan_results=results
        st.session_state.last_scan_time=now_wib.timestamp()
        st.session_state.last_scan_mode=scan_mode

        if tele_on and results:
            send_telegram(sorted(results,key=lambda x:x["Score"],reverse=True)[:5])

    if st.session_state.last_scan_time:
        _rem=max(0,300-(now_wib.timestamp()-st.session_state.last_scan_time))
        _last=datetime.fromtimestamp(st.session_state.last_scan_time,WIB).strftime("%H:%M:%S")
        lm=st.session_state.get("last_scan_mode","")
        st.caption(f"⏱️ Next: {int(_rem//60):02d}:{int(_rem%60):02d} · Last: {_last} WIB · {lm}")

    results=st.session_state.scan_results
    if not results and not do_scan:
        st.markdown(f"""
        <div style="text-align:center;padding:48px;color:#4a5568;font-family:Space Mono,monospace;">
          <div style="font-size:36px;margin-bottom:12px;">🚀</div>
          <div style="font-size:13px;letter-spacing:2px;">KLIK SCAN UNTUK MULAI</div>
          <div style="font-size:10px;margin-top:8px;color:#2d3748;">
            Indodax IDR · {"Top 50" if quick_mode else "Semua"} pairs · {regime} → {rcfg["mode"]}
          </div>
        </div>""", unsafe_allow_html=True)

    elif results:
        df_out=pd.DataFrame(results).sort_values("Score",ascending=False).reset_index(drop=True)
        gacor  =df_out[df_out["Signal"].str.contains("GACOR|REVERSAL",na=False)]
        bagger =df_out[df_out["Signal"].str.contains("BAGGER|KANDIDAT",na=False)]
        potensi=df_out[df_out["Signal"].str.contains("POTENSIAL",na=False)]
        avg_rsi=df_out["RSI-EMA"].mean()
        lm=st.session_state.get("last_scan_mode","")

        st.markdown(f"""
        <div class="metric-row">
          <div class="metric-card btc"><div class="metric-label">BTC Regime</div>
            <div class="metric-value" style="font-size:14px;color:{rcolor}">{regime}</div>
            <div class="metric-sub">{fmt_price(btc_price)} {chg_sym}{abs(btc_chg):.2f}%</div></div>
          <div class="metric-card orange"><div class="metric-label">Mode · TF</div>
            <div class="metric-value" style="font-size:11px;margin-top:4px;">{lm}</div>
            <div class="metric-sub">{tf_scan}</div></div>
          <div class="metric-card green"><div class="metric-label">Signal Lolos</div>
            <div class="metric-value">{len(df_out)}</div>
            <div class="metric-sub">dari {len(results)+sum(1 for _ in [])} pairs</div></div>
          <div class="metric-card purple"><div class="metric-label">BAGGER 💎</div>
            <div class="metric-value">{len(bagger)}</div>
            <div class="metric-sub">Wyckoff daily</div></div>
          <div class="metric-card red"><div class="metric-label">GACOR 🔥</div>
            <div class="metric-value">{len(gacor)}</div></div>
          <div class="metric-card amber"><div class="metric-label">POTENSIAL</div>
            <div class="metric-value">{len(potensi)}</div></div>
          <div class="metric-card"><div class="metric-label">Avg RSI</div>
            <div class="metric-value" style="font-size:20px;color:{'#00ff88' if avg_rsi>50 else '#ffb700' if avg_rsi>35 else '#ff3d5a'}">{avg_rsi:.1f}</div></div>
        </div>""", unsafe_allow_html=True)

        # Ticker tape
        th='<div class="tape-wrap"><div class="tape-inner">'
        for _,row in df_out.iterrows():
            roc=row["ROC 3%"]
            is_bag="BAGGER" in row["Signal"] or "KANDIDAT" in row["Signal"]
            cls='bagger' if is_bag else('up' if roc>0 else('down' if roc<0 else'flat'))
            sym='💎' if is_bag else('▲' if roc>0 else('▼' if roc<0 else'─'))
            th+=f'<span class="tape-item {cls}">{row["Coin"]} {fmt_price(row["Price"])} {sym}{abs(roc):.1f}% [{row["Signal"]}]</span>'
        th+=th.replace('tape-inner">',''); th+='</div></div>'
        st.markdown(th,unsafe_allow_html=True)

        if not bagger.empty:
            st.markdown(f'<div class="bagger-alert-box"><div class="bagger-title">💎 WYCKOFF BAGGER ALERT · {len(bagger)} KANDIDAT · DAILY TF</div><div style="font-size:11px;color:#4a5568;margin-top:4px;">Phase A-B Akumulasi · Spring/Shakeout · Phase D Breakout — Chart Harian Indodax</div></div>',unsafe_allow_html=True)
        if not gacor.empty:
            st.markdown(f'<div class="alert-box"><div class="alert-title">🚨 GACOR ALERT · {len(gacor)} PAIR · {lm} · {tf_scan}</div></div>',unsafe_allow_html=True)

        if view_mode=="Card View 🃏":
            st.markdown('<div class="section-title">Signal Cards</div>',unsafe_allow_html=True)
            card_html='<div class="signal-grid">'
            for _,row in df_out.head(24).iterrows():
                sc_int=int(row["Score"])
                is_bag="BAGGER" in row["Signal"] or "KANDIDAT" in row["Signal"]
                bar_cls="filled-purple" if is_bag else "filled"
                bars="".join([f'<div class="sc-bar {bar_cls if i<sc_int else "empty"}" style="width:26px"></div>' for i in range(6)])
                roc_c="#00ff88" if row["ROC 3%"]>0 else "#ff3d5a"
                te="📈" if "▲" in row["Trend"] else("📉" if "▼" in row["Trend"] else "➡️")
                sig_c="#bf5fff" if is_bag else("#00ff88" if sc_int>=5 else "#ffb700" if sc_int>=4 else "#00e5ff")
                tf_b=f'<span style="font-size:8px;color:{"#bf5fff" if row["TF"]=="1d" else "#4a5568"};">[{row["TF"]}]</span>'
                card_html+=f"""<div class="signal-card {row['_class']}">
                  <div style="display:flex;justify-content:space-between;align-items:flex-start;">
                    <div><div class="sc-ticker">{row['Coin']} {tf_b}</div>
                    <div class="sc-price" style="color:{roc_c}">{fmt_price(row['Price'])} {te}</div></div>
                    <div style="text-align:right">
                      <div style="font-size:9px;color:#4a5568;font-family:Space Mono,monospace">SCORE</div>
                      <div style="font-size:20px;font-weight:700;color:{sig_c};font-family:Space Mono,monospace">{row['Score']}</div>
                    </div>
                  </div>
                  <div class="sc-signal" style="color:{sig_c}">{row['Signal']}</div>
                  <div class="sc-bars">{bars}</div>
                  <div class="sc-stats">
                    <div class="sc-stat">RSI <span>{row['RSI-EMA']}</span></div>
                    <div class="sc-stat">STOCH <span>{row['Stoch K']:.0f}</span></div>
                    <div class="sc-stat">RVOL <span>{row['RVOL']}x</span></div>
                    <div class="sc-stat">ROC <span style="color:{roc_c}">{row['ROC 3%']:+.1f}%</span></div>
                  </div>
                  <div class="sc-stats" style="margin-top:6px">
                    <div class="sc-stat">TP <span style="color:#00ff88">{fmt_price(row['TP'])}</span></div>
                    <div class="sc-stat">SL <span style="color:#ff3d5a">{fmt_price(row['SL'])}</span></div>
                    <div class="sc-stat">R:R <span>{row['R:R']}</span></div>
                    <div class="sc-stat">Vol <span>{row['Vol IDR(M)']}M</span></div>
                  </div>
                  <div style="margin-top:8px;font-size:10px;color:#4a5568;line-height:1.4;font-family:Space Mono,monospace">{row['Reasons'][:80]}</div>
                </div>"""
            card_html+="</div>"
            st.markdown(card_html,unsafe_allow_html=True)

        st.markdown('<div class="section-title">Full Table</div>',unsafe_allow_html=True)
        disp_cols=["Pair","TF","Price","Score","Signal","Trend","RSI-EMA","Stoch K","RVOL","BB%","ROC 3%","MACD Hist","Vol IDR(M)","TP","SL","R:R","Reasons"]
        disp_cols=[c for c in disp_cols if c in df_out.columns]
        # Format Price, TP, SL untuk display
        df_disp=df_out[disp_cols].copy()
        df_disp["Price"]=df_disp["Price"].apply(fmt_price)
        df_disp["TP"]=df_disp["TP"].apply(fmt_price)
        df_disp["SL"]=df_disp["SL"].apply(fmt_price)
        st.dataframe(df_disp,width="stretch",hide_index=True,column_config={
            "Score":   st.column_config.ProgressColumn("Score",min_value=0,max_value=6,format="%.1f"),
            "RSI-EMA": st.column_config.NumberColumn("RSI-EMA",format="%.1f"),
            "RVOL":    st.column_config.NumberColumn("RVOL",format="%.2fx"),
            "ROC 3%":  st.column_config.NumberColumn("ROC 3%",format="%.2f%%"),
            "Vol IDR(M)": st.column_config.NumberColumn("Vol IDR(M)",format="Rp%.0fM"),
        })


# ════════════════════════════════════════════════════
#  TAB 2: WATCHLIST
# ════════════════════════════════════════════════════
with tab_watchlist:
    st.markdown("""<div style="font-family:Space Mono,monospace;font-size:10px;color:#4a5568;margin-bottom:12px;
        padding:10px 14px;background:#0d1117;border-radius:6px;border-left:3px solid #f7931a;">
      Analisa mendalam per pair. Input format: BTC, ETH, SOL (tanpa /IDR). Bagger 💎 otomatis Daily TF.
    </div>""", unsafe_allow_html=True)

    wc1,wc2,wc3=st.columns([3,1,1])
    with wc1:
        wl_input=st.text_area("Pairs",placeholder="BTC\nETH, SOL, BNB\nXRP, ADA, DOGE",
                              height=120,label_visibility="collapsed",key="wl_input")
    with wc2:
        wl_mode=st.radio("Mode",["Scalping ⚡","Momentum 🚀","Reversal 🎯","Bagger 💎"],key="wl_mode")
        if wl_mode!="Bagger 💎":
            wl_tf=st.radio("TF",["15m","1h","4h"],horizontal=True,key="wl_tf")
        else:
            wl_tf="1d"
            st.markdown('<div style="font-size:9px;color:#bf5fff;">📅 Daily TF</div>',unsafe_allow_html=True)
        st.caption(f"BTC suggests: {rcfg['mode']}")
    with wc3:
        st.markdown("<br>",unsafe_allow_html=True)
        wl_run  =st.button("🔍 Analisa",use_container_width=True,key="wl_run")
        wl_tele =st.button("📡 Telegram",use_container_width=True,key="wl_tele")
        wl_share=st.button("📋 Copy",use_container_width=True,key="wl_share")

    if wl_run and wl_input.strip():
        raw_wl=list(dict.fromkeys([
            t.strip().upper().replace("/IDR","")
            for line in wl_input.split("\n")
            for t in line.split(",") if t.strip()
        ]))
        pairs_wl=[f"{c}/IDR" for c in raw_wl]
        is_bag_wl=(wl_mode=="Bagger 💎")
        tf_wl="1d" if is_bag_wl else wl_tf
        min_b=20 if is_bag_wl else 55

        with st.spinner(f"Menganalisa {len(pairs_wl)} pairs ({tf_wl})..."):
            wl_res=[]; ex=get_exchange()
            for pair_w in pairs_wl:
                coin_w=pair_w.split("/")[0]
                df_w=None
                try:
                    raw=ex.fetch_ohlcv(pair_w,tf_wl,limit=220)
                    if raw and len(raw)>=min_b:
                        df_w=pd.DataFrame(raw,columns=["ts","Open","High","Low","Close","Volume"])
                        df_w["ts"]=pd.to_datetime(df_w["ts"],unit="ms",utc=True).dt.tz_convert(WIB)
                        df_w.set_index("ts",inplace=True); df_w=df_w.astype(float)
                except: pass

                if df_w is None:
                    wl_res.append({"Pair":pair_w,"Coin":coin_w,"Price":0,"Score":0,
                        "Signal":"No data","RSI-EMA":0,"Stoch K":0,"RVOL":0,
                        "Trend":"-","TF":tf_wl,"TP":0,"SL":0,"R:R":0,"ROC 3%":0,
                        "Vol IDR(M)":0,"Reasons":"No data","_class":""}); continue
                try:
                    df_w=apply_indicators(df_w)
                    r=df_w.iloc[-1]; p=df_w.iloc[-2]; p2=df_w.iloc[-3] if len(df_w)>=3 else p
                    close=float(r["Close"]); atr=float(r["ATR"]) if not np.isnan(float(r["ATR"])) else close*0.02
                    slm=rcfg.get("sl_mult",0.8)
                    if wl_mode=="Scalping ⚡":   sc,reasons,_=score_scalping(r,p,p2);  tp=close+1.5*atr; sl=close-slm*atr
                    elif wl_mode=="Momentum 🚀": sc,reasons,_=score_momentum(r,p,p2);  tp=close+2.0*atr; sl=close-slm*atr
                    elif wl_mode=="Bagger 💎":   sc,reasons,_=score_bagger(r,p,p2,df_w); tp=close+3.5*atr; sl=close-1.2*atr
                    else:                        sc,reasons,_=score_reversal(r,p,p2);  tp=close+2.5*atr; sl=close-slm*atr
                    sig=get_signal(sc,wl_mode); rr=(tp-close)/max(close-sl,0.0001)
                    e9=float(r["EMA9"]); e21=float(r["EMA21"]); e50=float(r["EMA50"])
                    trend="▲ UP" if e9>e21>e50 else("▼ DOWN" if e9<e21<e50 else "◆ SIDE")
                    # Funding rate untuk coin ini
                    bn_sym=f"{coin_w}/USDT:USDT"
                    fr_coin=fr_data.get(coin_w,{}).get("rate",None)
                    fr_info=f"{fr_coin:+.4f}%" if fr_coin is not None else "N/A"
                    wl_res.append({"Pair":pair_w,"Coin":coin_w,"Price":close,"Score":sc,
                        "Signal":sig,"Trend":trend,"TF":tf_wl,
                        "RSI-EMA":round(float(r["RSI_EMA"]),1),"Stoch K":round(float(r["STOCH_K"]),1),
                        "RVOL":round(float(r["RVOL"]),2),"BB%":round(float(r["BB_pct"]),2),
                        "ROC 3%":round(float(r["ROC3"])*100,2),"MACD Hist":round(float(r["MACD_Hist"]),6),
                        "TP":tp,"SL":sl,"R:R":round(rr,1),
                        "Vol IDR(M)":round(close*float(r["Volume"])/1e6,1),
                        "FR":fr_info,"Reasons":" · ".join(reasons),"_class":get_card_class(sig)})
                except Exception as ex2:
                    wl_res.append({"Pair":pair_w,"Coin":coin_w,"Price":0,"Score":0,
                        "Signal":f"Err:{str(ex2)[:20]}","RSI-EMA":0,"Stoch K":0,"RVOL":0,
                        "Trend":"-","TF":tf_wl,"TP":0,"SL":0,"R:R":0,"ROC 3%":0,
                        "Vol IDR(M)":0,"FR":"N/A","Reasons":"","_class":""})

        st.session_state.wl_results=wl_res
        ok=[r for r in wl_res if r["Score"]>0]
        bag=[r for r in ok if any(k in r.get("Signal","") for k in ["BAGGER","KANDIDAT"])]
        gcr=[r for r in ok if any(k in r.get("Signal","") for k in ["GACOR","REVERSAL"])]
        pot=[r for r in ok if "POTENSIAL" in r.get("Signal","")]
        st.markdown(f"""<div class="metric-row" style="margin-top:16px;">
          <div class="metric-card orange"><div class="metric-label">Dipantau</div><div class="metric-value">{len(raw_wl)}</div></div>
          <div class="metric-card purple"><div class="metric-label">BAGGER 💎</div><div class="metric-value">{len(bag)}</div></div>
          <div class="metric-card green"><div class="metric-label">GACOR 🔥</div><div class="metric-value">{len(gcr)}</div></div>
          <div class="metric-card amber"><div class="metric-label">POTENSIAL</div><div class="metric-value">{len(pot)}</div></div>
          <div class="metric-card"><div class="metric-label">Data OK</div><div class="metric-value">{len(ok)}</div></div>
        </div>""", unsafe_allow_html=True)

        ch='<div class="signal-grid">'
        for row in sorted(wl_res,key=lambda x:x["Score"],reverse=True):
            if row["Price"]==0:
                ch+=f'<div class="signal-card"><div class="sc-ticker">{row["Coin"]}</div><div style="font-size:11px;color:#4a5568;margin-top:6px;">{row.get("Signal","No data")}</div></div>'
                continue
            sc_int=int(row["Score"]); bars="".join([f'<div class="sc-bar {"filled" if i<sc_int else "empty"}" style="width:24px"></div>' for i in range(6)])
            sig=row.get("Signal","-"); is_bag="BAGGER" in sig or "KANDIDAT" in sig
            sc_col="#bf5fff" if is_bag else("#00ff88" if "GACOR" in sig or "REVERSAL" in sig else "#ffb700" if "POTENSIAL" in sig else "#00e5ff" if "WATCH" in sig else "#4a5568")
            rsi_v=row["RSI-EMA"]; rsi_c="#ff3d5a" if rsi_v<30 else("#ffb700" if rsi_v<45 else "#00ff88" if rsi_v>60 else "#c9d1d9")
            roc_c="#00ff88" if row.get("ROC 3%",0)>0 else "#ff3d5a"
            te="📈" if "▲" in row["Trend"] else("📉" if "▼" in row["Trend"] else "➡️")
            fr_v=row.get("FR","N/A")
            fr_c="#ff3d5a" if "+" in str(fr_v) and fr_v!="N/A" else("#00ff88" if "-" in str(fr_v) else "#4a5568")
            ch+=f"""<div class="signal-card {row['_class']}">
              <div style="display:flex;justify-content:space-between;">
                <div><div class="sc-ticker">{row['Coin']} <span style="font-size:8px;color:#4a5568;">[{row['TF']}]</span></div>
                <div class="sc-price" style="color:{roc_c}">{fmt_price(row['Price'])} {te}</div></div>
                <div style="text-align:right">
                  <div style="font-size:9px;color:#4a5568;font-family:Space Mono,monospace">SCORE</div>
                  <div style="font-size:22px;font-weight:700;color:{sc_col};font-family:Space Mono,monospace">{row['Score']}</div>
                </div>
              </div>
              <div class="sc-signal" style="color:{sc_col}">{sig}</div>
              <div class="sc-bars">{bars}</div>
              <div class="sc-stats">
                <div class="sc-stat">RSI <span style="color:{rsi_c}">{rsi_v}</span></div>
                <div class="sc-stat">STOCH <span>{row['Stoch K']:.0f}</span></div>
                <div class="sc-stat">RVOL <span>{row['RVOL']}x</span></div>
              </div>
              <div class="sc-stats" style="margin-top:6px">
                <div class="sc-stat">TP <span style="color:#00ff88">{fmt_price(row['TP'])}</span></div>
                <div class="sc-stat">SL <span style="color:#ff3d5a">{fmt_price(row['SL'])}</span></div>
                <div class="sc-stat">R:R <span>{row['R:R']}</span></div>
              </div>
              <div style="margin-top:6px;font-family:Space Mono,monospace;font-size:9px;color:#4a5568;">
                FR Sentiment: <span style="color:{fr_c}">{fr_v}</span>
              </div>
              <div style="margin-top:6px;font-size:10px;color:#4a5568;font-family:Space Mono,monospace;line-height:1.4">{row['Reasons'][:80]}</div>
            </div>"""
        ch+="</div>"
        st.markdown(ch,unsafe_allow_html=True)

        df_wl=pd.DataFrame([r for r in wl_res if r["Price"]>0])
        if not df_wl.empty:
            show=["Pair","TF","Price","Score","Signal","Trend","RSI-EMA","Stoch K","RVOL","BB%","ROC 3%","FR","TP","SL","R:R","Vol IDR(M)","Reasons"]
            show=[c for c in show if c in df_wl.columns]
            df_wl_d=df_wl[show].copy()
            df_wl_d["Price"]=df_wl_d["Price"].apply(fmt_price)
            df_wl_d["TP"]=df_wl_d["TP"].apply(fmt_price)
            df_wl_d["SL"]=df_wl_d["SL"].apply(fmt_price)
            st.dataframe(df_wl_d,width="stretch",hide_index=True,column_config={
                "Score":  st.column_config.ProgressColumn("Score",min_value=0,max_value=6,format="%.1f"),
                "RVOL":   st.column_config.NumberColumn("RVOL",format="%.2fx"),
                "ROC 3%": st.column_config.NumberColumn("ROC 3%",format="%.2f%%"),
            })

    if wl_tele and st.session_state.wl_results:
        to_send=[r for r in st.session_state.wl_results if r["Price"]>0]
        if to_send: send_telegram(to_send[:5],source="Watchlist"); st.success("📡 Terkirim!")

    if wl_share and st.session_state.wl_results:
        now_s=datetime.now(WIB).strftime("%d %b %Y %H:%M")
        wl_m=st.session_state.get("wl_mode","")
        txt=f"🚀 CRYPTO TURBO WATCHLIST\n⏰ {now_s} WIB\n📊 Mode: {wl_m} | BTC: {regime}\n"+"─"*28+"\n"
        for r in sorted(st.session_state.wl_results,key=lambda x:x["Score"],reverse=True):
            if r["Price"]==0: continue
            sig=r.get("Signal","-")
            em="💎" if "BAGGER" in sig or "KANDIDAT" in sig else("🔥" if "GACOR" in sig or "REVERSAL" in sig else "⚡")
            txt+=f"{em} {r['Coin']}[{r['TF']}] | {fmt_price(r['Price'])} | Score:{r['Score']} | RSI:{r['RSI-EMA']} | {sig}\n"
            if r.get("Reasons"): txt+=f"   → {r['Reasons'][:60]}\n"
        txt+="─"*28+"\nby Crypto Turbo 🚀 SPOT IDR (Indodax)"
        st.text_area("Copy:",txt,height=260,key="share_out")

    if not st.session_state.wl_results and not wl_run:
        st.markdown("""<div style="text-align:center;padding:48px;color:#4a5568;font-family:Space Mono,monospace;">
          <div style="font-size:32px;margin-bottom:12px;">👁️</div>
          <div style="font-size:12px;letter-spacing:2px;">MASUKKAN PAIR DI ATAS</div>
          <div style="font-size:10px;margin-top:8px;color:#2d3748;">Format: BTC, ETH, SOL (tanpa /IDR)</div>
        </div>""", unsafe_allow_html=True)

# ════════════════════════════════════════════════════
#  TAB 3: MARKET OVERVIEW
# ════════════════════════════════════════════════════
with tab_market:
    st.markdown('<div class="section-title">BTC Regime + Funding Rate Overview</div>',unsafe_allow_html=True)

    c1,c2=st.columns(2)
    with c1:
        st.markdown(f"""<div style="background:var(--surface);border:1px solid {rcolor}44;border-radius:8px;padding:16px;border-left:4px solid {rcolor};">
          <div style="font-family:Space Mono,monospace;font-size:11px;color:{rcolor};font-weight:700;">₿ BTC/IDR — {regime}</div>
          <div style="font-family:Space Mono,monospace;font-size:26px;font-weight:700;color:{rcolor};margin:8px 0;">{fmt_price(btc_price)}</div>
          <div style="font-family:Space Mono,monospace;font-size:12px;color:{chg_col}">{chg_sym} {abs(btc_chg):.2f}% hari ini</div>
          <div style="font-size:10px;color:#4a5568;margin-top:8px;">{regime_detail}</div>
          <div style="font-size:10px;color:#4a5568;margin-top:4px;">EMA20: {fmt_price(ema20)} · EMA55: {fmt_price(ema55)}</div>
        </div>""", unsafe_allow_html=True)

    with c2:
        st.markdown(f"""<div style="background:var(--surface);border:1px solid #1c2533;border-radius:8px;padding:16px;">
          <div style="font-family:Space Mono,monospace;font-size:11px;color:#4a5568;font-weight:700;">₿ BTC DOMINANCE</div>
          <div style="font-family:Space Mono,monospace;font-size:26px;font-weight:700;color:var(--btc);margin:8px 0;">{btc_dom_str}</div>
          <div style="font-size:11px;color:#c9d1d9;margin-top:4px;">{dom_label}</div>
          <div style="margin-top:10px;height:6px;background:#1c2533;border-radius:3px;overflow:hidden;">
            <div style="width:{btc_dom if btc_dom else 50}%;height:100%;background:var(--btc);border-radius:3px;"></div>
          </div>
          <div style="font-size:9px;color:#4a5568;margin-top:3px;">BTC {btc_dom_str} · Alt {f"{100-btc_dom:.1f}%" if btc_dom else "N/A"}</div>
        </div>""", unsafe_allow_html=True)

    st.markdown('<div class="section-title" style="margin-top:20px;">Funding Rate — Sentiment SPOT Trader</div>',unsafe_allow_html=True)
    st.markdown("""<div style="font-family:Space Mono,monospace;font-size:10px;color:#4a5568;margin-bottom:12px;
        padding:8px 12px;background:#0d1117;border-radius:6px;">
      ℹ️ <b style="color:#c9d1d9">SPOT trader:</b> FR positif tinggi = pasar terlalu long = risiko flush. FR negatif = banyak short = potensi squeeze ke atas.
    </div>""", unsafe_allow_html=True)

    if fr_data:
        fr_cols=st.columns(len(fr_data))
        for idx,(coin,data) in enumerate(fr_data.items()):
            rate=data["rate"]
            lbl,col=fr_label(rate)
            with fr_cols[idx]:
                st.markdown(f"""<div style="background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:12px;text-align:center;">
                  <div style="font-family:Space Mono,monospace;font-size:12px;font-weight:700;color:#c9d1d9;">{coin}</div>
                  <div style="font-family:Space Mono,monospace;font-size:20px;font-weight:700;color:{col};margin:6px 0;">{rate:+.4f}%</div>
                  <div style="font-size:9px;color:{col};">{lbl.split("—")[0].strip()}</div>
                </div>""", unsafe_allow_html=True)
    else:
        st.info("Funding rate tidak tersedia — cek koneksi Binance API")

    # Market heatmap top pairs
    st.markdown('<div class="section-title" style="margin-top:20px;">Quick Market Scan (Top 20 IDR Pairs)</div>',unsafe_allow_html=True)
    if st.button("🔄 Refresh Market Data",key="btn_market"):
        all_p=get_idr_pairs()[:20]
        ex=get_exchange()
        mkt_data=[]
        with st.spinner("Fetching top 20 pairs..."):
            for p in all_p:
                try:
                    tk=ex.fetch_ticker(p)
                    mkt_data.append({
                        "Pair":p,"Price":tk["last"],"Chg 24h%":tk["percentage"],
                        "Vol IDR(M)":round((tk["quoteVolume"] or 0)/1e6,1),
                        "High 24h":tk["high"],"Low 24h":tk["low"],
                    })
                except: pass
        if mkt_data:
            df_mkt=pd.DataFrame(mkt_data).sort_values("Chg 24h%",ascending=False)
            df_mkt["Price"]=df_mkt["Price"].apply(fmt_price)
            df_mkt["High 24h"]=df_mkt["High 24h"].apply(fmt_price)
            df_mkt["Low 24h"]=df_mkt["Low 24h"].apply(fmt_price)
            st.dataframe(df_mkt,width="stretch",hide_index=True,column_config={
                "Chg 24h%":st.column_config.NumberColumn("Chg 24h%",format="%.2f%%"),
                "Vol IDR(M)":st.column_config.NumberColumn("Vol IDR(M)",format="Rp%.0fM"),
            })

# ════════════════════════════════════════════════════
#  TAB 4: BACKTEST
# ════════════════════════════════════════════════════
with tab_backtest:
    st.markdown('<div class="section-title">Backtest Engine · Crypto SPOT · Indodax IDR</div>',unsafe_allow_html=True)
    st.markdown("""<div style="font-family:Space Mono,monospace;font-size:10px;color:#4a5568;margin-bottom:14px;
        padding:8px 12px;background:#0d1117;border-radius:6px;">
      ℹ️ Backtest pakai data historis dari Indodax. Pilih pair, timeframe, dan mode.
    </div>""", unsafe_allow_html=True)

    bt_c1,bt_c2,bt_c3,bt_c4=st.columns(4)
    bt_pair  =bt_c1.text_input("Pair (e.g. BTC)",value="BTC",key="bt_pair").upper()
    bt_mode  =bt_c2.selectbox("Mode",["Scalping ⚡","Momentum 🚀","Reversal 🎯","Bagger 💎"],key="bt_mode")
    bt_tf    =bt_c3.selectbox("Timeframe",["15m","1h","4h","1d"],key="bt_tf")
    bt_sc    =bt_c4.slider("Min Score",0,6,4,key="bt_sc")
    bt_c5,bt_c6=st.columns(2)
    bt_fwd   =int(bt_c5.number_input("Hold (bars)",value=4,step=1,min_value=1,max_value=50))
    bt_sl_m  =bt_c6.number_input("SL mult (x ATR)",value=1.0,step=0.1,min_value=0.1,max_value=5.0)
    st.caption(f"Hold {bt_fwd} bars × {bt_tf} = ~{bt_fwd * {'15m':15,'1h':60,'4h':240,'1d':1440}.get(bt_tf,15)} menit per trade")

    if st.button("🚀 Run Backtest",type="primary",key="bt_run"):
        sym=f"{bt_pair}/IDR"
        with st.spinner(f"Fetching {sym} {bt_tf} data..."):
            ex=get_exchange()
            try:
                raw=ex.fetch_ohlcv(sym,bt_tf,limit=500)
                if not raw or len(raw)<60:
                    st.error(f"Data {sym} kurang ({len(raw) if raw else 0} bars). Coba pair lain.")
                else:
                    df_bt=pd.DataFrame(raw,columns=["ts","Open","High","Low","Close","Volume"])
                    df_bt["ts"]=pd.to_datetime(df_bt["ts"],unit="ms",utc=True).dt.tz_convert(WIB)
                    df_bt.set_index("ts",inplace=True); df_bt=df_bt.astype(float)
                    df_bt=apply_indicators(df_bt)
                    bt_results=[]; is_bag_bt=(bt_mode=="Bagger 💎")
                    for ii in range(50,len(df_bt)-bt_fwd):
                        r0=df_bt.iloc[ii]; r1=df_bt.iloc[ii-1]; r2=df_bt.iloc[ii-2]
                        if bt_mode=="Scalping ⚡":   sc,_,_=score_scalping(r0,r1,r2)
                        elif bt_mode=="Momentum 🚀": sc,_,_=score_momentum(r0,r1,r2)
                        elif bt_mode=="Bagger 💎":   sc,_,_=score_bagger(r0,r1,r2,df_bt.iloc[:ii+1])
                        else:                         sc,_,_=score_reversal(r0,r1,r2)
                        if sc<bt_sc: continue
                        entry=float(r0["Close"]); atr_v=float(r0["ATR"]) if not np.isnan(float(r0["ATR"])) else entry*0.02
                        # SPOT: TP/SL sesuai mode
                        if bt_mode=="Scalping ⚡":   tp_p=entry+1.5*atr_v; sl_p=entry-bt_sl_m*atr_v
                        elif bt_mode=="Momentum 🚀": tp_p=entry+2.0*atr_v; sl_p=entry-bt_sl_m*atr_v
                        elif bt_mode=="Bagger 💎":   tp_p=entry+3.5*atr_v; sl_p=entry-1.2*atr_v
                        else:                         tp_p=entry+2.5*atr_v; sl_p=entry-bt_sl_m*atr_v
                        exit_p=float(df_bt.iloc[ii+bt_fwd]["Close"])
                        for fi in range(1,bt_fwd+1):
                            bar=df_bt.iloc[ii+fi]
                            if float(bar["High"])>=tp_p: exit_p=tp_p; break
                            if float(bar["Low"])<=sl_p:  exit_p=sl_p; break
                        bt_results.append((exit_p-entry)/entry*100)
                    st.session_state.bt_results_data=bt_results
            except Exception as be:
                st.error(f"Error: {str(be)[:100]}")

    if st.session_state.bt_results_data:
        arr=np.array(st.session_state.bt_results_data)
        if len(arr)>0:
            wr=len(arr[arr>0])/len(arr)*100; avg=np.mean(arr); med=np.median(arr)
            pf=arr[arr>0].sum()/max(abs(arr[arr<0].sum()),0.01)
            mxdd=arr[arr<0].min() if len(arr[arr<0])>0 else 0
            st.markdown(f"""<div style="background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:20px;margin-top:12px;">
              <div style="font-family:Space Mono,monospace;font-size:10px;color:#4a5568;margin-bottom:14px;">
                {len(arr)} TRADES · {bt_pair}/IDR · {bt_tf} · SCORE≥{bt_sc} · HOLD {bt_fwd} BARS · {bt_mode}
              </div>
              <div style="display:flex;flex-wrap:wrap;gap:24px;">
                <div><div style="font-family:Space Mono,monospace;font-size:22px;font-weight:700;color:{'#00ff88' if wr>=55 else '#ffb700' if wr>=50 else '#ff3d5a'}">{wr:.1f}%</div><div style="font-size:10px;color:#4a5568">Win Rate</div></div>
                <div><div style="font-family:Space Mono,monospace;font-size:22px;font-weight:700;color:{'#00ff88' if avg>0 else '#ff3d5a'}">{avg:+.2f}%</div><div style="font-size:10px;color:#4a5568">Avg Return</div></div>
                <div><div style="font-family:Space Mono,monospace;font-size:22px;font-weight:700;color:#00e5ff">{med:+.2f}%</div><div style="font-size:10px;color:#4a5568">Median</div></div>
                <div><div style="font-family:Space Mono,monospace;font-size:22px;font-weight:700;color:{'#00ff88' if pf>=1.5 else '#ffb700' if pf>=1 else '#ff3d5a'}">{pf:.2f}x</div><div style="font-size:10px;color:#4a5568">Profit Factor</div></div>
                <div><div style="font-family:Space Mono,monospace;font-size:22px;font-weight:700;color:#ff3d5a">{mxdd:.1f}%</div><div style="font-size:10px;color:#4a5568">Max Loss</div></div>
                <div><div style="font-family:Space Mono,monospace;font-size:22px;font-weight:700;color:#00ff88">{sum(1 for x in arr if x>0)}</div><div style="font-size:10px;color:#4a5568">TP Hits</div></div>
                <div><div style="font-family:Space Mono,monospace;font-size:22px;font-weight:700;color:#ff3d5a">{sum(1 for x in arr if x<0)}</div><div style="font-size:10px;color:#4a5568">SL Hits</div></div>
              </div>
            </div>""", unsafe_allow_html=True)
        else:
            st.warning("Tidak ada trades yang match. Turunkan Min Score atau pilih pair lebih liquid.")

# ════════════════════════════════════════════════════
#  FOOTER + AUTO-REFRESH
# ════════════════════════════════════════════════════
_now_f=now_wib.timestamp()
if st.session_state.last_scan_time:
    _rem=max(0,300-(_now_f-st.session_state.last_scan_time))
    mnt=int(_rem//60); sec=int(_rem%60)
    last_t=datetime.fromtimestamp(st.session_state.last_scan_time,WIB).strftime("%H:%M:%S")
    time_info=f"⏱️ Next: <span style='color:#f7931a'>{mnt:02d}:{sec:02d}</span> · Last: <span style='color:#2dd4bf'>{last_t} WIB</span>"
else:
    _rem=300; time_info="⏱️ Klik Scan untuk mulai"

st.markdown(f"""
<div style="margin-top:28px;padding-top:14px;border-top:1px solid #1c2533;
     display:flex;justify-content:space-between;flex-wrap:wrap;gap:8px;">
  <div style="font-family:Space Mono,monospace;font-size:10px;color:#4a5568;">
    🚀 Crypto Turbo · SPOT IDR · Indodax · Wyckoff Bagger · 24/7
  </div>
  <div style="font-family:Space Mono,monospace;font-size:10px;color:#4a5568;">{time_info}</div>
</div>""", unsafe_allow_html=True)

# Auto-rerun
if st.session_state.last_scan_time:
    if _now_f-st.session_state.last_scan_time>=295:
        time.sleep(5)
        st.rerun()

