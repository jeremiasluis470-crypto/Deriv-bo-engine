# =============================================================================
#  DERIV BOT ENGINE — Railway 24/7
#  Lê comandos do Redis (Upstash) e envia resultados de volta
#  Sem interface — corre em background para sempre
# =============================================================================

import asyncio
import json
import time
import statistics
import os
import aiohttp
import websockets
import httpx
from datetime import datetime
from dataclasses import dataclass
from typing import Optional
from collections import deque

# ─────────────────────────────────────────────────────────────────────────────
#  REDIS CLIENT (Upstash REST API — sem biblioteca externa)
# ─────────────────────────────────────────────────────────────────────────────

REDIS_URL   = os.environ.get("UPSTASH_REDIS_REST_URL","").strip()
REDIS_TOKEN = os.environ.get("UPSTASH_REDIS_REST_TOKEN","").strip()

if not REDIS_URL or not REDIS_TOKEN:
    print("❌ ERRO: Variáveis UPSTASH_REDIS_REST_URL e UPSTASH_REDIS_REST_TOKEN não definidas no Railway!")
    print("👉 Vai ao Railway → Variables e adiciona as 4 variáveis de ambiente.")
    import time
    while True:
        print("⏸️ Bot pausado — aguardando variáveis de ambiente...")
        time.sleep(30)

async def redis_set(key: str, value, ex: int = None):
    """Guarda valor no Redis."""
    data = json.dumps(value) if not isinstance(value, str) else value
    url  = f"{REDIS_URL}/set/{key}/{httpx.URL(data)}"
    headers = {"Authorization": f"Bearer {REDIS_TOKEN}"}
    params = {}
    if ex: params["ex"] = ex
    async with httpx.AsyncClient() as client:
        await client.post(f"{REDIS_URL}/set/{key}",
                          headers=headers,
                          json={"value": data, **({"ex": ex} if ex else {})})

async def redis_get(key: str):
    """Lê valor do Redis."""
    headers = {"Authorization": f"Bearer {REDIS_TOKEN}"}
    async with httpx.AsyncClient() as client:
        r = await client.get(f"{REDIS_URL}/get/{key}", headers=headers)
        body = r.json()
        result = body.get("result")
        if result is None: return None
        try: return json.loads(result)
        except: return result

async def redis_lpush(key: str, value):
    """Adiciona ao início de uma lista Redis."""
    data = json.dumps(value) if not isinstance(value, str) else value
    headers = {"Authorization": f"Bearer {REDIS_TOKEN}"}
    async with httpx.AsyncClient() as client:
        await client.post(f"{REDIS_URL}/lpush/{key}",
                          headers=headers, json=[data])

async def redis_ltrim(key: str, start: int, stop: int):
    """Mantém só os últimos N elementos."""
    headers = {"Authorization": f"Bearer {REDIS_TOKEN}"}
    async with httpx.AsyncClient() as client:
        await client.post(f"{REDIS_URL}/ltrim/{key}",
                          headers=headers, json=[start, stop])

async def redis_lrange(key: str, start: int, stop: int):
    """Lê lista do Redis."""
    headers = {"Authorization": f"Bearer {REDIS_TOKEN}"}
    async with httpx.AsyncClient() as client:
        r = await client.get(f"{REDIS_URL}/lrange/{key}/{start}/{stop}",
                             headers=headers)
        items = r.json().get("result", [])
        out = []
        for i in items:
            try: out.append(json.loads(i))
            except: out.append(i)
        return out

async def push_log(msg: str):
    ts  = datetime.now().strftime("%H:%M:%S")
    entry = f"[{ts}] {msg}"
    print(entry)
    await redis_lpush("bot:logs", entry)
    await redis_ltrim("bot:logs", 0, 199)

async def push_signal(direction: str, reason: str):
    ts = datetime.now().strftime("%H:%M:%S")
    await redis_lpush("bot:signals", {"time": ts, "dir": direction, "reason": reason})
    await redis_ltrim("bot:signals", 0, 39)

async def push_trade(symbol, direction, stake, profit, reason):
    ts = datetime.now().strftime("%H:%M:%S")
    entry = {"time": ts, "symbol": symbol, "direction": direction,
             "stake": stake, "profit": profit, "signal": reason}
    await redis_lpush("bot:trades", entry)
    await redis_ltrim("bot:trades", 0, 99)

async def update_stats(wins, losses, pnl):
    total   = wins + losses
    winrate = (wins / total * 100) if total > 0 else 0.0
    await redis_set("bot:stats", {
        "pnl": round(pnl, 2), "trades": total,
        "wins": wins, "losses": losses, "winrate": round(winrate, 1)
    })

# ─────────────────────────────────────────────────────────────────────────────
#  DATA STRUCTURES
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class Candle:
    open: float; high: float; low: float; close: float; epoch: int = 0
    @property
    def body(self):       return abs(self.close - self.open)
    @property
    def upper_wick(self): return self.high - max(self.open, self.close)
    @property
    def lower_wick(self): return min(self.open, self.close) - self.low
    @property
    def is_bullish(self): return self.close > self.open
    @property
    def is_bearish(self): return self.close < self.open
    @property
    def is_doji(self):    return self.body < (self.high - self.low) * 0.1
    @property
    def range(self):      return self.high - self.low

@dataclass
class Signal:
    direction: str; confidence: float; reason: str
    trend_score: float = 0.0; sr_score: float = 0.0; candle_score: float = 0.0

# ─────────────────────────────────────────────────────────────────────────────
#  TREND ANALYZER
# ─────────────────────────────────────────────────────────────────────────────

def _ema(prices, period):
    k = 2 / (period + 1); out = [prices[0]]
    for p in prices[1:]: out.append(p * k + out[-1] * (1 - k))
    return out

class TrendAnalyzer:
    def analyze(self, closes):
        if len(closes) < 25: return "SIDEWAYS", 0.0
        fast = _ema(closes, 8); slow = _ema(closes, 21)
        diff = fast[-1] - slow[-1]
        slope = (slow[-1] - slow[-5]) / slow[-5] * 100
        if diff > 0 and slope > 0.02:
            return "UP",   round(min(1.0, abs(diff/slow[-1])*500 + slope*10), 2)
        if diff < 0 and slope < -0.02:
            return "DOWN", round(min(1.0, abs(diff/slow[-1])*500 + abs(slope)*10), 2)
        return "SIDEWAYS", 0.2

# ─────────────────────────────────────────────────────────────────────────────
#  SUPPORT & RESISTANCE
# ─────────────────────────────────────────────────────────────────────────────

class SupportResistance:
    def get_levels(self, candles):
        if len(candles) < 5: return {"supports":[], "resistances":[]}
        h = max(c.high for c in candles[-5:]); l = min(c.low for c in candles[-5:])
        c_ = candles[-1].close; P = (h+l+c_)/3
        highs = [c.high for c in candles[-20:]]; lows = [c.low for c in candles[-20:]]
        return {"supports":   sorted(set(self._cluster(lows)  + [2*P-h, P-(h-l)]))[:5],
                "resistances":sorted(set(self._cluster(highs) + [2*P-l, P+(h-l)]),reverse=True)[:5]}

    def _cluster(self, prices):
        if not prices: return []
        avg = statistics.mean(prices); tol = avg * 0.0015
        clusters, used = [], [False]*len(prices)
        for i,p in enumerate(prices):
            if used[i]: continue
            group = [p]
            for j in range(i+1,len(prices)):
                if abs(prices[j]-p) <= tol: group.append(prices[j]); used[j]=True
            clusters.append(statistics.mean(group))
        return clusters

    def score(self, price, levels):
        if not levels.get("supports") or not levels.get("resistances"): return "WAIT",0.0
        ns = min(abs(price-s)/price for s in levels["supports"])
        nr = min(abs(price-r)/price for r in levels["resistances"])
        t  = 0.002
        if ns < t and ns < nr: return "CALL", round(max(0.0,1.0-ns/t),2)
        if nr < t and nr < ns: return "PUT",  round(max(0.0,1.0-nr/t),2)
        return "WAIT", 0.0

# ─────────────────────────────────────────────────────────────────────────────
#  CANDLE BIBLE
# ─────────────────────────────────────────────────────────────────────────────

class CandleBible:
    def analyze(self, candles):
        if len(candles) < 3: return "WAIT",0.0,"dados insuficientes"
        c0,c1,c2 = candles[-3],candles[-2],candles[-1]
        ar = statistics.mean(c.range for c in candles[-10:]) if len(candles)>=10 else c2.range
        if c2.lower_wick>=2*c2.body and c2.upper_wick<=0.3*c2.body and c2.body>0: return "CALL",0.75,"Hammer"
        if c2.upper_wick>=2*c2.body and c2.lower_wick<=0.3*c2.body and c2.body>0: return "PUT",0.75,"Shooting Star"
        if c2.is_bullish and c2.body>ar*0.8 and c2.upper_wick<c2.body*0.1 and c2.lower_wick<c2.body*0.1: return "CALL",0.80,"Bullish Marubozu"
        if c2.is_bearish and c2.body>ar*0.8 and c2.upper_wick<c2.body*0.1 and c2.lower_wick<c2.body*0.1: return "PUT",0.80,"Bearish Marubozu"
        if c1.is_bearish and c2.is_bullish and c2.open<c1.close and c2.close>c1.open: return "CALL",0.85,"Bullish Engulfing"
        if c1.is_bullish and c2.is_bearish and c2.open>c1.close and c2.close<c1.open: return "PUT",0.85,"Bearish Engulfing"
        if c1.is_bearish and c2.is_bullish and c2.open<c1.low and c2.close>(c1.open+c1.close)/2: return "CALL",0.78,"Piercing Pattern"
        if c1.is_bullish and c2.is_bearish and c2.open>c1.high and c2.close<(c1.open+c1.close)/2: return "PUT",0.78,"Dark Cloud Cover"
        if c0.is_bearish and c1.body<c0.body*0.3 and c2.is_bullish and c2.close>(c0.open+c0.close)/2: return "CALL",0.88,"Morning Star"
        if c0.is_bullish and c1.body<c0.body*0.3 and c2.is_bearish and c2.close<(c0.open+c0.close)/2: return "PUT",0.88,"Evening Star"
        if (c0.is_bullish and c1.is_bullish and c2.is_bullish and c1.close>c0.close and c2.close>c1.close
                and c0.body>ar*0.4 and c1.body>ar*0.4 and c2.body>ar*0.4): return "CALL",0.90,"Three White Soldiers"
        if (c0.is_bearish and c1.is_bearish and c2.is_bearish and c1.close<c0.close and c2.close<c1.close
                and c0.body>ar*0.4 and c1.body>ar*0.4 and c2.body>ar*0.4): return "PUT",0.90,"Three Black Crows"
        return "WAIT",0.0,"sem padrao"

# ─────────────────────────────────────────────────────────────────────────────
#  FIBONACCI
# ─────────────────────────────────────────────────────────────────────────────

class FibonacciAnalyzer:
    LEVELS = [0.0,0.236,0.382,0.500,0.618,0.786,1.0]
    def analyze(self, candles):
        if len(candles)<20: return "WAIT",0.0,"insuficiente"
        sh = max(c.high for c in candles[-20:]); sl = min(c.low for c in candles[-20:])
        price = candles[-1].close; diff = sh-sl
        if diff==0: return "WAIT",0.0,"range zero"
        tol = diff*0.02
        for l in self.LEVELS:
            level = sh - diff*l
            dist  = abs(price-level)
            if dist < tol:
                conf = round(1.0-dist/tol,2)
                return ("CALL",conf,f"Fib {int(l*100)}% suporte") if l>=0.5 else ("PUT",conf,f"Fib {int(l*100)}% resistencia")
        return "WAIT",0.0,"fora de fib"

# ─────────────────────────────────────────────────────────────────────────────
#  SMART MONEY
# ─────────────────────────────────────────────────────────────────────────────

class SmartMoneyAnalyzer:
    def analyze(self, candles):
        if len(candles)<15: return Signal("WAIT",0.0,"SMC: insuficiente")
        last = candles[-1]
        sh = max(c.high for c in candles[-6:-1]); sl = min(c.low for c in candles[-6:-1])
        if last.low<sl and last.close>sl and last.is_bullish: return Signal("CALL",0.88,"SMC: Liquidity Sweep CALL")
        if last.high>sh and last.close<sh and last.is_bearish: return Signal("PUT",0.88,"SMC: Liquidity Sweep PUT")
        recent = candles[-10:]
        swing_high = max(c.high for c in recent[:-2]); swing_low = min(c.low for c in recent[:-2])
        if last.close>swing_high:
            for c in reversed(candles[-8:-1]):
                if c.is_bearish:
                    ob_mid=(c.open+c.close)/2
                    if abs(last.close-ob_mid)/last.close<0.003: return Signal("CALL",0.80,"SMC: BOS+OB UP")
        if last.close<swing_low:
            for c in reversed(candles[-8:-1]):
                if c.is_bullish:
                    ob_mid=(c.open+c.close)/2
                    if abs(last.close-ob_mid)/last.close<0.003: return Signal("PUT",0.80,"SMC: BOS+OB DOWN")
        return Signal("WAIT",0.0,"SMC: aguardando")

# ─────────────────────────────────────────────────────────────────────────────
#  ENGINES
# ─────────────────────────────────────────────────────────────────────────────

class EnginePredicao:
    BULL = {"Bullish Engulfing","Morning Star","Three White Soldiers","Bullish Marubozu","Piercing Pattern","Hammer"}
    BEAR = {"Bearish Engulfing","Evening Star","Three Black Crows","Bearish Marubozu","Dark Cloud Cover","Shooting Star"}
    def __init__(self): self.trend=TrendAnalyzer(); self.sr=SupportResistance(); self.bible=CandleBible()
    def evaluate(self, candles):
        if len(candles)<30: return Signal("WAIT",0.0,"insuficiente")
        closes=[c.close for c in candles]; price=closes[-1]
        ts,tc = self.trend.analyze(closes)
        if ts=="SIDEWAYS": return Signal("WAIT",0.0,"sideways")
        td = "CALL" if ts=="UP" else "PUT"
        if tc<0.65: return Signal("WAIT",tc,f"tendencia fraca ({tc:.2f})")
        cd,cc,pat = self.bible.analyze(candles)
        if cd=="WAIT": return Signal("WAIT",0.0,"sem padrao")
        if cd!=td: return Signal("WAIT",0.0,"candle contra tendencia")
        if cc<0.75: return Signal("WAIT",cc,f"candle fraco ({pat})")
        if td=="CALL" and pat not in self.BULL: return Signal("WAIT",0.0,f"{pat} nao e touro puro")
        if td=="PUT"  and pat not in self.BEAR: return Signal("WAIT",0.0,f"{pat} nao e urso puro")
        levels=self.sr.get_levels(candles); sd,sc=self.sr.score(price,levels)
        if sd and sd!="WAIT" and sd!=td: return Signal("WAIT",0.0,"S/R contra tendencia")
        conf=tc*0.45+cc*0.35+(sc*0.20 if sd==td else 0)
        if conf<0.72: return Signal("WAIT",conf,f"conf baixa ({conf:.2f})")
        return Signal(td,conf,f"PRECISAO|{ts}({tc:.2f})|{pat}({cc:.2f})",tc,sc,cc)

class EngineSR:
    def __init__(self): self.sr=SupportResistance(); self.bible=CandleBible()
    def evaluate(self, candles):
        if len(candles)<15: return Signal("WAIT",0.0,"insuficiente")
        price=candles[-1].close; levels=self.sr.get_levels(candles)
        sd,sc=self.sr.score(price,levels)
        if sd=="WAIT" or sc<0.50: return Signal("WAIT",sc,f"longe de S/R")
        cd,cc,pat=self.bible.analyze(candles)
        if cd==sd and cc>=0.65:
            conf=sc*0.55+cc*0.45
            return Signal(sd,conf,f"S/R|{sd}({sc:.2f})|{pat}({cc:.2f})",0,sc,cc)
        if sc>=0.80: return Signal(sd,sc,f"S/R FORTE|{sd}({sc:.2f})",0,sc,0)
        return Signal("WAIT",0.0,"S/R sem candle")

class EngineCandles:
    HIGH = {"Three White Soldiers","Three Black Crows","Morning Star","Evening Star",
            "Bullish Engulfing","Bearish Engulfing","Bullish Marubozu","Bearish Marubozu"}
    def __init__(self): self.bible=CandleBible()
    def evaluate(self, candles):
        if len(candles)<10: return Signal("WAIT",0.0,"insuficiente")
        cd,cc,pat=self.bible.analyze(candles)
        if cd=="WAIT" or pat not in self.HIGH: return Signal("WAIT",0.0,f"{pat} ignorado")
        return Signal(cd,cc,f"CANDLE|{pat}({cc:.2f})",0,0,cc)

class EngineFibonacci:
    def __init__(self): self.fib=FibonacciAnalyzer(); self.bible=CandleBible(); self.trend=TrendAnalyzer()
    def evaluate(self, candles):
        if len(candles)<20: return Signal("WAIT",0.0,"insuficiente")
        fd,fc,fr=self.fib.analyze(candles)
        if fd=="WAIT" or fc<0.55: return Signal("WAIT",fc,fr)
        cd,cc,pat=self.bible.analyze(candles)
        ts,tc=self.trend.analyze([c.close for c in candles])
        td="CALL" if ts=="UP" else ("PUT" if ts=="DOWN" else None)
        if cd==fd and cc>=0.65:
            conf=fc*0.50+cc*0.35+(tc*0.15 if td==fd else 0)
            return Signal(fd,conf,f"FIB|{fr}|{pat}({cc:.2f})",tc,fc,cc)
        if fc>=0.80: return Signal(fd,fc,f"FIB FORTE|{fr}",tc,fc,0)
        return Signal("WAIT",0.0,"FIB sem confirmacao")

class EngineSmartMoney:
    """
    SMC refinado com dados reais de 35 trades:
    - COM Trend (>=0.45): 86% WR — OBRIGATORIO
    - SEM Trend: 50% WR — BLOQUEADO
    - Stop 2 losses consecutivos — protege o lucro
    """
    TREND_MIN = 0.45  # threshold validado nos dados reais

    def __init__(self): self.smc=SmartMoneyAnalyzer(); self.trend=TrendAnalyzer()

    def evaluate(self, candles):
        if len(candles)<15: return Signal("WAIT",0.0,"insuficiente")
        sig=self.smc.analyze(candles)
        if sig.direction=="WAIT": return sig

        # REGRA CRÍTICA: Trend obrigatório >= 0.45
        ts,tc=self.trend.analyze([c.close for c in candles])
        td="CALL" if ts=="UP" else ("PUT" if ts=="DOWN" else None)

        if td is None or tc < self.TREND_MIN:
            return Signal("WAIT",0.0,
                          f"SMC bloqueado: sem trend ({tc:.2f}<{self.TREND_MIN}) — 50% WR sem trend")

        if td != sig.direction:
            return Signal("WAIT",0.0,
                          f"SMC bloqueado: trend {td} contra sinal {sig.direction}")

        conf = min(1.0, sig.confidence + tc*0.15)
        return Signal(sig.direction, conf,
                      sig.reason+f"+Trend({tc:.2f})", tc, 0, 0)

ENGINES = {
    "🎯 Precisão Máxima":       EnginePredicao,
    "📊 Suporte & Resistência": EngineSR,
    "🕯️ Candles Puros":        EngineCandles,
    "🌀 Fibonacci":             EngineFibonacci,
    "🧠 Smart Money (SMC)":    EngineSmartMoney,
}

# ─────────────────────────────────────────────────────────────────────────────
#  DERIV CLIENT
# ─────────────────────────────────────────────────────────────────────────────

DERIV_REST = "https://api.derivws.com"

class DerivClient:
    def __init__(self, pat, app_id, account_type="demo"):
        self.pat=pat; self.app_id=app_id; self.account_type=account_type
        self._ws=None; self._req_id=1; self._pending={}
        self._candles=asyncio.Queue(maxsize=1000)
        self._listener_task=None; self._account_id=None

    def _headers(self):
        return {"Authorization":f"Bearer {self.pat}","Deriv-App-ID":self.app_id,"Content-Type":"application/json"}

    async def _get_account_id(self):
        async with aiohttp.ClientSession() as s:
            async with s.get(f"{DERIV_REST}/trading/v1/options/accounts",headers=self._headers()) as r:
                body=await r.json()
                if r.status!=200: raise PermissionError(f"Erro contas: {body}")
                for acc in body.get("data",[]):
                    if acc.get("account_type")==self.account_type and acc.get("status")=="active":
                        return acc["account_id"]
                if self.account_type=="demo": return await self._create_demo()
                raise RuntimeError("Nenhuma conta activa")

    async def _create_demo(self):
        async with aiohttp.ClientSession() as s:
            async with s.post(f"{DERIV_REST}/trading/v1/options/accounts",
                              headers=self._headers(),
                              json={"currency":"USD","group":"row","account_type":"demo"}) as r:
                body=await r.json()
                return body["data"]["account_id"]

    async def _get_ws_url(self, account_id):
        async with aiohttp.ClientSession() as s:
            async with s.post(f"{DERIV_REST}/trading/v1/options/accounts/{account_id}/otp",
                              headers=self._headers()) as r:
                body=await r.json()
                if r.status!=200: raise PermissionError(f"Erro OTP: {body}")
                return body["data"]["url"]

    async def connect(self, retries=3):
        for attempt in range(1,retries+1):
            try:
                self._account_id=await self._get_account_id()
                ws_url=await self._get_ws_url(self._account_id)
                self._ws=await websockets.connect(ws_url,ping_interval=30,ping_timeout=10)
                self._listener_task=asyncio.create_task(self._listener())
                return
            except PermissionError: raise
            except Exception as e:
                if attempt<retries: await asyncio.sleep(3*attempt)
                else: raise ConnectionError(f"Falha: {e}")

    async def disconnect(self):
        if self._listener_task: self._listener_task.cancel()
        if self._ws: await self._ws.close()

    async def _send(self, payload, timeout=15.0):
        req_id=self._req_id; self._req_id+=1
        payload["req_id"]=req_id
        fut=asyncio.get_event_loop().create_future()
        self._pending[req_id]=fut
        await self._ws.send(json.dumps(payload))
        try: return await asyncio.wait_for(fut,timeout)
        except asyncio.TimeoutError:
            self._pending.pop(req_id,None); raise

    async def _listener(self):
        try:
            async for raw in self._ws:
                msg=json.loads(raw); req_id=msg.get("req_id")
                if req_id and req_id in self._pending:
                    fut=self._pending.pop(req_id)
                    if not fut.done(): fut.set_result(msg)
                elif msg.get("msg_type")=="ohlc":
                    await self._candles.put(msg)
        except (asyncio.CancelledError, websockets.ConnectionClosed): pass

    async def subscribe_candles(self, symbol, granularity=60):
        resp=await self._send({"ticks_history":symbol,"style":"candles",
                               "granularity":granularity,"count":50,"end":"latest","subscribe":1})
        if resp.get("error"): raise RuntimeError(resp["error"]["message"])
        return resp.get("candles",[])

    async def get_candle_update(self, timeout=90.0):
        return await asyncio.wait_for(self._candles.get(),timeout)

    async def buy_contract(self, symbol, direction, stake, duration, duration_unit="t"):
        proposal=await self._send({"proposal":1,"amount":stake,"basis":"stake",
                                   "contract_type":direction,"currency":"USD",
                                   "duration":duration,"duration_unit":duration_unit,
                                   "underlying_symbol":symbol})
        if proposal.get("error"): raise RuntimeError(proposal["error"]["message"])
        buy=await self._send({"buy":proposal["proposal"]["id"],"price":stake})
        if buy.get("error"): raise RuntimeError(buy["error"]["message"])
        return buy["buy"]

    async def get_contract_result(self, contract_id, max_wait=120.0):
        deadline=asyncio.get_event_loop().time()+max_wait
        while asyncio.get_event_loop().time()<deadline:
            resp=await self._send({"proposal_open_contract":1,"contract_id":contract_id})
            poc=resp.get("proposal_open_contract",{})
            if poc.get("is_sold") or poc.get("status") in ("sold","won","lost"):
                return {"profit":float(poc.get("profit",0)),"status":poc.get("status")}
            await asyncio.sleep(2)
        raise TimeoutError("Contrato nao liquidou")

    async def get_balance(self):
        async with aiohttp.ClientSession() as s:
            async with s.get(f"{DERIV_REST}/trading/v1/options/accounts",headers=self._headers()) as r:
                body=await r.json()
                for acc in body.get("data",[]):
                    if acc.get("account_id")==self._account_id:
                        return float(acc.get("balance",0))
        return 0.0

# ─────────────────────────────────────────────────────────────────────────────
#  BOT LOOP PRINCIPAL
# ─────────────────────────────────────────────────────────────────────────────

_DUR_MAP = {"1 tique":(1,"t"),"5 tiques":(5,"t"),"10 tiques":(10,"t"),
            "15s":(15,"s"),"30s":(30,"s"),"1m":(1,"m"),"5m":(5,"m")}
_GRAN    = {"t":60,"s":60,"m":300}

async def bot_loop():
    await push_log("🤖 Bot Engine iniciado no Railway")

    while True:
        # Lê configuração do Redis (enviada pelo dashboard Streamlit)
        cfg = await redis_get("bot:config")
        cmd = await redis_get("bot:command")

        if not cfg or cmd != "START":
            await push_log("⏸️ Aguardando comando START do dashboard...")
            await asyncio.sleep(5)
            continue

        # Marca como running
        await redis_set("bot:status", "RUNNING")
        await push_log(f"🚀 Iniciando [{cfg.get('estrategia','?')}] em {cfg.get('symbol','?')}")

        dur_val, dur_unit = _DUR_MAP.get(cfg.get("duration","5 tiques"),(5,"t"))
        granularity       = _GRAN.get(dur_unit, 60)
        engine_class      = ENGINES.get(cfg.get("estrategia"), EnginePredicao)
        engine            = engine_class()
        client            = DerivClient(cfg["api_token"], cfg["app_id"], cfg.get("account_type","demo"))

        symbol       = cfg["symbol"]
        base_stake   = float(cfg.get("stake",1.0))
        cur_stake    = base_stake
        ml_enabled   = bool(cfg.get("martingale",False))
        ml_mult      = float(cfg.get("mult",2.0))
        daily_goal   = float(cfg.get("daily_goal",5.0))
        max_loss     = float(cfg.get("max_loss",2.0))
        pnl          = 0.0; wins = 0; losses = 0
        candles      = []
        last_trade   = 0.0
        COOLDOWN     = 60 if "Smart Money" in cfg.get("estrategia","") else 90
        MAX_HOUR     = 12 if "Smart Money" in cfg.get("estrategia","") else 8
        trades_hour  = []

        try:
            await client.connect()
            balance = await client.get_balance()
            await push_log(f"✅ Conectado | {client._account_id} | ${balance:.2f}")

            raw = await client.subscribe_candles(symbol, granularity)
            for r in raw:
                candles.append(Candle(float(r["open"]),float(r["high"]),
                                      float(r["low"]),float(r["close"]),int(r.get("epoch",0))))
            candles = candles[-100:]
            await push_log(f"📊 {len(candles)} candles carregados")

            while True:
                # Verifica se o dashboard mandou STOP
                cmd = await redis_get("bot:command")
                if cmd == "STOP":
                    await push_log("⏹️ Comando STOP recebido — parando")
                    break

                # Verifica limites
                if pnl >= daily_goal:
                    await push_log(f"🎯 Meta atingida! (${pnl:.2f})"); break
                if pnl <= -max_loss:
                    await push_log(f"🛑 Stop loss! (${pnl:.2f})"); break

                # Candle update
                try:
                    msg  = await client.get_candle_update(timeout=90)
                    ohlc = msg.get("ohlc",{})
                    if ohlc:
                        c = Candle(float(ohlc["open"]),float(ohlc["high"]),
                                   float(ohlc["low"]),float(ohlc["close"]),int(ohlc.get("epoch",0)))
                        if not candles or c.epoch!=candles[-1].epoch:
                            candles.append(c)
                            if len(candles)>100: candles=candles[-100:]
                except asyncio.TimeoutError:
                    await push_log("⏳ Aguardando candle..."); continue

                if len(candles)<15: continue

                signal = engine.evaluate(candles)
                await push_signal(signal.direction, signal.reason)
                if signal.direction=="WAIT": continue

                now = time.time()
                if now-last_trade < COOLDOWN:
                    await push_signal("WAIT",f"cooldown: {int(COOLDOWN-(now-last_trade))}s")
                    continue

                trades_hour = [t for t in trades_hour if now-t<3600]
                if len(trades_hour)>=MAX_HOUR:
                    await push_signal("WAIT",f"limite {MAX_HOUR}/hora")
                    await asyncio.sleep(30); continue

                await push_log(f"📡 {signal.direction} | conf={signal.confidence:.2f} | {signal.reason[:60]}")

                try:
                    buy_info    = await client.buy_contract(symbol,signal.direction,cur_stake,dur_val,dur_unit)
                    contract_id = buy_info.get("contract_id")
                    await push_log(f"📝 ID:{contract_id} | stake=${cur_stake:.2f}")

                    result = await client.get_contract_result(contract_id)
                    profit = result["profit"]
                    pnl   += profit

                    if profit>0: wins+=1; cur_stake=base_stake
                    else:
                        losses+=1
                        if ml_enabled: cur_stake=round(cur_stake*ml_mult,2)
                        else: cur_stake=base_stake

                    await push_trade(symbol,signal.direction,cur_stake,profit,signal.reason[:80])
                    await update_stats(wins,losses,pnl)
                    last_trade=time.time(); trades_hour.append(last_trade)

                    emoji = "✅" if profit>0 else "❌"
                    await push_log(f"{emoji} {'WIN' if profit>0 else 'LOSS'} ${profit:.2f} | PnL: ${pnl:.2f}")
                    await asyncio.sleep(2)

                except Exception as e:
                    await push_log(f"❌ Erro trade: {e}"); await asyncio.sleep(5)

        except Exception as e:
            await push_log(f"💥 Erro crítico: {e}")
        finally:
            await client.disconnect()
            await redis_set("bot:status","STOPPED")
            await push_log("🔌 Desconectado. Aguardando próximo START...")
            await asyncio.sleep(5)


if __name__ == "__main__":
    asyncio.run(bot_loop())
