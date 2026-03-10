import csv
import json
import os
import time
from datetime import datetime, timedelta

import ccxt
import pandas as pd
import pytz
import requests
from dotenv import load_dotenv
from openai import OpenAI

# ==========================================
# 1. 초기 환경 및 API 세팅
# ==========================================
load_dotenv()
api_key = os.getenv("BYBIT_API_KEY")
secret = os.getenv("BYBIT_SECRET_KEY")
telegram_token = os.getenv("TELEGRAM_TOKEN")
telegram_chat_id = os.getenv("TELEGRAM_CHAT_ID")

exchange = ccxt.bybit(
    {
        "apiKey": api_key,
        "secret": secret,
        "enableRateLimit": True,
        "options": {
            "defaultType": "swap",
            "adjustForTimeDifference": True,
        },
    }
)

client = OpenAI()

# 종목별 최대 허용 손절폭
SL_CAPS = {
    "BTC/USDT:USDT": 1.8,
    "ETH/USDT:USDT": 2.2,
    "SOL/USDT:USDT": 3.0,
    "XRP/USDT:USDT": 3.2,
    "SUI/USDT:USDT": 4.0,
    "AVAX/USDT:USDT": 3.0,
    "LINK/USDT:USDT": 2.5,
    "NEAR/USDT:USDT": 3.5,
    "DOGE/USDT:USDT": 3.5,
    "OP/USDT:USDT": 3.5,
}


def send_telegram_msg(text):
    if not telegram_token or not telegram_chat_id:
        return
    url = f"https://api.telegram.org/bot{telegram_token}/sendMessage"
    try:
        requests.post(
            url,
            json={"chat_id": telegram_chat_id, "text": text, "parse_mode": "Markdown"},
            timeout=10,
        )
    except Exception as e:
        print(f"🚨 텔레그램 에러: {e}")


# ==========================================
# 📊 매매 내역 DB 기록
# ==========================================
def log_trade(sym, direction, entry_price, exit_price, pnl, reason):
    filename = "trade_history.csv"
    file_exists = os.path.isfile(filename)
    with open(filename, "a", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(
                [
                    "Timestamp",
                    "Symbol",
                    "Direction",
                    "EntryPrice",
                    "ExitPrice",
                    "PnL_USDT",
                    "Reason",
                ]
            )
        now_kst = datetime.now(pytz.timezone("Asia/Seoul")).strftime("%Y-%m-%d %H:%M:%S")
        writer.writerow([now_kst, sym, direction, entry_price, exit_price, round(pnl, 4), reason])


# ==========================================
# 🕒 킬존 & 보조지표 계산기
# ==========================================
def get_session_info():
    now_kst = datetime.now(pytz.timezone("Asia/Seoul"))
    curr_hour = now_kst.hour
    base_date = now_kst - timedelta(days=1) if curr_hour == 0 else now_kst
    asia_start = base_date.replace(hour=9, minute=0, second=0, microsecond=0)
    asia_end = base_date.replace(hour=15, minute=0, second=0, microsecond=0)

    is_kz = False
    s_name = "None"
    pre90_start = None
    pre90_end = None

    if 16 <= curr_hour < 19:
        is_kz = True
        s_name = "London"
        pre90_end = base_date.replace(hour=16, minute=0, second=0, microsecond=0)
        pre90_start = pre90_end - timedelta(minutes=90)
    elif 21 <= curr_hour <= 23 or curr_hour == 0:
        is_kz = True
        s_name = "NewYork"
        pre90_end = base_date.replace(hour=21, minute=0, second=0, microsecond=0)
        pre90_start = pre90_end - timedelta(minutes=90)

    return {
        "is_killzone": is_kz,
        "session_name": s_name,
        "asia_start": asia_start,
        "asia_end": asia_end,
        "pre90_start": pre90_start,
        "pre90_end": pre90_end,
    }


def add_hybrid_features(df, session):
    df["bull_fvg"] = df["low"] > df["high"].shift(2)
    df["bear_fvg"] = df["high"] < df["low"].shift(2)
    df["ema_9"] = df["close"].ewm(span=9, adjust=False).mean()
    df["ema_20"] = df["close"].ewm(span=20, adjust=False).mean()
    df["typical_price"] = (df["high"] + df["low"] + df["close"]) / 3
    df["vwap"] = (df["typical_price"] * df["volume"]).cumsum() / df["volume"].cumsum()

    df["timestamp_dt"] = pd.to_datetime(df["timestamp"])
    asia_mask = (
        df["timestamp_dt"] >= session["asia_start"].strftime("%Y-%m-%d %H:%M:%S")
    ) & (
        df["timestamp_dt"] <= session["asia_end"].strftime("%Y-%m-%d %H:%M:%S")
    )
    df["asia_high"] = df.loc[asia_mask, "high"].max() if not df[asia_mask].empty else None
    df["asia_low"] = df.loc[asia_mask, "low"].min() if not df[asia_mask].empty else None

    if session["pre90_start"]:
        pre_mask = (
            df["timestamp_dt"] >= session["pre90_start"].strftime("%Y-%m-%d %H:%M:%S")
        ) & (df["timestamp_dt"] < session["pre90_end"].strftime("%Y-%m-%d %H:%M:%S"))
        df["pre90_high"] = df.loc[pre_mask, "high"].max() if not df[pre_mask].empty else None
        df["pre90_low"] = df.loc[pre_mask, "low"].min() if not df[pre_mask].empty else None
    else:
        df["pre90_high"] = None
        df["pre90_low"] = None

    df.drop(columns=["timestamp_dt", "typical_price"], inplace=True)
    df.fillna(0.0, inplace=True)
    return df.tail(15)


# ==========================================
# 3. 메인 자동매매 봇 클래스
# ==========================================
class HybridTradingBot:
    def __init__(self, symbols, leverage=7):
        self.symbols = symbols
        self.leverage = leverage
        self.total_capital = self._get_real_balance()
        self.db = {sym: self._init_state() for sym in symbols}

        send_telegram_msg(
            f"🛡️ *[스나이퍼 V2.5 가동]*\n100달러(Notional) 강제 고정 및 API 속도 패치 완료!\n현재 실잔고: {self.total_capital:.2f} USDT"
        )

    def _get_real_balance(self):
        try:
            balance = exchange.fetch_balance()
            return float(balance["USDT"]["free"])
        except Exception:
            return 272.49

    def _init_state(self):
        return {
            "state": "IDLE",
            "direction": None,
            "target_entry": 0.0,
            "target_sl": 0.0,
            "current_sl": 0.0,
            "r_distance": 0.0,
            "total_qty": 0.0,
            "remaining_qty": 0.0,
            "tp1_price": 0.0,
            "tp2_price": 0.0,
            "tp1_hit": False,
            "tp2_hit": False,
            "extreme_price": 0.0,
            "entry_price": 0.0,
            "last_alert_time": 0.0,
            "signal_time": 0.0,
        }

    def _close_position(self, sym, qty, reason):
        st = self.db[sym]
        side = "sell" if st["direction"] == "Long" else "buy"
        try:
            formatted_qty = exchange.amount_to_precision(sym, qty)
            exchange.create_order(
                symbol=sym,
                type="market",
                side=side,
                amount=float(formatted_qty),
                params={"reduceOnly": True},
            )

            ticker = exchange.fetch_ticker(sym)
            exit_price = ticker["last"]

            if st["direction"] == "Long":
                pnl = (exit_price - st["entry_price"]) * float(formatted_qty)
            else:
                pnl = (st["entry_price"] - exit_price) * float(formatted_qty)

            log_trade(sym, st["direction"], st["entry_price"], exit_price, pnl, reason)
            return True
        except Exception as e:
            # 에러 알림은 동일 사유당 한 번만 보내 스팸을 막습니다.
            error_key = f"error_reported_{reason}"
            if not st.get(error_key):
                send_telegram_msg(
                    f"🚨 *[{sym}] {reason} 청산 실패!*\n에러: {e}\n(이 알림은 스팸 방지를 위해 1회만 발송됩니다.)"
                )
                st[error_key] = True
            return False

    def run(self):
        print("\n🛡️ SMC V2.5 스나이퍼 시작!")
        while True:
            session = get_session_info()
            if session["is_killzone"]:
                self.total_capital = self._get_real_balance()

            if not session["is_killzone"]:
                now_str = datetime.now(pytz.timezone("Asia/Seoul")).strftime("%H:%M")
                print(f"☕ [{now_str}] 킬존 대기 중 (보유 포지션만 관리합니다)")
            else:
                print(f"\n🔥 [{session['session_name']} 킬존 오픈!] (잔고: {self.total_capital:.2f} USDT)")

            for sym in self.symbols:
                try:
                    st = self.db[sym]
                    if st["state"] == "IDLE":
                        if not session["is_killzone"]:
                            continue
                        self.process_idle(sym, session)
                    elif st["state"] == "WAIT_RETEST":
                        self.process_wait_retest(sym)
                    elif st["state"] == "MANAGE":
                        self.process_manage(sym)
                except Exception as e:
                    print(f"🚨 [{sym}] 관리 에러: {e}")

                time.sleep(1)

            time.sleep(60)

    def process_idle(self, sym, session):
        market_data = {}
        for tf in ["1h", "15m", "5m"]:
            ohlcv = exchange.fetch_ohlcv(sym, tf, limit=50)
            df = pd.DataFrame(ohlcv, columns=["timestamp", "open", "high", "low", "close", "volume"])
            df["timestamp"] = (
                pd.to_datetime(df["timestamp"], unit="ms", utc=True)
                .dt.tz_convert("Asia/Seoul")
                .dt.strftime("%Y-%m-%d %H:%M:%S")
            )
            df = add_hybrid_features(df, session)
            market_data[tf] = json.loads(df.to_json(orient="records"))

            # 거래소 호출 속도 제한을 피하기 위한 짧은 대기입니다.
            time.sleep(0.3)

        try:
            funding_rate = exchange.fetch_funding_rate(sym).get("fundingRate", 0.0)
        except Exception:
            funding_rate = 0.0

        final_json = json.dumps({sym: market_data, "current_funding_rate": funding_rate})

        prompt = f"""You are an elite SMC Sniper. Current session: {session['session_name']} Killzone.
        CRITICAL RULES:
        1. Dual Sweep: Check 'asia_high/low' and 'pre90_high/low'.
        2. Trend: Only Long if Price > VWAP & EMAs. Only Short if Price < VWAP & EMAs.
        3. Contrarian Funding Rate Filter: Current Funding Rate is {funding_rate:.6f}.
        If valid setup exists, respond strictly in JSON: {{"signal": "Long", "entry_price": 90100.5, "stop_loss": 89800.0, "reasoning": "text"}}.
        If no setup, return {{"signal": "None"}}."""

        try:
            response = client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": prompt},
                    {"role": "user", "content": final_json},
                ],
                response_format={"type": "json_object"},
            )

            raw_text = response.choices[0].message.content.strip()
            ai_data = json.loads(raw_text)

            if ai_data.get("signal") in ["Long", "Short"]:
                st = self.db[sym]
                st["direction"] = ai_data["signal"]
                st["target_entry"] = float(ai_data["entry_price"])
                st["target_sl"] = float(ai_data["stop_loss"])
                st["state"] = "WAIT_RETEST"
                st["signal_time"] = time.time()

                min_notional = 101.0
                expected_qty = min_notional / st["target_entry"]
                try:
                    expected_qty = float(exchange.amount_to_precision(sym, expected_qty))
                except Exception:
                    expected_qty = round(expected_qty, 4)

                expected_value = expected_qty * st["target_entry"]
                expected_margin = expected_value / self.leverage

                msg = (
                    f"🎯 *[{sym}] 킬존 타점 포착!*\n"
                    f"━━━━━━━━━━━━━━━\n"
                    f"📈 방향: {st['direction']}\n"
                    f"💰 진입 목표가: {st['target_entry']}\n"
                    f"🛑 손절가: {st['target_sl']}\n"
                    f"📦 예상 수량: {expected_qty}개\n"
                    f"💵 총 매수규모: ${expected_value:.2f}\n"
                    f"🛡️ 사용 증거금: ${expected_margin:.2f}\n"
                    f"━━━━━━━━━━━━━━━\n"
                    f"리테스트 대기 ⏳"
                )
                send_telegram_msg(msg)

        except Exception as e:
            print(f"🚨 [{sym}] AI 분석 중 통신/파싱 대기... (정상): {e}")

    def process_wait_retest(self, sym):
        st = self.db[sym]
        if time.time() - st["signal_time"] > 1800:
            send_telegram_msg(f"⏳ *[{sym}] 타점 무효화 (30분 경과)*")
            self.db[sym] = self._init_state()
            return

        current_price = exchange.fetch_ticker(sym)["last"]
        if abs(current_price - st["target_entry"]) <= st["target_entry"] * 0.001:
            self.execute_trade(sym, current_price)

    def execute_trade(self, sym, entry_price):
        st = self.db[sym]

        actual_sl_percent = (abs(entry_price - st["target_sl"]) / entry_price) * 100
        if actual_sl_percent < 0.5:
            if st["direction"] == "Long":
                st["target_sl"] = entry_price * 0.995
            else:
                st["target_sl"] = entry_price * 1.005
            actual_sl_percent = 0.5

        max_allowed_cap = SL_CAPS.get(sym, 2.0)
        if actual_sl_percent > max_allowed_cap:
            send_telegram_msg(f"⚠️ *[{sym}] 진입 거절 (손절폭 초과)*")
            self.db[sym] = self._init_state()
            return

        st["r_distance"] = abs(entry_price - st["target_sl"])
        if st["r_distance"] == 0:
            self.db[sym] = self._init_state()
            return

        min_notional = 101.0
        final_raw_qty = min_notional / entry_price

        try:
            st["total_qty"] = float(exchange.amount_to_precision(sym, final_raw_qty))
        except Exception:
            st["total_qty"] = round(final_raw_qty, 4)

        if st["total_qty"] <= 0:
            self.db[sym] = self._init_state()
            return

        total_value = st["total_qty"] * entry_price
        print(f"🧐 [디버그] {sym} 진입 시도: 수량 {st['total_qty']}개 (약 {total_value:.2f} USDT 규모)")

        st["remaining_qty"] = st["total_qty"]
        st["current_sl"] = st["target_sl"]
        st["extreme_price"] = entry_price

        if st["direction"] == "Long":
            st["tp1_price"] = entry_price + (st["r_distance"] * 1)
            st["tp2_price"] = entry_price + (st["r_distance"] * 3)
        else:
            st["tp1_price"] = entry_price - (st["r_distance"] * 1)
            st["tp2_price"] = entry_price - (st["r_distance"] * 3)

        side = "buy" if st["direction"] == "Long" else "sell"

        max_retries = 3
        retry_count = 0
        success = False

        while retry_count < max_retries and not success:
            try:
                exchange.create_order(symbol=sym, type="market", side=side, amount=st["total_qty"])
                st["entry_price"] = entry_price
                st["last_alert_time"] = time.time()
                st["state"] = "MANAGE"

                send_telegram_msg(
                    f"🔥 *[{sym}] {st['direction']} 진입 완료!*\n(진입가: {entry_price}, 수량: {st['total_qty']}개)"
                )
                success = True
            except Exception as e:
                retry_count += 1
                if retry_count < max_retries:
                    st["total_qty"] = float(exchange.amount_to_precision(sym, st["total_qty"] * 0.95))
                    time.sleep(1)
                else:
                    send_telegram_msg(f"🚨 *[{sym}] 최종 진입 실패!*\n재시도 3회 초과: {e}")
                    self.db[sym] = self._init_state()

    def process_manage(self, sym):
        st = self.db[sym]
        current_price = exchange.fetch_ticker(sym)["last"]

        now = time.time()
        if st["last_alert_time"] > 0 and (now - st["last_alert_time"] > 1800):
            if st["direction"] == "Long":
                pnl = (current_price - st["entry_price"]) * st["remaining_qty"]
            else:
                pnl = (st["entry_price"] - current_price) * st["remaining_qty"]
            margin = (st["entry_price"] * st["remaining_qty"]) / self.leverage
            roe = (pnl / margin) * 100 if margin > 0 else 0
            icon = "🔵" if pnl >= 0 else "🔴"
            send_telegram_msg(f"{icon} *[{sym}] 실전 포지션 현황*\n수익금: {pnl:.2f} USDT ({roe:.2f}%)")
            st["last_alert_time"] = now

        if st["direction"] == "Long":
            if current_price <= st["current_sl"]:
                if self._close_position(sym, st["remaining_qty"], "손절/트레일링"):
                    send_telegram_msg(f"🛑 *[{sym}] 포지션 종료*\n손절 터치 (가격: {current_price:.2f})")
                self.db[sym] = self._init_state()
                return
            if current_price >= st["tp1_price"] and not st["tp1_hit"]:
                close_qty = st["total_qty"] * 0.5
                if self._close_position(sym, close_qty, "TP1(1R)"):
                    st["tp1_hit"] = True
                    st["current_sl"] = st["entry_price"]
                    st["remaining_qty"] -= close_qty
                    send_telegram_msg(f"🟢 *[{sym}] TP1 달성 (50% 매도)!*\n본절로 방어벽 이동! 📈")
            if current_price >= st["tp2_price"] and not st["tp2_hit"]:
                close_qty = st["total_qty"] * 0.3
                if self._close_position(sym, close_qty, "TP2(3R)"):
                    st["tp2_hit"] = True
                    st["remaining_qty"] -= close_qty
                    send_telegram_msg(f"🚀 *[{sym}] TP2 달성 (30% 추가매도)!*\n남은 물량 트레일링 시작!")
            if st["tp2_hit"] and current_price > st["extreme_price"]:
                st["extreme_price"] = current_price
                if st["extreme_price"] - (st["r_distance"] * 1.0) > st["current_sl"]:
                    st["current_sl"] = st["extreme_price"] - (st["r_distance"] * 1.0)

        elif st["direction"] == "Short":
            if current_price >= st["current_sl"]:
                if self._close_position(sym, st["remaining_qty"], "손절/트레일링"):
                    send_telegram_msg(f"🛑 *[{sym}] 포지션 종료*\n손절 터치 (가격: {current_price:.2f})")
                self.db[sym] = self._init_state()
                return
            if current_price <= st["tp1_price"] and not st["tp1_hit"]:
                close_qty = st["total_qty"] * 0.5
                if self._close_position(sym, close_qty, "TP1(1R)"):
                    st["tp1_hit"] = True
                    st["current_sl"] = st["entry_price"]
                    st["remaining_qty"] -= close_qty
                    send_telegram_msg(f"🔴 *[{sym}] TP1 달성 (50% 커버링)!*\n본절로 방어벽 이동! 📉")
            if current_price <= st["tp2_price"] and not st["tp2_hit"]:
                close_qty = st["total_qty"] * 0.3
                if self._close_position(sym, close_qty, "TP2(3R)"):
                    st["tp2_hit"] = True
                    st["remaining_qty"] -= close_qty
                    send_telegram_msg(f"🚀 *[{sym}] TP2 달성 (30% 커버링)!*\n남은 물량 트레일링 시작!")
            if st["tp2_hit"] and current_price < st["extreme_price"]:
                st["extreme_price"] = current_price
                if st["extreme_price"] + (st["r_distance"] * 1.0) < st["current_sl"]:
                    st["current_sl"] = st["extreme_price"] + (st["r_distance"] * 1.0)


if __name__ == "__main__":
    symbols_to_trade = list(SL_CAPS.keys())
    bot = HybridTradingBot(symbols=symbols_to_trade, leverage=7)
    bot.run()
