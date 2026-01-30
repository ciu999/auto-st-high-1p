import asyncio
import math
import traceback
from datetime import datetime, timedelta
from typing import Dict, Optional, Tuple

from .rest import KISRest
from .ws import KISWebSocket
from .time_utils import kst_now


def pick_code_from_ranking_item(item: dict, logger) -> Optional[str]:
    code = (item.get("stck_shrn_iscd") or item.get("mksc_shrn_iscd") or item.get("code") or "").strip()
    rate = item.get("prdy_ctrt") or item.get("prdy_ctrt_1") or item.get("fluc_rt") or item.get("rate")
    try:
        rate_f = float(str(rate))
    except Exception:
        logger.warning("conversion float error when pick code from ranking item")
    if 20.0 <= rate_f <= 28.0 and code:
        return code
    return None


async def find_candidate(
    rest: KISRest,
    cooldowns: Dict[str, datetime],
    logger,
    *,
    session_end: datetime,
) -> Optional[str]:
    poll_interval = 1.0 / 3.0  # 초당 3회
    try_cnt = 0
    while kst_now() < session_end:
        res = await rest.get_ranking_fluctuation(20, 28)
        if try_cnt < 10:
            print(res)
        try_cnt += 1
        items = []
        for k in ("output", "output1", "Output", "Output1"):
            v = res.get(k)
            if isinstance(v, list):
                items = v
                break

        now = kst_now()
        for it in items:
            if not isinstance(it, dict):
                continue
            code = pick_code_from_ranking_item(it, logger)
            if not code:
                continue

            last = cooldowns.get(code)
            if last and (now - last) < timedelta(minutes=20):
                continue

            logger.info("Candidate picked: %s", code)
            return code

        await asyncio.sleep(poll_interval)

    return None


async def ioc_buy_with_ws(
    rest: KISRest,
    ws: KISWebSocket,
    code: str,
    logger,
    *,
    session_end: datetime,
    max_attempts: int = 20,
    cash_use_ratio: float = 0.98,
) -> Tuple[int, float]:
    """
    요구사항 반영:
    - (WS) 호가 1회 수신 -> IOC 1회 주문 -> 다음 호가(WS) -> 다음 주문 반복
    """
    approval_key = await rest.get_approval_key()
    await ws.subscribe("H0STASP0", code, approval_key=approval_key)

    total_qty = 0
    total_value = 0
    last_seq = ws._orderbook_seq.get(code, 0)

    try:
        for attempt in range(1, max_attempts + 1):
            if kst_now() >= session_end:
                break

            seq, ask1, askq1, bid1, bidq1 = await ws.wait_next_orderbook(code, last_seq, timeout=5.0)
            last_seq = seq
            if ask1 <= 0:
                continue

            psbl_cash = await rest.inquire_psbl_cash(code, ord_unpr=ask1, ord_dvsn="00")
            psbl_cash = int(psbl_cash * cash_use_ratio)
            affordable = psbl_cash // ask1
            if affordable <= 0:
                logger.info("No more cash to buy. psbl_cash=%s ask1=%s", psbl_cash, ask1)
                break

            ord_dvsn = "11"  # IOC 지정가
            ord_qty = int(affordable)

            logger.info("[BUY][%s/%s] ask1=%s cash=%s qty=%s", attempt, max_attempts, ask1, psbl_cash, ord_qty)
            odno = await rest.order_cash("buy", code, ord_qty, ask1, ord_dvsn=ord_dvsn)

            filled_qty, filled_value = await ws.wait_order_fills(odno, total_timeout=3.0, idle_timeout=0.5)
            if filled_qty <= 0:
                logger.warning("BUY no fill (order=%s). next orderbook...", odno)
                continue

            total_qty += filled_qty
            total_value += filled_value
    except Exception:
        print(traceback.format_exc())

    finally:
        await ws.unsubscribe("H0STASP0", code, approval_key=approval_key)

    if total_qty <= 0:
        return 0, 0.0
    return total_qty, total_value / total_qty


async def ioc_sell_with_ws(
    rest: KISRest,
    ws: KISWebSocket,
    code: str,
    qty_to_sell: int,
    logger,
    *,
    session_end: datetime,
    max_attempts: int = 30,
) -> Tuple[int, int]:
    approval_key = await rest.get_approval_key()
    await ws.subscribe("H0STASP0", code, approval_key=approval_key)

    remain = int(qty_to_sell)
    sold_qty = 0
    sold_value = 0
    last_seq = ws._orderbook_seq.get(code, 0)

    try:
        for attempt in range(1, max_attempts + 1):
            if remain <= 0:
                break

            seq, ask1, askq1, bid1, bidq1 = await ws.wait_next_orderbook(code, last_seq, timeout=5.0)
            last_seq = seq
            if bid1 <= 0:
                continue

            ord_dvsn = "11"  # IOC 지정가
            ord_qty = remain

            logger.info("[SELL][%s/%s] bid1=%s remain=%s", attempt, max_attempts, bid1, remain)
            odno = await rest.order_cash("sell", code, ord_qty, bid1, ord_dvsn=ord_dvsn)

            filled_qty, filled_value = await ws.wait_order_fills(odno, total_timeout=3.0, idle_timeout=0.5)
            if filled_qty <= 0:
                logger.warning("SELL no fill (order=%s). next orderbook...", odno)
                continue

            sold_qty += filled_qty
            sold_value += filled_value
            remain -= filled_qty
    except Exception:
        print(traceback.format_exc())

    finally:
        await ws.unsubscribe("H0STASP0", code, approval_key=approval_key)

    return sold_qty, sold_value


async def wait_for_1pct_then_sell(
    rest: KISRest,
    ws: KISWebSocket,
    code: str,
    buy_qty: int,
    avg_buy: float,
    logger,
    *,
    session_end: datetime,
) -> Tuple[int, int, bool]:
    """
    - 1% 목표가까지 기다리되, 15:00(session_end)까지 못 찍으면 종료 시점에 강제 매도 시도
    """
    approval_key = await rest.get_approval_key()
    await ws.subscribe("H0STCNT0", code, approval_key=approval_key)

    target = math.ceil(avg_buy * 1.01)
    logger.info("Target(+1%%)=%s (avg_buy=%.2f)", target, avg_buy)

    target_hit = False
    try:
        try:
            last_price = await ws.wait_trade_reach(code, target, session_end=session_end)
            logger.info("Reached target: last=%s target=%s", last_price, target)
            target_hit = True
        except asyncio.TimeoutError:
            logger.warning("Session end reached before target. Force sell attempt.")
    except Exception:
        print(traceback.format_exc())
    finally:
        await ws.unsubscribe("H0STCNT0", code, approval_key=approval_key)

    sold_qty, sold_value = await ioc_sell_with_ws(rest, ws, code, buy_qty, logger, session_end=session_end)
    return sold_qty, sold_value, target_hit
