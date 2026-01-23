from datetime import datetime
from typing import Dict, Optional, Tuple

from .config import Config
from .rest import KISRest
from .ws import KISWebSocket
from .strategy import find_candidate, ioc_buy_with_ws, wait_for_1pct_then_sell, ioc_sell_with_ws
from .time_utils import is_weekend, kst_now


async def run_trading_session(
    cfg: Config,
    rest: KISRest,
    cooldowns: Dict[str, datetime],
    logger,
    *,
    session_start: datetime,
    session_end: datetime,
):
    """
    09:20~15:00 세션 동안 최대 5회 반복.
    세션 종료(15:00) 시 포지션 남아있으면 즉시 IOC로 정리 시도.
    """
    if is_weekend(session_start):
        logger.info("Weekend. Skip session: %s", session_start.date())
        return

    ws = KISWebSocket(cfg, rest, logger)
    await ws.connect()

    open_position: Optional[Tuple[str, int, float]] = None  # (code, qty, avg_buy)

    try:
        done = 0
        while done < 5 and kst_now() < session_end:
            code = await find_candidate(rest, cooldowns, logger, session_end=session_end)
            if not code:
                logger.info("No candidate until session end.")
                break

            buy_qty, avg_buy = await ioc_buy_with_ws(rest, ws, code, logger, session_end=session_end)
            cooldowns[code] = kst_now()

            if buy_qty <= 0:
                logger.warning("BUY failed. back to scanning...")
                continue

            open_position = (code, buy_qty, avg_buy)

            sold_qty, sold_value, target_hit = await wait_for_1pct_then_sell(
                rest, ws, code, buy_qty, avg_buy, logger, session_end=session_end
            )
            cooldowns[code] = kst_now()

            if sold_qty <= 0:
                logger.warning("SELL failed or not filled. (code=%s qty=%s)", code, buy_qty)
                open_position = (code, buy_qty, avg_buy)
            else:
                avg_sell = sold_value / sold_qty
                pnl_pct = (avg_sell / avg_buy - 1.0) * 100.0
                logger.info(
                    "TRADE DONE code=%s qty=%s avg_buy=%.2f avg_sell=%.2f pnl=%.3f%% target_hit=%s",
                    code, sold_qty, avg_buy, avg_sell, pnl_pct, target_hit
                )
                open_position = None
                done += 1
                logger.info("Progress: %s/5", done)

        if open_position and kst_now() >= session_end:
            code, qty, avg_buy = open_position
            logger.warning("Session end. Force liquidation attempt: %s qty=%s", code, qty)
            sold_qty, sold_value = await ioc_sell_with_ws(rest, ws, code, qty, logger, session_end=session_end)
            if sold_qty > 0:
                avg_sell = sold_value / sold_qty
                pnl_pct = (avg_sell / avg_buy - 1.0) * 100.0
                logger.info("LIQUIDATED code=%s qty=%s avg_sell=%.2f pnl=%.3f%%", code, sold_qty, avg_sell, pnl_pct)
            else:
                logger.error("LIQUIDATION FAILED code=%s qty=%s (manual check needed)", code, qty)

    finally:
        await ws.close()
