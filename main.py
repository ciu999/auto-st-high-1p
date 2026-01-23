import asyncio
import logging
from datetime import datetime, timedelta

from kis.config import load_config
from kis.rest import KISRest
from kis.session import run_trading_session
from kis.time_utils import kst_now, next_kst_datetime, sleep_until


async def daily_scheduler():
    """
    요구사항:
    - 매일 08:30 토큰/approval_key 강제 재발급
    - 매일 09:20 시작, 15:00 중지
    - 다음날 반복
    """
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    logger = logging.getLogger("kis_1pct_bot")

    cfg = load_config()
    logger.info("ENV=%s REST=%s WS=%s", cfg.env, cfg.rest_base, cfg.ws_url)

    cooldowns: dict[str, datetime] = {}

    async with KISRest(cfg) as rest:
        while True:
            now = kst_now()

            # 08:30 refresh (이미 08:30~09:20 사이면 즉시 refresh)
            today_0830 = now.replace(hour=8, minute=30, second=0, microsecond=0)
            if today_0830 <= now < today_0830 + timedelta(hours=1):
                refresh_at = now
            else:
                refresh_at = next_kst_datetime(8, 30, base=now)

            if refresh_at > now:
                logger.info("Next token refresh at %s", refresh_at.isoformat())
                await sleep_until(refresh_at)

            logger.info("Refreshing access_token + approval_key (forced)")
            await rest.force_refresh_tokens()

            # 09:20 start
            now = kst_now()
            session_start = now.replace(hour=9, minute=20, second=0, microsecond=0)
            if session_start < now:
                session_start = now  # 이미 지났으면 즉시 시작(단 15:00 전)

            # 15:00 end
            session_end = now.replace(hour=15, minute=0, second=0, microsecond=0)
            if session_end <= session_start:
                logger.info("Market session already ended today. Skip to next day.")
                await asyncio.sleep(1.0)
                continue

            if session_start > now:
                logger.info("Session starts at %s", session_start.isoformat())
                await sleep_until(session_start)

            logger.info("Session running: %s ~ %s", session_start.isoformat(), session_end.isoformat())
            await run_trading_session(
                cfg, rest, cooldowns, logger,
                session_start=session_start, session_end=session_end
            )

            logger.info("Session ended. Waiting for next schedule cycle.")


def main():
    asyncio.run(daily_scheduler())


if __name__ == "__main__":
    main()
