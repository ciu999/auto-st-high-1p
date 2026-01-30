import os
from dataclasses import dataclass
from dotenv import load_dotenv
from datetime import timedelta

load_dotenv()

@dataclass(frozen=True)
class Config:
    env: str  # "prod" | "vts"
    app_key: str
    app_secret: str
    cano: str
    acnt_prdt_cd: str
    hts_id: str
    custtype: str = "P"

    @property
    def rest_base(self) -> str:
        if self.env == "vts":
            return "https://openapivts.koreainvestment.com:29443"
        return "https://openapi.koreainvestment.com:9443"

    @property
    def ws_url(self) -> str:
        if self.env == "vts":
            return "ws://ops.koreainvestment.com:31000"
        return "ws://ops.koreainvestment.com:21000"

    @property
    def exec_tr_id(self) -> str:
        # 체결통보: 실전 H0STCNI0 / 모의 H0STCNI9
        return "H0STCNI9" if self.env == "vts" else "H0STCNI0"


def load_config() -> Config:
    env = os.getenv("KIS_ENV", "prod").strip().lower()
    cfg = Config(
        env=env,
        app_key=os.getenv("KIS_APP_KEY", "").strip(),
        app_secret=os.getenv("KIS_APP_SECRET", "").strip(),
        cano=os.getenv("KIS_CANO", "").strip(),
        acnt_prdt_cd=os.getenv("KIS_ACNT_PRDT_CD", "01").strip(),
        hts_id=os.getenv("KIS_HTS_ID", "").strip(),
        custtype=os.getenv("KIS_CUSTTYPE", "P").strip() or "P",
    )

    missing = [k for k, v in {
        "KIS_APP_KEY": cfg.app_key,
        "KIS_APP_SECRET": cfg.app_secret,
        "KIS_CANO": cfg.cano,
        "KIS_ACNT_PRDT_CD": cfg.acnt_prdt_cd,
        "KIS_HTS_ID": cfg.hts_id,
    }.items() if not v]

    if missing:
        raise RuntimeError(f"환경변수 누락: {missing}")
    if cfg.env not in ("prod", "vts"):
        raise RuntimeError("KIS_ENV는 'prod' 또는 'vts'만 허용")

    return cfg
