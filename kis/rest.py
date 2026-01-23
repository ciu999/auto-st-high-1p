import json
import time
from typing import Any, Dict, Optional

import aiohttp

from .config import Config


class KISRest:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self._access_token: Optional[str] = None
        self._token_expire_at: float = 0.0
        self._approval_key: Optional[str] = None
        self._session: Optional[aiohttp.ClientSession] = None

    async def __aenter__(self):
        timeout = aiohttp.ClientTimeout(total=10)
        self._session = aiohttp.ClientSession(timeout=timeout)
        return self

    async def __aexit__(self, exc_type, exc, tb):
        if self._session:
            await self._session.close()

    @property
    def session(self) -> aiohttp.ClientSession:
        assert self._session is not None
        return self._session

    async def _request(
        self,
        method: str,
        path: str,
        *,
        headers: Optional[Dict[str, str]] = None,
        params: Optional[Dict[str, str]] = None,
        json_body: Optional[Dict[str, Any]] = None,
        data: Optional[str] = None,
        retry: int = 2,
    ) -> Dict[str, Any]:
        url = self.cfg.rest_base + path
        last_err = None
        for _ in range(retry + 1):
            try:
                async with self.session.request(
                    method,
                    url,
                    headers=headers,
                    params=params,
                    json=json_body,
                    data=data,
                ) as resp:
                    txt = await resp.text()
                    if resp.status >= 400:
                        raise RuntimeError(f"HTTP {resp.status}: {txt}")
                    if not txt:
                        return {}
                    return json.loads(txt)
            except Exception as e:
                last_err = e
                await aiohttp.helpers.asyncio.sleep(0.2)  # type: ignore
        raise RuntimeError(f"REST 요청 실패: {method} {path}: {last_err}")

    async def force_refresh_tokens(self):
        self._access_token = None
        self._token_expire_at = 0.0
        self._approval_key = None
        await self.get_access_token()
        await self.get_approval_key()

    async def get_access_token(self) -> str:
        now = time.time()
        if self._access_token and now < self._token_expire_at - 30:
            return self._access_token

        body = {
            "grant_type": "client_credentials",
            "appkey": self.cfg.app_key,
            "appsecret": self.cfg.app_secret,
        }
        headers = {"content-type": "application/json"}
        res = await self._request("POST", "/oauth2/tokenP", headers=headers, json_body=body)

        token = res.get("access_token")
        expires_in = int(res.get("expires_in", 0)) or 0
        if not token:
            raise RuntimeError(f"토큰 발급 실패: {res}")

        self._access_token = token
        self._token_expire_at = time.time() + max(60, expires_in)
        return token

    async def get_approval_key(self) -> str:
        if self._approval_key:
            return self._approval_key

        body = {
            "grant_type": "client_credentials",
            "appkey": self.cfg.app_key,
            "secretkey": self.cfg.app_secret,
            "appsecret": self.cfg.app_secret,
        }
        headers = {"content-type": "application/json"}
        res = await self._request("POST", "/oauth2/Approval", headers=headers, json_body=body)

        key = res.get("approval_key")
        if not key:
            raise RuntimeError(f"approval_key 발급 실패: {res}")

        self._approval_key = key
        return key

    async def hashkey(self, data_obj: Dict[str, Any]) -> str:
        headers = {
            "content-type": "application/json",
            "appKey": self.cfg.app_key,
            "appSecret": self.cfg.app_secret,
        }
        res = await self._request("POST", "/uapi/hashkey", headers=headers, json_body=data_obj)
        h = res.get("HASH")
        if not h:
            raise RuntimeError(f"hashkey 실패: {res}")
        return h

    async def get_ranking_fluctuation(self, min_rate: int = 20, max_rate: int = 28) -> Dict[str, Any]:
        token = await self.get_access_token()
        headers = {
            "content-type": "application/json; charset=utf-8",
            "authorization": f"Bearer {token}",
            "appKey": self.cfg.app_key,
            "appSecret": self.cfg.app_secret,
            "tr_id": "FHPST01700000",
            "custtype": self.cfg.custtype,
        }
        params = {
            "fid_rsfl_rate1": str(min_rate),
            "fid_rsfl_rate2": str(max_rate),
            "fid_cond_mrkt_div_code": "J",
            "fid_cond_scr_div_code": "20170",
            "fid_input_iscd": "0000",
            "fid_rank_sort_cls_code": "0",
            "fid_input_cnt_1": "0",
            "fid_prc_cls_code": "0",
            "fid_input_price_1": "",
            "fid_input_price_2": "",
            "fid_vol_cnt": "",
            "fid_trgt_cls_code": "0",
            "fid_trgt_exls_cls_code": "0",
        }
        return await self._request(
            "GET",
            "/uapi/domestic-stock/v1/ranking/fluctuation",
            headers=headers,
            params=params,
        )

    async def inquire_psbl_cash(self, pdno: str, ord_unpr: int, ord_dvsn: str = "00") -> int:
        token = await self.get_access_token()
        tr_id = "VTTC8908R" if self.cfg.env == "vts" else "TTTC8908R"
        headers = {
            "content-type": "application/json; charset=utf-8",
            "authorization": f"Bearer {token}",
            "appKey": self.cfg.app_key,
            "appSecret": self.cfg.app_secret,
            "tr_id": tr_id,
            "custtype": self.cfg.custtype,
        }
        params = {
            "CANO": self.cfg.cano,
            "ACNT_PRDT_CD": self.cfg.acnt_prdt_cd,
            "PDNO": pdno,
            "ORD_UNPR": str(ord_unpr),
            "ORD_DVSN": ord_dvsn,
            "CMA_EVLU_AMT_ICLD_YN": "Y",
            "OVRS_ICLD_YN": "Y",
        }
        res = await self._request(
            "GET",
            "/uapi/domestic-stock/v1/trading/inquire-psbl-order",
            headers=headers,
            params=params,
        )
        out = res.get("output") or {}
        for k in ("ord_psbl_cash", "ORD_PSBL_CASH"):
            if k in out:
                return int(float(out[k]))
        raise RuntimeError(f"매수가능금액 파싱 실패: {res}")

    async def order_cash(self, side: str, pdno: str, qty: int, unpr: int, ord_dvsn: str) -> str:
        token = await self.get_access_token()

        if self.cfg.env == "vts":
            candidates = [("VTTC0012U", "VTTC0011U"), ("VTTC0802U", "VTTC0801U")]
        else:
            candidates = [("TTTC0012U", "TTTC0011U"), ("TTTC0802U", "TTTC0801U")]

        body = {
            "CANO": self.cfg.cano,
            "ACNT_PRDT_CD": self.cfg.acnt_prdt_cd,
            "PDNO": pdno,
            "ORD_DVSN": ord_dvsn,
            "ORD_QTY": str(int(qty)),
            "ORD_UNPR": str(int(unpr)),
        }

        hk = await self.hashkey(body)

        last_err = None
        for buy_tr, sell_tr in candidates:
            tr_id = buy_tr if side == "buy" else sell_tr
            headers = {
                "content-type": "application/json; charset=utf-8",
                "authorization": f"Bearer {token}",
                "appKey": self.cfg.app_key,
                "appSecret": self.cfg.app_secret,
                "tr_id": tr_id,
                "custtype": self.cfg.custtype,
                "hashkey": hk,
            }
            try:
                res = await self._request(
                    "POST",
                    "/uapi/domestic-stock/v1/trading/order-cash",
                    headers=headers,
                    json_body=body,
                )
                out = res.get("output") or {}
                odno = out.get("ODNO") or out.get("odno")
                if not odno:
                    raise RuntimeError(f"주문 ODNO 없음: {res}")
                return str(odno)
            except Exception as e:
                last_err = e

        raise RuntimeError(f"주문 실패: {last_err}")
