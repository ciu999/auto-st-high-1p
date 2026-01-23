import asyncio
import base64
import json
import logging
from dataclasses import dataclass
from typing import Dict, Optional, Tuple

import websockets
from Crypto.Cipher import AES
from Crypto.Util.Padding import unpad

from .config import Config
from .rest import KISRest
from .time_utils import kst_now


def aes_cbc_base64_decrypt(cipher_b64: str, key: str, iv: str) -> str:
    cipher_bytes = base64.b64decode(cipher_b64)
    aes = AES.new(key.encode("utf-8"), AES.MODE_CBC, iv.encode("utf-8"))
    plain = aes.decrypt(cipher_bytes)
    plain = unpad(plain, 16)
    return plain.decode("utf-8")


def _is_truthy(v: str) -> bool:
    v = (v or "").strip().upper()
    return v in ("Y", "1", "T", "TRUE")

def _safe_int(v: str, default: int = 0) -> int:
    try:
        return int(float((v or "").strip()))
    except Exception:
        return default


@dataclass
class ExecFill:
    order_no: str
    pdno: str
    qty: int
    price: int
    raw_fields: list


class KISWebSocket:
    def __init__(self, cfg: Config, rest: KISRest, logger: logging.Logger):
        self.cfg = cfg
        self.rest = rest
        self.log = logger

        self.ws: Optional[websockets.WebSocketClientProtocol] = None

        self._aes_key: Dict[str, str] = {}
        self._aes_iv: Dict[str, str] = {}

        self._orderbook: Dict[str, Tuple[int, int, int, int]] = {}
        self._orderbook_seq: Dict[str, int] = {}
        self._orderbook_event: Dict[str, asyncio.Event] = {}

        self._trade_price: Dict[str, int] = {}
        self._trade_seq: Dict[str, int] = {}
        self._trade_event: Dict[str, asyncio.Event] = {}

        self._fills_by_order: Dict[str, asyncio.Queue] = {}
        self._recv_task: Optional[asyncio.Task] = None

    async def connect(self):
        approval_key = await self.rest.get_approval_key()
        self.ws = await websockets.connect(self.cfg.ws_url, ping_interval=20, ping_timeout=20)
        self.log.info("WS connected: %s", self.cfg.ws_url)

        await self.subscribe(self.cfg.exec_tr_id, self.cfg.hts_id, approval_key=approval_key)
        self._recv_task = asyncio.create_task(self._recv_loop())

    async def close(self):
        if self._recv_task:
            self._recv_task.cancel()
        if self.ws:
            await self.ws.close()

    async def subscribe(self, tr_id: str, tr_key: str, *, approval_key: str):
        assert self.ws is not None
        msg = {
            "header": {
                "approval_key": approval_key,
                "custtype": self.cfg.custtype,
                "tr_type": "1",
                "content-type": "utf-8",
            },
            "body": {"tr_id": tr_id, "tr_key": tr_key},
        }
        await self.ws.send(json.dumps(msg))
        self.log.info("WS subscribe: %s %s", tr_id, tr_key)

    async def unsubscribe(self, tr_id: str, tr_key: str, *, approval_key: str):
        assert self.ws is not None
        msg = {
            "header": {
                "approval_key": approval_key,
                "custtype": self.cfg.custtype,
                "tr_type": "2",
                "content-type": "utf-8",
            },
            "body": {"tr_id": tr_id, "tr_key": tr_key},
        }
        await self.ws.send(json.dumps(msg))
        self.log.info("WS unsubscribe: %s %s", tr_id, tr_key)

    def _ensure_event(self, d: Dict[str, asyncio.Event], key: str) -> asyncio.Event:
        if key not in d:
            d[key] = asyncio.Event()
        return d[key]

    def _bump_seq(self, d: Dict[str, int], key: str) -> int:
        d[key] = d.get(key, 0) + 1
        return d[key]

    async def wait_next_orderbook(self, code: str, last_seq: int, timeout: float = 5.0):
        evt = self._ensure_event(self._orderbook_event, code)
        end = asyncio.get_event_loop().time() + timeout
        while asyncio.get_event_loop().time() < end:
            cur = self._orderbook_seq.get(code, 0)
            if cur > last_seq and code in self._orderbook:
                ask1, askq1, bid1, bidq1 = self._orderbook[code]
                return cur, ask1, askq1, bid1, bidq1
            evt.clear()
            try:
                await asyncio.wait_for(evt.wait(), timeout=end - asyncio.get_event_loop().time())
            except asyncio.TimeoutError:
                break
        raise asyncio.TimeoutError("orderbook timeout")

    async def wait_trade_reach(self, code: str, target_price: int, *, session_end, poll_max_wait: float = 3.0) -> int:
        evt = self._ensure_event(self._trade_event, code)
        while kst_now() < session_end:
            price = self._trade_price.get(code)
            if price is not None and price >= target_price:
                return price
            evt.clear()
            try:
                remain = (session_end - kst_now()).total_seconds()
                if remain <= 0:
                    break
                await asyncio.wait_for(evt.wait(), timeout=min(poll_max_wait, remain))
            except asyncio.TimeoutError:
                continue
        raise asyncio.TimeoutError("trade reach timeout (session ended)")

    def _get_order_queue(self, order_no: str) -> asyncio.Queue:
        if order_no not in self._fills_by_order:
            self._fills_by_order[order_no] = asyncio.Queue()
        return self._fills_by_order[order_no]

    async def wait_order_fills(self, order_no: str, *, total_timeout: float = 3.0, idle_timeout: float = 0.5):
        q = self._get_order_queue(order_no)
        filled_qty = 0
        filled_value = 0

        try:
            first: ExecFill = await asyncio.wait_for(q.get(), timeout=total_timeout)
        except asyncio.TimeoutError:
            return 0, 0

        filled_qty += first.qty
        filled_value += first.qty * first.price

        while True:
            try:
                nxt: ExecFill = await asyncio.wait_for(q.get(), timeout=idle_timeout)
                filled_qty += nxt.qty
                filled_value += nxt.qty * nxt.price
            except asyncio.TimeoutError:
                break

        return filled_qty, filled_value

    async def _recv_loop(self):
        assert self.ws is not None
        while True:
            msg = await self.ws.recv()

            # subscribe ack + encrypt key/iv
            if isinstance(msg, str) and msg.startswith("{"):
                try:
                    j = json.loads(msg)
                    h = j.get("header") or {}
                    tr_id = h.get("tr_id")
                    encrypt = h.get("encrypt")
                    if encrypt == "Y":
                        out = (j.get("body") or {}).get("output") or {}
                        iv = out.get("iv")
                        key = out.get("key")
                        if tr_id and iv and key:
                            self._aes_iv[tr_id] = iv
                            self._aes_key[tr_id] = key
                            self.log.info("WS encrypt set: %s", tr_id)
                except Exception:
                    pass
                continue

            if not isinstance(msg, str) or "|" not in msg:
                continue

            parts = msg.split("|", 3)
            if len(parts) < 4:
                continue
            data_type, tr_id, third, data = parts[0], parts[1], parts[2], parts[3]

            # 호가: 0|H0STASP0|005930|...
            if tr_id == "H0STASP0":
                vals = data.split("^")
                try:
                    code = vals[0]
                    ask1 = int(vals[3])
                    bid1 = int(vals[13])
                    askq1 = int(vals[23])
                    bidq1 = int(vals[33])
                    self._orderbook[code] = (ask1, askq1, bid1, bidq1)
                    self._bump_seq(self._orderbook_seq, code)
                    self._ensure_event(self._orderbook_event, code).set()
                except Exception:
                    continue
                continue

            # 체결가: 0|H0STCNT0|...|code^time^price^...
            if tr_id == "H0STCNT0":
                vals = data.split("^")
                try:
                    code = vals[0]
                    price = int(vals[2])
                    self._trade_price[code] = price
                    self._bump_seq(self._trade_seq, code)
                    self._ensure_event(self._trade_event, code).set()
                except Exception:
                    continue
                continue

            # 체결통보: 1|H0STCNI0/H0STCNI9|...|<b64>
            if tr_id == self.cfg.exec_tr_id and data_type == "1":
                key = self._aes_key.get(tr_id)
                iv = self._aes_iv.get(tr_id)
                if not key or not iv:
                    continue

                try:
                    plain = aes_cbc_base64_decrypt(data, key, iv)
                    fields = plain.split("^")

                    # 기본 필드(네가 준 순서 기준)
                    # 2: 주문번호, 8: 종목코드, 9: 체결수량, 10: 체결단가
                    # 12: 거부여부, 13: 체결여부, 14: 접수여부
                    order_no = fields[2] if len(fields) > 2 else ""
                    pdno = fields[8] if len(fields) > 8 else ""

                    qty = _safe_int(fields[9] if len(fields) > 9 else "0", 0)
                    price = _safe_int(fields[10] if len(fields) > 10 else "0", 0)

                    reject_flag = fields[12] if len(fields) > 12 else ""
                    exec_flag = fields[13] if len(fields) > 13 else ""
                    accept_flag = fields[14] if len(fields) > 14 else ""

                    is_reject = _is_truthy(reject_flag)

                    # ✅ 실체결로 인정하는 기준:
                    # - 거부가 아니고
                    # - 체결수량/체결단가가 양수
                    is_fill = (not is_reject) and (qty > 0) and (price > 0)

                    if is_fill:
                        fill = ExecFill(order_no=order_no, pdno=pdno, qty=qty, price=price, raw_fields=fields)
                        q = self._get_order_queue(order_no)
                        q.put_nowait(fill)
                        self.log.info(
                            "EXEC_FILL order=%s code=%s qty=%s price=%s (rej=%s exec=%s acc=%s)",
                            order_no, pdno, qty, price, reject_flag, exec_flag, accept_flag
                        )
                    else:
                        # ✅ 실체결이 아닌 이벤트(거부/접수/주문상태 등): 로그만 남김
                        self.log.info(
                            "EXEC_NONFILL order=%s code=%s qty=%s price=%s (rej=%s exec=%s acc=%s)",
                            order_no, pdno, qty, price, reject_flag, exec_flag, accept_flag
                        )

                except Exception as e:
                    self.log.warning("EXEC_NOTIFY parse/decrypt failed: %r", e)
                continue
