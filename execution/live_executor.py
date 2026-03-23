"""
execution/live_executor.py

Live executor using the official Polymarket CLOB API (py_clob_client).
Handles auth, order construction, signing, submission, and fill monitoring.
"""
from __future__ import annotations

import asyncio
import time
from datetime import datetime, timezone
from typing import Any, Optional

import structlog
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from core.exceptions import (
    ExecutionError,
    InsufficientBalanceError,
    SlippageExceededError,
)
from execution.base import BaseExecutor
from execution.models import OrderResult, TradeRecord
from strategy.models import OrderRequest, Portfolio, Position

logger = structlog.get_logger(__name__)


def _now() -> datetime:
    return datetime.now(timezone.utc)


class LiveExecutor(BaseExecutor):
    """
    Places real orders on Polymarket via the py_clob_client SDK.

    Order flow:
      1. Validate available balance
      2. Fetch current best price from CLOB
      3. Check slippage vs requested price
      4. Construct signed order (EIP-712)
      5. Submit to CLOB API
      6. Poll for fill confirmation (up to order_timeout_s)
      7. Return OrderResult

    Retries network errors up to 3 times with exponential backoff.
    """

    mode = "live"

    # Order type constants (from py_clob_client)
    LIMIT_ORDER = "LIMIT"
    MARKET_ORDER = "MARKET"
    GTC = "GTC"
    IOC = "IOC"
    FOK = "FOK"
    GTD = "GTD"

    def __init__(
        self,
        private_key: str,
        api_key: str,
        api_secret: str,
        api_passphrase: str,
        chain_id: int = 137,
        clob_api_url: str = "https://clob.polymarket.com",
        order_timeout_s: int = 30,
        max_retries: int = 3,
        fee_rate: float = 0.02,
        max_slippage_pct: float = 0.02,
    ) -> None:
        self._private_key = private_key
        self._api_key = api_key
        self._api_secret = api_secret
        self._api_passphrase = api_passphrase
        self._chain_id = chain_id
        self._clob_url = clob_api_url
        self._timeout_s = order_timeout_s
        self._max_retries = max_retries
        self._fee_rate = fee_rate
        self._max_slippage = max_slippage_pct
        self._log = logger.bind(executor="live")

        self._client: Any = None   # py_clob_client.ClobClient
        self._positions: dict[str, Position] = {}   # position_id → Position
        self._open_orders: dict[str, dict] = {}     # order_id → raw order

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def initialize(self) -> None:
        """Build and authenticate the CLOB client."""
        try:
            from py_clob_client.client import ClobClient
            from py_clob_client.clob_types import ApiCreds

            creds = ApiCreds(
                api_key=self._api_key,
                api_secret=self._api_secret,
                api_passphrase=self._api_passphrase,
            )
            # ClobClient is synchronous — run in executor for async compatibility
            loop = asyncio.get_event_loop()
            self._client = await loop.run_in_executor(
                None,
                lambda: ClobClient(
                    host=self._clob_url,
                    key=self._private_key,
                    chain_id=self._chain_id,
                    creds=creds,
                ),
            )
            self._log.info("clob_client_initialized", chain_id=self._chain_id)
        except ImportError:
            raise ImportError(
                "py_clob_client not installed. Run: pip install py-clob-client"
            )
        except Exception as exc:
            raise ExecutionError(f"CLOB client init failed: {exc}") from exc

    # ── Order placement ───────────────────────────────────────────────────────

    async def place_order(self, order: OrderRequest) -> OrderResult:
        if self._client is None:
            await self.initialize()

        self._log.info(
            "placing_live_order",
            order_id=order.order_id,
            market=order.market_question[:60],
            side=order.side,
            outcome=order.outcome,
            size_usd=order.size_usd,
            price=order.price,
        )

        try:
            # 1. Validate balance
            await self._validate_balance(order)

            # 2. Get best current price from CLOB
            best_price = await self._get_best_price(order)

            # 3. Slippage check
            self._check_slippage(order, best_price)

            # 4. Build and sign order
            raw_order = await self._build_order(order, best_price)

            # 5. Submit
            response = await self._submit_with_retry(raw_order)

            # 6. Poll for fill
            result = await self._await_fill(order, response)

            self._log.info(
                "order_filled",
                order_id=order.order_id,
                status=result.status,
                fill_price=result.fill_price,
                slippage=result.slippage,
            )
            return result

        except InsufficientBalanceError as exc:
            self._log.warning("insufficient_balance", error=str(exc))
            return OrderResult.error(order.order_id, str(exc))
        except SlippageExceededError as exc:
            self._log.warning("slippage_exceeded", error=str(exc))
            return OrderResult.error(order.order_id, str(exc))
        except ExecutionError as exc:
            self._log.error("execution_error", error=str(exc))
            return OrderResult.error(order.order_id, str(exc))
        except Exception as exc:
            self._log.error("unexpected_order_error", error=str(exc), exc_info=True)
            return OrderResult.error(order.order_id, f"Unexpected error: {exc}")

    async def _validate_balance(self, order: OrderRequest) -> None:
        balance = await self.get_balance()
        required = order.size_usd * (1 + self._fee_rate)
        if balance < required:
            raise InsufficientBalanceError(
                f"Need ${required:.2f} (incl. fees), have ${balance:.2f}"
            )

    async def _get_best_price(self, order: OrderRequest) -> float:
        """Fetch current best bid/ask from CLOB orderbook."""
        loop = asyncio.get_event_loop()
        try:
            book = await loop.run_in_executor(
                None,
                lambda: self._client.get_order_book(order.market_id),
            )
            if order.side == "buy":
                asks = book.asks or []
                if asks:
                    return float(sorted(asks, key=lambda x: float(x.price))[0].price)
            else:
                bids = book.bids or []
                if bids:
                    return float(sorted(bids, key=lambda x: float(x.price), reverse=True)[0].price)
        except Exception as exc:
            self._log.warning("get_best_price_failed", error=str(exc))
        return order.price   # fall back to requested price

    def _check_slippage(self, order: OrderRequest, best_price: float) -> None:
        if best_price <= 0:
            return
        slippage = abs(best_price - order.price) / order.price
        effective_max = order.max_slippage_pct or self._max_slippage
        if slippage > effective_max:
            raise SlippageExceededError(
                f"Slippage {slippage:.2%} > max {effective_max:.2%}. "
                f"Requested {order.price:.4f}, best {best_price:.4f}"
            )

    async def _build_order(self, order: OrderRequest, fill_price: float) -> dict:
        """Construct order payload for py_clob_client."""
        from py_clob_client.clob_types import OrderArgs, OrderType

        # Map our order types to CLOB types
        order_type_map = {
            "limit": OrderType.GTC,
            "ioc": OrderType.IOC,
            "market": OrderType.FOK,
            "gtd": OrderType.GTD,
        }
        clob_order_type = order_type_map.get(order.order_type.lower(), OrderType.GTC)

        # Token size = USD size / price (number of outcome tokens)
        token_size = order.size_usd / fill_price if fill_price > 0 else 0

        loop = asyncio.get_event_loop()
        signed_order = await loop.run_in_executor(
            None,
            lambda: self._client.create_order(
                OrderArgs(
                    token_id=order.market_id,
                    price=fill_price,
                    size=token_size,
                    side=order.side.upper(),
                    fee_rate_bps=int(self._fee_rate * 10_000),
                    nonce=int(time.time() * 1000),
                    expiration=0,
                ),
                options={"order_type": clob_order_type},
            ),
        )
        return signed_order

    @retry(
        retry=retry_if_exception_type((ConnectionError, TimeoutError, OSError)),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        reraise=True,
    )
    async def _submit_with_retry(self, signed_order: Any) -> dict:
        """Submit signed order to CLOB. Retries network errors 3×."""
        loop = asyncio.get_event_loop()
        response = await loop.run_in_executor(
            None,
            lambda: self._client.post_order(signed_order),
        )
        if not response:
            raise ExecutionError("Empty response from CLOB API")
        return response if isinstance(response, dict) else {"order_id": str(response)}

    async def _await_fill(self, order: OrderRequest, response: dict) -> OrderResult:
        """Poll order status until filled, cancelled, or timeout."""
        clob_order_id = response.get("orderID") or response.get("order_id", "")
        deadline = time.monotonic() + self._timeout_s
        loop = asyncio.get_event_loop()

        while time.monotonic() < deadline:
            await asyncio.sleep(2)
            try:
                status = await loop.run_in_executor(
                    None,
                    lambda: self._client.get_order(clob_order_id),
                )
                state = (status.get("status") or "").upper()

                if state in ("MATCHED", "FILLED"):
                    fill_price = float(status.get("avgPrice") or order.price)
                    size_matched = float(status.get("sizeMatched") or 0)
                    size_usd = size_matched * fill_price
                    fee = size_usd * self._fee_rate
                    slippage = (fill_price - order.price) / order.price if order.price > 0 else 0

                    return OrderResult(
                        order_id=order.order_id,
                        status="filled",
                        fill_price=round(fill_price, 6),
                        fill_size=round(size_usd, 4),
                        fill_size_tokens=round(size_matched, 4),
                        fee=round(fee, 4),
                        slippage=round(slippage, 6),
                        timestamp=_now(),
                        tx_hash=status.get("transactionHash"),
                        raw_response=status,
                    )

                elif state == "CANCELLED":
                    return OrderResult(
                        order_id=order.order_id,
                        status="cancelled",
                        fill_price=0.0,
                        fill_size=0.0,
                        fill_size_tokens=0.0,
                        fee=0.0,
                        slippage=0.0,
                        timestamp=_now(),
                        raw_response=status,
                    )

                elif state in ("REJECTED", "INVALID"):
                    return OrderResult.error(
                        order.order_id,
                        f"Order rejected by CLOB: {status.get('errorMsg', '')}",
                    )

            except Exception as exc:
                self._log.warning("fill_poll_error", error=str(exc))

        # Timeout — try to cancel and return partial/timeout
        try:
            await self.cancel_order(clob_order_id)
        except Exception:
            pass

        return OrderResult(
            order_id=order.order_id,
            status="cancelled",
            fill_price=0.0,
            fill_size=0.0,
            fill_size_tokens=0.0,
            fee=0.0,
            slippage=0.0,
            timestamp=_now(),
            error_message=f"Fill timeout after {self._timeout_s}s",
        )

    # ── Queries ───────────────────────────────────────────────────────────────

    async def cancel_order(self, order_id: str) -> bool:
        loop = asyncio.get_event_loop()
        try:
            await loop.run_in_executor(
                None, lambda: self._client.cancel_order(order_id)
            )
            return True
        except Exception as exc:
            self._log.warning("cancel_failed", order_id=order_id, error=str(exc))
            return False

    async def get_balance(self) -> float:
        loop = asyncio.get_event_loop()
        try:
            bal = await loop.run_in_executor(None, self._client.get_balance)
            return float(bal) if bal else 0.0
        except Exception as exc:
            self._log.error("get_balance_failed", error=str(exc))
            return 0.0

    async def get_positions(self) -> list[Position]:
        return list(self._positions.values())

    async def get_portfolio(self) -> Portfolio:
        from strategy.models import Portfolio as P
        balance = await self.get_balance()
        positions = await self.get_positions()
        pos_value = sum(p.size_usd for p in positions)
        return P(
            total_balance=balance + pos_value,
            available_balance=balance,
            total_position_value=pos_value,
            positions=positions,
            daily_pnl=sum(p.unrealized_pnl for p in positions),
            daily_pnl_pct=0.0,
            total_trades_today=0,
            wins_today=0,
            losses_today=0,
            mode=self.mode,
        )

    def register_position(self, position: Position) -> None:
        self._positions[position.position_id] = position

    def remove_position(self, position_id: str) -> None:
        self._positions.pop(position_id, None)
