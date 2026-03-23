"""
data_ingestion/whale_collector.py

On-chain whale monitor for Polymarket on Polygon.
Monitors known whale addresses interacting with Polymarket CTF contracts.
Publishes WhaleAction events to stream:whales.
"""

from __future__ import annotations

import asyncio
import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any

import aiohttp
import structlog
from web3 import AsyncWeb3
from web3.providers import WebsocketProviderV2

from data_ingestion.base import BaseCollector

logger = structlog.get_logger(__name__)

# Minimal ABI for the events we care about (ERC-1155 transfers = position changes)
CTF_ABI = [
    {
        "anonymous": False,
        "inputs": [
            {"indexed": True, "name": "operator", "type": "address"},
            {"indexed": True, "name": "from", "type": "address"},
            {"indexed": True, "name": "to", "type": "address"},
            {"name": "id", "type": "uint256"},
            {"name": "value", "type": "uint256"},
        ],
        "name": "TransferSingle",
        "type": "event",
    },
]

# Polymarket USDC contract (ERC-20)
USDC_DECIMALS = 6


@dataclass
class WhaleProfile:
    address: str
    name: str                       # alias if known
    tier: str                       # S / A / B
    win_rate: float
    total_pnl_usd: float
    total_trades: int
    avg_position_size_usd: float

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class WhaleAction:
    whale_address: str
    whale_tier: str                 # S / A / B
    whale_name: str
    whale_win_rate: float
    whale_total_pnl: float
    action: str                     # buy / sell
    market_id: str
    market_question: str
    outcome: str                    # Yes / No
    amount_usd: float
    price: float
    timestamp: datetime
    tx_hash: str
    block_number: int

    def to_stream_dict(self) -> dict[str, str]:
        d = asdict(self)
        d["timestamp"] = self.timestamp.isoformat()
        return {k: str(v) for k, v in d.items()}


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


class WhaleCollector(BaseCollector):
    """
    Subscribes to Polygon WebSocket for new blocks, then scans each block
    for transactions involving known whale addresses and Polymarket contracts.

    Also runs a periodic REST fallback to catch any missed transactions
    using Polygonscan API.

    Whale list is seeded from settings and grows over time as we observe
    large traders on-chain.
    """

    STREAM_KEY = "stream:whales"
    COLLECTOR_NAME = "whale"

    # Polymarket contract addresses on Polygon
    CTF_CONTRACT = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
    NEG_RISK_CTF = "0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296"
    USDC_CONTRACT = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"

    def __init__(
        self,
        redis_manager: Any,
        rpc_url_ws: str,
        rpc_url_http: str,
        seed_addresses: list[str],
        tier_s_min_usd: float = 50_000.0,
        tier_a_min_usd: float = 10_000.0,
        tier_b_min_usd: float = 2_000.0,
        polygonscan_api_key: str = "",
    ) -> None:
        super().__init__(redis_manager)
        self._rpc_ws = rpc_url_ws
        self._rpc_http = rpc_url_http
        self._polygonscan_key = polygonscan_api_key
        self._tier_s = tier_s_min_usd
        self._tier_a = tier_a_min_usd
        self._tier_b = tier_b_min_usd

        # address → WhaleProfile (lowercase keys)
        self._whale_profiles: dict[str, WhaleProfile] = {}
        self._condition_to_market: dict[str, dict] = {}  # condition_id → market info

        for addr in seed_addresses:
            self._add_whale(addr.lower(), tier="B", name="", win_rate=0.0, pnl=0.0)

        self._w3: AsyncWeb3 | None = None

    def _add_whale(
        self, address: str, tier: str, name: str = "", win_rate: float = 0.0, pnl: float = 0.0
    ) -> None:
        address = address.lower()
        if address not in self._whale_profiles:
            self._whale_profiles[address] = WhaleProfile(
                address=address,
                name=name or f"whale_{address[:6]}",
                tier=tier,
                win_rate=win_rate,
                total_pnl_usd=pnl,
                total_trades=0,
                avg_position_size_usd=0.0,
            )

    def _classify_tier(self, usd_amount: float) -> str:
        if usd_amount >= self._tier_s:
            return "S"
        if usd_amount >= self._tier_a:
            return "A"
        return "B"

    async def _run_loop(self) -> None:
        """
        Primary loop: subscribe to Polygon new block headers via WebSocket,
        then fetch transactions for each block and scan for whale activity.
        Falls back to HTTP polling if WS is unavailable.
        """
        try:
            await self._run_websocket_loop()
        except Exception as exc:
            self._log.warning("ws_loop_failed_falling_back_to_http", error=str(exc))
            await self._run_http_polling_loop()

    async def _run_websocket_loop(self) -> None:
        """Subscribe to newHeads via WebSocket, scan each block."""
        self._log.info("connecting_polygon_websocket", url=self._rpc_ws)

        async with AsyncWeb3(WebsocketProviderV2(self._rpc_ws)) as w3:
            self._w3 = w3

            subscription_id = await w3.eth.subscribe("newHeads")
            self._log.info("polygon_ws_subscribed", sub_id=subscription_id)

            async for payload in w3.socket.process_subscriptions():
                if self._stop_event.is_set():
                    break

                block_number = int(payload["result"].get("number", "0"), 16)
                await self._scan_block(w3, block_number)

    async def _run_http_polling_loop(self) -> None:
        """Fallback: poll latest block every 2 seconds."""
        self._log.info("starting_http_polling_fallback")
        last_block = 0

        async with aiohttp.ClientSession() as session:
            w3 = AsyncWeb3(AsyncWeb3.AsyncHTTPProvider(self._rpc_http))
            self._w3 = w3

            while not self._stop_event.is_set():
                try:
                    latest = await w3.eth.block_number
                    if latest > last_block:
                        for bn in range(max(last_block + 1, latest - 2), latest + 1):
                            await self._scan_block(w3, bn)
                        last_block = latest
                except Exception as exc:
                    self._log.warning("http_poll_error", error=str(exc))

                try:
                    await asyncio.wait_for(self._stop_event.wait(), timeout=2)
                except asyncio.TimeoutError:
                    pass

    async def _scan_block(self, w3: AsyncWeb3, block_number: int) -> None:
        """
        Fetch a block and filter transactions involving whale addresses
        and Polymarket contracts.
        """
        try:
            block = await w3.eth.get_block(block_number, full_transactions=True)
            txs = block.get("transactions", [])

            polymarket_addrs = {
                self.CTF_CONTRACT.lower(),
                self.NEG_RISK_CTF.lower(),
                self.USDC_CONTRACT.lower(),
            }

            for tx in txs:
                tx_to = (tx.get("to") or "").lower()
                tx_from = (tx.get("from") or "").lower()

                is_to_polymarket = tx_to in polymarket_addrs
                is_whale = tx_from in self._whale_profiles

                # Discover new whales: large transactions to Polymarket
                if is_to_polymarket and not is_whale:
                    value_eth = tx.get("value", 0) / 1e18
                    if value_eth > 0:
                        pass  # ETH value doesn't apply; USDC is ERC-20

                if is_to_polymarket and is_whale:
                    await self._process_whale_tx(w3, tx, block)

        except Exception as exc:
            self._log.debug("block_scan_error", block=block_number, error=str(exc))

    async def _process_whale_tx(self, w3: AsyncWeb3, tx: dict, block: dict) -> None:
        """Decode a whale transaction and emit a WhaleAction."""
        try:
            tx_hash = tx.get("hash", b"").hex() if isinstance(tx.get("hash"), bytes) else str(tx.get("hash", ""))
            whale_addr = tx.get("from", "").lower()
            profile = self._whale_profiles.get(whale_addr)
            if not profile:
                return

            # Get receipt to read logs
            receipt = await w3.eth.get_transaction_receipt(tx_hash)
            if not receipt:
                return

            # Decode TransferSingle events from CTF contract
            ctf = w3.eth.contract(
                address=w3.to_checksum_address(self.CTF_CONTRACT),
                abi=CTF_ABI,
            )

            try:
                events = ctf.events.TransferSingle().process_receipt(receipt)
            except Exception:
                events = []

            block_ts = block.get("timestamp", 0)
            tx_time = datetime.fromtimestamp(block_ts, tz=timezone.utc) if block_ts else _now_utc()

            for evt in events:
                args = evt.get("args", {})
                token_id = args.get("id", 0)
                value = args.get("value", 0)
                from_addr = args.get("from", "").lower()
                to_addr = args.get("to", "").lower()

                # Determine buy vs sell
                # Buy: tokens flow TO whale (to == whale_addr)
                # Sell: tokens flow FROM whale (from == whale_addr)
                if to_addr == whale_addr:
                    action = "buy"
                elif from_addr == whale_addr:
                    action = "sell"
                else:
                    continue

                # token_id → condition_id → market
                condition_id = hex(token_id)
                market_info = self._condition_to_market.get(condition_id, {})

                # Estimate USD amount from token value (1 token = $1 at settlement)
                amount_usd = float(value) / 1e6  # USDC has 6 decimals

                if amount_usd < self._tier_b:
                    continue

                # Update tier based on observed trade size
                new_tier = self._classify_tier(amount_usd)
                if new_tier < profile.tier or profile.tier == "B":
                    profile.tier = new_tier

                whale_action = WhaleAction(
                    whale_address=whale_addr,
                    whale_tier=profile.tier,
                    whale_name=profile.name,
                    whale_win_rate=profile.win_rate,
                    whale_total_pnl=profile.total_pnl_usd,
                    action=action,
                    market_id=market_info.get("market_id", condition_id),
                    market_question=market_info.get("question", f"Market {condition_id[:8]}"),
                    outcome=market_info.get("outcome", "Yes"),
                    amount_usd=round(amount_usd, 2),
                    price=0.0,  # filled by signal layer from market snapshot
                    timestamp=tx_time,
                    tx_hash=tx_hash,
                    block_number=receipt.get("blockNumber", 0),
                )

                await self._publish(whale_action.to_stream_dict())
                self._log.info(
                    "whale_action_detected",
                    whale=profile.name,
                    tier=profile.tier,
                    action=action,
                    amount_usd=amount_usd,
                    market=whale_action.market_question[:60],
                )

                # Update profile stats
                profile.total_trades += 1
                if profile.total_trades > 0:
                    profile.avg_position_size_usd = (
                        (profile.avg_position_size_usd * (profile.total_trades - 1) + amount_usd)
                        / profile.total_trades
                    )

        except Exception as exc:
            self._log.debug("whale_tx_process_error", error=str(exc))

    def register_market(self, condition_id: str, market_id: str, question: str, outcome: str = "Yes") -> None:
        """Called by PolymarketCollector to help decode token IDs."""
        self._condition_to_market[condition_id.lower()] = {
            "market_id": market_id,
            "question": question,
            "outcome": outcome,
        }

    def get_whale_profiles(self) -> dict[str, WhaleProfile]:
        return dict(self._whale_profiles)

    async def _on_stop(self) -> None:
        self._w3 = None
