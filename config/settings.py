"""
config/settings.py

Central configuration using pydantic-settings.
Sensitive values injected via env vars. Defaults live in settings.yaml.

Usage:
    from config.settings import settings
    print(settings.database.postgres_url)
"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

ROOT_DIR = Path(__file__).parent.parent


class DatabaseSettings(BaseModel):
    postgres_host: str = "localhost"
    postgres_port: int = 5432
    postgres_db: str = "polymarket"
    postgres_user: str = "trader"
    postgres_password: SecretStr = SecretStr("changeme")
    postgres_pool_size: int = 10
    postgres_max_overflow: int = 20
    redis_host: str = "localhost"
    redis_port: int = 6379
    redis_password: SecretStr = SecretStr("changeme")
    redis_db: int = 0
    redis_pool_size: int = 20

    @property
    def postgres_url(self) -> str:
        pw = self.postgres_password.get_secret_value()
        return (
            f"postgresql+asyncpg://{self.postgres_user}:{pw}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )

    @property
    def redis_url(self) -> str:
        pw = self.redis_password.get_secret_value()
        return f"redis://:{pw}@{self.redis_host}:{self.redis_port}/{self.redis_db}"


class PolymarketSettings(BaseModel):
    chain_id: int = 137
    rpc_url: str = "https://polygon-rpc.com"
    rpc_url_ws: str = "wss://polygon-bor-rpc.publicnode.com"
    clob_api_url: str = "https://clob.polymarket.com"
    gamma_api_url: str = "https://gamma-api.polymarket.com"
    private_key: SecretStr = SecretStr("")
    api_key: SecretStr = SecretStr("")
    api_secret: SecretStr = SecretStr("")
    api_passphrase: SecretStr = SecretStr("")
    usdc_address: str = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
    ctf_address: str = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
    neg_risk_ctf_address: str = "0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296"
    market_refresh_interval_s: int = 300
    price_refresh_interval_s: int = 10
    min_market_age_hours: int = 24
    max_markets_to_watch: int = 50


class NewsSettings(BaseModel):
    twitter_bearer_token: SecretStr = SecretStr("")
    newsapi_key: SecretStr = SecretStr("")
    gnews_api_key: SecretStr = SecretStr("")
    twitter_poll_interval_s: int = 0
    newsapi_poll_interval_s: int = 30
    rss_poll_interval_s: int = 60
    twitter_watchlist: list[str] = Field(default_factory=lambda: [
        "POTUS", "WhiteHouse", "AP", "Reuters", "Bloomberg",
        "BBCBreaking", "nytimes", "WSJ", "FT",
    ])
    rss_feeds: list[str] = Field(default_factory=lambda: [
        "https://feeds.bbci.co.uk/news/rss.xml",
        "https://rss.nytimes.com/services/xml/rss/nyt/HomePage.xml",
        "https://feeds.reuters.com/reuters/topNews",
        "https://feeds.a.dj.com/rss/RSSWorldNews.xml",
    ])


class SportsSettings(BaseModel):
    api_football_key: SecretStr = SecretStr("")
    odds_api_key: SecretStr = SecretStr("")
    balldontlie_api_key: SecretStr = SecretStr("")
    football_poll_interval_s: int = 10
    basketball_poll_interval_s: int = 15
    nfl_poll_interval_s: int = 15
    tracked_leagues: list[str] = Field(default_factory=lambda: [
        "Premier League", "La Liga", "Champions League",
        "NBA", "NFL", "MLB",
    ])


class WhaleSettings(BaseModel):
    tier_s_min_usd: float = 50_000.0
    tier_a_min_usd: float = 10_000.0
    tier_b_min_usd: float = 2_000.0
    seed_addresses: list[str] = Field(default_factory=list)
    follow_delay_minutes: int = 10
    min_whale_win_rate: float = 0.55
    consensus_min_count: int = 3


class SignalSettings(BaseModel):
    openai_api_key: SecretStr = SecretStr("")
    openai_model: str = "gpt-4o-mini"
    anthropic_api_key: SecretStr = SecretStr("")
    use_llm_for_direction: bool = True
    llm_timeout_s: float = 3.0
    market_match_threshold: float = 0.60
    speed_advantage_max_price_move_pct: float = 0.05
    dedup_window_s: int = 300


class RiskSettings(BaseModel):
    max_single_position_pct: float = 0.08
    max_total_exposure_pct: float = 0.60
    max_concurrent_positions: int = 8
    max_same_category_positions: int = 4
    min_cash_reserve_pct: float = 0.10
    max_daily_loss_pct: float = 0.05
    max_daily_loss_hard_pct: float = 0.10
    max_daily_trades: int = 20
    min_market_liquidity_usd: float = 2_000.0
    max_spread_pct: float = 0.05
    min_market_age_hours: int = 24
    max_trades_per_30min: int = 5
    revenge_trade_lockout_minutes: int = 3
    no_large_orders_quiet_hours: bool = True
    default_sizing_method: str = "fixed_pct"
    kelly_fraction: float = 0.5
    score_to_position: dict[str, float] = Field(default_factory=lambda: {
        "strong_buy": 0.06,
        "buy": 0.03,
    })


class StrategySettings(BaseModel):
    enabled_strategies: list[str] = Field(default_factory=lambda: [
        "swing_mean_reversion", "swing_breakout", "swing_event_driven",
        "sports_safe_lock", "sports_reversal", "sports_live_ou",
        "whale_single_follow", "whale_consensus",
    ])
    strong_buy_threshold: int = 80
    buy_threshold: int = 60
    watch_threshold: int = 40
    mean_reversion_lookback_days: int = 7
    mean_reversion_entry_deviation: float = 0.15
    breakout_confirmation_volume_multiplier: float = 2.0
    safe_lock_max_remaining_minutes: int = 15
    safe_lock_min_lead_goals: int = 2
    reversal_max_market_price: float = 0.35
    whale_follow_min_score: int = 65


class ExecutionSettings(BaseModel):
    mode: str = "paper"
    max_slippage_pct: float = 0.02
    order_timeout_s: int = 30
    max_retries: int = 3
    retry_delay_s: float = 1.0
    paper_initial_balance: float = 10_000.0
    fee_rate: float = 0.02


class NotificationSettings(BaseModel):
    telegram_bot_token: SecretStr = SecretStr("")
    telegram_chat_id: str = ""
    telegram_admin_ids: list[int] = Field(default_factory=list)
    max_messages_per_minute: int = 10
    quiet_hours_start_utc: int = 2
    quiet_hours_end_utc: int = 6
    notify_on_signal: bool = True
    notify_on_trade: bool = True
    notify_on_stop_triggered: bool = True
    notify_on_whale_alert: bool = True
    notify_on_risk_alert: bool = True
    notify_on_circuit_breaker: bool = True
    notify_daily_report: bool = True
    notify_weekly_report: bool = True


class BacktestSettings(BaseModel):
    initial_balance: float = 10_000.0
    fee_rate: float = 0.02
    slippage_model: str = "volume_based"
    fixed_slippage_pct: float = 0.005
    signal_delay_ms: int = 500
    execution_delay_ms: int = 1_000
    data_dir: str = "data/historical"
    walk_forward_train_months: int = 3
    walk_forward_test_months: int = 1


class LoggingSettings(BaseModel):
    level: str = "INFO"
    format: str = "json"
    log_dir: str = "logs"
    max_file_size_mb: int = 100
    backup_count: int = 10


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_nested_delimiter="__",
        case_sensitive=False,
        extra="ignore",
    )

    database: DatabaseSettings = Field(default_factory=DatabaseSettings)
    polymarket: PolymarketSettings = Field(default_factory=PolymarketSettings)
    news: NewsSettings = Field(default_factory=NewsSettings)
    sports: SportsSettings = Field(default_factory=SportsSettings)
    whale: WhaleSettings = Field(default_factory=WhaleSettings)
    signal: SignalSettings = Field(default_factory=SignalSettings)
    risk: RiskSettings = Field(default_factory=RiskSettings)
    strategy: StrategySettings = Field(default_factory=StrategySettings)
    execution: ExecutionSettings = Field(default_factory=ExecutionSettings)
    notification: NotificationSettings = Field(default_factory=NotificationSettings)
    backtest: BacktestSettings = Field(default_factory=BacktestSettings)
    logging: LoggingSettings = Field(default_factory=LoggingSettings)

    @classmethod
    def from_yaml(cls, yaml_path: Path | str | None = None) -> "Settings":
        yaml_path = yaml_path or ROOT_DIR / "config" / "settings.yaml"
        yaml_data: dict[str, Any] = {}
        if Path(yaml_path).exists():
            with open(yaml_path) as f:
                yaml_data = yaml.safe_load(f) or {}
        return cls(**yaml_data)

    @field_validator("execution")
    @classmethod
    def validate_mode(cls, v: ExecutionSettings) -> ExecutionSettings:
        assert v.mode in ("live", "paper", "backtest"), \
            f"execution.mode must be live/paper/backtest, got: {v.mode}"
        return v

    def is_live(self) -> bool:
        return self.execution.mode == "live"

    def is_paper(self) -> bool:
        return self.execution.mode == "paper"

    def is_backtest(self) -> bool:
        return self.execution.mode == "backtest"


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings.from_yaml()


settings = get_settings()
