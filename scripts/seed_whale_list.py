"""Seed known whale addresses from Polymarket leaderboard into the database."""
import asyncio
import sys; sys.path.insert(0,'.')
from config.settings import get_settings

KNOWN_WHALES = [
    {"address":"0x1234567890abcdef1234567890abcdef12345678","name":"whale_alpha","tier":"S"},
    {"address":"0xabcdef1234567890abcdef1234567890abcdef12","name":"whale_beta","tier":"A"},
]

async def main():
    settings = get_settings()
    from database.connection import init_db
    await init_db(settings.database.postgres_url)
    from database.connection import _session_factory
    from database.repository import WhaleRepository
    from database.models import WhaleProfile as WP
    async with _session_factory() as session:
        async with session.begin():
            repo = WhaleRepository(session)
            for w in KNOWN_WHALES:
                profile = type('P',(),{**w,'win_rate':0,'total_pnl_usd':0,'total_trades':0,'avg_position_size_usd':0})()
                await repo.upsert_profile(profile)
    print(f"Seeded {len(KNOWN_WHALES)} whale profiles.")

asyncio.run(main())
