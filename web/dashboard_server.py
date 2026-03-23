"""
web/dashboard_server.py
Serves dashboard.html on port 3056.
Run standalone: python web/dashboard_server.py
"""
import asyncio
from pathlib import Path
from aiohttp import web

DASHBOARD_PATH = Path(__file__).parent / "dashboard.html"


async def index(request):
    return web.FileResponse(DASHBOARD_PATH)


async def start_dashboard(host: str = "0.0.0.0", port: int = 3056):
    app = web.Application()
    app.router.add_get("/", index)
    app.router.add_get("/dashboard", index)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host, port)
    await site.start()
    return runner


if __name__ == "__main__":
    async def main():
        runner = await start_dashboard()
        print(f"Dashboard running at http://localhost:3056")
        print(f"API backend should be at http://localhost:8088")
        try:
            await asyncio.Event().wait()
        finally:
            await runner.cleanup()
    asyncio.run(main())
