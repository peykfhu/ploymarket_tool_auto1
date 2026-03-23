"""
web/dashboard_server.py
Serves dashboard.html as a static file on port 3056.
Completely standalone — no trading system imports needed.
"""
import asyncio
import os
from pathlib import Path

DASHBOARD_PATH = Path(__file__).parent / "dashboard.html"
HOST = os.environ.get("DASHBOARD_HOST", "0.0.0.0")
PORT = int(os.environ.get("DASHBOARD_PORT", "3056"))


async def start_dashboard():
    try:
        # Try aiohttp first
        from aiohttp import web

        async def index(request):
            return web.FileResponse(DASHBOARD_PATH)

        app = web.Application()
        app.router.add_get("/", index)
        app.router.add_get("/dashboard", index)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, HOST, PORT)
        await site.start()
        print(f"[dashboard] Listening on http://{HOST}:{PORT}", flush=True)
        print(f"[dashboard] Serving: {DASHBOARD_PATH}", flush=True)
        return runner

    except ImportError:
        # Fallback: Python built-in HTTP server
        import http.server
        import threading

        os.chdir(str(DASHBOARD_PATH.parent))

        class Handler(http.server.SimpleHTTPRequestHandler):
            def do_GET(self):
                self.path = "/dashboard.html"
                return super().do_GET()
            def log_message(self, fmt, *args):
                pass  # suppress request logs

        server = http.server.HTTPServer((HOST, PORT), Handler)
        t = threading.Thread(target=server.serve_forever, daemon=True)
        t.start()
        print(f"[dashboard] Listening on http://{HOST}:{PORT} (stdlib fallback)", flush=True)
        return server


async def main():
    runner = await start_dashboard()
    try:
        # Keep alive forever
        while True:
            await asyncio.sleep(3600)
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass
    finally:
        if hasattr(runner, "cleanup"):
            await runner.cleanup()


if __name__ == "__main__":
    asyncio.run(main())
