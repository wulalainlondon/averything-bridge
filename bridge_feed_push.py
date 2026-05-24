#!/usr/bin/env python3
"""Push an HTML article to connected phone clients via the bridge feed channel.

Usage:
    python bridge_feed_push.py --title "Article Title" --html-file /path/to/article.html
    python bridge_feed_push.py --title "Article Title" --html "<h1>Content</h1>"
    cat article.html | python bridge_feed_push.py --title "Article Title" --html-stdin
"""
import argparse
import asyncio
import json
import os
import sys


async def feed_push(
    title: str,
    html: str,
    source: str,
    url: str,
    client_dedup_key: str,
    port: int,
    content_type: str = "html",
) -> None:
    try:
        import websockets
    except ImportError:
        print("Error: websockets not installed. Run: pip install websockets")
        sys.exit(1)

    if len(html.encode("utf-8")) > 5 * 1024 * 1024:
        print("✗ Error: HTML size exceeds 5 MB limit")
        sys.exit(1)

    ws_url = f"ws://localhost:{port}"
    print(f"Connecting to bridge at {ws_url} ...")

    try:
        async with websockets.connect(ws_url, open_timeout=5, max_size=None) as ws:
            await ws.send(json.dumps({
                "type": "hello",
                "device_id": "cli_feed_push",
                "device_name": "bridge_feed_push CLI",
            }))

            # Drain until hello_ack (max 5 attempts)
            for _ in range(5):
                raw = await asyncio.wait_for(ws.recv(), timeout=3)
                msg = json.loads(raw)
                if msg.get("type") == "hello_ack":
                    break

            await ws.send(json.dumps({
                "type": "feed_push",
                "title": title,
                "html": html,
                "source": source,
                "url": url,
                "client_dedup_key": client_dedup_key,
                "content_type": content_type,
            }))

            # Drain until feed_ack or error (max 15 seconds total)
            deadline = asyncio.get_event_loop().time() + 15
            while True:
                remaining = deadline - asyncio.get_event_loop().time()
                if remaining <= 0:
                    print("✗ Timeout")
                    sys.exit(1)
                raw = await asyncio.wait_for(ws.recv(), timeout=remaining)
                msg = json.loads(raw)
                t = msg.get("type")
                if t == "feed_ack":
                    print(f"✓ Feed pushed: {msg.get('feed_id')}")
                    return
                if t == "error":
                    print(f"✗ Error: {msg.get('message')}")
                    sys.exit(1)

    except ConnectionRefusedError:
        print(f"✗ Could not connect to bridge on port {port}. Is it running?")
        sys.exit(1)
    except Exception as exc:
        print(f"✗ {type(exc).__name__}: {exc}")
        sys.exit(1)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Push an HTML article to the bridge feed channel.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--title", required=True, help="Article title (required)")

    content_group = parser.add_mutually_exclusive_group(required=True)
    content_group.add_argument("--html", metavar="TEXT", help="HTML string")
    content_group.add_argument("--html-file", metavar="PATH", help="Path to HTML or Markdown file (auto-detected by extension)")
    content_group.add_argument("--html-stdin", action="store_true", help="Read HTML from stdin")
    content_group.add_argument("--md", metavar="TEXT", help="Markdown string")
    content_group.add_argument("--md-file", metavar="PATH", help="Path to Markdown file")
    content_group.add_argument("--md-stdin", action="store_true", help="Read Markdown from stdin")

    parser.add_argument("--source", default="pipeline", help='Source label (default: "pipeline")')
    parser.add_argument("--url", default="", help="Original article URL (default: \"\")")
    parser.add_argument("--client-dedup-key", default="", metavar="TEXT", help="Deduplication key (default: \"\")")
    parser.add_argument(
        "--port",
        type=int,
        default=int(os.environ.get("BRIDGE_PORT", "8766")),
        help="Bridge WebSocket port (default: env BRIDGE_PORT or 8766)",
    )

    args = parser.parse_args()

    content_type = "html"

    if args.html is not None:
        html_content = args.html
    elif args.md is not None:
        html_content = args.md
        content_type = "markdown"
    elif args.md_file is not None:
        try:
            with open(args.md_file, "r", encoding="utf-8") as f:
                html_content = f.read()
        except OSError as exc:
            print(f"✗ Could not read file: {exc}")
            sys.exit(1)
        content_type = "markdown"
    elif args.md_stdin:
        html_content = sys.stdin.read()
        content_type = "markdown"
    elif args.html_file is not None:
        try:
            with open(args.html_file, "r", encoding="utf-8") as f:
                html_content = f.read()
        except OSError as exc:
            print(f"✗ Could not read file: {exc}")
            sys.exit(1)
        if args.html_file.lower().endswith(".md"):
            content_type = "markdown"
    else:
        html_content = sys.stdin.read()

    asyncio.run(
        feed_push(
            title=args.title,
            html=html_content,
            source=args.source,
            url=args.url,
            client_dedup_key=args.client_dedup_key,
            port=args.port,
            content_type=content_type,
        )
    )


if __name__ == "__main__":
    main()
