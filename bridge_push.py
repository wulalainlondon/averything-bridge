#!/usr/bin/env python3
"""Push a file to connected phone clients via the bridge.

Usage:
    python bridge_push.py /path/to/file.apk
    python bridge_push.py ~/Downloads/photo.jpg
"""
import asyncio
import json
import os
import sys


async def push(path: str) -> None:
    try:
        import websockets
    except ImportError:
        print("Error: websockets not installed. Run: pip install websockets")
        sys.exit(1)

    port = int(os.environ.get("BRIDGE_PORT", "8766"))
    url = f"ws://localhost:{port}"

    print(f"Connecting to bridge at {url} ...")
    try:
        async with websockets.connect(url, open_timeout=5, max_size=None) as ws:
            await ws.send(json.dumps({"type": "hello", "device_id": "cli_push", "device_name": "bridge_push CLI"}))

            # Wait for hello_ack
            for _ in range(5):
                raw = await asyncio.wait_for(ws.recv(), timeout=3)
                msg = json.loads(raw)
                if msg.get("type") == "hello_ack":
                    break

            import os as _os
            target_filename = _os.path.basename(path)
            print(f"Uploading: {path}")
            await ws.send(json.dumps({"type": "push_file", "path": path}))

            # Wait for push_ack (direct ack from bridge) or file_push echo or error.
            for _ in range(20):
                raw = await asyncio.wait_for(ws.recv(), timeout=15)
                msg = json.loads(raw)
                t = msg.get("type")
                if t == "push_ack":
                    print(f"✓ Pushed: {msg['filename']} ({msg['size']} bytes)")
                    return
                if t == "file_push" and msg.get("filename") == target_filename:
                    print(f"✓ Pushed: {msg['filename']} ({msg['size']} bytes)")
                    if msg.get('url'):
                        print(f"  Download URL: {msg['url']}")
                    return
                if t == "error":
                    print(f"✗ Error: {msg.get('message')}")
                    sys.exit(1)

            print("✗ Timeout waiting for upload confirmation")
            sys.exit(1)

    except ConnectionRefusedError:
        print(f"✗ Could not connect to bridge on port {port}. Is it running?")
        sys.exit(1)
    except Exception as exc:
        print(f"✗ {type(exc).__name__}: {exc}")
        sys.exit(1)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python bridge_push.py <file_path>")
        sys.exit(1)
    asyncio.run(push(sys.argv[1]))
