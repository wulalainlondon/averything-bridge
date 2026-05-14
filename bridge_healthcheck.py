#!/usr/bin/env python3
import argparse
import socket
import sys


def main() -> int:
    p = argparse.ArgumentParser(description='Bridge TCP healthcheck')
    p.add_argument('--host', default='127.0.0.1')
    p.add_argument('--port', type=int, default=8766)
    p.add_argument('--timeout', type=float, default=1.5)
    args = p.parse_args()

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(args.timeout)
    try:
      s.connect((args.host, args.port))
      return 0
    except Exception:
      return 1
    finally:
      try:
        s.close()
      except Exception:
        pass


if __name__ == '__main__':
    raise SystemExit(main())
