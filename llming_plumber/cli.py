from __future__ import annotations


def main() -> None:
    """CLI entry point for llming-plumber."""
    import argparse

    parser = argparse.ArgumentParser(prog="llming-plumber")
    sub = parser.add_subparsers(dest="command")

    serve = sub.add_parser("serve", help="Start the Plumber HTTP server")
    serve.add_argument(
        "--mode",
        choices=["all", "ui", "worker"],
        default=None,
        help="Deployment mode (default: from PLUMBER_MODE env var)",
    )
    serve.add_argument("--host", default=None, help="Bind host")
    serve.add_argument("--port", type=int, default=None, help="Bind port")

    args = parser.parse_args()

    if args.command == "serve":
        import os

        import uvicorn

        from llming_plumber.config import settings

        if args.mode is not None:
            os.environ["PLUMBER_MODE"] = args.mode

        host = args.host or settings.host
        port = args.port or settings.port

        uvicorn.run(
            "llming_plumber.main:app",
            host=host,
            port=port,
            reload=True,
        )
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
