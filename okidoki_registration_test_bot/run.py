from __future__ import annotations

from app.bot import run_bot
from app.config import load_config
from app.logging_setup import setup_logging


def main() -> None:
    cfg = load_config()
    setup_logging(cfg.log_file)
    run_bot(cfg)


if __name__ == "__main__":
    main()

