from __future__ import annotations

import argparse
import logging
import signal
import sys
import threading
from logging.handlers import RotatingFileHandler
from pathlib import Path

from playerokapi.account import Account
from playerokapi.exceptions import BotCheckDetectedException, UnauthorizedError

from republisher import (
    ConfigurationError,
    Republisher,
    Settings,
    SingleInstance,
    StateStore,
    load_env,
    load_item_ids,
)


ROOT = Path(__file__).resolve().parent

for stream in (sys.stdout, sys.stderr):
    try:
        stream.reconfigure(encoding="utf-8")
    except (AttributeError, ValueError):
        pass


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Следит за выбранными товарами Playerok и бесплатно перевыставляет их."
    )
    parser.add_argument("--once", action="store_true", help="Проверить список один раз и выйти")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Проверять статусы, но ничего не перевыставлять",
    )
    return parser.parse_args()


def configure_logging(settings: Settings) -> None:
    logs = ROOT / "logs"
    logs.mkdir(exist_ok=True)
    level = getattr(logging, settings.log_level, logging.INFO)
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    console = logging.StreamHandler()
    console.setFormatter(formatter)
    file_handler = RotatingFileHandler(
        logs / "republisher.log",
        maxBytes=5 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)
    logging.basicConfig(level=level, handlers=[console, file_handler])
    logging.getLogger("FunPayAPI").setLevel(logging.WARNING)


def build_account(settings: Settings) -> Account:
    env = load_env(ROOT / ".env")
    cookies = env.get("PLAYEROK_COOKIES", "").strip()
    if not cookies or cookies == "ВСТАВЬТЕ_COOKIES_СЮДА":
        raise ConfigurationError(
            "Заполните PLAYEROK_COOKIES в файле .env и запустите снова."
        )
    return Account(
        cookies=cookies,
        user_agent=env.get("PLAYEROK_USER_AGENT", "").strip(),
        proxy=env.get("PLAYEROK_PROXY", "").strip() or None,
        requests_timeout=settings.request_timeout_seconds,
    )


def main() -> int:
    args = parse_args()
    settings = Settings.load(ROOT / "settings.json")
    configure_logging(settings)
    logger = logging.getLogger("playerok_republisher")

    item_ids = load_item_ids(ROOT / "items.txt")
    if not item_ids:
        raise ConfigurationError(
            "items.txt пуст. Добавьте ID товаров: по одному UUID на строку."
        )

    stop_event = threading.Event()

    def request_stop(signum: int, frame: object) -> None:
        del signum, frame
        logger.info("Получен сигнал остановки. Завершаю текущую операцию…")
        stop_event.set()

    signal.signal(signal.SIGINT, request_stop)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, request_stop)

    with SingleInstance(ROOT / ".republisher.lock"):
        account = build_account(settings)
        logger.info("Подключаюсь к Playerok…")
        account.get()
        if not account.id:
            raise RuntimeError("Playerok не вернул ID аккаунта")
        if account.can_publish_items is False:
            raise RuntimeError("Playerok запретил этому аккаунту публиковать товары")
        logger.info(
            "Подключено: %s (%s). Товаров в списке: %s. Режим: %s",
            account.username,
            account.id,
            len(item_ids),
            "ПРОВЕРКА БЕЗ ИЗМЕНЕНИЙ" if args.dry_run else "АВТОПЕРЕВЫСТАВЛЕНИЕ",
        )

        state = StateStore(ROOT / "state.json")
        state.bind_account(str(account.id))
        republisher = Republisher(
            account,
            settings,
            state,
            ROOT / "items.txt",
            dry_run=args.dry_run,
            stop_event=stop_event,
        )
        if args.once:
            summary = republisher.run_cycle(initial=True)
            logger.info(
                "Готово: проверено=%s, активно=%s, ожидают=%s, "
                "перевыставлено=%s, требовало бы перевыставления=%s, "
                "заблокировано=%s, ошибок=%s",
                summary.checked,
                summary.active,
                summary.waiting,
                summary.republished,
                summary.would_republish,
                summary.stopped,
                summary.failed,
            )
            return 1 if summary.failed else 0

        logger.info(
            "Мониторинг запущен. Основной режим: события новых сделок; "
            "страховочная сверка раз в %.0f мин. Остановка: Ctrl+C.",
            settings.reconciliation_interval_seconds / 60,
        )
        republisher.run_forever()
        logger.info("Скрипт остановлен.")
        return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except ConfigurationError as exc:
        print(f"\nОШИБКА НАСТРОЙКИ: {exc}\n", file=sys.stderr)
        raise SystemExit(2)
    except (UnauthorizedError, BotCheckDetectedException) as exc:
        print(f"\nОШИБКА АВТОРИЗАЦИИ PLAYEROK: {exc}\nОбновите cookies в .env.\n", file=sys.stderr)
        raise SystemExit(3)
    except KeyboardInterrupt:
        print("\nОстановлено пользователем.")
        raise SystemExit(130)
    except Exception as exc:
        logging.getLogger("playerok_republisher").exception("Критическая ошибка: %s", exc)
        print(
            f"\nКРИТИЧЕСКАЯ ОШИБКА: {exc}\n"
            "Подробности сохранены в logs/republisher.log.\n",
            file=sys.stderr,
        )
        raise SystemExit(1)

