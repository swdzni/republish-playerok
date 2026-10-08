from __future__ import annotations

import json
import logging
import os
import queue
import re
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable
from urllib.parse import unquote, urlparse

log = logging.getLogger("playerok_republisher")

ITEM_ID_RE = re.compile(
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
)
ITEM_SLUG_RE = re.compile(r"^[0-9a-fA-F]{12}-[a-zA-Z0-9][a-zA-Z0-9-]*$")

ACTIVE_STATUSES = {"APPROVED"}
WAITING_STATUSES = {"PENDING_APPROVAL", "PENDING_MODERATION"}
REPUBLISHABLE_STATUSES = {
    "SOLD",
    "EXPIRED",
    "DRAFT",
    "DISCONTINUED",
    "UNPUBLISHED",
}
STOPPED_STATUSES = {"BLOCKED", "DECLINED"}


class ConfigurationError(RuntimeError):
    pass


class PremiumSafetyError(RuntimeError):
    pass


class OwnershipError(RuntimeError):
    pass


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def enum_name(value: Any) -> str:
    return str(getattr(value, "name", value) or "")


def is_auth_error(exc: Exception) -> bool:
    return exc.__class__.__name__ in {"UnauthorizedError", "BotCheckDetectedException"}


def load_env(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if not path.exists():
        raise ConfigurationError(f"Не найден файл {path.name}")
    for raw in path.read_text(encoding="utf-8-sig").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
            value = value[1:-1]
        values[key.strip()] = value
    return values


def normalize_item_reference(value: str) -> str:
    candidate = value.strip()
    if candidate.casefold().startswith(("playerok.com/", "www.playerok.com/")):
        candidate = "https://" + candidate
    if "://" in candidate:
        parsed = urlparse(candidate)
        host = (parsed.hostname or "").casefold()
        if host not in {"playerok.com", "www.playerok.com"}:
            raise ValueError("ссылка ведёт не на playerok.com")
        parts = [unquote(part) for part in parsed.path.split("/") if part]
        if len(parts) < 2 or parts[0].casefold() != "products":
            raise ValueError("ожидалась ссылка вида playerok.com/products/...")
        candidate = parts[1]
    candidate = candidate.strip().strip("/").casefold()
    if ITEM_ID_RE.fullmatch(candidate) or ITEM_SLUG_RE.fullmatch(candidate):
        return candidate
    raise ValueError("ожидался полный UUID, slug товара или ссылка Playerok")


def load_item_ids(path: Path) -> tuple[str, ...]:
    if not path.exists():
        raise ConfigurationError(f"Не найден файл {path.name}")
    result: list[str] = []
    seen: set[str] = set()
    invalid: list[str] = []
    for line_number, raw in enumerate(
        path.read_text(encoding="utf-8-sig").splitlines(), start=1
    ):
        value = raw.split("#", 1)[0].strip()
        if not value:
            continue
        try:
            normalized = normalize_item_reference(value)
        except ValueError as exc:
            invalid.append(f"строка {line_number}: {value!r} ({exc})")
            continue
        if normalized not in seen:
            result.append(normalized)
            seen.add(normalized)
    if invalid:
        raise ConfigurationError(
            f"Некорректные ID/ссылки в {path.name}: " + "; ".join(invalid)
        )
    return tuple(result)


@dataclass(frozen=True)
class Settings:
    poll_interval_seconds: float = 10.0
    reconciliation_interval_seconds: float = 900.0
    items_reload_interval_seconds: float = 30.0
    event_reconnect_delay_seconds: float = 10.0
    event_status_retry_attempts: int = 3
    event_status_retry_delay_seconds: float = 3.0
    item_request_delay_seconds: float = 0.5
    request_timeout_seconds: int = 45
    retry_attempts: int = 5
    retry_base_delay_seconds: float = 5.0
    retry_max_delay_seconds: float = 60.0
    republish_cooldown_seconds: float = 30.0
    republish_failure_cooldown_seconds: float = 300.0
    heartbeat_interval_seconds: float = 300.0
    log_level: str = "INFO"
    missing_attribute_defaults: dict[str, str] = field(
        default_factory=lambda: {"region": "global"}
    )

    @classmethod
    def load(cls, path: Path) -> "Settings":
        if not path.exists():
            return cls()
        data = json.loads(path.read_text(encoding="utf-8-sig"))
        result = cls(
            poll_interval_seconds=float(data.get("poll_interval_seconds", 10)),
            reconciliation_interval_seconds=float(
                data.get("reconciliation_interval_seconds", 900)
            ),
            items_reload_interval_seconds=float(
                data.get("items_reload_interval_seconds", 30)
            ),
            event_reconnect_delay_seconds=float(
                data.get("event_reconnect_delay_seconds", 10)
            ),
            event_status_retry_attempts=int(
                data.get("event_status_retry_attempts", 3)
            ),
            event_status_retry_delay_seconds=float(
                data.get("event_status_retry_delay_seconds", 3)
            ),
            item_request_delay_seconds=float(data.get("item_request_delay_seconds", 0.5)),
            request_timeout_seconds=int(data.get("request_timeout_seconds", 45)),
            retry_attempts=int(data.get("retry_attempts", 5)),
            retry_base_delay_seconds=float(data.get("retry_base_delay_seconds", 5)),
            retry_max_delay_seconds=float(data.get("retry_max_delay_seconds", 60)),
            republish_cooldown_seconds=float(data.get("republish_cooldown_seconds", 30)),
            republish_failure_cooldown_seconds=float(
                data.get("republish_failure_cooldown_seconds", 300)
            ),
            heartbeat_interval_seconds=float(data.get("heartbeat_interval_seconds", 300)),
            log_level=str(data.get("log_level", "INFO")).upper(),
            missing_attribute_defaults={
                str(key): str(value)
                for key, value in dict(
                    data.get("missing_attribute_defaults", {"region": "global"})
                ).items()
            },
        )
        if result.poll_interval_seconds < 1:
            raise ConfigurationError("poll_interval_seconds должен быть не меньше 1")
        if result.reconciliation_interval_seconds < 60:
            raise ConfigurationError(
                "reconciliation_interval_seconds должен быть не меньше 60"
            )
        if result.items_reload_interval_seconds < 5:
            raise ConfigurationError("items_reload_interval_seconds должен быть не меньше 5")
        if result.event_reconnect_delay_seconds < 1:
            raise ConfigurationError("event_reconnect_delay_seconds должен быть не меньше 1")
        if result.event_status_retry_attempts < 1:
            raise ConfigurationError("event_status_retry_attempts должен быть не меньше 1")
        if result.event_status_retry_delay_seconds < 1:
            raise ConfigurationError(
                "event_status_retry_delay_seconds должен быть не меньше 1"
            )
        if result.item_request_delay_seconds < 0:
            raise ConfigurationError("item_request_delay_seconds не может быть отрицательным")
        if result.request_timeout_seconds < 5:
            raise ConfigurationError("request_timeout_seconds должен быть не меньше 5")
        if result.retry_attempts < 1:
            raise ConfigurationError("retry_attempts должен быть не меньше 1")
        if result.retry_base_delay_seconds < 1 or result.retry_max_delay_seconds < 1:
            raise ConfigurationError("Задержки повторов должны быть не меньше 1 секунды")
        if result.republish_cooldown_seconds < 0:
            raise ConfigurationError("republish_cooldown_seconds не может быть отрицательным")
        if result.republish_failure_cooldown_seconds < 0:
            raise ConfigurationError(
                "republish_failure_cooldown_seconds не может быть отрицательным"
            )
        return result


class StateStore:
    def __init__(self, path: Path):
        self.path = path
        self.data: dict[str, Any] = {"version": 1, "account_id": "", "items": {}}
        if path.exists():
            loaded = json.loads(path.read_text(encoding="utf-8-sig"))
            if int(loaded.get("version", 0)) != 1:
                raise ConfigurationError("Неподдерживаемая версия state.json")
            self.data.update(loaded)

    def bind_account(self, account_id: str) -> None:
        previous = str(self.data.get("account_id", ""))
        if previous and previous != account_id:
            stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
            backup = self.path.with_name(f"state.{previous}.{stamp}.json")
            if self.path.exists():
                backup.write_bytes(self.path.read_bytes())
            log.warning(
                "Аккаунт изменился (%s → %s). Старое состояние сохранено в %s",
                previous,
                account_id,
                backup.name,
            )
            self.data = {"version": 1, "account_id": account_id, "items": {}}
        else:
            self.data["account_id"] = account_id
        self.save()

    def item(self, item_id: str) -> dict[str, Any]:
        return dict(self.data.setdefault("items", {}).get(item_id, {}))

    def update(self, item_id: str, **fields: Any) -> None:
        items = self.data.setdefault("items", {})
        current = dict(items.get(item_id, {}))
        changed = False
        for key, value in fields.items():
            if value is None:
                if key in current:
                    current.pop(key, None)
                    changed = True
            elif current.get(key) != value:
                current[key] = value
                changed = True
        if not changed:
            return
        current["updated_at"] = utc_now()
        items[item_id] = current
        self.save()

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.path.with_suffix(".tmp")
        temporary.write_text(
            json.dumps(self.data, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        temporary.replace(self.path)


@dataclass
class CycleSummary:
    checked: int = 0
    active: int = 0
    waiting: int = 0
    republished: int = 0
    would_republish: int = 0
    cooldown: int = 0
    stopped: int = 0
    failed: int = 0


class Republisher:
    def __init__(
        self,
        account: Any,
        settings: Settings,
        state: StateStore,
        items_path: Path,
        *,
        dry_run: bool = False,
        stop_event: threading.Event | None = None,
    ):
        self.account = account
        self.settings = settings
        self.state = state
        self.items_path = items_path
        self.dry_run = dry_run
        self.stop_event = stop_event or threading.Event()
        self._known_ids: tuple[str, ...] = ()
        self._last_heartbeat = 0.0
        self._processed_deal_ids: set[str] = set()
        self._processed_deal_order: deque[str] = deque(maxlen=500)

    @staticmethod
    def _is_rate_limit_error(exc: Exception) -> bool:
        message = str(exc).casefold()
        return any(marker in message for marker in (
            "слишком много попыток",
            "too many requests",
            "rate limit",
            "status code: 429",
            "код ошибки: 429",
        ))

    @staticmethod
    def _is_transient_error(exc: Exception) -> bool:
        message = str(exc).casefold()
        return any(marker in message for marker in (
            "i/o timeout",
            "timed out",
            "timeout",
            "connection reset",
            "connection aborted",
            "temporarily unavailable",
            "код ошибки: 0",
            "status code: 0",
        ))

    @staticmethod
    def _is_already_published_error(exc: Exception) -> bool:
        message = str(exc).casefold()
        return "нельзя обновить статус" in message or "cannot update status" in message

    @staticmethod
    def _is_missing_characteristics_error(exc: Exception) -> bool:
        message = str(exc).casefold()
        return any(marker in message for marker in (
            "обязательные характеристики",
            "required characteristics",
            "required attributes",
        ))

    def _wait(self, seconds: float) -> bool:
        return self.stop_event.wait(max(0.0, seconds))

    def _retry_read(self, label: str, operation: Callable[[], Any]) -> Any:
        for attempt in range(1, self.settings.retry_attempts + 1):
            try:
                return operation()
            except Exception as exc:
                if is_auth_error(exc):
                    raise
                retryable = self._is_rate_limit_error(exc) or self._is_transient_error(exc)
                if not retryable or attempt >= self.settings.retry_attempts:
                    raise
                delay = min(
                    self.settings.retry_max_delay_seconds,
                    self.settings.retry_base_delay_seconds * (2 ** (attempt - 1)),
                )
                log.warning(
                    "%s: временная ошибка, попытка %s/%s: %s. Повтор через %.0f сек.",
                    label,
                    attempt,
                    self.settings.retry_attempts,
                    exc,
                    delay,
                )
                if self._wait(delay):
                    raise KeyboardInterrupt

    def _get_item(self, item_reference: str) -> Any:
        if ITEM_ID_RE.fullmatch(item_reference):
            lookup = lambda: self.account.get_item(id=item_reference)
        else:
            lookup = lambda: self.account.get_item(slug=item_reference)
        return self._retry_read(
            f"Проверка {item_reference}", lookup
        )

    def _validate_ownership(self, item: Any) -> None:
        owner_id = str(getattr(getattr(item, "user", None), "id", "") or "")
        if owner_id and owner_id != str(self.account.id):
            raise OwnershipError(
                f"товар принадлежит аккаунту {owner_id}, текущий аккаунт {self.account.id}"
            )

    def _free_priority_id(self, item: Any) -> str:
        statuses = self._retry_read(
            f"Получение бесплатного статуса {item.id}",
            lambda: self.account.get_item_priority_statuses(item.id, int(item.price)),
        )
        free = [
            status
            for status in statuses
            if enum_name(getattr(status, "type", "")) == "DEFAULT"
            and int(getattr(status, "price", -1)) == 0
        ]
        if not free:
            offered = ", ".join(
                f"{enum_name(getattr(status, 'type', 'UNKNOWN'))}="
                f"{getattr(status, 'price', '?')} ₽"
                for status in statuses
            )
            raise PremiumSafetyError(
                "Playerok не предложил бесплатный DEFAULT-статус. "
                f"Публикация отменена; доступно: {offered or 'ничего'}"
            )
        return str(free[0].id)

    def _restore_required_characteristics(self, item: Any) -> Any:
        category_id = str(getattr(getattr(item, "category", None), "id", "") or "")
        if not category_id:
            raise RuntimeError("у товара не найден ID категории")
        category = self._retry_read(
            f"Получение характеристик категории {category_id}",
            lambda: self.account.get_game_category(id=category_id),
        )
        options = list(getattr(category, "options", None) or [])
        if not options:
            raise RuntimeError("Playerok не вернул характеристики категории")

        attributes = dict(getattr(item, "attributes", None) or {})
        defaults_used: dict[str, str] = {}
        selected: list[Any] = []
        missing: list[str] = []
        fields = sorted({str(option.field) for option in options})
        for option_field in fields:
            raw_value = attributes.get(option_field)
            if raw_value is None or raw_value == "" or raw_value == []:
                default = self.settings.missing_attribute_defaults.get(option_field)
                if default is None:
                    available = [
                        str(option.value)
                        for option in options
                        if str(option.field) == option_field
                    ]
                    missing.append(f"{option_field} ({', '.join(available)})")
                    continue
                raw_value = default
                defaults_used[option_field] = default

            values = (
                {str(value) for value in raw_value}
                if isinstance(raw_value, (list, tuple, set))
                else {str(raw_value)}
            )
            matched = [
                option
                for option in options
                if str(option.field) == option_field and str(option.value) in values
            ]
            if not matched:
                missing.append(f"{option_field}={raw_value!r}")
                continue
            selected.extend(matched)

        if missing:
            raise RuntimeError(
                "не удалось безопасно восстановить характеристики: " + "; ".join(missing)
            )
        log.warning(
            "%s: Playerok потребовал пересохранить характеристики. "
            "Сохраняю прежние значения%s; остальные поля товара не меняются.",
            item.id,
            (
                ", добавлено обязательное: "
                + ", ".join(f"{key}={value}" for key, value in defaults_used.items())
                if defaults_used
                else ""
            ),
        )
        return self._retry_read(
            f"Пересохранение характеристик {item.id}",
            lambda: self.account.update_item(item.id, options=selected),
        )

    def _publish_free(self, item: Any) -> Any:
        item_id = str(item.id)
        free_priority_id = self._free_priority_id(item)
        last_error: Exception | None = None
        characteristics_repaired = False
        for attempt in range(1, self.settings.retry_attempts + 1):
            try:
                published = self.account.publish_item(item_id, free_priority_id)
                priority = enum_name(getattr(published, "priority", ""))
                if priority and priority != "DEFAULT":
                    raise PremiumSafetyError(
                        f"Playerok вернул неожиданный приоритет {priority}; ожидался DEFAULT"
                    )
                return published
            except Exception as exc:
                if is_auth_error(exc) or isinstance(exc, PremiumSafetyError):
                    raise
                if (
                    self._is_missing_characteristics_error(exc)
                    and not characteristics_repaired
                ):
                    item = self._restore_required_characteristics(item)
                    characteristics_repaired = True
                    repaired_status = enum_name(getattr(item, "status", ""))
                    if repaired_status in ACTIVE_STATUSES | WAITING_STATUSES:
                        log.info(
                            "%s после пересохранения характеристик уже имеет статус %s",
                            item_id,
                            repaired_status,
                        )
                        return item
                    continue
                last_error = exc
                retryable = (
                    self._is_rate_limit_error(exc)
                    or self._is_transient_error(exc)
                    or self._is_already_published_error(exc)
                )
                if not retryable:
                    raise

                # A timed-out POST may have succeeded. Check before repeating it.
                try:
                    refreshed = self._get_item(item_id)
                except Exception as verify_error:
                    if is_auth_error(verify_error):
                        raise
                    log.warning(
                        "%s: не удалось проверить результат публикации: %s",
                        item_id,
                        verify_error,
                    )
                else:
                    refreshed_status = enum_name(getattr(refreshed, "status", ""))
                    if refreshed_status in ACTIVE_STATUSES | WAITING_STATUSES:
                        priority = enum_name(getattr(refreshed, "priority", ""))
                        if priority and priority != "DEFAULT":
                            raise PremiumSafetyError(
                                f"после публикации получен приоритет {priority}, а не DEFAULT"
                            )
                        log.warning(
                            "%s: ответ публикации потерян, но сайт уже показывает %s",
                            item_id,
                            refreshed_status,
                        )
                        return refreshed

                if attempt >= self.settings.retry_attempts:
                    break
                delay = min(
                    self.settings.retry_max_delay_seconds,
                    self.settings.retry_base_delay_seconds * (2 ** (attempt - 1)),
                )
                log.warning(
                    "%s: публикация не подтверждена, попытка %s/%s: %s. "
                    "Повтор через %.0f сек.",
                    item_id,
                    attempt,
                    self.settings.retry_attempts,
                    exc,
                    delay,
                )
                if self._wait(delay):
                    raise KeyboardInterrupt
        raise RuntimeError(
            f"не удалось перевыставить после {self.settings.retry_attempts} попыток: "
            f"{last_error}"
        )

    def _cooldown_active(self, item_id: str) -> bool:
        saved = self.state.item(item_id)
        last_attempt = float(saved.get("last_publish_attempt_unix", 0) or 0)
        if time.time() - last_attempt < self.settings.republish_cooldown_seconds:
            return True
        last_failure = float(saved.get("last_failure_unix", 0) or 0)
        # The current version can repair this former terminal error, so an old
        # state.json must not delay the first run after upgrading.
        last_error = str(saved.get("last_error", "") or "")
        if last_error and self._is_missing_characteristics_error(RuntimeError(last_error)):
            return False
        return (
            time.time() - last_failure
            < self.settings.republish_failure_cooldown_seconds
        )

    def process_item(self, item_id: str, *, initial: bool = False) -> str:
        tracked_reference = item_id
        item = self._get_item(tracked_reference)
        if item is None:
            raise RuntimeError("Playerok не вернул товар")
        self._validate_ownership(item)

        actual_item_id = str(getattr(item, "id", "") or "")
        if not actual_item_id:
            raise RuntimeError("Playerok не вернул настоящий ID товара")

        status = enum_name(getattr(item, "status", ""))
        if not status:
            # Playerok currently returns null/unknown `status` after the seller
            # manually removes an item from sale. `mayBePublished=true` is the
            # explicit server-side permission to publish that item again. Never
            # infer this for deleted or non-publishable objects.
            may_be_published = getattr(item, "may_be_published", None)
            deleted_at = getattr(item, "deleted_at", None)
            if may_be_published is True and not deleted_at:
                status = "UNPUBLISHED"
            else:
                status = "UNKNOWN"
        priority = enum_name(getattr(item, "priority", "")) or "UNKNOWN"
        name = str(getattr(item, "name", "") or "без названия")
        saved = self.state.item(tracked_reference)
        status_changed = saved.get("last_status") != status
        priority_changed = saved.get("last_priority") != priority
        if initial or status_changed or priority_changed:
            log.info(
                "Товар %s | %s | статус=%s | приоритет=%s | цена=%s ₽",
                actual_item_id,
                name,
                status,
                priority,
                getattr(item, "price", "?"),
            )
        self.state.update(
            tracked_reference,
            resolved_item_id=actual_item_id,
            name=name,
            last_status=status,
            last_priority=priority,
            last_error=None,
        )

        if status in ACTIVE_STATUSES:
            if priority == "PREMIUM" and (initial or priority_changed):
                log.warning(
                    "%s сейчас активен с Premium. Бот его не изменяет; после продажи "
                    "перевыставление будет только бесплатным DEFAULT.",
                    actual_item_id,
                )
            return "active"
        if status in WAITING_STATUSES:
            return "waiting"
        if status in STOPPED_STATUSES:
            if initial or status_changed:
                log.error(
                    "%s имеет статус %s. Автопубликация запрещена; проверьте товар вручную.",
                    actual_item_id,
                    status,
                )
            return "stopped"
        if status not in REPUBLISHABLE_STATUSES:
            raise RuntimeError(f"неизвестный/неподдерживаемый статус {status}")
        if (
            status == "DISCONTINUED"
            and "UNKNOWN" in str(saved.get("last_error", "") or "").upper()
        ):
            # Older versions could not parse Playerok's DISCONTINUED value and
            # stored an UNKNOWN failure cooldown. It must not delay the first
            # valid republish after upgrading.
            self.state.update(
                tracked_reference,
                last_failure_unix=None,
                last_error=None,
            )
        if self._cooldown_active(tracked_reference):
            return "cooldown"

        log.warning(
            "%s больше не активен (%s). %s",
            actual_item_id,
            status,
            "DRY-RUN: требовалось бы бесплатное перевыставление."
            if self.dry_run
            else "Запускаю бесплатное перевыставление.",
        )
        if self.dry_run:
            return "would_republish"

        self.state.update(
            tracked_reference,
            resolved_item_id=actual_item_id,
            last_publish_attempt_unix=time.time(),
            last_action="publishing",
        )
        published = self._publish_free(item)
        new_status = enum_name(getattr(published, "status", "")) or "UNKNOWN"
        new_priority = enum_name(getattr(published, "priority", "")) or "UNKNOWN"
        self.state.update(
            tracked_reference,
            resolved_item_id=actual_item_id,
            last_status=new_status,
            last_priority=new_priority,
            last_action="republished_free",
            last_publish_success_at=utc_now(),
            last_failure_unix=None,
            last_error=None,
        )
        log.info(
            "✅ %s бесплатно перевыставлен | статус=%s | приоритет=%s",
            actual_item_id,
            new_status,
            new_priority,
        )
        return "republished"

    def run_cycle(self, *, initial: bool = False) -> CycleSummary:
        try:
            item_ids = load_item_ids(self.items_path)
        except ConfigurationError as exc:
            if not self._known_ids:
                raise
            log.error(
                "Не удалось перечитать items.txt: %s. Продолжаю со старым списком из %s ID.",
                exc,
                len(self._known_ids),
            )
            item_ids = self._known_ids
        if item_ids != self._known_ids:
            added = [item_id for item_id in item_ids if item_id not in self._known_ids]
            removed = [item_id for item_id in self._known_ids if item_id not in item_ids]
            if added:
                log.info("Добавлены в отслеживание: %s", ", ".join(added))
            if removed:
                log.info("Убраны из отслеживания: %s", ", ".join(removed))
            self._known_ids = item_ids

        summary = CycleSummary()
        for index, item_id in enumerate(item_ids):
            if self.stop_event.is_set():
                break
            summary.checked += 1
            try:
                result = self.process_item(item_id, initial=initial)
            except Exception as exc:
                if is_auth_error(exc):
                    raise
                summary.failed += 1
                previous_error = str(self.state.item(item_id).get("last_error", ""))
                self.state.update(
                    item_id,
                    last_error=str(exc),
                    last_action="error",
                    last_failure_unix=time.time(),
                )
                if previous_error != str(exc):
                    log.exception("❌ %s: %s", item_id, exc)
                else:
                    log.debug("%s: повторяется ошибка: %s", item_id, exc)
            else:
                if result == "active":
                    summary.active += 1
                elif result == "would_republish":
                    summary.would_republish += 1
                elif result == "waiting":
                    summary.waiting += 1
                elif result == "republished":
                    summary.republished += 1
                elif result == "cooldown":
                    summary.cooldown += 1
                elif result == "stopped":
                    summary.stopped += 1
            if index + 1 < len(item_ids) and self.settings.item_request_delay_seconds:
                if self._wait(self.settings.item_request_delay_seconds):
                    break
        return summary

    def _reload_items_and_check_added(self) -> None:
        """Hot-reload items.txt without rechecking items that were already tracked."""
        try:
            item_ids = load_item_ids(self.items_path)
        except ConfigurationError as exc:
            log.error(
                "Не удалось перечитать items.txt: %s. Оставляю прежний список.", exc
            )
            return

        previous = self._known_ids
        if item_ids == previous:
            return
        added = [item_id for item_id in item_ids if item_id not in previous]
        removed = [item_id for item_id in previous if item_id not in item_ids]
        self._known_ids = item_ids
        if added:
            log.info("Добавлены в отслеживание: %s", ", ".join(added))
        if removed:
            log.info("Убраны из отслеживания: %s", ", ".join(removed))

        # Only newly added entries need an immediate lookup. Existing entries are
        # deliberately left alone until a sale event or the safety reconciliation.
        for index, item_id in enumerate(added):
            if self.stop_event.is_set():
                break
            try:
                self.process_item(item_id, initial=True)
            except Exception as exc:
                if is_auth_error(exc):
                    raise
                self.state.update(
                    item_id,
                    last_error=str(exc),
                    last_action="error",
                    last_failure_unix=time.time(),
                )
                log.exception("❌ Новый товар %s: %s", item_id, exc)
            if index + 1 < len(added) and self.settings.item_request_delay_seconds:
                if self._wait(self.settings.item_request_delay_seconds):
                    break

    def _tracked_reference_for_item(
        self, actual_item_id: str, item_slug: str = ""
    ) -> str | None:
        actual = actual_item_id.casefold()
        slug = item_slug.casefold()
        for reference in self._known_ids:
            if reference.casefold() in {actual, slug}:
                return reference
            resolved = str(
                self.state.item(reference).get("resolved_item_id", "") or ""
            ).casefold()
            if resolved and resolved == actual:
                return reference
        return None

    def _remember_deal(self, deal_id: str) -> bool:
        """Return False for a duplicate event while keeping memory bounded."""
        if deal_id in self._processed_deal_ids:
            return False
        if len(self._processed_deal_order) == self._processed_deal_order.maxlen:
            oldest = self._processed_deal_order.popleft()
            self._processed_deal_ids.discard(oldest)
        self._processed_deal_order.append(deal_id)
        self._processed_deal_ids.add(deal_id)
        return True

    @staticmethod
    def _is_new_deal_event(event: Any) -> bool:
        event_type = enum_name(getattr(event, "type", ""))
        return event.__class__.__name__ == "NewDealEvent" or event_type == "NEW_DEAL"

    def handle_event(self, event: Any) -> bool:
        """Handle one Playerok event and touch only its tracked item."""
        if not self._is_new_deal_event(event):
            return False
        deal = getattr(event, "deal", None)
        deal_id = str(getattr(deal, "id", "") or "")
        deal_item = getattr(deal, "item", None)
        actual_item_id = str(getattr(deal_item, "id", "") or "")
        item_slug = str(getattr(deal_item, "slug", "") or "")
        if not deal_id or not actual_item_id:
            log.warning(
                "Событие новой сделки не содержит deal/item ID; его подхватит "
                "страховочная сверка."
            )
            return False

        reference = self._tracked_reference_for_item(actual_item_id, item_slug)
        if reference is None:
            log.debug(
                "Новая сделка %s относится к неотслеживаемому товару %s — пропуск.",
                deal_id,
                actual_item_id,
            )
            return False
        if not self._remember_deal(deal_id):
            log.debug("Повтор события сделки %s безопасно пропущен.", deal_id)
            return True

        log.info(
            "🛒 Новая сделка %s по товару %s. Проверяю только этот ID.",
            deal_id,
            actual_item_id,
        )
        attempts = self.settings.event_status_retry_attempts
        for attempt in range(1, attempts + 1):
            try:
                result = self.process_item(reference)
            except Exception as exc:
                if is_auth_error(exc):
                    raise
                self.state.update(
                    reference,
                    last_error=str(exc),
                    last_action="error",
                    last_failure_unix=time.time(),
                )
                log.exception(
                    "❌ Сделка %s, товар %s: %s", deal_id, actual_item_id, exc
                )
                return True

            # Playerok occasionally emits the payment event a moment before the
            # item endpoint changes APPROVED -> SOLD. Retry this one item only.
            if result == "active" and attempt < attempts:
                log.info(
                    "%s ещё отображается активным после сделки %s; повтор %s/%s "
                    "через %.0f сек.",
                    actual_item_id,
                    deal_id,
                    attempt + 1,
                    attempts,
                    self.settings.event_status_retry_delay_seconds,
                )
                if self._wait(self.settings.event_status_retry_delay_seconds):
                    return True
                continue
            if result == "active":
                log.warning(
                    "%s после сделки %s всё ещё APPROVED. Оставляю его до "
                    "страховочной сверки, чтобы не спамить API.",
                    actual_item_id,
                    deal_id,
                )
            return True
        return True

    def _event_worker(self, output: "queue.Queue[Any]") -> None:
        """Listen to WebSocket events without the library's periodic deal polling."""
        from playerokapi.listener.listener import EventListener

        while not self.stop_event.is_set():
            try:
                listener = EventListener(self.account)

                # The bundled library normally starts listen_new_deals(), which
                # calls get_chats every ~15 seconds even when nothing happened.
                # NewDealEvent is already emitted by the WebSocket message path,
                # so replace that polling generator with an idle one. A rare full
                # reconciliation below is the fallback for a missed WS event.
                def no_periodic_deal_polling() -> Iterable[Any]:
                    while not self.stop_event.wait(60):
                        if False:  # keep this function a generator
                            yield None

                listener.listen_new_deals = no_periodic_deal_polling

                original_messages = listener.listen_new_messages

                def resilient_messages() -> Iterable[Any]:
                    delay = self.settings.event_reconnect_delay_seconds
                    while not self.stop_event.is_set():
                        try:
                            yield from original_messages()
                            if self.stop_event.is_set():
                                return
                            raise RuntimeError("поток WebSocket завершился")
                        except Exception as exc:
                            if self.stop_event.is_set():
                                return
                            log.warning(
                                "WebSocket Playerok потерял соединение (%s). "
                                "Переподключение через %.0f сек.",
                                exc,
                                delay,
                            )
                            if self._wait(delay):
                                return

                listener.listen_new_messages = resilient_messages
                log.info(
                    "Слушатель продаж запущен: проверки товаров идут только по "
                    "событию новой сделки."
                )
                for event in listener.listen(get_new_review_events=False):
                    if self.stop_event.is_set():
                        return
                    if self._is_new_deal_event(event):
                        output.put(event)
            except Exception as exc:
                if self.stop_event.is_set():
                    return
                log.exception(
                    "Слушатель Playerok остановился: %s. Перезапуск через %.0f сек.",
                    exc,
                    self.settings.event_reconnect_delay_seconds,
                )
                if self._wait(self.settings.event_reconnect_delay_seconds):
                    return

    def run_forever(self) -> None:
        summary = self.run_cycle(initial=True)
        log.info(
            "Стартовая проверка завершена: проверено=%s, активно=%s, "
            "ожидают=%s, перевыставлено=%s, ошибок=%s",
            summary.checked,
            summary.active,
            summary.waiting,
            summary.republished,
            summary.failed,
        )

        events: queue.Queue[Any] = queue.Queue()
        threading.Thread(
            target=self._event_worker,
            args=(events,),
            name="playerok-sale-listener",
            daemon=True,
        ).start()

        now = time.monotonic()
        next_reconciliation = now + self.settings.reconciliation_interval_seconds
        next_items_reload = now + self.settings.items_reload_interval_seconds
        self._last_heartbeat = now
        while not self.stop_event.is_set():
            now = time.monotonic()
            timeout = max(
                0.1,
                min(next_reconciliation, next_items_reload, self._last_heartbeat + self.settings.heartbeat_interval_seconds) - now,
            )
            try:
                event = events.get(timeout=min(timeout, 5.0))
            except queue.Empty:
                event = None
            if event is not None:
                self.handle_event(event)

            now = time.monotonic()
            if now >= next_items_reload:
                self._reload_items_and_check_added()
                next_items_reload = now + self.settings.items_reload_interval_seconds

            if now >= next_reconciliation:
                log.info(
                    "Страховочная сверка: проверяю весь список после %.0f минут.",
                    self.settings.reconciliation_interval_seconds / 60,
                )
                summary = self.run_cycle()
                if summary.republished or summary.failed:
                    log.info(
                        "Сверка: проверено=%s, перевыставлено=%s, ошибок=%s",
                        summary.checked,
                        summary.republished,
                        summary.failed,
                    )
                next_reconciliation = time.monotonic() + self.settings.reconciliation_interval_seconds

            if time.monotonic() - self._last_heartbeat >= self.settings.heartbeat_interval_seconds:
                log.info(
                    "Работаю: отслеживается=%s; жду события новых сделок. "
                    "Следующая страховочная сверка не чаще чем раз в %.0f минут.",
                    len(self._known_ids),
                    self.settings.reconciliation_interval_seconds / 60,
                )
                self._last_heartbeat = time.monotonic()


class SingleInstance:
    def __init__(self, path: Path):
        self.path = path
        self.acquired = False

    @staticmethod
    def _pid_is_alive(pid: int) -> bool:
        if pid <= 0:
            return False
        if os.name == "nt":
            # os.kill(pid, 0) is not a reliable existence check on Windows and
            # can raise SystemError/WinError 87 for an already exited process.
            import ctypes
            from ctypes import wintypes

            process_query_limited_information = 0x1000
            error_access_denied = 5
            kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
            open_process = kernel32.OpenProcess
            open_process.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
            open_process.restype = wintypes.HANDLE
            close_handle = kernel32.CloseHandle
            close_handle.argtypes = [wintypes.HANDLE]
            close_handle.restype = wintypes.BOOL

            handle = open_process(process_query_limited_information, False, pid)
            if handle:
                close_handle(handle)
                return True
            return ctypes.get_last_error() == error_access_denied
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return False
        except PermissionError:
            return True
        except OSError:
            return False
        return True

    def __enter__(self) -> "SingleInstance":
        try:
            descriptor = os.open(self.path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError:
            try:
                existing_pid = int(self.path.read_text(encoding="ascii").strip())
            except (OSError, ValueError):
                existing_pid = 0
            if self._pid_is_alive(existing_pid):
                raise RuntimeError(
                    f"Скрипт уже запущен (PID {existing_pid}). Второй экземпляр остановлен."
                )
            self.path.unlink(missing_ok=True)
            descriptor = os.open(self.path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        with os.fdopen(descriptor, "w", encoding="ascii") as file:
            file.write(str(os.getpid()))
        self.acquired = True
        return self

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        if self.acquired:
            self.path.unlink(missing_ok=True)

