from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from republisher import (
    ConfigurationError,
    OwnershipError,
    PremiumSafetyError,
    Republisher,
    Settings,
    StateStore,
    load_item_ids,
    normalize_item_reference,
)


def named(name: str) -> SimpleNamespace:
    return SimpleNamespace(name=name)


def item(
    item_id: str,
    *,
    status: str,
    priority: str = "DEFAULT",
    owner_id: str = "seller",
    attributes: dict[str, str] | None = None,
    may_be_published: bool | None = None,
    deleted_at: str | None = None,
) -> SimpleNamespace:
    return SimpleNamespace(
        id=item_id,
        name="Тестовый товар",
        status=named(status),
        priority=named(priority),
        price=100,
        user=SimpleNamespace(id=owner_id),
        category=SimpleNamespace(id="category"),
        attributes=attributes if attributes is not None else {"amount": "120"},
        may_be_published=may_be_published,
        deleted_at=deleted_at,
    )


class FakeAccount:
    id = "seller"

    def __init__(self, current: SimpleNamespace):
        self.current = current
        self.get_calls = 0
        self.priority_statuses = [
            SimpleNamespace(id="free-default", type=named("DEFAULT"), price=0)
        ]
        self.publish_calls: list[tuple[str, str]] = []
        self.priority_calls = 0

    def get_item(self, id: str | None = None, slug: str | None = None) -> SimpleNamespace:
        self.get_calls += 1
        self.last_lookup = (id, slug)
        return self.current

    def get_item_priority_statuses(self, item_id: str, item_price: int):
        self.priority_calls += 1
        return self.priority_statuses

    def publish_item(self, item_id: str, priority_status_id: str):
        self.publish_calls.append((item_id, priority_status_id))
        self.current = item(
            item_id, status="PENDING_APPROVAL", priority="DEFAULT"
        )
        return self.current


class RepublisherTests(unittest.TestCase):
    def make_manager(self, account: FakeAccount, root: Path, **kwargs) -> Republisher:
        item_id = account.current.id
        (root / "items.txt").write_text(item_id + "\n", encoding="utf-8")
        state = StateStore(root / "state.json")
        state.bind_account(account.id)
        settings = Settings(
            poll_interval_seconds=1,
            item_request_delay_seconds=0,
            retry_attempts=3,
            retry_base_delay_seconds=1,
            retry_max_delay_seconds=1,
            republish_cooldown_seconds=0,
        )
        manager = Republisher(
            account,
            settings,
            state,
            root / "items.txt",
            **kwargs,
        )
        manager._wait = lambda seconds: False
        return manager

    def test_item_file_deduplicates_ids_and_supports_comments(self):
        item_id = "1f1b2592-43bf-6a60-de04-b2be340b9620"
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "items.txt"
            path.write_text(
                f"# comment\n{item_id}\n{item_id.upper()} # duplicate\n",
                encoding="utf-8",
            )
            self.assertEqual(load_item_ids(path), (item_id,))

    def test_playerok_url_and_slug_are_accepted(self):
        slug = "e2986a399499-avtovydacha-540-robuksov-na-akkaunt-bez-vhoda"
        self.assertEqual(normalize_item_reference(slug), slug)
        self.assertEqual(
            normalize_item_reference(f"https://playerok.com/products/{slug}?from=test"),
            slug,
        )
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "items.txt"
            path.write_text(
                f"{slug}\nhttps://playerok.com/products/{slug}\n",
                encoding="utf-8",
            )
            self.assertEqual(load_item_ids(path), (slug,))

    def test_slug_is_looked_up_through_slug_argument(self):
        item_id = "1f1b2592-43bf-6a60-de04-b2be340b9620"
        slug = "e2986a399499-avtovydacha-540-robuksov-na-akkaunt-bez-vhoda"
        with tempfile.TemporaryDirectory() as temp:
            account = FakeAccount(item(item_id, status="APPROVED"))
            manager = self.make_manager(account, Path(temp))
            self.assertEqual(manager.process_item(slug, initial=True), "active")
            self.assertEqual(account.last_lookup, (None, slug))
            self.assertEqual(manager.state.item(slug)["resolved_item_id"], item_id)

    def test_invalid_item_id_has_line_number(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "items.txt"
            path.write_text("not-an-id\n", encoding="utf-8")
            with self.assertRaisesRegex(ConfigurationError, "строка 1"):
                load_item_ids(path)

    def test_active_item_is_not_modified(self):
        item_id = "1f1b2592-43bf-6a60-de04-b2be340b9620"
        with tempfile.TemporaryDirectory() as temp:
            account = FakeAccount(item(item_id, status="APPROVED"))
            manager = self.make_manager(account, Path(temp))
            self.assertEqual(manager.process_item(item_id, initial=True), "active")
            self.assertEqual(account.priority_calls, 0)
            self.assertEqual(account.publish_calls, [])

    def test_sold_item_is_republished_with_free_default_only(self):
        item_id = "1f1b2592-43bf-6a60-de04-b2be340b9620"
        with tempfile.TemporaryDirectory() as temp:
            account = FakeAccount(item(item_id, status="SOLD", priority="PREMIUM"))
            manager = self.make_manager(account, Path(temp))
            self.assertEqual(manager.process_item(item_id, initial=True), "republished")
            self.assertEqual(account.publish_calls, [(item_id, "free-default")])
            saved = manager.state.item(item_id)
            self.assertEqual(saved["last_action"], "republished_free")
            self.assertEqual(saved["last_priority"], "DEFAULT")

    def test_premium_only_offer_fails_closed(self):
        item_id = "1f1b2592-43bf-6a60-de04-b2be340b9620"
        with tempfile.TemporaryDirectory() as temp:
            account = FakeAccount(item(item_id, status="SOLD"))
            account.priority_statuses = [
                SimpleNamespace(id="premium", type=named("PREMIUM"), price=99)
            ]
            manager = self.make_manager(account, Path(temp))
            with self.assertRaises(PremiumSafetyError):
                manager.process_item(item_id, initial=True)
            self.assertEqual(account.publish_calls, [])

    def test_declined_item_is_logged_but_never_published(self):
        item_id = "1f1b2592-43bf-6a60-de04-b2be340b9620"
        with tempfile.TemporaryDirectory() as temp:
            account = FakeAccount(item(item_id, status="DECLINED"))
            manager = self.make_manager(account, Path(temp))
            self.assertEqual(manager.process_item(item_id, initial=True), "stopped")
            self.assertEqual(account.publish_calls, [])

    def test_foreign_item_is_rejected(self):
        item_id = "1f1b2592-43bf-6a60-de04-b2be340b9620"
        with tempfile.TemporaryDirectory() as temp:
            account = FakeAccount(item(item_id, status="SOLD", owner_id="other"))
            manager = self.make_manager(account, Path(temp))
            with self.assertRaises(OwnershipError):
                manager.process_item(item_id, initial=True)
            self.assertEqual(account.publish_calls, [])

    def test_dry_run_does_not_request_priority_or_publish(self):
        item_id = "1f1b2592-43bf-6a60-de04-b2be340b9620"
        with tempfile.TemporaryDirectory() as temp:
            account = FakeAccount(item(item_id, status="EXPIRED"))
            manager = self.make_manager(account, Path(temp), dry_run=True)
            self.assertEqual(manager.process_item(item_id, initial=True), "would_republish")
            self.assertEqual(account.priority_calls, 0)
            self.assertEqual(account.publish_calls, [])

    def test_lost_publish_response_is_verified_before_retry(self):
        item_id = "1f1b2592-43bf-6a60-de04-b2be340b9620"

        class AmbiguousAccount(FakeAccount):
            def publish_item(self, item_id: str, priority_status_id: str):
                self.publish_calls.append((item_id, priority_status_id))
                self.current = item(
                    item_id, status="PENDING_APPROVAL", priority="DEFAULT"
                )
                raise RuntimeError("Client.Timeout exceeded while awaiting headers")

        with tempfile.TemporaryDirectory() as temp:
            account = AmbiguousAccount(item(item_id, status="SOLD"))
            manager = self.make_manager(account, Path(temp))
            self.assertEqual(manager.process_item(item_id, initial=True), "republished")
            self.assertEqual(len(account.publish_calls), 1)

    def test_missing_required_characteristic_is_restored_without_other_edits(self):
        item_id = "1f1b2592-43bf-6a60-de04-b2be340b9620"

        class RequiredAttributeAccount(FakeAccount):
            def __init__(self, current):
                super().__init__(current)
                self.update_calls = []

            def publish_item(self, item_id: str, priority_status_id: str):
                self.publish_calls.append((item_id, priority_status_id))
                if len(self.publish_calls) == 1:
                    raise RuntimeError(
                        "Пожалуйста, заполните все обязательные характеристики товара"
                    )
                self.current = item(
                    item_id,
                    status="PENDING_APPROVAL",
                    priority="DEFAULT",
                    attributes={"amount": "120", "region": "global"},
                )
                return self.current

            def get_game_category(self, id: str):
                return SimpleNamespace(options=[
                    SimpleNamespace(field="amount", value="120", label="120", id="amount-120"),
                    SimpleNamespace(field="region", value="ru", label="RU", id="region-ru"),
                    SimpleNamespace(
                        field="region", value="global", label="Global", id="region-global"
                    ),
                ])

            def update_item(self, item_id: str, **kwargs):
                self.update_calls.append((item_id, kwargs))
                self.current.attributes = {
                    option.field: option.value for option in kwargs["options"]
                }
                return self.current

        with tempfile.TemporaryDirectory() as temp:
            account = RequiredAttributeAccount(
                item(item_id, status="SOLD", attributes={"amount": "120"})
            )
            manager = self.make_manager(account, Path(temp))
            self.assertEqual(manager.process_item(item_id, initial=True), "republished")
            self.assertEqual(len(account.publish_calls), 2)
            self.assertEqual(len(account.update_calls), 1)
            _, kwargs = account.update_calls[0]
            self.assertEqual(set(kwargs), {"options"})
            selected = {option.field: option.value for option in kwargs["options"]}
            self.assertEqual(selected, {"amount": "120", "region": "global"})

    def test_new_deal_event_checks_only_matching_tracked_item(self):
        item_id = "1f1b2592-43bf-6a60-de04-b2be340b9620"
        with tempfile.TemporaryDirectory() as temp:
            account = FakeAccount(item(item_id, status="SOLD"))
            manager = self.make_manager(account, Path(temp))
            manager._known_ids = (item_id,)
            event = SimpleNamespace(
                type=named("NEW_DEAL"),
                deal=SimpleNamespace(
                    id="deal-1",
                    item=SimpleNamespace(id=item_id, slug=""),
                ),
            )

            self.assertTrue(manager.handle_event(event))
            self.assertEqual(account.get_calls, 1)
            self.assertEqual(account.publish_calls, [(item_id, "free-default")])

            # The listener can expose NEW_DEAL and ITEM_PAID for the same deal.
            # A duplicate must not query the item again.
            self.assertTrue(manager.handle_event(event))
            self.assertEqual(account.get_calls, 1)

    def test_new_deal_for_untracked_item_makes_no_item_request(self):
        tracked_id = "1f1b2592-43bf-6a60-de04-b2be340b9620"
        other_id = "2f1b2592-43bf-6a60-de04-b2be340b9620"
        with tempfile.TemporaryDirectory() as temp:
            account = FakeAccount(item(tracked_id, status="APPROVED"))
            manager = self.make_manager(account, Path(temp))
            manager._known_ids = (tracked_id,)
            event = SimpleNamespace(
                type=named("NEW_DEAL"),
                deal=SimpleNamespace(
                    id="deal-other",
                    item=SimpleNamespace(id=other_id, slug=""),
                ),
            )

            self.assertFalse(manager.handle_event(event))
            self.assertEqual(account.get_calls, 0)

    def test_manually_discontinued_item_is_republished(self):
        item_id = "1f1b2592-43bf-6a60-de04-b2be340b9620"
        with tempfile.TemporaryDirectory() as temp:
            account = FakeAccount(
                item(
                    item_id,
                    status="DISCONTINUED",
                    may_be_published=False,
                )
            )
            manager = self.make_manager(account, Path(temp))
            manager.state.update(
                item_id,
                last_error="неизвестный/неподдерживаемый статус UNKNOWN",
                last_failure_unix=999999999999,
            )

            self.assertEqual(manager.process_item(item_id, initial=True), "republished")
            self.assertEqual(account.publish_calls, [(item_id, "free-default")])
            self.assertEqual(manager.state.item(item_id)["last_action"], "republished_free")

    def test_empty_status_with_explicit_publish_permission_is_republished(self):
        item_id = "1f1b2592-43bf-6a60-de04-b2be340b9620"
        with tempfile.TemporaryDirectory() as temp:
            account = FakeAccount(
                item(item_id, status="", may_be_published=True)
            )
            manager = self.make_manager(account, Path(temp))

            self.assertEqual(manager.process_item(item_id, initial=True), "republished")
            self.assertEqual(account.publish_calls, [(item_id, "free-default")])

    def test_unknown_status_without_publish_permission_fails_closed(self):
        item_id = "1f1b2592-43bf-6a60-de04-b2be340b9620"
        with tempfile.TemporaryDirectory() as temp:
            account = FakeAccount(
                item(
                    item_id,
                    status="",
                    may_be_published=False,
                )
            )
            manager = self.make_manager(account, Path(temp))

            with self.assertRaisesRegex(RuntimeError, "UNKNOWN"):
                manager.process_item(item_id, initial=True)
            self.assertEqual(account.publish_calls, [])


if __name__ == "__main__":
    unittest.main()

