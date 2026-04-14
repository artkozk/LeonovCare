from __future__ import annotations

import html
import json
import logging
import os
import re
import sqlite3
from datetime import datetime, timedelta

from aiogram.types import FSInputFile
from telebot import TeleBot
from telebot.types import CallbackQuery, Message, InlineKeyboardMarkup, InlineKeyboardButton

from app.db import (
    repo_admins,
    repo_checkout_payments,
    repo_states,
    repo_students,
    repo_requests,
    repo_settings,
    repo_postpay,
    repo_student_codes,
    repo_analytics,
    repo_payments,
    repo_enrollment_contracts,
    repo_autoapply_runs,
)
from app.services.autoapply_queries import get_core_query
from app.services.autoapply_client import AutoApplyClient, AutoApplyClientError
from app.services import admin_notification_settings
from app.services.okidoki_client import OkiDokiClient, OkiDokiClientError
from app.services.postpay import next_due_after_payment, post_total, recommended_payment
from app.services.timeutils import now, format_date_ddmmyyyy, iso
from app.services.cardlink_client import CardlinkClient, CardlinkClientError, normalize_status as cardlink_status
from app.ui import texts
from app.ui.keyboards import (
    regular_menu_kb,
    user_menu_kb,
    user_profile_start_mode_kb,
    user_direction_kb,
    user_stage_kb,
    user_tariff_kb,
    user_prepay_kb,
    user_post_total_kb,
    user_post_monthly_kb,
    request_admin_kb,
    request_open_kb,
)


STATE_PROFILE = "u_profile"
STATE_ACCOUNTS = "u_accounts"
STATE_PAYMENT = "u_payment"
STATE_ENROLL_PAYMENT = "u_enroll_payment"
STATE_STUDENT_CODE = "u_student_code"
STATE_AUTOAPPLY = "u_autoapply"
STATE_ENROLL = "u_enroll"

AUTO_DIRECTIONS = ("java", "golang", "frontend", "python")
AUTOAPPLY_FREE50_KEY_PREFIX = "autoapply.free50.used:"
AUTOAPPLY_PAID_CLICKS_KEY_PREFIX = "autoapply.paid_clicks:"

log = logging.getLogger(__name__)

ENROLL_TRACKS: dict[str, dict] = {
    "zero-offer": {
        "alias": "zero",
        "title": "С нуля до оффера",
        "level": "zero",
        "plans": [
            {
                "key": "a100",
                "title": "Сопровождение до работы",
                "subtitle": "С нуля до оффера",
                "price": "60 000 ₽ + 100% от оффера / 30 000 ₽ + 150% от оффера",
            },
            {
                "key": "month",
                "title": "Сопровождение 1 месяц",
                "subtitle": "Фиксированный тариф",
                "price": "27 900 ₽",
            },
            {
                "key": "diag",
                "title": "Диагностика и roadmap",
                "subtitle": "Разовый формат",
                "price": "8 900 ₽",
            },
        ],
    },
    "interview-prep": {
        "alias": "prep",
        "title": "После курсов до оффера",
        "level": "not_zero",
        "plans": [
            {
                "key": "a100",
                "title": "Сопровождение до работы",
                "subtitle": "После курсов до оффера",
                "price": "60 000 ₽ + 100% от оффера / 30 000 ₽ + 150% от оффера",
            },
            {
                "key": "int2w",
                "title": "Интенсив 2 недели",
                "subtitle": "Фиксированный тариф",
                "price": "16 900 ₽",
            },
            {
                "key": "mock",
                "title": "1 mock-собеседование",
                "subtitle": "Разовый формат",
                "price": "4 990 ₽",
            },
        ],
    },
    "grade-salary": {
        "alias": "grade",
        "title": "Увеличение зарплаты",
        "level": "not_zero",
        "plans": [
            {
                "key": "a100",
                "title": "Сопровождение до работы",
                "subtitle": "Рост дохода",
                "price": "60 000 ₽ + 100% от оффера / 30 000 ₽ + 150% от оффера",
            },
            {
                "key": "sprint",
                "title": "Рост-спринт 4 недели",
                "subtitle": "Фиксированный тариф",
                "price": "24 900 ₽",
            },
            {
                "key": "mock",
                "title": "1 mock-собеседование",
                "subtitle": "Разовый формат",
                "price": "4 990 ₽",
            },
            {
                "key": "audit",
                "title": "Аудит грейда",
                "subtitle": "Разовый формат",
                "price": "8 900 ₽",
            },
        ],
    },
}

ENROLL_TRACK_BY_ALIAS = {v["alias"]: k for k, v in ENROLL_TRACKS.items()}


def init(bot: TeleBot, ctx: dict) -> None:
    cfg = ctx["cfg"]
    conn: sqlite3.Connection = ctx["conn"]
    tz = cfg.TZ
    auto_client = AutoApplyClient(
        base_url=cfg.AUTOAPPLY_API_BASE_URL,
        internal_token=cfg.AUTOAPPLY_INTERNAL_TOKEN,
        timeout_sec=cfg.AUTOAPPLY_TIMEOUT_SEC,
    )
    okidoki_client = OkiDokiClient(
        create_contract_url=cfg.OKIDOKI_CREATE_CONTRACT_URL,
        api_token=cfg.OKIDOKI_API_TOKEN,
        timeout_sec=30,
    )
    cardlink_client = CardlinkClient(
        shop_id=cfg.CARDLINK_SHOP_ID,
        bearer_token=cfg.CARDLINK_BEARER_TOKEN,
        return_url=cfg.CARDLINK_RETURN_URL,
        timeout_sec=cfg.CARDLINK_TIMEOUT_SEC,
        api_base_url=cfg.CARDLINK_API_BASE_URL,
    )
    def is_admin(user_id: int) -> bool:
        return repo_admins.is_admin(conn, int(user_id), ctx["cfg"].ADMIN_IDS)

    def _parse_money(text: str) -> int:
        digits = "".join(ch for ch in (text or "") if ch.isdigit())
        return int(digits) if digits else 0

    def _is_http_url(value: str | None) -> bool:
        raw = str(value or "").strip().lower()
        return raw.startswith("http://") or raw.startswith("https://")

    def _user_show_pay_button(owner_tg_id: int) -> bool:
        s = repo_students.get_by_owner(conn, int(owner_tg_id))
        if not s or not s["offer_amount"]:
            return False
        pp = repo_postpay.get(conn, int(s["id"]))
        return bool(pp and int(pp["enabled"] or 0) == 1)

    def _has_student_access(owner_tg_id: int) -> bool:
        latest_student = repo_students.get_latest_by_owner(conn, int(owner_tg_id))
        if latest_student:
            return int(latest_student["archived"] or 0) == 0
        return repo_student_codes.has_used_code(conn, int(owner_tg_id))

    def send_user_home(chat_id: int, owner_tg_id: int) -> None:
        repo_states.clear_state(conn, owner_tg_id, tz)
        repo_analytics.touch_user(conn, tz, owner_tg_id, role_type=repo_analytics.ROLE_STUDENT)
        bot.send_message(chat_id, texts.user_title(), reply_markup=user_menu_kb(show_pay=_user_show_pay_button(owner_tg_id)))

    def send_regular_home(chat_id: int, owner_tg_id: int) -> None:
        repo_states.clear_state(conn, owner_tg_id, tz)
        if _has_student_access(owner_tg_id):
            send_user_home(chat_id, owner_tg_id)
            return
        repo_analytics.touch_user(conn, tz, owner_tg_id, role_type=repo_analytics.ROLE_REGULAR)
        bot.send_message(chat_id, texts.regular_title(), reply_markup=regular_menu_kb())

    def _home_menu_kb(owner_tg_id: int) -> InlineKeyboardMarkup:
        if _has_student_access(owner_tg_id):
            return user_menu_kb(show_pay=_user_show_pay_button(owner_tg_id))
        return regular_menu_kb()

    def _v2_back_kb() -> InlineKeyboardMarkup:
        kb = InlineKeyboardMarkup()
        kb.row(InlineKeyboardButton("⬅️ В меню", callback_data="v2:home"))
        return kb

    def _jetbrains_kb() -> InlineKeyboardMarkup:
        kb = InlineKeyboardMarkup()
        kb.row(InlineKeyboardButton("📘 Открыть гайд", url="https://t.me/c/3730365848/143/145"))
        kb.row(InlineKeyboardButton("🔑 Получить ключ", url="https://jb.js7.uz/"))
        kb.row(InlineKeyboardButton("⬅️ В меню", callback_data="v2:home"))
        return kb

    def _send_student_access_required(chat_id: int) -> None:
        kb = InlineKeyboardMarkup()
        kb.row(InlineKeyboardButton("Я ученик", callback_data="v2:student"))
        kb.row(InlineKeyboardButton("⬅️ В меню", callback_data="v2:home"))
        bot.send_message(
            chat_id,
            "Доступ к кабинету ученика закрыт.\nНажми «Я ученик» и введи одноразовый код от ментора.",
            reply_markup=kb,
        )

    def _admin_targets() -> list[int]:
        targets: set[int] = set()
        for uid in repo_admins.list_notify_ids(conn, ctx["cfg"].ADMIN_IDS):
            targets.add(int(uid))
        raw = repo_settings.get(conn, "ADMIN_CHAT_ID", None)
        if raw:
            try:
                targets.add(int(raw))
            except Exception:
                pass
        return list(targets)

    def notify_admins(text: str, kb: InlineKeyboardMarkup | None = None) -> None:
        for chat_id in _admin_targets():
            if not admin_notification_settings.pings_enabled(conn, int(chat_id)):
                continue
            try:
                bot.send_message(int(chat_id), text, reply_markup=kb)
            except Exception as exc:
                err_text = f"{type(exc).__name__}: {exc}"
                log.warning("notify_admins failed chat_id=%s: %s", chat_id, err_text)
                try:
                    repo_analytics.log_event(
                        conn,
                        tz,
                        0,
                        "ops.notify_admin_error",
                        {
                            "chat_id": int(chat_id),
                            "error": err_text[:300],
                            "text_preview": (text or "")[:200],
                        },
                    )
                except Exception:
                    # keep notifier non-blocking
                    pass

    def _auto_unavailable_reason() -> str:
        reason = str(cfg.AUTOAPPLY_TEMP_UNAVAILABLE_TEXT or "").strip()
        return reason or "Временно недоступно"

    def _is_auto_temporarily_unavailable() -> bool:
        return bool(cfg.AUTOAPPLY_TEMP_UNAVAILABLE)

    def _auto_temporarily_unavailable_text() -> str:
        return (
            "<b>Автоотклики</b>\n"
            f"{html.escape(_auto_unavailable_reason())}\n"
            "Попробуй позже или напиши ментору."
        )

    def _digits_only(value: str | None) -> str:
        return "".join(ch for ch in str(value or "") if ch.isdigit())

    def _parse_date_ru(text: str | None):
        raw = (text or "").strip()
        if not raw:
            return None
        try:
            return datetime.strptime(raw, "%d.%m.%Y").date()
        except Exception:
            return None

    def _format_money_value(value: int | None) -> str:
        try:
            n = int(value or 0)
        except Exception:
            n = 0
        return f"{n:,}".replace(",", " ")

    def _auto_free50_key(owner_tg_id: int) -> str:
        return f"{AUTOAPPLY_FREE50_KEY_PREFIX}{int(owner_tg_id)}"

    def _auto_free50_used_count(owner_tg_id: int) -> int:
        marker = str(repo_settings.get(conn, _auto_free50_key(owner_tg_id), None) or "").strip()
        if not marker:
            return 0

        tail = marker.rsplit("|", 1)[-1] if "|" in marker else marker
        digits = "".join(ch for ch in tail if ch.isdigit())
        if digits:
            try:
                used = int(digits)
                return max(0, min(used, 50))
            except Exception:
                pass
        # Legacy marker without numeric payload -> treat as fully used.
        return 50

    def _auto_free50_remaining(owner_tg_id: int) -> int:
        return max(50 - _auto_free50_used_count(owner_tg_id), 0)

    def _auto_free50_already_used(owner_tg_id: int) -> bool:
        return _auto_free50_remaining(owner_tg_id) <= 0

    def _auto_mark_free50_used(owner_tg_id: int, free_clicks_granted: int) -> None:
        granted = max(int(free_clicks_granted or 0), 0)
        if granted <= 0:
            return
        used_total = min(_auto_free50_used_count(owner_tg_id) + granted, 50)
        marker = f"{format_date_ddmmyyyy(now(tz).date())}|{used_total}"
        repo_settings.set(conn, _auto_free50_key(owner_tg_id), marker)

    def _auto_price_breakdown(owner_tg_id: int, raw_clicks: int) -> dict:
        clicks = max(int(raw_clicks or 0), 0)
        per_click = 7
        if 200 <= clicks < 500:
            per_click = 6
        elif clicks >= 500:
            per_click = 5

        free_remaining = _auto_free50_remaining(owner_tg_id)
        free_available = free_remaining > 0
        free_clicks = 0
        if free_available and clicks < 200:
            free_clicks = min(clicks, free_remaining)

        paid_clicks = max(clicks - free_clicks, 0)
        total_price = int(paid_clicks * per_click)
        return {
            "clicks": clicks,
            "per_click": per_click,
            "free_clicks": free_clicks,
            "free_remaining": free_remaining,
            "free_used": 50 - free_remaining,
            "paid_clicks": paid_clicks,
            "total_price": total_price,
            "free_available": free_available,
        }

    def _auto_price_for_user(owner_tg_id: int, raw_clicks: int) -> int:
        return int(_auto_price_breakdown(owner_tg_id, raw_clicks).get("total_price") or 0)

    def _payment_due_context(owner_tg_id: int) -> dict:
        out = {
            "has_student": False,
            "student_id": None,
            "offer_amount": 0,
            "postpay_enabled": False,
            "schedule_type": "",
            "per_period_value": None,
            "total_postpay": None,
            "paid_postpay": 0,
            "remaining_postpay": None,
            "recommended_amount": None,
            "invoice_amount": 0,
            "closed": False,
        }
        s = repo_students.get_by_owner(conn, int(owner_tg_id))
        if not s:
            return out
        out["has_student"] = True
        sid = int(s["id"])
        out["student_id"] = sid
        offer_amount = int(s["offer_amount"] or 0)
        out["offer_amount"] = offer_amount

        pp = repo_postpay.get(conn, sid)
        if not pp:
            return out
        out["postpay_enabled"] = int(pp["enabled"] or 0) == 1
        out["schedule_type"] = str(pp["schedule_type"] or "").strip()
        out["per_period_value"] = pp["per_period_value"]

        total_postpay = None
        try:
            if offer_amount > 0 and pp["total_percent"]:
                total_postpay = int(round(float(offer_amount) * float(pp["total_percent"]) / 100))
        except Exception:
            total_postpay = None
        out["total_postpay"] = total_postpay

        paid_postpay = repo_payments.sum_payments(conn, sid, "postpay")
        out["paid_postpay"] = int(paid_postpay or 0)

        remaining_postpay = None
        if total_postpay is not None:
            remaining_postpay = max(int(total_postpay) - int(out["paid_postpay"]), 0)
        out["remaining_postpay"] = remaining_postpay

        rec = None
        try:
            rec = recommended_payment(
                offer_amount if offer_amount > 0 else None,
                out["schedule_type"],
                out["per_period_value"],
            )
        except Exception:
            rec = None
        if rec is not None and rec > 0:
            out["recommended_amount"] = int(rec)

        invoice_amount = 0
        if remaining_postpay is not None and remaining_postpay > 0:
            if out["recommended_amount"] and out["recommended_amount"] > 0:
                invoice_amount = min(int(out["recommended_amount"]), int(remaining_postpay))
            else:
                invoice_amount = int(remaining_postpay)
        out["invoice_amount"] = max(int(invoice_amount or 0), 0)
        out["closed"] = remaining_postpay is not None and int(remaining_postpay) <= 0
        return out

    def _payment_invoice_text(amount: int, purpose: str, due: dict | None = None) -> str:
        due = due or {}
        expected_inn = _digits_only(cfg.PAYMENT_EXPECTED_INN)
        receiver = str(cfg.PAYMENT_RECEIVER_NAME or "").strip()
        bank_name = str(cfg.PAYMENT_BANK_NAME or "").strip()
        account_number = _digits_only(cfg.PAYMENT_ACCOUNT_NUMBER)
        corr_account = _digits_only(cfg.PAYMENT_CORR_ACCOUNT)
        bik = _digits_only(cfg.PAYMENT_BIK)
        payment_link = str(cfg.PAYMENT_LINK_URL or "").strip()

        lines = ["<b>Счёт на оплату</b>"]
        if receiver:
            lines.append(f"Получатель: <b>{html.escape(receiver)}</b>")
        if expected_inn:
            lines.append(f"ИНН: <code>{expected_inn}</code>")
        if bank_name:
            lines.append(f"Банк: <b>{html.escape(bank_name)}</b>")
        if bik:
            lines.append(f"БИК: <code>{bik}</code>")
        if account_number:
            lines.append(f"Р/с: <code>{account_number}</code>")
        if corr_account:
            lines.append(f"К/с: <code>{corr_account}</code>")
        if amount > 0:
            lines.append(f"Сумма к оплате: <b>{_format_money_value(amount)} ₽</b>")
        else:
            lines.append("Сумма к оплате: <b>уточняется</b>")

        remaining = due.get("remaining_postpay")
        if remaining is not None:
            lines.append(f"Остаток постоплаты: <b>{_format_money_value(int(remaining))} ₽</b>")
        rec_amount = due.get("recommended_amount")
        if rec_amount:
            lines.append(f"Рекомендованный платеж: <b>{_format_money_value(int(rec_amount))} ₽</b>")

        lines.append(f"Назначение: <code>{html.escape(str(purpose or '').strip())}</code>")
        if payment_link:
            lines.append(f"Ссылка на оплату: {html.escape(payment_link)}")

        if not any([receiver, expected_inn, bank_name, account_number, payment_link]):
            lines.append("Реквизиты счёта не настроены. Перед оплатой уточни их у ментора.")
            if cfg.MENTOR_CONTACT_URL:
                lines.append(f"Связь с ментором: {html.escape(str(cfg.MENTOR_CONTACT_URL))}")
        return "\n".join(lines)

    def _send_manual_payment_handoff(
        owner_tg_id: int,
        owner_chat_id: int,
        owner_username: str | None,
        req_type: str,
        service_title: str,
        amount: int,
        purpose: str,
        extra_payload: dict | None = None,
    ) -> int:
        amount = max(int(amount or 0), 0)
        expected_inn = _digits_only(cfg.PAYMENT_EXPECTED_INN)
        payload = {
            "owner_chat_id": int(owner_chat_id),
            "owner_username": owner_username,
            "service_title": str(service_title or "").strip(),
            "purpose": str(purpose or "").strip(),
            "manual_handoff": True,
            "source": "mentor_handoff",
            "pay_date": format_date_ddmmyyyy(now(tz).date()),
        }
        if amount > 0:
            payload["amount"] = amount
        if expected_inn:
            payload["recipient_inn"] = expected_inn
        if isinstance(extra_payload, dict):
            payload.update(extra_payload)

        req_id = repo_requests.create(conn, tz, owner_tg_id, req_type, payload, None)
        notify_admins(
            f"<b>[ЗАЯВКА]</b> {texts.request_title(req_type)}\n"
            f"От: @{owner_username or owner_tg_id}\n"
            f"ID: {req_id}\n"
            f"Услуга: {html.escape(str(service_title or '—'))}\n"
            f"Сумма: {amount if amount > 0 else 'уточнить'}\n"
            "Оплата принимается вручную у ментора.",
            kb=request_admin_kb(req_id),
        )

        mentor_url = (cfg.MENTOR_CONTACT_URL or "").strip()
        user_lines = [
            "<b>Оплата через ментора</b>",
            f"Услуга: <b>{html.escape(str(service_title or '—'))}</b>",
            f"Сумма к оплате: <b>{_format_money_value(amount)} ₽</b>" if amount > 0 else "Сумма к оплате: <b>уточняется ментором</b>",
            "Ментор уже получил уведомление с суммой и выбранной услугой.",
            "Оплата проводится напрямую у ментора.",
        ]
        if mentor_url:
            user_lines.append(f"Контакт ментора: {html.escape(mentor_url)}")
        else:
            user_lines.append("Контакт ментора не настроен в боте. Напиши в поддержку.")
        user_lines.append("После оплаты отправь чек напрямую ментору в этот диалог.")
        bot.send_message(owner_chat_id, "\n".join(user_lines))
        return req_id

    def _payment_channel_label() -> str:
        return "через Cardlink"

    def _checkout_wait_kb(check_callback: str, payment_url: str | None, back_callback: str = "v2:home") -> InlineKeyboardMarkup:
        kb = InlineKeyboardMarkup()
        if payment_url:
            kb.row(InlineKeyboardButton("💳 Оплатить", url=payment_url))
        kb.row(InlineKeyboardButton("🔄 Проверить оплату", callback_data=check_callback))
        mentor_url = (cfg.MENTOR_CONTACT_URL or "").strip()
        if mentor_url:
            kb.row(InlineKeyboardButton("Написать ментору", url=mentor_url))
        kb.row(InlineKeyboardButton("⬅️ В меню", callback_data=back_callback))
        return kb

    def _create_checkout_payment(
        owner_tg_id: int,
        owner_chat_id: int,
        owner_username: str | None,
        req_type: str,
        service_key: str,
        service_title: str,
        amount: int,
        purpose: str,
        metadata: dict | None = None,
    ) -> tuple[sqlite3.Row | None, str]:
        invoice_amount = max(int(amount or 0), 0)
        if invoice_amount <= 0:
            return None, "Сумма оплаты не определена. Напиши ментору для уточнения."
        if not cardlink_client.enabled:
            return None, "Cardlink пока не настроена. Оплата доступна только через ментора."
        payload_meta = {
            "owner_tg_id": int(owner_tg_id),
            "owner_chat_id": int(owner_chat_id),
            "req_type": str(req_type or "").strip(),
            "service_key": str(service_key or "").strip(),
        }
        if isinstance(metadata, dict):
            payload_meta.update(metadata)
        description = f"{service_title} (TG {owner_tg_id})"
        try:
            provider = cardlink_client.create_payment(
                amount_rub=invoice_amount,
                description=description,
                metadata=payload_meta,
            )
        except CardlinkClientError as exc:
            return None, f"Не удалось создать платёж в Cardlink: {html.escape(str(exc))}"

        provider_payment_id = str(provider.get("id") or "").strip()
        if not provider_payment_id:
            return None, "Cardlink не вернула id платежа. Напиши ментору."
        status = cardlink_status(provider.get("status"))
        confirmation_url = str(cardlink_client._extract_confirmation_url(provider)).strip()
        if not confirmation_url:
            try:
                probe = cardlink_client.get_payment(provider_payment_id)
                if isinstance(probe, dict):
                    provider = {**provider, "_status_probe": probe}
                    confirmation_url = str(cardlink_client._extract_confirmation_url(probe)).strip()
            except CardlinkClientError:
                pass
        if not confirmation_url:
            return None, "Cardlink не вернула ссылку оплаты. Проверь настройки кассы и попробуй ещё раз."
        local_id = repo_checkout_payments.create(
            conn,
            tz,
            owner_tg_id=owner_tg_id,
            owner_chat_id=owner_chat_id,
            owner_username=owner_username,
            req_type=req_type,
            service_key=service_key,
            service_title=service_title,
            purpose=purpose,
            amount=invoice_amount,
            currency=str(provider.get("amount", {}).get("currency") or "RUB"),
            provider="cardlink",
            provider_payment_id=provider_payment_id,
            idempotence_key=str(provider.get("idempotence_key") or "").strip() or None,
            status=status,
            confirmation_url=confirmation_url,
            metadata=payload_meta,
            provider_payload=provider,
        )
        row = repo_checkout_payments.get(conn, local_id)
        return row, ""

    def _refresh_checkout_payment(local_payment_id: int) -> tuple[sqlite3.Row | None, str]:
        row = repo_checkout_payments.get(conn, int(local_payment_id))
        if not row:
            return None, "Платёж не найден."
        if not cardlink_client.enabled:
            return row, "Cardlink не настроена в текущем окружении."

        provider_payment_id = str(row["provider_payment_id"] or "").strip()
        if not provider_payment_id:
            return row, "Не найден provider_payment_id."
        try:
            provider = cardlink_client.get_payment(provider_payment_id)
        except CardlinkClientError as exc:
            return row, f"Не удалось обновить статус: {html.escape(str(exc))}"

        status = cardlink_status(provider.get("status"))
        confirmation_url = str(cardlink_client._extract_confirmation_url(provider) or row["confirmation_url"] or "").strip()
        paid_at = str(provider.get("paid_at") or provider.get("captured_at") or "").strip() or None
        repo_checkout_payments.set_status(
            conn,
            tz,
            int(local_payment_id),
            status=status,
            provider_payload=provider,
            confirmation_url=confirmation_url or None,
            paid_at=paid_at,
        )
        return repo_checkout_payments.get(conn, int(local_payment_id)), ""

    def _apply_postpay_checkout(owner_tg_id: int, amount: int, payment_row_id: int) -> tuple[bool, str]:
        due = _payment_due_context(owner_tg_id)
        sid = due.get("student_id")
        if not sid:
            return False, "Не найдена карточка ученика для фиксации платежа."

        invoice_amount = max(int(amount or 0), 0)
        if invoice_amount <= 0:
            return False, "Сумма платежа не определена."

        pay_date = format_date_ddmmyyyy(now(tz).date())
        repo_payments.add_payment(
            conn,
            int(sid),
            "postpay",
            invoice_amount,
            pay_date,
            tz,
            source="student",
            note=f"cardlink:{payment_row_id}",
        )

        pp = repo_postpay.get(conn, int(sid))
        if pp and int(pp["enabled"] or 0) == 1:
            new_due = next_due_after_payment(pp["next_due_date"], pay_date, pp["schedule_type"], pp["per_period_value"])
            repo_postpay.upsert(conn, int(sid), next_due_date=new_due, student_next_notify_date=new_due, enabled=1)
            s = repo_students.get_student(conn, int(sid))
            if s:
                total = post_total(s["offer_amount"], pp["total_percent"])
                if total is not None:
                    paid = repo_payments.sum_payments(conn, int(sid), "postpay")
                    if paid >= total:
                        repo_postpay.disable(conn, int(sid))
        try:
            s = repo_students.get_student(conn, int(sid))
            schat = int(s["owner_chat_id"]) if s and s["owner_chat_id"] else None
        except Exception:
            schat = None
        if schat:
            conn.execute(
                "UPDATE alerts SET status='SEEN', updated_at=? "
                "WHERE status='PENDING' AND alert_type='PAYMENT' AND related_table='students' AND related_id=? AND chat_id=?",
                (iso(now(tz)), int(sid), schat),
            )
            conn.commit()
        return True, ""

    def _apply_enrollment_checkout(owner_tg_id: int, amount: int, payment_row_id: int) -> tuple[bool, str]:
        invoice_amount = max(int(amount or 0), 0)
        contract = repo_enrollment_contracts.get_by_owner(conn, int(owner_tg_id))
        if contract:
            repo_enrollment_contracts.set_status(
                conn,
                tz,
                int(owner_tg_id),
                status="paid",
                note=f"cardlink:{payment_row_id}",
            )

        s = repo_students.get_by_owner(conn, int(owner_tg_id))
        if s and invoice_amount > 0:
            repo_payments.add_payment(
                conn,
                int(s["id"]),
                "prepay",
                invoice_amount,
                format_date_ddmmyyyy(now(tz).date()),
                tz,
                source="student",
                note=f"enrollment_cardlink:{payment_row_id}",
            )
        return True, ""

    def _receipt_media_verification(m: Message) -> tuple[str | None, dict]:
        file_id = None
        media_type = "unknown"
        mime_type = ""
        file_name = ""
        if m.photo:
            media_type = "photo"
            file_id = m.photo[-1].file_id
        elif m.document:
            media_type = "document"
            file_id = m.document.file_id
            mime_type = str(m.document.mime_type or "").strip().lower()
            file_name = str(m.document.file_name or "").strip().lower()

        caption = str(getattr(m, "caption", "") or "").strip().lower()
        hint_tokens = ("чек", "квитан", "receipt", "payment", "оплат", "сбп")
        caption_hint = any(token in caption for token in hint_tokens)
        file_name_hint = any(token in file_name for token in hint_tokens)
        format_supported = bool(m.photo) or mime_type.startswith("image/") or mime_type == "application/pdf"
        checks = {
            "file_present": bool(file_id),
            "format_supported": bool(format_supported),
            "receipt_hint_detected": bool(caption_hint or file_name_hint),
        }
        warnings: list[str] = []
        if not checks["format_supported"]:
            warnings.append("Файл не похож на чек: нужен скрин/фото или PDF.")
        if not checks["receipt_hint_detected"]:
            warnings.append("Не найден явный признак чека (добавь подпись «чек»/«квитанция»/«receipt»).")

        return file_id, {
            "ok": bool(checks["file_present"] and checks["format_supported"] and checks["receipt_hint_detected"]),
            "checks": checks,
            "warnings": warnings,
            "media_type": media_type,
            "mime_type": mime_type,
            "file_name": file_name,
        }

    def _service_token_to_track(service: str | None) -> str | None:
        token = (service or "").strip().lower()
        if token in {"zerooffer", "zero"}:
            return "zero-offer"
        if token.startswith("interview") or token in {"prep", "interviewprep"}:
            return "interview-prep"
        if token.startswith("gradesal") or token in {"grade", "gradesalary"}:
            return "grade-salary"
        return None

    def _get_plan(track_key: str, plan_key: str) -> dict | None:
        track = ENROLL_TRACKS.get(track_key)
        if not track:
            return None
        for plan in track.get("plans") or []:
            if (plan.get("key") or "").strip() == (plan_key or "").strip():
                return plan
        return None

    def _enroll_expected_amount(price_label: str | None) -> int:
        raw = str(price_label or "").strip()
        if not raw:
            return 0
        # For combo tariffs like "60 000 ₽ + 100%..." invoice the fixed upfront part.
        first_part = raw.split("+", 1)[0]
        amount = _parse_money(first_part)
        if amount > 0:
            return amount
        return _parse_money(raw)

    def _enroll_contract_template_path(level_key: str | None, track_key: str | None) -> str:
        level = str(level_key or "").strip().lower()
        track = str(track_key or "").strip().lower()
        is_zero = track == "zero-offer" or level == "zero"
        # zero-offer -> stricter template (prepayment by fixed date),
        # not_zero tracks -> general template.
        if is_zero:
            contract_path = (cfg.CONTRACT_FILE_PATH_ZERO or cfg.CONTRACT_FILE_PATH or "").strip()
        else:
            contract_path = (cfg.CONTRACT_FILE_PATH_NOT_ZERO or cfg.CONTRACT_FILE_PATH or "").strip()
        if contract_path and not os.path.isabs(contract_path):
            contract_path = os.path.abspath(contract_path)
        return contract_path

    def _enroll_contract_context(owner_tg_id: int) -> dict:
        out = {
            "has_contract": False,
            "contract_id": None,
            "track_key": "",
            "track_title": "",
            "plan_key": "",
            "plan_title": "",
            "price_label": "",
            "expected_amount": 0,
            "purpose": f"Оплата обучения (TG {owner_tg_id})",
        }
        contract = repo_enrollment_contracts.get_by_owner(conn, int(owner_tg_id))
        if not contract:
            return out

        track_key = str(contract["track_key"] or "").strip()
        plan_key = str(contract["tariff_key"] or "").strip()
        plan = _get_plan(track_key, plan_key) or {}

        track_title = str(plan.get("track_title") or contract["track_title"] or "").strip()
        plan_title = str(plan.get("title") or contract["tariff_title"] or "").strip()
        price_label = str(plan.get("price") or contract["tariff_price"] or "").strip()

        purpose_parts = [x for x in [track_title, plan_title] if x]
        purpose_tail = " / ".join(purpose_parts) if purpose_parts else "оплата по договору"

        out.update(
            {
                "has_contract": True,
                "contract_id": int(contract["id"]),
                "track_key": track_key,
                "track_title": track_title or str(contract["track_title"] or "").strip(),
                "plan_key": plan_key,
                "plan_title": plan_title or str(contract["tariff_title"] or "").strip(),
                "price_label": price_label or str(contract["tariff_price"] or "").strip(),
                "expected_amount": _enroll_expected_amount(price_label or contract["tariff_price"]),
                "purpose": f"Оплата по договору: {purpose_tail} (TG {owner_tg_id})",
            }
        )
        return out

    def _start_enroll_payment_wizard(owner_tg_id: int, chat_id: int, owner_username: str | None = None) -> None:
        enroll_ctx = _enroll_contract_context(owner_tg_id)
        if not enroll_ctx.get("has_contract"):
            bot.send_message(
                chat_id,
                "Не найден активный договор. Сначала выбери тариф в разделе «Вступить на обучение».",
            )
            return

        invoice_amount = int(enroll_ctx.get("expected_amount") or 0)
        if not cardlink_client.enabled:
            repo_states.clear_state(conn, owner_tg_id, tz)
            bot.send_message(
                chat_id,
                "Оплата доступна только через кассу Cardlink.\n"
                "Касса сейчас не настроена, попробуй позже.",
            )
            return
        row, err = _create_checkout_payment(
            owner_tg_id=owner_tg_id,
            owner_chat_id=chat_id,
            owner_username=owner_username,
            req_type="ENROLLMENT_PAYMENT",
            service_title=f"Оплата после договора: {enroll_ctx.get('track_title') or 'обучение'} / {enroll_ctx.get('plan_title') or 'тариф'}",
            service_key="enrollment",
            amount=invoice_amount,
            purpose=str(enroll_ctx.get("purpose") or "").strip(),
            metadata={
                "contract_id": enroll_ctx.get("contract_id"),
                "track_key": enroll_ctx.get("track_key"),
                "track_title": enroll_ctx.get("track_title"),
                "plan_key": enroll_ctx.get("plan_key"),
                "plan_title": enroll_ctx.get("plan_title"),
                "price_label": enroll_ctx.get("price_label"),
                "expected_amount": invoice_amount,
            },
        )
        if not row:
            bot.send_message(chat_id, err or "Не удалось создать платёж в Cardlink. Попробуй позже.")
            return
        local_id = int(row["id"])
        check_cb = f"enr:pay:check:{local_id}"
        repo_states.set_state(
            conn,
            int(owner_tg_id),
            STATE_ENROLL_PAYMENT,
            {"step": "checkout_wait", "checkout_payment_id": local_id},
            tz,
        )
        bot.send_message(
            chat_id,
            "<b>Оплата по договору</b>\n"
            f"Сумма: <b>{_format_money_value(invoice_amount)} ₽</b>\n"
            "Нажми «Оплатить», затем «Проверить оплату».",
            reply_markup=_checkout_wait_kb(
                check_callback=check_cb,
                payment_url=str(row["confirmation_url"] or ""),
            ),
        )

    def _enroll_payment_verification(owner_tg_id: int, pay: dict, expected_amount: int, receipt_check: dict | None = None) -> dict:
        base = _payment_verification(owner_tg_id, pay, receipt_check=receipt_check)
        checks = base.get("checks") if isinstance(base.get("checks"), dict) else {}
        warnings = [str(w).strip() for w in (base.get("warnings") or []) if str(w).strip()]
        amount = int(pay.get("amount") or 0)
        expected = int(expected_amount or 0)
        if expected > 0:
            checks["amount_match_expected"] = amount == expected
            if not checks["amount_match_expected"]:
                warnings.append(f"Сумма в чеке ({amount}) не совпадает с суммой по договору ({expected}).")
        base["checks"] = checks
        base["warnings"] = warnings
        base["ok"] = all(bool(v) for v in checks.values())
        base["expected_amount"] = expected
        return base

    def _safe_edit(chat_id: int, message_id: int | None, text: str, kb: InlineKeyboardMarkup | None = None) -> None:
        if message_id is None:
            bot.send_message(chat_id, text, reply_markup=kb)
            return
        bot.edit_message_text(text, chat_id, message_id, reply_markup=kb)

    def _auto_menu_kb() -> InlineKeyboardMarkup:
        kb = InlineKeyboardMarkup()
        if _is_auto_temporarily_unavailable():
            kb.row(InlineKeyboardButton("ℹ️ Временно недоступно", callback_data="auto:disabled"))
        else:
            kb.row(InlineKeyboardButton("🚀 Запустить автоотклики", callback_data="auto:start"))
            kb.row(InlineKeyboardButton("📊 Последний запуск", callback_data="auto:last"))
        kb.row(InlineKeyboardButton("⬅️ В меню", callback_data="v2:home"))
        return kb

    def _auto_direction_kb() -> InlineKeyboardMarkup:
        kb = InlineKeyboardMarkup()
        kb.row(
            InlineKeyboardButton("java", callback_data="auto:dir:java"),
            InlineKeyboardButton("golang", callback_data="auto:dir:golang"),
        )
        kb.row(
            InlineKeyboardButton("frontend", callback_data="auto:dir:frontend"),
            InlineKeyboardButton("python", callback_data="auto:dir:python"),
        )
        kb.row(InlineKeyboardButton("✍️ Другое направление", callback_data="auto:dir:manual"))
        kb.row(InlineKeyboardButton("✖️ Отмена", callback_data="v2:home"))
        return kb

    def _auto_query_kb() -> InlineKeyboardMarkup:
        kb = InlineKeyboardMarkup()
        kb.row(InlineKeyboardButton("Использовать такое же слово, как направление", callback_data="auto:query:default"))
        kb.row(InlineKeyboardButton("Оставить пустым (поиск по направлению)", callback_data="auto:query:skip"))
        kb.row(InlineKeyboardButton("✖️ Отмена", callback_data="v2:home"))
        return kb

    def _auto_login_pick_kb(saved_login: str) -> InlineKeyboardMarkup:
        kb = InlineKeyboardMarkup()
        label = f"Использовать сохранённый HH: {saved_login}"
        if len(label) > 64:
            label = label[:63] + "…"
        kb.row(InlineKeyboardButton(label, callback_data="auto:login:saved"))
        kb.row(InlineKeyboardButton("Ввести другой логин", callback_data="auto:login:manual"))
        kb.row(InlineKeyboardButton("✖️ Отмена", callback_data="v2:home"))
        return kb

    def _auto_confirm_kb() -> InlineKeyboardMarkup:
        kb = InlineKeyboardMarkup()
        kb.row(InlineKeyboardButton("✅ Запустить автоотклики", callback_data="auto:run"))
        kb.row(InlineKeyboardButton("✏️ Начать заново", callback_data="auto:start"))
        kb.row(InlineKeyboardButton("⬅️ В меню", callback_data="v2:home"))
        return kb

    def _enroll_level_kb() -> InlineKeyboardMarkup:
        kb = InlineKeyboardMarkup()
        kb.row(
            InlineKeyboardButton("С нуля", callback_data="enr:level:zero"),
            InlineKeyboardButton("Есть база", callback_data="enr:level:not_zero"),
        )
        kb.row(InlineKeyboardButton("⬅️ В меню", callback_data="v2:home"))
        return kb

    def _enroll_track_kb() -> InlineKeyboardMarkup:
        kb = InlineKeyboardMarkup()
        kb.row(InlineKeyboardButton("После курсов", callback_data="enr:track:prep"))
        kb.row(InlineKeyboardButton("Увеличение зарплаты", callback_data="enr:track:grade"))
        kb.row(InlineKeyboardButton("⬅️ Назад", callback_data="enr:back:level"))
        return kb

    def _enroll_plans_kb(track_key: str) -> InlineKeyboardMarkup:
        kb = InlineKeyboardMarkup()
        track = ENROLL_TRACKS.get(track_key) or {}
        alias = track.get("alias") or ""
        for plan in track.get("plans") or []:
            label = f"{plan.get('title')} • {plan.get('price')}"
            if len(label) > 60:
                label = label[:59] + "…"
            kb.row(InlineKeyboardButton(label, callback_data=f"enr:plan:{alias}:{plan.get('key')}"))
        if (track.get("level") or "") == "zero":
            kb.row(InlineKeyboardButton("⬅️ Назад", callback_data="enr:back:level"))
        else:
            kb.row(InlineKeyboardButton("⬅️ Назад", callback_data="enr:back:track"))
        return kb

    def _enroll_confirm_kb(track_key: str) -> InlineKeyboardMarkup:
        kb = InlineKeyboardMarkup()
        alias = (ENROLL_TRACKS.get(track_key) or {}).get("alias") or ""
        kb.row(InlineKeyboardButton("✅ Продолжить с этим тарифом", callback_data="enr:confirm"))
        kb.row(InlineKeyboardButton("⬅️ К списку тарифов", callback_data=f"enr:back:plans:{alias}"))
        kb.row(InlineKeyboardButton("✖️ В меню", callback_data="v2:home"))
        return kb

    def _enroll_review_kb(track_key: str) -> InlineKeyboardMarkup:
        kb = InlineKeyboardMarkup()
        alias = (ENROLL_TRACKS.get(track_key) or {}).get("alias") or ""
        kb.row(InlineKeyboardButton("✅ Согласен(на), создать на подпись", callback_data="enr:sign:create"))
        if (cfg.MENTOR_CONTACT_URL or "").strip():
            kb.row(InlineKeyboardButton("❓Есть вопросы к договору", url=cfg.MENTOR_CONTACT_URL))
        if alias:
            kb.row(InlineKeyboardButton("⬅️ К списку тарифов", callback_data=f"enr:back:plans:{alias}"))
        kb.row(InlineKeyboardButton("✖️ В меню", callback_data="v2:home"))
        return kb

    def _send_enroll_template(chat_id: int, track_key: str, track: dict, plan: dict, contract_path: str) -> bool:
        contract_sent = False
        if contract_path and os.path.isfile(contract_path):
            try:
                bot.send_document(
                    chat_id,
                    FSInputFile(contract_path),
                    caption=(
                        f"Договор по тарифу:\n"
                        f"{track['title']} -> {plan['title']}\n"
                        f"Цена: {plan['price']}"
                    ),
                )
                contract_sent = True
            except Exception as exc:
                log.warning("Contract send failed chat=%s track=%s plan=%s: %s", chat_id, track_key, plan.get("key"), exc)
                contract_sent = False

        if contract_sent:
            return True

        if contract_path and os.path.isfile(contract_path):
            bot.send_message(
                chat_id,
                "Не удалось отправить файл договора автоматически.\n"
                "Ментор отправит документ вручную.",
            )
        else:
            template_name = "для трека «С нуля до оффера»" if track_key == "zero-offer" else "для треков «После курсов / Увеличение зарплаты»"
            bot.send_message(
                chat_id,
                f"Файл договора {template_name} пока не прикреплён в конфиге бота.\n"
                "Ментор отправит документ вручную.",
            )
        return False

    def _handle_existing_enroll_contract(owner_tg_id: int, owner_chat_id: int, owner_username: str | None) -> bool:
        existing = repo_enrollment_contracts.get_by_owner(conn, int(owner_tg_id))
        if not existing:
            return False

        mentor_url = (cfg.MENTOR_CONTACT_URL or "").strip()
        req_payload = {
            "owner_chat_id": owner_chat_id,
            "owner_username": owner_username,
            "event": "reissue_request",
            "reason": "contract_already_issued",
        }
        req_id = repo_requests.create(conn, tz, owner_tg_id, "ENROLLMENT", req_payload, None)
        notify_admins(
            f"<b>[ЗАЯВКА]</b> {texts.request_title('ENROLLMENT')}\n"
            f"От: @{owner_username or owner_tg_id}\n"
            f"ID: {req_id}\n"
            "Запрошено пересоздание договора (one-time ограничение OkiDoki).",
            kb=request_open_kb(req_id),
        )
        text = (
            "Договор по OkiDoki уже формировался для этого аккаунта (по правилам — только один раз).\n"
            "Если нужно пересоздать договор, напиши ментору.\n"
            "Если текущий договор уже подписан, можно сразу перейти к оплате."
        )
        if mentor_url:
            text += f"\n{mentor_url}"
        existing_kb = InlineKeyboardMarkup()
        existing_sign_url = str(existing["sign_url"] or "").strip()
        if existing_sign_url:
            existing_kb.row(InlineKeyboardButton("✍️ Подписать текущий договор", url=existing_sign_url))
        existing_kb.row(InlineKeyboardButton("✅ Договор подписан, перейти к оплате", callback_data="enr:pay:start"))
        if mentor_url:
            existing_kb.row(InlineKeyboardButton("Написать ментору", url=mentor_url))
        bot.send_message(
            owner_chat_id,
            text,
            reply_markup=existing_kb if existing_kb.inline_keyboard else _home_menu_kb(owner_tg_id),
        )
        return True

    def _auto_cover_letter(direction: str) -> str:
        d = (direction or "").strip().lower()
        if d == "java":
            role = "Java Developer"
        elif d == "golang":
            role = "Golang Developer"
        elif d == "frontend":
            role = "Frontend Developer"
        elif d == "python":
            role = "Python Developer"
        else:
            role = f"{(direction or 'Software').strip().title()} Developer"
        return (
            f"Здравствуйте!\n\n"
            f"Откликаюсь на позицию {role}. Готов(а) пройти техническое интервью, быстро погрузиться в стек команды "
            f"и закрывать задачи в срок. Буду рад(а) пообщаться."
        )

    def _build_candidate_context(owner_tg_id: int, direction: str) -> dict:
        s = repo_students.get_by_owner(conn, owner_tg_id)
        tg_username = ""
        if s and s["username"]:
            tg_username = "@" + str(s["username"]).lstrip("@")
        elif s and s["owner_username"]:
            tg_username = "@" + str(s["owner_username"]).lstrip("@")
        role_map = {
            "java": "Java Developer",
            "golang": "Golang Developer",
            "frontend": "Frontend Developer",
            "python": "Python Developer",
        }
        role = role_map.get((direction or "").strip().lower())
        if not role:
            raw = (direction or "").strip()
            role = f"{raw.title()} Developer" if raw else "Software Developer"
        salary = None
        location = None
        if s:
            salary = s["hh_salary_comfort"] or s["hh_salary_min"]
            location = s["hh_location"]
        payload = {
            "role": role,
            "salary_expectation": f"от {int(salary)} ₽ net" if salary else "",
            "work_format": "удалённо",
            "location": str(location or "").strip(),
            "telegram": tg_username,
        }
        return {k: v for k, v in payload.items() if str(v or "").strip()}

    def _auto_target_preselected(auto: dict) -> bool:
        try:
            site_target = int(auto.get("target_site_clicks") or 0)
        except Exception:
            site_target = 0
        if site_target > 0:
            return True
        if bool(auto.get("target_locked")):
            return True
        source = str(auto.get("source") or "").strip().lower()
        if source != "site":
            return False
        try:
            target = int(auto.get("target_applies") or 0)
        except Exception:
            target = 0
        return target > 0

    def _auto_locked_target(auto: dict) -> int:
        try:
            explicit = int(auto.get("target_site_clicks") or 0)
        except Exception:
            explicit = 0
        if explicit > 0:
            return explicit
        if not _auto_target_preselected(auto):
            return 0
        try:
            target = int(auto.get("target_applies") or 0)
        except Exception:
            target = 0
        return max(target, 0)

    def _auto_requires_site_payment(owner_tg_id: int, auto: dict) -> bool:
        if _has_student_access(owner_tg_id):
            return False
        expected = int(auto.get("price_total") or 0)
        return expected > 0

    def _auto_confirm_text(owner_tg_id: int, auto: dict) -> str:
        summary = (
            "<b>Проверка параметров автооткликов</b>\n"
            f"Направление: <b>{auto.get('direction')}</b>\n"
            f"Логин: <code>{auto.get('login')}</code>\n"
            f"Цель: <b>{int(auto.get('target_applies') or 0)}</b> откликов\n"
            f"Запрос: <code>{auto.get('query_text') or auto.get('direction')}</code>"
        )
        if _auto_requires_site_payment(owner_tg_id, auto):
            summary += f"\nОплата автооткликов: <b>{int(auto.get('price_total') or 0)} ₽</b> ({_payment_channel_label()})."
        return summary

    def _auto_continue_after_auth(owner_tg_id: int, chat_id: int, auto: dict) -> None:
        locked_target = _auto_locked_target(auto)
        if locked_target > 0:
            auto["target_applies"] = locked_target
            auto["target_locked"] = True
            auto["target_site_clicks"] = locked_target
            fixed_query = get_core_query(conn, auto.get("direction"))
            if fixed_query:
                auto["query_text"] = fixed_query
                repo_states.set_state(conn, owner_tg_id, STATE_AUTOAPPLY, {"step": "confirm", "auto": auto}, tz)
                bot.send_message(
                    chat_id,
                    (
                        f"Цель откликов взята с сайта: <b>{locked_target}</b>.\n"
                        f"Запрос для направления зафиксирован: <code>{fixed_query}</code>."
                    ),
                )
                bot.send_message(chat_id, _auto_confirm_text(owner_tg_id, auto), reply_markup=_auto_confirm_kb())
            else:
                repo_states.set_state(conn, owner_tg_id, STATE_AUTOAPPLY, {"step": "query", "auto": auto}, tz)
                bot.send_message(
                    chat_id,
                    (
                        f"Цель откликов взята с сайта: <b>{locked_target}</b>.\n"
                        "Теперь укажи поисковый запрос HH.\n"
                        "Это слово/фраза для поиска вакансий.\n"
                        "Примеры: Java developer, Python backend, Frontend React, QA engineer, DevOps.\n"
                        "Можно отправить текст или выбрать кнопку ниже."
                    ),
                    reply_markup=_auto_query_kb(),
                )
            return
        repo_states.set_state(conn, owner_tg_id, STATE_AUTOAPPLY, {"step": "target", "auto": auto}, tz)
        bot.send_message(chat_id, "Цель откликов (числом, например 200):")

    def _auto_status_label(status: str | None) -> str:
        s = str(status or "").strip().lower()
        if s == "launched":
            return "запущено"
        if s == "failed":
            return "ошибка"
        if s == "created":
            return "создано"
        return s or "неизвестно"

    def _safe_int(value: object) -> int | None:
        try:
            if value is None:
                return None
            if isinstance(value, bool):
                return int(value)
            return int(float(str(value).strip()))
        except Exception:
            return None

    def _pick_int(data: dict, keys: tuple[str, ...]) -> int | None:
        for k in keys:
            if k in data:
                val = _safe_int(data.get(k))
                if val is not None and val >= 0:
                    return val
        return None

    def _pick_bool(data: dict, keys: tuple[str, ...]) -> bool | None:
        for k in keys:
            if k not in data:
                continue
            val = data.get(k)
            if isinstance(val, bool):
                return val
            txt = str(val or "").strip().lower()
            if txt in {"1", "true", "yes", "on", "running"}:
                return True
            if txt in {"0", "false", "no", "off", "stopped"}:
                return False
        return None

    def _pick_text(data: dict, keys: tuple[str, ...]) -> str:
        for k in keys:
            val = str(data.get(k) or "").strip()
            if val:
                return val
        return ""

    def _auto_payment_verification(auto: dict, pay: dict, receipt_check: dict | None = None) -> dict:
        expected_amount = int(auto.get("price_total") or 0)
        amount = int(pay.get("amount") or 0)
        pay_date_raw = str(pay.get("pay_date") or "").strip()
        pay_date = _parse_date_ru(pay_date_raw)
        inn = _digits_only(pay.get("recipient_inn"))
        expected_inn = _digits_only(cfg.PAYMENT_EXPECTED_INN)
        today = now(tz).date()
        max_age_days = max(int(cfg.PAYMENT_MAX_AGE_DAYS or 90), 1)
        checks: dict[str, bool] = {
            "amount_positive": amount > 0,
            "amount_match_expected": amount == expected_amount if expected_amount > 0 else amount > 0,
            "date_valid": pay_date is not None,
            "date_not_future": bool(pay_date and pay_date <= today),
            "date_recent": bool(pay_date and pay_date >= (today - timedelta(days=max_age_days))),
            "recipient_inn_match": bool(not expected_inn or inn == expected_inn),
        }
        warnings: list[str] = []
        if not checks["amount_match_expected"] and expected_amount > 0:
            warnings.append(f"Сумма в чеке ({amount}) не совпадает с ожидаемой ({expected_amount}).")
        if not checks["recipient_inn_match"]:
            warnings.append("ИНН получателя не совпадает с расчётным счётом.")
        if not checks["date_recent"]:
            warnings.append("Дата платежа выходит за допустимый интервал проверки.")
        receipt_check = receipt_check if isinstance(receipt_check, dict) else {}
        if receipt_check:
            receipt_checks = receipt_check.get("checks") if isinstance(receipt_check.get("checks"), dict) else {}
            checks["receipt_format_supported"] = bool(receipt_checks.get("format_supported", False))
            checks["receipt_hint_detected"] = bool(receipt_checks.get("receipt_hint_detected", False))
            for w in (receipt_check.get("warnings") or []):
                w = str(w or "").strip()
                if w and w not in warnings:
                    warnings.append(w)
        return {
            "expected_amount": expected_amount,
            "amount": amount,
            "pay_date": pay_date_raw,
            "recipient_inn": inn,
            "checks": checks,
            "ok": all(checks.values()),
            "warnings": warnings,
            "receipt_check": receipt_check,
        }

    def _payment_verification(owner_tg_id: int, pay: dict, receipt_check: dict | None = None) -> dict:
        amount = int(pay.get("amount") or 0)
        pay_date_raw = str(pay.get("pay_date") or "").strip()
        pay_date = _parse_date_ru(pay_date_raw)
        inn = _digits_only(pay.get("recipient_inn"))
        expected_inn = _digits_only(cfg.PAYMENT_EXPECTED_INN)
        today = now(tz).date()
        max_age_days = max(int(cfg.PAYMENT_MAX_AGE_DAYS or 90), 1)

        checks: dict[str, bool] = {
            "amount_positive": amount > 0,
            "date_valid": pay_date is not None,
            "date_not_future": bool(pay_date and pay_date <= today),
            "date_recent": bool(pay_date and pay_date >= (today - timedelta(days=max_age_days))),
            "recipient_inn_match": bool(not expected_inn or inn == expected_inn),
        }
        warnings: list[str] = []
        if not checks["recipient_inn_match"]:
            warnings.append("ИНН получателя не совпадает с расчётным счётом ментора.")
        if not checks["date_recent"]:
            warnings.append("Дата платежа выходит за допустимый интервал проверки.")

        s = repo_students.get_by_owner(conn, owner_tg_id)
        if s and s["offer_amount"]:
            pp = repo_postpay.get(conn, int(s["id"]))
            if pp and pp["enabled"] and pp["total_percent"]:
                total = int(round(float(s["offer_amount"]) * float(pp["total_percent"]) / 100))
                paid = repo_payments.sum_payments(conn, int(s["id"]), "postpay")
                remaining = max(total - paid, 0)
                checks["amount_reasonable"] = bool(remaining <= 0 or amount <= (remaining + 5000))
                if not checks["amount_reasonable"]:
                    warnings.append("Сумма сильно выше ожидаемого остатка постоплаты.")
        else:
            checks["amount_reasonable"] = True

        receipt_check = receipt_check if isinstance(receipt_check, dict) else {}
        if receipt_check:
            receipt_checks = receipt_check.get("checks") if isinstance(receipt_check.get("checks"), dict) else {}
            checks["receipt_format_supported"] = bool(receipt_checks.get("format_supported", False))
            checks["receipt_hint_detected"] = bool(receipt_checks.get("receipt_hint_detected", False))
            for w in (receipt_check.get("warnings") or []):
                w = str(w or "").strip()
                if w and w not in warnings:
                    warnings.append(w)

        ok = all(bool(v) for v in checks.values())
        return {
            "ok": ok,
            "checks": checks,
            "warnings": warnings,
            "expected_inn": expected_inn,
            "provided_inn": inn,
            "pay_date": pay_date_raw,
            "amount": amount,
            "receipt_check": receipt_check,
        }

    def _last_request(owner_tg_id: int, req_type: str) -> sqlite3.Row | None:
        return conn.execute(
            "SELECT * FROM student_requests WHERE owner_tg_id=? AND req_type=? ORDER BY id DESC LIMIT 1",
            (int(owner_tg_id), str(req_type)),
        ).fetchone()

    def _prefill_profile_from_student(owner_tg_id: int) -> dict:
        s = repo_students.get_by_owner(conn, int(owner_tg_id))
        if not s:
            return {}
        return {
            "fio": s["fio"],
            "username": ("@" + s["username"]) if s["username"] else None,
            "direction": s["direction_name"],
            "stage": s["stage_name"],
            "tariff": (s["tariff"] or "post").strip(),
            "contract_url": s["contract_url"],
            "prepay": 0,
        }

    def _prefill_profile_from_contract_row(contract: sqlite3.Row | dict | None) -> dict:
        if not cfg.OKIDOKI_PROFILE_PREFILL_ENABLED:
            return {}
        if not contract:
            return {}

        payload: dict = {}
        contract_keys: set[str] = set()
        if hasattr(contract, "keys"):
            try:
                contract_keys = set(contract.keys())  # type: ignore[arg-type]
            except Exception:
                contract_keys = set()

        def _contract_get(key: str, default=None):
            try:
                if contract_keys and key not in contract_keys:
                    return default
                return contract[key]  # type: ignore[index]
            except Exception:
                return default

        def _merge_payload(raw: str | None) -> None:
            if not raw:
                return
            try:
                data = json.loads(str(raw))
            except Exception:
                return
            if isinstance(data, dict):
                payload.update(data)

        if "okidoki_request_json" in contract_keys:
            _merge_payload(_contract_get("okidoki_request_json"))
        if "okidoki_response_json" in contract_keys:
            _merge_payload(_contract_get("okidoki_response_json"))

        payload_layers: list[dict] = []
        if isinstance(payload, dict):
            payload_layers.append(payload)
            for nested_key in ("contract", "data", "result", "payload"):
                nested_val = payload.get(nested_key)
                if isinstance(nested_val, dict):
                    payload_layers.append(nested_val)

        def _pick_text(*keys: str) -> str | None:
            for layer in payload_layers:
                for k in keys:
                    val = layer.get(k)
                    if isinstance(val, str) and val.strip():
                        return val.strip()
            return None

        def _pick_entity_value(*entity_keys: str) -> str | None:
            wanted = {str(k or "").strip().lower() for k in entity_keys if str(k or "").strip()}
            if not wanted:
                return None
            for layer in payload_layers:
                entities = layer.get("entities")
                if not isinstance(entities, list):
                    continue
                for item in entities:
                    if not isinstance(item, dict):
                        continue
                    key_name = str(item.get("keyword") or item.get("name") or item.get("id") or "").strip().lower()
                    if key_name not in wanted:
                        continue
                    val = item.get("value")
                    if isinstance(val, str) and val.strip():
                        return val.strip()
                    if isinstance(val, (int, float)) and not isinstance(val, bool):
                        return str(val)
            return None

        def _pick_entity_number(*entity_keys: str) -> float | None:
            raw = _pick_entity_value(*entity_keys)
            if raw is None:
                return None
            txt = str(raw).strip().replace(",", ".")
            if not txt:
                return None
            filtered = "".join(ch for ch in txt if (ch.isdigit() or ch == "."))
            if not filtered:
                return None
            try:
                val = float(filtered)
            except Exception:
                return None
            return val if val > 0 else None

        def _pick_number(*keys: str) -> float | None:
            for layer in payload_layers:
                for k in keys:
                    val = layer.get(k)
                    if isinstance(val, bool):
                        continue
                    if isinstance(val, (int, float)):
                        f = float(val)
                        if f > 0:
                            return f
                        continue
                    txt = str(val or "").strip().replace(",", ".")
                    if not txt:
                        continue
                    # Пробуем извлечь число даже из строк "60 000 ₽"
                    digits = "".join(ch for ch in txt if (ch.isdigit() or ch == "."))
                    if not digits:
                        continue
                    try:
                        f = float(digits)
                    except Exception:
                        continue
                    if f > 0:
                        return f
            return None

        def _normalize_stage(raw_stage: str | None) -> str | None:
            s = str(raw_stage or "").strip().lower()
            if not s:
                return None
            if "с нуля" in s or "zero" in s:
                return "С нуля"
            if "дообуч" in s or "not_zero" in s:
                return "Дообучение"
            if "собесед" in s or "interview" in s:
                return "Собеседования"
            if "дипф" in s:
                return "Дипфейк"
            return None

        def _normalize_tariff(raw_tariff: str | None) -> str | None:
            t = str(raw_tariff or "").strip().lower()
            if not t:
                return None
            if t in {"pre", "post", "pre_post"}:
                return t
            has_pre = ("пред" in t) or ("pre" in t)
            has_post = ("пост" in t) or ("post" in t)
            if has_pre and has_post:
                return "pre_post"
            if has_post:
                return "post"
            if has_pre:
                return "pre"
            return None

        def _infer_tariff_from_template_name(raw_name: str | None) -> str | None:
            name = str(raw_name or "").strip().lower()
            if not name:
                return None
            m = re.search(r"шаблон\s+обучение\s+(\d+)", name)
            if not m:
                return None
            try:
                num = int(m.group(1))
            except Exception:
                return None
            # Эвристика по текущим шаблонам OkiDoki:
            # 1/2/4/7 -> с предоплатой + постоплатой, 3/5 -> только постоплата.
            if num in {1, 2, 4, 7}:
                return "pre_post"
            if num in {3, 5}:
                return "post"
            return None

        out: dict = {}
        fio = _pick_text("fio", "full_name", "fullname", "name", "client_name", "customer_name", "person_name")
        if not fio:
            fio = _pick_entity_value("ФИО", "Фио", "ФИО клиента", "Клиент")
        if fio:
            out["fio"] = fio
        direction = _pick_text(
            "direction",
            "specialization",
            "stack",
            "profile",
            "course_direction",
            "area",
            "Область",
            "область",
        )
        if not direction:
            direction = _pick_entity_value("Область", "Направление", "Специализация", "stack", "profile")
        if direction:
            out["direction"] = direction

        stage_direct = _normalize_stage(_pick_text("stage", "student_stage", "learning_stage"))
        if not stage_direct:
            stage_direct = _normalize_stage(_pick_entity_value("Этап", "Уровень", "Этап обучения"))
        if stage_direct:
            out["stage"] = stage_direct

        tariff_direct = _normalize_tariff(_pick_text("tariff", "tariff_type"))
        if tariff_direct:
            out["tariff"] = tariff_direct
        if not out.get("tariff"):
            tpl_tariff = _infer_tariff_from_template_name(_pick_text("name", "template_name", "contract_name"))
            if tpl_tariff:
                out["tariff"] = tpl_tariff

        # stage fallback from enrollment level
        level_key = str(_contract_get("level_key") or "").strip().lower()
        if level_key == "zero" and not out.get("stage"):
            out["stage"] = "С нуля"
        elif level_key and not out.get("stage"):
            out["stage"] = "Дообучение"

        # infer tariff and numeric parts from tariff price
        tariff_price = str(_contract_get("tariff_price") or "").strip()
        if not tariff_price:
            tariff_price = str(_pick_entity_value("Тариф", "Стоимость", "Цена", "Предоплата") or "").strip()

        prepay_direct = _pick_number("prepay", "prepayment", "prepay_amount")
        if not prepay_direct:
            prepay_direct = _pick_entity_number("Предоплата", "Сумма предоплаты")
        if prepay_direct and float(prepay_direct) > 0:
            out["prepay"] = int(round(float(prepay_direct)))

        total_percent_direct = _pick_number("post_total_percent", "postpay_total_percent", "total_percent")
        if total_percent_direct and float(total_percent_direct) > 0:
            out["post_total_percent"] = float(total_percent_direct)

        monthly_percent_direct = _pick_number("post_monthly_percent", "postpay_monthly_percent", "monthly_percent")
        if monthly_percent_direct and float(monthly_percent_direct) > 0:
            out["post_monthly_percent"] = float(monthly_percent_direct)

        # Шаблоны OkiDoki часто хранят не проценты, а количество месяцев постоплаты.
        # Если есть months, переводим в проценты (для анкеты бота): total=100, monthly=100/months.
        if float(out.get("post_total_percent") or 0) <= 0 or float(out.get("post_monthly_percent") or 0) <= 0:
            post_months = _pick_entity_number("Количество месяцев постоплаты", "Месяцев постоплаты", "Постоплата месяцев")
            if post_months and float(post_months) > 0:
                months = max(1.0, float(post_months))
                if float(out.get("post_total_percent") or 0) <= 0:
                    out["post_total_percent"] = 100.0
                if float(out.get("post_monthly_percent") or 0) <= 0:
                    out["post_monthly_percent"] = round(100.0 / months, 2)

        tg_client = _pick_entity_value("Телеграм клиента", "Telegram клиента", "Телеграм")
        if tg_client and not out.get("username"):
            tg = str(tg_client).strip()
            if tg.startswith("@"):
                out["username"] = tg

        if not out.get("tariff"):
            has_prepay = float(out.get("prepay") or 0) > 0
            has_post = float(out.get("post_total_percent") or 0) > 0
            if has_prepay and has_post:
                out["tariff"] = "pre_post"
            elif has_post:
                out["tariff"] = "post"
            elif has_prepay:
                out["tariff"] = "pre"

        if tariff_price:
            has_plus = "+" in tariff_price
            has_percent = "%" in tariff_price
            if has_plus and has_percent and not out.get("tariff"):
                out["tariff"] = "pre_post"
            elif has_percent and not out.get("tariff"):
                out["tariff"] = "post"
            elif not out.get("tariff"):
                out["tariff"] = "pre"

            first_part = tariff_price.split("+", 1)[0]
            prepay_amount = _parse_money(first_part)
            if prepay_amount > 0 and float(out.get("prepay") or 0) <= 0:
                out["prepay"] = prepay_amount

            percents: list[float] = []
            for part in tariff_price.replace(",", ".").split("%"):
                tail_digits = "".join(ch for ch in part[-6:] if (ch.isdigit() or ch in "."))
                if not tail_digits:
                    continue
                try:
                    percents.append(float(tail_digits))
                except Exception:
                    continue
            if percents and float(out.get("post_total_percent") or 0) <= 0:
                out["post_total_percent"] = float(percents[0])
                if len(percents) > 1 and float(out.get("post_monthly_percent") or 0) <= 0:
                    out["post_monthly_percent"] = float(percents[1])

        sign_url = _pick_text("contract_url", "sign_url", "signUrl", "url", "link")
        if not sign_url:
            sign_url = str(_contract_get("sign_url") or _contract_get("link") or "").strip()
        if sign_url:
            out["contract_url"] = sign_url
        return out

    def _prefill_profile_from_contract(owner_tg_id: int) -> dict:
        contract = repo_enrollment_contracts.get_by_owner(conn, int(owner_tg_id))
        return _prefill_profile_from_contract_row(contract)

    def _prefill_profile_from_contract_api(contract_url: str) -> dict:
        """
        Fallback: если локальный кэш договора пуст, пробуем прочитать карточку
        напрямую из API OkiDoki по contract_id из ссылки.
        """
        if not cfg.OKIDOKI_PROFILE_PREFILL_ENABLED:
            return {}
        url = str(contract_url or "").strip()
        if not url:
            return {}
        try:
            payload = okidoki_client.fetch_contract_payload(url)
        except OkiDokiClientError as exc:
            log.info("OkiDoki prefill lookup failed for url=%s: %s", url, exc)
            return {}
        if not isinstance(payload, dict) or not payload:
            return {}
        payload_root = payload.get("contract") if isinstance(payload.get("contract"), dict) else payload
        pseudo_contract = {
            "level_key": str(payload_root.get("level_key") or payload_root.get("level") or "").strip(),
            "tariff_price": str(payload_root.get("tariff_price") or payload_root.get("price") or "").strip(),
            "sign_url": (
                str(
                    payload_root.get("sign_url")
                    or payload_root.get("signUrl")
                    or payload_root.get("url")
                    or payload_root.get("link")
                    or ""
                ).strip()
                or url
            ),
            "okidoki_request_json": "",
            "okidoki_response_json": json.dumps(payload, ensure_ascii=False),
        }
        return _prefill_profile_from_contract_row(pseudo_contract)

    def _prefill_profile_from_contract_url(owner_tg_id: int, contract_url: str) -> dict:
        """
        Подтягивает данные анкеты по ссылке на договор.
        Источник данных — последняя запись enrollment_contracts текущего пользователя
        (payload OkiDoki). Ссылка нужна как явное подтверждение пользователя и
        сохраняется в карточку даже если часть полей не удалось разобрать.
        """
        url = str(contract_url or "").strip()
        if not url:
            return {}
        out: dict = {"contract_url": url}
        if not cfg.OKIDOKI_PROFILE_PREFILL_ENABLED:
            return out

        # 1) Пытаемся найти договор по самой ссылке (в пределах owner, затем глобально).
        contract = repo_enrollment_contracts.find_by_contract_url(conn, url, int(owner_tg_id))
        # 2) Fallback: последняя запись договора пользователя.
        if not contract:
            contract = repo_enrollment_contracts.get_by_owner(conn, int(owner_tg_id))
        merged = _merge_profile_prefills(_prefill_profile_from_contract_row(contract), out)
        useful = {k for k, v in merged.items() if k != "contract_url" and v not in (None, "", 0)}
        if not useful:
            # 3) Fallback: пробуем live lookup из OkiDoki API по contract_id в ссылке.
            merged = _merge_profile_prefills(merged, _prefill_profile_from_contract_api(url))
        merged["contract_url"] = url
        return merged

    def _merge_profile_prefills(*sources: dict | None) -> dict:
        merged: dict = {}
        numeric_keys = {"prepay", "post_total_percent", "post_monthly_percent"}
        for src in sources:
            if not isinstance(src, dict):
                continue
            for key, value in src.items():
                if key in numeric_keys:
                    if value is None:
                        continue
                    merged[key] = value
                    continue
                if isinstance(value, str):
                    if value.strip():
                        merged[key] = value.strip()
                    continue
                if value is not None:
                    merged[key] = value
        return merged

    def _profile_next_step(prof: dict) -> str | None:
        fio = str(prof.get("fio") or "").strip()
        if len(fio) < 5:
            return "fio"

        if "username" not in prof:
            return "username"

        if not str(prof.get("direction") or "").strip():
            return "direction"
        if not str(prof.get("stage") or "").strip():
            return "stage"

        tariff = str(prof.get("tariff") or "").strip().lower()
        if tariff not in {"pre", "post", "pre_post"}:
            return "tariff"

        if tariff in {"pre", "pre_post"} and "prepay" not in prof:
            return "prepay"

        if tariff in {"post", "pre_post"}:
            total = prof.get("post_total_percent")
            if total is None or float(total or 0) <= 0:
                return "post_total_percent"
            monthly = prof.get("post_monthly_percent")
            if monthly is None or float(monthly or 0) <= 0:
                return "post_monthly_percent"

        contract_url = str(prof.get("contract_url") or "").strip()
        if not contract_url.lower().startswith(("http://", "https://")):
            return "contract"

        return None

    def _profile_missing_labels(prof: dict) -> list[str]:
        missing: list[str] = []
        fio = str(prof.get("fio") or "").strip()
        if len(fio) < 5:
            missing.append("ФИО")
        if "username" not in prof:
            missing.append("username")
        if not str(prof.get("direction") or "").strip():
            missing.append("направление")
        if not str(prof.get("stage") or "").strip():
            missing.append("этап")
        tariff = str(prof.get("tariff") or "").strip().lower()
        if tariff not in {"pre", "post", "pre_post"}:
            missing.append("тариф")
        if tariff in {"pre", "pre_post"} and "prepay" not in prof:
            missing.append("предоплата")
        if tariff in {"post", "pre_post"}:
            total = prof.get("post_total_percent")
            try:
                total_ok = total is not None and float(total or 0) > 0
            except Exception:
                total_ok = False
            monthly = prof.get("post_monthly_percent")
            try:
                monthly_ok = monthly is not None and float(monthly or 0) > 0
            except Exception:
                monthly_ok = False
            if not total_ok:
                missing.append("постоплата всего (%)")
            if not monthly_ok:
                missing.append("постоплата в месяц (%)")
        contract_url = str(prof.get("contract_url") or "").strip()
        if not contract_url.lower().startswith(("http://", "https://")):
            missing.append("ссылка на договор")
        return missing

    def _submit_profile_request(owner_tg_id: int, chat_id: int, owner_username: str | None, prof: dict) -> None:
        payload = dict(prof)
        payload["owner_chat_id"] = chat_id
        payload["owner_username"] = owner_username
        payload["join_date"] = payload.get("join_date") or format_date_ddmmyyyy(now(tz).date())
        if not payload.get("username") and owner_username:
            payload["username"] = "@" + owner_username
        req_id = repo_requests.create(conn, tz, owner_tg_id, "PROFILE", payload, None)
        repo_analytics.log_event(conn, tz, owner_tg_id, "funnel.student.profile_submitted", {"req_id": req_id})
        bot.send_message(chat_id, "Анкета отправлена ментору на подтверждение.")
        notify_admins(
            f"<b>[ЗАЯВКА]</b> {texts.request_title('PROFILE')}\nОт: @{owner_username or owner_tg_id}\nID: {req_id}",
            kb=request_admin_kb(req_id),
        )
        send_user_home(chat_id, owner_tg_id)

    def _profile_continue_wizard(owner_tg_id: int, chat_id: int, prof: dict, owner_username: str | None = None) -> None:
        if "username" not in prof and owner_username:
            prof["username"] = "@" + owner_username
        step = _profile_next_step(prof)
        if step is None:
            repo_states.clear_state(conn, owner_tg_id, tz)
            _submit_profile_request(owner_tg_id, chat_id, owner_username, prof)
            return
        data = {"step": step, "profile": prof}
        repo_states.set_state(conn, owner_tg_id, STATE_PROFILE, data, tz)
        if step == "contract_lookup":
            bot.send_message(
                chat_id,
                "Вставь ссылку на договор (http/https), и я попробую подтянуть данные анкеты автоматически:",
            )
            return
        if step == "fio":
            bot.send_message(chat_id, "ФИО (как в договоре):")
            return
        if step == "username":
            kb = InlineKeyboardMarkup()
            if owner_username:
                kb.row(InlineKeyboardButton(f"Использовать @{owner_username}", callback_data=f"uform:uname:@{owner_username}"))
            kb.row(InlineKeyboardButton("У меня нет username", callback_data="uform:uname:skip"))
            kb.row(InlineKeyboardButton("✖️ Отмена", callback_data="u:cancel"))
            bot.send_message(chat_id, "Username (если есть). Выбери кнопку:", reply_markup=kb)
            return
        if step == "direction":
            bot.send_message(chat_id, "Направление?", reply_markup=user_direction_kb())
            return
        if step == "stage":
            bot.send_message(chat_id, "Этап?", reply_markup=user_stage_kb())
            return
        if step == "tariff":
            bot.send_message(chat_id, "Тариф?", reply_markup=user_tariff_kb())
            return
        if step == "prepay":
            bot.send_message(chat_id, "Предоплата (если 0 — выбери 0 или напиши сумму):", reply_markup=user_prepay_kb())
            return
        if step == "post_total_percent":
            bot.send_message(chat_id, "Процент постоплаты всего:", reply_markup=user_post_total_kb())
            return
        if step == "post_monthly_percent":
            bot.send_message(chat_id, "Процент в месяц:", reply_markup=user_post_monthly_kb())
            return
        bot.send_message(chat_id, "Ссылка на договор (обязательно):")

    def _student_hh_credentials(owner_tg_id: int) -> tuple[str, str]:
        s = repo_students.get_by_owner(conn, int(owner_tg_id))
        if not s:
            return "", ""
        login = str(s["hh_email"] or s["hh_phone"] or "").strip()
        password = str(s["hh_password"] or "").strip()
        return login, password

    def _store_student_hh_credentials(owner_tg_id: int, login: str, password: str) -> bool:
        student = repo_students.get_by_owner(conn, int(owner_tg_id))
        if not student:
            return False
        login_clean = str(login or "").strip()
        password_clean = str(password or "").strip()
        if not login_clean or not password_clean:
            return False
        fields = {"hh_password": password_clean}
        if "@" in login_clean:
            fields["hh_email"] = login_clean
            fields["hh_phone"] = None
        else:
            fields["hh_phone"] = login_clean
            fields["hh_email"] = None
        repo_students.update_student(conn, int(student["id"]), tz, **fields)
        return True

    def _start_profile_wizard(owner_tg_id: int, chat_id: int, prefill: dict | None = None, owner_username: str | None = None) -> None:
        _profile_continue_wizard(owner_tg_id, chat_id, dict(prefill or {}), owner_username=owner_username)

    def _start_accounts_wizard(owner_tg_id: int, chat_id: int) -> None:
        repo_states.set_state(conn, owner_tg_id, STATE_ACCOUNTS, {"step": "phone", "acc": {}}, tz)
        bot.send_message(chat_id, "Телефон (для HH/контакта). Если нет — напиши '-':")

    def _start_payment_wizard(owner_tg_id: int, chat_id: int, owner_username: str | None = None) -> None:
        if not _user_show_pay_button(owner_tg_id):
            bot.send_message(
                chat_id,
                "Пока не могу оформить запрос на оплату: в карточке ученика не выставлен оффер или не включена постоплата.\n"
                "Напиши ментору, и после активации сможешь продолжить.",
            )
            return
        due = _payment_due_context(owner_tg_id)
        if due.get("closed"):
            bot.send_message(chat_id, "Постоплата уже закрыта. Если видишь несоответствие, напиши ментору.")
            return

        invoice_amount = int(due.get("invoice_amount") or 0)
        if not cardlink_client.enabled:
            repo_states.clear_state(conn, owner_tg_id, tz)
            bot.send_message(
                chat_id,
                "Оплата доступна только через кассу Cardlink.\n"
                "Касса сейчас не настроена, попробуй позже.",
            )
            return
        row, err = _create_checkout_payment(
            owner_tg_id=owner_tg_id,
            owner_chat_id=chat_id,
            owner_username=owner_username,
            req_type="PAYMENT",
            service_key="postpay",
            service_title="Постоплата по обучению",
            amount=invoice_amount,
            purpose=f"Оплата обучения (TG {owner_tg_id})",
            metadata={
                "remaining_postpay": due.get("remaining_postpay"),
                "recommended_amount": due.get("recommended_amount"),
            },
        )
        if not row:
            bot.send_message(chat_id, err or "Не удалось создать платёж в Cardlink. Попробуй позже.")
            return
        local_id = int(row["id"])
        repo_states.set_state(
            conn,
            int(owner_tg_id),
            STATE_PAYMENT,
            {"step": "checkout_wait", "checkout_payment_id": local_id},
            tz,
        )
        bot.send_message(
            chat_id,
            "<b>Постоплата</b>\n"
            f"Сумма: <b>{_format_money_value(invoice_amount)} ₽</b>\n"
            "Нажми «Оплатить», затем «Проверить оплату».",
            reply_markup=_checkout_wait_kb(
                check_callback=f"u:pay:check:{local_id}",
                payment_url=str(row["confirmation_url"] or ""),
                back_callback="u:cancel",
            ),
        )

    def _deeplink_payload(meta: dict | None, route: str) -> dict:
        meta = meta or {}
        return {
            "route": route,
            "source": meta.get("source", ""),
            "service": meta.get("service", ""),
            "clicks": meta.get("clicks"),
            "version": meta.get("version", ""),
        }

    def _auto_text(owner_tg_id: int, meta: dict | None = None) -> str:
        if _is_auto_temporarily_unavailable():
            return _auto_temporarily_unavailable_text()
        clicks = 0
        if isinstance(meta, dict):
            try:
                clicks = max(int(meta.get("clicks") or 0), 0)
            except Exception:
                clicks = 0
        clicks_line = f"\nВыбор с сайта: <b>{clicks}</b> откликов." if clicks > 0 else ""
        target = clicks if clicks > 0 else 200
        price = _auto_price_breakdown(owner_tg_id, target) if target > 0 else {}
        price_total = int(price.get("total_price") or 0)
        free_clicks = int(price.get("free_clicks") or 0)
        free_remaining = int(price.get("free_remaining") or 0)
        if target < 200 and free_remaining > 0:
            promo_line = (
                f"\nПромо: в этом запуске бесплатно до <b>{free_clicks}</b> откликов "
                f"(остаток по аккаунту: <b>{free_remaining}</b> из 50)."
            )
        elif target < 200:
            promo_line = "\nПромо первых 50 откликов уже использовано для этого аккаунта."
        else:
            promo_line = ""
        payment_preview = {"source": "site", "price_total": price_total}
        price_line = (
            f"\nОплата по калькулятору: <b>{price_total} ₽</b> ({_payment_channel_label()})."
            if clicks > 0 and _auto_requires_site_payment(owner_tg_id, payment_preview)
            else ""
        )
        last = repo_autoapply_runs.last_by_owner(conn, int(owner_tg_id))
        last_line = ""
        if last:
            last_status = _auto_status_label(str(last["status"] or ""))
            last_line = (
                "\n\n"
                "<b>Текущий статус:</b>\n"
                f"Последний запуск: <b>{last_status}</b> ({last['created_at']})\n"
                f"Цель: <b>{int(last['target_applies'] or 0)}</b> • Направление: <b>{last['direction']}</b>"
            )
        return (
            "<b>Автоотклики</b>\n"
            "Запуск выполняется прямо из бота через внутренний API.\n"
            "Шаги: направление -> цель откликов -> запуск.\n"
            "Для учеников HH логин/пароль заполняются один раз при первом запуске.\n"
            f"Стартовая цель: <b>{target}</b> откликов.{clicks_line}{promo_line}{price_line}{last_line}"
        )

    def _open_auto_menu(chat_id: int, owner_tg_id: int, message_id: int | None = None, meta: dict | None = None) -> None:
        if _is_auto_temporarily_unavailable():
            repo_states.clear_state(conn, owner_tg_id, tz)
            _safe_edit(chat_id, message_id, _auto_temporarily_unavailable_text(), _auto_menu_kb())
            return
        clicks = 0
        site_entry = False
        if isinstance(meta, dict):
            try:
                clicks = max(int(meta.get("clicks") or 0), 0)
            except Exception:
                clicks = 0
            site_entry = bool(meta.get("version") or meta.get("source") or meta.get("raw"))
        st, data = repo_states.get_state(conn, owner_tg_id)
        auto = (data.get("auto") if st == STATE_AUTOAPPLY else {}) or {}
        if clicks > 0:
            auto["target_applies"] = clicks
            auto["target_locked"] = True
            auto["target_site_clicks"] = clicks
            auto["price_total"] = _auto_price_for_user(owner_tg_id, clicks)
        auto.setdefault("target_applies", 200)
        auto.setdefault("target_locked", False)
        auto.setdefault("target_site_clicks", 0)
        auto.setdefault("price_total", _auto_price_for_user(owner_tg_id, int(auto.get("target_applies") or 0)))
        if site_entry:
            auto["source"] = "site"
            locked_target = _auto_locked_target(auto)
            if locked_target > 0:
                auto["target_applies"] = locked_target
                auto["target_locked"] = True
                auto["target_site_clicks"] = locked_target
        else:
            # Keep site-fixed target for the current in-progress request.
            # It should not be reset by reopening the auto menu mid-flow.
            locked_target = _auto_locked_target(auto)
            if locked_target > 0 and str(auto.get("source") or "").strip().lower() == "site":
                auto["source"] = "site"
                auto["target_applies"] = locked_target
                auto["target_locked"] = True
                auto["target_site_clicks"] = locked_target
            else:
                auto["source"] = "menu"
                auto["target_locked"] = False
                auto["target_site_clicks"] = 0
        repo_states.set_state(conn, owner_tg_id, STATE_AUTOAPPLY, {"step": "menu", "auto": auto}, tz)
        _safe_edit(chat_id, message_id, _auto_text(owner_tg_id, meta), _auto_menu_kb())

    def _open_enroll_level(chat_id: int, owner_tg_id: int, message_id: int | None = None) -> None:
        repo_states.set_state(conn, owner_tg_id, STATE_ENROLL, {"step": "level", "enroll": {}}, tz)
        _safe_edit(
            chat_id,
            message_id,
            "<b>Вступить на обучение</b>\nВыбери стартовую точку:",
            _enroll_level_kb(),
        )

    def _render_enroll_track(chat_id: int, owner_tg_id: int, message_id: int | None = None) -> None:
        st, data = repo_states.get_state(conn, owner_tg_id)
        enroll = (data.get("enroll") if st == STATE_ENROLL else {}) or {}
        enroll["level"] = "not_zero"
        repo_states.set_state(conn, owner_tg_id, STATE_ENROLL, {"step": "track", "enroll": enroll}, tz)
        _safe_edit(
            chat_id,
            message_id,
            "<b>Вступить на обучение</b>\nВыбери направление:",
            _enroll_track_kb(),
        )

    def _render_enroll_plans(chat_id: int, owner_tg_id: int, track_key: str, message_id: int | None = None) -> None:
        track = ENROLL_TRACKS.get(track_key)
        if not track:
            _safe_edit(chat_id, message_id, "Не удалось открыть тарифы. Попробуй снова через меню.", _v2_back_kb())
            return
        st, data = repo_states.get_state(conn, owner_tg_id)
        enroll = (data.get("enroll") if st == STATE_ENROLL else {}) or {}
        enroll["track_key"] = track_key
        enroll["level"] = track.get("level", enroll.get("level") or "not_zero")
        repo_states.set_state(conn, owner_tg_id, STATE_ENROLL, {"step": "plans", "enroll": enroll}, tz)
        _safe_edit(
            chat_id,
            message_id,
            f"<b>{track['title']}</b>\nВыбери тариф:",
            _enroll_plans_kb(track_key),
        )

    def _render_enroll_confirm(chat_id: int, owner_tg_id: int, track_key: str, plan_key: str, message_id: int | None = None) -> None:
        track = ENROLL_TRACKS.get(track_key)
        plan = _get_plan(track_key, plan_key)
        if not track or not plan:
            _safe_edit(chat_id, message_id, "Не удалось открыть тариф. Вернись и выбери снова.", _v2_back_kb())
            return
        st, data = repo_states.get_state(conn, owner_tg_id)
        enroll = (data.get("enroll") if st == STATE_ENROLL else {}) or {}
        enroll.update(
            {
                "level": track.get("level") or enroll.get("level") or "not_zero",
                "track_key": track_key,
                "plan_key": plan_key,
            }
        )
        repo_states.set_state(conn, owner_tg_id, STATE_ENROLL, {"step": "confirm", "enroll": enroll}, tz)
        text = (
            "<b>Подтверждение тарифа</b>\n"
            f"Направление: <b>{track['title']}</b>\n"
            f"Тариф: <b>{plan['title']}</b>\n"
            f"Формат: {plan['subtitle']}\n"
            f"Цена: <b>{plan['price']}</b>\n\n"
            "Если устраивает, нажми «Продолжить с этим тарифом»."
        )
        _safe_edit(chat_id, message_id, text, _enroll_confirm_kb(track_key))

    def _open_route_home(chat_id: int, owner_tg_id: int, meta: dict | None = None) -> None:
        repo_analytics.log_event(conn, tz, owner_tg_id, "deeplink.route_open", _deeplink_payload(meta, "home"))
        send_regular_home(chat_id, owner_tg_id)

    def _open_route_auto(chat_id: int, owner_tg_id: int, meta: dict | None = None) -> None:
        repo_states.clear_state(conn, owner_tg_id, tz)
        repo_analytics.log_event(conn, tz, owner_tg_id, "deeplink.route_open", _deeplink_payload(meta, "auto"))
        _open_auto_menu(chat_id, owner_tg_id, message_id=None, meta=meta)

    def _open_route_student(chat_id: int, owner_tg_id: int, meta: dict | None = None) -> None:
        repo_analytics.log_event(conn, tz, owner_tg_id, "deeplink.route_open", _deeplink_payload(meta, "student"))
        if _has_student_access(owner_tg_id):
            send_user_home(chat_id, owner_tg_id)
            return

        repo_states.set_state(conn, owner_tg_id, STATE_STUDENT_CODE, {}, tz)
        bot.send_message(
            chat_id,
            "Введи одноразовый код ученика от ментора.\n"
            "Пример: <code>STU-AB12CD34</code>",
            reply_markup=_v2_back_kb(),
        )

    def _open_route_enroll(chat_id: int, owner_tg_id: int, meta: dict | None = None) -> None:
        repo_analytics.log_event(conn, tz, owner_tg_id, "deeplink.route_open", _deeplink_payload(meta, "enroll"))
        service = (meta or {}).get("service")
        plan_token = str((meta or {}).get("plan") or "").strip().lower()
        track_key = _service_token_to_track(service)
        if not track_key:
            _open_enroll_level(chat_id, owner_tg_id, message_id=None)
            return
        track = ENROLL_TRACKS.get(track_key) or {}
        if track.get("level") == "not_zero":
            # not_zero route can come from two tracks; open specific list directly
            _render_enroll_plans(chat_id, owner_tg_id, track_key, message_id=None)
        else:
            _render_enroll_plans(chat_id, owner_tg_id, track_key, message_id=None)
        if plan_token:
            for p in track.get("plans") or []:
                if str(p.get("key") or "").startswith(plan_token[:4]):
                    _render_enroll_confirm(chat_id, owner_tg_id, track_key, p["key"], message_id=None)
                    break

    public_routes = ctx.setdefault("public_routes", {})
    if isinstance(public_routes, dict):
        public_routes["home"] = _open_route_home
        public_routes["auto"] = _open_route_auto
        public_routes["student"] = _open_route_student
        public_routes["enroll"] = _open_route_enroll

    # =========================
    # v2 start menu
    # =========================

    @bot.callback_query_handler(func=lambda c: c.data == "v2:home")
    def cb_v2_home(c: CallbackQuery):
        if is_admin(c.from_user.id):
            bot.answer_callback_query(c.id, "Это пользовательское меню")
            return
        user_id = int(c.from_user.id)
        repo_states.clear_state(conn, user_id, tz)
        if _has_student_access(user_id):
            repo_analytics.log_event(conn, tz, user_id, "menu.student_home")
            repo_analytics.touch_user(conn, tz, user_id, role_type=repo_analytics.ROLE_STUDENT)
            bot.edit_message_text(
                texts.user_title(),
                c.message.chat.id,
                c.message.message_id,
                reply_markup=user_menu_kb(show_pay=_user_show_pay_button(user_id)),
            )
        else:
            repo_analytics.log_event(conn, tz, user_id, "menu.regular_home")
            repo_analytics.touch_user(conn, tz, user_id, role_type=repo_analytics.ROLE_REGULAR)
            bot.edit_message_text(
                texts.regular_title(),
                c.message.chat.id,
                c.message.message_id,
                reply_markup=regular_menu_kb(),
            )
        bot.answer_callback_query(c.id)

    @bot.callback_query_handler(func=lambda c: c.data == "v2:auto")
    def cb_v2_auto(c: CallbackQuery):
        if is_admin(c.from_user.id):
            bot.answer_callback_query(c.id, "Это пользовательское меню")
            return
        repo_analytics.log_event(conn, tz, c.from_user.id, "feature.auto_menu_open")
        _open_auto_menu(c.message.chat.id, int(c.from_user.id), message_id=c.message.message_id, meta=None)
        bot.answer_callback_query(c.id)

    @bot.callback_query_handler(func=lambda c: c.data == "v2:jetbrains")
    def cb_v2_jetbrains(c: CallbackQuery):
        if is_admin(c.from_user.id):
            bot.answer_callback_query(c.id, "Это пользовательское меню")
            return
        repo_analytics.log_event(conn, tz, c.from_user.id, "feature.jetbrains_license_open")
        bot.edit_message_text(
            "<b>Лицензия Jetbrains</b>\n"
            "Если ты прошёл гайд по установке, бесплатный ключ можно получить по кнопке ниже.",
            c.message.chat.id,
            c.message.message_id,
            reply_markup=_jetbrains_kb(),
        )
        bot.answer_callback_query(c.id)

    @bot.callback_query_handler(func=lambda c: c.data == "auto:disabled")
    def cb_auto_disabled(c: CallbackQuery):
        if is_admin(c.from_user.id):
            bot.answer_callback_query(c.id, "Это пользовательское меню")
            return
        repo_states.clear_state(conn, c.from_user.id, tz)
        bot.answer_callback_query(c.id, _auto_unavailable_reason())
        _safe_edit(c.message.chat.id, c.message.message_id, _auto_temporarily_unavailable_text(), _auto_menu_kb())

    @bot.callback_query_handler(func=lambda c: c.data == "v2:enroll")
    def cb_v2_enroll(c: CallbackQuery):
        if is_admin(c.from_user.id):
            bot.answer_callback_query(c.id, "Это пользовательское меню")
            return
        repo_analytics.log_event(conn, tz, c.from_user.id, "funnel.enroll.entry_click")
        _open_enroll_level(c.message.chat.id, int(c.from_user.id), message_id=c.message.message_id)
        bot.answer_callback_query(c.id)

    @bot.callback_query_handler(func=lambda c: c.data == "auto:start")
    def cb_auto_start(c: CallbackQuery):
        if is_admin(c.from_user.id):
            bot.answer_callback_query(c.id, "Это пользовательское меню")
            return
        if _is_auto_temporarily_unavailable():
            repo_states.clear_state(conn, c.from_user.id, tz)
            bot.answer_callback_query(c.id, _auto_unavailable_reason())
            _safe_edit(c.message.chat.id, c.message.message_id, _auto_temporarily_unavailable_text(), _auto_menu_kb())
            return
        owner_id = int(c.from_user.id)
        st, data = repo_states.get_state(conn, owner_id)
        auto = (data.get("auto") if st == STATE_AUTOAPPLY else {}) or {}
        saved_login, saved_password = _student_hh_credentials(owner_id)
        if saved_login:
            auto.setdefault("login", saved_login)
        if saved_password:
            auto.setdefault("password", saved_password)
        auto.setdefault("target_applies", 200)
        auto.setdefault("target_locked", False)
        auto.setdefault("target_site_clicks", 0)
        locked_target = _auto_locked_target(auto)
        if locked_target > 0:
            auto["target_applies"] = locked_target
            auto["target_locked"] = True
            auto["target_site_clicks"] = locked_target
        auto["price_total"] = _auto_price_for_user(owner_id, int(auto.get("target_applies") or 0))

        if _has_student_access(owner_id):
            login_now = str(auto.get("login") or "").strip()
            password_now = str(auto.get("password") or "").strip()
            if not login_now or not password_now:
                if login_now and not password_now:
                    repo_states.set_state(
                        conn,
                        owner_id,
                        STATE_AUTOAPPLY,
                        {"step": "student_hh_password", "auto": auto},
                        tz,
                    )
                    bot.send_message(
                        c.message.chat.id,
                        "<b>Первый запуск автооткликов</b>\n"
                        f"Логин HH найден: <code>{html.escape(login_now)}</code>\n"
                        "Введи пароль HH (сохраним и больше не будем спрашивать):",
                        reply_markup=_v2_back_kb(),
                    )
                else:
                    repo_states.set_state(
                        conn,
                        owner_id,
                        STATE_AUTOAPPLY,
                        {"step": "student_hh_login", "auto": auto},
                        tz,
                    )
                    bot.send_message(
                        c.message.chat.id,
                        "<b>Первый запуск автооткликов</b>\n"
                        "Для учеников нужно один раз заполнить доступы HH.\n"
                        "Введи логин HH (почта или телефон):",
                        reply_markup=_v2_back_kb(),
                    )
                bot.answer_callback_query(c.id)
                return

        repo_states.set_state(conn, owner_id, STATE_AUTOAPPLY, {"step": "direction", "auto": auto}, tz)
        bot.edit_message_text(
            "<b>Автоотклики</b>\nВыбери направление поиска:",
            c.message.chat.id,
            c.message.message_id,
            reply_markup=_auto_direction_kb(),
        )
        bot.answer_callback_query(c.id)

    @bot.callback_query_handler(func=lambda c: c.data.startswith("auto:dir:"))
    def cb_auto_dir(c: CallbackQuery):
        if is_admin(c.from_user.id):
            bot.answer_callback_query(c.id, "Это пользовательское меню")
            return
        if _is_auto_temporarily_unavailable():
            repo_states.clear_state(conn, c.from_user.id, tz)
            bot.answer_callback_query(c.id, _auto_unavailable_reason())
            _safe_edit(c.message.chat.id, c.message.message_id, _auto_temporarily_unavailable_text(), _auto_menu_kb())
            return
        direction = c.data.split(":")[2].strip().lower()
        st, data = repo_states.get_state(conn, c.from_user.id)
        auto = (data.get("auto") if st == STATE_AUTOAPPLY else {}) or {}
        if direction == "manual":
            repo_states.set_state(conn, c.from_user.id, STATE_AUTOAPPLY, {"step": "direction_manual", "auto": auto}, tz)
            bot.send_message(
                c.message.chat.id,
                "Напиши направление вручную.\nПримеры: qa, devops, analyst, product manager, 1c, mobile.",
            )
            bot.answer_callback_query(c.id)
            return
        if direction not in AUTO_DIRECTIONS:
            direction = direction or "software"
        auto["direction"] = direction
        locked_target = _auto_locked_target(auto)
        if locked_target > 0:
            auto["target_applies"] = locked_target
            auto["target_locked"] = True
            auto["target_site_clicks"] = locked_target
        owner_id = int(c.from_user.id)
        saved_login = str(auto.get("login") or "").strip()
        saved_password = str(auto.get("password") or "").strip()
        if _has_student_access(owner_id):
            if not saved_login or not saved_password:
                login_from_card, password_from_card = _student_hh_credentials(owner_id)
                if login_from_card and not saved_login:
                    saved_login = login_from_card
                    auto["login"] = login_from_card
                if password_from_card and not saved_password:
                    saved_password = password_from_card
                    auto["password"] = password_from_card
            if saved_login and saved_password:
                _auto_continue_after_auth(owner_id, c.message.chat.id, auto)
            else:
                next_step = "student_hh_login"
                prompt = (
                    "<b>Для запуска автооткликов нужен HH-доступ</b>\n"
                    "Введи логин HH (почта или телефон):"
                )
                if saved_login:
                    auto["login"] = saved_login
                    next_step = "student_hh_password"
                    prompt = (
                        "<b>Для запуска автооткликов нужен HH-доступ</b>\n"
                        f"Логин HH: <code>{html.escape(saved_login)}</code>\n"
                        "Введи пароль HH:"
                    )
                repo_states.set_state(conn, owner_id, STATE_AUTOAPPLY, {"step": next_step, "auto": auto}, tz)
                bot.send_message(c.message.chat.id, prompt, reply_markup=_v2_back_kb())
            bot.answer_callback_query(c.id)
            return
        if saved_login and saved_password:
            repo_states.set_state(conn, c.from_user.id, STATE_AUTOAPPLY, {"step": "login_pick", "auto": auto}, tz)
            bot.send_message(
                c.message.chat.id,
                "Нашёл сохранённые доступы HH. Использовать их или ввести новые?",
                reply_markup=_auto_login_pick_kb(saved_login),
            )
        else:
            repo_states.set_state(conn, c.from_user.id, STATE_AUTOAPPLY, {"step": "login", "auto": auto}, tz)
            bot.send_message(c.message.chat.id, "Логин HH (почта/телефон):")
        bot.answer_callback_query(c.id)

    @bot.callback_query_handler(func=lambda c: c.data in {"auto:login:saved", "auto:login:manual"})
    def cb_auto_login_pick(c: CallbackQuery):
        if is_admin(c.from_user.id):
            bot.answer_callback_query(c.id, "Это пользовательское меню")
            return
        if _is_auto_temporarily_unavailable():
            repo_states.clear_state(conn, c.from_user.id, tz)
            bot.answer_callback_query(c.id, _auto_unavailable_reason())
            _safe_edit(c.message.chat.id, c.message.message_id, _auto_temporarily_unavailable_text(), _auto_menu_kb())
            return
        st, data = repo_states.get_state(conn, c.from_user.id)
        if st != STATE_AUTOAPPLY or (data or {}).get("step") != "login_pick":
            bot.answer_callback_query(c.id)
            return
        auto = (data.get("auto") or {}).copy()
        if c.data.endswith("manual"):
            auto["login"] = ""
            auto["password"] = ""
            repo_states.set_state(conn, c.from_user.id, STATE_AUTOAPPLY, {"step": "login", "auto": auto}, tz)
            bot.send_message(c.message.chat.id, "Введи HH логин (почта/телефон):")
            bot.answer_callback_query(c.id)
            return
        _auto_continue_after_auth(int(c.from_user.id), c.message.chat.id, auto)
        bot.answer_callback_query(c.id)

    @bot.callback_query_handler(func=lambda c: c.data in {"auto:query:default", "auto:query:skip"})
    def cb_auto_query_pick(c: CallbackQuery):
        if is_admin(c.from_user.id):
            bot.answer_callback_query(c.id, "Это пользовательское меню")
            return
        if _is_auto_temporarily_unavailable():
            repo_states.clear_state(conn, c.from_user.id, tz)
            bot.answer_callback_query(c.id, _auto_unavailable_reason())
            _safe_edit(c.message.chat.id, c.message.message_id, _auto_temporarily_unavailable_text(), _auto_menu_kb())
            return
        st, data = repo_states.get_state(conn, c.from_user.id)
        if st != STATE_AUTOAPPLY or (data or {}).get("step") != "query":
            bot.answer_callback_query(c.id)
            return
        auto = (data.get("auto") or {}).copy()
        if c.data.endswith("default"):
            auto["query_text"] = str(auto.get("direction") or "Java")
        else:
            auto["query_text"] = ""
        repo_states.set_state(conn, c.from_user.id, STATE_AUTOAPPLY, {"step": "confirm", "auto": auto}, tz)
        summary = _auto_confirm_text(int(c.from_user.id), auto)
        bot.send_message(c.message.chat.id, summary, reply_markup=_auto_confirm_kb())
        bot.answer_callback_query(c.id)

    @bot.callback_query_handler(func=lambda c: c.data == "auto:last")
    def cb_auto_last(c: CallbackQuery):
        if is_admin(c.from_user.id):
            bot.answer_callback_query(c.id, "Это пользовательское меню")
            return
        if _is_auto_temporarily_unavailable():
            repo_states.clear_state(conn, c.from_user.id, tz)
            bot.answer_callback_query(c.id, _auto_unavailable_reason())
            _safe_edit(c.message.chat.id, c.message.message_id, _auto_temporarily_unavailable_text(), _auto_menu_kb())
            return
        row = repo_autoapply_runs.last_by_owner(conn, int(c.from_user.id))
        if not row:
            bot.edit_message_text(
                "<b>Автоотклики</b>\nПока запусков не было.",
                c.message.chat.id,
                c.message.message_id,
                reply_markup=_auto_menu_kb(),
            )
            bot.answer_callback_query(c.id)
            return
        owner_id = int(c.from_user.id)
        summary = repo_autoapply_runs.summary_by_owner(conn, owner_id)
        create_snapshot = repo_autoapply_runs.parse_response_json(row["create_response_json"])
        run_snapshot = repo_autoapply_runs.parse_response_json(row["run_response_json"])
        snapshot: dict = {}
        if isinstance(create_snapshot, dict):
            snapshot.update(create_snapshot)
        if isinstance(run_snapshot, dict):
            snapshot.update(run_snapshot)

        live_error = ""
        login = str(row["login"] or "").strip()
        if auto_client.enabled and login:
            try:
                live_snapshot = auto_client.account_stats(login)
                if isinstance(live_snapshot, dict):
                    snapshot.update(live_snapshot)
            except AutoApplyClientError as exc:
                live_error = str(exc)

        service_state = _pick_text(snapshot, ("status", "state", "runStatus", "accountStatus"))
        if not service_state:
            running = _pick_bool(snapshot, ("running", "isRunning", "active", "inProgress", "runInProgress"))
            if running is True:
                service_state = "running"
            elif running is False:
                service_state = "stopped"

        sent = _pick_int(
            snapshot,
            ("sent", "applied", "appliesSent", "success", "successCount", "sentCount", "responsesSent"),
        )
        failed = _pick_int(snapshot, ("failed", "errors", "errorCount", "failedCount", "sendFailed"))
        found = _pick_int(snapshot, ("found", "foundCount", "vacanciesFound", "candidatesFound"))
        target = int(row["target_applies"] or 0)

        lines = [
            "<b>Статус автооткликов</b>",
            f"Статус запуска: <b>{_auto_status_label(str(row['status'] or ''))}</b>",
            f"Логин HH: <code>{row['login']}</code>",
            f"Направление: <b>{row['direction']}</b>",
            f"Цель: <b>{target}</b> откликов",
            f"Последний запуск: {row['created_at']}",
        ]
        if service_state:
            lines.append(f"Состояние в сервисе: <b>{html.escape(service_state)}</b>")
        if sent is not None:
            if target > 0:
                progress_pct = min(max(int(round((sent / max(target, 1)) * 100)), 0), 100)
                lines.append(f"Отправлено: <b>{sent}</b> / {target} ({progress_pct}%)")
            else:
                lines.append(f"Отправлено: <b>{sent}</b>")
        if failed is not None:
            lines.append(f"Ошибок отправки: <b>{failed}</b>")
        if found is not None:
            lines.append(f"Найдено подходящих: <b>{found}</b>")
        if summary["total"] > 1:
            lines.append(
                f"Запусков всего: <b>{summary['total']}</b> "
                f"(успешно: {summary['launched']}, ошибок: {summary['failed']})"
            )
        if row["error_text"]:
            lines.append(f"Ошибка последней попытки: <code>{html.escape(str(row['error_text'])[:300])}</code>")
        if live_error:
            lines.append(f"Онлайн-статус сервиса: <code>{html.escape(live_error[:220])}</code>")
        text = "\n".join(lines)
        bot.edit_message_text(text, c.message.chat.id, c.message.message_id, reply_markup=_auto_menu_kb())
        bot.answer_callback_query(c.id)

    def _launch_autoapply(owner_id: int, chat_id: int, owner_username: str | None, auto: dict) -> tuple[bool, str]:
        login = str(auto.get("login") or "").strip()
        password = str(auto.get("password") or "").strip()
        direction = str(auto.get("direction") or "").strip().lower()
        target = max(int(auto.get("target_applies") or 0), 1)
        locked_target = _auto_locked_target(auto)
        if locked_target > 0:
            target = locked_target
        query_text = str(auto.get("query_text") or direction).strip()
        if not login or not password or not direction:
            return False, "Не хватает данных для запуска"

        price_info = _auto_price_breakdown(owner_id, target)
        candidate_context = _build_candidate_context(owner_id, direction)
        cover_letter = _auto_cover_letter(direction)
        payload = {
            "login": login,
            "password": password,
            "direction": direction,
            "headless": bool(cfg.AUTOAPPLY_DEFAULT_HEADLESS),
            "slowMoMs": int(cfg.AUTOAPPLY_DEFAULT_SLOWMO_MS),
            "queryText": query_text or direction,
            "targetApplies": target,
            "active": True,
        }
        if cover_letter.strip():
            payload["coverLetter"] = cover_letter
        if candidate_context:
            payload["candidateContext"] = candidate_context
        create_response: dict = {}
        run_response: dict = {}

        def _ensure_run_ok(response: dict) -> None:
            if not isinstance(response, dict):
                return
            if response.get("ok") is False:
                details = str(response.get("details") or response.get("message") or "ok=false").strip()
                sent = response.get("sent")
                if isinstance(sent, int):
                    details = f"{details}; sent={sent}"
                raise AutoApplyClientError(
                    "Сервис автооткликов не подтвердил запуск",
                    body=details[:300],
                )

        def _run_login_candidates(raw_login: str) -> list[str]:
            seen: set[str] = set()
            out: list[str] = []

            def _add(value: str | None) -> None:
                val = str(value or "").strip()
                if val and val not in seen:
                    seen.add(val)
                    out.append(val)

            _add(raw_login)
            if "@" not in raw_login:
                digits = "".join(ch for ch in raw_login if ch.isdigit())
                _add(digits)
                if len(digits) == 11:
                    if digits.startswith("8"):
                        _add("7" + digits[1:])
                        _add("+7" + digits[1:])
                    elif digits.startswith("7"):
                        _add("8" + digits[1:])
                        _add("+8" + digits[1:])
                    _add("+" + digits)
                elif len(digits) == 10:
                    _add("7" + digits)
                    _add("8" + digits)
                    _add("+7" + digits)
                    _add("+8" + digits)
                elif digits and not raw_login.startswith("+"):
                    _add("+" + digits)
            return out

        def _attempt_code(exc: AutoApplyClientError) -> str:
            if exc.status_code is not None:
                return f"HTTP {exc.status_code}"
            body = str(getattr(exc, "body", "") or "").strip().lower()
            if "auth_failed" in body or "auth failed" in body:
                return "AUTH_FAILED"
            msg = str(exc.message or "").strip().lower()
            if "связаться" in msg or "подключ" in msg:
                return "NETWORK"
            return "ERR"

        def _is_auth_failed(exc: AutoApplyClientError | None) -> bool:
            if exc is None:
                return False
            blob = " ".join(
                [
                    str(getattr(exc, "message", "") or ""),
                    str(getattr(exc, "body", "") or ""),
                    str(exc),
                ]
            ).lower()
            return "auth_failed" in blob or "auth failed" in blob or "authorization failed" in blob

        try:
            if not auto_client.enabled:
                raise AutoApplyClientError("Интеграция автооткликов выключена в конфиге")
            try:
                auto_client.check_health()
            except AutoApplyClientError as exc:
                # Some older builds may not expose actuator; do not block flow on 404/405.
                if exc.status_code not in {404, 405}:
                    raise AutoApplyClientError(
                        "Сервис автооткликов недоступен",
                        status_code=exc.status_code,
                        body=(exc.body or str(exc))[:220],
                    ) from exc
            login_candidates = _run_login_candidates(login)
            service_login = login
            run_started = False
            run_error: AutoApplyClientError | None = None
            run_attempts: list[str] = []

            # First try to run an already existing account to avoid create-side 500 on duplicates.
            for idx, run_login in enumerate(login_candidates):
                try:
                    run_response = auto_client.run_account(run_login)
                    _ensure_run_ok(run_response)
                    service_login = run_login
                    run_started = True
                    create_response = {"warning": "run_without_create_existing_account"}
                    break
                except AutoApplyClientError as exc:
                    run_error = exc
                    run_attempts.append(f"{run_login}/run:{_attempt_code(exc)}")
                    # Try all login variants first (+7/7/8/etc). A first 404 can still
                    # be followed by a successful run on another normalized login.
                    if idx < len(login_candidates) - 1 and exc.status_code in {400, 404, 422, 500}:
                        continue
                    # do not raise here: create+run fallback can still recover
                    break

            if not run_started:
                create_ok = False
                create_error: AutoApplyClientError | None = None
                create_attempts: list[str] = []
                create_fatal = False
                base_minimal = {
                    "password": password,
                    "direction": direction,
                    "queryText": query_text or direction,
                    "targetApplies": target,
                    "active": True,
                }
                base_core = {
                    "password": password,
                    "direction": direction,
                    "active": True,
                }
                for idx, candidate_login in enumerate(login_candidates):
                    candidate_payload_full = payload.copy()
                    candidate_payload_full["login"] = candidate_login
                    candidate_payload_min = base_minimal.copy()
                    candidate_payload_min["login"] = candidate_login
                    candidate_payload_core = base_core.copy()
                    candidate_payload_core["login"] = candidate_login
                    payload_variants = [
                        ("full", candidate_payload_full),
                        ("minimal", candidate_payload_min),
                        ("core", candidate_payload_core),
                    ]
                    for v_idx, (variant_name, candidate_payload) in enumerate(payload_variants):
                        try:
                            create_response = auto_client.create_account(candidate_payload)
                            service_login = candidate_login
                            create_ok = True
                            break
                        except AutoApplyClientError as exc:
                            msg = str(exc).lower()
                            if exc.status_code == 409 or "already" in msg or "существ" in msg:
                                create_response = {"warning": str(exc)}
                                service_login = candidate_login
                                create_ok = True
                                break
                            create_error = exc
                            create_attempts.append(f"{candidate_login}/{variant_name}:{_attempt_code(exc)}")
                            if int(exc.status_code or 0) >= 500:
                                # Some service builds return HTTP 500 on duplicate create instead of 409.
                                # If account is visible via /stats, continue flow as "create recovered".
                                try:
                                    auto_client.account_stats(candidate_login)
                                    create_attempts.append(f"{candidate_login}/{variant_name}:EXISTS_AFTER_500")
                                    create_response = {
                                        "warning": "create_500_existing_account",
                                        "create_error": str(exc)[:220],
                                    }
                                    service_login = candidate_login
                                    create_ok = True
                                    break
                                except AutoApplyClientError as stats_exc:
                                    create_attempts.append(f"{candidate_login}/{variant_name}:stats:{_attempt_code(stats_exc)}")
                            can_retry_variant = v_idx < len(payload_variants) - 1
                            can_retry_login = idx < len(login_candidates) - 1
                            if (can_retry_variant or can_retry_login) and exc.status_code in {400, 404, 422, 500}:
                                continue
                            create_fatal = True
                            break
                    if create_ok or create_fatal:
                        break
                if not create_ok:
                    # Recovery path: some backends return 500 in create, but account is still persisted.
                    # Try run one more time before failing the flow.
                    if create_error is not None and int(create_error.status_code or 0) >= 500:
                        recovery_candidates: list[str] = []
                        for candidate in [service_login, *login_candidates]:
                            if candidate and candidate not in recovery_candidates:
                                recovery_candidates.append(candidate)
                        for idx, run_login in enumerate(recovery_candidates):
                            try:
                                run_response = auto_client.run_account(run_login)
                                _ensure_run_ok(run_response)
                                service_login = run_login
                                run_started = True
                                create_response = {
                                    "warning": "create_failed_run_recovered",
                                    "create_error": str(create_error)[:220],
                                }
                                run_attempts.append(f"{run_login}/recovery:OK")
                                break
                            except AutoApplyClientError as exc:
                                run_error = exc
                                run_attempts.append(f"{run_login}/recovery:{_attempt_code(exc)}")
                                if idx < len(recovery_candidates) - 1 and exc.status_code in {400, 404, 422, 500}:
                                    continue
                                break
                    # Strong recovery: force recreate with minimal "core" payload.
                    if not run_started and create_error is not None and int(create_error.status_code or 0) >= 500:
                        for idx, candidate_login in enumerate(login_candidates):
                            try:
                                auto_client.delete_account(candidate_login)
                                create_attempts.append(f"{candidate_login}/delete:OK")
                            except AutoApplyClientError as exc:
                                create_attempts.append(f"{candidate_login}/delete:{_attempt_code(exc)}")
                                if exc.status_code not in {400, 404, 422, 500}:
                                    continue

                            recreate_payload = {
                                "login": candidate_login,
                                "password": password,
                                "direction": direction,
                                "active": True,
                            }
                            recreated = False
                            try:
                                create_response = auto_client.create_account(recreate_payload)
                                create_attempts.append(f"{candidate_login}/recreate:OK")
                                recreated = True
                            except AutoApplyClientError as exc:
                                msg = str(exc).lower()
                                create_attempts.append(f"{candidate_login}/recreate:{_attempt_code(exc)}")
                                if exc.status_code == 409 or "already" in msg or "существ" in msg:
                                    create_response = {"warning": str(exc)}
                                    recreated = True
                                elif idx < len(login_candidates) - 1 and exc.status_code in {400, 404, 422, 500}:
                                    continue
                                else:
                                    continue

                            if not recreated:
                                continue
                            try:
                                run_response = auto_client.run_account(candidate_login)
                                _ensure_run_ok(run_response)
                                service_login = candidate_login
                                run_started = True
                                create_response = {
                                    "warning": "create_500_recreate_recovered",
                                    "create_error": str(create_error)[:220],
                                }
                                run_attempts.append(f"{candidate_login}/recreate_run:OK")
                                break
                            except AutoApplyClientError as exc:
                                run_error = exc
                                run_attempts.append(f"{candidate_login}/recreate_run:{_attempt_code(exc)}")
                                if idx < len(login_candidates) - 1 and exc.status_code in {400, 404, 422, 500}:
                                    continue
                                continue
                    if run_started:
                        pass
                    elif run_error is not None:
                        attempt_trace = "; ".join(create_attempts + run_attempts)[:500]
                        raise AutoApplyClientError(
                            "Сервис автооткликов не подтвердил запуск",
                            status_code=run_error.status_code,
                            body=attempt_trace or (run_error.body or str(run_error))[:300],
                        ) from run_error
                    elif create_error is not None:
                        attempt_trace = "; ".join(create_attempts + run_attempts)[:500]
                        if attempt_trace:
                            raise AutoApplyClientError(
                                "Сервис автооткликов не подтвердил создание аккаунта",
                                status_code=create_error.status_code,
                                body=attempt_trace,
                            ) from create_error
                        raise create_error
                    else:
                        raise AutoApplyClientError("Сервис автооткликов не подтвердил создание аккаунта")
                if run_started:
                    pass
                else:
                    run_started = False
                    run_error = None
                    run_candidates: list[str] = []
                    for candidate in [service_login, *login_candidates]:
                        if candidate and candidate not in run_candidates:
                            run_candidates.append(candidate)
                    for idx, run_login in enumerate(run_candidates):
                        try:
                            run_response = auto_client.run_account(run_login)
                            _ensure_run_ok(run_response)
                            service_login = run_login
                            run_started = True
                            break
                        except AutoApplyClientError as exc:
                            run_error = exc
                            run_attempts.append(f"{run_login}:{_attempt_code(exc)}")
                            if idx < len(run_candidates) - 1 and exc.status_code in {400, 404, 422, 500}:
                                continue
                            break
                    if not run_started and _is_auth_failed(run_error):
                        for idx, candidate_login in enumerate(run_candidates):
                            try:
                                auto_client.delete_account(candidate_login)
                                create_attempts.append(f"{candidate_login}/auth_reset_delete:OK")
                            except AutoApplyClientError as exc:
                                create_attempts.append(f"{candidate_login}/auth_reset_delete:{_attempt_code(exc)}")
                                if exc.status_code not in {400, 404, 422, 500}:
                                    continue

                            reset_variants = [
                                (
                                    "full",
                                    {
                                        **payload,
                                        "login": candidate_login,
                                    },
                                ),
                                (
                                    "minimal",
                                    {
                                        "login": candidate_login,
                                        "password": password,
                                        "direction": direction,
                                        "queryText": query_text or direction,
                                        "targetApplies": target,
                                        "active": True,
                                    },
                                ),
                                (
                                    "core",
                                    {
                                        "login": candidate_login,
                                        "password": password,
                                        "direction": direction,
                                        "active": True,
                                    },
                                ),
                            ]
                            recreated = False
                            for variant_name, reset_payload in reset_variants:
                                try:
                                    create_response = auto_client.create_account(reset_payload)
                                    create_attempts.append(f"{candidate_login}/auth_reset_{variant_name}:OK")
                                    recreated = True
                                    break
                                except AutoApplyClientError as exc:
                                    create_attempts.append(f"{candidate_login}/auth_reset_{variant_name}:{_attempt_code(exc)}")
                                    msg = str(exc).lower()
                                    if exc.status_code == 409 or "already" in msg or "существ" in msg:
                                        create_response = {"warning": str(exc)}
                                        recreated = True
                                        break
                                    if exc.status_code in {400, 404, 422, 500}:
                                        continue
                                    break
                            if not recreated:
                                if idx < len(run_candidates) - 1:
                                    continue
                                break
                            try:
                                run_response = auto_client.run_account(candidate_login)
                                _ensure_run_ok(run_response)
                                service_login = candidate_login
                                run_started = True
                                run_attempts.append(f"{candidate_login}/auth_reset_run:OK")
                                create_response = {
                                    "warning": "auth_failed_recreated",
                                }
                                break
                            except AutoApplyClientError as exc:
                                run_error = exc
                                run_attempts.append(f"{candidate_login}/auth_reset_run:{_attempt_code(exc)}")
                                if idx < len(run_candidates) - 1 and exc.status_code in {400, 404, 422, 500}:
                                    continue
                                break
                    if not run_started:
                        if run_error is not None:
                            attempt_trace = "; ".join(run_attempts)[:300]
                            if attempt_trace:
                                raise AutoApplyClientError(
                                    "Сервис автооткликов не подтвердил запуск",
                                    status_code=run_error.status_code,
                                    body=attempt_trace,
                                ) from run_error
                            raise run_error
                        raise AutoApplyClientError("Сервис автооткликов не подтвердил запуск")
            repo_autoapply_runs.create(
                conn,
                tz,
                owner_tg_id=owner_id,
                owner_chat_id=chat_id,
                login=service_login,
                direction=direction,
                target_applies=target,
                query_text=query_text,
                status="launched",
                create_response=create_response,
                run_response=run_response,
                error_text=None,
            )
            _auto_mark_free50_used(owner_id, int(price_info.get("free_clicks") or 0))
            repo_states.clear_state(conn, owner_id, tz)
            bot.send_message(
                chat_id,
                "✅ Автоотклики запущены.\n"
                "Статус запуска сохранён.",
                reply_markup=_home_menu_kb(owner_id),
            )
            return True, ""
        except Exception as exc:
            err_text = str(exc)
            repo_autoapply_runs.create(
                conn,
                tz,
                owner_tg_id=owner_id,
                owner_chat_id=chat_id,
                login=login,
                direction=direction or "unknown",
                target_applies=target,
                query_text=query_text,
                status="failed",
                create_response=create_response,
                run_response=run_response,
                error_text=err_text,
            )
            req_payload = {
                "owner_chat_id": chat_id,
                "owner_username": owner_username,
                "login": login,
                "direction": direction,
                "target_applies": target,
                "query_text": query_text,
                "source": str(auto.get("source") or ""),
                "target_site_clicks": int(auto.get("target_site_clicks") or 0),
                "error": err_text,
            }
            req_id = repo_requests.create(conn, tz, owner_id, "AUTOAPPLY", req_payload, None)
            notify_admins(
                f"<b>[ЗАЯВКА]</b> {texts.request_title('AUTOAPPLY')} (ошибка)\n"
                f"От: @{owner_username or owner_id}\n"
                f"ID: {req_id}\n"
                f"Логин: <code>{login}</code>\n"
                f"Ошибка: <code>{err_text[:220]}</code>",
                kb=request_open_kb(req_id),
            )
            mentor_url = (cfg.MENTOR_CONTACT_URL or "").strip()
            err_blob = str(err_text or "").lower()
            auth_failed = "auth_failed" in err_blob or "auth failed" in err_blob or "authorization failed" in err_blob
            if auth_failed:
                user_text = (
                    "Не удалось запустить автоотклики: HH отклонил авторизацию.\n"
                    "Проверь логин/пароль HH и запусти ещё раз."
                )
                if mentor_url:
                    user_text += f"\nЕсли ошибка повторится, напиши ментору: {mentor_url}"
                bot.send_message(chat_id, user_text, reply_markup=_auto_menu_kb())
            elif mentor_url:
                bot.send_message(
                    chat_id,
                    "Не удалось автоматически запустить автоотклики.\n"
                    f"Напиши ментору: {mentor_url}",
                    reply_markup=_auto_menu_kb(),
                )
            else:
                bot.send_message(
                    chat_id,
                    "Не удалось автоматически запустить автоотклики.\n"
                    "Напиши ментору для ручного запуска.",
                    reply_markup=_auto_menu_kb(),
                )
            return False, err_text

    @bot.callback_query_handler(func=lambda c: c.data == "auto:run")
    def cb_auto_run(c: CallbackQuery):
        if is_admin(c.from_user.id):
            bot.answer_callback_query(c.id, "Это пользовательское меню")
            return
        if _is_auto_temporarily_unavailable():
            repo_states.clear_state(conn, c.from_user.id, tz)
            bot.answer_callback_query(c.id, _auto_unavailable_reason())
            _safe_edit(c.message.chat.id, c.message.message_id, _auto_temporarily_unavailable_text(), _auto_menu_kb())
            return
        owner_id = int(c.from_user.id)
        st, data = repo_states.get_state(conn, c.from_user.id)
        if st != STATE_AUTOAPPLY or (data or {}).get("step") != "confirm":
            bot.answer_callback_query(c.id, "Сначала заполни параметры")
            return
        auto = (data.get("auto") or {}).copy()
        login = str(auto.get("login") or "").strip()
        password = str(auto.get("password") or "").strip()
        direction = str(auto.get("direction") or "").strip().lower()
        target = max(int(auto.get("target_applies") or 0), 1)
        locked_target = _auto_locked_target(auto)
        if locked_target > 0:
            target = locked_target
            auto["target_applies"] = locked_target
            auto["target_locked"] = True
            auto["target_site_clicks"] = locked_target
        query_text = str(auto.get("query_text") or direction).strip()
        if not login or not password or not direction:
            bot.answer_callback_query(c.id, "Не хватает данных для запуска")
            return
        price_info = _auto_price_breakdown(owner_id, target)
        auto["price_total"] = int(price_info.get("total_price") or 0)
        auto["query_text"] = query_text
        if _auto_requires_site_payment(owner_id, auto):
            amount = int(auto.get("price_total") or 0)
            if not cardlink_client.enabled:
                repo_states.clear_state(conn, owner_id, tz)
                bot.send_message(
                    c.message.chat.id,
                    "Оплата автооткликов доступна только через кассу Cardlink.\n"
                    "Касса сейчас не настроена, попробуй позже.",
                    reply_markup=_auto_menu_kb(),
                )
                bot.answer_callback_query(c.id, "Касса недоступна")
                return
            checkout_row, err = _create_checkout_payment(
                owner_tg_id=owner_id,
                owner_chat_id=int(c.message.chat.id),
                owner_username=c.from_user.username,
                req_type="AUTOAPPLY_PAYMENT",
                service_key="autoapply",
                service_title="Автоотклики",
                amount=amount,
                purpose=f"Оплата автооткликов (TG {owner_id})",
                metadata={
                    "source": "autoapply_site",
                    "auto": auto,
                    "free_clicks": int(price_info.get("free_clicks") or 0),
                    "paid_clicks": int(price_info.get("paid_clicks") or 0),
                    "per_click": int(price_info.get("per_click") or 0),
                },
            )
            if not checkout_row:
                bot.send_message(c.message.chat.id, err or "Не удалось создать платёж в Cardlink. Попробуй позже.")
                bot.answer_callback_query(c.id, "Ошибка оплаты")
                return
            local_id = int(checkout_row["id"])
            repo_states.set_state(
                conn,
                owner_id,
                STATE_AUTOAPPLY,
                {"step": "pay_wait", "auto": auto, "checkout_payment_id": local_id},
                tz,
            )
            bot.send_message(
                c.message.chat.id,
                "<b>Оплата автооткликов</b>\n"
                f"Сумма: <b>{_format_money_value(amount)} ₽</b>\n"
                "Нажми «Оплатить», затем «Проверить оплату».",
                reply_markup=_checkout_wait_kb(
                    check_callback=f"auto:pay:check:{local_id}",
                    payment_url=str(checkout_row["confirmation_url"] or ""),
                ),
            )
            bot.answer_callback_query(c.id, "Ссылка на оплату готова")
            return

        try:
            bot.answer_callback_query(c.id, "Запускаю...")
        except Exception:
            pass
        _launch_autoapply(owner_id, int(c.message.chat.id), c.from_user.username, auto)

    @bot.callback_query_handler(func=lambda c: c.data.startswith("auto:pay:check:"))
    def cb_auto_pay_check(c: CallbackQuery):
        if is_admin(c.from_user.id):
            bot.answer_callback_query(c.id, "Это пользовательское меню")
            return
        owner_id = int(c.from_user.id)
        payment_row_id = int(c.data.split(":")[3])
        row = repo_checkout_payments.get(conn, payment_row_id)
        if not row or int(row["owner_tg_id"] or 0) != owner_id:
            bot.answer_callback_query(c.id, "Платёж не найден")
            return
        row, err = _refresh_checkout_payment(payment_row_id)
        if not row:
            bot.answer_callback_query(c.id, "Платёж не найден")
            return
        if err:
            bot.answer_callback_query(c.id, "Статус не обновлён")
            bot.send_message(c.message.chat.id, err)
            return
        status = str(row["status"] or "").strip().upper()
        if status in {repo_checkout_payments.STATUS_PENDING, repo_checkout_payments.STATUS_WAITING_CAPTURE}:
            bot.send_message(
                c.message.chat.id,
                "Платёж ещё не подтверждён в Cardlink. Если ты уже оплатил, попробуй ещё раз через 10-20 секунд.",
                reply_markup=_checkout_wait_kb(
                    check_callback=f"auto:pay:check:{payment_row_id}",
                    payment_url=str(row["confirmation_url"] or ""),
                ),
            )
            bot.answer_callback_query(c.id, "Пока ожидаем")
            return
        if status != repo_checkout_payments.STATUS_SUCCEEDED:
            bot.send_message(
                c.message.chat.id,
                "Платёж не завершён. Попробуй оплатить снова или напиши ментору.",
                reply_markup=_auto_menu_kb(),
            )
            bot.answer_callback_query(c.id, "Оплата не подтверждена")
            return
        if int(row["action_done"] or 0) == 1:
            bot.send_message(c.message.chat.id, "Оплата уже подтверждена. Автоотклики запущены или были запущены ранее.")
            bot.answer_callback_query(c.id, "Уже подтверждено")
            return

        st, data = repo_states.get_state(conn, owner_id)
        auto = {}
        if st == STATE_AUTOAPPLY and isinstance(data, dict):
            auto = (data.get("auto") or {}).copy()
        if not auto:
            auto = (repo_checkout_payments.metadata(row).get("auto") or {}).copy()
        if not auto:
            bot.send_message(
                c.message.chat.id,
                "Не удалось восстановить параметры запуска. Напиши ментору, запуск будет выполнен вручную.",
                reply_markup=_auto_menu_kb(),
            )
            bot.answer_callback_query(c.id, "Нужна помощь ментора")
            return

        ok, err_launch = _launch_autoapply(owner_id, int(c.message.chat.id), c.from_user.username, auto)
        if ok:
            repo_checkout_payments.mark_action_done(conn, tz, payment_row_id, note="autoapply_launched")
            notify_admins(
                f"<b>[ОПЛАТА]</b> Автоотклики оплачены через Cardlink\n"
                f"Пользователь: @{c.from_user.username or owner_id}\n"
                f"Сумма: {int(row['amount'] or 0)} ₽\n"
                f"Статус: подтверждено",
            )
            bot.answer_callback_query(c.id, "Оплата подтверждена")
            return
        bot.answer_callback_query(c.id, "Ошибка запуска")
        if err_launch:
            bot.send_message(c.message.chat.id, f"Ошибка запуска после оплаты: {html.escape(err_launch[:220])}")

    @bot.callback_query_handler(func=lambda c: c.data.startswith("enr:level:"))
    def cb_enroll_level(c: CallbackQuery):
        if is_admin(c.from_user.id):
            bot.answer_callback_query(c.id, "Это пользовательское меню")
            return
        level = c.data.split(":")[2]
        if level == "zero":
            _render_enroll_plans(c.message.chat.id, int(c.from_user.id), "zero-offer", message_id=c.message.message_id)
        else:
            _render_enroll_track(c.message.chat.id, int(c.from_user.id), message_id=c.message.message_id)
        bot.answer_callback_query(c.id)

    @bot.callback_query_handler(func=lambda c: c.data == "enr:back:level")
    def cb_enroll_back_level(c: CallbackQuery):
        if is_admin(c.from_user.id):
            bot.answer_callback_query(c.id, "Это пользовательское меню")
            return
        _open_enroll_level(c.message.chat.id, int(c.from_user.id), message_id=c.message.message_id)
        bot.answer_callback_query(c.id)

    @bot.callback_query_handler(func=lambda c: c.data == "enr:back:track")
    def cb_enroll_back_track(c: CallbackQuery):
        if is_admin(c.from_user.id):
            bot.answer_callback_query(c.id, "Это пользовательское меню")
            return
        _render_enroll_track(c.message.chat.id, int(c.from_user.id), message_id=c.message.message_id)
        bot.answer_callback_query(c.id)

    @bot.callback_query_handler(func=lambda c: c.data.startswith("enr:track:"))
    def cb_enroll_track(c: CallbackQuery):
        if is_admin(c.from_user.id):
            bot.answer_callback_query(c.id, "Это пользовательское меню")
            return
        alias = c.data.split(":")[2].strip().lower()
        track_key = ENROLL_TRACK_BY_ALIAS.get(alias)
        if not track_key:
            bot.answer_callback_query(c.id, "Неизвестное направление")
            return
        _render_enroll_plans(c.message.chat.id, int(c.from_user.id), track_key, message_id=c.message.message_id)
        bot.answer_callback_query(c.id)

    @bot.callback_query_handler(func=lambda c: c.data.startswith("enr:back:plans:"))
    def cb_enroll_back_plans(c: CallbackQuery):
        if is_admin(c.from_user.id):
            bot.answer_callback_query(c.id, "Это пользовательское меню")
            return
        alias = c.data.split(":")[3].strip().lower()
        track_key = ENROLL_TRACK_BY_ALIAS.get(alias)
        if not track_key:
            bot.answer_callback_query(c.id, "Не удалось открыть тарифы")
            return
        _render_enroll_plans(c.message.chat.id, int(c.from_user.id), track_key, message_id=c.message.message_id)
        bot.answer_callback_query(c.id)

    @bot.callback_query_handler(func=lambda c: c.data.startswith("enr:plan:"))
    def cb_enroll_plan(c: CallbackQuery):
        if is_admin(c.from_user.id):
            bot.answer_callback_query(c.id, "Это пользовательское меню")
            return
        parts = c.data.split(":")
        if len(parts) < 4:
            bot.answer_callback_query(c.id, "Ошибка данных")
            return
        alias = parts[2].strip().lower()
        plan_key = parts[3].strip().lower()
        track_key = ENROLL_TRACK_BY_ALIAS.get(alias)
        if not track_key:
            bot.answer_callback_query(c.id, "Неизвестный тариф")
            return
        _render_enroll_confirm(c.message.chat.id, int(c.from_user.id), track_key, plan_key, message_id=c.message.message_id)
        bot.answer_callback_query(c.id)

    @bot.callback_query_handler(func=lambda c: c.data == "enr:confirm")
    def cb_enroll_confirm(c: CallbackQuery):
        if is_admin(c.from_user.id):
            bot.answer_callback_query(c.id, "Это пользовательское меню")
            return
        st, data = repo_states.get_state(conn, c.from_user.id)
        if st != STATE_ENROLL or (data or {}).get("step") != "confirm":
            bot.answer_callback_query(c.id, "Сначала выбери тариф")
            return
        enroll = (data.get("enroll") or {}).copy()
        track_key = str(enroll.get("track_key") or "").strip()
        plan_key = str(enroll.get("plan_key") or "").strip()
        track = ENROLL_TRACKS.get(track_key)
        plan = _get_plan(track_key, plan_key)
        if not track or not plan:
            bot.answer_callback_query(c.id, "Тариф не найден")
            return

        contract_path = _enroll_contract_template_path(
            str(enroll.get("level") or track.get("level") or "").strip(),
            track_key,
        )
        _send_enroll_template(c.message.chat.id, track_key, track, plan, contract_path)
        repo_states.set_state(conn, c.from_user.id, STATE_ENROLL, {"step": "review_template", "enroll": enroll}, tz)
        bot.send_message(
            c.message.chat.id,
            "Сначала посмотри договор.\n"
            "Если всё устраивает, нажми «Согласен(на), создать на подпись».\n"
            "Если есть вопросы — напиши ментору.",
            reply_markup=_enroll_review_kb(track_key),
        )
        bot.answer_callback_query(c.id)

    @bot.callback_query_handler(func=lambda c: c.data == "enr:sign:create")
    def cb_enroll_sign_create(c: CallbackQuery):
        if is_admin(c.from_user.id):
            bot.answer_callback_query(c.id, "Это пользовательское меню")
            return
        st, data = repo_states.get_state(conn, c.from_user.id)
        if st != STATE_ENROLL or (data or {}).get("step") != "review_template":
            bot.answer_callback_query(c.id, "Сначала выбери тариф и посмотри договор")
            return

        enroll = (data.get("enroll") or {}).copy()
        track_key = str(enroll.get("track_key") or "").strip()
        plan_key = str(enroll.get("plan_key") or "").strip()
        track = ENROLL_TRACKS.get(track_key)
        plan = _get_plan(track_key, plan_key)
        if not track or not plan:
            bot.answer_callback_query(c.id, "Тариф не найден")
            return

        owner_id = int(c.from_user.id)
        if _handle_existing_enroll_contract(owner_id, c.message.chat.id, c.from_user.username):
            repo_states.clear_state(conn, owner_id, tz)
            bot.answer_callback_query(c.id, "Договор уже выдавался")
            return

        contract_path = _enroll_contract_template_path(
            str(enroll.get("level") or track.get("level") or "").strip(),
            track_key,
        )
        sign_url = (cfg.OKIDOKI_SIGN_URL or "").strip()
        okidoki_error = ""
        okidoki_payload: dict | None = None
        okidoki_response: dict | None = None
        if okidoki_client.enabled:
            raw_okidoki_payload = {
                "user_id": owner_id,
                "username": c.from_user.username,
                "chat_id": int(c.message.chat.id),
                "level": str(enroll.get("level") or track.get("level") or "not_zero"),
                "track_key": track_key,
                "track_title": track.get("title"),
                "tariff_key": plan_key,
                "tariff_title": plan.get("title"),
                "tariff_price": plan.get("price"),
            }
            try:
                okidoki_payload = okidoki_client.prepare_enroll_contract_payload(raw_okidoki_payload)
                okidoki_response = okidoki_client.create_contract(okidoki_payload)
                nested_contract = okidoki_response.get("contract") if isinstance(okidoki_response.get("contract"), dict) else {}
                dynamic_sign_url = str(
                    okidoki_response.get("sign_url")
                    or okidoki_response.get("signUrl")
                    or okidoki_response.get("url")
                    or okidoki_response.get("link")
                    or nested_contract.get("sign_url")
                    or nested_contract.get("signUrl")
                    or nested_contract.get("url")
                    or nested_contract.get("link")
                    or ""
                ).strip()
                if dynamic_sign_url:
                    sign_url = dynamic_sign_url
            except OkiDokiClientError as exc:
                okidoki_error = str(exc)

        if not sign_url:
            mentor_url = (cfg.MENTOR_CONTACT_URL or "").strip()
            fail_text = "Не удалось подготовить ссылку на подписание.\nНапиши ментору, чтобы он помог с договором."
            fail_kb = InlineKeyboardMarkup()
            if mentor_url:
                fail_kb.row(InlineKeyboardButton("Написать ментору", url=mentor_url))
            fail_kb.row(InlineKeyboardButton("⬅️ В меню", callback_data="v2:home"))
            bot.send_message(c.message.chat.id, fail_text, reply_markup=fail_kb)
            if okidoki_error:
                bot.send_message(
                    c.message.chat.id,
                    "OkiDoki вернул ошибку. Ментор проверит и поможет продолжить.",
                )
            bot.answer_callback_query(c.id, "Ссылка пока недоступна")
            return

        created_id = repo_enrollment_contracts.create_once(
            conn,
            tz,
            owner_tg_id=owner_id,
            owner_chat_id=int(c.message.chat.id),
            level_key=str(enroll.get("level") or track.get("level") or "not_zero"),
            track_key=track_key,
            track_title=str(track.get("title") or ""),
            tariff_key=plan_key,
            tariff_title=str(plan.get("title") or ""),
            tariff_price=str(plan.get("price") or ""),
            contract_file_path=contract_path,
            sign_url=sign_url,
            okidoki_request_json=okidoki_payload,
            okidoki_response_json=okidoki_response,
            status="sent",
            note="issued_by_bot_after_user_agree",
        )
        if created_id is None and _handle_existing_enroll_contract(owner_id, c.message.chat.id, c.from_user.username):
            repo_states.clear_state(conn, owner_id, tz)
            bot.answer_callback_query(c.id, "Договор уже выдавался")
            return

        sign_kb = InlineKeyboardMarkup()
        sign_kb.row(InlineKeyboardButton("✍️ Подписать договор", url=sign_url))
        sign_kb.row(InlineKeyboardButton("✅ Договор подписан, перейти к оплате", callback_data="enr:pay:start"))
        if (cfg.MENTOR_CONTACT_URL or "").strip():
            sign_kb.row(InlineKeyboardButton("Написать ментору", url=cfg.MENTOR_CONTACT_URL))
        bot.send_message(
            c.message.chat.id,
            "Ссылка на подписание подготовлена.\n"
            "Шаг 1: подпиши договор.\n"
            "Шаг 2: нажми «Договор подписан, перейти к оплате».",
            reply_markup=sign_kb,
        )
        repo_states.clear_state(conn, owner_id, tz)
        bot.answer_callback_query(c.id, "Ссылка подготовлена")

    @bot.callback_query_handler(func=lambda c: c.data == "enr:pay:start")
    def cb_enroll_pay_start(c: CallbackQuery):
        if is_admin(c.from_user.id):
            bot.answer_callback_query(c.id, "Это пользовательское меню")
            return
        owner_id = int(c.from_user.id)
        contract = repo_enrollment_contracts.get_by_owner(conn, owner_id)
        if not contract:
            bot.send_message(
                c.message.chat.id,
                "Не найден активный договор. Оформи тариф в «Вступить на обучение» и получи договор заново.",
                reply_markup=_home_menu_kb(owner_id),
            )
            bot.answer_callback_query(c.id, "Сначала оформи договор")
            return

        repo_enrollment_contracts.set_status(
            conn,
            tz,
            owner_id,
            status="signed",
            note="signed_confirmed_by_user_in_bot",
        )
        _start_enroll_payment_wizard(owner_id, c.message.chat.id, owner_username=c.from_user.username)
        bot.answer_callback_query(c.id)

    @bot.callback_query_handler(func=lambda c: c.data.startswith("enr:pay:check:"))
    def cb_enroll_pay_check(c: CallbackQuery):
        if is_admin(c.from_user.id):
            bot.answer_callback_query(c.id, "Это пользовательское меню")
            return
        owner_id = int(c.from_user.id)
        payment_row_id = int(c.data.split(":")[3])
        row = repo_checkout_payments.get(conn, payment_row_id)
        if not row or int(row["owner_tg_id"] or 0) != owner_id:
            bot.answer_callback_query(c.id, "Платёж не найден")
            return
        row, err = _refresh_checkout_payment(payment_row_id)
        if not row:
            bot.answer_callback_query(c.id, "Платёж не найден")
            return
        if err:
            bot.answer_callback_query(c.id, "Статус не обновлён")
            bot.send_message(c.message.chat.id, err)
            return
        status = str(row["status"] or "").strip().upper()
        if status in {repo_checkout_payments.STATUS_PENDING, repo_checkout_payments.STATUS_WAITING_CAPTURE}:
            bot.send_message(
                c.message.chat.id,
                "Платёж ещё не подтверждён в Cardlink. Если уже оплатил — попробуй ещё раз через 10-20 секунд.",
                reply_markup=_checkout_wait_kb(
                    check_callback=f"enr:pay:check:{payment_row_id}",
                    payment_url=str(row["confirmation_url"] or ""),
                ),
            )
            bot.answer_callback_query(c.id, "Пока ожидаем")
            return
        if status != repo_checkout_payments.STATUS_SUCCEEDED:
            bot.send_message(
                c.message.chat.id,
                "Платёж не завершён. Попробуй оплатить снова или напиши ментору.",
                reply_markup=_home_menu_kb(owner_id),
            )
            bot.answer_callback_query(c.id, "Оплата не подтверждена")
            return
        if int(row["action_done"] or 0) == 1:
            bot.send_message(c.message.chat.id, "Оплата по договору уже подтверждена ✅")
            bot.answer_callback_query(c.id, "Уже подтверждено")
            return

        ok, err_apply = _apply_enrollment_checkout(owner_id, int(row["amount"] or 0), payment_row_id)
        if not ok:
            bot.send_message(
                c.message.chat.id,
                f"Оплата прошла, но не удалось завершить шаг: {html.escape(err_apply)}\n"
                "Напиши ментору, он завершит вручную.",
            )
            bot.answer_callback_query(c.id, "Нужна помощь ментора")
            return

        repo_checkout_payments.mark_action_done(conn, tz, payment_row_id, note="enrollment_paid")
        repo_states.clear_state(conn, owner_id, tz)
        notify_admins(
            f"<b>[ОПЛАТА]</b> Договор оплачен через Cardlink\n"
            f"Пользователь: @{c.from_user.username or owner_id}\n"
            f"Сумма: {int(row['amount'] or 0)} ₽\n"
            "Статус: подтверждено",
        )
        bot.send_message(
            c.message.chat.id,
            "Оплата подтверждена ✅\n"
            "Договор отмечен как оплаченный. Дальше можешь перейти в раздел «Я ученик».",
            reply_markup=_home_menu_kb(owner_id),
        )
        bot.answer_callback_query(c.id, "Оплата подтверждена")

    @bot.callback_query_handler(func=lambda c: c.data == "v2:student")
    def cb_v2_student(c: CallbackQuery):
        if is_admin(c.from_user.id):
            bot.answer_callback_query(c.id, "Это пользовательское меню")
            return
        user_id = int(c.from_user.id)
        repo_analytics.log_event(conn, tz, user_id, "funnel.student.entry_click")
        if _has_student_access(user_id):
            repo_analytics.touch_user(conn, tz, user_id, role_type=repo_analytics.ROLE_STUDENT)
            repo_states.clear_state(conn, user_id, tz)
            bot.edit_message_text(
                texts.user_title(),
                c.message.chat.id,
                c.message.message_id,
                reply_markup=user_menu_kb(show_pay=_user_show_pay_button(user_id)),
            )
            bot.answer_callback_query(c.id)
            return

        repo_states.set_state(conn, user_id, STATE_STUDENT_CODE, {}, tz)
        bot.edit_message_text(
            "Введи одноразовый код ученика от ментора.\n"
            "Пример: <code>STU-AB12CD34</code>",
            c.message.chat.id,
            c.message.message_id,
            reply_markup=_v2_back_kb(),
        )
        bot.answer_callback_query(c.id)

    @bot.callback_query_handler(func=lambda c: c.data in {"u:profile:start:auto", "u:profile:start:manual"})
    def cb_profile_start_mode(c: CallbackQuery):
        if is_admin(c.from_user.id):
            bot.answer_callback_query(c.id, "Нет")
            return
        owner_id = int(c.from_user.id)
        if not _has_student_access(owner_id):
            repo_states.set_state(conn, owner_id, STATE_STUDENT_CODE, {}, tz)
            bot.send_message(
                c.message.chat.id,
                "Сначала активируй доступ: введи одноразовый код ученика.",
                reply_markup=_v2_back_kb(),
            )
            bot.answer_callback_query(c.id, "Сначала код")
            return
        mode = "auto" if c.data.endswith(":auto") else "manual"
        try:
            bot.edit_message_reply_markup(c.message.chat.id, c.message.message_id, reply_markup=None)
        except Exception:
            pass
        cur_st, cur_data = repo_states.get_state(conn, owner_id)
        if cur_st in {STATE_PROFILE, "u_profile_starting"} and (cur_data or {}).get("step"):
            bot.answer_callback_query(c.id, "Анкета уже запущена")
            return
        # Антидубль: серия быстрых тапов по кнопке режима не должна запускать
        # несколько одинаковых wizard-цепочек одновременно.
        repo_states.set_state(conn, owner_id, "u_profile_starting", {"step": "lock"}, tz)
        last = _last_request(owner_id, "PROFILE")
        base_prefill = _merge_profile_prefills(
            _prefill_profile_from_student(owner_id),
            repo_requests.payload(last) if last else {},
        )
        if base_prefill.get("username") and not str(base_prefill["username"]).startswith("@"):
            base_prefill["username"] = "@" + str(base_prefill["username"])
        if mode == "auto":
            repo_analytics.log_event(conn, tz, owner_id, "funnel.student.profile_mode_auto")
            auto_data = {
                "step": "contract_lookup",
                "profile": base_prefill,
                "mode": "auto",
            }
            repo_states.set_state(conn, owner_id, STATE_PROFILE, auto_data, tz)
            bot.send_message(
                c.message.chat.id,
                "Отправь ссылку на договор — из неё подтянутся данные анкеты, а недостающее я дозапрошу.",
            )
        else:
            repo_analytics.log_event(conn, tz, owner_id, "funnel.student.profile_mode_manual")
            prefill = base_prefill
            if prefill.get("username") and not str(prefill["username"]).startswith("@"):
                prefill["username"] = "@" + str(prefill["username"])
            _start_profile_wizard(
                owner_id,
                c.message.chat.id,
                prefill=prefill if prefill else None,
                owner_username=c.from_user.username,
            )
        bot.answer_callback_query(c.id)

    # =========================
    # User cabinet
    # =========================

    @bot.callback_query_handler(func=lambda c: c.data == "u:cancel")
    def cb_cancel(c: CallbackQuery):
        if is_admin(c.from_user.id):
            bot.answer_callback_query(c.id, "Нет")
            return
        if _has_student_access(int(c.from_user.id)):
            send_user_home(c.message.chat.id, c.from_user.id)
        else:
            send_regular_home(c.message.chat.id, c.from_user.id)
        bot.answer_callback_query(c.id)

    @bot.callback_query_handler(func=lambda c: c.data == "u:profile")
    def cb_profile(c: CallbackQuery):
        if is_admin(c.from_user.id):
            bot.answer_callback_query(c.id, "Это меню ученика")
            return
        if not _has_student_access(int(c.from_user.id)):
            _send_student_access_required(c.message.chat.id)
            bot.answer_callback_query(c.id, "Нужен код ученика")
            return

        owner_id = int(c.from_user.id)
        s = repo_students.get_by_owner(conn, owner_id)
        last = _last_request(owner_id, "PROFILE")
        status = (last["status"] if last else None) if not s else "APPROVED"

        msg = ""
        if status == "APPROVED":
            msg = "Анкета подтверждена ментором. Если нужно — можно обновить данные (уйдёт новая заявка на подтверждение)."
        elif status == "PENDING":
            msg = "Анкета отправлена и ждёт подтверждения ментора. Если ошибся — можешь отправить новую версию."
        elif status == "REJECTED":
            msg = "Анкета отклонена ментором. Исправь и отправь заново."
        else:
            msg = "Анкеты ещё нет. Заполни и отправь — она придёт ментору на подтверждение."

        kb = InlineKeyboardMarkup()
        kb.row(InlineKeyboardButton("✏️ Заполнить/изменить", callback_data="u:profile:edit"))
        kb.row(InlineKeyboardButton("🏠 В меню", callback_data="u:cancel"))
        bot.send_message(c.message.chat.id, msg, reply_markup=kb)
        bot.answer_callback_query(c.id)

    @bot.callback_query_handler(func=lambda c: c.data == "u:profile:edit")
    def cb_profile_edit(c: CallbackQuery):
        if is_admin(c.from_user.id):
            bot.answer_callback_query(c.id, "Это меню ученика")
            return
        if not _has_student_access(int(c.from_user.id)):
            _send_student_access_required(c.message.chat.id)
            bot.answer_callback_query(c.id, "Нужен код ученика")
            return
        repo_states.clear_state(conn, int(c.from_user.id), tz)
        bot.send_message(
            c.message.chat.id,
            "Выбери режим заполнения анкеты:",
            reply_markup=user_profile_start_mode_kb(),
        )
        bot.answer_callback_query(c.id)

    @bot.callback_query_handler(func=lambda c: c.data == "u:accounts")
    def cb_accounts(c: CallbackQuery):
        if is_admin(c.from_user.id):
            bot.answer_callback_query(c.id, "Это меню ученика")
            return
        if not _has_student_access(int(c.from_user.id)):
            _send_student_access_required(c.message.chat.id)
            bot.answer_callback_query(c.id, "Нужен код ученика")
            return
        _start_accounts_wizard(int(c.from_user.id), c.message.chat.id)
        bot.answer_callback_query(c.id)

    @bot.callback_query_handler(func=lambda c: c.data == "u:pay")
    def cb_pay(c: CallbackQuery):
        if is_admin(c.from_user.id):
            bot.answer_callback_query(c.id, "Это меню ученика")
            return
        if not _has_student_access(int(c.from_user.id)):
            _send_student_access_required(c.message.chat.id)
            bot.answer_callback_query(c.id, "Нужен код ученика")
            return
        _start_payment_wizard(int(c.from_user.id), c.message.chat.id, owner_username=c.from_user.username)
        bot.answer_callback_query(c.id)

    @bot.callback_query_handler(func=lambda c: c.data.startswith("u:pay:check:"))
    def cb_pay_check(c: CallbackQuery):
        if is_admin(c.from_user.id):
            bot.answer_callback_query(c.id, "Это меню ученика")
            return
        owner_id = int(c.from_user.id)
        payment_row_id = int(c.data.split(":")[3])
        row = repo_checkout_payments.get(conn, payment_row_id)
        if not row or int(row["owner_tg_id"] or 0) != owner_id:
            bot.answer_callback_query(c.id, "Платёж не найден")
            return
        row, err = _refresh_checkout_payment(payment_row_id)
        if not row:
            bot.answer_callback_query(c.id, "Платёж не найден")
            return
        if err:
            bot.answer_callback_query(c.id, "Статус не обновлён")
            bot.send_message(c.message.chat.id, err)
            return
        status = str(row["status"] or "").strip().upper()
        if status in {repo_checkout_payments.STATUS_PENDING, repo_checkout_payments.STATUS_WAITING_CAPTURE}:
            bot.send_message(
                c.message.chat.id,
                "Платёж ещё не подтверждён в Cardlink. Если уже оплатил — попробуй ещё раз через 10-20 секунд.",
                reply_markup=_checkout_wait_kb(
                    check_callback=f"u:pay:check:{payment_row_id}",
                    payment_url=str(row["confirmation_url"] or ""),
                    back_callback="u:cancel",
                ),
            )
            bot.answer_callback_query(c.id, "Пока ожидаем")
            return
        if status != repo_checkout_payments.STATUS_SUCCEEDED:
            bot.send_message(
                c.message.chat.id,
                "Платёж не завершён. Попробуй оплатить снова или напиши ментору.",
                reply_markup=user_menu_kb(show_pay=_user_show_pay_button(owner_id)),
            )
            bot.answer_callback_query(c.id, "Оплата не подтверждена")
            return
        if int(row["action_done"] or 0) == 1:
            bot.send_message(c.message.chat.id, "Оплата уже подтверждена ✅")
            bot.answer_callback_query(c.id, "Уже подтверждено")
            return

        ok, err_apply = _apply_postpay_checkout(owner_id, int(row["amount"] or 0), payment_row_id)
        if not ok:
            bot.send_message(
                c.message.chat.id,
                f"Оплата прошла, но не удалось зафиксировать платёж: {html.escape(err_apply)}\n"
                "Напиши ментору, он завершит вручную.",
            )
            bot.answer_callback_query(c.id, "Нужна помощь ментора")
            return

        repo_checkout_payments.mark_action_done(conn, tz, payment_row_id, note="postpay_saved")
        repo_states.clear_state(conn, owner_id, tz)
        notify_admins(
            f"<b>[ОПЛАТА]</b> Постоплата подтверждена через Cardlink\n"
            f"Пользователь: @{c.from_user.username or owner_id}\n"
            f"Сумма: {int(row['amount'] or 0)} ₽",
        )
        bot.send_message(
            c.message.chat.id,
            "Оплата подтверждена ✅\nПлатёж зафиксирован в карточке ученика.",
            reply_markup=user_menu_kb(show_pay=_user_show_pay_button(owner_id)),
        )
        bot.answer_callback_query(c.id, "Оплата подтверждена")

    # =========================
    # Profile form buttons
    # =========================

    def _get_profile_state(user_id: int) -> tuple[str | None, dict, dict]:
        st, data = repo_states.get_state(conn, user_id)
        data = data or {}
        prof = data.get("profile") or {}
        return st, data, prof

    @bot.callback_query_handler(func=lambda c: c.data.startswith("uform:uname:"))
    def uform_uname(c: CallbackQuery):
        if is_admin(c.from_user.id):
            bot.answer_callback_query(c.id, "Нет")
            return
        st, data, prof = _get_profile_state(c.from_user.id)
        if st != STATE_PROFILE or data.get("step") != "username":
            bot.answer_callback_query(c.id)
            return
        val = c.data.split(":", 2)[2]
        if val == "skip":
            prof["username"] = None
        else:
            prof["username"] = val
        _profile_continue_wizard(int(c.from_user.id), c.message.chat.id, prof, owner_username=c.from_user.username)
        bot.answer_callback_query(c.id)

    @bot.callback_query_handler(func=lambda c: c.data.startswith("uform:dir:"))
    def uform_dir(c: CallbackQuery):
        if is_admin(c.from_user.id):
            bot.answer_callback_query(c.id, "Нет")
            return
        st, data, prof = _get_profile_state(c.from_user.id)
        if st != STATE_PROFILE or data.get("step") != "direction":
            bot.answer_callback_query(c.id)
            return
        val = c.data.split(":", 2)[2]
        if val == "manual":
            bot.send_message(c.message.chat.id, "Напиши направление вручную (например: java):")
            bot.answer_callback_query(c.id)
            return
        prof["direction"] = val
        _profile_continue_wizard(int(c.from_user.id), c.message.chat.id, prof, owner_username=c.from_user.username)
        bot.answer_callback_query(c.id)

    @bot.callback_query_handler(func=lambda c: c.data.startswith("uform:stage:"))
    def uform_stage(c: CallbackQuery):
        if is_admin(c.from_user.id):
            bot.answer_callback_query(c.id, "Нет")
            return
        st, data, prof = _get_profile_state(c.from_user.id)
        if st != STATE_PROFILE or data.get("step") != "stage":
            bot.answer_callback_query(c.id)
            return
        val = c.data.split(":", 2)[2]
        if val == "manual":
            bot.send_message(c.message.chat.id, "Напиши этап вручную (например: Дипфейк):")
            bot.answer_callback_query(c.id)
            return
        prof["stage"] = val
        _profile_continue_wizard(int(c.from_user.id), c.message.chat.id, prof, owner_username=c.from_user.username)
        bot.answer_callback_query(c.id)

    @bot.callback_query_handler(func=lambda c: c.data.startswith("uform:tariff:"))
    def uform_tariff(c: CallbackQuery):
        if is_admin(c.from_user.id):
            bot.answer_callback_query(c.id, "Нет")
            return
        st, data, prof = _get_profile_state(c.from_user.id)
        if st != STATE_PROFILE or data.get("step") != "tariff":
            bot.answer_callback_query(c.id)
            return
        tariff = c.data.split(":", 2)[2]
        prof["tariff"] = tariff

        # Важно: если только постоплата — предоплату не спрашиваем.
        if tariff == "post":
            prof["prepay"] = 0
        if tariff == "pre":
            prof.pop("post_total_percent", None)
            prof.pop("post_monthly_percent", None)
        _profile_continue_wizard(int(c.from_user.id), c.message.chat.id, prof, owner_username=c.from_user.username)

        bot.answer_callback_query(c.id)

    @bot.callback_query_handler(func=lambda c: c.data.startswith("uform:prepay:"))
    def uform_prepay(c: CallbackQuery):
        if is_admin(c.from_user.id):
            bot.answer_callback_query(c.id, "Нет")
            return
        st, data, prof = _get_profile_state(c.from_user.id)
        if st != STATE_PROFILE or data.get("step") != "prepay":
            bot.answer_callback_query(c.id)
            return
        val = c.data.split(":", 2)[2]
        if val == "manual":
            bot.send_message(c.message.chat.id, "Напиши сумму предоплаты числом (например 30000):")
            bot.answer_callback_query(c.id)
            return
        prof["prepay"] = _parse_money(val)
        _profile_continue_wizard(int(c.from_user.id), c.message.chat.id, prof, owner_username=c.from_user.username)
        bot.answer_callback_query(c.id)

    @bot.callback_query_handler(func=lambda c: c.data.startswith("uform:post_total:"))
    def uform_post_total(c: CallbackQuery):
        if is_admin(c.from_user.id):
            bot.answer_callback_query(c.id, "Нет")
            return
        st, data, prof = _get_profile_state(c.from_user.id)
        if st != STATE_PROFILE or data.get("step") != "post_total_percent":
            bot.answer_callback_query(c.id)
            return
        val = c.data.split(":", 2)[2]
        if val == "manual":
            bot.send_message(c.message.chat.id, "Напиши процент постоплаты всего числом (например 600):")
            bot.answer_callback_query(c.id)
            return
        try:
            prof["post_total_percent"] = float(val.replace(",", "."))
        except Exception:
            prof["post_total_percent"] = 0.0
        _profile_continue_wizard(int(c.from_user.id), c.message.chat.id, prof, owner_username=c.from_user.username)
        bot.answer_callback_query(c.id)

    @bot.callback_query_handler(func=lambda c: c.data.startswith("uform:post_m:"))
    def uform_post_monthly(c: CallbackQuery):
        if is_admin(c.from_user.id):
            bot.answer_callback_query(c.id, "Нет")
            return
        st, data, prof = _get_profile_state(c.from_user.id)
        if st != STATE_PROFILE or data.get("step") != "post_monthly_percent":
            bot.answer_callback_query(c.id)
            return
        val = c.data.split(":", 2)[2]
        if val == "manual":
            bot.send_message(c.message.chat.id, "Напиши процент в месяц (например 50):")
            bot.answer_callback_query(c.id)
            return
        try:
            prof["post_monthly_percent"] = float(val.replace(",", "."))
        except Exception:
            prof["post_monthly_percent"] = 50.0
        _profile_continue_wizard(int(c.from_user.id), c.message.chat.id, prof, owner_username=c.from_user.username)
        bot.answer_callback_query(c.id)

    # =========================
    # Text handlers for user states
    # =========================

    @bot.message_handler(
        func=lambda m: repo_states.get_state(conn, m.from_user.id)[0] == STATE_STUDENT_CODE,
        content_types=["text"],
    )
    def st_student_code(m: Message):
        if is_admin(m.from_user.id):
            return
        code = (m.text or "").strip().upper()
        if len(code) < 4:
            repo_analytics.log_event(conn, tz, m.from_user.id, "funnel.student.code_invalid", {"reason": "too_short"})
            bot.send_message(m.chat.id, "Код слишком короткий. Проверь и отправь ещё раз.")
            return

        row, err = repo_student_codes.validate_for_use(conn, tz, code)
        if err:
            error_text = {
                "not_found": "Код не найден. Проверь и попробуй снова.",
                "used": "Этот код уже использован.",
                "revoked": "Этот код отозван ментором.",
                "expired": "Срок действия кода истёк.",
            }.get(err, "Код невалиден. Попробуй снова.")
            repo_analytics.log_event(conn, tz, m.from_user.id, "funnel.student.code_invalid", {"reason": err})
            bot.send_message(m.chat.id, error_text, reply_markup=_v2_back_kb())
            return
        if not row:
            repo_analytics.log_event(conn, tz, m.from_user.id, "funnel.student.code_invalid", {"reason": "unknown"})
            bot.send_message(m.chat.id, "Код невалиден. Попробуй снова.", reply_markup=_v2_back_kb())
            return

        ok = repo_student_codes.mark_used(conn, tz, int(row["id"]), int(m.from_user.id))
        if not ok:
            repo_analytics.log_event(conn, tz, m.from_user.id, "funnel.student.code_invalid", {"reason": "race"})
            bot.send_message(m.chat.id, "Не удалось активировать код. Попробуй ещё раз.", reply_markup=_v2_back_kb())
            return

        owner_id = int(m.from_user.id)
        repo_analytics.touch_user(conn, tz, owner_id, role_type=repo_analytics.ROLE_STUDENT)
        repo_analytics.log_event(conn, tz, owner_id, "funnel.student.code_valid")
        repo_states.clear_state(conn, owner_id, tz)
        bot.send_message(
            m.chat.id,
            "Код принят ✅\nВыбери, как заполнить анкету:",
            reply_markup=user_profile_start_mode_kb(),
        )

    @bot.message_handler(
        func=lambda m: repo_states.get_state(conn, m.from_user.id)[0] == STATE_AUTOAPPLY,
        content_types=["text"],
    )
    def st_autoapply(m: Message):
        if is_admin(m.from_user.id):
            return
        st, data = repo_states.get_state(conn, m.from_user.id)
        data = data or {}
        if st != STATE_AUTOAPPLY:
            return
        if _is_auto_temporarily_unavailable():
            repo_states.clear_state(conn, m.from_user.id, tz)
            bot.send_message(
                m.chat.id,
                _auto_temporarily_unavailable_text(),
                reply_markup=_home_menu_kb(int(m.from_user.id)),
            )
            return
        auto = (data.get("auto") or {}).copy()
        step = (data.get("step") or "").strip()
        text = (m.text or "").strip()

        if step == "student_hh_login":
            if len(text) < 4:
                bot.send_message(m.chat.id, "Логин слишком короткий. Введи HH логин ещё раз:")
                return
            auto["login"] = text
            repo_states.set_state(conn, m.from_user.id, STATE_AUTOAPPLY, {"step": "student_hh_password", "auto": auto}, tz)
            bot.send_message(m.chat.id, "Пароль HH:")
            return

        if step == "student_hh_password":
            if len(text) < 4:
                bot.send_message(m.chat.id, "Пароль слишком короткий. Введи пароль HH:")
                return
            auto["password"] = text
            saved = _store_student_hh_credentials(int(m.from_user.id), auto.get("login") or "", text)
            repo_states.set_state(conn, m.from_user.id, STATE_AUTOAPPLY, {"step": "direction", "auto": auto}, tz)
            if saved:
                bot.send_message(
                    m.chat.id,
                    "<b>Доступы HH сохранены ✅</b>\nТеперь выбери направление поиска:",
                    reply_markup=_auto_direction_kb(),
                )
            else:
                bot.send_message(
                    m.chat.id,
                    "<b>Доступы HH приняты ✅</b>\nТеперь выбери направление поиска:",
                    reply_markup=_auto_direction_kb(),
                )
            return

        if step == "direction_manual":
            if len(text) < 2:
                bot.send_message(m.chat.id, "Слишком коротко. Напиши направление, например: qa, devops, analyst.")
                return
            auto["direction"] = text.strip().lower()
            saved_login = str(auto.get("login") or "").strip()
            saved_password = str(auto.get("password") or "").strip()
            owner_id = int(m.from_user.id)
            if _has_student_access(owner_id):
                if not saved_login or not saved_password:
                    login_from_card, password_from_card = _student_hh_credentials(owner_id)
                    if login_from_card and not saved_login:
                        saved_login = login_from_card
                        auto["login"] = login_from_card
                    if password_from_card and not saved_password:
                        saved_password = password_from_card
                        auto["password"] = password_from_card
                if saved_login and saved_password:
                    _auto_continue_after_auth(owner_id, m.chat.id, auto)
                else:
                    next_step = "student_hh_login"
                    prompt = (
                        "<b>Для запуска автооткликов нужен HH-доступ</b>\n"
                        "Введи логин HH (почта или телефон):"
                    )
                    if saved_login:
                        auto["login"] = saved_login
                        next_step = "student_hh_password"
                        prompt = (
                            "<b>Для запуска автооткликов нужен HH-доступ</b>\n"
                            f"Логин HH: <code>{html.escape(saved_login)}</code>\n"
                            "Введи пароль HH:"
                        )
                    repo_states.set_state(conn, owner_id, STATE_AUTOAPPLY, {"step": next_step, "auto": auto}, tz)
                    bot.send_message(m.chat.id, prompt, reply_markup=_v2_back_kb())
                return
            if saved_login and saved_password:
                repo_states.set_state(conn, m.from_user.id, STATE_AUTOAPPLY, {"step": "login_pick", "auto": auto}, tz)
                bot.send_message(
                    m.chat.id,
                    "Нашёл сохранённые доступы HH. Использовать их или ввести новые?",
                    reply_markup=_auto_login_pick_kb(saved_login),
                )
            else:
                repo_states.set_state(conn, m.from_user.id, STATE_AUTOAPPLY, {"step": "login", "auto": auto}, tz)
                bot.send_message(m.chat.id, "Логин HH (почта/телефон):")
            return

        if step == "login":
            if len(text) < 4:
                bot.send_message(m.chat.id, "Логин слишком короткий. Введи HH логин ещё раз:")
                return
            auto["login"] = text
            repo_states.set_state(conn, m.from_user.id, STATE_AUTOAPPLY, {"step": "password", "auto": auto}, tz)
            bot.send_message(m.chat.id, "Пароль HH:")
            return

        if step == "password":
            if len(text) < 4:
                bot.send_message(m.chat.id, "Пароль слишком короткий. Введи пароль HH:")
                return
            auto["password"] = text
            if _has_student_access(int(m.from_user.id)):
                _store_student_hh_credentials(int(m.from_user.id), auto.get("login") or "", text)
            _auto_continue_after_auth(int(m.from_user.id), m.chat.id, auto)
            return

        if step == "target":
            locked_target = _auto_locked_target(auto)
            if locked_target > 0:
                auto["target_applies"] = locked_target
                auto["target_locked"] = True
                auto["target_site_clicks"] = locked_target
                auto["price_total"] = _auto_price_for_user(int(m.from_user.id), locked_target)
                fixed_query = get_core_query(conn, auto.get("direction"))
                if fixed_query:
                    auto["query_text"] = fixed_query
                    repo_states.set_state(conn, m.from_user.id, STATE_AUTOAPPLY, {"step": "confirm", "auto": auto}, tz)
                    bot.send_message(
                        m.chat.id,
                        (
                            f"Количество откликов уже зафиксировано с сайта: <b>{locked_target}</b>.\n"
                            f"Запрос для направления зафиксирован: <code>{fixed_query}</code>."
                        ),
                    )
                    bot.send_message(m.chat.id, _auto_confirm_text(int(m.from_user.id), auto), reply_markup=_auto_confirm_kb())
                else:
                    repo_states.set_state(conn, m.from_user.id, STATE_AUTOAPPLY, {"step": "query", "auto": auto}, tz)
                    bot.send_message(
                        m.chat.id,
                        (
                            f"Количество откликов уже зафиксировано с сайта: <b>{locked_target}</b>.\n"
                            "Изменить его в боте нельзя. Укажи поисковый запрос HH."
                        ),
                        reply_markup=_auto_query_kb(),
                    )
                return
            target = _parse_money(text)
            if target <= 0:
                bot.send_message(m.chat.id, "Введи число больше 0 (например 200):")
                return
            auto["target_applies"] = min(target, 5000)
            auto["price_total"] = _auto_price_for_user(int(m.from_user.id), auto["target_applies"])
            fixed_query = get_core_query(conn, auto.get("direction"))
            if fixed_query:
                auto["query_text"] = fixed_query
                repo_states.set_state(conn, m.from_user.id, STATE_AUTOAPPLY, {"step": "confirm", "auto": auto}, tz)
                bot.send_message(
                    m.chat.id,
                    f"Запрос для направления зафиксирован: <code>{fixed_query}</code>.",
                )
                bot.send_message(m.chat.id, _auto_confirm_text(int(m.from_user.id), auto), reply_markup=_auto_confirm_kb())
            else:
                repo_states.set_state(conn, m.from_user.id, STATE_AUTOAPPLY, {"step": "query", "auto": auto}, tz)
                bot.send_message(
                    m.chat.id,
                    (
                        "Поисковый запрос HH.\n"
                        "Это слово/фраза для поиска вакансий.\n"
                        "Примеры: Java developer, Python backend, Frontend React, QA engineer, DevOps.\n"
                        "Можно отправить текст или выбрать кнопку ниже:"
                    ),
                    reply_markup=_auto_query_kb(),
                )
            return

        if step == "query":
            auto["query_text"] = text
            repo_states.set_state(conn, m.from_user.id, STATE_AUTOAPPLY, {"step": "confirm", "auto": auto}, tz)
            summary = _auto_confirm_text(int(m.from_user.id), auto)
            bot.send_message(m.chat.id, summary, reply_markup=_auto_confirm_kb())
            return

        if step in {"pay_wait", "pay_date", "pay_inn", "pay_photo"}:
            pay_id = int(data.get("checkout_payment_id") or 0)
            row = repo_checkout_payments.get(conn, pay_id) if pay_id > 0 else None
            if row:
                bot.send_message(
                    m.chat.id,
                    "Оплата ожидается в Cardlink.\n"
                    "Нажми «Проверить оплату» после завершения платежа.",
                    reply_markup=_checkout_wait_kb(
                        check_callback=f"auto:pay:check:{pay_id}",
                        payment_url=str(row["confirmation_url"] or ""),
                    ),
                )
            else:
                bot.send_message(
                    m.chat.id,
                    "Сценарий оплаты не завершён. Нажми «Автоотклики» и начни заново.",
                    reply_markup=_home_menu_kb(int(m.from_user.id)),
                )
            return

        bot.send_message(
            m.chat.id,
            "Сценарий автооткликов не завершён. Нажми «Автоотклики» в меню и начни заново.",
            reply_markup=_home_menu_kb(int(m.from_user.id)),
        )

    @bot.message_handler(content_types=["photo", "document"], func=lambda m: repo_states.get_state(conn, m.from_user.id)[0] == STATE_AUTOAPPLY)
    def st_autoapply_payment_photo(m: Message):
        if is_admin(m.from_user.id):
            return
        st, data = repo_states.get_state(conn, m.from_user.id)
        data = data or {}
        if st != STATE_AUTOAPPLY or (data.get("step") or "") not in {"pay_photo", "pay_wait"}:
            return
        pay_id = int(data.get("checkout_payment_id") or 0)
        row = repo_checkout_payments.get(conn, pay_id) if pay_id > 0 else None
        bot.send_message(
            m.chat.id,
            "Чек загружать не нужно: статус оплаты проверяется автоматически через Cardlink.",
            reply_markup=_checkout_wait_kb(
                check_callback=f"auto:pay:check:{pay_id}",
                payment_url=str(row["confirmation_url"] or ""),
            ) if row else _home_menu_kb(int(m.from_user.id)),
        )

    @bot.message_handler(func=lambda m: repo_states.get_state(conn, m.from_user.id)[0] == STATE_PROFILE)
    def st_profile(m: Message):
        if is_admin(m.from_user.id):
            return
        st, data = repo_states.get_state(conn, m.from_user.id)
        if st != STATE_PROFILE:
            return
        data = data or {}
        prof = data.get("profile") or {}
        step = data.get("step")
        text = (m.text or "").strip()

        if step == "contract_lookup":
            if not _is_http_url(text):
                bot.send_message(m.chat.id, "Нужна ссылка, начинающаяся с http:// или https://")
                return
            contract_prefill = _prefill_profile_from_contract_url(int(m.from_user.id), text)
            prefill_keys = {k for k, v in (contract_prefill or {}).items() if v not in (None, "", 0)}
            merged = _merge_profile_prefills(
                prof,
                contract_prefill,
            )
            pulled = [k for k, v in (contract_prefill or {}).items() if v not in (None, "", 0)]
            missing = _profile_missing_labels(merged)
            if pulled:
                bot.send_message(m.chat.id, "Из договора подтянул: " + ", ".join(pulled) + ".")
            if prefill_keys <= {"contract_url"}:
                bot.send_message(
                    m.chat.id,
                    "Не нашёл данные анкеты по этой ссылке в OkiDoki. "
                    "Сохранил ссылку и продолжаю заполнение вручную.",
                )
            elif missing:
                bot.send_message(
                    m.chat.id,
                    "В договоре нет части полей. Дозаполним вручную: " + ", ".join(missing) + ".",
                )
            _profile_continue_wizard(int(m.from_user.id), m.chat.id, merged, owner_username=m.from_user.username)
            return

        if step == "fio":
            if len(text) < 5:
                bot.send_message(m.chat.id, "Напиши ФИО полностью (минимум 5 символов):")
                return
            prof["fio"] = text
            _profile_continue_wizard(int(m.from_user.id), m.chat.id, prof, owner_username=m.from_user.username)
            return

        if step == "username":
            # allow manual entry
            if text.lower() in ("нет", "-", "пропустить"):
                prof["username"] = None
            else:
                prof["username"] = text if text.startswith("@") else ("@" + text)
            _profile_continue_wizard(int(m.from_user.id), m.chat.id, prof, owner_username=m.from_user.username)
            return

        if step == "direction":
            prof["direction"] = text
            _profile_continue_wizard(int(m.from_user.id), m.chat.id, prof, owner_username=m.from_user.username)
            return

        if step == "stage":
            prof["stage"] = text
            _profile_continue_wizard(int(m.from_user.id), m.chat.id, prof, owner_username=m.from_user.username)
            return

        if step == "tariff":
            t = text.lower()
            tariff = None
            if "пост" in t and "пред" not in t:
                tariff = "post"
            elif "пред" in t and "пост" in t:
                tariff = "pre_post"
            elif "пред" in t:
                tariff = "pre"
            elif t in ("post", "pre", "pre_post"):
                tariff = t
            if not tariff:
                bot.send_message(m.chat.id, "Выбери тариф кнопкой ниже:", reply_markup=user_tariff_kb())
                return
            prof["tariff"] = tariff
            if tariff == "post":
                prof["prepay"] = 0
            if tariff == "pre":
                prof.pop("post_total_percent", None)
                prof.pop("post_monthly_percent", None)
            _profile_continue_wizard(int(m.from_user.id), m.chat.id, prof, owner_username=m.from_user.username)
            return

        if step == "prepay":
            prof["prepay"] = _parse_money(text)
            _profile_continue_wizard(int(m.from_user.id), m.chat.id, prof, owner_username=m.from_user.username)
            return

        if step == "post_total_percent":
            try:
                prof["post_total_percent"] = float(text.replace(",", "."))
            except Exception:
                prof["post_total_percent"] = 0.0
            _profile_continue_wizard(int(m.from_user.id), m.chat.id, prof, owner_username=m.from_user.username)
            return

        if step == "post_monthly_percent":
            try:
                prof["post_monthly_percent"] = float(text.replace(",", "."))
            except Exception:
                prof["post_monthly_percent"] = 50.0
            _profile_continue_wizard(int(m.from_user.id), m.chat.id, prof, owner_username=m.from_user.username)
            return

        if step == "contract":
            if not _is_http_url(text):
                bot.send_message(m.chat.id, "Нужна ссылка (начинается с http). Вставь ссылку на договор:")
                return
            prof["contract_url"] = text
            _profile_continue_wizard(int(m.from_user.id), m.chat.id, prof, owner_username=m.from_user.username)
            return

        # unexpected
        bot.send_message(m.chat.id, "Я потерял шаг анкеты. Открой 'Моя анкета' и начни заново.")
        send_user_home(m.chat.id, m.from_user.id)

    @bot.message_handler(func=lambda m: repo_states.get_state(conn, m.from_user.id)[0] == STATE_ACCOUNTS)
    def st_accounts(m: Message):
        if is_admin(m.from_user.id):
            return
        _st, data = repo_states.get_state(conn, m.from_user.id)
        data = data or {}
        step = data.get("step")
        acc = data.get("acc") or {}
        text = (m.text or "").strip()

        if step == "phone":
            acc["hh_phone"] = None if text in {"-", "—"} else text
            data["acc"] = acc
            data["step"] = "email"
            repo_states.set_state(conn, m.from_user.id, STATE_ACCOUNTS, data, tz)
            bot.send_message(m.chat.id, "Почта HH (или '-' если нет):")
            return
        if step == "email":
            acc["hh_email"] = None if text in {"-", "—"} else text
            data["acc"] = acc
            data["step"] = "pass"
            repo_states.set_state(conn, m.from_user.id, STATE_ACCOUNTS, data, tz)
            bot.send_message(m.chat.id, "Пароль HH (или '-' если нет):")
            return
        if step == "pass":
            acc["hh_password"] = None if text in {"-", "—"} else text
            data["acc"] = acc
            data["step"] = "birth"
            repo_states.set_state(conn, m.from_user.id, STATE_ACCOUNTS, data, tz)
            bot.send_message(m.chat.id, "Дата рождения (дд.мм.гггг) или '-' :")
            return

        if step == "birth":
            acc["hh_birth_date"] = None if text in {"-", "—"} else text
            data["acc"] = acc
            data["step"] = "loc"
            repo_states.set_state(conn, m.from_user.id, STATE_ACCOUNTS, data, tz)
            bot.send_message(m.chat.id, "Локация (город/страна) или '-' :")
            return

        if step == "loc":
            acc["hh_location"] = None if text in {"-", "—"} else text
            data["acc"] = acc
            data["step"] = "salary_min"
            repo_states.set_state(conn, m.from_user.id, STATE_ACCOUNTS, data, tz)
            bot.send_message(m.chat.id, "Фин ожидания: минимум (число в ₽) или '-' :")
            return

        if step == "salary_min":
            if text in {"-", "—"}:
                acc["hh_salary_min"] = None
            else:
                n = _parse_money(text)
                if n <= 0:
                    bot.send_message(m.chat.id, "Введи число (например 220000) или '-' :")
                    return
                acc["hh_salary_min"] = n
            data["acc"] = acc
            data["step"] = "salary_comfort"
            repo_states.set_state(conn, m.from_user.id, STATE_ACCOUNTS, data, tz)
            bot.send_message(m.chat.id, "Фин ожидания: комфорт (число в ₽) или '-' :")
            return

        if step == "salary_comfort":
            if text in {"-", "—"}:
                acc["hh_salary_comfort"] = None
            else:
                n = _parse_money(text)
                if n <= 0:
                    bot.send_message(m.chat.id, "Введи число (например 260000) или '-' :")
                    return
                acc["hh_salary_comfort"] = n

            payload = {**acc, "owner_chat_id": m.chat.id, "owner_username": m.from_user.username}
            req_id = repo_requests.create(conn, tz, m.from_user.id, "ACCOUNTS", payload, None)

            bot.send_message(m.chat.id, "Данные HH отправлены ментору на подтверждение.")
            notify_admins(
                f"<b>[ЗАЯВКА]</b> {texts.request_title('ACCOUNTS')}\nОт: @{m.from_user.username or m.from_user.id}\nID: {req_id}",
                kb=request_admin_kb(req_id),
            )
            send_user_home(m.chat.id, m.from_user.id)
            return

        bot.send_message(m.chat.id, "Я потерял шаг. Открой 'Доступы HH' и заполни заново.")
        send_user_home(m.chat.id, m.from_user.id)

    @bot.message_handler(func=lambda m: repo_states.get_state(conn, m.from_user.id)[0] == STATE_ENROLL_PAYMENT)
    def st_enroll_payment(m: Message):
        if is_admin(m.from_user.id):
            return
        st, data = repo_states.get_state(conn, m.from_user.id)
        data = data or {}
        if st == STATE_ENROLL_PAYMENT and (data.get("step") or "") == "checkout_wait":
            pay_id = int(data.get("checkout_payment_id") or 0)
            row = repo_checkout_payments.get(conn, pay_id) if pay_id > 0 else None
            if row:
                bot.send_message(
                    m.chat.id,
                    "Оплата по договору ожидается в Cardlink.\n"
                    "Нажми «Проверить оплату» после завершения платежа.",
                    reply_markup=_checkout_wait_kb(
                        check_callback=f"enr:pay:check:{pay_id}",
                        payment_url=str(row["confirmation_url"] or ""),
                    ),
                )
                return
        repo_states.clear_state(conn, m.from_user.id, tz)
        _start_enroll_payment_wizard(int(m.from_user.id), m.chat.id, owner_username=m.from_user.username)
        return

    @bot.message_handler(content_types=["photo", "document"], func=lambda m: repo_states.get_state(conn, m.from_user.id)[0] == STATE_ENROLL_PAYMENT)
    def st_enroll_payment_photo(m: Message):
        if is_admin(m.from_user.id):
            return
        st, data = repo_states.get_state(conn, m.from_user.id)
        data = data or {}
        if st == STATE_ENROLL_PAYMENT and (data.get("step") or "") == "checkout_wait":
            pay_id = int(data.get("checkout_payment_id") or 0)
            row = repo_checkout_payments.get(conn, pay_id) if pay_id > 0 else None
            if row:
                bot.send_message(
                    m.chat.id,
                    "Чек загружать не нужно: статус оплаты берётся из Cardlink.",
                    reply_markup=_checkout_wait_kb(
                        check_callback=f"enr:pay:check:{pay_id}",
                        payment_url=str(row["confirmation_url"] or ""),
                    ),
                )
                return
        repo_states.clear_state(conn, m.from_user.id, tz)
        _start_enroll_payment_wizard(int(m.from_user.id), m.chat.id, owner_username=m.from_user.username)
        return

    @bot.message_handler(func=lambda m: repo_states.get_state(conn, m.from_user.id)[0] == STATE_PAYMENT)
    def st_payment(m: Message):
        if is_admin(m.from_user.id):
            return
        st, data = repo_states.get_state(conn, m.from_user.id)
        data = data or {}
        if st == STATE_PAYMENT and (data.get("step") or "") == "checkout_wait":
            pay_id = int(data.get("checkout_payment_id") or 0)
            row = repo_checkout_payments.get(conn, pay_id) if pay_id > 0 else None
            if row:
                bot.send_message(
                    m.chat.id,
                    "Оплата ожидается в Cardlink.\n"
                    "Нажми «Проверить оплату» после завершения платежа.",
                    reply_markup=_checkout_wait_kb(
                        check_callback=f"u:pay:check:{pay_id}",
                        payment_url=str(row["confirmation_url"] or ""),
                        back_callback="u:cancel",
                    ),
                )
                return
        repo_states.clear_state(conn, m.from_user.id, tz)
        _start_payment_wizard(int(m.from_user.id), m.chat.id, owner_username=m.from_user.username)
        return

    @bot.message_handler(content_types=["photo", "document"], func=lambda m: repo_states.get_state(conn, m.from_user.id)[0] == STATE_PAYMENT)
    def st_payment_photo(m: Message):
        if is_admin(m.from_user.id):
            return
        st, data = repo_states.get_state(conn, m.from_user.id)
        data = data or {}
        if st == STATE_PAYMENT and (data.get("step") or "") == "checkout_wait":
            pay_id = int(data.get("checkout_payment_id") or 0)
            row = repo_checkout_payments.get(conn, pay_id) if pay_id > 0 else None
            if row:
                bot.send_message(
                    m.chat.id,
                    "Чек загружать не нужно: статус оплаты берётся из Cardlink.",
                    reply_markup=_checkout_wait_kb(
                        check_callback=f"u:pay:check:{pay_id}",
                        payment_url=str(row["confirmation_url"] or ""),
                        back_callback="u:cancel",
                    ),
                )
                return
        repo_states.clear_state(conn, m.from_user.id, tz)
        _start_payment_wizard(int(m.from_user.id), m.chat.id, owner_username=m.from_user.username)
        return
