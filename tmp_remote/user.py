from __future__ import annotations

import html
import json
import logging
import os
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
from app.services.yookassa_client import YooKassaClient, YooKassaClientError, normalize_status as yookassa_status
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
    yookassa_client = YooKassaClient(
        shop_id=cfg.YOOKASSA_SHOP_ID,
        secret_key=cfg.YOOKASSA_SECRET_KEY,
        return_url=cfg.YOOKASSA_RETURN_URL,
        timeout_sec=cfg.YOOKASSA_TIMEOUT_SEC,
        oauth_token=cfg.YOOKASSA_OAUTH_TOKEN,
        oauth_client_id=cfg.YOOKASSA_OAUTH_CLIENT_ID,
        oauth_client_secret=cfg.YOOKASSA_OAUTH_CLIENT_SECRET,
        oauth_authorization_code=cfg.YOOKASSA_OAUTH_AUTH_CODE,
        oauth_token_url=cfg.YOOKASSA_OAUTH_TOKEN_URL,
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
        return "через YooKassa"

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
        if not yookassa_client.enabled:
            return None, "YooKassa пока не настроена. Оплата доступна только через ментора."
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
            provider = yookassa_client.create_payment(
                amount_rub=invoice_amount,
                description=description,
                metadata=payload_meta,
            )
        except YooKassaClientError as exc:
            return None, f"Не удалось создать платёж в YooKassa: {html.escape(str(exc))}"

        provider_payment_id = str(provider.get("id") or "").strip()
        if not provider_payment_id:
            return None, "YooKassa не вернула id платежа. Напиши ментору."
        status = yookassa_status(provider.get("status"))
        confirmation = provider.get("confirmation") if isinstance(provider.get("confirmation"), dict) else {}
        confirmation_url = str(
            confirmation.get("confirmation_url")
            or confirmation.get("confirmationUrl")
            or provider.get("confirmation_url")
            or ""
        ).strip()
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
        if not yookassa_client.enabled:
            return row, "YooKassa не настроена в текущем окружении."

        provider_payment_id = str(row["provider_payment_id"] or "").strip()
        if not provider_payment_id:
            return row, "Не найден provider_payment_id."
        try:
            provider = yookassa_client.get_payment(provider_payment_id)
        except YooKassaClientError as exc:
            return row, f"Не удалось обновить статус: {html.escape(str(exc))}"

        status = yookassa_status(provider.get("status"))
        confirmation = provider.get("confirmation") if isinstance(provider.get("confirmation"), dict) else {}
        confirmation_url = str(
            confirmation.get("confirmation_url")
            or confirmation.get("confirmationUrl")
            or row["confirmation_url"]
            or ""
        ).strip()
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
            note=f"yookassa:{payment_row_id}",
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
                note=f"yookassa:{payment_row_id}",
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
                note=f"enrollment_yookassa:{payment_row_id}",
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
        if not yookassa_client.enabled:
            repo_states.clear_state(conn, owner_tg_id, tz)
            bot.send_message(
                chat_id,
                "Оплата доступна только через кассу YooKassa.\n"
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
            bot.send_message(chat_id, err or "Не удалось создать платёж в YooKassa. Попробуй позже.")
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
        source = str(auto.get("source") or "").strip().lower()
        expected = int(auto.get("price_total") or 0)
        return source == "site" and expected > 0

    def _auto_confirm_text(owner_tg_id: int, auto: dict) -> str:
        summary = (
            "<b>Проверка параметров автооткликов</b>\n"
            f"Направление: <b>{auto.get('direction')}</b>\n"
            f"Логин: <code>{auto.get('login')}</code>\n"
            f"Цель: <b>{int(auto.get('target_applies') or 0)}</b> откликов\n"
            f"Запрос: <code>{auto.get('query_text') or auto.get('direction')}</code>"
        )
        if _auto_requires_site_payment(owner_tg_id, auto):
            summary += f"\nОплата с сайта: <b>{int(auto.get('price_total') or 0)} ₽</b> ({_payment_channel_label()})."
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

    def _prefill_profile_from_contract_row(contract: sqlite3.Row | None) -> dict:
        if not cfg.OKIDOKI_PROFILE_PREFILL_ENABLED:
            return {}
        if not contract:
            return {}

        payload: dict = {}

        def _merge_payload(raw: str | None) -> None:
            if not raw:
                return
            try:
                data = json.loads(str(raw))
            except Exception:
                return
            if isinstance(data, dict):
                payload.update(data)

        if hasattr(contract, "keys"):
            keys = set(contract.keys())
            if "okidoki_request_json" in keys:
                _merge_payload(contract["okidoki_request_json"])
            if "okidoki_response_json" in keys:
                _merge_payload(contract["okidoki_response_json"])

        def _pick_text(*keys: str) -> str | None:
            for k in keys:
                val = payload.get(k)
                if isinstance(val, str) and val.strip():
                    return val.strip()
            return None

        out: dict = {}
        fio = _pick_text("fio", "full_name", "fullname", "name", "client_name", "customer_name", "person_name")
        if fio:
            out["fio"] = fio
        direction = _pick_text("direction", "specialization", "stack", "profile", "course_direction")
        if direction:
            out["direction"] = direction

        # stage fallback from enrollment level
        level_key = str(contract["level_key"] or "").strip().lower()
        if level_key == "zero":
            out["stage"] = "С нуля"
        elif level_key:
            out["stage"] = "Дообучение"

        # infer tariff and numeric parts from tariff price
        tariff_price = str(contract["tariff_price"] or "").strip()
        if tariff_price:
            has_plus = "+" in tariff_price
            has_percent = "%" in tariff_price
            if has_plus and has_percent:
                out["tariff"] = "pre_post"
            elif has_percent:
                out["tariff"] = "post"
            else:
                out["tariff"] = "pre"

            first_part = tariff_price.split("+", 1)[0]
            prepay_amount = _parse_money(first_part)
            if prepay_amount > 0:
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
            if percents:
                out["post_total_percent"] = float(percents[0])
                if len(percents) > 1:
                    out["post_monthly_percent"] = float(percents[1])

        sign_url = _pick_text("contract_url", "sign_url", "signUrl", "url")
        if not sign_url:
            sign_url = str(contract["sign_url"] or "").strip()
        if sign_url:
            out["contract_url"] = sign_url
        return out

    def _prefill_profile_from_contract(owner_tg_id: int) -> dict:
        contract = repo_enrollment_contracts.get_by_owner(conn, int(owner_tg_id))
        return _prefill_profile_from_contract_row(contract)

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
        if not contract:
            return out

        merged = _merge_profile_prefills(_prefill_profile_from_contract_row(contract), out)
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
        if not yookassa_client.enabled:
            repo_states.clear_state(conn, owner_tg_id, tz)
            bot.send_message(
                chat_id,
                "Оплата доступна только через кассу YooKassa.\n"
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
            bot.send_message(chat_id, err or "Не удалось создать платёж в YooKassa. Попробуй позже.")
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
          