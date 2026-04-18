from __future__ import annotations

import html
import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from telebot import TeleBot
from telebot.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message

from .config import Config
from .okidoki_readonly_client import OkiDokiReadOnlyClient, OkiDokiReadOnlyError
from .prefill_parser import (
    ContractValidationResult,
    extract_prefill,
    get_missing_fields,
    validate_contract,
)
from .storage import Storage

log = logging.getLogger(__name__)


FIELD_LABELS = {
    "fio": "ФИО",
    "direction": "Направление",
    "tariff": "Тариф",
    "study_start_date": "Дата обучения",
    "paid_amount": "Сколько уже заплатил",
    "will_pay_amount": "Сколько заплатит",
    "contract_url": "Ссылка на договор",
    "username": "Telegram username",
}

FIELD_PROMPTS = {
    "fio": "Введи ФИО как в договоре:",
    "direction": "Введи направление (например: Java, Python, Frontend):",
    "tariff": "Введи тариф (`pre`, `post` или `pre_post`):",
    "study_start_date": "Введи дату обучения в формате `ДД.ММ.ГГГГ`:",
    "paid_amount": "Введи сумму, которую уже заплатил (число в рублях):",
    "will_pay_amount": "Введи, сколько заплатит (сумма или текст):",
}

TARIFF_LABELS = {
    "pre": "Только предоплата",
    "post": "Только постоплата",
    "pre_post": "Предоплата + постоплата",
}


@dataclass(slots=True)
class Session:
    step: str
    mode: str
    contract_url: str = ""
    payload: dict[str, Any] = field(default_factory=dict)
    prefill: dict[str, Any] = field(default_factory=dict)
    missing_fields: list[str] = field(default_factory=list)
    current_field: str = ""
    reason: str = ""
    validation: ContractValidationResult | None = None


def _normalize_username(raw: str | None) -> str:
    return str(raw or "").strip().lstrip("@").lower()


def _format_date(raw: str) -> str:
    txt = str(raw or "").strip()
    if not txt:
        return ""
    for fmt in ("%d.%m.%Y", "%Y-%m-%d", "%d-%m-%Y", "%Y/%m/%d"):
        try:
            parsed = datetime.strptime(txt, fmt)
            return parsed.strftime("%d.%m.%Y")
        except Exception:
            continue
    return ""


def _parse_amount(raw: str) -> int | None:
    digits = "".join(ch for ch in str(raw or "") if ch.isdigit())
    if not digits:
        return None
    try:
        return max(int(digits), 0)
    except Exception:
        return None


def _normalize_tariff(raw: str) -> str:
    txt = str(raw or "").strip().lower()
    if txt in {"pre", "post", "pre_post"}:
        return txt
    has_pre = ("пред" in txt) or ("pre" in txt)
    has_post = ("пост" in txt) or ("post" in txt)
    if has_pre and has_post:
        return "pre_post"
    if has_post:
        return "post"
    if has_pre:
        return "pre"
    return ""


def _display_value(key: str, value: Any) -> str:
    if value is None:
        return "—"
    if key == "username":
        username = _normalize_username(str(value))
        return f"@{username}" if username else "—"
    if key == "paid_amount" and isinstance(value, (int, float)):
        return f"{int(value)} ₽"
    return str(value)


def _prefill_text(prefill: dict[str, Any]) -> str:
    ordered = [
        "fio",
        "username",
        "direction",
        "tariff",
        "study_start_date",
        "paid_amount",
        "will_pay_amount",
        "contract_url",
    ]
    lines = ["Что удалось заполнить:"]
    for key in ordered:
        if key not in prefill:
            continue
        label = FIELD_LABELS.get(key, key)
        lines.append(f"- {label}: {_display_value(key, prefill.get(key))}")
    return "\n".join(lines)


def _as_float(raw: Any) -> float | None:
    if raw is None:
        return None
    if isinstance(raw, (int, float)) and not isinstance(raw, bool):
        return float(raw)
    text = str(raw).strip().replace(",", ".")
    if not text:
        return None
    try:
        return float(text)
    except Exception:
        return None


def _format_money(raw: Any) -> str:
    if raw is None:
        return "—"
    if isinstance(raw, (int, float)) and not isinstance(raw, bool):
        amount = int(raw)
    else:
        amount = _parse_amount(str(raw))
    if amount is None:
        return html.escape(str(raw))
    return f"{amount:,}".replace(",", " ") + " ₽"


def _post_rule_text(prefill: dict[str, Any], tariff_code: str) -> str:
    if tariff_code not in {"post", "pre_post"}:
        return "—"
    total = _as_float(prefill.get("post_total_percent")) or 0.0
    monthly = _as_float(prefill.get("post_monthly_percent"))
    months = _as_float(prefill.get("postpay_months"))

    if total > 0 and monthly is not None and monthly > 0:
        return f"{total:.1f}% всего; {monthly:.1f}% в месяц"
    if total > 0 and months is not None and months > 0:
        calc_monthly = total / months
        return f"{total:.1f}% всего; {calc_monthly:.1f}% в месяц"
    if total > 0:
        return f"{total:.1f}% всего; кастом"

    will_pay = str(prefill.get("will_pay_amount") or "").strip()
    return will_pay or "—"


def _student_card_preview_text(prefill: dict[str, Any]) -> str:
    fio = html.escape(str(prefill.get("fio") or "—"))
    direction = html.escape(str(prefill.get("direction") or "—"))
    stage = html.escape(str(prefill.get("stage") or "С нуля"))

    username_norm = _normalize_username(prefill.get("username"))
    username = f"@{username_norm}" if username_norm else "—"

    lead_source = html.escape(str(prefill.get("lead_source") or "—"))
    tariff_code = _normalize_tariff(prefill.get("tariff"))
    tariff_title = TARIFF_LABELS.get(tariff_code, str(prefill.get("tariff") or "—"))
    tariff_percent = _as_float(prefill.get("post_total_percent"))
    if tariff_code in {"post", "pre_post"} and tariff_percent is not None and tariff_percent > 0:
        tariff_title = f"{tariff_title} ({tariff_percent:.1f}%)"

    prepay = _format_money(prefill.get("paid_amount"))
    post_rule = html.escape(_post_rule_text(prefill, tariff_code))
    if tariff_code in {"post", "pre_post"}:
        post_status = "ожидает оффер (первый платёж через 1 месяц после оффера)"
    else:
        post_status = "—"

    join_date_raw = str(prefill.get("study_start_date") or "").strip()
    join_date = _format_date(join_date_raw) or join_date_raw or "—"
    contract_status = "ссылка добавлена" if str(prefill.get("contract_url") or "").strip() else "не заполнен"

    lines = [
        "<b>Ученик 1/1</b>",
        "",
        f"<b>{fio}</b>",
        f"Направление: <b>{direction}</b>",
        f"Этап: <b>{stage}</b>",
        f"Username: {html.escape(username)}",
        f"Источник лида: <b>{lead_source}</b>",
        f"Тариф: <b>{html.escape(tariff_title)}</b>",
        f"Предоплата: <b>{prepay}</b>",
        f"Постоплата правило: {post_rule}",
        f"Постоплата: <b>{html.escape(post_status)}</b>",
        f"На обучении с: <b>{html.escape(join_date)}</b>",
        "Собесы: <b>нет</b>",
        f"Договор: <b>{contract_status}</b>",
        "Автосообщения: <b>0/0</b> прочитано",
    ]
    return "\n".join(lines)


def _student_start_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup()
    kb.row(InlineKeyboardButton("Я ученик", callback_data="student:start"))
    return kb


def _load_template_catalog(client: OkiDokiReadOnlyClient) -> dict[str, dict[str, Any]]:
    catalog: dict[str, dict[str, Any]] = {}
    templates = client.list_templates()
    for item in templates:
        template_id = str(item.get("template_id") or "").strip()
        if not template_id:
            continue
        entities = client.get_template_entities(template_id)
        entity_names: list[str] = []
        for entity in entities:
            name = str(entity.get("keyword") or entity.get("name") or "").strip()
            if name:
                entity_names.append(name)
        catalog[template_id] = {
            "template_name": str(item.get("template_name") or "").strip(),
            "entity_names": entity_names,
        }
    return catalog


def create_bot(cfg: Config) -> TeleBot:
    bot = TeleBot(cfg.bot_token, parse_mode="HTML")
    storage = Storage(cfg.db_path)
    okidoki = OkiDokiReadOnlyClient(cfg.okidoki_api_token, cfg.okidoki_api_base)
    sessions: dict[int, Session] = {}
    mentor_contact_url = str(cfg.mentor_contact_url or "").strip()
    mentor_contact_username = mentor_contact_url.rstrip("/").split("/")[-1].lstrip("@") if mentor_contact_url else ""

    template_catalog: dict[str, dict[str, Any]] = {}
    try:
        template_catalog = _load_template_catalog(okidoki)
        log.info("Loaded OkiDoki template catalog: count=%s", len(template_catalog))
    except Exception:
        log.exception("Failed to load OkiDoki template catalog on startup")

    def _mentor_contact_kb() -> InlineKeyboardMarkup | None:
        if not mentor_contact_url:
            return None
        kb = InlineKeyboardMarkup()
        kb.row(InlineKeyboardButton("Написать ментору", url=mentor_contact_url))
        return kb

    def _mentor_contact_hint() -> str:
        if mentor_contact_username:
            return f"@{mentor_contact_username}"
        return mentor_contact_url or "ментору"

    def _notify_mentor(request_id: int, user: Message, session: Session, final_prefill: dict[str, Any]) -> None:
        mentor_chat_id = cfg.mentor_chat_id
        if not mentor_chat_id:
            return
        lines = [
            f"<b>Новая заявка регистрации #{request_id}</b>",
            f"mode: {session.mode}",
            f"reason: {session.reason or '—'}",
            f"user_id: {user.from_user.id}",
            f"user: @{_normalize_username(user.from_user.username) or user.from_user.id}",
            f"contract_url: {session.contract_url or '—'}",
            "",
            _prefill_text(final_prefill),
        ]
        if session.validation:
            lines.append("")
            lines.append(f"validation: {session.validation.reason}")
            if session.validation.template_name:
                lines.append(f"template: {session.validation.template_name}")
        try:
            bot.send_message(mentor_chat_id, "\n".join(lines))
        except Exception:
            log.exception("Failed to notify mentor chat=%s request_id=%s", mentor_chat_id, request_id)

    def _finish(message: Message, session: Session, status: str) -> None:
        user_id = int(message.from_user.id)
        final_prefill = dict(session.prefill)
        missing = get_missing_fields(final_prefill)
        request_id = storage.save_request(
            tg_user_id=user_id,
            tg_username=_normalize_username(message.from_user.username) or None,
            status=status,
            reason=session.reason or "",
            contract_url=session.contract_url,
            payload=session.payload,
            prefill=final_prefill,
            missing_fields=missing,
        )
        _notify_mentor(request_id, message, session, final_prefill)

        if status == "auto_ready" and not missing:
            bot.send_message(
                message.chat.id,
                "Договор валиден, данные подтянуты и регистрация отправлена на модерацию ментору.",
            )
        else:
            contact_hint = _mentor_contact_hint()
            bot.send_message(
                message.chat.id,
                f"Отправил на ручную регистрацию и модерацию ментору ({contact_hint}).",
                reply_markup=_mentor_contact_kb(),
            )
        sessions.pop(user_id, None)
        log.info(
            "Registration finished request_id=%s user_id=%s status=%s reason=%s missing=%s",
            request_id,
            user_id,
            status,
            session.reason,
            missing,
        )

    def _ask_next_missing(message: Message, session: Session) -> None:
        if not session.missing_fields:
            final_status = "auto_ready" if session.mode == "auto" else "manual_review"
            _finish(message, session, final_status)
            return
        field = session.missing_fields[0]
        session.current_field = field
        session.step = "await_missing_field"
        prompt = FIELD_PROMPTS.get(field, f"Введи поле: {field}")
        bot.send_message(message.chat.id, prompt)

    def _reject_contract(
        message: Message,
        contract_url: str,
        reason: str,
        payload: dict[str, Any] | None = None,
        prefill: dict[str, Any] | None = None,
    ) -> None:
        user_id = int(message.from_user.id)
        sessions[user_id] = Session(
            step="await_contract_link",
            mode="auto",
            contract_url=contract_url,
            payload=dict(payload or {}),
            prefill=dict(prefill or {}),
            reason=reason,
        )
        log.info("Contract rejected user_id=%s reason=%s url=%s", user_id, reason, contract_url)
        contact_hint = _mentor_contact_hint()
        bot.send_message(
            message.chat.id,
            "Договор не валиден, обратись к ментору.\n"
            f"Контакт: {contact_hint}\n"
            "После уточнения пришли корректную ссылку на договор OkiDoki.",
            reply_markup=_mentor_contact_kb(),
        )

    def _handle_contract_link(message: Message, session: Session, text: str) -> None:
        user_id = int(message.from_user.id)
        username_telegram = _normalize_username(message.from_user.username)
        if not text.lower().startswith(("http://", "https://")):
            bot.send_message(
                message.chat.id,
                "Это не похоже на ссылку договора OkiDoki.\n"
                "Пришли ссылку вида http://... или https://...",
            )
            return
        session.contract_url = text
        if not username_telegram:
            bot.send_message(
                message.chat.id,
                "Для регистрации обязателен Telegram username.\n"
                "Создай username в Telegram (Настройки -> Имя пользователя) и отправь ссылку снова.",
            )
            return

        try:
            payload = okidoki.fetch_contract_payload(text)
        except OkiDokiReadOnlyError as exc:
            log.info("Contract fetch failed user_id=%s url=%s error=%s", user_id, text, exc)
            _reject_contract(message, text, "contract_not_readable", {}, {"contract_url": text})
            return

        validation = validate_contract(payload, template_catalog)
        prefill = extract_prefill(payload, text)
        session.payload = payload
        session.validation = validation
        session.prefill = prefill

        if not validation.is_valid:
            _reject_contract(message, text, f"invalid_contract:{validation.reason}", payload, prefill)
            return

        username_contract = _normalize_username(prefill.get("username"))
        if not username_contract:
            _reject_contract(message, text, "contract_has_no_telegram_username", payload, prefill)
            return

        if username_telegram != cfg.test_exception_username and username_telegram != username_contract:
            _reject_contract(
                message,
                text,
                f"username_mismatch:tg={username_telegram},contract={username_contract}",
                payload,
                prefill,
            )
            return

        session.mode = "auto"
        session.reason = "contract_valid"
        session.missing_fields = get_missing_fields(prefill)
        sessions[user_id] = session
        bot.send_message(message.chat.id, "Ссылка валидна, договор распознан как наш.")
        bot.send_message(message.chat.id, _student_card_preview_text(prefill))
        if session.missing_fields:
            missing_labels = [FIELD_LABELS.get(field, field) for field in session.missing_fields]
            bot.send_message(
                message.chat.id,
                "Нужно дозаполнить поля: " + ", ".join(missing_labels),
            )
        _ask_next_missing(message, session)

    def _apply_answer(session: Session, field: str, raw_value: str) -> tuple[bool, str]:
        value = str(raw_value or "").strip()
        if field == "fio":
            if len(value) < 5:
                return False, "ФИО слишком короткое. Введи полное ФИО."
            session.prefill[field] = value
            return True, ""
        if field == "direction":
            if not value:
                return False, "Направление не может быть пустым."
            session.prefill[field] = value
            return True, ""
        if field == "tariff":
            tariff = _normalize_tariff(value)
            if not tariff:
                return False, "Тариф должен быть `pre`, `post` или `pre_post`."
            session.prefill[field] = tariff
            return True, ""
        if field == "study_start_date":
            date_value = _format_date(value)
            if not date_value:
                return False, "Неверный формат даты. Используй `ДД.ММ.ГГГГ`."
            session.prefill[field] = date_value
            return True, ""
        if field == "paid_amount":
            amount = _parse_amount(value)
            if amount is None:
                return False, "Сумма должна быть числом."
            session.prefill[field] = amount
            return True, ""
        if field == "will_pay_amount":
            amount = _parse_amount(value)
            session.prefill[field] = amount if amount is not None else value
            return True, ""
        return False, "Неизвестное поле, попробуй снова."

    @bot.message_handler(commands=["start"])
    def cmd_start(message: Message) -> None:
        sessions.pop(int(message.from_user.id), None)
        bot.send_message(
            message.chat.id,
            "Тестовый бот регистрации через OkiDoki.\nНажми кнопку ниже, чтобы начать.",
            reply_markup=_student_start_kb(),
        )

    @bot.message_handler(commands=["templates"])
    def cmd_templates(message: Message) -> None:
        if not template_catalog:
            bot.send_message(message.chat.id, "Каталог шаблонов пока не загружен.")
            return
        lines = ["Шаблоны OkiDoki:"]
        for tid, data in template_catalog.items():
            name = str(data.get("template_name") or tid)
            entities = data.get("entity_names") or []
            lines.append(f"- {name} ({tid})")
            if entities:
                lines.append("  поля: " + ", ".join(str(e) for e in entities))
        bot.send_message(message.chat.id, "\n".join(lines))

    @bot.callback_query_handler(func=lambda c: c.data == "student:start")
    def cb_student_start(call: CallbackQuery) -> None:
        user_id = int(call.from_user.id)
        sessions[user_id] = Session(step="await_contract_link", mode="auto")
        bot.answer_callback_query(call.id)
        bot.send_message(
            call.message.chat.id,
            "Вставь ссылку на договор OkiDoki. Я проверю её и попробую заполнить карточку автоматически.",
        )
        log.info("Registration started user_id=%s", user_id)

    @bot.message_handler(content_types=["text"])
    def on_text(message: Message) -> None:
        user_id = int(message.from_user.id)
        text = str(message.text or "").strip()
        session = sessions.get(user_id)

        if not session:
            bot.send_message(
                message.chat.id,
                "Нажми /start и выбери «Я ученик», чтобы начать регистрацию.",
            )
            return

        if session.step == "await_contract_link":
            _handle_contract_link(message, session, text)
            return

        if session.step == "await_missing_field":
            field = session.current_field
            ok, err = _apply_answer(session, field, text)
            if not ok:
                bot.send_message(message.chat.id, err)
                return
            if session.missing_fields and session.missing_fields[0] == field:
                session.missing_fields.pop(0)
            log.info("Field collected user_id=%s field=%s mode=%s", user_id, field, session.mode)
            _ask_next_missing(message, session)
            return

        bot.send_message(
            message.chat.id,
            "Сессия сбилась. Нажми /start и начни заново.",
        )
        sessions.pop(user_id, None)

    return bot


def run_bot(cfg: Config) -> None:
    bot = create_bot(cfg)
    log.info("Starting test bot polling")
    bot.infinity_polling(skip_pending=True, timeout=60, long_polling_timeout=60)
