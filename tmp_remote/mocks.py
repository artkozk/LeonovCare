from __future__ import annotations

import datetime as dt
import sqlite3
import threading

from telebot import TeleBot
from telebot.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message

from app.handlers.common import is_admin
from app.db import repo_mocks, repo_states, repo_students
from app.services.timeutils import format_dt_ddmmyyyy_hhmm, iso, now, tzinfo
from app.ui.keyboards import user_menu_kb


MOCK_SLOT_STEP_MIN = 30
MOCK_SLOT_FROM = dt.time(9, 0)
MOCK_SLOT_TO = dt.time(21, 0)
MOCK_LOCK = threading.Lock()


def init(bot: TeleBot, ctx: dict) -> None:
    conn: sqlite3.Connection = ctx["conn"]
    tz = ctx["cfg"].TZ

    def _active_student(user_id: int) -> sqlite3.Row | None:
        s = repo_students.get_by_owner(conn, int(user_id))
        if not s:
            return None
        if int(s["archived"] or 0) == 1:
            return None
        return s

    def _slot_starts(day: dt.date) -> list[dt.datetime]:
        tzi = tzinfo(tz)
        cur = dt.datetime.combine(day, MOCK_SLOT_FROM).replace(tzinfo=tzi)
        end = dt.datetime.combine(day, MOCK_SLOT_TO).replace(tzinfo=tzi)
        out: list[dt.datetime] = []
        while cur <= end:
            out.append(cur)
            cur += dt.timedelta(minutes=MOCK_SLOT_STEP_MIN)
        return out

    def _day_key(day: dt.date) -> str:
        return day.strftime("%Y%m%d")

    def _parse_day_key(raw: str) -> dt.date:
        return dt.datetime.strptime(raw, "%Y%m%d").date()

    def _is_student_user(user_id: int) -> bool:
        return _active_student(user_id) is not None

    def _mock_menu_kb(owner_tg_id: int) -> InlineKeyboardMarkup:
        sub = repo_mocks.is_subscribed(conn, owner_tg_id)
        kb = InlineKeyboardMarkup()
        kb.row(
            InlineKeyboardButton(
                f"{'✅' if sub else '⬜️'} Получать уведомления о моке",
                callback_data="u:mocks:sub:toggle",
            )
        )
        kb.row(InlineKeyboardButton("🤝 Напарник для мока", callback_data="u:mocks:find"))
        kb.row(InlineKeyboardButton("📋 Мой мок", callback_data="u:mocks:my"))
        kb.row(InlineKeyboardButton("⬅️ В меню", callback_data="u:cancel"))
        return kb

    def _fmt_student_label(username: str | None, fio: str | None) -> str:
        uname = str(username or "").strip()
        if uname:
            return uname if uname.startswith("@") else f"@{uname}"
        return str(fio or "ученик").strip() or "ученик"

    def _pair_users_text(pair: sqlite3.Row) -> str:
        a = _fmt_student_label(pair["student_a_username"], pair["student_a_fio"])
        b = _fmt_student_label(pair["student_b_username"], pair["student_b_fio"]) if pair["student_b_id"] else "—"
        return f"{a} {b}".strip()

    def _pair_card_text(pair: sqlite3.Row) -> str:
        direction = str(pair["direction_name"] or "—")
        users = _pair_users_text(pair)
        status = str(pair["status"] or "")
        status_label = {
            "waiting": "Ищем напарника",
            "paired": "Пара найдена",
            "scheduled": "Назначено",
            "canceled": "Отменено",
            "done": "Завершено",
        }.get(status, status)
        when = format_dt_ddmmyyyy_hhmm(pair["start_at"], tz) if pair["start_at"] else "не назначено"
        link = str(pair["meet_link"] or "").strip() or "не указана"
        lines = [
            "<b>Моки</b>",
            f"Статус: <b>{status_label}</b>",
            f"Направление: <b>{direction}</b>",
            f"Участники: {users}",
            f"Время: <b>{when}</b>",
            f"Ссылка: {link}",
        ]
        return "\n".join(lines)

    def _user_in_pair(pair: sqlite3.Row, owner_tg_id: int) -> bool:
        a_owner = int(pair["student_a_owner_tg_id"] or 0)
        b_owner = int(pair["student_b_owner_tg_id"] or 0)
        return int(owner_tg_id) in {a_owner, b_owner}

    def _pair_kb(pair: sqlite3.Row, owner_tg_id: int) -> InlineKeyboardMarkup:
        kb = InlineKeyboardMarkup()
        pid = int(pair["id"])
        status = str(pair["status"] or "")
        if _user_in_pair(pair, owner_tg_id) and status in {"paired", "scheduled"}:
            kb.row(InlineKeyboardButton("🕘 Выбрать слот", callback_data=f"u:mocks:slots:day:{pid}:{_day_key(now(tz).date())}"))
            kb.row(InlineKeyboardButton("🔗 Указать ссылку", callback_data=f"u:mocks:set_link:{pid}"))
            kb.row(InlineKeyboardButton("❌ Отменить мок", callback_data=f"u:mocks:cancel:{pid}"))
        kb.row(InlineKeyboardButton("⬅️ Назад", callback_data="u:mocks"))
        return kb

    def _send_pair_to_participants(pair: sqlite3.Row) -> None:
        for owner_id in (int(pair["student_a_owner_tg_id"] or 0), int(pair["student_b_owner_tg_id"] or 0)):
            if owner_id <= 0:
                continue
            try:
                bot.send_message(owner_id, _pair_card_text(pair), reply_markup=_pair_kb(pair, owner_id))
            except Exception:
                continue

    def _announce_if_ready(pair_id: int) -> None:
        pair = repo_mocks.get_pair(conn, pair_id)
        if not pair:
            return
        if str(pair["status"] or "") not in {"scheduled"}:
            return
        link = str(pair["meet_link"] or "").strip()
        if not pair["start_at"] or not link:
            return
        if pair["announced_at"]:
            return
        who = _pair_users_text(pair)
        direction = str(pair["direction_name"] or "—")
        at = dt.datetime.fromisoformat(str(pair["start_at"]))
        text = (
            f"Мок будет проходить у {who} по направлению {direction} в {at.strftime('%H:%M')}\n\n"
            f"подключиться по ссылке - {link}"
        )
        subs = repo_mocks.list_subscribed_student_user_ids(conn)
        participants = {int(pair["student_a_owner_tg_id"] or 0), int(pair["student_b_owner_tg_id"] or 0)}
        for uid in subs:
            try:
                if int(uid) in participants:
                    continue
                bot.send_message(int(uid), text)
            except Exception:
                continue
        for uid in participants:
            if uid <= 0:
                continue
            try:
                kb = InlineKeyboardMarkup()
                kb.row(InlineKeyboardButton("❌ Отменить мок", callback_data=f"u:mocks:cancel:{int(pair['id'])}"))
                kb.row(InlineKeyboardButton("📋 Открыть мок", callback_data=f"u:mocks:open:{int(pair['id'])}"))
                bot.send_message(int(uid), text, reply_markup=kb)
            except Exception:
                continue
        repo_mocks.mark_announced(conn, tz, int(pair["id"]))

    @bot.callback_query_handler(func=lambda c: c.data == "u:mocks")
    def cb_mocks_menu(c: CallbackQuery):
        if is_admin(ctx, c.from_user.id):
            bot.answer_callback_query(c.id, "Это раздел ученика")
            return
        if not _is_student_user(int(c.from_user.id)):
            bot.answer_callback_query(c.id, "Раздел доступен только ученикам")
            return
        bot.edit_message_text(
            "<b>Моки</b>\n"
            "Здесь можно найти напарника по своему направлению, назначить слот и ссылку.\n"
            "Уведомления о моках получают только подписанные ученики.",
            c.message.chat.id,
            c.message.message_id,
            reply_markup=_mock_menu_kb(int(c.from_user.id)),
        )
        bot.answer_callback_query(c.id)

    @bot.callback_query_handler(func=lambda c: c.data == "u:mocks:sub:toggle")
    def cb_mocks_sub_toggle(c: CallbackQuery):
        if is_admin(ctx, c.from_user.id):
            bot.answer_callback_query(c.id, "Это раздел ученика")
            return
        owner_id = int(c.from_user.id)
        if not _is_student_user(owner_id):
            bot.answer_callback_query(c.id, "Раздел доступен только ученикам")
            return
        new_enabled = not repo_mocks.is_subscribed(conn, owner_id)
        repo_mocks.set_subscribed(conn, tz, owner_id, new_enabled)
        bot.edit_message_reply_markup(c.message.chat.id, c.message.message_id, reply_markup=_mock_menu_kb(owner_id))
        bot.answer_callback_query(c.id, "Включено" if new_enabled else "Выключено")

    @bot.callback_query_handler(func=lambda c: c.data == "u:mocks:my")
    def cb_mocks_my(c: CallbackQuery):
        if is_admin(ctx, c.from_user.id):
            bot.answer_callback_query(c.id, "Это раздел ученика")
            return
        student = _active_student(int(c.from_user.id))
        if not student:
            bot.answer_callback_query(c.id, "Раздел доступен только ученикам")
            return
        pair = repo_mocks.get_active_pair_for_student(conn, int(student["id"]))
        if not pair:
            bot.edit_message_text(
                "Активного мока пока нет. Нажми «Напарник для мока».",
                c.message.chat.id,
                c.message.message_id,
                reply_markup=_mock_menu_kb(int(c.from_user.id)),
            )
            bot.answer_callback_query(c.id)
            return
        bot.edit_message_text(
            _pair_card_text(pair),
            c.message.chat.id,
            c.message.message_id,
            reply_markup=_pair_kb(pair, int(c.from_user.id)),
        )
        bot.answer_callback_query(c.id)

    @bot.callback_query_handler(func=lambda c: c.data == "u:mocks:find")
    def cb_mocks_find(c: CallbackQuery):
        if is_admin(ctx, c.from_user.id):
            bot.answer_callback_query(c.id, "Это раздел ученика")
            return
        owner_id = int(c.from_user.id)
        student = _active_student(owner_id)
        if not student:
            bot.answer_callback_query(c.id, "Раздел доступен только ученикам")
            return
        if not student["direction_id"]:
            bot.answer_callback_query(c.id, "Не задано направление в анкете")
            bot.edit_message_text(
                "Не удалось начать поиск: у тебя не заполнено направление в анкете.",
                c.message.chat.id,
                c.message.message_id,
                reply_markup=user_menu_kb(show_pay=False),
            )
            return
        with MOCK_LOCK:
            active = repo_mocks.get_active_pair_for_student(conn, int(student["id"]))
            if active:
                pair = active
            else:
                waiting = repo_mocks.find_waiting_pair(conn, int(student["direction_id"]), int(student["id"]))
                if waiting and repo_mocks.attach_partner(conn, tz, int(waiting["id"]), int(student["id"])):
                    pair = repo_mocks.get_pair(conn, int(waiting["id"]))
                else:
                    pid = repo_mocks.create_waiting_pair(conn, tz, int(student["direction_id"]), int(student["id"]))
                    pair = repo_mocks.get_pair(conn, pid)
        if not pair:
            bot.answer_callback_query(c.id, "Ошибка, попробуй ещё раз")
            return
        status = str(pair["status"] or "")
        if status == "waiting":
            bot.edit_message_text(
                "Ищем напарника твоего направления. Как только пара найдётся — придёт уведомление.",
                c.message.chat.id,
                c.message.message_id,
                reply_markup=_pair_kb(pair, owner_id),
            )
            bot.answer_callback_query(c.id, "Ожидание напарника")
            return
        _send_pair_to_participants(pair)
        bot.edit_message_text(
            "Пара найдена ✅\nНазначьте время и ссылку для мока.",
            c.message.chat.id,
            c.message.message_id,
            reply_markup=_pair_kb(pair, owner_id),
        )
        bot.answer_callback_query(c.id, "Пара найдена")

    @bot.callback_query_handler(func=lambda c: c.data.startswith("u:mocks:open:"))
    def cb_mocks_open(c: CallbackQuery):
        if is_admin(ctx, c.from_user.id):
            bot.answer_callback_query(c.id, "Это раздел ученика")
            return
        pair_id = int(c.data.split(":")[3])
        pair = repo_mocks.get_pair(conn, pair_id)
        if not pair:
            bot.answer_callback_query(c.id, "Мок не найден")
            return
        bot.edit_message_text(
            _pair_card_text(pair),
            c.message.chat.id,
            c.message.message_id,
            reply_markup=_pair_kb(pair, int(c.from_user.id)),
        )
        bot.answer_callback_query(c.id)

    @bot.callback_query_handler(func=lambda c: c.data.startswith("u:mocks:slots:day:"))
    def cb_mock_slots_day(c: CallbackQuery):
        if is_admin(ctx, c.from_user.id):
            bot.answer_callback_query(c.id, "Это раздел ученика")
            return
        parts = c.data.split(":")
        if len(parts) != 6:
            bot.answer_callback_query(c.id, "Некорректный запрос")
            return
        pair_id = int(parts[4])
        day = _parse_day_key(parts[5])
        if day < now(tz).date():
            day = now(tz).date()
        pair = repo_mocks.get_pair(conn, pair_id)
        if not pair:
            bot.answer_callback_query(c.id, "Мок не найден")
            return
        if not _user_in_pair(pair, int(c.from_user.id)):
            bot.answer_callback_query(c.id, "Только участники мока")
            return
        kb = InlineKeyboardMarkup()
        prev_d = day - dt.timedelta(days=1)
        next_d = day + dt.timedelta(days=1)
        if day > now(tz).date():
            kb.row(InlineKeyboardButton("⬅️", callback_data=f"u:mocks:slots:day:{pair_id}:{_day_key(prev_d)}"))
        kb.row(
            InlineKeyboardButton("Сегодня", callback_data=f"u:mocks:slots:day:{pair_id}:{_day_key(now(tz).date())}"),
            InlineKeyboardButton("➡️", callback_data=f"u:mocks:slots:day:{pair_id}:{_day_key(next_d)}"),
        )
        for i, sl in enumerate([x for x in _slot_starts(day) if x > now(tz)]):
            if i % 3 == 0:
                row: list[InlineKeyboardButton] = []
            row.append(InlineKeyboardButton(sl.strftime("%H:%M"), callback_data=f"u:mocks:slots:set:{pair_id}:{_day_key(day)}:{sl.strftime('%H%M')}"))
            if len(row) == 3:
                kb.row(*row)
        if "row" in locals() and row:
            kb.row(*row)
        kb.row(InlineKeyboardButton("⬅️ Назад", callback_data=f"u:mocks:open:{pair_id}"))
        bot.edit_message_text(
            f"<b>Выбор времени мока</b> • {day.strftime('%d.%m.%Y')}\nВыбери слот начала.",
            c.message.chat.id,
            c.message.message_id,
            reply_markup=kb,
        )
        bot.answer_callback_query(c.id)

    @bot.callback_query_handler(func=lambda c: c.data.startswith("u:mocks:slots:set:"))
    def cb_mock_slots_set(c: CallbackQuery):
        if is_admin(ctx, c.from_user.id):
            bot.answer_callback_query(c.id, "Это раздел ученика")
            return
        parts = c.data.split(":")
        if len(parts) != 7:
            bot.answer_callback_query(c.id, "Некорректный запрос")
            return
        pair_id = int(parts[4])
        day = _parse_day_key(parts[5])
        hhmm = parts[6]
        h = int(hhmm[:2])
        m = int(hhmm[2:])
        start_at = dt.datetime.combine(day, dt.time(h, m)).replace(tzinfo=tzinfo(tz))
        pair = repo_mocks.get_pair(conn, pair_id)
        if not pair:
            bot.answer_callback_query(c.id, "Мок не найден")
            return
        if not _user_in_pair(pair, int(c.from_user.id)):
            bot.answer_callback_query(c.id, "Только участники мока")
            return
        if start_at <= now(tz):
            bot.answer_callback_query(c.id, "Время уже прошло")
            return
        if not repo_mocks.set_start_time(conn, tz, pair_id, iso(start_at), int(c.from_user.id)):
            bot.answer_callback_query(c.id, "Не удалось обновить время")
            return
        pair = repo_mocks.get_pair(conn, pair_id)
        if pair:
            _send_pair_to_participants(pair)
            _announce_if_ready(pair_id)
        bot.edit_message_text(
            "Время мока обновлено ✅",
            c.message.chat.id,
            c.message.message_id,
            reply_markup=_pair_kb(pair, int(c.from_user.id)) if pair else _mock_menu_kb(int(c.from_user.id)),
        )
        bot.answer_callback_query(c.id, "Время сохранено")

    @bot.callback_query_handler(func=lambda c: c.data.startswith("u:mocks:set_link:"))
    def cb_mock_set_link(c: CallbackQuery):
        if is_admin(ctx, c.from_user.id):
            bot.answer_callback_query(c.id, "Это раздел ученика")
            return
        pair_id = int(c.data.split(":")[3])
        pair = repo_mocks.get_pair(conn, pair_id)
        if not pair:
            bot.answer_callback_query(c.id, "Мок не найден")
            return
        if not _user_in_pair(pair, int(c.from_user.id)):
            bot.answer_callback_query(c.id, "Только участники мока")
            return
        repo_states.set_state(conn, int(c.from_user.id), "MOCK_SET_LINK", {"pair_id": pair_id}, tz)
        bot.send_message(c.message.chat.id, "Отправь ссылку на мок (Яндекс Телемост/другая):")
        bot.answer_callback_query(c.id)

    @bot.callback_query_handler(func=lambda c: c.data.startswith("u:mocks:cancel:"))
    def cb_mock_cancel_ask(c: CallbackQuery):
        if is_admin(ctx, c.from_user.id):
            bot.answer_callback_query(c.id, "Это раздел ученика")
            return
        pair_id = int(c.data.split(":")[3])
        pair = repo_mocks.get_pair(conn, pair_id)
        if not pair:
            bot.answer_callback_query(c.id, "Мок не найден")
            return
        if not _user_in_pair(pair, int(c.from_user.id)):
            bot.answer_callback_query(c.id, "Только участники мока")
            return
        kb = InlineKeyboardMarkup()
        kb.row(
            InlineKeyboardButton("Да, отменить", callback_data=f"u:mocks:cancelok:{pair_id}"),
            InlineKeyboardButton("Нет", callback_data=f"u:mocks:open:{pair_id}"),
        )
        bot.edit_message_text("Отменить мок?", c.message.chat.id, c.message.message_id, reply_markup=kb)
        bot.answer_callback_query(c.id)

    @bot.callback_query_handler(func=lambda c: c.data.startswith("u:mocks:cancelok:"))
    def cb_mock_cancel_ok(c: CallbackQuery):
        if is_admin(ctx, c.from_user.id):
            bot.answer_callback_query(c.id, "Это раздел ученика")
            return
        pair_id = int(c.data.split(":")[3])
        pair = repo_mocks.get_pair(conn, pair_id)
        if not pair:
            bot.answer_callback_query(c.id, "Мок не найден")
            return
        if not _user_in_pair(pair, int(c.from_user.id)):
            bot.answer_callback_query(c.id, "Только участники мока")
            return
        if repo_mocks.cancel_pair(conn, tz, pair_id, int(c.from_user.id), reason="user_cancel"):
            pair = repo_mocks.get_pair(conn, pair_id)
            for uid in {int(pair["student_a_owner_tg_id"] or 0), int(pair["student_b_owner_tg_id"] or 0)}:
                if uid <= 0:
                    continue
                try:
                    bot.send_message(uid, "Мок отменён одним из участников.")
                except Exception:
                    continue
            bot.edit_message_text("Мок отменён.", c.message.chat.id, c.message.message_id, reply_markup=_mock_menu_kb(int(c.from_user.id)))
            bot.answer_callback_query(c.id, "Отменено")
            return
        bot.answer_callback_query(c.id, "Уже отменён")

    @bot.message_handler(func=lambda m: repo_states.get_state(conn, int(m.from_user.id))[0] == "MOCK_SET_LINK", content_types=["text"])
    def msg_mock_set_link(m: Message):
        if is_admin(ctx, m.from_user.id):
            return
        st, data = repo_states.get_state(conn, int(m.from_user.id))
        if st != "MOCK_SET_LINK":
            return
        data = data or {}
        pair_id = int(data.get("pair_id") or 0)
        pair = repo_mocks.get_pair(conn, pair_id)
        if not pair:
            repo_states.clear_state(conn, int(m.from_user.id), tz)
            bot.send_message(m.chat.id, "Мок не найден.", reply_markup=_mock_menu_kb(int(m.from_user.id)))
            return
        if not _user_in_pair(pair, int(m.from_user.id)):
            repo_states.clear_state(conn, int(m.from_user.id), tz)
            bot.send_message(m.chat.id, "Недостаточно прав.", reply_markup=_mock_menu_kb(int(m.from_user.id)))
            return
        link = (m.text or "").strip()
        if not link.startswith("http"):
            bot.send_message(m.chat.id, "Нужна ссылка формата https://...")
            return
        repo_mocks.set_meet_link(conn, tz, pair_id, link, int(m.from_user.id))
        repo_states.clear_state(conn, int(m.from_user.id), tz)
        pair = repo_mocks.get_pair(conn, pair_id)
        if pair:
            _send_pair_to_participants(pair)
            _announce_if_ready(pair_id)
        bot.send_message(m.chat.id, "Ссылка сохранена ✅", reply_markup=_pair_kb(pair, int(m.from_user.id)) if pair else _mock_menu_kb(int(m.from_user.id)))
