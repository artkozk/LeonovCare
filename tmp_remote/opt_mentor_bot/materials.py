from __future__ import annotations

import logging
import re
import sqlite3

from telebot import TeleBot
from telebot.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message

from app.db import repo_admins, repo_material_progress, repo_materials, repo_states, repo_analytics
from app.handlers.common import admin_has as common_admin_has
from app.ui.formatters import esc

log = logging.getLogger(__name__)

_STATE_SET_GROUP_ID = "MAT_SET_GROUP_ID"
_STATE_SET_INVITE = "MAT_SET_INVITE"
_STATE_ADD_END_TITLE = "MAT_ADD_END_TITLE"
_STATE_ADD_END_URL = "MAT_ADD_END_URL"
_STATE_INSERT_TITLE = "MAT_INSERT_TITLE"
_STATE_INSERT_URL = "MAT_INSERT_URL"
_STATE_REPLACE_TITLE = "MAT_REPLACE_TITLE"
_STATE_REPLACE_URL = "MAT_REPLACE_URL"
_STATE_EDIT_TITLE = "MAT_EDIT_TITLE"
_STATE_EDIT_URL = "MAT_EDIT_URL"
_STATE_SET_TOPIC = "MAT_SET_TOPIC"
_STATE_DETECT_TOPIC = "MAT_DETECT_TOPIC"


def _is_member_status(status: object) -> bool:
    raw = getattr(status, "value", status)
    s = str(raw or "").strip().lower()
    if "." in s:
        s = s.split(".")[-1]
    return s in {"member", "administrator", "creator", "restricted"}


def _material_dirs_kb(prefix: str, with_back: bool = True, back_cb: str = "v2:home") -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup()
    kb.row(
        InlineKeyboardButton("java", callback_data=f"{prefix}:java"),
        InlineKeyboardButton("golang", callback_data=f"{prefix}:golang"),
    )
    kb.row(
        InlineKeyboardButton("frontend", callback_data=f"{prefix}:frontend"),
        InlineKeyboardButton("python", callback_data=f"{prefix}:python"),
    )
    if with_back:
        kb.row(InlineKeyboardButton("⬅️ Назад", callback_data=back_cb))
    return kb


def _truncate_label(text: str, max_len: int = 34) -> str:
    s = (text or "").strip()
    return (s[: max_len - 1] + "…") if len(s) > max_len else s


def _admin_back_main_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup()
    kb.row(InlineKeyboardButton("⬅️ В материалы", callback_data="m:materials_admin"))
    return kb


_ROADMAP_RE_FULL = re.compile(
    r'[Бб]лок\s*(\d+)\s*[.,;:\-—|/\s]+[Шш]аг\s*(\d+)\s*[.,;:\-—|/\s]+(.+)', re.DOTALL,
)
_ROADMAP_RE_NOTEXT = re.compile(
    r'[Бб]лок\s*(\d+)\s*[.,;:\-—|/\s]+[Шш]аг\s*(\d+)\s*$',
)
_ROADMAP_RE_SHORT = re.compile(r'(\d+)[./](\d+)\.?\s+(.+)', re.DOTALL)
_ROADMAP_RE_SHORT_NOTEXT = re.compile(r'(\d+)[./](\d+)\s*$')

_ALL_DIRECTIONS_GROUPS: tuple[tuple[str, tuple[tuple[str, str], ...]], ...] = (
    (
        "Backend",
        (
            ("Go / Golang", "https://t.me/c/3730365848/79/81"),
            ("Python", "https://t.me/c/3730365848/235/236"),
            ("Java", "https://t.me/c/3730365848/143/145"),
            ("C# / .NET", "https://disk.yandex.ru/d/h5LvIYEMGiOfpg"),
            ("PHP", "https://disk.yandex.ru/d/jI2-rwil1HOQdg"),
            ("Backend", "https://disk.yandex.ru/d/KidrCSXDcfq76g"),
        ),
    ),
    (
        "Frontend",
        (
            ("Frontend", "https://t.me/c/3730365848/264/265"),
        ),
    ),
    (
        "Mobile",
        (
            ("Android", "https://disk.yandex.ru/d/pIWllaQu1kx3gw"),
            ("Flutter", "https://disk.yandex.ru/d/DYgc5dTLQCYugg"),
            ("iOS", "https://disk.yandex.ru/d/qdnEi_LXLtgD9Q"),
        ),
    ),
    (
        "Data & ML",
        (
            ("Machine Learning / Data Science", "https://disk.yandex.ru/d/mqtGr4vAMQmFrQ"),
            ("Data Engineer", "https://disk.yandex.ru/d/Xxu4CEzSZJZeyw"),
        ),
    ),
    (
        "QA",
        (
            ("QA / Тестирование", "https://disk.yandex.ru/d/lTc7rz9bYb78ww"),
            ("AQA / Автотестирование", "https://disk.yandex.ru/d/nVAEyzJnmRDr4Q"),
        ),
    ),
    (
        "DevOps",
        (
            ("DevOps / SRE", "https://disk.yandex.ru/d/qAcqx-IEnR1i7Q"),
        ),
    ),
    (
        "Аналитика",
        (
            ("Системный аналитик", "https://disk.yandex.ru/d/C9BwqAnCOtURNA"),
            ("Аналитик данных / Продуктовая аналитика", "https://disk.yandex.ru/d/ON5uk0e-yC7yfA"),
            ("Бизнес-аналитик", "https://disk.yandex.ru/d/l24ajFb7cvjMtQ"),
        ),
    ),
    (
        "Management",
        (
            ("Project Manager", "https://disk.yandex.ru/d/sWfDR8mOyPi4PA"),
            ("Product Manager", "https://disk.yandex.ru/d/uKczdzFQW1izpw"),
        ),
    ),
    (
        "Security",
        (
            ("Информационная безопасность", "https://disk.yandex.ru/d/kaRRuZdJFNEkZA"),
        ),
    ),
    (
        "Карьера",
        (
            ("Валютные удаленки / Linkedin", "https://disk.yandex.ru/d/tY4-EfYiz_37oA"),
        ),
    ),
    (
        "База",
        (
            ("Git и GitHub", "https://disk.yandex.ru/d/tZRQ4iaqu36aVg"),
            ("Linux и Bash", "https://disk.yandex.ru/d/FR-FD-D6nSv87A"),
            ("HTTP и REST API", "https://disk.yandex.ru/d/c4hiTH7jJGqMOA"),
            ("PostgreSQL", "https://disk.yandex.ru/d/xjSmgv9Z2U6Khw"),
            ("Redis", "https://disk.yandex.ru/d/EJcl_tBhjCEDlA"),
            ("Docker и Docker Compose", "https://disk.yandex.ru/d/7QR1_Njjm6_MBA"),
            ("Nginx", "https://disk.yandex.ru/d/hkuEEt8F2H9hoQ"),
            ("GitHub Actions и CI/CD", "https://disk.yandex.ru/d/w7N32ktfeM1M7w"),
        ),
    ),
)


def _parse_roadmap_message(text: str) -> tuple[int, int, str] | None:
    if not text or not text.strip():
        return None
    lines = text.strip().split('\n')
    first = lines[0].strip()

    m = _ROADMAP_RE_FULL.match(first)
    if m:
        return int(m.group(1)), int(m.group(2)), m.group(3).strip()

    m = _ROADMAP_RE_NOTEXT.match(first)
    if m and len(lines) > 1:
        title = lines[1].strip()
        if title:
            return int(m.group(1)), int(m.group(2)), title

    m = _ROADMAP_RE_SHORT.match(first)
    if m:
        return int(m.group(1)), int(m.group(2)), m.group(3).strip()

    m = _ROADMAP_RE_SHORT_NOTEXT.match(first)
    if m and len(lines) > 1:
        title = lines[1].strip()
        if title:
            return int(m.group(1)), int(m.group(2)), title

    return None


def init(bot: TeleBot, ctx: dict) -> None:
    conn: sqlite3.Connection = ctx["conn"]
    tz = ctx["cfg"].TZ
    cfg = ctx["cfg"]

    def _is_admin(user_id: int) -> bool:
        return common_admin_has(ctx, int(user_id), repo_admins.PERM_MATERIALS_MANAGE)

    def _effective_settings() -> dict:
        st = repo_materials.get_settings(conn)
        if getattr(cfg, "MATERIALS_GROUP_CHAT_ID", None) is not None:
            try:
                st["group_chat_id"] = int(cfg.MATERIALS_GROUP_CHAT_ID)
            except Exception:
                st["group_chat_id"] = None
        cfg_invite = (getattr(cfg, "MATERIALS_GROUP_INVITE_URL", "") or "").strip()
        if cfg_invite:
            st["invite_url"] = cfg_invite
        return st

    def _check_subscription(user_id: int) -> tuple[bool, str]:
        settings = _effective_settings()
        if not settings["require_subscription"]:
            return True, ""
        group_chat_id = settings["group_chat_id"]
        if not group_chat_id:
            return False, "group_not_configured"
        try:
            member = bot.get_chat_member(group_chat_id, int(user_id))
            status = getattr(member, "status", "") or ""
            if _is_member_status(status):
                return True, ""
            return False, "not_member"
        except Exception:
            return False, "cannot_check"

    def _build_user_gate(reason: str) -> tuple[str, InlineKeyboardMarkup]:
        settings = _effective_settings()
        invite_url = (settings["invite_url"] or "").strip()
        if reason == "group_not_configured":
            text = (
                "<b>Бесплатные материалы</b>\n"
                "Раздел пока недоступен: не задан ID обязательной группы.\n"
                "Укажи MATERIALS_GROUP_CHAT_ID в app/config.py (или env), либо задай ID в админке материалов."
            )
        elif reason == "cannot_check":
            text = (
                "<b>Бесплатные материалы</b>\n"
                "Не удалось проверить подписку на группу.\n"
                "Проверь, что у группы корректный chat_id и бот добавлен в группу (лучше админом)."
            )
        else:
            text = (
                "<b>Бесплатные материалы</b>\n"
                "Доступ только для участников обязательной Telegram-группы."
            )

        kb = InlineKeyboardMarkup()
        if invite_url:
            kb.row(InlineKeyboardButton("Перейти в группу", url=invite_url))
        kb.row(InlineKeyboardButton("Проверить подписку", callback_data="mat:user:check"))
        kb.row(InlineKeyboardButton("⬅️ В меню", callback_data="v2:home"))
        return text, kb

    def _render_user_gate(chat_id: int, message_id: int, reason: str) -> None:
        text, kb = _build_user_gate(reason)
        bot.edit_message_text(text, chat_id, message_id, reply_markup=kb)

    def _send_user_gate(chat_id: int, reason: str) -> None:
        text, kb = _build_user_gate(reason)
        bot.send_message(chat_id, text, reply_markup=kb)

    def _build_user_directions(user_id: int) -> tuple[str, InlineKeyboardMarkup]:
        overall = repo_material_progress.overall_stats(conn, user_id)
        text = (
            "<b>Бесплатные материалы</b>\n"
            f"Общий прогресс: <b>{overall['percent']}%</b> ({overall['done']} из {overall['total']})\n\n"
            "Выбери направление:"
        )
        kb = InlineKeyboardMarkup()
        for d in repo_materials.DIRECTIONS:
            st = repo_material_progress.direction_stats(conn, user_id, d)
            kb.row(
                InlineKeyboardButton(
                    f"{d} • {st['percent']}% ({st['done']}/{st['total']})",
                    callback_data=f"mat:user:dir:{d}",
                )
            )
        kb.row(InlineKeyboardButton("🌐 Все направления", callback_data="mat:user:all_dirs"))
        kb.row(InlineKeyboardButton("⬅️ В меню", callback_data="v2:home"))
        return text, kb

    def _render_user_directions(chat_id: int, message_id: int, user_id: int) -> None:
        text, kb = _build_user_directions(user_id)
        bot.edit_message_text(text, chat_id, message_id, reply_markup=kb)

    def _send_user_directions(chat_id: int, user_id: int) -> None:
        text, kb = _build_user_directions(user_id)
        bot.send_message(chat_id, text, reply_markup=kb)

    def _render_all_directions(chat_id: int, message_id: int) -> None:
        text_lines = ["<b>Все направления</b>", "Выбери направление:"]
        kb = InlineKeyboardMarkup()
        for group_title, items in _ALL_DIRECTIONS_GROUPS:
            for label, url in items:
                kb.row(InlineKeyboardButton(label, url=url))
        kb.row(InlineKeyboardButton("⬅️ К направлениям", callback_data="mat:user:dirs"))
        bot.edit_message_text("\n".join(text_lines), chat_id, message_id, reply_markup=kb)

    def _render_user_direction(chat_id: int, message_id: int, user_id: int, direction: str) -> None:
        d = repo_materials.normalize_direction(direction)
        if not d:
            bot.edit_message_text("Неизвестное направление.", chat_id, message_id)
            return

        blocks = repo_materials.list_blocks(conn, d)
        if blocks:
            _render_user_blocks(chat_id, message_id, user_id, d, blocks)
            return

        rows = repo_materials.list_steps(conn, d)
        st = repo_material_progress.direction_stats(conn, user_id, d)

        text = (
            f"<b>Roadmap: {d}</b>\n"
            f"Прогресс направления: <b>{st['percent']}%</b>\n"
            f"Пройдено: {st['done']} из {st['total']}"
        )
        kb = InlineKeyboardMarkup()
        if not rows:
            text += "\n\nШагов пока нет."
        else:
            done_map = repo_material_progress.done_map(conn, user_id, d)
            for r in rows:
                mark = "✅" if done_map.get(int(r["id"]), False) else "▫️"
                label = _truncate_label(f"{mark} {int(r['position'])}. {r['title']}", 48)
                kb.row(InlineKeyboardButton(label, callback_data=f"mat:user:step:{int(r['id'])}"))
        kb.row(InlineKeyboardButton("⬅️ К направлениям", callback_data="mat:user:dirs"))
        bot.edit_message_text(text, chat_id, message_id, reply_markup=kb)

    def _render_user_blocks(chat_id: int, message_id: int, user_id: int, d: str, blocks: list) -> None:
        st = repo_material_progress.direction_stats(conn, user_id, d)
        text = (
            f"<b>Roadmap: {d}</b>\n"
            f"Прогресс: <b>{st['percent']}%</b> ({st['done']} из {st['total']})\n\n"
            "Выбери блок:"
        )
        kb = InlineKeyboardMarkup()
        for b in blocks:
            bn = int(b["block_number"])
            bt = b["block_title"] or f"Блок {bn}"
            bs = repo_material_progress.block_stats(conn, user_id, d, bn)
            label = _truncate_label(f"📦 {bt} ({bs['done']}/{bs['total']})", 48)
            kb.row(InlineKeyboardButton(label, callback_data=f"mat:user:block:{d}:{bn}"))
        kb.row(InlineKeyboardButton("⬅️ К направлениям", callback_data="mat:user:dirs"))
        bot.edit_message_text(text, chat_id, message_id, reply_markup=kb)

    def _render_user_block(chat_id: int, message_id: int, user_id: int, direction: str, block_number: int) -> None:
        d = repo_materials.normalize_direction(direction)
        if not d:
            bot.edit_message_text("Неизвестное направление.", chat_id, message_id)
            return
        rows = repo_materials.list_steps_in_block(conn, d, block_number)
        bs = repo_material_progress.block_stats(conn, user_id, d, block_number)
        block_title = rows[0]["block_title"] if rows and rows[0]["block_title"] else f"Блок {block_number}"

        text = (
            f"<b>{d} • {esc(block_title)}</b>\n"
            f"Прогресс блока: <b>{bs['percent']}%</b>\n"
            f"Пройдено: {bs['done']} из {bs['total']}"
        )
        kb = InlineKeyboardMarkup()
        if not rows:
            text += "\n\nШагов пока нет."
        else:
            done_map = repo_material_progress.done_map(conn, user_id, d)
            for r in rows:
                mark = "✅" if done_map.get(int(r["id"]), False) else "▫️"
                label = _truncate_label(f"{mark} {int(r['position'])}. {r['title']}", 48)
                kb.row(InlineKeyboardButton(label, callback_data=f"mat:user:step:{int(r['id'])}"))
        kb.row(InlineKeyboardButton(f"⬅️ Блоки ({d})", callback_data=f"mat:user:dir:{d}"))
        bot.edit_message_text(text, chat_id, message_id, reply_markup=kb)

    def _render_user_step(chat_id: int, message_id: int, user_id: int, step_id: int) -> None:
        row = repo_materials.get_step(conn, step_id)
        if not row or int(row["is_active"] or 0) != 1:
            bot.edit_message_text("Шаг не найден.", chat_id, message_id)
            return
        d = row["direction"]
        block_number = int(row["block_number"]) if row["block_number"] else 0
        block_title = row["block_title"] if row["block_title"] else ""
        done = repo_material_progress.is_done(conn, user_id, int(row["id"]))
        status = "Пройдено ✅" if done else "Не пройдено ▫️"

        if block_number > 0:
            header = f"<b>{d} • {esc(block_title or f'Блок {block_number}')} • Шаг {int(row['position'])}</b>"
        else:
            header = f"<b>{d} • шаг {int(row['position'])}</b>"

        text = (
            f"{header}\n"
            f"{esc(row['title'])}\n\n"
            f"Статус: <b>{status}</b>"
        )
        kb = InlineKeyboardMarkup()
        kb.row(InlineKeyboardButton("📖 Перейти в группу", url=row["message_url"]))
        if done:
            kb.row(InlineKeyboardButton("Снять отметку", callback_data=f"mat:user:undone:{int(row['id'])}"))
        else:
            kb.row(InlineKeyboardButton("Отметить пройдено", callback_data=f"mat:user:done:{int(row['id'])}"))
        if block_number > 0:
            kb.row(InlineKeyboardButton("⬅️ Назад к блоку", callback_data=f"mat:user:block:{d}:{block_number}"))
        else:
            kb.row(InlineKeyboardButton("⬅️ Назад к roadmap", callback_data=f"mat:user:dir:{d}"))
        bot.edit_message_text(text, chat_id, message_id, reply_markup=kb)

    def _render_admin_menu(chat_id: int, message_id: int) -> None:
        st = _effective_settings()
        group_id = st["group_chat_id"] if st["group_chat_id"] is not None else "не задан"
        invite = st["invite_url"] or "не задана"
        req = "включена" if st["require_subscription"] else "выключена"
        text = (
            "<b>Материалы: админка</b>\n\n"
            f"Обязательная подписка: <b>{req}</b>\n"
            f"ID группы: <code>{group_id}</code>\n"
            f"Ссылка-приглашение: <code>{esc(invite)}</code>"
        )
        kb = InlineKeyboardMarkup()
        kb.row(
            InlineKeyboardButton("✏️ ID группы", callback_data="mat:admin:set_gid"),
            InlineKeyboardButton("✏️ Ссылка группы", callback_data="mat:admin:set_inv"),
        )
        kb.row(InlineKeyboardButton("🔁 Вкл/Выкл подписку", callback_data="mat:admin:toggle_sub"))
        kb.row(InlineKeyboardButton("🛠 Редактор roadmap", callback_data="mat:admin:dirs"))
        kb.row(InlineKeyboardButton("🔄 Sync из канала", callback_data="mat:admin:sync"))
        kb.row(InlineKeyboardButton("👁 Preview roadmap", callback_data="mat:admin:preview_dirs"))
        kb.row(InlineKeyboardButton("🏠 Main", callback_data="m:main"))
        bot.edit_message_text(text, chat_id, message_id, reply_markup=kb)

    def _render_admin_direction(chat_id: int, message_id: int, direction: str) -> None:
        d = repo_materials.normalize_direction(direction)
        if not d:
            bot.edit_message_text("Неизвестное направление.", chat_id, message_id)
            return
        rows = repo_materials.list_steps(conn, d)
        text = f"<b>Редактор roadmap: {d}</b>"
        if not rows:
            text += "\n\nПока нет шагов."
        else:
            text += f"\n\nШагов: <b>{len(rows)}</b>"
        kb = InlineKeyboardMarkup()
        for r in rows:
            label = _truncate_label(f"{int(r['position'])}. {r['title']}", 44)
            kb.row(InlineKeyboardButton(label, callback_data=f"mat:admin:step:{int(r['id'])}"))
        kb.row(InlineKeyboardButton("➕ Добавить шаг в конец", callback_data=f"mat:admin:add_end:{d}"))
        kb.row(InlineKeyboardButton("👁 Preview этого направления", callback_data=f"mat:admin:preview:{d}"))
        kb.row(InlineKeyboardButton("⬅️ К направлениям", callback_data="mat:admin:dirs"))
        bot.edit_message_text(text, chat_id, message_id, reply_markup=kb)

    def _render_admin_step(chat_id: int, message_id: int, step_id: int) -> None:
        row = repo_materials.get_step(conn, step_id)
        if not row or int(row["is_active"] or 0) != 1:
            bot.edit_message_text("Шаг не найден.", chat_id, message_id)
            return
        d = row["direction"]
        text = (
            f"<b>{d} • шаг {int(row['position'])}</b>\n"
            f"Название: {esc(row['title'])}\n"
            f"Ссылка: <code>{esc(row['message_url'])}</code>"
        )
        kb = InlineKeyboardMarkup()
        kb.row(
            InlineKeyboardButton("✏️ Изменить название", callback_data=f"mat:admin:edit_title:{int(row['id'])}"),
            InlineKeyboardButton("🔗 Изменить ссылку", callback_data=f"mat:admin:edit_url:{int(row['id'])}"),
        )
        kb.row(
            InlineKeyboardButton("♻️ Заменить этот пункт", callback_data=f"mat:admin:replace:{int(row['id'])}"),
            InlineKeyboardButton("➕ Вставить на это место", callback_data=f"mat:admin:insert:{int(row['id'])}"),
        )
        kb.row(InlineKeyboardButton("🗑 Удалить пункт", callback_data=f"mat:admin:del:{int(row['id'])}"))
        kb.row(InlineKeyboardButton("⬅️ К шагам", callback_data=f"mat:admin:dir:{d}"))
        bot.edit_message_text(text, chat_id, message_id, reply_markup=kb)

    def _deeplink_payload(meta: dict | None, route: str) -> dict:
        meta = meta or {}
        return {
            "route": route,
            "source": meta.get("source", ""),
            "service": meta.get("service", ""),
            "clicks": meta.get("clicks"),
            "version": meta.get("version", ""),
        }

    def _open_route_materials(chat_id: int, user_id: int, meta: dict | None = None) -> None:
        if _is_admin(user_id):
            return
        repo_states.clear_state(conn, user_id, tz)
        repo_analytics.log_event(conn, tz, user_id, "deeplink.route_open", _deeplink_payload(meta, "materials"))
        repo_analytics.log_event(conn, tz, user_id, "funnel.materials.open", {"source": "deeplink"})
        ok, reason = _check_subscription(user_id)
        if not ok:
            _send_user_gate(chat_id, reason)
            return
        repo_analytics.log_event(conn, tz, user_id, "funnel.materials.subscription_ok")
        _send_user_directions(chat_id, user_id)

    public_routes = ctx.setdefault("public_routes", {})
    if isinstance(public_routes, dict):
        public_routes["materials"] = _open_route_materials

    @bot.callback_query_handler(func=lambda c: c.data == "v2:materials")
    def cb_user_materials(c: CallbackQuery):
        if _is_admin(c.from_user.id):
            bot.answer_callback_query(c.id, "Это пользовательское меню")
            return
        repo_analytics.log_event(conn, tz, c.from_user.id, "funnel.materials.open")
        ok, reason = _check_subscription(c.from_user.id)
        if not ok:
            _render_user_gate(c.message.chat.id, c.message.message_id, reason)
            bot.answer_callback_query(c.id)
            return
        repo_analytics.log_event(conn, tz, c.from_user.id, "funnel.materials.subscription_ok")
        _render_user_directions(c.message.chat.id, c.message.message_id, c.from_user.id)
        bot.answer_callback_query(c.id)

    @bot.callback_query_handler(func=lambda c: c.data == "mat:user:check")
    def cb_user_check(c: CallbackQuery):
        if _is_admin(c.from_user.id):
            bot.answer_callback_query(c.id, "Это пользовательское меню")
            return
        ok, reason = _check_subscription(c.from_user.id)
        if not ok:
            _render_user_gate(c.message.chat.id, c.message.message_id, reason)
            bot.answer_callback_query(c.id, "Подписка не подтверждена")
            return
        repo_analytics.log_event(conn, tz, c.from_user.id, "funnel.materials.subscription_ok")
        _render_user_directions(c.message.chat.id, c.message.message_id, c.from_user.id)
        bot.answer_callback_query(c.id, "Подписка подтверждена")

    @bot.callback_query_handler(func=lambda c: c.data == "mat:user:dirs")
    def cb_user_dirs(c: CallbackQuery):
        if _is_admin(c.from_user.id):
            bot.answer_callback_query(c.id, "Это пользовательское меню")
            return
        _render_user_directions(c.message.chat.id, c.message.message_id, c.from_user.id)
        bot.answer_callback_query(c.id)

    @bot.callback_query_handler(func=lambda c: c.data == "mat:user:all_dirs")
    def cb_user_all_dirs(c: CallbackQuery):
        if _is_admin(c.from_user.id):
            bot.answer_callback_query(c.id, "Это пользовательское меню")
            return
        repo_analytics.log_event(conn, tz, c.from_user.id, "funnel.materials.all_directions_open")
        _render_all_directions(c.message.chat.id, c.message.message_id)
        bot.answer_callback_query(c.id)

    @bot.callback_query_handler(func=lambda c: c.data.startswith("mat:user:dir:"))
    def cb_user_dir(c: CallbackQuery):
        if _is_admin(c.from_user.id):
            bot.answer_callback_query(c.id, "Это пользовательское меню")
            return
        direction = c.data.split(":")[3]
        repo_analytics.log_event(conn, tz, c.from_user.id, "funnel.materials.direction_selected", {"direction": direction})
        _render_user_direction(c.message.chat.id, c.message.message_id, c.from_user.id, direction)
        bot.answer_callback_query(c.id)

    @bot.callback_query_handler(func=lambda c: c.data.startswith("mat:user:step:"))
    def cb_user_step(c: CallbackQuery):
        if _is_admin(c.from_user.id):
            bot.answer_callback_query(c.id, "Это пользовательское меню")
            return
        step_id = int(c.data.split(":")[3])
        step_row = repo_materials.get_step(conn, step_id)
        payload = {"step_id": step_id}
        if step_row:
            payload["direction"] = step_row["direction"]
            payload["position"] = int(step_row["position"])
        repo_analytics.log_event(conn, tz, c.from_user.id, "funnel.materials.step_open", payload)
        _render_user_step(c.message.chat.id, c.message.message_id, c.from_user.id, step_id)
        bot.answer_callback_query(c.id)

    @bot.callback_query_handler(func=lambda c: c.data.startswith("mat:user:done:"))
    def cb_user_done(c: CallbackQuery):
        if _is_admin(c.from_user.id):
            bot.answer_callback_query(c.id, "Это пользовательское меню")
            return
        step_id = int(c.data.split(":")[3])
        repo_material_progress.set_done(conn, tz, c.from_user.id, step_id, True)
        step_row = repo_materials.get_step(conn, step_id)
        payload = {"step_id": step_id}
        if step_row:
            payload["direction"] = step_row["direction"]
            payload["position"] = int(step_row["position"])
        repo_analytics.log_event(conn, tz, c.from_user.id, "funnel.materials.step_done", payload)
        _render_user_step(c.message.chat.id, c.message.message_id, c.from_user.id, step_id)
        bot.answer_callback_query(c.id, "Отмечено")

    @bot.callback_query_handler(func=lambda c: c.data.startswith("mat:user:undone:"))
    def cb_user_undone(c: CallbackQuery):
        if _is_admin(c.from_user.id):
            bot.answer_callback_query(c.id, "Это пользовательское меню")
            return
        step_id = int(c.data.split(":")[3])
        repo_material_progress.set_done(conn, tz, c.from_user.id, step_id, False)
        _render_user_step(c.message.chat.id, c.message.message_id, c.from_user.id, step_id)
        bot.answer_callback_query(c.id, "Снято")

    @bot.callback_query_handler(func=lambda c: c.data == "m:materials_admin")
    def cb_admin_menu(c: CallbackQuery):
        if not _is_admin(c.from_user.id):
            bot.answer_callback_query(c.id, "Нет доступа")
            return
        repo_states.clear_state(conn, c.from_user.id, tz)
        _render_admin_menu(c.message.chat.id, c.message.message_id)
        bot.answer_callback_query(c.id)

    @bot.callback_query_handler(func=lambda c: c.data == "mat:admin:set_gid")
    def cb_admin_set_gid(c: CallbackQuery):
        if not _is_admin(c.from_user.id):
            bot.answer_callback_query(c.id, "Нет доступа")
            return
        repo_states.set_state(conn, c.from_user.id, _STATE_SET_GROUP_ID, {}, tz)
        bot.send_message(c.message.chat.id, "Введи chat_id обязательной группы (пример: -1001234567890), или '-' чтобы очистить.")
        bot.answer_callback_query(c.id)

    @bot.callback_query_handler(func=lambda c: c.data == "mat:admin:set_inv")
    def cb_admin_set_inv(c: CallbackQuery):
        if not _is_admin(c.from_user.id):
            bot.answer_callback_query(c.id, "Нет доступа")
            return
        repo_states.set_state(conn, c.from_user.id, _STATE_SET_INVITE, {}, tz)
        bot.send_message(c.message.chat.id, "Введи ссылку группы (https://... или https://t.me/...), или '-' чтобы очистить.")
        bot.answer_callback_query(c.id)

    @bot.callback_query_handler(func=lambda c: c.data == "mat:admin:toggle_sub")
    def cb_admin_toggle_sub(c: CallbackQuery):
        if not _is_admin(c.from_user.id):
            bot.answer_callback_query(c.id, "Нет доступа")
            return
        st = repo_materials.get_settings(conn)
        repo_materials.set_require_subscription(conn, not st["require_subscription"])
        _render_admin_menu(c.message.chat.id, c.message.message_id)
        bot.answer_callback_query(c.id, "Обновлено")

    @bot.callback_query_handler(func=lambda c: c.data == "mat:admin:dirs")
    def cb_admin_dirs(c: CallbackQuery):
        if not _is_admin(c.from_user.id):
            bot.answer_callback_query(c.id, "Нет доступа")
            return
        text = "<b>Редактор roadmap</b>\nВыбери направление:"
        kb = _material_dirs_kb("mat:admin:dir", with_back=True, back_cb="m:materials_admin")
        bot.edit_message_text(text, c.message.chat.id, c.message.message_id, reply_markup=kb)
        bot.answer_callback_query(c.id)

    @bot.callback_query_handler(func=lambda c: c.data.startswith("mat:admin:dir:"))
    def cb_admin_dir(c: CallbackQuery):
        if not _is_admin(c.from_user.id):
            bot.answer_callback_query(c.id, "Нет доступа")
            return
        direction = c.data.split(":")[3]
        _render_admin_direction(c.message.chat.id, c.message.message_id, direction)
        bot.answer_callback_query(c.id)

    @bot.callback_query_handler(func=lambda c: c.data.startswith("mat:admin:add_end:"))
    def cb_admin_add_end(c: CallbackQuery):
        if not _is_admin(c.from_user.id):
            bot.answer_callback_query(c.id, "Нет доступа")
            return
        direction = c.data.split(":")[3]
        repo_states.set_state(conn, c.from_user.id, _STATE_ADD_END_TITLE, {"direction": direction}, tz)
        bot.send_message(c.message.chat.id, f"[{direction}] Введи название нового шага:")
        bot.answer_callback_query(c.id)

    @bot.callback_query_handler(func=lambda c: c.data.startswith("mat:admin:step:"))
    def cb_admin_step(c: CallbackQuery):
        if not _is_admin(c.from_user.id):
            bot.answer_callback_query(c.id, "Нет доступа")
            return
        step_id = int(c.data.split(":")[3])
        _render_admin_step(c.message.chat.id, c.message.message_id, step_id)
        bot.answer_callback_query(c.id)

    @bot.callback_query_handler(func=lambda c: c.data.startswith("mat:admin:edit_title:"))
    def cb_admin_edit_title(c: CallbackQuery):
        if not _is_admin(c.from_user.id):
            bot.answer_callback_query(c.id, "Нет доступа")
            return
        step_id = int(c.data.split(":")[3])
        row = repo_materials.get_step(conn, step_id)
        if not row:
            bot.answer_callback_query(c.id, "Шаг не найден")
            return
        repo_states.set_state(conn, c.from_user.id, _STATE_EDIT_TITLE, {"step_id": step_id, "direction": row["direction"]}, tz)
        bot.send_message(c.message.chat.id, "Введи новое название шага:")
        bot.answer_callback_query(c.id)

    @bot.callback_query_handler(func=lambda c: c.data.startswith("mat:admin:edit_url:"))
    def cb_admin_edit_url(c: CallbackQuery):
        if not _is_admin(c.from_user.id):
            bot.answer_callback_query(c.id, "Нет доступа")
            return
        step_id = int(c.data.split(":")[3])
        row = repo_materials.get_step(conn, step_id)
        if not row:
            bot.answer_callback_query(c.id, "Шаг не найден")
            return
        repo_states.set_state(conn, c.from_user.id, _STATE_EDIT_URL, {"step_id": step_id, "direction": row["direction"]}, tz)
        bot.send_message(c.message.chat.id, "Введи новую ссылку (http/https):")
        bot.answer_callback_query(c.id)

    @bot.callback_query_handler(func=lambda c: c.data.startswith("mat:admin:replace:"))
    def cb_admin_replace(c: CallbackQuery):
        if not _is_admin(c.from_user.id):
            bot.answer_callback_query(c.id, "Нет доступа")
            return
        step_id = int(c.data.split(":")[3])
        row = repo_materials.get_step(conn, step_id)
        if not row:
            bot.answer_callback_query(c.id, "Шаг не найден")
            return
        repo_states.set_state(conn, c.from_user.id, _STATE_REPLACE_TITLE, {"step_id": step_id, "direction": row["direction"]}, tz)
        bot.send_message(c.message.chat.id, "Замена пункта: введи новое название:")
        bot.answer_callback_query(c.id)

    @bot.callback_query_handler(func=lambda c: c.data.startswith("mat:admin:insert:"))
    def cb_admin_insert(c: CallbackQuery):
        if not _is_admin(c.from_user.id):
            bot.answer_callback_query(c.id, "Нет доступа")
            return
        step_id = int(c.data.split(":")[3])
        row = repo_materials.get_step(conn, step_id)
        if not row:
            bot.answer_callback_query(c.id, "Шаг не найден")
            return
        repo_states.set_state(
            conn,
            c.from_user.id,
            _STATE_INSERT_TITLE,
            {"direction": row["direction"], "position": int(row["position"])},
            tz,
        )
        bot.send_message(c.message.chat.id, "Вставка шага: введи название нового пункта:")
        bot.answer_callback_query(c.id)

    @bot.callback_query_handler(func=lambda c: c.data.startswith("mat:admin:del:"))
    def cb_admin_del(c: CallbackQuery):
        if not _is_admin(c.from_user.id):
            bot.answer_callback_query(c.id, "Нет доступа")
            return
        step_id = int(c.data.split(":")[3])
        row = repo_materials.get_step(conn, step_id)
        if not row:
            bot.answer_callback_query(c.id, "Шаг не найден")
            return
        direction = row["direction"]
        repo_materials.delete_step(conn, tz, step_id)
        _render_admin_direction(c.message.chat.id, c.message.message_id, direction)
        bot.answer_callback_query(c.id, "Удалено")

    @bot.callback_query_handler(func=lambda c: c.data == "mat:admin:preview_dirs")
    def cb_admin_preview_dirs(c: CallbackQuery):
        if not _is_admin(c.from_user.id):
            bot.answer_callback_query(c.id, "Нет доступа")
            return
        text = "<b>Preview roadmap как пользователь</b>\nВыбери направление:"
        kb = _material_dirs_kb("mat:admin:preview", with_back=True, back_cb="m:materials_admin")
        bot.edit_message_text(text, c.message.chat.id, c.message.message_id, reply_markup=kb)
        bot.answer_callback_query(c.id)

    @bot.callback_query_handler(func=lambda c: c.data.startswith("mat:admin:preview:"))
    def cb_admin_preview(c: CallbackQuery):
        if not _is_admin(c.from_user.id):
            bot.answer_callback_query(c.id, "Нет доступа")
            return
        direction = c.data.split(":")[3]
        d = repo_materials.normalize_direction(direction)
        if not d:
            bot.answer_callback_query(c.id, "Неизвестное направление")
            return
        rows = repo_materials.list_steps(conn, d)
        text = f"<b>Preview: {d}</b>"
        kb = InlineKeyboardMarkup()
        if not rows:
            text += "\n\nШагов пока нет."
        else:
            for r in rows:
                label = _truncate_label(f"▫️ {int(r['position'])}. {r['title']}", 48)
                kb.row(InlineKeyboardButton(label, callback_data=f"mat:admin:pstep:{int(r['id'])}"))
        kb.row(InlineKeyboardButton("⬅️ К направлениям", callback_data="mat:admin:preview_dirs"))
        bot.edit_message_text(text, c.message.chat.id, c.message.message_id, reply_markup=kb)
        bot.answer_callback_query(c.id)

    @bot.callback_query_handler(func=lambda c: c.data.startswith("mat:admin:pstep:"))
    def cb_admin_preview_step(c: CallbackQuery):
        if not _is_admin(c.from_user.id):
            bot.answer_callback_query(c.id, "Нет доступа")
            return
        step_id = int(c.data.split(":")[3])
        row = repo_materials.get_step(conn, step_id)
        if not row:
            bot.answer_callback_query(c.id, "Шаг не найден")
            return
        text = (
            f"<b>Preview: {row['direction']} • шаг {int(row['position'])}</b>\n"
            f"{esc(row['title'])}\n\n"
            "Статус: <b>Не пройдено ▫️</b>"
        )
        kb = InlineKeyboardMarkup()
        kb.row(InlineKeyboardButton("📖 Открыть в канале", url=row["message_url"]))
        kb.row(InlineKeyboardButton("⬅️ Назад к roadmap", callback_data=f"mat:admin:preview:{row['direction']}"))
        bot.edit_message_text(text, c.message.chat.id, c.message.message_id, reply_markup=kb)
        bot.answer_callback_query(c.id)

    # ------------------------------------------------------------------
    # User: block-level navigation
    # ------------------------------------------------------------------

    @bot.callback_query_handler(func=lambda c: c.data.startswith("mat:user:block:"))
    def cb_user_block(c: CallbackQuery):
        if _is_admin(c.from_user.id):
            bot.answer_callback_query(c.id, "Это пользовательское меню")
            return
        parts = c.data.split(":")
        direction = parts[3]
        block_number = int(parts[4])
        repo_analytics.log_event(conn, tz, c.from_user.id, "funnel.materials.block_open", {"direction": direction, "block": block_number})
        _render_user_block(c.message.chat.id, c.message.message_id, c.from_user.id, direction, block_number)
        bot.answer_callback_query(c.id)

    # ------------------------------------------------------------------
    # Admin: topic sync management
    # ------------------------------------------------------------------

    @bot.callback_query_handler(func=lambda c: c.data == "mat:admin:sync")
    def cb_admin_sync(c: CallbackQuery):
        if not _is_admin(c.from_user.id):
            bot.answer_callback_query(c.id, "Нет доступа")
            return
        mappings = {r["direction"]: int(r["thread_id"]) for r in repo_materials.get_topic_mappings(conn)}
        text = "<b>Sync roadmap из канала</b>\n\nПривязка направлений к топикам группы:"
        kb = InlineKeyboardMarkup()
        for d in repo_materials.DIRECTIONS:
            tid = mappings.get(d)
            label = f"{d}: topic {tid}" if tid else f"{d}: не задан"
            kb.row(InlineKeyboardButton(label, callback_data=f"mat:admin:sync_dir:{d}"))
        kb.row(InlineKeyboardButton("❓ Почему не одна кнопка", callback_data="mat:admin:bulk_info"))
        kb.row(InlineKeyboardButton("⬅️ В материалы", callback_data="m:materials_admin"))
        bot.edit_message_text(text, c.message.chat.id, c.message.message_id, reply_markup=kb)
        bot.answer_callback_query(c.id)

    @bot.callback_query_handler(func=lambda c: c.data == "mat:admin:bulk_info")
    def cb_admin_bulk_info(c: CallbackQuery):
        if not _is_admin(c.from_user.id):
            bot.answer_callback_query(c.id, "Нет доступа")
            return
        bot.answer_callback_query(c.id)
        bot.send_message(
            c.message.chat.id,
            "<b>Почему нельзя одной кнопкой в боте</b>\n\n"
            "Telegram <b>не отдаёт боту</b> историю чата — только новые апдейты. "
            "Поэтому «весь роадмап разом» из уже существующих постов бот сам скачать не может.\n\n"
            "<b>Как импортировать всё сразу</b>\n"
            "1) Один раз на ПК: <code>pip install telethon</code>\n"
            "2) API id/hash: my.telegram.org\n"
            "3) Запуск (подставь свои числа):\n"
            "<pre>python scripts/import_roadmap_topic.py \\\n"
            "  --api-id ID --api-hash HASH \\\n"
            "  --chat-id GROUP_ID --topic-id THREAD_ID \\\n"
            "  --direction golang</pre>\n"
            "<code>THREAD_ID</code> = topic id (как в Sync). После импорта живой sync в боте как раньше.",
        )

    @bot.callback_query_handler(func=lambda c: c.data.startswith("mat:admin:bulk:") and c.data != "mat:admin:bulk_info")
    def cb_admin_bulk(c: CallbackQuery):
        if not _is_admin(c.from_user.id):
            bot.answer_callback_query(c.id, "Нет доступа")
            return
        d = c.data.split(":")[3]
        tid = repo_materials.get_topic_for_direction(conn, d)
        st = _effective_settings()
        gid = st.get("group_chat_id") or getattr(cfg, "MATERIALS_GROUP_CHAT_ID", None) or 0
        bot.answer_callback_query(c.id)
        pre = (
            f"<b>Полный импорт: {d}</b>\n\n"
            "Это <b>не</b> запускается кнопкой в Telegram — нужен скрипт на твоём ПК "
            "(один раз), потому что API бота не видит старые сообщения.\n\n"
        )
        if gid and tid:
            pre += (
                f"Твои параметры:\n"
                f"<code>--chat-id {gid}</code>\n"
                f"<code>--topic-id {tid}</code>\n"
                f"<code>--direction {d}</code>\n\n"
            )
        else:
            pre += "Сначала привяжи топик (Sync → направление), чтобы подставились chat_id и topic_id.\n\n"
        pre += (
            "Команда (из папки репозитория):\n"
            "<pre>pip install telethon\n"
            "python scripts/import_roadmap_topic.py \\\n"
            "  --api-id ID --api-hash HASH \\\n"
            f"  --chat-id {gid or 'GROUP_ID'} --topic-id {tid or 'THREAD_ID'} \\\n"
            f"  --direction {d}</pre>"
        )
        kb = InlineKeyboardMarkup()
        kb.row(InlineKeyboardButton("⬅️ Назад", callback_data=f"mat:admin:sync_dir:{d}"))
        bot.send_message(c.message.chat.id, pre, reply_markup=kb)

    @bot.callback_query_handler(func=lambda c: c.data.startswith("mat:admin:sync_dir:"))
    def cb_admin_sync_dir(c: CallbackQuery):
        if not _is_admin(c.from_user.id):
            bot.answer_callback_query(c.id, "Нет доступа")
            return
        direction = c.data.split(":")[3]
        d = repo_materials.normalize_direction(direction)
        if not d:
            bot.answer_callback_query(c.id, "Неизвестное направление")
            return
        tid = repo_materials.get_topic_for_direction(conn, d)
        blocks = repo_materials.list_blocks(conn, d)
        if tid:
            text = (
                f"<b>Sync: {d}</b>\n\n"
                f"Topic ID: <code>{tid}</code>\n"
                f"Блоков: <b>{len(blocks)}</b>\n\n"
                "Бот автоматически добавляет сообщения из этого топика."
            )
        else:
            text = (
                f"<b>Sync: {d}</b>\n\n"
                "Топик не привязан.\n\n"
                "Введи thread_id вручную или отправь сообщение в нужном топике группы "
                "(бот определит topic автоматически)."
            )
        kb = InlineKeyboardMarkup()
        if tid:
            kb.row(InlineKeyboardButton("🔄 Сменить topic", callback_data=f"mat:admin:set_tid:{d}"))
            kb.row(InlineKeyboardButton("❌ Отключить sync", callback_data=f"mat:admin:del_tid:{d}"))
        else:
            kb.row(InlineKeyboardButton("✏️ Ввести thread_id", callback_data=f"mat:admin:set_tid:{d}"))
            kb.row(InlineKeyboardButton("🔍 Определить из группы", callback_data=f"mat:admin:detect:{d}"))
        kb.row(InlineKeyboardButton("📥 Полный импорт (вся история)", callback_data=f"mat:admin:bulk:{d}"))
        kb.row(InlineKeyboardButton("⬅️ К sync", callback_data="mat:admin:sync"))
        bot.edit_message_text(text, c.message.chat.id, c.message.message_id, reply_markup=kb)
        bot.answer_callback_query(c.id)

    @bot.callback_query_handler(func=lambda c: c.data.startswith("mat:admin:set_tid:"))
    def cb_admin_set_tid(c: CallbackQuery):
        if not _is_admin(c.from_user.id):
            bot.answer_callback_query(c.id, "Нет доступа")
            return
        direction = c.data.split(":")[3]
        repo_states.set_state(conn, c.from_user.id, _STATE_SET_TOPIC, {"direction": direction}, tz)
        bot.send_message(c.message.chat.id, f"Введи thread_id топика для <b>{direction}</b>:")
        bot.answer_callback_query(c.id)

    @bot.callback_query_handler(func=lambda c: c.data.startswith("mat:admin:detect:"))
    def cb_admin_detect(c: CallbackQuery):
        if not _is_admin(c.from_user.id):
            bot.answer_callback_query(c.id, "Нет доступа")
            return
        direction = c.data.split(":")[3]
        repo_states.set_state(conn, c.from_user.id, _STATE_DETECT_TOPIC, {"direction": direction}, tz)
        bot.send_message(
            c.message.chat.id,
            f"Отправь любое сообщение в топике группы для <b>{direction}</b>.\n"
            "Бот определит topic ID автоматически.",
        )
        bot.answer_callback_query(c.id)

    @bot.callback_query_handler(func=lambda c: c.data.startswith("mat:admin:del_tid:"))
    def cb_admin_del_tid(c: CallbackQuery):
        if not _is_admin(c.from_user.id):
            bot.answer_callback_query(c.id, "Нет доступа")
            return
        direction = c.data.split(":")[3]
        repo_materials.remove_topic_mapping(conn, direction)
        bot.answer_callback_query(c.id, "Sync отключён")
        cb_admin_sync(c)

    # ------------------------------------------------------------------
    # Group message listener: auto-parse roadmap from forum topics
    # ------------------------------------------------------------------

    _materials_group_id = int(cfg.MATERIALS_GROUP_CHAT_ID) if cfg.MATERIALS_GROUP_CHAT_ID else 0

    @bot.message_handler(
        func=lambda m: (
            m.chat
            and getattr(m.chat, "type", "") in ("group", "supergroup")
            and int(m.chat.id) == _materials_group_id
            and _materials_group_id != 0
            and getattr(m, "message_thread_id", None) is not None
        ),
        content_types=["text", "photo", "video", "document", "audio"],
    )
    def on_group_roadmap_message(m: Message):
        thread_id = int(m.message_thread_id)

        if _is_admin(m.from_user.id):
            st, data = repo_states.get_state(conn, m.from_user.id)
            if st == _STATE_DETECT_TOPIC:
                direction = (data or {}).get("direction")
                if direction:
                    repo_materials.set_topic_mapping(conn, direction, thread_id, tz)
                    repo_states.clear_state(conn, m.from_user.id, tz)
                    try:
                        bot.send_message(
                            m.from_user.id,
                            f"✅ Topic ID <code>{thread_id}</code> привязан к <b>{direction}</b>.\n"
                            "Теперь сообщения из этого топика добавляются в roadmap.",
                            reply_markup=_admin_back_main_kb(),
                        )
                    except Exception:
                        log.warning("Cannot send DM to admin %s after topic detect", m.from_user.id)
                    return

        direction = repo_materials.get_direction_for_thread(conn, thread_id)
        if not direction:
            return

        text = m.text or getattr(m, "caption", "") or ""
        parsed = _parse_roadmap_message(text)
        if not parsed:
            return

        block_number, step_number, title = parsed
        block_title = f"Блок {block_number}"
        message_url = repo_materials.build_message_url(m.chat.id, m.message_id)

        existing = repo_materials.find_step_by_url(conn, message_url)
        if existing:
            repo_materials.update_step_full(
                conn, tz, int(existing["id"]),
                title=title, block_number=block_number, block_title=block_title, position=step_number,
            )
        else:
            repo_materials.upsert_auto_step(
                conn, tz, direction, block_number, block_title, step_number, title, message_url,
            )
        log.info("Roadmap auto-sync: %s block=%d step=%d '%s'", direction, block_number, step_number, title)

    @bot.message_handler(
        func=lambda m: (
            getattr(m.chat, "type", "private") == "private"
            and repo_states.get_state(conn, m.from_user.id)[0]
            in {
                _STATE_SET_GROUP_ID,
                _STATE_SET_INVITE,
                _STATE_ADD_END_TITLE,
                _STATE_ADD_END_URL,
                _STATE_INSERT_TITLE,
                _STATE_INSERT_URL,
                _STATE_REPLACE_TITLE,
                _STATE_REPLACE_URL,
                _STATE_EDIT_TITLE,
                _STATE_EDIT_URL,
                _STATE_SET_TOPIC,
                _STATE_DETECT_TOPIC,
            }
        ),
        content_types=["text"],
    )
    def st_admin_materials(m: Message):
        if not _is_admin(m.from_user.id):
            return
        st, data = repo_states.get_state(conn, m.from_user.id)
        data = data or {}
        txt = (m.text or "").strip()

        if st == _STATE_SET_GROUP_ID:
            if txt in {"-", "—"}:
                repo_materials.set_group_chat_id(conn, None)
                repo_states.clear_state(conn, m.from_user.id, tz)
                bot.send_message(m.chat.id, "ID группы очищен.", reply_markup=_admin_back_main_kb())
                return
            try:
                gid = int(txt)
            except Exception:
                bot.send_message(m.chat.id, "Нужен integer chat_id (например -1001234567890).")
                return
            repo_materials.set_group_chat_id(conn, gid)
            repo_states.clear_state(conn, m.from_user.id, tz)
            bot.send_message(m.chat.id, f"Сохранил ID группы: <code>{gid}</code>", reply_markup=_admin_back_main_kb())
            return

        if st == _STATE_SET_INVITE:
            if txt in {"-", "—"}:
                repo_materials.set_invite_url(conn, "")
                repo_states.clear_state(conn, m.from_user.id, tz)
                bot.send_message(m.chat.id, "Ссылка группы очищена.", reply_markup=_admin_back_main_kb())
                return
            if not (txt.startswith("http://") or txt.startswith("https://")):
                bot.send_message(m.chat.id, "Ссылка должна начинаться с http:// или https://")
                return
            repo_materials.set_invite_url(conn, txt)
            repo_states.clear_state(conn, m.from_user.id, tz)
            bot.send_message(m.chat.id, "Ссылка группы сохранена.", reply_markup=_admin_back_main_kb())
            return

        if st == _STATE_SET_TOPIC:
            try:
                tid = int(txt)
            except Exception:
                bot.send_message(m.chat.id, "Нужен целочисленный thread_id.")
                return
            direction = data.get("direction")
            repo_materials.set_topic_mapping(conn, direction, tid, tz)
            repo_states.clear_state(conn, m.from_user.id, tz)
            bot.send_message(
                m.chat.id,
                f"✅ Topic ID <code>{tid}</code> привязан к <b>{direction}</b>.",
                reply_markup=_admin_back_main_kb(),
            )
            return

        if st == _STATE_DETECT_TOPIC:
            bot.send_message(
                m.chat.id,
                "Жду сообщение в групповом топике, не в ЛС.\n"
                "Отправь любое сообщение в нужном топике группы.",
            )
            return

        if st == _STATE_ADD_END_TITLE:
            if len(txt) < 2:
                bot.send_message(m.chat.id, "Название слишком короткое.")
                return
            data["title"] = txt
            repo_states.set_state(conn, m.from_user.id, _STATE_ADD_END_URL, data, tz)
            bot.send_message(m.chat.id, "Введи ссылку шага (http/https):")
            return

        if st == _STATE_ADD_END_URL:
            if not (txt.startswith("http://") or txt.startswith("https://")):
                bot.send_message(m.chat.id, "Ссылка должна начинаться с http:// или https://")
                return
            direction = data.get("direction")
            title = data.get("title")
            repo_materials.add_step_end(conn, tz, direction, title, txt)
            repo_states.clear_state(conn, m.from_user.id, tz)
            bot.send_message(m.chat.id, f"Шаг добавлен в конец ({direction}).", reply_markup=_admin_back_main_kb())
            return

        if st == _STATE_INSERT_TITLE:
            if len(txt) < 2:
                bot.send_message(m.chat.id, "Название слишком короткое.")
                return
            data["title"] = txt
            repo_states.set_state(conn, m.from_user.id, _STATE_INSERT_URL, data, tz)
            bot.send_message(m.chat.id, "Введи ссылку шага (http/https):")
            return

        if st == _STATE_INSERT_URL:
            if not (txt.startswith("http://") or txt.startswith("https://")):
                bot.send_message(m.chat.id, "Ссылка должна начинаться с http:// или https://")
                return
            direction = data.get("direction")
            position = int(data.get("position") or 1)
            title = data.get("title")
            repo_materials.insert_step_at(conn, tz, direction, position, title, txt)
            repo_states.clear_state(conn, m.from_user.id, tz)
            bot.send_message(
                m.chat.id,
                f"Шаг вставлен в позицию {position} ({direction}).",
                reply_markup=_admin_back_main_kb(),
            )
            return

        if st == _STATE_REPLACE_TITLE:
            if len(txt) < 2:
                bot.send_message(m.chat.id, "Название слишком короткое.")
                return
            data["title"] = txt
            repo_states.set_state(conn, m.from_user.id, _STATE_REPLACE_URL, data, tz)
            bot.send_message(m.chat.id, "Введи новую ссылку шага (http/https):")
            return

        if st == _STATE_REPLACE_URL:
            if not (txt.startswith("http://") or txt.startswith("https://")):
                bot.send_message(m.chat.id, "Ссылка должна начинаться с http:// или https://")
                return
            step_id = int(data.get("step_id"))
            title = data.get("title")
            repo_materials.update_step(conn, tz, step_id, title=title, message_url=txt)
            repo_states.clear_state(conn, m.from_user.id, tz)
            bot.send_message(m.chat.id, "Пункт заменён.", reply_markup=_admin_back_main_kb())
            return

        if st == _STATE_EDIT_TITLE:
            if len(txt) < 2:
                bot.send_message(m.chat.id, "Название слишком короткое.")
                return
            step_id = int(data.get("step_id"))
            repo_materials.update_step(conn, tz, step_id, title=txt)
            repo_states.clear_state(conn, m.from_user.id, tz)
            bot.send_message(m.chat.id, "Название обновлено.", reply_markup=_admin_back_main_kb())
            return

        if st == _STATE_EDIT_URL:
            if not (txt.startswith("http://") or txt.startswith("https://")):
                bot.send_message(m.chat.id, "Ссылка должна начинаться с http:// или https://")
                return
            step_id = int(data.get("step_id"))
            repo_materials.update_step(conn, tz, step_id, message_url=txt)
            repo_states.clear_state(conn, m.from_user.id, tz)
            bot.send_message(m.chat.id, "Ссылка обновлена.", reply_markup=_admin_back_main_kb())
