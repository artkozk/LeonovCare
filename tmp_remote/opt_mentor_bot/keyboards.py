from __future__ import annotations
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton

def main_menu_kb(
    can_manage_materials: bool = True,
    can_manage_broadcasts: bool = True,
    can_manage_admins: bool = True,
) -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup()
    kb.row(
        InlineKeyboardButton("Ученики", callback_data="m:students"),
        InlineKeyboardButton("Напоминалки", callback_data="m:reminders"),
    )
    kb.row(
        InlineKeyboardButton("Уведомления", callback_data="m:events"),
        InlineKeyboardButton("Платежи", callback_data="m:payments"),
    )
    kb.row(
        InlineKeyboardButton("🔑 Доступы HH", callback_data="m:hh"),
        InlineKeyboardButton("🎟 Коды ученика", callback_data="m:student_codes"),
    )
    if can_manage_materials:
        kb.row(InlineKeyboardButton("📚 Материалы", callback_data="m:materials_admin"))
    if can_manage_broadcasts:
        kb.row(InlineKeyboardButton("📣 Сообщения", callback_data="m:broadcasts"))
    if can_manage_admins:
        kb.row(InlineKeyboardButton("👮 Админы", callback_data="m:admins"))
    kb.row(InlineKeyboardButton("Статистика", callback_data="m:stats"))
    return kb


def regular_menu_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup()
    kb.row(
        InlineKeyboardButton("Вступить на обучение", callback_data="v2:enroll"),
    )
    kb.row(
        InlineKeyboardButton("Бесплатные материалы", callback_data="v2:materials"),
    )
    kb.row(
        InlineKeyboardButton("Автоотклики", callback_data="v2:auto"),
    )
    kb.row(
        InlineKeyboardButton("Interview helper", callback_data="v2:interview_helper"),
    )
    kb.row(
        InlineKeyboardButton("Лицензия Jetbrains", callback_data="v2:jetbrains"),
    )
    kb.row(
        InlineKeyboardButton("Диагностический созвон", callback_data="u:calls"),
    )
    kb.row(
        InlineKeyboardButton("Я ученик", callback_data="v2:student"),
    )
    return kb


def user_menu_kb(show_pay: bool = True) -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup()
    kb.row(InlineKeyboardButton("Моя анкета", callback_data="u:profile"))
    kb.row(InlineKeyboardButton("Доступы HH", callback_data="u:accounts"))
    kb.row(InlineKeyboardButton("Бесплатные материалы", callback_data="v2:materials"))
    kb.row(InlineKeyboardButton("Созвоны с ментором", callback_data="u:calls"))
    kb.row(InlineKeyboardButton("Моки", callback_data="u:mocks"))
    kb.row(InlineKeyboardButton("Автоотклики", callback_data="v2:auto"))
    kb.row(InlineKeyboardButton("Interview helper", callback_data="v2:interview_helper"))
    kb.row(InlineKeyboardButton("Лицензия Jetbrains", callback_data="v2:jetbrains"))
    if show_pay:
        kb.row(InlineKeyboardButton("Подтвердить платёж", callback_data="u:pay"))
    return kb


def user_profile_start_mode_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup()
    kb.row(InlineKeyboardButton("✍️ Заполнить вручную", callback_data="u:profile:start:manual"))
    kb.row(InlineKeyboardButton("⬅️ В меню", callback_data="v2:home"))
    return kb


def user_direction_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup()
    kb.row(
        InlineKeyboardButton("java", callback_data="uform:dir:java"),
        InlineKeyboardButton("golang", callback_data="uform:dir:golang"),
    )
    kb.row(
        InlineKeyboardButton("frontend", callback_data="uform:dir:frontend"),
        InlineKeyboardButton("python", callback_data="uform:dir:python"),
    )
    kb.row(
        InlineKeyboardButton("c++", callback_data="uform:dir:c++"),
        InlineKeyboardButton("✍️ Ввести вручную", callback_data="uform:dir:manual"),
    )
    kb.row(InlineKeyboardButton("✖️ Отмена", callback_data="u:cancel"))
    return kb


def user_stage_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup()
    kb.row(
        InlineKeyboardButton("с нуля", callback_data="uform:stage:С нуля"),
        InlineKeyboardButton("дообучение", callback_data="uform:stage:Дообучение"),
    )
    kb.row(
        InlineKeyboardButton("собеседования", callback_data="uform:stage:Собеседования"),
        InlineKeyboardButton("дипфейк", callback_data="uform:stage:Дипфейк"),
    )
    kb.row(InlineKeyboardButton("✍️ Ввести вручную", callback_data="uform:stage:manual"))
    kb.row(InlineKeyboardButton("✖️ Отмена", callback_data="u:cancel"))
    return kb


def user_tariff_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup()
    kb.row(
        InlineKeyboardButton("Только постоплата", callback_data="uform:tariff:post"),
    )
    kb.row(
        InlineKeyboardButton("Только предоплата", callback_data="uform:tariff:pre"),
        InlineKeyboardButton("Предоплата + постоплата", callback_data="uform:tariff:pre_post"),
    )
    kb.row(InlineKeyboardButton("✖️ Отмена", callback_data="u:cancel"))
    return kb


def user_prepay_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup()
    kb.row(
        InlineKeyboardButton("0", callback_data="uform:prepay:0"),
        InlineKeyboardButton("30000", callback_data="uform:prepay:30000"),
        InlineKeyboardButton("60000", callback_data="uform:prepay:60000"),
    )
    kb.row(InlineKeyboardButton("✍️ Другая сумма", callback_data="uform:prepay:manual"))
    kb.row(InlineKeyboardButton("✖️ Отмена", callback_data="u:cancel"))
    return kb


def user_post_total_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup()
    kb.row(
        InlineKeyboardButton("350%", callback_data="uform:post_total:350"),
        InlineKeyboardButton("600%", callback_data="uform:post_total:600"),
    )
    kb.row(
        InlineKeyboardButton("300%", callback_data="uform:post_total:300"),
        InlineKeyboardButton("✍️ Ввести вручную", callback_data="uform:post_total:manual"),
    )
    kb.row(InlineKeyboardButton("✖️ Отмена", callback_data="u:cancel"))
    return kb


def user_post_monthly_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup()
    kb.row(
        InlineKeyboardButton("50%", callback_data="uform:post_m:50"),
        InlineKeyboardButton("25%", callback_data="uform:post_m:25"),
        InlineKeyboardButton("100%", callback_data="uform:post_m:100"),
    )
    kb.row(InlineKeyboardButton("✍️ Ввести вручную", callback_data="uform:post_m:manual"))
    kb.row(InlineKeyboardButton("✖️ Отмена", callback_data="u:cancel"))
    return kb


def request_admin_kb(req_id: int) -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup()
    kb.row(
        InlineKeyboardButton("✅ Одобрить", callback_data=f"req:ok:{req_id}"),
        InlineKeyboardButton("❌ Отклонить", callback_data=f"req:ask_no:{req_id}"),
    )
    kb.row(InlineKeyboardButton("Открыть", callback_data=f"req:open:{req_id}"))
    return kb


def request_open_kb(req_id: int) -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup()
    kb.row(InlineKeyboardButton("Открыть", callback_data=f"req:open:{req_id}"))
    return kb


def request_reject_confirm_kb(req_id: int, source_message_id: int | None = None) -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup()
    suffix = f":{int(source_message_id)}" if source_message_id is not None else ""
    kb.row(
        InlineKeyboardButton("Да, отклонить", callback_data=f"req:no:{req_id}{suffix}"),
        InlineKeyboardButton("Назад", callback_data=f"req:open:{req_id}"),
    )
    return kb

def request_lead_source_kb(req_id: int) -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup()
    kb.row(
        InlineKeyboardButton("Канал", callback_data=f"req:src:{req_id}:channel"),
        InlineKeyboardButton("Холодные", callback_data=f"req:src:{req_id}:cold"),
    )
    kb.row(
        InlineKeyboardButton("Сарафанка", callback_data=f"req:src:{req_id}:referral"),
        InlineKeyboardButton("Ом", callback_data=f"req:src:{req_id}:om"),
    )
    kb.row(
        InlineKeyboardButton("Свой вариант", callback_data=f"req:src:{req_id}:custom"),
    )
    kb.row(InlineKeyboardButton("⬅️ Назад", callback_data=f"req:open:{req_id}"))
    return kb

def students_menu_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup()
    kb.row(
        InlineKeyboardButton("👤 Карточки", callback_data="st:cards"),
        InlineKeyboardButton("📋 Список", callback_data="st:list"),
        InlineKeyboardButton("➕ Добавить", callback_data="st:add"),
    )
    kb.row(
        InlineKeyboardButton("🔎 Поиск", callback_data="st:search"),
        InlineKeyboardButton("🧩 Фильтры", callback_data="st:filters"),
    )
    kb.row(InlineKeyboardButton("🗂 Архив/Активные", callback_data="st:archive_toggle"))
    kb.row(InlineKeyboardButton("🏠 Main", callback_data="m:main"))
    return kb


def student_cards_kb(student_id: int, idx: int, total: int, back_cb: str = "m:students") -> InlineKeyboardMarkup:
    """Carousel-style student card keyboard (prev/next + actions)."""
    kb = InlineKeyboardMarkup()
    can_prev = idx > 0
    can_next = idx < (total - 1)

    nav = []
    if can_prev:
        nav.append(InlineKeyboardButton("◀️", callback_data="st:card:prev"))
    nav.append(InlineKeyboardButton(f"{idx+1}/{max(total,1)}", callback_data="noop"))
    if can_next:
        nav.append(InlineKeyboardButton("▶️", callback_data="st:card:next"))
    kb.row(*nav)

    kb.row(
        InlineKeyboardButton("✏️ Изменить", callback_data=f"st:edit:{student_id}"),
        InlineKeyboardButton("💰 Платежи", callback_data=f"st:pay:{student_id}"),
    )
    kb.row(InlineKeyboardButton("🔑 HH доступы", callback_data=f"hh:open:{student_id}"))
    kb.row(
        InlineKeyboardButton("📌 Напоминалки", callback_data=f"st:rem:{student_id}"),
        InlineKeyboardButton("📅 Уведомление", callback_data=f"st:ev_add:{student_id}"),
    )
    kb.row(
        InlineKeyboardButton("📄 Договор", callback_data=f"st:contract:{student_id}"),
        InlineKeyboardButton("📈 Постоплата", callback_data=f"st:post:{student_id}"),
    )
    kb.row(InlineKeyboardButton("🗑 Архив/Удалить", callback_data=f"st:arch:{student_id}"))
    kb.row(
        InlineKeyboardButton("📋 Список", callback_data="st:list"),
        InlineKeyboardButton("⬅️ Назад", callback_data=back_cb),
    )
    return kb

def list_nav_kb(can_prev: bool, can_next: bool, back_cb: str) -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup()
    row=[]
    if can_prev:
        row.append(InlineKeyboardButton("◀️", callback_data="nav:prev"))
    if can_next:
        row.append(InlineKeyboardButton("▶️", callback_data="nav:next"))
    if row:
        kb.row(*row)
    kb.row(InlineKeyboardButton("⬅️ Назад", callback_data=back_cb))
    return kb

def open_student_kb(student_id: int) -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup()
    kb.row(InlineKeyboardButton("Открыть карточку", callback_data=f"st:open:{student_id}"))
    return kb

def student_card_kb(student_id: int, back_cb: str="st:list") -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup()
    kb.row(
        InlineKeyboardButton("✏️ Изменить", callback_data=f"st:edit:{student_id}"),
        InlineKeyboardButton("💰 Платежи", callback_data=f"st:pay:{student_id}"),
    )
    kb.row(
        InlineKeyboardButton("📌 Напоминалки", callback_data=f"st:rem:{student_id}"),
        InlineKeyboardButton("📅 Уведомление", callback_data=f"st:ev_add:{student_id}"),
    )
    kb.row(
        InlineKeyboardButton("📄 Договор", callback_data=f"st:contract:{student_id}"),
        InlineKeyboardButton("📈 Постоплата", callback_data=f"st:post:{student_id}"),
    )
    kb.row(InlineKeyboardButton("🗑 Архив/Удалить", callback_data=f"st:arch:{student_id}"))
    kb.row(InlineKeyboardButton("⬅️ Назад", callback_data=back_cb))
    return kb

def edit_fields_kb(student_id: int) -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup()
    kb.row(InlineKeyboardButton("ФИО", callback_data=f"ed:{student_id}:fio"),
           InlineKeyboardButton("Username", callback_data=f"ed:{student_id}:username"))
    kb.row(InlineKeyboardButton("Направление", callback_data=f"ed:{student_id}:direction"),
           InlineKeyboardButton("Этап", callback_data=f"ed:{student_id}:stage"))
    kb.row(InlineKeyboardButton("Тариф", callback_data=f"ed:{student_id}:tariff"),
           InlineKeyboardButton("Комментарий", callback_data=f"ed:{student_id}:comment"))
    kb.row(InlineKeyboardButton("Источник лида", callback_data=f"ed:{student_id}:lead_source"))
    kb.row(InlineKeyboardButton("Сумма предоплаты", callback_data=f"ed:{student_id}:prepay_total"),
           InlineKeyboardButton("Правило постоплаты", callback_data=f"ed:{student_id}:postpay_rule"))
    kb.row(InlineKeyboardButton("Оффер", callback_data=f"ed:{student_id}:offer"),
           InlineKeyboardButton("Дата входа", callback_data=f"ed:{student_id}:join_date"))
    kb.row(InlineKeyboardButton("Собесы", callback_data=f"ed:{student_id}:interviews"))
    kb.row(InlineKeyboardButton("Договор/HH", callback_data=f"ed:{student_id}:private"))
    kb.row(InlineKeyboardButton("⬅️ Назад", callback_data=f"st:open:{student_id}"))
    return kb

def pick_list_kb(items: list[tuple[str,str]], back_cb: str) -> InlineKeyboardMarkup:
    # items: (label, callback)
    kb = InlineKeyboardMarkup()
    for label, cb in items:
        kb.row(InlineKeyboardButton(label, callback_data=cb))
    kb.row(InlineKeyboardButton("⬅️ Назад", callback_data=back_cb))
    return kb

def alert_kb(alert_id: int, student_id: int | None=None, event_id: int | None=None) -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup()
    kb.row(InlineKeyboardButton("✅ Увидел", callback_data=f"a:seen:{alert_id}"))
    kb.row(
        InlineKeyboardButton("😴 10 мин", callback_data=f"a:snz:{alert_id}:10"),
        InlineKeyboardButton("😴 1 час", callback_data=f"a:snz:{alert_id}:60"),
    )
    kb.row(
        InlineKeyboardButton("➡️ Завтра", callback_data=f"a:shift:{alert_id}:1"),
        InlineKeyboardButton("➡️ +3 дня", callback_data=f"a:shift:{alert_id}:3"),
        InlineKeyboardButton("📅 Дата", callback_data=f"a:date:{alert_id}"),
    )
    if student_id is not None:
        kb.row(
            InlineKeyboardButton("👤 Открыть ученика", callback_data=f"a:open_s:{student_id}"),
            InlineKeyboardButton("💰 Внести платёж", callback_data=f"a:pay:{student_id}"),
        )
    if event_id is not None:
        kb.row(InlineKeyboardButton("📅 Открыть событие", callback_data=f"a:open_e:{event_id}"))
    return kb

def reminders_menu_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup()
    kb.row(InlineKeyboardButton("✅ Сегодня", callback_data="rm:today"),
           InlineKeyboardButton("➕ Добавить", callback_data="rm:add"))
    kb.row(
        InlineKeyboardButton("📅 Расписание завтра", callback_data="ev:tomorrow"),
        InlineKeyboardButton("📅 Расписание неделя", callback_data="ev:week"),
    )
    kb.row(InlineKeyboardButton("📋 Все", callback_data="rm:list"),
           InlineKeyboardButton("🧩 Правила этапов", callback_data="rm:rules"))
    kb.row(InlineKeyboardButton("🏠 Main", callback_data="m:main"))
    return kb

def events_menu_kb() -> InlineKeyboardMarkup:
    kb=InlineKeyboardMarkup()
    kb.row(InlineKeyboardButton("Сегодня", callback_data="ev:today"),
           InlineKeyboardButton("Неделя", callback_data="ev:week"))
    kb.row(InlineKeyboardButton("🕘 Слоты", callback_data="ev:slots"))
    kb.row(InlineKeyboardButton("➕ Добавить", callback_data="ev:add"),
           InlineKeyboardButton("📋 Все предстоящие", callback_data="ev:list"))
    kb.row(InlineKeyboardButton("⚙️ Мои уведомления", callback_data="adm:notify_settings"))
    kb.row(InlineKeyboardButton("🏠 Main", callback_data="m:main"))
    return kb

def payments_menu_kb() -> InlineKeyboardMarkup:
    kb=InlineKeyboardMarkup()
    kb.row(InlineKeyboardButton("💸 Ждём сегодня", callback_data="py:today"),
           InlineKeyboardButton("📆 На неделе", callback_data="py:week"))
    kb.row(InlineKeyboardButton("💼 Бюджет", callback_data="py:budget"))
    kb.row(InlineKeyboardButton("🧪 Проверка оплаты (Cardlink)", callback_data="py:cardlink_check"))
    kb.row(InlineKeyboardButton("➕ Внести платёж", callback_data="py:add"),
           InlineKeyboardButton("📊 Долги", callback_data="py:debts"))
    kb.row(InlineKeyboardButton("🏠 Main", callback_data="m:main"))
    return kb

def stats_menu_kb(show_payments: bool = True) -> InlineKeyboardMarkup:
    kb=InlineKeyboardMarkup()
    kb.row(
        InlineKeyboardButton("Обзор", callback_data="stt:overview"),
        InlineKeyboardButton("Пользователи", callback_data="stt:users"),
    )
    kb.row(
        InlineKeyboardButton("Материалы", callback_data="stt:materials"),
        InlineKeyboardButton("Я ученик", callback_data="stt:student_funnel"),
    )
    if show_payments:
        kb.row(
            InlineKeyboardButton("Платежи", callback_data="stt:payments"),
            InlineKeyboardButton("Операционка", callback_data="stt:ops"),
        )
    else:
        kb.row(InlineKeyboardButton("Операционка", callback_data="stt:ops"))
    kb.row(
        InlineKeyboardButton("Экспорт HTML", callback_data="stt:export:html"),
        InlineKeyboardButton("Экспорт TXT", callback_data="stt:export:txt"),
    )
    kb.row(
        InlineKeyboardButton("Лог действий HTML", callback_data="stt:events:html"),
        InlineKeyboardButton("Лог действий TXT", callback_data="stt:events:txt"),
    )
    kb.row(InlineKeyboardButton("Неактивные ученики", callback_data="stt:inactive"))
    kb.row(InlineKeyboardButton("🏠 Main", callback_data="m:main"))
    return kb


def admins_menu_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup()
    kb.row(InlineKeyboardButton("📋 Список админов", callback_data="adm:list"))
    kb.row(
        InlineKeyboardButton("➕ Добавить full", callback_data="adm:add:full"),
        InlineKeyboardButton("➕ Добавить limited", callback_data="adm:add:limited"),
    )
    kb.row(InlineKeyboardButton("➖ Удалить админа", callback_data="adm:remove"))
    kb.row(InlineKeyboardButton("🧹 Удалить всех динамических", callback_data="adm:remove_all"))
    kb.row(InlineKeyboardButton("🏠 Main", callback_data="m:main"))
    return kb


def reminder_card_kb(reminder_id: int, back_cb: str, student_id: int | None=None) -> InlineKeyboardMarkup:
    kb=InlineKeyboardMarkup()
    kb.row(InlineKeyboardButton("✏️ Заголовок", callback_data=f"rm:edit:{reminder_id}:title"),
           InlineKeyboardButton("✏️ Коммент", callback_data=f"rm:edit:{reminder_id}:note"))
    kb.row(InlineKeyboardButton("⏰ Время", callback_data=f"rm:edit:{reminder_id}:time"),
           InlineKeyboardButton("🔁 Частота", callback_data=f"rm:edit:{reminder_id}:freq"))
    kb.row(InlineKeyboardButton("✅ Вкл/Выкл", callback_data=f"rm:toggle:{reminder_id}"),
           InlineKeyboardButton("🗑 Удалить", callback_data=f"rm:del:{reminder_id}"))
    if student_id is not None:
        kb.row(InlineKeyboardButton("👤 Ученик", callback_data=f"st:open:{student_id}"))
    kb.row(InlineKeyboardButton("⬅️ Назад", callback_data=back_cb))
    return kb


def stage_rule_card_kb(stage_id: int, back_cb: str) -> InlineKeyboardMarkup:
    kb=InlineKeyboardMarkup()
    kb.row(InlineKeyboardButton("✏️ Заголовок", callback_data=f"sr:edit:{stage_id}:title"),
           InlineKeyboardButton("✏️ Текст", callback_data=f"sr:edit:{stage_id}:note"))
    kb.row(InlineKeyboardButton("⏰ Время", callback_data=f"sr:edit:{stage_id}:time"),
           InlineKeyboardButton("🔁 Частота", callback_data=f"sr:edit:{stage_id}:freq"))
    kb.row(InlineKeyboardButton("✅ Вкл/Выкл", callback_data=f"sr:toggle:{stage_id}"),
           InlineKeyboardButton("↻ Применить к ученикам", callback_data=f"sr:apply:{stage_id}"))
    kb.row(InlineKeyboardButton("⬅️ Назад", callback_data=back_cb))
    return kb


def weekday_picker_kb(prefix: str, selected: set[int], done_cb: str, back_cb: str) -> InlineKeyboardMarkup:
    # prefix: callback prefix like 'srwd:STAGEID' or 'rmwd:REMINDERID'
    days=[(1,"Пн"),(2,"Вт"),(3,"Ср"),(4,"Чт"),(5,"Пт"),(6,"Сб"),(7,"Вс")]
    kb=InlineKeyboardMarkup()
    row=[]
    for d,lab in days:
        mark='✅' if d in selected else '▫️'
        row.append(InlineKeyboardButton(f"{mark}{lab}", callback_data=f"{prefix}:{d}"))
        if len(row)==4:
            kb.row(*row); row=[]
    if row:
        kb.row(*row)
    kb.row(InlineKeyboardButton("Готово", callback_data=done_cb))
    kb.row(InlineKeyboardButton("⬅️ Назад", callback_data=back_cb))
    return kb
