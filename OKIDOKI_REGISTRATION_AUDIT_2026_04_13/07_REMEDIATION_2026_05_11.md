# OkiDoki Registration Remediation (2026-05-11)

Документ append-only. Описывает точечные правки, закрывающие часть боли «договоры OkiDoki часто не проходят автозаполнение».

## Контекст

Жалоба: ученики получают «Договор не валиден, обратись к ментору» при отправке валидной ссылки. После расследования (см. снимок production-кода в `bot/`) подтверждено, что значительная часть аудита 2026-04-13 уже закрыта (PDF hint extraction, soft fallback, `ok_by_sender_username`, корректный funnel logging). Основной незакрытый блок — жёсткое отклонение при несовпадении Telegram username с полем «Телеграм клиента» в договоре.

## Что изменилось в коде

### 1. Смягчение `username_mismatch` под env-флаг

Файл: [bot/app/handlers/user.py:2045-2061](bot/app/handlers/user.py)

До: при `tg_username != contract_username` функция `_prefill_and_validate_contract` возвращала `ContractValidationResult(False, "username_mismatch", ...)`. Обработчик показывал «Договор не валиден, обратись к ментору». Все автозаполненные поля терялись.

После: новый env-флаг `OKIDOKI_STRICT_USERNAME`:

- по умолчанию `False` (мягкий режим): договор принимается, поле `username` берётся из договора, а в `prefill` кладётся служебный ключ `_review_username_mismatch={"tg": ..., "contract": ...}`. Reason возврата — `ok_with_username_mismatch_review`.
- при `True` сохраняется прежнее жёсткое поведение для отката.

### 2. Обработка `_review_username_mismatch` в `contract_lookup`

Файл: [bot/app/handlers/user.py:4703-4760](bot/app/handlers/user.py)

После успешного prefill маркер извлекается, **не попадает в profile data**, а используется чтобы:

1. Залоггировать аналитическое событие `funnel.student.contract_username_mismatch_review` с обоими username и URL договора.
2. Показать пользователю предупреждение «Анкета принята, но ментор сверит данные перед окончательной активацией».
3. Продолжить wizard как при обычном автозаполнении.

Решения по модерации:

- Уведомление ментору пока идёт через analytics-event, а не через прямой Telegram-канал (в проде `MENTOR_CHAT_ID` не настроен, только `MENTOR_CONTACT_URL`). Это сознательный минимальный шаг; полноценный moderator-bot ход отдельной задачей.

### 3. Расширение парсера ФИО (PDF hint extraction)

Файл: [bot/app/services/okidoki_client.py:441-465](bot/app/services/okidoki_client.py)

Добавлены два паттерна:

- `Ф.И.О.: Иванов Иван Иванович` (с точками в Ф.И.О. — частый формат в PDF-печати договоров OkiDoki).
- `Заказчик: ...`, `Клиент: ...`, `Ученик: ...` без «именуемый в дальнейшем» (короткие шапки на странице карточки).

Тесты:

- [bot/tests/test_okidoki_client.py](bot/tests/test_okidoki_client.py) — три новых кейса: `_fio_from_fio_label_with_dots`, `_fio_from_zakazchik_label`, `_fio_from_uchenik_label`.

### 4. Логирование причин недоступности PDF

Файл: [bot/app/services/okidoki_client.py:376-398](bot/app/services/okidoki_client.py)

Каждый `return None` в `_download_contract_pdf` теперь логирует конкретную причину через `log.warning`:

- `no_api_token`
- `empty_contract_id`
- `network` (вместе с текстом исключения)
- `http_<status_code>`
- `payload_too_small`

Это позволит ментору и SRE видеть в `journalctl -u mentor-bot | grep okidoki.pdf_download` точную причину, по которой PDF hints (включая username и ФИО) не подтянулись.

## Что НЕ менялось (намеренно)

1. **`_as_amount` / `_normalize_date`** — таких функций в production-боте нет; парсинг сумм и дат идёт через `installment_parser.py`, который был усилен 27.04.2026 (требует ≥2 явные даты, отсекает «% от дохода» без графика). Дополнительные правки рискуют регрессом.
2. **HH parser retry/timeout** (`app/services/owner_hh_parser.py`) — обёртка вокруг подмодуля `parser_hh`. Без знания публичных исключений parser_hh добавлять retry рискованно. Текущее состояние: исключения пробрасываются наверх, cleanup в `finally`. Если будут конкретные инциденты с истёкшими cookies, добавим точечно.
3. **Дедупликация отписей** (`scripts/owner_tg_process_replies.py:1162-1188`) — уже использует и `message_id`, и `timestamp` (строгое `>`). Race condition теоретически возможен только при равных timestamp и обратной упорядоченности id, что не наблюдается на проде.
4. **Ротация `OWNER_OPENAI_API_KEY`** — по решению owner'a не трогаем в этой итерации.

## Открытые задачи (не закрыто этой итерацией)

1. Прямое Telegram-уведомление ментору при `requires_review` (нужно настроить `MENTOR_CHAT_ID` и отдельный канал/чат).
2. UI для модерации заявок `requires_review` в админ-панели (`bot/app/handlers/admins.py` или `owner_tools.py`).
3. Read-only enforcement OkiDoki API из flow регистрации (отдельная задача из `02_TARGET_FLOW_AND_RULES.md`).
4. Полный набор 6 структурированных событий `okidoki.profile.*` (из `03_LOGGING_AND_MODERATION_PLAN.md`) — сейчас закрыто частично через существующие `funnel.student.*`.

## Верификация

1. `py -3 -m pytest tests/test_okidoki_client.py tests/test_okidoki_contract_validation.py -q` — все тесты зелёные (22 пройдено: 19 существующих + 3 новых на ФИО).
2. Smoke (после деплоя):
   - валидная ссылка с совпадающим username → автозаполнение, нет review-сообщения.
   - валидная ссылка с **другим** username → автозаполнение + сообщение «ментор сверит» + event `funnel.student.contract_username_mismatch_review`.
   - битая ссылка → прежний путь `contract_invalid`.
3. В случае регресса откат — `OKIDOKI_STRICT_USERNAME=1` в `/opt/mentor-bot/.env` и `systemctl restart mentor-bot`.

## Связанные документы

- [`01_CURRENT_IMPLEMENTATION.md`](./01_CURRENT_IMPLEMENTATION.md)
- [`02_TARGET_FLOW_AND_RULES.md`](./02_TARGET_FLOW_AND_RULES.md)
- [`03_LOGGING_AND_MODERATION_PLAN.md`](./03_LOGGING_AND_MODERATION_PLAN.md)
- [`bot/OKIDOKI_CONTRACT_PARSING_FIX_2026_04_16.md`](../bot/OKIDOKI_CONTRACT_PARSING_FIX_2026_04_16.md)
- [`bot/OKIDOKI_INSTALLMENT_AUDIT_2026_04_27.md`](../bot/OKIDOKI_INSTALLMENT_AUDIT_2026_04_27.md)
