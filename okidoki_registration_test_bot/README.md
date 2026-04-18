# OkiDoki Registration Test Bot

Отдельный тестовый Telegram-бот для сценария регистрации ученика через ссылку на договор OkiDoki.

## Что делает

1. Пользователь нажимает `Я ученик`.
2. Отправляет ссылку на договор OkiDoki.
3. Бот в `read-only` режиме читает договор и проверяет, что это наш шаблон.
4. Если договор валиден:
   - автозаполняет известные поля карточки;
   - показывает предпросмотр в формате стандартной карточки ученика;
   - дозапрашивает только отсутствующие поля.
5. Если договор невалиден:
   - сообщает, что договор невалиден;
   - предлагает обратиться к ментору `@mr_winchester1`;
   - ждёт новую корректную ссылку на договор.

## Важные ограничения

1. Бот не создает/не изменяет/не удаляет договоры в OkiDoki.
2. Используются только GET-запросы.
3. Username вручную не спрашивается:
   - берется из `Телеграм клиента` в договоре;
   - сверяется с Telegram username пользователя;
   - исключение: `@artkozk`.
4. Если у пользователя в Telegram нет username — авто-регистрация невозможна.

## Структура

1. `app/okidoki_readonly_client.py` — только read-only API клиент OkiDoki.
2. `app/prefill_parser.py` — валидация договора и парсинг автозаполнения.
3. `app/bot.py` — Telegram FSM регистрации.
4. `app/storage.py` — SQLite лог заявок.
5. `run.py` — точка входа.

## Быстрый запуск локально

```powershell
cd C:\prog\Comercial\LeonovCareSite\okidoki_registration_test_bot
py -3 -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
copy .env.example .env
python run.py
```

## Переменные окружения

См. `.env.example`.

Ключевые:

1. `BOT_TOKEN` — токен отдельного тестового бота.
2. `OKIDOKI_API_TOKEN` — токен OkiDoki.
3. `MENTOR_CHAT_ID` — chat id ментора для модерации заявок (опционально).
4. `MENTOR_CONTACT_URL=https://t.me/mr_winchester1` — ссылка для связи при невалидном договоре.
5. `TEST_EXCEPTION_USERNAME=artkozk` — исключение для теста.

## Логи

1. Файл: `logs/testbot.log` (ротация 5 x 5MB).
2. SQLite заявок: `data/testbot.db`.

## Проверка шаблонов

Команда в боте: `/templates` — покажет загруженные шаблоны и их поля.

## Поля автозаполнения

Подробно: [AUTOFILL_FIELDS.md](./docs/AUTOFILL_FIELDS.md)

## Документация по потоку регистрации

Подробно: [REGISTRATION_FLOW.md](./docs/REGISTRATION_FLOW.md)

## История фиксов

1. Фикс парсинга ФИО из договора: [FIO_PARSING_FIX_2026_04_14.md](./docs/FIO_PARSING_FIX_2026_04_14.md)
