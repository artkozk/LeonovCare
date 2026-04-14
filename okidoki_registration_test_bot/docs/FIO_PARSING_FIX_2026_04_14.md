# Фикс парсинга ФИО в test-боте (14.04.2026)

## Проблема

В части договоров ФИО не приходило в `entities` как системное поле, из-за чего бот мог дозапрашивать ФИО вручную.

Дополнительно:

1. структура payload у OkiDoki может быть глубже, чем 1 уровень (`result -> data -> contract`);
2. ФИО может приходить в альтернативных ключах (например, `customer_fio`);
3. PDF fallback не всегда доступен (на некоторых токенах `download` возвращает `403`), значит полагаться только на PDF нельзя.

## Что сделано

1. Расширен разбор payload:
   - добавлен рекурсивный обход вложенных словарей (`_layers`) вместо только верхнего уровня.
2. Усилен парсинг ФИО:
   - поддержка дополнительных ключей: `customer_fio`, `client_fio`, `fio_client`, `client_full_name`, `customer_full_name`, `user_fio`, `student_fio`;
   - поддержка извлечения ФИО из свободного текста договора (`text/body/content/...`) по регулярным шаблонам;
   - фильтр `_looks_like_fio`, чтобы не принять имя шаблона договора за ФИО.
3. Улучшено обогащение payload из PDF hints:
   - при наличии `fio` теперь добавляется entity `ФИО`.
4. Добавлены тесты:
   - `tests/test_prefill_parser_fio.py` (4 кейса: root/nested/text/защита от ложного срабатывания).

## Измененные файлы

1. `app/prefill_parser.py`
2. `app/okidoki_readonly_client.py`
3. `tests/test_prefill_parser_fio.py`

## Проверка

Локально:

```bash
cd okidoki_registration_test_bot
python -m pytest
```

Результат: `4 passed`.
