# OkiDoki Registration Audit (2026-04-13)

Папка создана в отдельной ветке `codex/okidoki-hh-audit-2026-04-13`.
Рабочий код не изменялся.

## Состав

1. `01_CURRENT_IMPLEMENTATION.md` — как сейчас работает сценарий `Я ученик -> анкета -> ссылка`.
2. `02_TARGET_FLOW_AND_RULES.md` — целевой процесс и правила валидации «наш договор».
3. `03_LOGGING_AND_MODERATION_PLAN.md` — что логировать, чтобы не терялись причины fallback.
4. `04_DEPLOY_NON_PROD_PLAN.md` — как вынести тест в отдельный контур без касания прода.
5. `05_SECRETS_CHECK.md` — результат поиска `OKIDOKI_API_TOKEN` и смежные риски.
6. `06_OKIDOKI_TEMPLATE_FIELDS.md` — фактические поля шаблонов договоров из OkiDoki API.
