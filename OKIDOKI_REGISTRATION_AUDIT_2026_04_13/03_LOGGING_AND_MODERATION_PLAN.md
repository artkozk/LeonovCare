# 3. Логирование и модерация

## Обязательные события логирования

1. `okidoki.profile.link_received`
- `owner_tg_id`, `request_id`, `link_hash`.

2. `okidoki.profile.link_validated`
- `is_valid`, `contract_id`, `validation_error`.

3. `okidoki.profile.lookup_result`
- `source=db|api|pdf`;
- `is_our_contract=true|false`;
- `reason`.

4. `okidoki.profile.prefill_result`
- `filled_fields`;
- `missing_fields`;
- `payment_fields_quality=ok|partial|invalid`.

5. `okidoki.profile.fallback_manual`
- `fallback_reason`;
- `moderation_required=true`.

6. `okidoki.profile.submitted`
- `mode=auto|manual|fallback`;
- `request_id`.

## Что передавать ментору в модерации

1. Исходную ссылку на договор.
2. Причину fallback (если была).
3. Какие поля подтянуты автоматически.
4. Какие поля введены вручную.

## Технические требования к логам

1. Структурированные JSON-логи.
2. Корреляция по `request_id`.
3. Маскирование чувствительных данных:
- не логировать ключи;
- не логировать полный токен бота;
- ссылку договора хранить в логах как hash + mask.
