# Proxy Rotation: DE Frankfurt (2026-05-12)

## Контекст

Старый прокси `141.11.162.95:47198` (creds `PSJ3ETN5:2S3W79O2`) перестал работать. По данным БД, последняя поставленная реакция в `owner_channel_reactions` датируется **2026-04-20 20:19 MSK** — то есть watcher молча падал в retry-loop около 3 недель, пока не был замечен.

Диагностика — `curl -x http://PSJ3ETN5:2S3W79O2@141.11.162.95:47198 https://api.ipify.org` возвращал `HTTP/1.1 407 Proxy Authentication Required` с `Proxy-Authenticate: Basic realm="shadowsocks"`. TCP-коннект до прокси проходил (~78 мс), но любые creds отвергались. Признаки совпадают с истёкшей подпиской / отозванными creds на стороне провайдера.

Тот же прокси использовался параллельно для AI-запросов (`OWNER_AI_HTTP_PROXY`), поэтому LLM-ответы в outreach тоже могли деградировать.

## Новый прокси

`154.194.111.210:63670` (creds `9wuhgZ99:sUwJZxbW`).

- Локация: Frankfurt am Main, Hesse, DE.
- AS: AS62240 Clouvider.
- Проверка `curl -x http://9wuhgZ99:sUwJZxbW@154.194.111.210:63670 https://api.ipify.org` → `HTTP=200`, `154.194.111.210`.
- Прозрачен для `api.telegram.org` → `HTTP=302` (нормальный редирект корня).

## Что сделано

### На сервере `/opt/mentor-bot/.env`

- Бэкап: `/opt/mentor-bot/.deploy_backups/20260512_092752_env_before_proxy_rotation.env` (root:root, 600).
- Изменены две строки:
  - `OWNER_TG_PROXY=154.194.111.210:63670:9wuhgZ99:sUwJZxbW` (для Telethon, формат `host:port:user:pass`).
  - `OWNER_AI_HTTP_PROXY=http://9wuhgZ99:sUwJZxbW@154.194.111.210:63670` (для OpenAI/LLM-прокси).
- `systemctl restart mentor-bot` → `active (running)`. В журнале сразу появилось: `Watcher реакций: прокси включен (9wuhgZ99:sUwJZxbW@154.194.111.210:63670)` + `Connection to 149.154.167.51:443/TcpFull complete!` — Telethon MTProto-сессии устанавливаются.

### В git репозитории

- В `bot/tests/test_owner_tg_bulk_send.py` обнаружен и удалён leak старых production-creds (использовались как test fixture для `_parse_generic_proxy`). Заменены на синтетические `198.51.100.10:42000:proxy_user_example:proxy_pass_example` — теперь ротация production creds не зависит от тестов. Тест зелёный (7/7).
- В `bot/deploy/env.example` производственные значения отсутствовали (только пустые placeholder-строки) — не трогаем.
- В `data/owner_tools/reactions/reactions_*.json` лежат **исторические снимки** запусков с creds — это append-only логи runtime, не code. Не трогаем.

## Что НЕ трогали

- Сам провайдер прокси (это не наша инфра).
- Список аккаунтов Telethon-watcher'а (Олег / 123 / Аккаунт 2-5) — он берётся из БД (`owner_tg_accounts`), а не из .env. Если какие-то аккаунты ругаются на entity-resolve (`Cannot find any entity corresponding to "+U-zJpCVD0N05NDg6"`) — это другая проблема (каналы стали закрытыми или сменились), к прокси не относится.

## Откат (если новый прокси упадёт)

```bash
sudo cp /opt/mentor-bot/.deploy_backups/20260512_092752_env_before_proxy_rotation.env /opt/mentor-bot/.env
sudo chown mentorbot:mentorbot /opt/mentor-bot/.env && sudo chmod 600 /opt/mentor-bot/.env
sudo systemctl restart mentor-bot
```

Поскольку старый прокси сам нерабочий — откат восстановит только последний рабочий конфиг до ротации, но не функциональность. Откатываемся только если новый прокси тоже отвалится и нужно зафиксировать «было плохо везде».

## Smoke-проверка

1. `journalctl -u mentor-bot --since "5 minutes ago" | grep "Watcher реакций"` — должно быть `прокси включен (9wuhgZ99...)` и НЕ должно быть `407 Proxy Authentication Required`.
2. Подождать поставленную реакцию в наблюдаемом канале — должна появиться новая запись в `owner_channel_reactions`.
3. AI-ответы в outreach — проверить, что LLM-обращения не падают по `407`.
