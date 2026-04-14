# Деплой на сервер (изолированно от прода)

## 1. Копирование проекта

```bash
mkdir -p /opt/okidoki-registration-test-bot
```

Скопируй содержимое `okidoki_registration_test_bot` в `/opt/okidoki-registration-test-bot`.

## 2. Виртуальное окружение

```bash
cd /opt/okidoki-registration-test-bot
python3 -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install -r requirements.txt
```

## 3. Конфиг

Создай `/opt/okidoki-registration-test-bot/.env`:

```env
BOT_TOKEN=<отдельный тестовый токен бота>
OKIDOKI_API_TOKEN=<токен OkiDoki>
OKIDOKI_API_BASE=https://api.doki.online
MENTOR_CHAT_ID=<chat_id ментора, опционально>
MENTOR_CONTACT_URL=https://t.me/mr_winchester1
TEST_EXCEPTION_USERNAME=artkozk
DB_PATH=/opt/okidoki-registration-test-bot/data/testbot.db
LOG_FILE=/opt/okidoki-registration-test-bot/logs/testbot.log
```

## 4. Systemd

```bash
cp deploy/okidoki-registration-test-bot.service /etc/systemd/system/
systemctl daemon-reload
systemctl enable --now okidoki-registration-test-bot
systemctl status okidoki-registration-test-bot --no-pager
```

## 5. Логи

```bash
journalctl -u okidoki-registration-test-bot -f
tail -f /opt/okidoki-registration-test-bot/logs/testbot.log
```
