# PROD Sync And Backup Report (2026-04-22)

## Цель

Синхронизировать локальный проект и Git с текущим продом на сервере и сделать полный резерв данных, чтобы исключить потерю проекта и БД.

## Почему источник истины — сервер

1. По договоренности в задаче прод на сервере указан как актуальная версия.
2. Перед началом работ проверен git-статус на сервере в `/main/LeonovCare/LeonovCare`.
3. Все выравнивание выполнено по фактическим продовым commit/hash, а не по локальным незакоммиченным изменениям.

## Проверка актуальности (локально / Git / сервер)

### Локально до синхронизации

1. В корневом репозитории `main` был на `61af06d`.
2. Были локальные расхождения по gitlink-пути `bot` и рабочему дереву `AutoHh/parser_hh`.

### На сервере (прод)

1. Путь прод-проекта: `/main/LeonovCare/LeonovCare`.
2. Продовый superproject commit: `282127ff75df64825d4491dd6c4436ab2ce73bb4`.
3. Продовый gitlink `bot`: `5fbc4c311c76bb5a860756142b76bebe29a9071a`.
4. Продовый gitlink `AutoHh/parser_hh`: `d6995171b760df85e1d576c2bdd5f51e1459020e`.

### Что выровнено

1. Локальный указатель `bot` выставлен на продовый hash `5fbc4c311c76bb5a860756142b76bebe29a9071a`.
2. `AutoHh/parser_hh` выставлен на `d6995171b760df85e1d576c2bdd5f51e1459020e` (как в проде).
3. Итоговая фиксация изменений выполнена в `main` с коммитом и пушем.

## Бэкап на сервере

Время среза: `20260422_231005`  
Каталог: `/main/LeonovCare/backups/20260422_231005`

Сформированы файлы:

1. `leonovcare_project_full.tar.gz` — полный архив проекта `/main/LeonovCare/LeonovCare` (включая `.git`).
2. `leonovcare_superproject.bundle` — git bundle superproject для переносимого восстановления истории.
3. `leonovcare_bot_dir_snapshot.tar.gz` — архив фактического каталога `bot` как он лежит на проде.
4. `postgresql_all.sql.gz` — полный дамп PostgreSQL (`pg_dumpall` от пользователя `postgres`).
5. `sqlite_and_db_files.tar.gz` — архив sqlite/db-файлов из путей:
   - `/opt/mentor-bot/mentor_bot.db`
   - `/opt/mentor-bot/data/mentor_bot.db`
   - `/opt/mentor-bot/data/bot.sqlite3`
   - `/opt/mentor-bot/data/app.db`
   - `/opt/okidoki-registration-test-bot/data/testbot.db`
   - `/root/untitled1/data/interview_assistant.mv.db`
   - плюс каталогов локальных backup-баз бота (`/opt/mentor-bot/data/backups`, `/opt/mentor-bot/data/db_backups`) при наличии.
6. `backup_manifest.txt` — манифест фиксации hash/branch/состояния.
7. `SHA256SUMS.txt` — контрольные суммы серверных файлов.

## Бэкап в Git-репозитории (локально)

Серверные архивы скачаны и добавлены в репозиторий:

`backups/production/20260422_231005/`

Содержимое каталога идентично серверному набору файлов, проверено сравнением SHA256 с `SHA256SUMS.txt`.

## Почему сделано именно так

1. Архив проекта + git bundle дают два независимых сценария восстановления: файловый и git-исторический.
2. Отдельный дамп PostgreSQL обязателен, потому что сервис БД активен на сервере и содержит runtime-состояние.
3. Отдельный архив sqlite/db-файлов нужен для сервисов ботов, которые хранят состояние не только в Postgres.
4. Контрольные суммы добавлены, чтобы в любой момент доказать целостность копии после переноса.
5. Выравнивание по серверному commit предотвращает ситуацию, когда в Git остаётся версия, не совпадающая с реальным продом.

## Инструкция восстановления (пошагово)

### Восстановление superproject как рабочей копии

1. Создать целевую директорию:
   `mkdir -p /main/LeonovCare`
2. Распаковать архив проекта:
   `tar -xzf leonovcare_project_full.tar.gz -C /main/LeonovCare`
3. Проверить commit:
   `cd /main/LeonovCare/LeonovCare && git rev-parse HEAD`

### Восстановление из git bundle

1. Клонировать bundle:
   `git clone leonovcare_superproject.bundle LeonovCare_restored`
2. Проверить ветки и commit:
   `cd LeonovCare_restored && git branch -a && git log --oneline -n 20`

### Восстановление PostgreSQL

1. Распаковать SQL:
   `gunzip -c postgresql_all.sql.gz > postgresql_all.sql`
2. Применить дамп:
   `runuser -u postgres -- psql -f postgresql_all.sql`

### Восстановление sqlite/db-файлов

1. Распаковать архив:
   `tar -xzf sqlite_and_db_files.tar.gz`
2. Разложить файлы по целевым runtime-путям сервисов.
3. Проверить владельцев/права после копирования (`chown/chmod` при необходимости).

## Дополнение по прозрачности процесса

В процессе создавались промежуточные попытки backup-папок с неуспешными шагами аутентификации PostgreSQL. Финальным и валидным набором для восстановления считать только срез:

`/main/LeonovCare/backups/20260422_231005`

