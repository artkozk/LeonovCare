# LeonovCare Site

Статический маркетинговый сайт карьерного менторства в IT с интеграцией Telegram-бота, тарифов, калькулятора автооткликов и отзывов через API.

## Что внутри

- `index.html` — главная страница с офферами, FAQ, отзывами из API.
- `zero-offer.html`, `interview-prep.html`, `grade-salary.html`, `autoapply.html` — сервисные лендинги.
- `directions.html` — хаб направлений.
- `java.html`, `golang.html`, `frontend.html`, `python.html`, `php.html`, `qa.html`, `ml.html`, `analytics.html`, `devops.html`, `mobile.html`, `data-engineering.html` — страницы треков.
- `interview-helper.html` — страница программы помощи во время собеседований.
- `config.js` — центральный источник контента и бизнес-настроек.
- `script.js` — единая клиентская логика рендера/интерактива.
- `styles.css` — дизайн-система и стили компонентов.
- `offer.html` — публичная оферта.
- `sitemap.xml`, `robots.txt` — SEO-технические файлы.

## Ключевая архитектура

- Сайт **данно-ориентированный**: тексты, тарифы, направления, график зарплат и API-настройки лежат в `config.js`.
- Все рендеры строятся в `script.js` по `data-*` и id-контейнерам, без фреймворка.
- Telegram-переходы формируются с deep-link payload для маршрутизации внутри бота.
- Отзывы грузятся через API IT Mentors (без фото-блока и без ручного обновления контента).

## Быстрый запуск локально

Любой статический сервер:

```powershell
cd C:\prog\Comercial\LeonovCareSite
python -m http.server 8080
```

Открыть: `http://localhost:8080`.

## Настройка отзывов API

В `config.js`:

```js
const reviews = {
  apiBaseUrl: "https://api.it-mentors.ru/v1",
  telegramId: "ВАШ_NUMERIC_TG_ID",
  perPage: 0
};
```

- `telegramId` — обязателен (числовой ID ментора).
- `perPage: 0` — запрашивать все отзывы.

## Полная спецификация

Подробный документ для воспроизведения проекта другой нейросетью:

- [`SITE_REPRO_SPEC.md`](SITE_REPRO_SPEC.md)

## Адаптив: защита от поломок

Базовый responsive-hardened слой вынесен в `styles.css` и покрывает все страницы, которые используют общий шаблон:

- Включен защитный режим от горизонтального переполнения (`overflow-x: clip`) и добавлены `min-width: 0` для ключевых flex/grid-контейнеров.
- Для текстов добавлен `overflow-wrap: anywhere`, чтобы длинные фразы и ссылки не ломали сетку на узких экранах.
- Кнопки переведены в безопасный режим (`max-width: 100%`, перенос текста внутри кнопки).
- Усилен брейкпоинт `<=760px`: более компактный header, контролируемая типографика, стек кнопок в колонку.
- Добавлен брейкпоинт `<=480px`: компактные отступы карточек/CTA, уменьшенные кнопки и безопасные размеры модальных/навигационных элементов.

Рекомендуемая ручная проверка перед релизом:

1. `320x568`
2. `375x667`
3. `390x844`
4. `768x1024`
5. `1280x800`

Проверяем, что нет горизонтального скролла, обрезанных кнопок/заголовков, наложений меню и «ломаных» карточек.

## Деплой на прод leonovcare.ru

Nginx-конфиг домена указывает на директорию:

- `/main/LeonovCare/LeonovCare`

Базовый деплой через git:

```bash
ssh root@85.198.82.221
cd /main/LeonovCare/LeonovCare
git fetch --all
git pull --ff-only origin main
nginx -t && systemctl reload nginx
```

Проверка:

1. `curl -I https://leonovcare.ru/`
2. Открыть главную + `interview-prep.html` с мобильным viewport в DevTools.
