# LeonovCare Site

Статический маркетинговый сайт карьерного менторства в IT с интеграцией Telegram-бота, тарифов, калькулятора автооткликов и отзывов через API.

## Что внутри

- `index.html` — главная страница с офферами, FAQ, отзывами из API.
- `zero-offer.html`, `interview-prep.html`, `grade-salary.html`, `autoapply.html` — сервисные лендинги.
- `directions.html` — хаб направлений.
- `java.html`, `golang.html`, `frontend.html`, `python.html`, `php.html`, `qa.html`, `ml.html`, `analytics.html` — страницы треков.
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

