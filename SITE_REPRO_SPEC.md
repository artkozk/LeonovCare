# SITE_REPRO_SPEC

## 1. Цель сайта

Сайт продает и объясняет форматы карьерного менторства в IT:

- старт с нуля до оффера;
- подготовка к интервью;
- рост грейда и дохода;
- сервис автооткликов;
- бесплатные roadmap-материалы через Telegram-бота.

Ключевая цель — перевод пользователя в Telegram (ментор/бот) с максимально конкретным контекстом выбранного тарифа.

## 2. Почему архитектура именно такая

### 2.1 Статический стек (HTML/CSS/JS, без SPA-фреймворка)

Причины:

- очень быстрый деплой на любой VPS + nginx;
- минимум операционного риска и зависимостей;
- высокая предсказуемость рендера и SEO;
- простая передача проекта между исполнителями.

### 2.2 Централизация контента в `config.js`

Причины:

- контент и тарифы меняются часто, а логика — редко;
- редактура в одном файле, без дублирования по страницам;
- возможность автоматической сборки/обновления данных в будущем.

### 2.3 Единый рантайм `script.js`

Причины:

- одинаковая логика ссылок/кнопок/карт по всем страницам;
- меньше расхождений поведения между лендингами;
- проще тестировать и поддерживать.

### 2.4 Data-attribute подход

Используются `data-page`, `data-track-page`, `data-link`, `data-bot-*`, `data-cart-*`.

Причины:

- привязка поведения к семантике элементов без тяжелого JS-фреймворка;
- модульность и возможность точечного переиспользования компонентов.

## 3. Файловая структура (ядро)

- `index.html` — главная.
- `zero-offer.html` — услуга “с нуля”.
- `interview-prep.html` — услуга “после курсов”.
- `grade-salary.html` — рост компенсации.
- `autoapply.html` — автоотклики + калькулятор.
- `directions.html` — каталог направлений.
- `python.html`, `java.html`, `golang.html`, `frontend.html`, `php.html`, `qa.html`, `ml.html`, `analytics.html` — страницы треков.
- `offer.html` — оферта.
- `config.js` — данные и настройки.
- `script.js` — логика рендера/интерактива.
- `styles.css` — дизайн-система.
- `sitemap.xml`, `robots.txt` — SEO.

## 4. Контентная модель `config.js`

`config.js` — это single source of truth для контента.

### 4.1 Ссылки и коммуникация

- `mentorUrl`
- `channelUrl`
- `botUrl`
- `prefilledMessage`

### 4.2 Тарифные блоки

- `packagePrices`
- `mentorshipPackages`
- `paymentModes`
- `servicePillars`
- `serviceTariffs`

`serviceTariffs` поддерживает:

- `oldPrice` + `price` (перечеркнутая/новая цена);
- `altPrice` (вторая цена под основной, другим цветом);
- `features`;
- `linkType` (`bot`/`mentor`).

### 4.3 Автоотклики

- `autoapply.rateRules`
- `autoapply.packs`
- `autoapply.subscriptions`

### 4.4 Направления

- `languageTracks` — 8 направлений:
  - `java`
  - `golang`
  - `frontend`
  - `python`
  - `php`
  - `qa`
  - `analytics`
  - `ml`

Каждый объект содержит:

- `slug`, `name`, `page`;
- `short`, `lead`;
- `whyLanguage`;
- `beginnerBenefits`;
- `experiencedBenefits`;
- `whatBuild`;
- `interviewFocus`;
- `firstProjects`;
- `roadmap`.

### 4.5 График ориентиров по доходу

- `languageSalaryChart`
- в графике оставлены стековые направления разработки;
- QA/Analytics/ML упомянуты отдельно как ролевые треки.

### 4.6 Отзывы API

`reviews`:

- `apiBaseUrl` = `https://api.it-mentors.ru/v1`
- `telegramId` = обязательный numeric ID ментора
- `perPage` (0 = все)

## 5. Поведение `script.js`

`script.js` — IIFE, запускается после загрузки `config.js`.

### 5.1 Рендер-модули

- `renderPricing`
- `renderPaymentModes`
- `renderAutoapplyPricing`
- `renderServicePillars`
- `renderServiceTariffs`
- `renderTrackCards`
- `renderTrackPage`
- `renderSalaryChart`
- `renderReviewsFromApi`

### 5.2 Telegram/deep-link логика

Ключевые функции:

- `toTelegramUrl`
- `buildStartPayload`
- `buildRouteMessage`
- `buildBotRouteLink`
- `buildBotCheckoutLink`

Payload формат:

- `lc2_<route>_<service>_<plan>_<source>_c<clicks>`

Где `route`:

- `home`
- `materials`
- `enroll`
- `auto`
- `student`

### 5.3 Корзина тарифа (localStorage)

Ключ: `lc_tariff_cart_v1`.

Хранит:

- услугу;
- план;
- цену;
- источник кнопки;
- параметры автооткликов;
- флаг рассрочки.

### 5.4 Калькулятор автооткликов

Функция: `calcAutoapplyPrice`.

Логика:

- `< 200`: 7 ₽, первые 50 бесплатно;
- `200-499`: 6 ₽;
- `>= 500`: 5 ₽.

### 5.5 Отзывы из API

Функция: `renderReviewsFromApi`.

Алгоритм:

1. Проверяет наличие `apiBaseUrl` и `telegramId`.
2. GET:
   - `/integrations/mentors/reviews/{telegramId}?page=1&per_page={N}`
3. Поддерживает форматы:
   - `payload.result.items`
   - `payload.result.reviews`
4. Нормализует запись:
   - `message`/`text`
   - `author.username`
5. Рендерит карточки в `#reviews-api-grid`.
6. Показывает статус:
   - загрузка;
   - пусто;
   - ошибка.

Причина именно такой обработки:

- API может возвращать данные в разных обертках;
- сайт не должен падать из-за неполной схемы ответа.

### 5.6 UI-интерактив

- адаптивное меню;
- smart-header (скрытие/показ при скролле);
- FAQ accordion;
- reveal-анимации через `IntersectionObserver`;
- актуальный год в футере.

`setupLightbox` и `setupReviewsCarousel` сохранены в коде как обратная совместимость, но в текущей главной не используются (блок фото-отзывов удален).

## 6. Страницы и шаблонная структура

Все страницы используют единый каркас:

- `header.site-header` + бренд + навигация;
- контентные секции `section` с `data-section`;
- футер с офертой.

Навигация унифицирована как “Направления и рынок”.

## 7. Дизайн-система (`styles.css`)

Основные принципы:

- карточный layout;
- градиентные акценты;
- единые кнопки (`btn-primary`, `btn-ghost`, `btn-secondary`);
- единый паттерн списков с маркерами (`.pricing-list`);
- адаптивные grid-сетки (`cards-2`, `cards-3`);
- отдельные стили второй цены (`.pricing-price .price-alt`);
- стили API-отзывов (`.review-api-*`).

## 8. SEO и индексация

- `sitemap.xml` содержит все ключевые страницы, включая новые:
  - `qa.html`, `php.html`, `ml.html`, `analytics.html`
- `robots.txt` и canonical-теги на страницах.

## 9. Что нужно сохранить, чтобы “воспроизведение” было 1:1

Другая нейросеть должна повторить:

1. Стек: чистый HTML/CSS/JS без фреймворка.
2. Центральный data-layer в `config.js`.
3. Единый рантайм `script.js` с тем же жизненным циклом рендера.
4. Telegram deep-link payload формата `lc2_*`.
5. Логику корзины тарифа в localStorage.
6. Калькулятор автооткликов с tier-правилами.
7. Отзывы через API (не через фото), с fallback-состояниями.
8. 8 направлений и отдельные страницы под каждый трек.
9. Карточный UI + responsive + анимации появления.

## 10. Минимальный чеклист перед релизом

1. `node --check config.js`
2. `node --check script.js`
3. Проверить `reviews.telegramId` (не пустой).
4. Проверить открытие всех страниц из `sitemap.xml`.
5. Проверить Telegram-кнопки:
   - главная;
   - тарифные карточки;
   - автоотклики;
   - материалы.
6. Проверить адаптив:
   - desktop;
   - tablet;
   - mobile.

## 11. Почему этот подход удобен для последующей автоматизации

- AI/скрипт может обновлять контент, редактируя один объект в `config.js`.
- Можно добавить сборку из CMS/Google Sheets, не меняя HTML.
- API-отзывы уже отделены от верстки и не требуют ручной модерации картинок.

