(() => {
  const config = window.siteConfig;
  if (!config) {
    return;
  }

  const formatMoney = (value) => {
    const num = Number(value);
    if (!Number.isFinite(num)) {
      return "По запросу";
    }
    return `${new Intl.NumberFormat("ru-RU").format(num)} ₽`;
  };

  const toTelegramUrl = (baseUrl, message, extraParams = {}) => {
    if (!baseUrl) {
      return "#";
    }
    try {
      const url = new URL(baseUrl);
      if (message) {
        url.searchParams.set("text", message);
      }
      Object.entries(extraParams).forEach(([key, value]) => {
        if (value !== null && value !== undefined && `${value}`.trim() !== "") {
          url.searchParams.set(key, String(value));
        }
      });
      return url.toString();
    } catch (error) {
      const params = new URLSearchParams();
      if (message) {
        params.set("text", message);
      }
      Object.entries(extraParams).forEach(([key, value]) => {
        if (value !== null && value !== undefined && `${value}`.trim() !== "") {
          params.set(key, String(value));
        }
      });
      const serialized = params.toString();
      if (!serialized) {
        return baseUrl;
      }
      const separator = baseUrl.includes("?") ? "&" : "?";
      return `${baseUrl}${separator}${serialized}`;
    }
  };

  const botBaseUrl = (config.botUrl && config.botUrl.trim())
    ? config.botUrl.trim()
    : config.mentorUrl;

  const mentorLink = toTelegramUrl(config.mentorUrl, config.prefilledMessage);
  const channelLink = config.channelUrl || "#";
  const cartStorageKey = "lc_tariff_cart_v1";

  const serviceTitleMap = {
    "zero-offer": "С нуля до оффера",
    "interview-prep": "После курсов до оффера",
    "grade-salary": "Увеличение зарплаты",
    "autoapply": "Автоотклики"
  };

  const routeByService = {
    "zero-offer": "enroll",
    "interview-prep": "enroll",
    "grade-salary": "enroll",
    "autoapply": "auto"
  };

  const routeLabels = {
    home: "Главное меню",
    student: "Раздел \"Я ученик\"",
    enroll: "Раздел \"Вступить на обучение\"",
    auto: "Раздел \"Автоотклики\"",
    materials: "Раздел \"Бесплатные материалы\""
  };

  const escapeHtml = (value) => String(value || "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;");

  const escapeAttr = (value) => escapeHtml(value).replace(/"/g, "&quot;");

  const compactToken = (value, max = 14) => String(value || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "")
    .slice(0, max);

  const normalizeRoute = (route) => {
    const token = compactToken(route || "home", 16);
    if (token === "student" || token === "auto" || token === "materials" || token === "enroll" || token === "home") {
      return token;
    }
    return "home";
  };

  const routeForService = (serviceSlug) => routeByService[serviceSlug] || "home";

  const buildStartPayload = ({
    route,
    serviceSlug,
    planSlug,
    source,
    clicks
  }) => {
    const routeToken = normalizeRoute(route || routeForService(serviceSlug));
    const serviceToken = compactToken(serviceSlug || "general", 10) || "general";
    const planToken = compactToken(planSlug || "na", 8) || "na";
    const sourceToken = compactToken(source || "site", 12) || "site";
    const clicksValue = Number.isFinite(Number(clicks))
      ? Math.max(Math.round(Number(clicks)), 0)
      : 0;
    const clickToken = `c${String(clicksValue).slice(0, 4) || "0"}`;
    return `lc2_${routeToken}_${serviceToken}_${planToken}_${sourceToken}_${clickToken}`.slice(0, 63);
  };

  const buildRouteMessage = ({
    route,
    source,
    planTitle,
    planPrice,
    clicks,
    totalPrice,
    installments
  }) => {
    const routeToken = normalizeRoute(route);
    const lines = ["Привет! Я перешел(ла) с сайта в бот."];
    lines.push(`Нужный раздел: ${routeLabels[routeToken] || routeLabels.home}.`);
    if (source) {
      lines.push(`Источник кнопки: ${source}.`);
    }
    if (planTitle) {
      lines.push(`Выбранный тариф: ${planTitle}.`);
    }
    if (planPrice) {
      lines.push(`Стоимость на сайте: ${planPrice}.`);
    }
    if (Number(clicks) > 0) {
      lines.push(`Количество откликов: ${clicks}.`);
      if (Number(totalPrice) > 0) {
        lines.push(`Расчет: ${formatMoney(totalPrice)}.`);
      }
    }
    if (installments) {
      lines.push("Хочу обсудить оплату в несколько платежей.");
    }
    lines.push("Подскажите следующий шаг.");
    return lines.join("\n");
  };

  const readCartState = () => {
    const fallback = {
      serviceSlug: "",
      serviceTitle: "",
      planTitle: "",
      planKey: "",
      planPrice: "",
      sourceButton: "",
      autoapplyClicks: 0,
      totalPrice: 0,
      installments: false
    };

    try {
      const raw = window.localStorage.getItem(cartStorageKey);
      if (!raw) {
        return fallback;
      }
      const parsed = JSON.parse(raw);
      return {
        ...fallback,
        ...parsed
      };
    } catch (error) {
      return fallback;
    }
  };

  const writeCartState = (nextState) => {
    try {
      window.localStorage.setItem(cartStorageKey, JSON.stringify(nextState));
    } catch (error) {
      // Ignore storage errors (private mode, disabled storage).
    }
  };

  const buildCartMessage = (cart) => {
    const lines = [
      "Привет! Хочу оформить тариф через сайт."
    ];

    if (cart.serviceTitle) {
      lines.push(`Направление: ${cart.serviceTitle}`);
    }
    if (cart.planTitle) {
      lines.push(`Тариф: ${cart.planTitle}`);
    }
    if (cart.planPrice) {
      lines.push(`Стоимость на сайте: ${cart.planPrice}`);
    }
    if (cart.sourceButton) {
      lines.push(`Кнопка на сайте: ${cart.sourceButton}`);
    }
    if (Number(cart.autoapplyClicks) > 0) {
      lines.push(`Откликов: ${cart.autoapplyClicks}`);
      if (Number(cart.totalPrice) > 0) {
        lines.push(`Расчёт автооткликов: ${formatMoney(cart.totalPrice)}`);
      }
    }
    if (cart.installments) {
      lines.push("Хочу разделить оплату на несколько платежей.");
    }

    lines.push("Пожалуйста, подтвердите детали и шаги старта.");
    return lines.join("\n");
  };

  const buildBotCheckoutLink = (cart) => {
    const payload = buildStartPayload({
      route: routeForService(cart.serviceSlug),
      serviceSlug: cart.serviceSlug,
      planSlug: cart.planKey || cart.planTitle,
      source: cart.sourceButton || cart.planTitle || "tariff",
      clicks: cart.autoapplyClicks
    });
    const message = buildCartMessage(cart);
    return toTelegramUrl(botBaseUrl, message, {
      start: payload
    });
  };

  const buildBotRouteLink = ({
    route,
    source,
    serviceSlug = "",
    planSlug = "",
    clicks = 0,
    extraMessage = ""
  }) => {
    const routeToken = normalizeRoute(route);
    const payload = buildStartPayload({
      route: routeToken,
      serviceSlug,
      planSlug,
      source,
      clicks
    });
    const baseMessage = buildRouteMessage({
      route: routeToken,
      source,
      clicks
    });
    const message = extraMessage
      ? `${baseMessage}\n${extraMessage}`
      : baseMessage;
    return toTelegramUrl(botBaseUrl, message, {
      start: payload
    });
  };

  const applyLinks = () => {
    const linkMap = {
      mentor: mentorLink,
      channel: channelLink
    };

    document.querySelectorAll("[data-link]").forEach((node) => {
      const type = node.getAttribute("data-link");
      let href = linkMap[type] || "#";

      if (type === "bot") {
        const explicitRoute = node.getAttribute("data-bot-route");
        const hasRoadmapWord = (node.textContent || "").toLowerCase().includes("roadmap");
        const route = explicitRoute || (hasRoadmapWord ? "materials" : "home");
        const source = node.getAttribute("data-bot-source")
          || node.getAttribute("id")
          || document.body.dataset.page
          || "site";
        const cart = readCartState();
        const serviceSlug = node.getAttribute("data-bot-service")
          || (route === "auto" ? "autoapply" : "");
        const planSlug = node.getAttribute("data-bot-plan") || "";
        const clicks = route === "auto" ? cart.autoapplyClicks : 0;
        href = buildBotRouteLink({
          route,
          source,
          serviceSlug,
          planSlug,
          clicks
        });
      }

      node.setAttribute("href", href);
      if (href !== "#") {
        node.setAttribute("target", "_blank");
        node.setAttribute("rel", "noopener noreferrer");
      }
    });

    const botNote = document.querySelector("[data-bot-note]");
    if (botNote) {
      botNote.textContent = (config.botUrl && config.botUrl.trim())
        ? "Бот активен: внутри бесплатные материалы, roadmap, автоотклики, вступление на обучение и доступ в сообщество."
        : "Бот не указан: кнопка ведет в Telegram к ментору с предзаполненной заявкой.";
    }
  };

  const renderPricing = () => {
    const grid = document.getElementById("pricing-grid");
    if (!grid || !Array.isArray(config.mentorshipPackages)) {
      return;
    }

    const items = config.mentorshipPackages.map((item) => {
      const price = config.packagePrices[item.priceKey] || "По запросу";
      const features = (item.features || [])
        .map((feature) => `<li>${feature}</li>`)
        .join("");

      return `
        <article class="card pricing-card ${item.featured ? "is-featured" : ""}" data-reveal>
          <span class="pricing-badge">${item.badge || "Формат"}</span>
          <h3>${item.title}</h3>
          <p class="muted">${item.subtitle || ""}</p>
          <p class="pricing-price">${price}</p>
          <ul class="pricing-list">${features}</ul>
          <a class="btn ${item.linkType === "bot" ? "btn-secondary" : "btn-primary"}" data-link="${item.linkType || "mentor"}" href="#">${item.ctaLabel || "Выбрать формат"}</a>
        </article>
      `;
    });

    grid.innerHTML = items.join("");
  };

  const renderPaymentModes = () => {
    const list = document.getElementById("payment-options");
    if (!list || !Array.isArray(config.paymentModes)) {
      return;
    }
    list.innerHTML = config.paymentModes.map((item) => `<li>${item}</li>`).join("");
  };

  const renderAutoapplyPricing = () => {
    const ratesList = document.getElementById("autoapply-rates");
    const packsList = document.getElementById("autoapply-packs");
    const subsList = document.getElementById("autoapply-subscriptions");
    const autoapply = config.autoapply || {};

    if (ratesList) {
      const rates = Array.isArray(autoapply.rateRules) ? autoapply.rateRules : [];
      ratesList.innerHTML = rates
        .map((item) => `<li>${item.range}: <strong>${item.perClick || "По запросу"}</strong></li>`)
        .join("");
    }

    if (packsList) {
      const packs = Array.isArray(autoapply.packs) ? autoapply.packs : [];
      packsList.innerHTML = packs
        .map((pack) => `
          <li>
            ${pack.name}: ${pack.oldPrice ? `<s>${pack.oldPrice}</s> ` : ""}<strong>${pack.price || "По запросу"}</strong>
            ${pack.note ? `<br><span class="muted">${pack.note}</span>` : ""}
          </li>
        `)
        .join("");
    }

    if (subsList) {
      const subscriptions = Array.isArray(autoapply.subscriptions) ? autoapply.subscriptions : [];
      subsList.innerHTML = subscriptions
        .map((subscription) => `
          <li>
            ${subscription.name}: ${subscription.oldPrice ? `<s>${subscription.oldPrice}</s> ` : ""}<strong>${subscription.price || "По запросу"}</strong>
            ${subscription.note ? `<br><span class="muted">${subscription.note}</span>` : ""}
          </li>
        `)
        .join("");
    }
  };

  const renderReviewsFromApi = async () => {
    const track = document.getElementById("reviews-api-track");
    const status = document.getElementById("reviews-api-status");
    if (!track) {
      return;
    }

    const setStatus = (text) => {
      if (status) {
        status.textContent = text;
      }
    };

    const reviewsConfig = config.reviews || {};
    const apiBaseUrl = String(reviewsConfig.apiBaseUrl || "").trim().replace(/\/+$/, "");
    const telegramId = String(reviewsConfig.telegramId || "").trim();
    const perPageRaw = Number.parseInt(reviewsConfig.perPage, 10);
    const perPage = Number.isFinite(perPageRaw) && perPageRaw >= 0 ? perPageRaw : 0;
    const previewCharsRaw = Number.parseInt(reviewsConfig.previewChars, 10);
    const previewChars = Number.isFinite(previewCharsRaw) && previewCharsRaw >= 80 ? previewCharsRaw : 220;

    if (!apiBaseUrl || !telegramId) {
      track.innerHTML = "";
      setStatus("Отзывы скоро появятся.");
      return;
    }

    const toReview = (item, index) => {
      const source = (item && typeof item === "object" && item.review && typeof item.review === "object")
        ? item.review
        : item;
      if (!source || typeof source !== "object") {
        return null;
      }

      const author = (source.author && typeof source.author === "object")
        ? source.author
        : ((item && item.author && typeof item.author === "object") ? item.author : {});

      const username = String(author.username || source.username || "").trim();
      const message = String(source.message || source.text || "").trim();
      const id = source.id || item.id || `r${index + 1}`;
      const createdAt = String(source.created_at || source.createdAt || item.created_at || "").trim();

      if (!message) {
        return null;
      }

      return { id, username, message, createdAt };
    };

    const makePreview = (text, limit) => {
      if (text.length <= limit) {
        return {
          shortText: text,
          trimmed: false
        };
      }

      const candidate = text.slice(0, limit + 1);
      const lastSpace = candidate.lastIndexOf(" ");
      const cutIndex = lastSpace > Math.floor(limit * 0.6) ? lastSpace : limit;
      return {
        shortText: text.slice(0, cutIndex).trimEnd(),
        trimmed: true
      };
    };

    try {
      setStatus("Загружаем отзывы…");
      const url = new URL(`${apiBaseUrl}/integrations/mentors/reviews/${encodeURIComponent(telegramId)}`);
      url.searchParams.set("page", "1");
      url.searchParams.set("per_page", String(perPage));

      const response = await fetch(url.toString(), {
        headers: {
          Accept: "application/json"
        }
      });

      if (!response.ok) {
        throw new Error(`reviews_api_status_${response.status}`);
      }

      const payload = await response.json();
      const result = (payload && typeof payload === "object" && payload.result && typeof payload.result === "object")
        ? payload.result
        : payload;

      const rawItems = Array.isArray(result?.items)
        ? result.items
        : (Array.isArray(result?.reviews) ? result.reviews : []);
      const items = rawItems
        .map((item, index) => toReview(item, index))
        .filter(Boolean);

      if (!items.length) {
        track.innerHTML = "";
        setStatus("Пока нет опубликованных отзывов.");
        return;
      }

      track.innerHTML = items
        .map((item) => {
          const authorHtml = item.username
            ? `<a href="https://t.me/${encodeURIComponent(item.username)}" target="_blank" rel="noopener noreferrer">@${escapeHtml(item.username)}</a>`
            : "Анонимный отзыв";
          const messageHtml = escapeHtml(item.message).replace(/\n/g, "<br>");
          const preview = makePreview(item.message, previewChars);
          const previewHtml = preview.trimmed
            ? `${escapeHtml(preview.shortText).replace(/\n/g, "<br>")}…`
            : messageHtml;
          let dateText = "";
          if (item.createdAt) {
            const parsedDate = new Date(item.createdAt);
            if (!Number.isNaN(parsedDate.getTime())) {
              dateText = new Intl.DateTimeFormat("ru-RU", {
                day: "2-digit",
                month: "2-digit",
                year: "numeric"
              }).format(parsedDate);
            }
          }
          const dateHtml = dateText ? `<span class="review-api-date">${dateText}</span>` : "";
          const expandButtonHtml = preview.trimmed
            ? '<button class="review-expand-btn" type="button" data-review-open>Читать полностью</button>'
            : "";
          return `
            <article class="card review-card review-api-card">
              <div class="review-api-head">
                <p class="review-api-author">${authorHtml}</p>
                ${dateHtml}
              </div>
              <div class="review-api-body">
                <p class="review-api-text review-api-text-short">${previewHtml}</p>
                <p class="review-api-text review-api-text-full" hidden>${messageHtml}</p>
                ${expandButtonHtml}
              </div>
            </article>
          `;
        })
        .join("");

      setupReviewExpander();
      setupReviewsCarousel();
      setStatus(`Показано отзывов: ${items.length}.`);
    } catch (error) {
      track.innerHTML = "";
      setStatus("Временно не удалось загрузить отзывы. Попробуйте позже.");
    }
  };

  const renderServicePillars = () => {
    const grid = document.getElementById("service-pillars-grid");
    const services = Array.isArray(config.servicePillars) ? config.servicePillars : [];
    if (!grid || !services.length) {
      return;
    }

    grid.innerHTML = services
      .map((service) => `
        <article class="card service-card" data-reveal>
          <p class="eyebrow">${service.tag || service.slug}</p>
          <h3>${service.title}</h3>
          <p class="service-price">${service.priceFrom || "Цена по запросу"}</p>
          <p class="muted">${service.summary}</p>
          <ul class="pricing-list">
            ${(service.bullets || []).map((bullet) => `<li>${bullet}</li>`).join("")}
          </ul>
          <a class="btn btn-primary" href="${service.page}">${service.cta || "Открыть раздел"}</a>
        </article>
      `)
      .join("");
  };

  const renderServiceTariffs = () => {
    const page = document.body.dataset.page;
    const root = document.getElementById("service-tariffs");
    if (!page || !root || !config.serviceTariffs) {
      return;
    }

    const serviceData = config.serviceTariffs[page];
    if (!serviceData || !Array.isArray(serviceData.plans) || !serviceData.plans.length) {
      return;
    }

    const note = document.getElementById("service-tariffs-note");
    if (note) {
      note.textContent = serviceData.note || "";
    }

    root.innerHTML = serviceData.plans
      .map((plan, index) => {
        const serviceTitle = serviceTitleMap[page] || page;
        const installments = (serviceData.note || "").toLowerCase().includes("платеж");
        const planKey = compactToken(plan.planKey || plan.key || plan.slug || plan.title || `p${index + 1}`, 8) || `p${index + 1}`;
        const priceValue = plan.price || "По запросу";
        const altPriceValue = plan.altPrice || "";
        const mainPriceHtml = plan.oldPrice
          ? `<span class="price-old">${plan.oldPrice}</span><span class="price-new">${priceValue}</span>`
          : `<span class="price-new">${priceValue}</span>`;
        const altPriceHtml = altPriceValue
          ? `<span class="price-alt">${altPriceValue}</span>`
          : "";
        const priceHtml = `${mainPriceHtml}${altPriceHtml}`;
        return `
          <article class="card pricing-card ${plan.featured ? "is-featured" : ""}" data-reveal>
            <span class="pricing-badge">${plan.badge || "Тариф"}</span>
            <h3>${plan.title}</h3>
            <p class="muted">${plan.subtitle || ""}</p>
            <p class="pricing-price">${priceHtml}</p>
            <ul class="pricing-list">
              ${(plan.features || []).map((item) => `<li>${item}</li>`).join("")}
            </ul>
            <a
              class="btn btn-primary"
              data-link="${plan.linkType || "mentor"}"
              data-cart-add="true"
              data-cart-service="${escapeAttr(page)}"
              data-cart-service-title="${escapeAttr(serviceTitle)}"
              data-cart-plan="${escapeAttr(plan.title || "")}"
              data-cart-plan-key="${escapeAttr(planKey)}"
              data-cart-price="${escapeAttr(priceValue)}"
              data-cart-source="${escapeAttr(plan.cta || plan.title || "")}"
              data-cart-installments="${installments ? "true" : "false"}"
              href="#"
            >${plan.cta || "Оставить заявку"}</a>
          </article>
        `;
      })
      .join("");
  };

  const renderTrackCards = () => {
    const tracks = Array.isArray(config.languageTracks) ? config.languageTracks : [];

    const previewGrid = document.getElementById("track-preview-grid");
    if (previewGrid) {
      previewGrid.innerHTML = tracks
        .map((track) => `
          <article class="card direction-card" data-reveal>
            <p class="eyebrow">${track.name}</p>
            <h3>${track.short}</h3>
            <p class="muted">${track.lead}</p>
            <a class="btn btn-ghost" href="${track.page}">Открыть трек ${track.name}</a>
          </article>
        `)
        .join("");
    }

    const directionsGrid = document.getElementById("directions-grid");
    if (directionsGrid) {
      directionsGrid.innerHTML = tracks
        .map((track) => {
          const highlights = [
            ...(track.whyLanguage || []).slice(0, 2),
            (track.beginnerBenefits || [])[0],
            (track.experiencedBenefits || [])[0]
          ]
            .filter(Boolean)
            .slice(0, 3)
            .map((item) => `<li>${item}</li>`)
            .join("");

          return `
            <article class="card direction-detail-card" data-reveal>
              <h3>${track.name}</h3>
              <p class="muted">${track.short}</p>
              <ul class="pricing-list">${highlights}</ul>
              <a class="btn btn-primary" href="${track.page}">Подробнее о ${track.name}</a>
            </article>
          `;
        })
        .join("");
    }
  };

  const renderTrackPage = () => {
    const slug = document.body.dataset.trackPage;
    if (!slug) {
      return;
    }

    const tracks = Array.isArray(config.languageTracks) ? config.languageTracks : [];
    const track = tracks.find((item) => item.slug === slug);
    if (!track) {
      return;
    }

    const chartData = config.languageSalaryChart && Array.isArray(config.languageSalaryChart.data)
      ? config.languageSalaryChart.data
      : [];
    const salaryItem = chartData.find((item) => item.track === slug);

    const setText = (id, text) => {
      const node = document.getElementById(id);
      if (node) {
        node.textContent = text;
      }
    };

    const renderListCards = (id, items) => {
      const container = document.getElementById(id);
      if (!container || !Array.isArray(items)) {
        return;
      }
      container.innerHTML = items
        .map((item) => `<article class="card artifact-card" data-reveal><p>${item}</p></article>`)
        .join("");
    };

    const renderRoadmapCards = (id, items) => {
      const container = document.getElementById(id);
      if (!container || !Array.isArray(items)) {
        return;
      }

      container.classList.add("roadmap-flow");
      container.innerHTML = items
        .map((item, index) => `
          <article class="card process-card roadmap-step-card" data-reveal>
            <span class="step">Шаг ${index + 1}</span>
            <p>${item}</p>
          </article>
        `)
        .join("");
    };

    setText("track-title", track.name);
    setText("track-lead", track.lead || track.short || "");
    setText("track-salary", salaryItem ? formatMoney(salaryItem.value) : "По запросу");
    setText(
      "track-salary-note",
      config.languageSalaryChart
        ? `${config.languageSalaryChart.sourceTitle}. ${config.languageSalaryChart.note}`
        : ""
    );

    renderListCards("track-what-build", track.whatBuild);
    renderListCards("track-why-language", track.whyLanguage);
    renderListCards("track-beginner-benefits", track.beginnerBenefits);
    renderListCards("track-experienced-benefits", track.experiencedBenefits);
    renderListCards("track-interview-focus", track.interviewFocus);
    renderListCards("track-first-projects", track.firstProjects);
    renderRoadmapCards("track-roadmap", track.roadmap);
  };

  const renderSalaryChart = () => {
    const chartRoot = document.getElementById("salary-chart");
    if (!chartRoot || !config.languageSalaryChart || !Array.isArray(config.languageSalaryChart.data)) {
      return;
    }

    const sourceNode = document.getElementById("salary-chart-source");
    const detailNode = document.getElementById("salary-chart-detail");
    const sortButtons = Array.from(document.querySelectorAll("[data-chart-sort]"));

    let sortMode = "desc";

    const trackMap = new Map(
      (config.languageTracks || []).map((track) => [track.slug, track])
    );

    const getSorted = () => {
      const sorted = [...config.languageSalaryChart.data];
      if (sortMode === "desc") {
        sorted.sort((a, b) => b.value - a.value);
      } else {
        sorted.sort((a, b) => a.value - b.value);
      }
      return sorted;
    };

    const render = () => {
      const items = getSorted();
      const max = Math.max(...items.map((item) => item.value), 1);

      chartRoot.innerHTML = items
        .map((item) => {
          const width = Math.round((item.value / max) * 100);
          const focusBadge = item.focus
            ? `<span class="chart-badge">Фокус программы</span>`
            : "";
          return `
            <button class="chart-row" type="button" data-chart-item data-track="${item.track || ""}" data-name="${item.name}" data-value="${item.value}">
              <span class="chart-lang-wrap">
                <span class="chart-lang">${item.name}</span>
                ${focusBadge}
              </span>
              <span class="chart-track"><span class="chart-fill" style="width: ${width}%;"></span></span>
              <span class="chart-value">${formatMoney(item.value)}</span>
            </button>
          `;
        })
        .join("");

      chartRoot.querySelectorAll("[data-chart-item]").forEach((button) => {
        button.addEventListener("click", () => {
          const trackSlug = button.getAttribute("data-track");
          const name = button.getAttribute("data-name") || "Направление";
          const value = Number(button.getAttribute("data-value"));

          if (trackSlug && trackMap.has(trackSlug)) {
            const track = trackMap.get(trackSlug);
            if (detailNode) {
              detailNode.textContent = `${name}: ориентир ${formatMoney(value)}. Открываем страницу направления.`;
            }
            window.location.href = track.page;
            return;
          }

          if (detailNode) {
            detailNode.textContent = `${name}: ориентир ${formatMoney(value)}. По этому направлению можно открыть roadmap и материалы в Telegram-боте.`;
          }
        });
      });

      sortButtons.forEach((button) => {
        button.classList.toggle("is-active", button.getAttribute("data-chart-sort") === sortMode);
      });
    };

    sortButtons.forEach((button) => {
      button.addEventListener("click", () => {
        const mode = button.getAttribute("data-chart-sort");
        sortMode = mode === "asc" ? "asc" : "desc";
        render();
      });
    });

    if (sourceNode) {
      const { sourceTitle, sourceUrl, note } = config.languageSalaryChart;
      sourceNode.innerHTML = sourceUrl
        ? `Источник: <a href="${sourceUrl}" target="_blank" rel="noopener noreferrer">${sourceTitle}</a>. ${note}`
        : `${sourceTitle}. ${note}`;
    }

    render();
  };

  const calcAutoapplyPrice = (rawClicks) => {
    const clicks = Math.max(Number.parseInt(rawClicks, 10) || 0, 0);
    let perClick = 7;
    let freeClicks = 0;

    if (clicks < 200) {
      perClick = 7;
      freeClicks = Math.min(clicks, 50);
    } else if (clicks < 500) {
      perClick = 6;
    } else {
      perClick = 5;
    }

    const paidClicks = Math.max(clicks - freeClicks, 0);
    const total = paidClicks * perClick;

    return {
      clicks,
      perClick,
      freeClicks,
      paidClicks,
      total
    };
  };

  const setupTariffCartLinks = () => {
    const buttons = Array.from(document.querySelectorAll("[data-cart-add]"));
    if (!buttons.length) {
      return;
    }

    const buildStateForButton = (button, baseState) => {
      const serviceSlug = button.getAttribute("data-cart-service") || baseState.serviceSlug || "";
      const serviceTitle = button.getAttribute("data-cart-service-title")
        || serviceTitleMap[serviceSlug]
        || baseState.serviceTitle
        || "";
      const planTitle = button.getAttribute("data-cart-plan") || baseState.planTitle || "";
      const planKey = button.getAttribute("data-cart-plan-key") || baseState.planKey || "";
      const planPrice = button.getAttribute("data-cart-price") || baseState.planPrice || "";
      const sourceButton = button.getAttribute("data-cart-source")
        || (button.textContent || "").trim()
        || baseState.sourceButton
        || "";
      const installments = button.getAttribute("data-cart-installments") === "true";

      return {
        ...baseState,
        serviceSlug,
        serviceTitle,
        planTitle,
        planKey,
        planPrice,
        sourceButton,
        installments: installments || baseState.installments
      };
    };

    const setLink = (button, state) => {
      const href = buildBotCheckoutLink(state);
      button.setAttribute("href", href);
      button.setAttribute("target", "_blank");
      button.setAttribute("rel", "noopener noreferrer");
    };

    buttons.forEach((button) => {
      const refreshLink = () => {
        const state = buildStateForButton(button, readCartState());
        setLink(button, state);
      };

      refreshLink();

      button.addEventListener("mouseenter", refreshLink);
      button.addEventListener("focus", refreshLink);
      button.addEventListener("click", () => {
        const state = buildStateForButton(button, readCartState());
        writeCartState(state);
        setLink(button, state);
      });
    });
  };

  const setupAutoapplyCalculator = () => {
    const slider = document.getElementById("autoapply-slider");
    if (!(slider instanceof HTMLInputElement)) {
      return;
    }

    const valueNode = document.getElementById("autoapply-slider-value");
    const totalNode = document.getElementById("autoapply-slider-total");
    const rateNode = document.getElementById("autoapply-slider-rate");
    const noteNode = document.getElementById("autoapply-slider-note");
    const freeNode = document.getElementById("autoapply-slider-free");
    const checkoutButton = document.getElementById("autoapply-slider-btn");
    const installmentsToggle = document.getElementById("autoapply-installments");

    const setCheckoutLink = (state) => {
      if (!(checkoutButton instanceof HTMLAnchorElement)) {
        return;
      }
      checkoutButton.setAttribute("href", buildBotCheckoutLink(state));
      checkoutButton.setAttribute("target", "_blank");
      checkoutButton.setAttribute("rel", "noopener noreferrer");
    };

    const sync = () => {
      const calc = calcAutoapplyPrice(slider.value);
      const formattedClicks = new Intl.NumberFormat("ru-RU").format(calc.clicks);
      const installments = Boolean(
        installmentsToggle instanceof HTMLInputElement
        && installmentsToggle.checked
      );

      if (valueNode) {
        valueNode.textContent = `${formattedClicks} откликов`;
      }

      if (totalNode) {
        totalNode.textContent = formatMoney(calc.total);
      }

      if (rateNode) {
        rateNode.textContent = `${calc.perClick} ₽ / отклик`;
      }

      if (freeNode) {
        freeNode.textContent = calc.freeClicks > 0
          ? `Из них ${calc.freeClicks} откликов бесплатны (акция действует один раз на аккаунт).`
          : "Для объема от 200 откликов действует тариф по объёму без бесплатного старта.";
      }

      if (noteNode) {
        noteNode.textContent = calc.clicks < 200
          ? "Пробный формат: можно начать с малого объёма и проверить конверсию."
          : "Объём уже достаточный для стабильной воронки ответов от рынка.";
      }

      const state = {
        ...readCartState(),
        serviceSlug: "autoapply",
        serviceTitle: serviceTitleMap["autoapply"],
        planTitle: `Автоотклики: ${formattedClicks}`,
        planKey: `auto${calc.clicks}`,
        planPrice: formatMoney(calc.total),
        sourceButton: "autoapply-slider",
        autoapplyClicks: calc.clicks,
        totalPrice: calc.total,
        installments
      };

      writeCartState(state);
      setCheckoutLink(state);
    };

    slider.addEventListener("input", sync);
    slider.addEventListener("change", sync);

    if (installmentsToggle instanceof HTMLInputElement) {
      installmentsToggle.addEventListener("change", sync);
    }

    if (checkoutButton instanceof HTMLAnchorElement) {
      checkoutButton.addEventListener("click", sync);
    }

    sync();
  };

  const setupPageActiveNav = () => {
    const currentPage = document.body.dataset.page;
    if (!currentPage) {
      return;
    }

    const servicePages = new Set([
      "zero-offer",
      "interview-prep",
      "grade-salary",
      "autoapply"
    ]);

    const activePage = servicePages.has(currentPage) ? "services" : currentPage;

    document.querySelectorAll(".site-nav [data-page]").forEach((link) => {
      link.classList.toggle("is-active", link.getAttribute("data-page") === activePage);
    });
  };

  const setupSmartHeader = () => {
    const header = document.querySelector(".site-header");
    const nav = document.getElementById("site-nav");
    if (!header) {
      return;
    }

    let lastY = window.scrollY;
    const threshold = 10;
    const minY = 90;

    const onScroll = () => {
      const y = window.scrollY;
      const diff = y - lastY;

      document.body.classList.toggle("is-scrolled", y > 8);

      if (nav && nav.classList.contains("is-open")) {
        header.classList.remove("is-hidden");
        lastY = y;
        return;
      }

      if (Math.abs(diff) > threshold) {
        if (diff > 0 && y > minY) {
          header.classList.add("is-hidden");
        } else if (diff < 0) {
          header.classList.remove("is-hidden");
        }
        lastY = y;
      }

      if (y <= 8) {
        header.classList.remove("is-hidden");
      }
    };

    onScroll();
    window.addEventListener("scroll", onScroll, { passive: true });
  };

  const setupMobileMenu = () => {
    const toggle = document.querySelector(".menu-toggle");
    const nav = document.getElementById("site-nav");
    if (!toggle || !nav) {
      return;
    }

    const closeMenu = () => {
      nav.classList.remove("is-open");
      toggle.setAttribute("aria-expanded", "false");
    };

    toggle.addEventListener("click", () => {
      const isOpen = nav.classList.toggle("is-open");
      toggle.setAttribute("aria-expanded", String(isOpen));
      if (isOpen) {
        document.querySelector(".site-header")?.classList.remove("is-hidden");
      }
    });

    nav.querySelectorAll("a").forEach((link) => {
      link.addEventListener("click", closeMenu);
    });

    document.addEventListener("click", (event) => {
      const target = event.target;
      if (!(target instanceof HTMLElement)) {
        return;
      }
      if (!nav.contains(target) && !toggle.contains(target)) {
        closeMenu();
      }
    });

    window.addEventListener("resize", () => {
      if (window.innerWidth > 1180) {
        closeMenu();
      }
    });
  };

  const setupFaq = () => {
    const faqItems = Array.from(document.querySelectorAll(".faq-item"));
    if (!faqItems.length) {
      return;
    }

    const setState = (item, open) => {
      const button = item.querySelector(".faq-question");
      const answer = item.querySelector(".faq-answer");
      if (!button || !answer) {
        return;
      }
      button.setAttribute("aria-expanded", String(open));
      answer.style.maxHeight = open ? `${answer.scrollHeight}px` : "0px";
    };

    faqItems.forEach((item, index) => {
      setState(item, index === 0);
      const button = item.querySelector(".faq-question");
      if (!button) {
        return;
      }
      button.addEventListener("click", () => {
        const isExpanded = button.getAttribute("aria-expanded") === "true";
        faqItems.forEach((entry) => setState(entry, false));
        setState(item, !isExpanded);
      });
    });

    window.addEventListener("resize", () => {
      faqItems.forEach((item) => {
        const button = item.querySelector(".faq-question");
        const answer = item.querySelector(".faq-answer");
        if (!button || !answer) {
          return;
        }
        if (button.getAttribute("aria-expanded") === "true") {
          answer.style.maxHeight = `${answer.scrollHeight}px`;
        }
      });
    });
  };

  const setupLightbox = () => {
    const lightbox = document.getElementById("lightbox");
    const lightboxImage = document.getElementById("lightbox-image");
    const lightboxCaption = document.getElementById("lightbox-caption");
    const closeButton = lightbox ? lightbox.querySelector("[data-lightbox-close]") : null;
    if (!lightbox || !lightboxImage || !lightboxCaption || !closeButton) {
      return;
    }

    const open = (src, caption, alt) => {
      lightboxImage.setAttribute("src", src);
      lightboxImage.setAttribute("alt", alt || caption || "Скрин отзыва");
      lightboxCaption.textContent = caption || "";
      lightbox.classList.add("is-open");
      lightbox.setAttribute("aria-hidden", "false");
      document.body.classList.add("modal-open");
    };

    const close = () => {
      lightbox.classList.remove("is-open");
      lightbox.setAttribute("aria-hidden", "true");
      document.body.classList.remove("modal-open");
    };

    document.querySelectorAll("[data-review-trigger]").forEach((button) => {
      button.addEventListener("click", () => {
        const image = button.getAttribute("data-image");
        const caption = button.getAttribute("data-caption");
        const imageNode = button.querySelector("img");
        const alt = imageNode ? imageNode.getAttribute("alt") : "Скрин отзыва";
        if (image) {
          open(image, caption, alt);
        }
      });
    });

    closeButton.addEventListener("click", close);
    lightbox.addEventListener("click", (event) => {
      if (event.target === lightbox) {
        close();
      }
    });

    document.addEventListener("keydown", (event) => {
      if (event.key === "Escape" && lightbox.classList.contains("is-open")) {
        close();
      }
    });
  };

  const setupReviewsCarousel = () => {
    const carousels = document.querySelectorAll("[data-reviews-carousel]");
    if (!carousels.length) {
      return;
    }

    carousels.forEach((carousel) => {
      const viewport = carousel.querySelector(".reviews-viewport");
      const track = carousel.querySelector(".reviews-track");
      const prev = carousel.querySelector("[data-reviews-prev]");
      const next = carousel.querySelector("[data-reviews-next]");
      if (!viewport || !track || !prev || !next) {
        return;
      }
      const cards = Array.from(track.querySelectorAll(".review-card"));
      const hasMultipleCards = cards.length > 1;
      prev.hidden = !hasMultipleCards;
      next.hidden = !hasMultipleCards;
      if (!hasMultipleCards) {
        return;
      }

      let dotsWrap = null;
      const nextNode = carousel.nextElementSibling;
      if (nextNode && nextNode.classList.contains("reviews-dots")) {
        dotsWrap = nextNode;
      } else {
        dotsWrap = document.createElement("div");
        dotsWrap.className = "reviews-dots";
        carousel.insertAdjacentElement("afterend", dotsWrap);
      }
      dotsWrap.innerHTML = "";

      const getStep = () => {
        const card = track.querySelector(".review-card");
        if (!card) {
          return viewport.clientWidth;
        }
        const gap = Number.parseFloat(window.getComputedStyle(track).gap || "0") || 0;
        return card.getBoundingClientRect().width + gap;
      };

      const getMaxIndex = () => {
        const step = Math.max(getStep(), 1);
        const maxScroll = Math.max(track.scrollWidth - viewport.clientWidth, 0);
        return Math.max(0, Math.round(maxScroll / step));
      };

      let dots = [];
      let dotsCount = 0;
      const rebuildDots = () => {
        const count = getMaxIndex() + 1;
        if (count === dotsCount) {
          return;
        }

        dotsCount = count;
        dotsWrap.innerHTML = "";
        dots = [];
        dotsWrap.hidden = count <= 1;

        for (let index = 0; index < count; index += 1) {
          const dot = document.createElement("button");
          dot.type = "button";
          dot.className = "review-dot";
          dot.setAttribute("aria-label", `Перейти к слайду ${index + 1}`);
          dot.addEventListener("click", () => {
            const step = getStep();
            viewport.scrollTo({ left: step * index, behavior: "smooth" });
          });
          dotsWrap.appendChild(dot);
          dots.push(dot);
        }
      };

      const update = () => {
        rebuildDots();

        const maxScroll = Math.max(track.scrollWidth - viewport.clientWidth, 0);
        prev.disabled = viewport.scrollLeft <= 2;
        next.disabled = viewport.scrollLeft >= maxScroll - 2;

        const step = Math.max(getStep(), 1);
        const activeIndex = Math.max(
          0,
          Math.min(Math.round(viewport.scrollLeft / step), getMaxIndex())
        );
        dots.forEach((dot, index) => {
          const isActive = index === activeIndex;
          dot.classList.toggle("is-active", isActive);
          dot.setAttribute("aria-current", isActive ? "true" : "false");
        });
      };

      prev.addEventListener("click", () => {
        viewport.scrollBy({ left: -getStep(), behavior: "smooth" });
      });

      next.addEventListener("click", () => {
        viewport.scrollBy({ left: getStep(), behavior: "smooth" });
      });

      viewport.addEventListener("scroll", update, { passive: true });
      window.addEventListener("resize", update);
      update();
    });
  };

  const setupReviewExpander = () => {
    const buttons = document.querySelectorAll("[data-review-open]");
    if (!buttons.length) {
      return;
    }

    let modal = document.getElementById("review-modal");
    if (!modal) {
      modal = document.createElement("div");
      modal.id = "review-modal";
      modal.className = "review-modal";
      modal.setAttribute("aria-hidden", "true");
      modal.innerHTML = `
        <div class="review-modal-inner" role="dialog" aria-modal="true" aria-labelledby="review-modal-author">
          <button class="review-modal-close" type="button" data-review-modal-close aria-label="Закрыть">×</button>
          <div class="review-modal-head">
            <p id="review-modal-author" class="review-modal-author" data-review-modal-author></p>
            <p class="review-modal-date" data-review-modal-date></p>
          </div>
          <div class="review-modal-text" data-review-modal-text></div>
        </div>
      `;
      document.body.appendChild(modal);
    }

    const modalAuthor = modal.querySelector("[data-review-modal-author]");
    const modalDate = modal.querySelector("[data-review-modal-date]");
    const modalText = modal.querySelector("[data-review-modal-text]");
    const closeButton = modal.querySelector("[data-review-modal-close]");
    if (!modalAuthor || !modalDate || !modalText || !closeButton) {
      return;
    }

    const closeModal = () => {
      modal.classList.remove("is-open");
      modal.setAttribute("aria-hidden", "true");
      document.body.classList.remove("modal-open");
    };

    const openModal = (card) => {
      const authorNode = card.querySelector(".review-api-author");
      const dateNode = card.querySelector(".review-api-date");
      const fullTextNode = card.querySelector(".review-api-text-full");
      if (!fullTextNode) {
        return;
      }

      modalAuthor.innerHTML = authorNode ? authorNode.innerHTML : "Анонимный отзыв";
      modalDate.textContent = dateNode ? dateNode.textContent || "" : "";
      modalText.innerHTML = fullTextNode.innerHTML;

      modal.classList.add("is-open");
      modal.setAttribute("aria-hidden", "false");
      document.body.classList.add("modal-open");
    };

    if (modal.dataset.bound !== "true") {
      modal.dataset.bound = "true";
      closeButton.addEventListener("click", closeModal);
      modal.addEventListener("click", (event) => {
        if (event.target === modal) {
          closeModal();
        }
      });
      document.addEventListener("keydown", (event) => {
        if (event.key === "Escape" && modal.classList.contains("is-open")) {
          closeModal();
        }
      });
    }

    buttons.forEach((button) => {
      if (button.dataset.bound === "true") {
        return;
      }
      button.dataset.bound = "true";
      button.addEventListener("click", () => {
        const card = button.closest(".review-api-card");
        if (!card) {
          return;
        }
        openModal(card);
      });
    });
  };

  const setupReveal = () => {
    const nodes = document.querySelectorAll("[data-reveal]");
    if (!nodes.length) {
      return;
    }

    const observer = new IntersectionObserver(
      (entries, currentObserver) => {
        entries.forEach((entry) => {
          if (entry.isIntersecting) {
            entry.target.classList.add("is-visible");
            currentObserver.unobserve(entry.target);
          }
        });
      },
      { threshold: 0.08, rootMargin: "0px 0px -40px 0px" }
    );

    nodes.forEach((node) => observer.observe(node));
  };

  const setYear = () => {
    const yearNode = document.getElementById("year");
    if (yearNode) {
      yearNode.textContent = String(new Date().getFullYear());
    }
  };

  renderPricing();
  renderPaymentModes();
  renderAutoapplyPricing();
  renderServicePillars();
  renderServiceTariffs();
  renderTrackCards();
  renderTrackPage();
  renderSalaryChart();
  void renderReviewsFromApi();

  applyLinks();
  setupTariffCartLinks();
  setupAutoapplyCalculator();
  setupPageActiveNav();
  setupSmartHeader();
  setupMobileMenu();
  setupFaq();
  setupLightbox();
  setupReviewsCarousel();
  setupReveal();
  setYear();
})();
