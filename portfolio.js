(() => {
  const body = document.body;
  if (!body || body.dataset.page !== "portfolio") {
    return;
  }

  const yearNode = document.getElementById("year");
  if (yearNode) {
    yearNode.textContent = String(new Date().getFullYear());
  }

  const mediaReduced = window.matchMedia("(prefers-reduced-motion: reduce)");
  const allowMotion = !mediaReduced.matches;

  const header = document.querySelector(".site-header");
  const menuToggle = document.querySelector(".p-menu-toggle");
  const nav = document.getElementById("site-nav");
  const navLinks = nav ? Array.from(nav.querySelectorAll('a[href^="#"]')) : [];

  const closeMenu = () => {
    if (!menuToggle || !nav) {
      return;
    }
    menuToggle.setAttribute("aria-expanded", "false");
    nav.classList.remove("is-open");
  };

  if (menuToggle && nav) {
    menuToggle.addEventListener("click", () => {
      const isOpen = menuToggle.getAttribute("aria-expanded") === "true";
      menuToggle.setAttribute("aria-expanded", String(!isOpen));
      nav.classList.toggle("is-open", !isOpen);
    });

    navLinks.forEach((link) => {
      link.addEventListener("click", closeMenu);
    });

    window.addEventListener("resize", () => {
      if (window.innerWidth > 900) {
        closeMenu();
      }
    });
  }

  const setHeaderState = () => {
    body.classList.toggle("is-scrolled", window.scrollY > 8);
  };

  setHeaderState();
  window.addEventListener("scroll", setHeaderState, { passive: true });

  if (!allowMotion) {
    document.querySelectorAll("[data-reveal]").forEach((node) => {
      node.classList.add("is-visible");
    });
  } else {
    const observer = new IntersectionObserver((entries) => {
      entries.forEach((entry) => {
        if (!entry.isIntersecting) {
          return;
        }
        entry.target.classList.add("is-visible");
        observer.unobserve(entry.target);
      });
    }, {
      rootMargin: "0px 0px -8% 0px",
      threshold: 0.12
    });

    document.querySelectorAll("[data-reveal]").forEach((node) => {
      observer.observe(node);
    });
  }

  const sections = navLinks
    .map((link) => {
      const targetId = link.getAttribute("href")?.slice(1);
      if (!targetId) {
        return null;
      }
      const target = document.getElementById(targetId);
      if (!target) {
        return null;
      }
      return { link, target };
    })
    .filter(Boolean);

  if (sections.length > 0) {
    const markActive = () => {
      const focusLine = window.scrollY + window.innerHeight * 0.26;
      let active = sections[0];

      sections.forEach((section) => {
        if (section.target.offsetTop <= focusLine) {
          active = section;
        }
      });

      sections.forEach((section) => {
        section.link.classList.toggle("is-active", section === active);
      });
    };

    markActive();
    window.addEventListener("scroll", markActive, { passive: true });
  }

  document.addEventListener("click", (event) => {
    const target = event.target;
    if (!(target instanceof HTMLElement)) {
      return;
    }

    const link = target.closest('a[href^="#"]');
    if (!link) {
      return;
    }

    const href = link.getAttribute("href");
    if (!href || href === "#") {
      return;
    }

    const id = href.slice(1);
    const node = document.getElementById(id);
    if (!node) {
      return;
    }

    event.preventDefault();

    const headerHeight = header ? header.getBoundingClientRect().height : 0;
    const top = node.getBoundingClientRect().top + window.scrollY - headerHeight - 12;

    window.scrollTo({
      top,
      behavior: allowMotion ? "smooth" : "auto"
    });
  });
})();
