from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any


@dataclass(slots=True)
class ContractValidationResult:
    is_valid: bool
    reason: str
    template_id: str
    template_name: str
    entity_names: list[str]


REQUIRED_SYSTEM_ENTITY_NAMES = {
    "телеграм клиента",
}

KNOWN_ENTITY_NAMES = {
    "телеграм клиента",
    "область",
    "предоплата",
    "дата предоплаты",
    "количество месяцев постоплаты",
}


def _normalize_entity_name(raw: str | None) -> str:
    txt = str(raw or "").strip().lower().replace("ё", "е")
    if not txt:
        return ""
    txt = txt.replace("_", " ")
    txt = re.sub(r"[^\w\s]", " ", txt, flags=re.UNICODE)
    return " ".join(txt.split())


def _layers(payload: dict[str, Any] | None) -> list[dict[str, Any]]:
    if not isinstance(payload, dict):
        return []
    out: list[dict[str, Any]] = []
    seen: set[int] = set()
    queue: list[tuple[dict[str, Any], int]] = [(payload, 0)]
    max_depth = 4
    max_nodes = 200
    while queue and len(out) < max_nodes:
        layer, depth = queue.pop(0)
        marker = id(layer)
        if marker in seen:
            continue
        seen.add(marker)
        out.append(layer)
        if depth >= max_depth:
            continue
        for value in layer.values():
            if isinstance(value, dict):
                queue.append((value, depth + 1))
            elif isinstance(value, list):
                for item in value:
                    if isinstance(item, dict):
                        queue.append((item, depth + 1))
    return out


def _entity_items(payload: dict[str, Any] | None) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for layer in _layers(payload):
        entities = layer.get("entities")
        if not isinstance(entities, list):
            continue
        for item in entities:
            if isinstance(item, dict):
                out.append(item)
    return out


def collect_entity_names(payload: dict[str, Any] | None) -> list[str]:
    names: list[str] = []
    seen: set[str] = set()
    for item in _entity_items(payload):
        name = str(item.get("keyword") or item.get("name") or "").strip()
        norm = _normalize_entity_name(name)
        if not norm or norm in seen:
            continue
        seen.add(norm)
        names.append(name)
    return names


def _pick_text(payload: dict[str, Any] | None, *keys: str) -> str:
    for layer in _layers(payload):
        for key in keys:
            raw = layer.get(key)
            if isinstance(raw, str) and raw.strip():
                return raw.strip()
            if isinstance(raw, (int, float)) and not isinstance(raw, bool):
                return str(raw)
    return ""


def _pick_entity(payload: dict[str, Any] | None, *names: str) -> str:
    wanted = {_normalize_entity_name(str(name or "")) for name in names if _normalize_entity_name(str(name or ""))}
    if not wanted:
        return ""
    for item in _entity_items(payload):
        key_name = _normalize_entity_name(
            str(item.get("keyword") or item.get("name") or item.get("id") or "")
        )
        if key_name not in wanted:
            continue
        raw = item.get("value")
        if isinstance(raw, str) and raw.strip():
            return raw.strip()
        if isinstance(raw, (int, float)) and not isinstance(raw, bool):
            return str(raw)
    return ""


def _looks_like_fio(raw: str | None) -> bool:
    txt = " ".join(str(raw or "").replace("\xa0", " ").split())
    if not txt:
        return False
    if any(ch.isdigit() for ch in txt):
        return False
    if len(txt) < 6 or len(txt) > 120:
        return False
    words = [w.strip(".,;:!?()[]{}\"'") for w in txt.split()]
    words = [w for w in words if w]
    if len(words) < 2 or len(words) > 4:
        return False
    good = 0
    for word in words:
        if re.fullmatch(r"[А-ЯЁA-Z][А-ЯЁA-Zа-яёa-z-]{1,}", word):
            good += 1
    return good >= 2


def _extract_fio_from_text(raw: str | None) -> str:
    txt = " ".join(str(raw or "").replace("\xa0", " ").split())
    if not txt:
        return ""
    patterns = [
        r"и\s+([А-ЯЁA-Z][А-ЯЁA-Zа-яёa-z-]+(?:\s+[А-ЯЁA-Z][А-ЯЁA-Zа-яёa-z-]+){1,3})\s*,?\s*именуем[а-яё\(\)\- ]*Заказчик",
        r"(?:гражданин(?:ка)?|заказчик|клиент)\s*[:,-]?\s*([А-ЯЁA-Z][А-ЯЁA-Zа-яёa-z-]+(?:\s+[А-ЯЁA-Z][А-ЯЁA-Zа-яёa-z-]+){1,3})",
        r"фио\s*[:,-]?\s*([А-ЯЁA-Z][А-ЯЁA-Zа-яёa-z-]+(?:\s+[А-ЯЁA-Z][А-ЯЁA-Zа-яёa-z-]+){1,3})",
    ]
    for pattern in patterns:
        m = re.search(pattern, txt, flags=re.IGNORECASE)
        if not m:
            continue
        candidate = " ".join(str(m.group(1) or "").split())
        if _looks_like_fio(candidate):
            return candidate
    return ""


def _pick_fio(payload: dict[str, Any] | None) -> str:
    direct = _pick_text(
        payload,
        "fio",
        "full_name",
        "fullname",
        "client_name",
        "customer_name",
        "person_name",
        "customer_fio",
        "client_fio",
        "fio_client",
        "client_full_name",
        "customer_full_name",
        "user_fio",
        "student_fio",
    )
    if _looks_like_fio(direct):
        return " ".join(str(direct).split())

    by_entity = _pick_entity(payload, "ФИО", "Фио", "ФИО клиента", "Клиент", "Заказчик", "Гражданин")
    if _looks_like_fio(by_entity):
        return " ".join(str(by_entity).split())

    first_name = _pick_text(payload, "first_name", "firstName", "client_first_name")
    last_name = _pick_text(payload, "last_name", "lastName", "client_last_name")
    middle_name = _pick_text(payload, "middle_name", "middleName", "client_middle_name")
    fio_parts = [part for part in (last_name, first_name, middle_name) if str(part or "").strip()]
    if fio_parts:
        by_parts = " ".join(str(part).strip() for part in fio_parts if str(part).strip())
        if _looks_like_fio(by_parts):
            return by_parts

    for layer in _layers(payload):
        for key, value in layer.items():
            if not isinstance(value, str):
                continue
            key_norm = str(key or "").strip().lower()
            if not key_norm:
                continue
            if any(token in key_norm for token in ("fio", "фио", "full_name", "fullname", "client", "customer", "person", "клиент", "заказчик")):
                if _looks_like_fio(value):
                    return " ".join(str(value).split())

    for layer in _layers(payload):
        for key, value in layer.items():
            if not isinstance(value, str):
                continue
            key_norm = str(key or "").strip().lower()
            if key_norm in {"body", "text", "content", "contract_text", "document_text", "html"}:
                from_text = _extract_fio_from_text(value)
                if from_text:
                    return from_text

    return ""


def _as_amount(raw: str) -> int | None:
    txt = str(raw or "").strip().lower()
    if not txt:
        return None
    if "%" in txt:
        return None
    # Accept plain numbers or monetary forms like "30 000", "30000 руб", "30000 ₽".
    if not re.fullmatch(r"[\d\s]+(?:[.,]\d+)?(?:\s*(?:₽|руб(?:\.|лей|ля|ль)?)\s*)?", txt):
        return None
    digits = "".join(ch for ch in txt if ch.isdigit())
    if not digits:
        return None
    try:
        amount = int(digits)
    except Exception:
        return None
    return amount if amount >= 0 else None


def _as_months(raw: str) -> int | None:
    digits = "".join(ch for ch in str(raw or "") if ch.isdigit())
    if not digits:
        return None
    try:
        months = int(digits)
    except Exception:
        return None
    return months if months > 0 else None


def _normalize_username(raw: str) -> str:
    txt = str(raw or "").strip()
    if not txt:
        return ""
    txt = txt.replace("https://t.me/", "").replace("http://t.me/", "").replace("t.me/", "")
    txt = txt.strip().lstrip("@").strip()
    txt = re.sub(r"[^A-Za-z0-9_]", "", txt)
    return txt.lower()


def _normalize_tariff(raw: str) -> str:
    txt = str(raw or "").strip().lower()
    if not txt:
        return ""
    if txt in {"pre", "post", "pre_post"}:
        return txt
    has_pre = ("пред" in txt) or ("pre" in txt)
    has_post = ("пост" in txt) or ("post" in txt)
    if has_pre and has_post:
        return "pre_post"
    if has_post:
        return "post"
    if has_pre:
        return "pre"
    return ""


def _normalize_date(raw: str) -> str:
    txt = str(raw or "").strip()
    if not txt:
        return ""
    for fmt in ("%d.%m.%Y", "%Y-%m-%d", "%d-%m-%Y", "%Y/%m/%d"):
        try:
            parsed = datetime.strptime(txt, fmt)
            return parsed.strftime("%d.%m.%Y")
        except Exception:
            continue
    m = re.search(r"(\d{1,2})\s+([а-яё]+)\s+(\d{4})", txt.lower())
    if m:
        month_map = {
            "января": 1,
            "февраля": 2,
            "марта": 3,
            "апреля": 4,
            "мая": 5,
            "июня": 6,
            "июля": 7,
            "августа": 8,
            "сентября": 9,
            "октября": 10,
            "ноября": 11,
            "декабря": 12,
        }
        day = int(m.group(1))
        month = month_map.get(m.group(2))
        year = int(m.group(3))
        if month:
            try:
                parsed = datetime(year, month, day)
                return parsed.strftime("%d.%m.%Y")
            except Exception:
                pass
    return txt


def validate_contract(
    payload: dict[str, Any] | None,
    known_templates: dict[str, dict[str, Any]] | None,
) -> ContractValidationResult:
    if not isinstance(payload, dict) or not payload:
        return ContractValidationResult(False, "empty_payload", "", "", [])

    template_id = _pick_text(payload, "template_id", "templateId", "id")
    template_name = _pick_text(payload, "template_name", "templateName", "name")
    entity_names = collect_entity_names(payload)
    entity_norm = {_normalize_entity_name(name) for name in entity_names}
    required_entity_names = {_normalize_entity_name(name) for name in REQUIRED_SYSTEM_ENTITY_NAMES}
    known_entity_names = {_normalize_entity_name(name) for name in KNOWN_ENTITY_NAMES}

    template_ok = False
    if template_id and isinstance(known_templates, dict):
        template_ok = template_id in known_templates
        if not template_name and template_ok:
            template_name = str(known_templates[template_id].get("template_name") or "").strip()

    if template_name and not template_ok:
        low = template_name.lower()
        if low.startswith("шаблон обучение") or low == "договор возмездного оказания услуг":
            template_ok = True

    has_required_system_fields = required_entity_names.issubset(entity_norm)
    has_known_system_fields = bool(entity_norm.intersection(known_entity_names))
    known_payload_hints = (
        _pick_text(
            payload,
            "direction",
            "specialization",
            "area",
            "prepay",
            "prepayment",
            "prepay_amount",
            "paid_amount",
            "tariff",
            "tariff_type",
            "post_total_percent",
            "postpay_total_percent",
            "total_percent",
            "post_monthly_percent",
            "postpay_monthly_percent",
            "monthly_percent",
            "postpay_months",
        )
        or _pick_entity(
            payload,
            "Область",
            "Направление",
            "Специализация",
            "Предоплата",
            "Сумма предоплаты",
            "Количество месяцев постоплаты",
            "Постоплата месяцев",
        )
    )
    has_known_payload_fields = bool(str(known_payload_hints or "").strip())
    payload_username = _normalize_username(
        _pick_text(payload, "username", "telegram", "tg_username", "telegram_username")
    )

    if has_required_system_fields and (has_known_system_fields or has_known_payload_fields):
        return ContractValidationResult(True, "ok", template_id, template_name, entity_names)
    if payload_username and (has_known_system_fields or has_known_payload_fields):
        return ContractValidationResult(True, "ok_by_payload", template_id, template_name, entity_names)
    if not has_required_system_fields and not payload_username:
        return ContractValidationResult(False, "missing_required_system_fields", template_id, template_name, entity_names)
    return ContractValidationResult(False, "unknown_template", template_id, template_name, entity_names)


def extract_prefill(payload: dict[str, Any] | None, contract_url: str) -> dict[str, Any]:
    prefill: dict[str, Any] = {
        "contract_url": str(contract_url or "").strip(),
    }
    if not isinstance(payload, dict):
        return prefill

    username_raw = (
        _pick_entity(payload, "Телеграм клиента", "Telegram клиента", "Телеграм")
        or _pick_text(payload, "username", "telegram", "tg_username", "telegram_username")
    )
    username = _normalize_username(username_raw)
    if username:
        prefill["username"] = username

    fio = _pick_fio(payload)
    if fio:
        prefill["fio"] = fio

    direction = _pick_entity(payload, "Область", "Направление", "Специализация") or _pick_text(
        payload, "direction", "specialization", "area"
    )
    if direction:
        prefill["direction"] = direction

    study_date = _pick_entity(payload, "Дата предоплаты", "Текущая дата") or _pick_text(
        payload, "study_start_date", "start_date", "join_date", "current_date"
    )
    study_date_norm = _normalize_date(study_date)
    if study_date_norm:
        prefill["study_start_date"] = study_date_norm

    paid_amount_raw = (
        _pick_entity(payload, "Предоплата", "Сумма предоплаты")
        or _pick_text(payload, "prepay", "prepayment", "prepay_amount")
    )
    paid_amount = _as_amount(paid_amount_raw)
    if paid_amount is not None:
        prefill["paid_amount"] = paid_amount

    post_total_raw = _pick_text(payload, "post_total_percent", "postpay_total_percent", "total_percent")
    post_monthly_raw = _pick_text(payload, "post_monthly_percent", "postpay_monthly_percent", "monthly_percent")
    try:
        post_total_percent = float(str(post_total_raw).replace(",", ".")) if str(post_total_raw).strip() else 0.0
    except Exception:
        post_total_percent = 0.0
    try:
        post_monthly_percent = float(str(post_monthly_raw).replace(",", ".")) if str(post_monthly_raw).strip() else 0.0
    except Exception:
        post_monthly_percent = 0.0
    if post_total_percent > 0:
        prefill["post_total_percent"] = post_total_percent
    if post_monthly_percent > 0:
        prefill["post_monthly_percent"] = post_monthly_percent

    post_months = _as_months(
        _pick_entity(payload, "Количество месяцев постоплаты", "Месяцев постоплаты", "Постоплата месяцев")
    )
    if post_months:
        prefill["postpay_months"] = post_months

    # Explicit tariff in payload has highest priority.
    tariff = _normalize_tariff(_pick_text(payload, "tariff", "tariff_type", "plan", "plan_key"))

    # Numeric fields are the next best signal (and override template-name heuristics).
    has_paid = paid_amount is not None and paid_amount > 0
    has_post = (post_months is not None and post_months > 0) or post_total_percent > 0 or post_monthly_percent > 0
    if not tariff:
        if has_paid and has_post:
            tariff = "pre_post"
        elif has_post and not has_paid:
            tariff = "post"
        elif has_paid and not has_post:
            tariff = "pre"
    if tariff:
        prefill["tariff"] = tariff

    will_pay_raw = _pick_text(payload, "will_pay_amount", "postpay_amount", "to_pay", "postpay_total", "postpay_text")
    will_pay_amount = _as_amount(will_pay_raw)
    if will_pay_amount is not None and will_pay_amount > 0:
        prefill["will_pay_amount"] = will_pay_amount
    elif str(will_pay_raw or "").strip():
        prefill["will_pay_amount"] = str(will_pay_raw).strip()
    elif post_months:
        prefill["will_pay_amount"] = f"100% дохода за {post_months} мес."
    elif post_total_percent > 0 and post_monthly_percent > 0:
        months = max(1, int(round(post_total_percent / post_monthly_percent)))
        prefill["will_pay_amount"] = f"{post_total_percent:.0f}% дохода за {months} мес."
    elif post_total_percent > 0:
        prefill["will_pay_amount"] = f"{post_total_percent:.0f}% дохода (постоплата)"

    return prefill


def get_missing_fields(prefill: dict[str, Any]) -> list[str]:
    required = ("fio", "direction", "tariff", "study_start_date", "paid_amount", "will_pay_amount")
    missing: list[str] = []
    for key in required:
        value = prefill.get(key)
        if value is None:
            missing.append(key)
            continue
        if isinstance(value, str) and not value.strip():
            missing.append(key)
    return missing
