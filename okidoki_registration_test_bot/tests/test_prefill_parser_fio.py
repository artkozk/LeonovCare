from __future__ import annotations

from app.prefill_parser import extract_prefill, validate_contract


def test_extract_fio_from_root_field() -> None:
    payload = {
        "contract": {
            "fio": "Иванов Иван Иванович",
            "entities": [{"keyword": "Телеграм клиента", "value": "@ivanov"}],
        }
    }
    prefill = extract_prefill(payload, "https://desktop.doki.online/contract/abc123")
    assert prefill.get("fio") == "Иванов Иван Иванович"


def test_extract_fio_from_nested_payload() -> None:
    payload = {
        "result": {
            "data": {
                "contract": {
                    "customer_fio": "Петров Пётр Петрович",
                    "entities": [{"keyword": "Телеграм клиента", "value": "@petrov"}],
                }
            }
        }
    }
    prefill = extract_prefill(payload, "https://desktop.doki.online/contract/abc123")
    assert prefill.get("fio") == "Петров Пётр Петрович"


def test_extract_fio_from_contract_text_pattern() -> None:
    payload = {
        "contract": {
            "text": (
                "Договор заключен между Исполнителем и "
                "Сидоров Сидор Сидорович, именуемый в дальнейшем Заказчик."
            ),
            "entities": [{"keyword": "Телеграм клиента", "value": "@sidorov"}],
        }
    }
    prefill = extract_prefill(payload, "https://desktop.doki.online/contract/abc123")
    assert prefill.get("fio") == "Сидоров Сидор Сидорович"


def test_does_not_take_template_name_as_fio() -> None:
    payload = {
        "contract": {
            "name": "Шаблон обучение 1 от 13.04.2026",
            "entities": [{"keyword": "Телеграм клиента", "value": "@user"}],
        }
    }
    prefill = extract_prefill(payload, "https://desktop.doki.online/contract/abc123")
    assert prefill.get("fio") is None


def test_extract_prepay_and_tariff_from_entities_with_colon() -> None:
    payload = {
        "contract": {
            "entities": [
                {"keyword": "Телеграм клиента:", "value": "@alex_lovser"},
                {"keyword": "Область:", "value": "Python"},
                {"keyword": "Предоплата:", "value": "30000"},
                {"keyword": "Количество месяцев постоплаты:", "value": "3"},
            ]
        }
    }
    prefill = extract_prefill(payload, "https://desktop.doki.online/contract/abc123")
    assert prefill.get("paid_amount") == 30000
    assert prefill.get("postpay_months") == 3
    assert prefill.get("tariff") == "pre_post"


def test_validate_contract_accepts_punctuated_entity_names() -> None:
    payload = {
        "result": {
            "data": {
                "contract": {
                    "entities": [
                        {"keyword": "Телеграм клиента:", "value": "@alex_lovser"},
                        {"keyword": "Область.", "value": "Python"},
                    ]
                }
            }
        }
    }
    result = validate_contract(payload, known_templates={})
    assert result.is_valid is True
    assert result.reason in {"ok", "ok_by_payload"}
