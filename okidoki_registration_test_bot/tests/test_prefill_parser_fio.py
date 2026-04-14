from __future__ import annotations

from app.prefill_parser import extract_prefill


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
