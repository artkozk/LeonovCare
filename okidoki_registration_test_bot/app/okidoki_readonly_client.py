from __future__ import annotations

from io import BytesIO
import re
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlparse

import requests


@dataclass(slots=True)
class OkiDokiReadOnlyError(RuntimeError):
    message: str
    status_code: int | None = None
    body: str = ""

    def __str__(self) -> str:
        code = f" (HTTP {self.status_code})" if self.status_code is not None else ""
        tail = f": {self.body}" if self.body else ""
        return f"{self.message}{code}{tail}"


class OkiDokiReadOnlyClient:
    """
    Read-only OkiDoki client for contract parsing.
    No create/update/delete methods by design.
    """

    def __init__(self, api_token: str, api_base: str = "https://api.doki.online", timeout_sec: int = 30) -> None:
        self.api_token = (api_token or "").strip()
        self.api_base = (api_base or "https://api.doki.online").rstrip("/")
        self.timeout_sec = max(int(timeout_sec or 30), 5)
        if not self.api_token:
            raise OkiDokiReadOnlyError("OKIDOKI_API_TOKEN is empty")

    def _headers(self) -> dict[str, str]:
        return {
            "Accept": "application/json",
            "Authorization": f"Bearer {self.api_token}",
            "X-API-KEY": self.api_token,
        }

    def list_templates(self) -> list[dict[str, str]]:
        url = f"{self.api_base}/external/templates"
        try:
            resp = requests.get(url, params={"api_key": self.api_token}, headers=self._headers(), timeout=self.timeout_sec)
        except requests.RequestException as exc:
            raise OkiDokiReadOnlyError("Failed to fetch templates") from exc

        if resp.status_code >= 400:
            raise OkiDokiReadOnlyError("Templates endpoint failed", resp.status_code, (resp.text or "")[:400])
        data = resp.json() if (resp.text or "").strip() else {}
        out: list[dict[str, str]] = []
        for item in data.get("templates", []) if isinstance(data, dict) else []:
            if not isinstance(item, dict):
                continue
            tid = str(item.get("template_id") or item.get("id") or "").strip()
            tname = str(item.get("template_name") or item.get("name") or "").strip()
            if not tid:
                continue
            out.append({"template_id": tid, "template_name": tname})
        return out

    def get_template_entities(self, template_id: str) -> list[dict[str, Any]]:
        tid = str(template_id or "").strip()
        if not tid:
            return []
        url = f"{self.api_base}/external/get-template-entities"
        try:
            resp = requests.get(
                url,
                params={"api_key": self.api_token, "template_id": tid},
                headers=self._headers(),
                timeout=self.timeout_sec,
            )
        except requests.RequestException as exc:
            raise OkiDokiReadOnlyError("Failed to fetch template entities") from exc
        if resp.status_code >= 400:
            raise OkiDokiReadOnlyError("Template entities endpoint failed", resp.status_code, (resp.text or "")[:400])
        data = resp.json() if (resp.text or "").strip() else {}
        entities = data.get("entities", []) if isinstance(data, dict) else []
        return [e for e in entities if isinstance(e, dict)]

    @staticmethod
    def extract_contract_id(contract_url_or_id: str | None) -> str:
        raw = str(contract_url_or_id or "").strip()
        if not raw:
            return ""
        if re.fullmatch(r"[A-Za-z0-9_-]{8,64}", raw):
            return raw
        try:
            path = urlparse(raw).path or ""
        except Exception:
            path = raw
        m = re.search(r"/contracts?/([A-Za-z0-9_-]{8,64})", path)
        if not m:
            m = re.search(r"/contract/([A-Za-z0-9_-]{8,64})", path)
        return str(m.group(1)).strip() if m else ""

    def _download_contract_pdf(self, contract_id: str) -> bytes | None:
        cid = str(contract_id or "").strip()
        if not cid:
            return None
        url = f"{self.api_base}/external/contracts/{cid}/download"
        headers = {
            "Accept": "application/pdf",
            "Authorization": f"Bearer {self.api_token}",
            "X-API-KEY": self.api_token,
        }
        try:
            resp = requests.get(url, headers=headers, timeout=self.timeout_sec)
        except requests.RequestException:
            return None
        if resp.status_code >= 400:
            return None
        data = resp.content or b""
        if len(data) < 32:
            return None
        if not data.startswith(b"%PDF"):
            return None
        return data

    @staticmethod
    def _clean_pdf_text(text: str) -> str:
        raw = str(text or "")
        if not raw:
            return ""
        return " ".join(raw.replace("\xa0", " ").split())

    def _extract_profile_hints_from_pdf(self, pdf_bytes: bytes) -> dict[str, Any]:
        if not pdf_bytes:
            return {}
        try:
            from pypdf import PdfReader  # type: ignore
        except Exception:
            return {}
        try:
            reader = PdfReader(BytesIO(pdf_bytes))
            text = "\n".join((page.extract_text() or "") for page in reader.pages)
        except Exception:
            return {}
        norm = self._clean_pdf_text(text)
        if not norm:
            return {}

        out: dict[str, Any] = {}

        # Username: usually two "Telegram:" blocks (executor and client), use the last one.
        usernames = re.findall(r"Telegram\s*:\s*@([A-Za-z0-9_]{3,64})", norm, flags=re.IGNORECASE)
        if usernames:
            out["username"] = str(usernames[-1]).strip().lower()

        # Full name: capture customer name before "именуемый(-ая) ... Заказчик".
        m_fio = re.search(
            r"и\s+([А-ЯЁA-Z][А-ЯЁA-Zа-яёa-z-]+(?:\s+[А-ЯЁA-Z][А-ЯЁA-Zа-яёa-z-]+){1,2})\s*,?\s*именуем[а-я\(\)\- ]*Заказчик",
            norm,
            flags=re.IGNORECASE,
        )
        if m_fio:
            out["fio"] = str(m_fio.group(1)).strip()
        else:
            # Fallback for section with "Telegram: @user Фамилия Имя Отчество Паспорт ..."
            m_fio2 = re.search(
                r"Telegram\s*:\s*@[\w_]{3,64}\s+([А-ЯЁA-Z][А-ЯЁA-Zа-яёa-z-]+(?:\s+[А-ЯЁA-Z][А-ЯЁA-Zа-яёa-z-]+){1,2})\s+Паспорт",
                norm,
                flags=re.IGNORECASE,
            )
            if m_fio2:
                out["fio"] = str(m_fio2.group(1)).strip()

        # Direction: "в области C#" style.
        m_direction = re.search(
            r"в\s+сфере\s+информационных\s+технологий\s+в\s+области\s+(.{1,80}?)(?:\s*\(далее|,|\.|\s{2,})",
            norm,
            flags=re.IGNORECASE,
        )
        if m_direction:
            out["direction"] = str(m_direction.group(1)).strip()

        # Prepay total.
        m_prepay = re.search(
            r"Предоплат[аы]\s*:\s*([\d\s]{1,12})\s*руб",
            norm,
            flags=re.IGNORECASE,
        )
        if m_prepay:
            digits = "".join(ch for ch in m_prepay.group(1) if ch.isdigit())
            if digits:
                out["paid_amount"] = int(digits)

        # Study start date from document date at the top.
        m_doc_date = re.search(
            r"(\d{1,2}\s+[а-яё]+\s+\d{4}\s*г\.)",
            norm,
            flags=re.IGNORECASE,
        )
        if m_doc_date:
            out["study_start_date"] = str(m_doc_date.group(1)).strip()

        # Post-pay monthly percent and months.
        m_post_monthly = re.search(
            r"Пост-?оплат[аы][^%]{0,600}(\d{1,3}(?:[.,]\d+)?)\s*%",
            norm,
            flags=re.IGNORECASE,
        )
        if m_post_monthly:
            try:
                out["post_monthly_percent"] = float(str(m_post_monthly.group(1)).replace(",", "."))
            except Exception:
                pass

        # Most templates in this product use 2 months post-pay.
        # Try explicit months first.
        m_months = re.search(
            r"за\s+(\d{1,2})\s*месяц",
            norm,
            flags=re.IGNORECASE,
        )
        if not m_months:
            m_months = re.search(
                r"в\s+течени[ея]\s+(\d{1,2})\s*месяц",
                norm,
                flags=re.IGNORECASE,
            )
        if m_months:
            try:
                out["postpay_months"] = max(1, int(m_months.group(1)))
            except Exception:
                pass
        else:
            # If payment schedule lists first/second month, infer 2 months.
            has_first = bool(re.search(r"перв(ый|ого)\s+месяц", norm, flags=re.IGNORECASE))
            has_second = bool(re.search(r"втор(ой|ого)\s+месяц", norm, flags=re.IGNORECASE))
            if has_first and has_second:
                out["postpay_months"] = 2

        if "postpay_months" in out:
            months = int(out["postpay_months"])
            if months > 0:
                out["post_total_percent"] = 100.0
                out["will_pay_amount"] = f"100% дохода за {months} мес."

        return out

    @staticmethod
    def _merge_hints_into_payload(payload: dict[str, Any], hints: dict[str, Any]) -> dict[str, Any]:
        if not isinstance(payload, dict):
            return payload
        if not isinstance(hints, dict) or not hints:
            return payload

        root = payload.get("contract") if isinstance(payload.get("contract"), dict) else payload
        for key, value in hints.items():
            if root.get(key) in (None, "", 0):
                root[key] = value
            if payload.get(key) in (None, "", 0):
                payload[key] = value

        entities = root.get("entities")
        if not isinstance(entities, list):
            entities = []
            root["entities"] = entities

        def _upsert_entity(keyword: str, value: Any) -> None:
            for item in entities:
                if not isinstance(item, dict):
                    continue
                k = str(item.get("keyword") or item.get("name") or "").strip().lower()
                if k == keyword.lower():
                    if item.get("value") in (None, "", 0):
                        item["value"] = value
                    return
            entities.append({"keyword": keyword, "value": value})

        if hints.get("username"):
            _upsert_entity("Телеграм клиента", f"@{str(hints['username']).lstrip('@')}")
        if hints.get("fio"):
            _upsert_entity("ФИО", str(hints["fio"]))
        if hints.get("direction"):
            _upsert_entity("Область", hints["direction"])
        if hints.get("paid_amount") is not None:
            _upsert_entity("Предоплата", str(hints["paid_amount"]))
        if hints.get("postpay_months"):
            _upsert_entity("Количество месяцев постоплаты", str(hints["postpay_months"]))
        return payload

    def fetch_contract_payload(self, contract_url_or_id: str) -> dict[str, Any]:
        contract_id = self.extract_contract_id(contract_url_or_id)
        if not contract_id:
            raise OkiDokiReadOnlyError("Cannot extract contract_id from URL")

        endpoints_with_params: list[tuple[str, dict[str, Any]]] = [
            (f"{self.api_base}/external/contract", {"api_key": self.api_token, "contract_id": contract_id}),
            (f"{self.api_base}/contracts/{contract_id}", {}),
            (f"{self.api_base}/v1/contracts/{contract_id}", {}),
            (f"{self.api_base}/public/contracts/{contract_id}", {}),
            (f"{self.api_base}/external/contracts/{contract_id}", {"api_key": self.api_token}),
        ]

        last_error: OkiDokiReadOnlyError | None = None
        for url, params in endpoints_with_params:
            try:
                resp = requests.get(
                    url,
                    params=params or None,
                    headers=self._headers(),
                    timeout=self.timeout_sec,
                )
            except requests.RequestException as exc:
                last_error = OkiDokiReadOnlyError("Request to OkiDoki failed", body=str(exc)[:300])
                continue

            body = (resp.text or "").strip()
            if resp.status_code == 404:
                continue
            if resp.status_code >= 400:
                last_error = OkiDokiReadOnlyError("OkiDoki returned error", resp.status_code, body[:400])
                continue
            if not body:
                return {}
            try:
                data = resp.json()
            except Exception as exc:
                last_error = OkiDokiReadOnlyError("OkiDoki returned non-JSON", resp.status_code, body[:400])
                continue
            if isinstance(data, dict):
                payload = data
                pdf_bytes = self._download_contract_pdf(contract_id)
                if pdf_bytes:
                    hints = self._extract_profile_hints_from_pdf(pdf_bytes)
                    payload = self._merge_hints_into_payload(payload, hints)
                return payload
            return {"data": data}

        if last_error:
            raise last_error
        raise OkiDokiReadOnlyError("Contract not found", 404)
