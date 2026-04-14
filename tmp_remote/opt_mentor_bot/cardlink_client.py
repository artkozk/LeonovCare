from __future__ import annotations

import uuid
from dataclasses import dataclass
from decimal import Decimal, ROUND_HALF_UP
from typing import Any

import requests


def _amount_to_decimal_str(amount_rub: int) -> str:
    value = Decimal(max(int(amount_rub or 0), 0)).quantize(Decimal("1"), rounding=ROUND_HALF_UP)
    return f"{value:.2f}"


def normalize_status(raw: str | None) -> str:
    val = str(raw or "").strip().upper()
    if val in {"SUCCESS", "PAID", "SUCCEEDED", "COMPLETED"}:
        return "SUCCEEDED"
    if val in {"FAIL", "FAILED", "CANCELED", "CANCELLED", "EXPIRED", "REJECTED"}:
        return "CANCELED"
    if val in {"WAITING_FOR_CAPTURE"}:
        return "WAITING_FOR_CAPTURE"
    return "PENDING"


@dataclass(slots=True)
class CardlinkClientError(RuntimeError):
    message: str
    status_code: int | None = None
    body: str = ""

    def __str__(self) -> str:
        code = f" (HTTP {self.status_code})" if self.status_code is not None else ""
        tail = f": {self.body}" if self.body else ""
        return f"{self.message}{code}{tail}"


class CardlinkClient:
    """Cardlink API client.

    The class intentionally keeps a constructor compatible with the previous checkout client
    (`shop_id`, `secret_key`, `return_url`) so handlers can be migrated safely.
    """

    def __init__(
        self,
        shop_id: str = "",
        secret_key: str = "",
        return_url: str = "",
        timeout_sec: int = 30,
        api_base_url: str = "https://cardlink.link/api",
        bearer_token: str = "",
    ) -> None:
        self.shop_id = str(shop_id or "").strip()
        self.bearer_token = str(bearer_token or secret_key or "").strip()
        self.return_url = str(return_url or "").strip()
        self.timeout_sec = max(int(timeout_sec or 30), 5)
        self.api_base_url = str(api_base_url or "").strip().rstrip("/")

    @property
    def enabled(self) -> bool:
        return bool(self.shop_id and self.bearer_token and self.api_base_url)

    def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        payload: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        if not self.enabled:
            raise CardlinkClientError("Интеграция Cardlink не настроена")
        url = f"{self.api_base_url}{path}"
        headers = {
            "Authorization": f"Bearer {self.bearer_token}",
            "Content-Type": "application/json",
        }
        try:
            resp = requests.request(
                method=method.upper(),
                url=url,
                headers=headers,
                params=params,
                json=payload,
                timeout=self.timeout_sec,
            )
        except requests.RequestException as exc:
            raise CardlinkClientError("Не удалось связаться с Cardlink API") from exc

        body = (resp.text or "").strip()
        if resp.status_code >= 400:
            raise CardlinkClientError(
                "Cardlink вернул ошибку",
                status_code=resp.status_code,
                body=body[:500],
            )
        if not body:
            return {}
        try:
            data = resp.json()
        except Exception as exc:
            raise CardlinkClientError(
                "Cardlink вернул не-JSON ответ",
                status_code=resp.status_code,
                body=body[:500],
            ) from exc
        return data if isinstance(data, dict) else {"data": data}

    @staticmethod
    def _pick(data: dict[str, Any], *keys: str) -> str:
        for key in keys:
            cur: Any = data
            parts = key.split(".")
            ok = True
            for part in parts:
                if isinstance(cur, dict) and part in cur:
                    cur = cur[part]
                else:
                    ok = False
                    break
            if ok:
                val = str(cur or "").strip()
                if val:
                    return val
        return ""

    @classmethod
    def _extract_confirmation_url(cls, data: dict[str, Any]) -> str:
        return cls._pick(
            data,
            "confirmation.confirmation_url",
            "confirmation.confirmationUrl",
            "confirmation.url",
            "confirmation_url",
            "confirmationUrl",
            "confirmation.link",
            "paymentUrl",
            "paymentURL",
            "payment_url",
            "paymentLink",
            "payment_link",
            "payUrl",
            "pay_url",
            "payLink",
            "pay_link",
            "checkoutUrl",
            "checkout_url",
            "checkoutLink",
            "checkout_link",
            "invoice_url",
            "bill.url",
            "bill_url",
            "link_page_url",
            "linkPageUrl",
            "link_url",
            "linkUrl",
            "link",
            "short_url",
            "shortUrl",
            "url",
            "redirect_url",
            "data.confirmation_url",
            "data.confirmationUrl",
            "data.confirmation.link",
            "data.paymentUrl",
            "data.payment_url",
            "data.paymentLink",
            "data.payment_link",
            "data.payUrl",
            "data.payLink",
            "data.checkoutUrl",
            "data.checkoutLink",
            "data.invoice_url",
            "data.link_page_url",
            "data.linkPageUrl",
            "data.link_url",
            "data.linkUrl",
            "data.link",
            "data.short_url",
            "data.shortUrl",
            "data.url",
        )

    def create_payment(
        self,
        amount_rub: int,
        description: str,
        metadata: dict[str, Any] | None = None,
        capture: bool = True,  # compatibility
        currency: str = "RUB",
    ) -> dict[str, Any]:
        del capture  # Cardlink does not use capture mode.
        idempotence_key = str(uuid.uuid4())
        payload: dict[str, Any] = {
            "shop_id": self.shop_id,
            # Cardlink ожидает целое число рублей (или строку-число).
            "amount": str(max(int(amount_rub or 0), 0)),
            "currency": (currency or "RUB").strip().upper() or "RUB",
            "order_id": idempotence_key,
            "description": (description or "").strip()[:255],
        }
        if self.return_url:
            payload["success_url"] = self.return_url
            payload["fail_url"] = self.return_url
        if metadata:
            payload["metadata"] = metadata
        data = self._request("POST", "/v1/bill/create", payload=payload)
        provider_payment_id = self._pick(
            data,
            "id",
            "billId",
            "bill_id",
            "bill.id",
            "data.id",
            "data.billId",
        )
        if not provider_payment_id:
            raise CardlinkClientError("Cardlink не вернул id счёта")
        confirmation_url = self._extract_confirmation_url(data)
        status = self._pick(data, "status", "bill.status", "data.status")
        result = dict(data)
        result["id"] = provider_payment_id
        result["status"] = status or "NEW"
        result["confirmation_url"] = confirmation_url
        result["idempotence_key"] = idempotence_key
        raw_amount = result.get("amount")
        if not isinstance(raw_amount, dict):
            result["amount"] = {
                "currency": payload["currency"],
                "value": str(raw_amount or payload["amount"]).strip() or payload["amount"],
            }
        else:
            amount_currency = str(raw_amount.get("currency") or payload["currency"]).strip().upper() or payload["currency"]
            amount_value = str(raw_amount.get("value") or payload["amount"]).strip() or payload["amount"]
            result["amount"] = {"currency": amount_currency, "value": amount_value}
        return result

    def get_payment(self, payment_id: str) -> dict[str, Any]:
        pid = str(payment_id or "").strip()
        if not pid:
            raise CardlinkClientError("Не передан payment_id для проверки")
        attempts: list[tuple[str, dict[str, Any] | None]] = [
            ("/v1/bill/status", {"id": pid}),
            (f"/bills/{pid}", None),
            ("/bills/status", {"id": pid}),
        ]
        last_error: CardlinkClientError | None = None
        for path, params in attempts:
            try:
                data = self._request("GET", path, params=params)
                result = dict(data)
                if not self._pick(result, "id", "billId", "bill_id", "bill.id"):
                    result["id"] = pid
                if not self._pick(result, "status", "bill.status", "data.status"):
                    result["status"] = "PENDING"
                result["confirmation_url"] = self._extract_confirmation_url(result)
                return result
            except CardlinkClientError as exc:
                last_error = exc
                if exc.status_code == 404:
                    continue
                raise
        if last_error:
            raise last_error
        raise CardlinkClientError("Не удалось получить статус счёта Cardlink")
