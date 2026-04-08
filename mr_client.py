"""
Managed Receiving - Unified Client Class
Route Optimizer 통합용. 로그인, 슬롯 조회, Help Assist 티켓 생성/수정을 하나의 클래스로 제공.

Usage:
    from mr_client import MRClient

    mr = MRClient()
    mr.login()

    # 개별 예약
    result = mr.book_appointment("MD", "MD1008883201", "2026-04-26", "21:00")

    # 일괄 예약
    results = mr.book_batch([
        {"dc_code": "MD", "po": "MD1008883201", "date": "2026-04-26", "time": "21:00"},
        {"dc_code": "MZ", "po": "MZ1008680101", "date": "2026-04-24", "time": "11:00"},
    ])
"""
import json
import os
import re
import ssl
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone
from zoneinfo import ZoneInfo


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
PARTNER_KEY = "c8786b9b-6c81-45b8-a632-649ece6868fb"
CARRIER_ID = 137415
DEFAULT_EMAIL = "logistics@innofoods.ca"

BASE_API = "https://managedreceiving.capstonelogistics.com/api"
BASE_APIV2 = "https://managedreceiving.capstonelogistics.com/apiv2"
APP_URL = "https://managedreceiving.capstonelogistics.com"

def _load_env():
    """Load .env file from same directory."""
    env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
    if os.path.exists(env_path):
        with open(env_path) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, v = line.split("=", 1)
                    os.environ.setdefault(k.strip(), v.strip())

_load_env()

USERNAME = os.environ.get("MR_USERNAME", "")
PASSWORD = os.environ.get("MR_PASSWORD", "")

TOKEN_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "mr_token.json")

# dc_code → site_id mapping
SITE_MAP = {
    "NW": 156, "MY": 157, "SW": 158, "NT": 159, "SE": 168,
    "MD": 169, "PA": 170, "MW": 171, "NC": 172, "MK": 173,
    "FE": 174, "HP": 175, "MG": 176, "MZ": 177, "MO": 178,
    "NE": 179, "SO": 180, "MP": 181, "SZ": 182, "MI": 183,
    "WJ": 184, "MS": 185, "MN": 186, "ME": 187, "WK": 251,
    "C1": 257, "C2": 258, "C3": 260,
}


class MRClient:
    """Managed Receiving API Client - 로그인, 조회, 예약 통합"""

    def __init__(self):
        self.token = None
        self.ctx = ssl.create_default_context()
        self._sites_cache = None

    # ── Auth ────────────────────────────────────────────────────

    def login(self) -> bool:
        """토큰 획득 (캐시 → 재로그인)"""
        # Try cached token
        if os.path.exists(TOKEN_FILE):
            with open(TOKEN_FILE) as f:
                data = json.load(f)
            if int(data.get("expiresOn", 0)) > time.time() + 300:
                self.token = data["accessToken"]
                return True

        # Headless browser login
        try:
            from playwright.sync_api import sync_playwright
        except ImportError:
            raise RuntimeError("playwright not installed. Run: pip3 install playwright && python3 -m playwright install chromium")

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_context().new_page()

            page.goto(APP_URL, wait_until="networkidle", timeout=60000)
            page.fill("#signInName", USERNAME)
            page.click("#continue")
            page.wait_for_timeout(5000)
            page.fill("#password", PASSWORD)
            page.click("#next")

            try:
                page.wait_for_url("**/managedreceiving.capstonelogistics.com/**", timeout=30000)
            except Exception:
                if page.query_selector('input#idSIButton9'):
                    page.click('input#idSIButton9')
                    page.wait_for_url("**/managedreceiving.capstonelogistics.com/**", timeout=15000)
                else:
                    browser.close()
                    return False

            page.wait_for_timeout(5000)

            token_data = page.evaluate("""() => {
                const r = {};
                for (let i = 0; i < localStorage.length; i++) {
                    const k = localStorage.key(i), v = localStorage.getItem(k);
                    try { const p = JSON.parse(v);
                        if (p && p.credentialType === 'AccessToken') { r.accessToken = p.secret; r.expiresOn = p.expiresOn; }
                        if (p && p.credentialType === 'RefreshToken') { r.refreshToken = p.secret; }
                    } catch(e) {}
                }
                return r;
            }""")
            browser.close()

        if token_data.get("accessToken"):
            self.token = token_data["accessToken"]
            with open(TOKEN_FILE, "w") as f:
                json.dump(token_data, f, indent=2)
            return True
        return False

    def _ensure_token(self):
        if not self.token:
            if not self.login():
                raise RuntimeError("Login failed")

    # ── HTTP ────────────────────────────────────────────────────

    def _headers(self) -> dict:
        return {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    def _get(self, url: str) -> dict:
        self._ensure_token()
        req = urllib.request.Request(url, headers=self._headers())
        resp = urllib.request.urlopen(req, timeout=15, context=self.ctx)
        return json.loads(resp.read())

    def _post(self, url: str, data: dict) -> dict:
        self._ensure_token()
        body = json.dumps(data).encode()
        req = urllib.request.Request(url, data=body, headers=self._headers(), method="POST")
        try:
            resp = urllib.request.urlopen(req, timeout=15, context=self.ctx)
        except urllib.error.HTTPError as e:
            body_text = e.read().decode("utf-8", errors="replace") if e.fp else ""
            raise RuntimeError(f"HTTP {e.code}: {body_text[:500]}") from e
        raw = resp.read()
        if not raw or not raw.strip():
            return {}
        return json.loads(raw)

    # ── Site / DC ───────────────────────────────────────────────

    @staticmethod
    def dc_to_site_id(dc_code: str) -> int:
        """dc_code → site_id 변환"""
        dc = dc_code.upper()
        if dc not in SITE_MAP:
            raise ValueError(f"Unknown dc_code: {dc_code}")
        return SITE_MAP[dc]

    def get_site_detail(self, site_id: int) -> dict | None:
        """사이트 상세 정보 (timezone 등)"""
        if not self._sites_cache:
            data = self._get(f"{BASE_API}/partner/{PARTNER_KEY}/sites")
            self._sites_cache = {s["id"]: s for s in data.get("value", [])}
        return self._sites_cache.get(site_id)

    def get_site_timezone(self, site_id: int) -> str:
        site = self.get_site_detail(site_id)
        return site.get("timeZone", "America/New_York") if site else "America/New_York"

    # ── Orders ──────────────────────────────────────────────────

    def find_order(self, site_id: int, po_number: str) -> dict | None:
        """PO 검색. 찾으면 foundOrder dict, 없으면 None"""
        results = self._post(
            f"{BASE_APIV2}/partner/{PARTNER_KEY}/Orders/FindOrders",
            {"searchTerms": [po_number], "siteId": site_id},
        )
        for r in results:
            if r.get("status") == "Found":
                return r
        return None

    # ── Duration ────────────────────────────────────────────────

    def calculate_duration(self, site_id: int, door_group_id: int, order: dict) -> int:
        order_for_api = {
            "caseCount": order["caseCount"],
            "dueDate": order["dueDate"],
            "doorGroupID": order.get("doorGroupId"),
            "managedType": order.get("managedType"),
            "palletCount": order["palletCount"],
            "vendorId": order["foundVendor"]["id"],
        }
        result = self._post(
            f"{BASE_API}/partner/{PARTNER_KEY}/PartnerAppointments/CalculateAppointmentDuration",
            {"appointmentPalletOverride": None, "doorGroupId": door_group_id, "orders": [order_for_api], "siteId": site_id},
        )
        return int(result.get("newDuration", 180))

    # ── Door Group ──────────────────────────────────────────────

    def get_door_group(self, site_id: int, order: dict) -> dict:
        order_for_api = {
            "caseCount": order["caseCount"],
            "dueDate": order["dueDate"],
            "doorGroupID": order.get("doorGroupId"),
            "managedType": order.get("managedType"),
            "palletCount": order["palletCount"],
            "vendorId": order["foundVendor"]["id"],
        }
        result = self._post(
            f"{BASE_API}/partner/{PARTNER_KEY}/sites/{site_id}/getDoorGroup",
            {"carrierId": CARRIER_ID, "orders": [order_for_api]},
        )
        return result.get("data", {})

    # ── Slots ───────────────────────────────────────────────────

    def get_unreserved_slots(self, site_id: int, door_group_id: int, duration: int, order: dict) -> list:
        """가용 슬롯 조회"""
        order_for_api = {
            "caseCount": order["caseCount"],
            "dueDate": order["dueDate"],
            "doorGroupID": order.get("doorGroupId"),
            "managedType": order.get("managedType"),
            "palletCount": order["palletCount"],
            "vendorId": order["foundVendor"]["id"],
        }
        result = self._post(
            f"{BASE_API}/partner/{PARTNER_KEY}/sites/{site_id}/getUnreservedSlots",
            {
                "appointmentPalletOverride": None,
                "appointmentDate": None,
                "appointmentDuration": duration,
                "carrierId": CARRIER_ID,
                "doorGroupId": door_group_id,
                "orders": [order_for_api],
            },
        )
        return result.get("slots", [])

    def find_matching_slot(self, slots: list, ideal_date: str, ideal_time: str, site_tz: str) -> dict | None:
        """슬롯 중 원하는 날짜/시간과 매칭되는 것 찾기"""
        tz = ZoneInfo(site_tz)
        for slot in slots:
            start_utc = datetime.fromisoformat(slot["startTime"].replace("Z", "+00:00"))
            start_local = start_utc.astimezone(tz)
            slot_date = start_local.strftime("%Y-%m-%d")
            slot_time = start_local.strftime("%H:%M")
            if slot_date == ideal_date and slot_time == ideal_time:
                return slot
        return None

    # ── Timezone Helper ─────────────────────────────────────────

    @staticmethod
    def _date_to_utc_midnight(date_str: str, tz_name: str) -> str:
        tz = ZoneInfo(tz_name)
        local_midnight = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=tz)
        utc_time = local_midnight.astimezone(timezone.utc)
        return utc_time.strftime("%Y-%m-%dT%H:%M:%S.000Z")

    # ── Help Assist ─────────────────────────────────────────────

    def _build_ticket_orders(self, orders_data: list) -> list:
        """FindOrders 결과를 티켓 order 포맷으로 변환"""
        ticket_orders = []
        for o in orders_data:
            found = o["foundOrder"]
            ticket_orders.append({
                "orderId": found["id"],
                "doorGroupID": found.get("doorGroupId"),
                "vendorId": found["foundVendor"]["id"],
                "asnBolNumber": found.get("asnBolNumber"),
                "asnProNumber": found.get("asnProNumber"),
                "backhaulPickupConfirmationNumber": None,
                "bolNumber": found.get("bolNumber"),
                "caseCount": found["caseCount"],
                "comments": found.get("comments"),
                "consigneeCode": found.get("consigneeCode"),
                "dueDate": found["dueDate"],
                "entryDate": found.get("entryDate"),
                "estimatedReceivedPalletCount": found.get("estReceivedPallets"),
                "freightBasis": found.get("freightBasis"),
                "loadWeight": found.get("loadWeight"),
                "managedType": found.get("managedType"),
                "number": found["poNumber"],
                "originCity": None, "originLatitude": None,
                "originLongitude": None, "originPostalCode": None, "originState": None,
                "palletCount": found["palletCount"],
                "pickupDate": None,
                "proNumber": found.get("proNumber"),
            })
        return ticket_orders

    def create_help_assist(self, site_id: int, ideal_date: str, ideal_time: str,
                           duration: int, orders_data: list, email: str = DEFAULT_EMAIL) -> dict:
        """Help Assist 티켓 생성. 이미 있으면 자동 update."""
        site_tz = self.get_site_timezone(site_id)
        main_po = orders_data[0]["foundOrder"]["poNumber"]
        ticket_orders = self._build_ticket_orders(orders_data)

        payload = {
            "emailAddress": email,
            "events": [],
            "orders": None,
            "phoneNumber": None,
            "siteId": site_id,
            "type": "Scheduling",
            "appointmentSchedule": {
                "appointmentId": None,
                "ticketAppointment": {
                    "appointmentPalletOverride": None,
                    "carrierId": CARRIER_ID,
                    "deliveryCarrier": None,
                    "deliveryCarrierRecordID": None,
                    "doorGroupId": None,
                    "doorId": None,
                    "duration": duration,
                    "idealAppointmentDate": self._date_to_utc_midnight(ideal_date, site_tz),
                    "idealStartTime": ideal_time,
                    "isDropload": False,
                    "isIntermodal": None,
                    "loadWeight": None,
                    "mainOrderNumber": main_po,
                    "notificationList": email,
                    "orders": ticket_orders,
                    "schedule": None,
                    "slotStartTime": None,
                    "unloader": "Capstone",
                },
            },
        }

        try:
            result = self._post(
                f"{BASE_API}/partner/{PARTNER_KEY}/HelpAssistTickets/CreateHelpAssistTicket",
                payload,
            )
            value = result.get("value", "{}")
            ticket = json.loads(value) if isinstance(value, str) else value
            return {"status": "created", "ticket_id": ticket.get("id"), "ticket": ticket}

        except RuntimeError as e:
            if "OPEN_TICKET_EXISTS" in str(e):
                match = re.search(r'"target":"(\d+)"', str(e))
                existing_id = int(match.group(1)) if match else None
                if existing_id:
                    return self.update_help_assist(existing_id, ideal_date, ideal_time, orders_data)
                raise
            raise

    def update_help_assist(self, ticket_id: int, ideal_date: str, ideal_time: str,
                           orders_data: list) -> dict:
        """기존 Help Assist 티켓 수정"""
        site_id = None
        # Get site_id from order consigneeCode or find from orders
        for o in orders_data:
            po = o["foundOrder"]["poNumber"]
            dc = po[:2]
            if dc in SITE_MAP:
                site_id = SITE_MAP[dc]
                break

        site_tz = self.get_site_timezone(site_id) if site_id else "America/New_York"
        ticket_orders = self._build_ticket_orders(orders_data)

        payload = {
            "appointmentPalletOverride": None,
            "id": ticket_id,
            "idealAppointmentDate": self._date_to_utc_midnight(ideal_date, site_tz),
            "idealStartTime": ideal_time,
            "orders": ticket_orders,
            "ticketCategory": None,
        }

        self._post(
            f"{BASE_API}/partner/{PARTNER_KEY}/HelpAssistTickets/updateDetails",
            payload,
        )
        return {"status": "updated", "ticket_id": ticket_id}

    # ── Main Booking Flow ───────────────────────────────────────

    def book_appointment(self, dc_code: str, po_number: str, date: str, time: str,
                         expected_pallets: int | None = None, email: str = DEFAULT_EMAIL) -> dict:
        """
        단일 PO 예약 처리.

        Returns:
            {
                "status": "matched" | "help_assist_created" | "help_assist_updated" | "error",
                "message": str,
                "ticket_id": int | None,
                "slot": dict | None,
            }
        """
        try:
            site_id = self.dc_to_site_id(dc_code)
            po_clean = po_number.replace("-", "")

            # 1. PO 검색
            found = self.find_order(site_id, po_clean)
            if not found:
                return {"status": "error", "message": f"PO {po_clean} not found on site {dc_code}"}

            order = found["foundOrder"]

            # 팔레트 수량 체크
            if expected_pallets and order["palletCount"] != expected_pallets:
                return {
                    "status": "error",
                    "message": f"Pallet mismatch: expected {expected_pallets}, got {order['palletCount']}",
                }

            # 2. Door group
            dg = self.get_door_group(site_id, order)
            door_group_id = dg.get("doorGroupID")

            # 3. Duration
            duration = self.calculate_duration(site_id, door_group_id, order)

            # 4. 슬롯 조회 + 매칭
            site_tz = self.get_site_timezone(site_id)
            slots = self.get_unreserved_slots(site_id, door_group_id, duration, order)
            matched = self.find_matching_slot(slots, date, time, site_tz)

            if matched:
                # TODO: 직접 예약 생성 (Method 1) — API 캡처 후 구현
                return {
                    "status": "matched",
                    "message": f"Slot found: {date} {time} on {dc_code}. Direct booking not yet implemented.",
                    "slot": matched,
                    "ticket_id": None,
                }

            # 5. 매칭 없으면 Help Assist
            result = self.create_help_assist(site_id, date, time, duration, [found], email)
            return {
                "status": f"help_assist_{result['status']}",
                "message": f"Help Assist ticket {result['status']}: #{result['ticket_id']}",
                "ticket_id": result["ticket_id"],
                "slot": None,
            }

        except Exception as e:
            return {"status": "error", "message": str(e)}

    def book_batch(self, load_plan: list[dict], email: str = DEFAULT_EMAIL) -> list[dict]:
        """
        일괄 예약 처리.

        Args:
            load_plan: [{"dc_code": "MD", "po": "MD1008883201", "date": "2026-04-26", "time": "21:00", "pallets": 10}, ...]

        Returns:
            각 PO별 결과 리스트
        """
        results = []
        for item in load_plan:
            result = self.book_appointment(
                dc_code=item["dc_code"],
                po_number=item["po"],
                date=item["date"],
                time=item["time"],
                expected_pallets=item.get("pallets"),
                email=email,
            )
            result["dc_code"] = item["dc_code"]
            result["po"] = item["po"]
            results.append(result)
        return results


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import sys

    if len(sys.argv) < 5:
        print("Usage: xvfb-run python3 mr_client.py <dc_code> <po> <date> <time> [pallets]")
        print("Example: xvfb-run python3 mr_client.py MD MD1008883201 2026-04-26 21:00 10")
        sys.exit(1)

    mr = MRClient()
    mr.login()

    dc = sys.argv[1]
    po = sys.argv[2]
    date = sys.argv[3]
    tm = sys.argv[4]
    pallets = int(sys.argv[5]) if len(sys.argv) > 5 else None

    result = mr.book_appointment(dc, po, date, tm, pallets)
    print(json.dumps(result, indent=2, ensure_ascii=False))
