"""
Transit Flow Backend Service - Leteane Industries Freight Logistics Engine
==========================================================================
Comprehensive freight capacity management, route scheduling, booking engine,
and RESTful API service for Transit Flow by Leteane Industries.

Features:
- Thread-safe SQLite persistence with automatic schema migration and seed data
- Freight Capacity Allocation & Overbooking Prevention Engine
- Dynamic route pricing & South African freight corridor calculations (N1, N2, N3)
- Full RESTful HTTP API with CORS support
- Automated self-testing suite (--test)
- Interactive Command-Line Interface (--cli)
- CSV/JSON fleet report export
"""

import argparse
import csv
import io
import json
import logging
import math
import os
import re
import sqlite3
import sys
import threading
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import parse_qs, urlparse

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("TransitFlow")

DEFAULT_DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "transit_flow.db")
DEFAULT_PORT = 8000
DEFAULT_HOST = "0.0.0.0"

# ---------------------------------------------------------------------------
# Data Models
# ---------------------------------------------------------------------------

@dataclass
class TruckRecord:
    id: str
    name: str
    plate: str
    type: str
    origin: str
    destination: str
    capacity: float
    total: float
    status: str
    departure: str
    driver: str
    carrier: str = "Flow Logistics"
    created_at: str = ""

@dataclass
class BookingRecord:
    id: str
    truck_id: str
    client_name: str
    cargo_type: str
    weight_tons: float
    origin: str
    destination: str
    rate_per_ton: float
    total_price_zar: float
    status: str
    booked_at: str

@dataclass
class RouteRecord:
    id: str
    origin: str
    destination: str
    corridor: str
    distance_km: float
    est_hours: float
    base_rate_per_ton: float

# Predefined South African freight corridors
DEFAULT_ROUTES: List[RouteRecord] = [
    RouteRecord("RT-JHB-CPT", "Johannesburg", "Cape Town", "N1 Corridor", 1398.0, 15.5, 1250.0),
    RouteRecord("RT-DBN-JHB", "Durban", "Johannesburg", "N3 Corridor", 568.0, 6.5, 680.0),
    RouteRecord("RT-CPT-PLZ", "Cape Town", "Port Elizabeth", "N2 Coastal", 754.0, 8.5, 780.0),
    RouteRecord("RT-JHB-DBN", "Johannesburg", "Durban", "N3 Corridor", 568.0, 6.5, 680.0),
    RouteRecord("RT-DBN-CPT", "Durban", "Cape Town", "N2 Coastal link", 1635.0, 18.0, 1420.0),
    RouteRecord("RT-JHB-PRY", "Johannesburg", "Pretoria", "N1 North link", 65.0, 1.2, 180.0),
]

SEED_TRUCKS: List[TruckRecord] = [
    TruckRecord(
        id="TF1024",
        name="Volvo FH 540",
        plate="GP 4821",
        type="Curtain side",
        origin="Johannesburg",
        destination="Cape Town",
        capacity=24.0,
        total=34.0,
        status="Available",
        departure="Today, 14:30",
        driver="Thabo Mokoena",
        carrier="Flow Logistics",
    ),
    TruckRecord(
        id="TF1025",
        name="Scania R500",
        plate="GP 6392",
        type="Refrigerated",
        origin="Durban",
        destination="Johannesburg",
        capacity=18.0,
        total=28.0,
        status="Available",
        departure="Today, 16:00",
        driver="Sipho Dlamini",
        carrier="Flow Logistics",
    ),
    TruckRecord(
        id="TF1026",
        name="Mercedes Actros",
        plate="CA 2106",
        type="Flatbed",
        origin="Cape Town",
        destination="Port Elizabeth",
        capacity=8.0,
        total=32.0,
        status="On route",
        departure="Today, 12:00",
        driver="David Petersen",
        carrier="Cape Freight",
    ),
    TruckRecord(
        id="TF1027",
        name="MAN TGX 480",
        plate="GP 8843",
        type="Curtain side",
        origin="Johannesburg",
        destination="Durban",
        capacity=30.0,
        total=34.0,
        status="Available",
        departure="Tomorrow, 08:00",
        driver="James Nkosi",
        carrier="Flow Logistics",
    ),
    TruckRecord(
        id="TF1028",
        name="Volvo FH 460",
        plate="ND 3927",
        type="Refrigerated",
        origin="Durban",
        destination="Cape Town",
        capacity=0.0,
        total=28.0,
        status="Fully booked",
        departure="Today, 18:00",
        driver="Bongani Zulu",
        carrier="Durban Transport",
    ),
    TruckRecord(
        id="TF1029",
        name="Scania S560",
        plate="GP 5710",
        type="Flatbed",
        origin="Johannesburg",
        destination="Pretoria",
        capacity=22.0,
        total=32.0,
        status="Available",
        departure="Tomorrow, 09:30",
        driver="Peter Molefe",
        carrier="Flow Logistics",
    ),
]

# ---------------------------------------------------------------------------
# Database Layer
# ---------------------------------------------------------------------------

class Database:
    """Thread-safe SQLite manager for Transit Flow."""

    def __init__(self, db_path: str = DEFAULT_DB_PATH):
        self.db_path = db_path
        self._lock = threading.Lock()
        self._mem_conn = None
        if self.db_path == ":memory:":
            self._mem_conn = sqlite3.connect(":memory:", check_same_thread=False)
            self._mem_conn.row_factory = sqlite3.Row
            self._mem_conn.execute("PRAGMA foreign_keys = ON")
        self._init_db()

    def get_connection(self) -> sqlite3.Connection:
        if self._mem_conn is not None:
            return self._mem_conn
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        return conn

    def _init_db(self) -> None:
        with self._lock:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.executescript(
                    """
                    CREATE TABLE IF NOT EXISTS trucks (
                        id TEXT PRIMARY KEY,
                        name TEXT NOT NULL,
                        plate TEXT NOT NULL,
                        type TEXT NOT NULL,
                        origin TEXT NOT NULL,
                        destination TEXT NOT NULL,
                        capacity REAL NOT NULL CHECK(capacity >= 0),
                        total REAL NOT NULL CHECK(total > 0),
                        status TEXT NOT NULL,
                        departure TEXT NOT NULL,
                        driver TEXT NOT NULL,
                        carrier TEXT NOT NULL DEFAULT 'Flow Logistics',
                        created_at TEXT NOT NULL
                    );

                    CREATE TABLE IF NOT EXISTS bookings (
                        id TEXT PRIMARY KEY,
                        truck_id TEXT NOT NULL,
                        client_name TEXT NOT NULL,
                        cargo_type TEXT NOT NULL,
                        weight_tons REAL NOT NULL CHECK(weight_tons > 0),
                        origin TEXT NOT NULL,
                        destination TEXT NOT NULL,
                        rate_per_ton REAL NOT NULL DEFAULT 0,
                        total_price_zar REAL NOT NULL DEFAULT 0,
                        status TEXT NOT NULL,
                        booked_at TEXT NOT NULL,
                        FOREIGN KEY (truck_id) REFERENCES trucks(id) ON DELETE CASCADE
                    );

                    CREATE TABLE IF NOT EXISTS routes (
                        id TEXT PRIMARY KEY,
                        origin TEXT NOT NULL,
                        destination TEXT NOT NULL,
                        corridor TEXT NOT NULL,
                        distance_km REAL NOT NULL,
                        est_hours REAL NOT NULL,
                        base_rate_per_ton REAL NOT NULL
                    );

                    CREATE TABLE IF NOT EXISTS support_tickets (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        name TEXT NOT NULL,
                        email TEXT NOT NULL,
                        message TEXT NOT NULL,
                        status TEXT NOT NULL DEFAULT 'Open',
                        created_at TEXT NOT NULL
                    );

                    CREATE TABLE IF NOT EXISTS audit_logs (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        action TEXT NOT NULL,
                        details TEXT NOT NULL,
                        timestamp TEXT NOT NULL
                    );
                    """
                )

                # Seed trucks if table is empty
                cursor.execute("SELECT COUNT(*) FROM trucks")
                count = cursor.fetchone()[0]
                if count == 0:
                    now = datetime.now(timezone.utc).isoformat()
                    for t in SEED_TRUCKS:
                        cursor.execute(
                            """
                            INSERT INTO trucks (id, name, plate, type, origin, destination, capacity, total, status, departure, driver, carrier, created_at)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """,
                            (t.id, t.name, t.plate, t.type, t.origin, t.destination, t.capacity, t.total, t.status, t.departure, t.driver, t.carrier, now),
                        )

                # Seed routes if empty
                cursor.execute("SELECT COUNT(*) FROM routes")
                route_count = cursor.fetchone()[0]
                if route_count == 0:
                    for r in DEFAULT_ROUTES:
                        cursor.execute(
                            """
                            INSERT INTO routes (id, origin, destination, corridor, distance_km, est_hours, base_rate_per_ton)
                            VALUES (?, ?, ?, ?, ?, ?, ?)
                            """,
                            (r.id, r.origin, r.destination, r.corridor, r.distance_km, r.est_hours, r.base_rate_per_ton),
                        )

                conn.commit()

    def log_activity(self, action: str, details: str) -> None:
        with self._lock:
            with self.get_connection() as conn:
                now = datetime.now(timezone.utc).isoformat()
                conn.execute(
                    "INSERT INTO audit_logs (action, details, timestamp) VALUES (?, ?, ?)",
                    (action, details, now),
                )
                conn.commit()

    # -----------------------------------------------------------------------
    # Trucks CRUD
    # -----------------------------------------------------------------------

    def get_trucks(
        self,
        search: str = "",
        origin: str = "",
        destination: str = "",
        truck_type: str = "",
        status: str = "",
        min_capacity: float = 0.0,
    ) -> List[Dict[str, Any]]:
        query = "SELECT * FROM trucks WHERE 1=1"
        params: List[Any] = []

        if search:
            query += " AND (name LIKE ? OR id LIKE ? OR origin LIKE ? OR destination LIKE ? OR driver LIKE ?)"
            term = f"%{search}%"
            params.extend([term, term, term, term, term])

        if origin:
            query += " AND origin = ?"
            params.append(origin)

        if destination:
            query += " AND destination = ?"
            params.append(destination)

        if truck_type:
            query += " AND type = ?"
            params.append(truck_type)

        if status:
            query += " AND status = ?"
            params.append(status)

        if min_capacity > 0:
            query += " AND capacity >= ?"
            params.append(min_capacity)

        query += " ORDER BY id ASC"

        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, params)
            return [dict(row) for row in cursor.fetchall()]

    def get_truck(self, truck_id: str) -> Optional[Dict[str, Any]]:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM trucks WHERE id = ?", (truck_id,))
            row = cursor.fetchone()
            if not row:
                return None
            truck = dict(row)
            cursor.execute("SELECT * FROM bookings WHERE truck_id = ?", (truck_id,))
            truck["bookings"] = [dict(b) for b in cursor.fetchall()]
            return truck

    def create_truck(self, data: Dict[str, Any]) -> Dict[str, Any]:
        with self._lock:
            with self.get_connection() as conn:
                cursor = conn.cursor()

                # Generate ID if missing
                truck_id = data.get("id")
                if not truck_id:
                    cursor.execute("SELECT id FROM trucks WHERE id LIKE 'TF%' ORDER BY id DESC LIMIT 1")
                    last = cursor.fetchone()
                    if last:
                        match = re.search(r"TF(\d+)", last[0])
                        next_num = int(match.group(1)) + 1 if match else 1030
                    else:
                        next_num = 1030
                    truck_id = f"TF{next_num}"

                total = float(data.get("total", data.get("capacity", 30.0)))
                capacity = float(data.get("capacity", total))
                status = data.get("status", "Available" if capacity > 0 else "Fully booked")
                now = datetime.now(timezone.utc).isoformat()

                truck = TruckRecord(
                    id=truck_id,
                    name=data.get("name", "Volvo FH 540"),
                    plate=data.get("plate", "GP 0000"),
                    type=data.get("type", "Curtain side"),
                    origin=data.get("origin", "Johannesburg"),
                    destination=data.get("destination", "Cape Town"),
                    capacity=capacity,
                    total=total,
                    status=status,
                    departure=data.get("departure", "Tomorrow, 08:00"),
                    driver=data.get("driver", "Unassigned Driver"),
                    carrier=data.get("carrier", "Flow Logistics"),
                    created_at=now,
                )

                cursor.execute(
                    """
                    INSERT INTO trucks (id, name, plate, type, origin, destination, capacity, total, status, departure, driver, carrier, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        truck.id,
                        truck.name,
                        truck.plate,
                        truck.type,
                        truck.origin,
                        truck.destination,
                        truck.capacity,
                        truck.total,
                        truck.status,
                        truck.departure,
                        truck.driver,
                        truck.carrier,
                        truck.created_at,
                    ),
                )
                conn.commit()

        self.log_activity("CREATE_TRUCK", f"Truck {truck_id} added ({truck.name})")
        return asdict(truck)

    def update_truck(self, truck_id: str, data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        with self._lock:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT * FROM trucks WHERE id = ?", (truck_id,))
                current = cursor.fetchone()
                if not current:
                    return None

                allowed_fields = [
                    "name", "plate", "type", "origin", "destination",
                    "capacity", "total", "status", "departure", "driver", "carrier"
                ]
                updates = []
                params = []
                for field in allowed_fields:
                    if field in data:
                        updates.append(f"{field} = ?")
                        params.append(data[field])

                if not updates:
                    return dict(current)

                params.append(truck_id)
                cursor.execute(f"UPDATE trucks SET {', '.join(updates)} WHERE id = ?", params)
                conn.commit()

        self.log_activity("UPDATE_TRUCK", f"Truck {truck_id} updated: {list(data.keys())}")
        return self.get_truck(truck_id)

    def delete_truck(self, truck_id: str) -> bool:
        with self._lock:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("DELETE FROM trucks WHERE id = ?", (truck_id,))
                deleted = cursor.rowcount > 0
                conn.commit()

        if deleted:
            self.log_activity("DELETE_TRUCK", f"Truck {truck_id} deleted")
        return deleted

    # -----------------------------------------------------------------------
    # Bookings & Capacity Allocation
    # -----------------------------------------------------------------------

    def reserve_capacity(
        self,
        truck_id: str,
        weight_tons: float,
        client_name: str = "Tintswalo Leteane",
        cargo_type: str = "General Freight",
    ) -> Dict[str, Any]:
        """Atomically allocates capacity, prevents overbooking, and updates status."""
        if weight_tons <= 0:
            raise ValueError("Weight tons must be greater than zero.")

        with self._lock:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT * FROM trucks WHERE id = ?", (truck_id,))
                row = cursor.fetchone()
                if not row:
                    raise KeyError(f"Truck {truck_id} not found.")

                truck = dict(row)
                if truck["status"] == "Fully booked":
                    raise ValueError(f"Truck {truck_id} is already fully booked.")
                if truck["status"] == "Maintenance":
                    raise ValueError(f"Truck {truck_id} is currently under maintenance.")
                if truck["capacity"] < weight_tons:
                    raise ValueError(
                        f"Insufficient capacity on truck {truck_id}. Requested: {weight_tons}t, Available: {truck['capacity']}t"
                    )

                new_capacity = round(truck["capacity"] - weight_tons, 2)
                new_status = "Fully booked" if new_capacity <= 0 else truck["status"]

                # Calculate route pricing
                cursor.execute(
                    "SELECT * FROM routes WHERE origin = ? AND destination = ?",
                    (truck["origin"], truck["destination"]),
                )
                route_row = cursor.fetchone()
                base_rate = route_row["base_rate_per_ton"] if route_row else 950.0

                # Refrigerated cold chain surcharge
                if truck["type"].lower() == "refrigerated":
                    base_rate *= 1.25

                total_zar = round(weight_tons * base_rate, 2)

                booking_id = f"BK-{int(time.time())}-{truck_id}"
                now = datetime.now(timezone.utc).isoformat()

                cursor.execute(
                    """
                    INSERT INTO bookings (id, truck_id, client_name, cargo_type, weight_tons, origin, destination, rate_per_ton, total_price_zar, status, booked_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        booking_id,
                        truck_id,
                        client_name,
                        cargo_type,
                        weight_tons,
                        truck["origin"],
                        truck["destination"],
                        base_rate,
                        total_zar,
                        "Reserved",
                        now,
                    ),
                )

                cursor.execute(
                    "UPDATE trucks SET capacity = ?, status = ? WHERE id = ?",
                    (new_capacity, new_status, truck_id),
                )
                conn.commit()

        self.log_activity(
            "RESERVE_CAPACITY",
            f"Reserved {weight_tons} tons on {truck_id} for {client_name} (Booking {booking_id})",
        )
        return {
            "booking_id": booking_id,
            "truck_id": truck_id,
            "client_name": client_name,
            "weight_tons": weight_tons,
            "remaining_capacity": new_capacity,
            "truck_status": new_status,
            "total_price_zar": total_zar,
            "status": "Reserved",
            "booked_at": now,
        }

    def get_bookings(self, truck_id: Optional[str] = None) -> List[Dict[str, Any]]:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            if truck_id:
                cursor.execute("SELECT * FROM bookings WHERE truck_id = ? ORDER BY booked_at DESC", (truck_id,))
            else:
                cursor.execute("SELECT * FROM bookings ORDER BY booked_at DESC")
            return [dict(b) for b in cursor.fetchall()]

    def cancel_booking(self, booking_id: str) -> Dict[str, Any]:
        """Cancels booking and restores truck capacity."""
        with self._lock:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT * FROM bookings WHERE id = ?", (booking_id,))
                booking_row = cursor.fetchone()
                if not booking_row:
                    raise KeyError(f"Booking {booking_id} not found.")

                booking = dict(booking_row)
                if booking["status"] == "Cancelled":
                    raise ValueError(f"Booking {booking_id} is already cancelled.")

                # Restore capacity
                truck_id = booking["truck_id"]
                weight = booking["weight_tons"]

                cursor.execute("SELECT * FROM trucks WHERE id = ?", (truck_id,))
                truck_row = cursor.fetchone()
                if truck_row:
                    truck = dict(truck_row)
                    restored_capacity = min(truck["total"], round(truck["capacity"] + weight, 2))
                    new_status = "Available" if restored_capacity > 0 else truck["status"]
                    cursor.execute(
                        "UPDATE trucks SET capacity = ?, status = ? WHERE id = ?",
                        (restored_capacity, new_status, truck_id),
                    )

                cursor.execute("UPDATE bookings SET status = 'Cancelled' WHERE id = ?", (booking_id,))
                conn.commit()

        self.log_activity("CANCEL_BOOKING", f"Cancelled booking {booking_id}, restored {weight}t on {truck_id}")
        return {"status": "Cancelled", "booking_id": booking_id, "restored_tons": weight}

    # -----------------------------------------------------------------------
    # Analytics & Aggregates
    # -----------------------------------------------------------------------

    def get_analytics(self) -> Dict[str, Any]:
        with self.get_connection() as conn:
            cursor = conn.cursor()

            cursor.execute("SELECT COUNT(*) FROM trucks")
            total_trucks = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(*) FROM trucks WHERE status = 'Available'")
            available_trucks = cursor.fetchone()[0]

            cursor.execute("SELECT SUM(capacity), SUM(total) FROM trucks")
            cap_row = cursor.fetchone()
            available_capacity = round(cap_row[0] or 0.0, 1)
            total_capacity = round(cap_row[1] or 0.0, 1)
            allocated_capacity = round(total_capacity - available_capacity, 1)
            utilization_rate = (
                round((allocated_capacity / total_capacity) * 100, 1) if total_capacity > 0 else 0.0
            )

            cursor.execute("SELECT COUNT(DISTINCT origin || '-' || destination) FROM trucks")
            active_routes = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(*) FROM bookings WHERE status = 'Reserved'")
            active_bookings = cursor.fetchone()[0]

            # Route breakdown
            cursor.execute(
                """
                SELECT origin, destination, COUNT(*) as truck_count, SUM(capacity) as free_cap, SUM(total) as tot_cap
                FROM trucks
                GROUP BY origin, destination
                """
            )
            route_stats = [dict(r) for r in cursor.fetchall()]

            # Fleet by type
            cursor.execute("SELECT type, COUNT(*) as count, SUM(capacity) as cap FROM trucks GROUP BY type")
            fleet_by_type = [dict(r) for r in cursor.fetchall()]

            # 7-day capacity simulation
            weekly_trend = [
                {"day": "Mon", "capacity_tons": 820, "load_pct": 54},
                {"day": "Tue", "capacity_tons": 790, "load_pct": 72},
                {"day": "Wed", "capacity_tons": 810, "load_pct": 62},
                {"day": "Thu", "capacity_tons": 842, "load_pct": 86},
                {"day": "Fri", "capacity_tons": 830, "load_pct": 75},
                {"day": "Sat", "capacity_tons": 760, "load_pct": 48},
                {"day": "Sun", "capacity_tons": 740, "load_pct": 38},
            ]

            return {
                "available_trucks": available_trucks,
                "total_trucks": total_trucks,
                "available_capacity_tons": available_capacity,
                "total_capacity_tons": total_capacity,
                "allocated_capacity_tons": allocated_capacity,
                "fleet_utilization_pct": utilization_rate,
                "active_routes_count": active_routes,
                "active_bookings_count": active_bookings,
                "route_breakdown": route_stats,
                "fleet_by_type": fleet_by_type,
                "weekly_trend": weekly_trend,
                "last_updated": datetime.now(timezone.utc).isoformat(),
            }

    def get_routes(self) -> List[Dict[str, Any]]:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM routes")
            routes = [dict(r) for r in cursor.fetchall()]

            # Add dynamic active trucks count
            for r in routes:
                cursor.execute(
                    "SELECT COUNT(*), SUM(capacity) FROM trucks WHERE origin = ? AND destination = ?",
                    (r["origin"], r["destination"]),
                )
                t_count, t_cap = cursor.fetchone()
                r["active_trucks"] = t_count or 0
                r["available_capacity"] = t_cap or 0.0

            return routes

    def get_carriers(self) -> List[Dict[str, Any]]:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT carrier as name, COUNT(*) as active_trucks, SUM(capacity) as available_capacity
                FROM trucks
                GROUP BY carrier
                """
            )
            return [dict(r) for r in cursor.fetchall()]

    def create_support_ticket(self, name: str, email: str, message: str) -> Dict[str, Any]:
        with self._lock:
            with self.get_connection() as conn:
                now = datetime.now(timezone.utc).isoformat()
                cursor = conn.cursor()
                cursor.execute(
                    "INSERT INTO support_tickets (name, email, message, status, created_at) VALUES (?, ?, ?, 'Open', ?)",
                    (name, email, message, now),
                )
                conn.commit()
                ticket_id = cursor.lastrowid

        self.log_activity("SUPPORT_TICKET", f"New support ticket #{ticket_id} from {email}")
        return {"id": ticket_id, "name": name, "email": email, "message": message, "status": "Open", "created_at": now}

    def export_csv(self) -> str:
        trucks = self.get_trucks()
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(["Truck ID", "Truck Model", "Plate", "Type", "Origin", "Destination", "Available Tons", "Total Tons", "Status", "Departure", "Driver", "Carrier"])
        for t in trucks:
            writer.writerow([
                t["id"], t["name"], t["plate"], t["type"], t["origin"], t["destination"],
                t["capacity"], t["total"], t["status"], t["departure"], t["driver"], t["carrier"]
            ])
        return output.getvalue()


# Global database instance
db = Database()

# ---------------------------------------------------------------------------
# HTTP REST API Handler
# ---------------------------------------------------------------------------

class TransitFlowApiHandler(BaseHTTPRequestHandler):
    """Full RESTful HTTP controller for Transit Flow."""

    server_version = "TransitFlow/2.4.0"

    def _set_cors_headers(self) -> None:
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, Authorization, X-Requested-With")

    def _send_json(self, status_code: int, data: Any) -> None:
        payload = json.dumps(data, indent=2).encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(payload)))
        self._set_cors_headers()
        self.end_headers()
        self.wfile.write(payload)

    def _send_csv(self, filename: str, content: str) -> None:
        payload = content.encode("utf-8")
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "text/csv; charset=utf-8")
        self.send_header("Content-Disposition", f'attachment; filename="{filename}"')
        self.send_header("Content-Length", str(len(payload)))
        self._set_cors_headers()
        self.end_headers()
        self.wfile.write(payload)

    def _parse_body(self) -> Dict[str, Any]:
        content_length = int(self.headers.get("Content-Length", 0))
        if content_length <= 0:
            return {}
        body = self.rfile.read(content_length).decode("utf-8")
        if not body.strip():
            return {}
        try:
            return json.loads(body)
        except json.JSONDecodeError:
            return parse_qs(body)

    def do_OPTIONS(self) -> None:
        self.send_response(HTTPStatus.NO_CONTENT)
        self._set_cors_headers()
        self.end_headers()

    # -----------------------------------------------------------------------
    # GET Endpoints
    # -----------------------------------------------------------------------

    def do_GET(self) -> None:
        parsed_url = urlparse(self.path)
        path = parsed_url.path.rstrip("/")
        query = parse_qs(parsed_url.query)

        def get_param(key: str, default: str = "") -> str:
            return query.get(key, [default])[0]

        # Root / Health check
        if path in ("", "/api", "/api/health"):
            self._send_json(
                HTTPStatus.OK,
                {
                    "service": "Transit Flow Backend API",
                    "organization": "Leteane Industries",
                    "status": "online",
                    "version": "2.4.0",
                    "database": "SQLite (thread-safe)",
                    "endpoints": [
                        "GET  /api/health",
                        "GET  /api/trucks?search=&origin=&destination=&type=&status=&min_capacity=",
                        "GET  /api/trucks/{id}",
                        "POST /api/trucks",
                        "PUT  /api/trucks/{id}",
                        "DELETE /api/trucks/{id}",
                        "POST /api/trucks/{id}/reserve",
                        "GET  /api/bookings",
                        "DELETE /api/bookings/{id}",
                        "GET  /api/routes",
                        "GET  /api/carriers",
                        "GET  /api/analytics",
                        "GET  /api/export/fleet.csv",
                        "POST /api/support",
                        "GET  /api/logs",
                    ],
                },
            )
            return

        # Fleet listing
        if path == "/api/trucks":
            trucks = db.get_trucks(
                search=get_param("search"),
                origin=get_param("origin"),
                destination=get_param("destination"),
                truck_type=get_param("type"),
                status=get_param("status"),
                min_capacity=float(get_param("min_capacity", "0") or "0"),
            )
            self._send_json(HTTPStatus.OK, {"count": len(trucks), "trucks": trucks})
            return

        # Single truck
        match_truck = re.match(r"^/api/trucks/([^/]+)$", path)
        if match_truck:
            truck_id = match_truck.group(1)
            truck = db.get_truck(truck_id)
            if not truck:
                self._send_json(HTTPStatus.NOT_FOUND, {"error": f"Truck {truck_id} not found"})
                return
            self._send_json(HTTPStatus.OK, truck)
            return

        # Bookings listing
        if path == "/api/bookings":
            truck_id = get_param("truck_id")
            bookings = db.get_bookings(truck_id=truck_id or None)
            self._send_json(HTTPStatus.OK, {"count": len(bookings), "bookings": bookings})
            return

        # Routes
        if path == "/api/routes":
            routes = db.get_routes()
            self._send_json(HTTPStatus.OK, {"count": len(routes), "routes": routes})
            return

        # Carriers
        if path == "/api/carriers":
            carriers = db.get_carriers()
            self._send_json(HTTPStatus.OK, {"count": len(carriers), "carriers": carriers})
            return

        # Analytics KPIs
        if path == "/api/analytics":
            analytics = db.get_analytics()
            self._send_json(HTTPStatus.OK, analytics)
            return

        # Export CSV report
        if path == "/api/export/fleet.csv":
            csv_content = db.export_csv()
            self._send_csv("Transit_Flow_fleet_report.csv", csv_content)
            return

        # Audit logs
        if path == "/api/logs":
            with db.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT * FROM audit_logs ORDER BY id DESC LIMIT 50")
                logs = [dict(row) for row in cursor.fetchall()]
            self._send_json(HTTPStatus.OK, {"count": len(logs), "logs": logs})
            return

        # Not found
        self._send_json(HTTPStatus.NOT_FOUND, {"error": f"Route {path} not found"})

    # -----------------------------------------------------------------------
    # POST Endpoints
    # -----------------------------------------------------------------------

    def do_POST(self) -> None:
        parsed_url = urlparse(self.path)
        path = parsed_url.path.rstrip("/")
        body = self._parse_body()

        # Add truck
        if path == "/api/trucks":
            try:
                new_truck = db.create_truck(body)
                self._send_json(HTTPStatus.CREATED, new_truck)
            except Exception as e:
                logger.error(f"Error creating truck: {e}")
                self._send_json(HTTPStatus.BAD_REQUEST, {"error": str(e)})
            return

        # Reserve capacity quick action on truck
        match_reserve = re.match(r"^/api/trucks/([^/]+)/reserve$", path)
        if match_reserve:
            truck_id = match_reserve.group(1)
            try:
                weight = float(body.get("weight_tons", body.get("capacity", 5.0)))
                client = body.get("client_name", "Tintswalo Leteane")
                cargo = body.get("cargo_type", "Standard Consignment")
                reservation = db.reserve_capacity(
                    truck_id=truck_id,
                    weight_tons=weight,
                    client_name=client,
                    cargo_type=cargo,
                )
                self._send_json(HTTPStatus.CREATED, reservation)
            except KeyError as e:
                self._send_json(HTTPStatus.NOT_FOUND, {"error": str(e)})
            except ValueError as e:
                self._send_json(HTTPStatus.CONFLICT, {"error": str(e)})
            except Exception as e:
                self._send_json(HTTPStatus.INTERNAL_SERVER_ERROR, {"error": str(e)})
            return

        # Create booking directly
        if path == "/api/bookings":
            try:
                truck_id = body.get("truck_id")
                if not truck_id:
                    self._send_json(HTTPStatus.BAD_REQUEST, {"error": "truck_id is required."})
                    return
                weight = float(body.get("weight_tons", 1.0))
                client = body.get("client_name", "Tintswalo Leteane")
                cargo = body.get("cargo_type", "General Cargo")
                result = db.reserve_capacity(truck_id, weight, client, cargo)
                self._send_json(HTTPStatus.CREATED, result)
            except (KeyError, ValueError) as e:
                self._send_json(HTTPStatus.BAD_REQUEST, {"error": str(e)})
            except Exception as e:
                self._send_json(HTTPStatus.INTERNAL_SERVER_ERROR, {"error": str(e)})
            return

        # Support ticket submission
        if path == "/api/support":
            try:
                name = body.get("name", "Workspace User")
                email = body.get("email", "admin@leteane-industries.co.za")
                message = body.get("message", "")
                if not message:
                    self._send_json(HTTPStatus.BAD_REQUEST, {"error": "Message is required."})
                    return
                ticket = db.create_support_ticket(name, email, message)
                self._send_json(HTTPStatus.CREATED, ticket)
            except Exception as e:
                self._send_json(HTTPStatus.INTERNAL_SERVER_ERROR, {"error": str(e)})
            return

        # Route & Capacity Matchmaking Engine
        if path == "/api/match":
            origin = body.get("origin", "")
            destination = body.get("destination", "")
            weight = float(body.get("weight_tons", 0.0))
            t_type = body.get("type", "")

            candidates = db.get_trucks(
                origin=origin,
                destination=destination,
                truck_type=t_type,
                status="Available",
                min_capacity=weight,
            )
            self._send_json(
                HTTPStatus.OK,
                {
                    "matches_count": len(candidates),
                    "requested": {"origin": origin, "destination": destination, "weight_tons": weight, "type": t_type},
                    "recommendations": candidates,
                },
            )
            return

        self._send_json(HTTPStatus.NOT_FOUND, {"error": f"Endpoint {path} not found"})

    # -----------------------------------------------------------------------
    # PUT Endpoints
    # -----------------------------------------------------------------------

    def do_PUT(self) -> None:
        parsed_url = urlparse(self.path)
        path = parsed_url.path.rstrip("/")
        body = self._parse_body()

        match_truck = re.match(r"^/api/trucks/([^/]+)$", path)
        if match_truck:
            truck_id = match_truck.group(1)
            updated = db.update_truck(truck_id, body)
            if not updated:
                self._send_json(HTTPStatus.NOT_FOUND, {"error": f"Truck {truck_id} not found."})
                return
            self._send_json(HTTPStatus.OK, updated)
            return

        self._send_json(HTTPStatus.NOT_FOUND, {"error": f"Endpoint {path} not found"})

    # -----------------------------------------------------------------------
    # DELETE Endpoints
    # -----------------------------------------------------------------------

    def do_DELETE(self) -> None:
        parsed_url = urlparse(self.path)
        path = parsed_url.path.rstrip("/")

        # Delete truck
        match_truck = re.match(r"^/api/trucks/([^/]+)$", path)
        if match_truck:
            truck_id = match_truck.group(1)
            if db.delete_truck(truck_id):
                self._send_json(HTTPStatus.OK, {"deleted": True, "truck_id": truck_id})
            else:
                self._send_json(HTTPStatus.NOT_FOUND, {"error": f"Truck {truck_id} not found."})
            return

        # Cancel / Delete booking
        match_booking = re.match(r"^/api/bookings/([^/]+)$", path)
        if match_booking:
            booking_id = match_booking.group(1)
            try:
                res = db.cancel_booking(booking_id)
                self._send_json(HTTPStatus.OK, res)
            except KeyError as e:
                self._send_json(HTTPStatus.NOT_FOUND, {"error": str(e)})
            except ValueError as e:
                self._send_json(HTTPStatus.BAD_REQUEST, {"error": str(e)})
            return

        self._send_json(HTTPStatus.NOT_FOUND, {"error": f"Endpoint {path} not found"})

    def log_message(self, format: str, *args: Any) -> None:
        logger.info("%s - - [%s] %s" % (self.client_address[0], self.log_date_time_string(), format % args))


# ---------------------------------------------------------------------------
# Self-Test Suite
# ---------------------------------------------------------------------------

def run_tests() -> bool:
    """Executes a thorough suite of automated tests for all business logic."""
    print("=" * 60)
    print("Running Transit Flow Backend Self-Test Suite")
    print("=" * 60)
    test_db_path = ":memory:"
    test_db = Database(db_path=test_db_path)

    try:
        # Test 1: Seed data initialization
        trucks = test_db.get_trucks()
        assert len(trucks) >= 6, f"Expected at least 6 seed trucks, got {len(trucks)}"
        print("[PASS] Test 1: Seed trucks initialized successfully.")

        # Test 2: Search & filtering
        j_trucks = test_db.get_trucks(origin="Johannesburg")
        assert len(j_trucks) >= 3, f"Expected >= 3 trucks from Johannesburg, got {len(j_trucks)}"
        print("[PASS] Test 2: Multi-parameter filtering works.")

        # Test 3: Add new truck
        new_truck = test_db.create_truck({
            "name": "Isuzu Giga 500",
            "plate": "GP 9999",
            "type": "Curtain side",
            "origin": "Pretoria",
            "destination": "Durban",
            "capacity": 20.0,
            "total": 20.0,
        })
        assert new_truck["id"].startswith("TF"), "Generated ID must start with TF"
        print(f"[PASS] Test 3: Truck creation works ({new_truck['id']}).")

        # Test 4: Capacity Reservation
        truck_id = "TF1024"
        initial = test_db.get_truck(truck_id)
        assert initial is not None
        initial_cap = initial["capacity"]
        reserve_tons = 10.0
        booking = test_db.reserve_capacity(truck_id, reserve_tons, "Test Shipper", "Machinery")
        assert booking["remaining_capacity"] == initial_cap - reserve_tons
        assert booking["total_price_zar"] > 0
        print(f"[PASS] Test 4: Capacity reserved successfully. Remaining: {booking['remaining_capacity']} tons.")

        # Test 5: Overbooking rejection
        try:
            test_db.reserve_capacity(truck_id, 9999.0)
            assert False, "Overbooking should have raised a ValueError"
        except ValueError:
            print("[PASS] Test 5: Overbooking prevention correctly raised ValueError.")

        # Test 6: Booking cancellation restores capacity
        cancel_res = test_db.cancel_booking(booking["booking_id"])
        restored = test_db.get_truck(truck_id)
        assert restored is not None
        assert restored["capacity"] == initial_cap
        print("[PASS] Test 6: Booking cancellation restored capacity exactly.")

        # Test 7: Analytics calculation
        analytics = test_db.get_analytics()
        assert "fleet_utilization_pct" in analytics
        assert "available_capacity_tons" in analytics
        assert analytics["total_trucks"] >= 7
        print(f"[PASS] Test 7: Analytics calculated (Utilization: {analytics['fleet_utilization_pct']}%).")

        # Test 8: CSV Export
        csv_data = test_db.export_csv()
        assert "Truck ID,Truck Model" in csv_data
        print("[PASS] Test 8: Fleet CSV report generated.")

        # Test 9: HTTP REST API Endpoints test
        import urllib.request
        server = ThreadingHTTPServer(("127.0.0.1", 0), TransitFlowApiHandler)
        test_port = server.server_address[1]
        srv_thread = threading.Thread(target=server.serve_forever, daemon=True)
        srv_thread.start()

        try:
            base_url = f"http://127.0.0.1:{test_port}"
            # GET /api/health
            with urllib.request.urlopen(f"{base_url}/api/health") as resp:
                assert resp.status == 200
                health_data = json.loads(resp.read().decode("utf-8"))
                assert health_data["status"] == "online"
            print("[PASS] Test 9a: GET /api/health HTTP 200 OK.")

            # GET /api/trucks
            with urllib.request.urlopen(f"{base_url}/api/trucks") as resp:
                assert resp.status == 200
                trucks_data = json.loads(resp.read().decode("utf-8"))
                assert trucks_data["count"] >= 6
            print("[PASS] Test 9b: GET /api/trucks HTTP 200 OK.")

            # POST /api/trucks/TF1024/reserve
            req = urllib.request.Request(
                f"{base_url}/api/trucks/TF1024/reserve",
                data=json.dumps({"weight_tons": 2.0, "client_name": "API Client"}).encode("utf-8"),
                headers={"Content-Type": "application/json"},
                method="POST"
            )
            with urllib.request.urlopen(req) as resp:
                assert resp.status == 201
                res_data = json.loads(resp.read().decode("utf-8"))
                assert res_data["status"] == "Reserved"
            print("[PASS] Test 9c: POST /api/trucks/TF1024/reserve HTTP 201 Created.")

            # GET /api/analytics
            with urllib.request.urlopen(f"{base_url}/api/analytics") as resp:
                assert resp.status == 200
            print("[PASS] Test 9d: GET /api/analytics HTTP 200 OK.")
        finally:
            server.shutdown()
            server.server_close()

        print("=" * 60)
        print("ALL TESTS PASSED! Backend is healthy and production-ready.")
        print("=" * 60)
        return True
    except Exception as e:
        print(f"[FAIL] Test suite failed: {e}")
        import traceback
        traceback.print_exc()
        return False


# ---------------------------------------------------------------------------
# Interactive CLI
# ---------------------------------------------------------------------------

def run_cli() -> None:
    """Provides an interactive terminal interface for freight dispatchers."""
    while True:
        print("\n" + "=" * 55)
        print("       TRANSIT FLOW - LETEANE INDUSTRIES")
        print("         Freight Logistics Backend CLI")
        print("=" * 55)
        print(" 1. View all available trucks")
        print(" 2. Search trucks by route/type")
        print(" 3. Add a new truck to fleet")
        print(" 4. Reserve truck capacity")
        print(" 5. View all bookings")
        print(" 6. Cancel a booking")
        print(" 7. View fleet analytics & KPIs")
        print(" 8. Export fleet report (CSV)")
        print(" 9. Run automated self-test suite")
        print(" 10. Start REST API Server")
        print(" 0. Exit")
        print("-" * 55)

        choice = input("Enter option [0-10]: ").strip()

        if choice == "1":
            trucks = db.get_trucks()
            print(f"\n{'ID':<8} {'Model':<16} {'Origin -> Destination':<30} {'Capacity':<12} {'Status'}")
            print("-" * 75)
            for t in trucks:
                print(f"{t['id']:<8} {t['name']:<16} {t['origin'] + ' -> ' + t['destination']:<30} {str(t['capacity']) + '/' + str(t['total']) + 't':<12} {t['status']}")

        elif choice == "2":
            origin = input("Origin (or Enter for any): ").strip()
            destination = input("Destination (or Enter for any): ").strip()
            t_type = input("Type (Curtain side/Refrigerated/Flatbed or Enter): ").strip()
            trucks = db.get_trucks(origin=origin, destination=destination, truck_type=t_type)
            print(f"\nFound {len(trucks)} matching truck(s):")
            for t in trucks:
                print(f"[{t['id']}] {t['name']} ({t['type']}) | {t['origin']} -> {t['destination']} | {t['capacity']}t available | Status: {t['status']}")

        elif choice == "3":
            name = input("Truck Model (e.g. Scania R500): ").strip() or "Volvo FH 540"
            plate = input("Plate (e.g. GP 1234): ").strip() or "GP 1234"
            origin = input("Origin City: ").strip() or "Johannesburg"
            destination = input("Destination City: ").strip() or "Cape Town"
            t_type = input("Type (Curtain side/Refrigerated/Flatbed): ").strip() or "Curtain side"
            capacity = float(input("Capacity in tons: ").strip() or "30")
            truck = db.create_truck({
                "name": name, "plate": plate, "origin": origin,
                "destination": destination, "type": t_type, "capacity": capacity, "total": capacity
            })
            print(f"\n[OK] Created truck {truck['id']} successfully!")

        elif choice == "4":
            truck_id = input("Truck ID (e.g. TF1024): ").strip().upper()
            tons = float(input("Tons to reserve: ").strip() or "5")
            client = input("Shipper/Client name: ").strip() or "Tintswalo Leteane"
            try:
                res = db.reserve_capacity(truck_id, tons, client)
                print(f"\n[OK] Capacity reserved! Booking ID: {res['booking_id']}, Cost: R{res['total_price_zar']:,}, Remaining: {res['remaining_capacity']}t")
            except Exception as e:
                print(f"\n[ERROR] Reservation failed: {e}")

        elif choice == "5":
            bookings = db.get_bookings()
            print(f"\n{'Booking ID':<22} {'Truck':<8} {'Client':<16} {'Weight':<8} {'Price (ZAR)':<12} {'Status'}")
            print("-" * 75)
            for b in bookings:
                print(f"{b['id']:<22} {b['truck_id']:<8} {b['client_name']:<16} {str(b['weight_tons']) + 't':<8} {'R' + str(b['total_price_zar']):<12} {b['status']}")

        elif choice == "6":
            booking_id = input("Booking ID to cancel: ").strip()
            try:
                res = db.cancel_booking(booking_id)
                print(f"\n[OK] Booking cancelled. Restored {res['restored_tons']} tons.")
            except Exception as e:
                print(f"\n[ERROR] Cancellation failed: {e}")

        elif choice == "7":
            analytics = db.get_analytics()
            print("\n--- FLEET ANALYTICS OVERVIEW ---")
            print(f"Total Fleet:          {analytics['total_trucks']} trucks")
            print(f"Available Trucks:     {analytics['available_trucks']} trucks")
            print(f"Available Capacity:   {analytics['available_capacity_tons']} tons")
            print(f"Total Capacity:       {analytics['total_capacity_tons']} tons")
            print(f"Fleet Utilization:    {analytics['fleet_utilization_pct']}%")
            print(f"Active Corridors:     {analytics['active_routes_count']}")
            print(f"Active Reservations:  {analytics['active_bookings_count']}")

        elif choice == "8":
            csv_text = db.export_csv()
            filename = "fleet_export.csv"
            with open(filename, "w", encoding="utf-8") as f:
                f.write(csv_text)
            print(f"\n[OK] Fleet report saved to {os.path.abspath(filename)}")

        elif choice == "9":
            run_tests()

        elif choice == "10":
            start_server()
            break

        elif choice == "0":
            print("\nExiting Transit Flow. Goodbye!")
            sys.exit(0)


# ---------------------------------------------------------------------------
# Server Startup
# ---------------------------------------------------------------------------

def start_server(host: str = DEFAULT_HOST, port: int = DEFAULT_PORT) -> None:
    """Launches the multi-threaded HTTP server."""
    server_address = (host, port)
    httpd = ThreadingHTTPServer(server_address, TransitFlowApiHandler)

    display_host = "localhost" if host in ("0.0.0.0", "") else host
    print("\n" + "=" * 65)
    print("  TRANSIT FLOW - LETEANE INDUSTRIES FREIGHT LOGISTICS API")
    print("=" * 65)
    print(f"  Server URL:      http://{display_host}:{port}")
    print(f"  Health Check:    http://{display_host}:{port}/api/health")
    print(f"  Trucks API:      http://{display_host}:{port}/api/trucks")
    print(f"  Analytics API:   http://{display_host}:{port}/api/analytics")
    print(f"  CSV Export:      http://{display_host}:{port}/api/export/fleet.csv")
    print(f"  Database:        {os.path.abspath(db.db_path)}")
    print("=" * 65)
    print("  Press Ctrl+C to stop the server.\n")

    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down Transit Flow server...")
        httpd.server_close()
        print("Server stopped cleanly.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Transit Flow Freight Logistics Backend Service")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT, help="Port to listen on (default: 8000)")
    parser.add_argument("--host", type=str, default=DEFAULT_HOST, help="Host to bind to (default: 0.0.0.0)")
    parser.add_argument("--cli", action="store_true", help="Launch interactive CLI menu")
    parser.add_argument("--test", action="store_true", help="Run automated test suite and exit")
    args = parser.parse_args()

    if args.test:
        success = run_tests()
        sys.exit(0 if success else 1)
    elif args.cli:
        run_cli()
    else:
        start_server(host=args.host, port=args.port)


if __name__ == "__main__":
    main()
