from __future__ import annotations

import hashlib
import hmac
import json
import os
import secrets
import sqlite3
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from fastapi import Depends, FastAPI, Header, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "static"
DB_PATH = Path(os.environ.get("FINWIZ_DB", BASE_DIR / "finwiz_simulation.db"))
PORT = int(os.environ.get("PORT", "8000"))

OWN_CAPITAL = 1_000_000
LOAN_PRINCIPAL = 5_000_000
TOTAL_CAPITAL = OWN_CAPITAL + LOAN_PRINCIPAL
DEFAULT_PREMIUM_RATE = 1.0  # competition rule: % of insured amount per year
DEFAULT_COVER = 5_000_000
DEFAULT_YEARS = 5
PASSWORD_ITERATIONS = 260_000

app = FastAPI(title="FinWiz Live Investment Simulation", version="1.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=30, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def hash_password(password: str) -> str:
    salt = secrets.token_bytes(16)
    digest = hashlib.pbkdf2_hmac("sha256", password.encode(), salt, PASSWORD_ITERATIONS)
    return f"{salt.hex()}:{digest.hex()}"


def verify_password(password: str, stored: str) -> bool:
    try:
        salt_hex, digest_hex = stored.split(":", 1)
        digest = hashlib.pbkdf2_hmac("sha256", password.encode(), bytes.fromhex(salt_hex), PASSWORD_ITERATIONS)
        return hmac.compare_digest(digest.hex(), digest_hex)
    except Exception:
        return False


def audit(conn: sqlite3.Connection, actor: str, action: str, detail: str) -> None:
    conn.execute(
        "INSERT INTO audit_logs(actor, action, detail, created_at) VALUES(?,?,?,?)",
        (actor, action, detail, now_iso()),
    )


class LiveHub:
    def __init__(self) -> None:
        self.connections: set[WebSocket] = set()

    async def connect(self, websocket: WebSocket) -> None:
        await websocket.accept()
        self.connections.add(websocket)

    def disconnect(self, websocket: WebSocket) -> None:
        self.connections.discard(websocket)

    async def broadcast(self, event: str = "refresh") -> None:
        stale: list[WebSocket] = []
        payload = json.dumps({"event": event, "at": now_iso()})
        for ws in list(self.connections):
            try:
                await ws.send_text(payload)
            except Exception:
                stale.append(ws)
        for ws in stale:
            self.disconnect(ws)


hub = LiveHub()


EQUITIES = [
    ("ADANIENT", "Adani Enterprises", "Diversified"),
    ("ADANIPORTS", "Adani Ports", "Infrastructure"),
    ("APOLLOHOSP", "Apollo Hospitals", "Healthcare"),
    ("ASIANPAINT", "Asian Paints", "Consumer"),
    ("AXISBANK", "Axis Bank", "Banking"),
    ("BAJAJ-AUTO", "Bajaj Auto", "Automobile"),
    ("BAJFINANCE", "Bajaj Finance", "Financial Services"),
    ("BAJAJFINSV", "Bajaj Finserv", "Financial Services"),
    ("BEL", "Bharat Electronics", "Defence"),
    ("BHARTIARTL", "Bharti Airtel", "Telecom"),
    ("CIPLA", "Cipla", "Pharmaceuticals"),
    ("COALINDIA", "Coal India", "Energy"),
    ("DRREDDY", "Dr. Reddy's Laboratories", "Pharmaceuticals"),
    ("EICHERMOT", "Eicher Motors", "Automobile"),
    ("GRASIM", "Grasim Industries", "Materials"),
    ("HCLTECH", "HCL Technologies", "IT"),
    ("HDFCBANK", "HDFC Bank", "Banking"),
    ("HDFCLIFE", "HDFC Life", "Insurance"),
    ("HEROMOTOCO", "Hero MotoCorp", "Automobile"),
    ("HINDALCO", "Hindalco Industries", "Metals"),
    ("HINDUNILVR", "Hindustan Unilever", "FMCG"),
    ("ICICIBANK", "ICICI Bank", "Banking"),
    ("ITC", "ITC", "FMCG"),
    ("INFY", "Infosys", "IT"),
    ("JSWSTEEL", "JSW Steel", "Metals"),
    ("JIOFIN", "Jio Financial Services", "Financial Services"),
    ("KOTAKBANK", "Kotak Mahindra Bank", "Banking"),
    ("LT", "Larsen & Toubro", "Infrastructure"),
    ("M&M", "Mahindra & Mahindra", "Automobile"),
    ("MARUTI", "Maruti Suzuki", "Automobile"),
    ("NESTLEIND", "Nestle India", "FMCG"),
    ("NTPC", "NTPC", "Energy"),
    ("ONGC", "ONGC", "Energy"),
    ("POWERGRID", "Power Grid Corporation", "Utilities"),
    ("RELIANCE", "Reliance Industries", "Energy"),
    ("SBIN", "State Bank of India", "Banking"),
    ("SBILIFE", "SBI Life Insurance", "Insurance"),
    ("SHRIRAMFIN", "Shriram Finance", "Financial Services"),
    ("SUNPHARMA", "Sun Pharma", "Pharmaceuticals"),
    ("TATACONSUM", "Tata Consumer Products", "FMCG"),
    ("TATAMOTORS", "Tata Motors", "Automobile"),
    ("TATASTEEL", "Tata Steel", "Metals"),
    ("TCS", "Tata Consultancy Services", "IT"),
    ("TECHM", "Tech Mahindra", "IT"),
    ("TITAN", "Titan Company", "Consumer"),
    ("TRENT", "Trent", "Retail"),
    ("ULTRACEMCO", "UltraTech Cement", "Materials"),
    ("WIPRO", "Wipro", "IT"),
    ("BPCL", "Bharat Petroleum", "Energy"),
    ("INDUSINDBK", "IndusInd Bank", "Banking"),
]


def init_db() -> None:
    conn = db()
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS users(
      id TEXT PRIMARY KEY, username TEXT UNIQUE NOT NULL, password_hash TEXT NOT NULL,
      role TEXT NOT NULL CHECK(role IN ('admin','player')), team_id TEXT, created_at TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS sessions(
      token TEXT PRIMARY KEY, user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
      expires_at TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS teams(
      id TEXT PRIMARY KEY, code TEXT UNIQUE NOT NULL, name TEXT NOT NULL, username TEXT UNIQUE NOT NULL,
      status TEXT NOT NULL DEFAULT 'active', created_at TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS accounts(
      team_id TEXT PRIMARY KEY REFERENCES teams(id) ON DELETE CASCADE,
      cash REAL NOT NULL, own_capital REAL NOT NULL, loan_principal REAL NOT NULL,
      loan_balance REAL NOT NULL, insurance_cover REAL NOT NULL, premium_due REAL NOT NULL DEFAULT 0,
      total_premium_paid REAL NOT NULL DEFAULT 0, settled INTEGER NOT NULL DEFAULT 0
    );
    CREATE TABLE IF NOT EXISTS instruments(
      id TEXT PRIMARY KEY, symbol TEXT UNIQUE NOT NULL, name TEXT NOT NULL,
      asset_class TEXT NOT NULL, sector TEXT NOT NULL, unit TEXT NOT NULL,
      price REAL NOT NULL, pending_return_pct REAL NOT NULL DEFAULT 0,
      pending_price REAL, active INTEGER NOT NULL DEFAULT 1
    );
    CREATE TABLE IF NOT EXISTS holdings(
      team_id TEXT NOT NULL REFERENCES teams(id) ON DELETE CASCADE,
      instrument_id TEXT NOT NULL REFERENCES instruments(id), quantity REAL NOT NULL,
      avg_cost REAL NOT NULL, PRIMARY KEY(team_id,instrument_id)
    );
    CREATE TABLE IF NOT EXISTS transactions(
      id TEXT PRIMARY KEY, team_id TEXT NOT NULL REFERENCES teams(id), instrument_id TEXT NOT NULL REFERENCES instruments(id),
      side TEXT NOT NULL, quantity REAL NOT NULL, price REAL NOT NULL, gross REAL NOT NULL,
      created_at TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS stages(
      id TEXT PRIMARY KEY, number INTEGER UNIQUE NOT NULL, title TEXT NOT NULL,
      status TEXT NOT NULL, opened_at TEXT, closed_at TEXT, published_at TEXT
    );
    CREATE TABLE IF NOT EXISTS market_updates(
      id TEXT PRIMARY KEY, stage_number INTEGER NOT NULL, scenario_name TEXT NOT NULL,
      update_json TEXT NOT NULL, created_at TEXT NOT NULL, actor TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS price_history(
      id TEXT PRIMARY KEY, instrument_id TEXT NOT NULL REFERENCES instruments(id), stage_number INTEGER NOT NULL,
      old_price REAL NOT NULL, new_price REAL NOT NULL, return_pct REAL NOT NULL, created_at TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS announcements(
      id TEXT PRIMARY KEY, title TEXT NOT NULL, body TEXT NOT NULL, created_at TEXT NOT NULL, actor TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS audit_logs(
      id INTEGER PRIMARY KEY AUTOINCREMENT, actor TEXT NOT NULL, action TEXT NOT NULL,
      detail TEXT NOT NULL, created_at TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS settings(key TEXT PRIMARY KEY, value TEXT NOT NULL);
    CREATE TABLE IF NOT EXISTS settlements(
      team_id TEXT PRIMARY KEY REFERENCES teams(id), gross_portfolio REAL NOT NULL,
      premiums_due REAL NOT NULL, net_assets REAL NOT NULL, loan_repaid REAL NOT NULL,
      insurer_payout REAL NOT NULL, residual_equity REAL NOT NULL, lender_shortfall REAL NOT NULL,
      created_at TEXT NOT NULL
    );
    """)

    defaults = {
        "premium_rate_pct": str(DEFAULT_PREMIUM_RATE),
        "insurance_cover": str(DEFAULT_COVER),
        "own_capital": str(OWN_CAPITAL),
        "loan_principal": str(LOAN_PRINCIPAL),
        "max_years": str(DEFAULT_YEARS),
        "leaderboard_visible": "1",
        "competition_name": "FinWiz Investment Simulation",
    }
    for key, value in defaults.items():
        conn.execute("INSERT OR IGNORE INTO settings(key,value) VALUES(?,?)", (key, value))

    if conn.execute("SELECT COUNT(*) n FROM users WHERE role='admin'").fetchone()["n"] == 0:
        conn.execute(
            "INSERT INTO users(id,username,password_hash,role,created_at) VALUES(?,?,?,?,?)",
            (str(uuid.uuid4()), "admin", hash_password("FinWiz@2026!"), "admin", now_iso()),
        )

    if conn.execute("SELECT COUNT(*) n FROM instruments").fetchone()["n"] == 0:
        for ticker, name, sector in EQUITIES:
            conn.execute(
                "INSERT INTO instruments(id,symbol,name,asset_class,sector,unit,price) VALUES(?,?,?,?,?,?,?)",
                (str(uuid.uuid4()), ticker, name, "Equity", sector, "units", 100.0),
            )
        other_assets = [
            ("GSEC2030", "Government Security 2030", "Bond", "Government", "face-value units", 100.0),
            ("GSEC2035", "Government Security 2035", "Bond", "Government", "face-value units", 100.0),
            ("AAA_CORP", "AAA Corporate Bond Basket", "Bond", "Corporate", "face-value units", 100.0),
            ("GOLD", "Gold", "Commodity", "Precious Metals", "units", 100.0),
            ("SILVER", "Silver", "Commodity", "Precious Metals", "units", 100.0),
        ]
        for symbol, name, asset_class, sector, unit, price in other_assets:
            conn.execute(
                "INSERT INTO instruments(id,symbol,name,asset_class,sector,unit,price) VALUES(?,?,?,?,?,?,?)",
                (str(uuid.uuid4()), symbol, name, asset_class, sector, unit, price),
            )

    if conn.execute("SELECT COUNT(*) n FROM stages").fetchone()["n"] == 0:
        conn.execute(
            "INSERT INTO stages(id,number,title,status) VALUES(?,?,?,?)",
            (str(uuid.uuid4()), 1, "Year 1", "closed"),
        )
    conn.commit()
    conn.close()


init_db()


def setting(conn: sqlite3.Connection, key: str) -> str:
    row = conn.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
    return row["value"] if row else ""


def set_setting(conn: sqlite3.Connection, key: str, value: Any) -> None:
    conn.execute("INSERT INTO settings(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (key, str(value)))


def get_user(token: str | None) -> sqlite3.Row:
    if not token:
        raise HTTPException(401, "Please log in")
    conn = db()
    row = conn.execute(
        "SELECT u.* FROM sessions s JOIN users u ON u.id=s.user_id WHERE s.token=? AND s.expires_at>?",
        (token, now_iso()),
    ).fetchone()
    conn.close()
    if not row:
        raise HTTPException(401, "Session expired. Please log in again.")
    return row


def bearer_user(authorization: str | None = Header(default=None)) -> sqlite3.Row:
    token = authorization.removeprefix("Bearer ") if authorization else None
    return get_user(token)


def admin_user(user: sqlite3.Row = Depends(bearer_user)) -> sqlite3.Row:
    if user["role"] != "admin":
        raise HTTPException(403, "Admin access required")
    return user


def player_user(user: sqlite3.Row = Depends(bearer_user)) -> sqlite3.Row:
    if user["role"] != "player" or not user["team_id"]:
        raise HTTPException(403, "Team login required")
    return user


def money(value: float) -> float:
    return round(float(value), 2)


def portfolio_snapshot(conn: sqlite3.Connection, team_id: str) -> dict[str, Any]:
    account = conn.execute("SELECT * FROM accounts WHERE team_id=?", (team_id,)).fetchone()
    team = conn.execute("SELECT * FROM teams WHERE id=?", (team_id,)).fetchone()
    rows = conn.execute("""
      SELECT h.instrument_id,h.quantity,h.avg_cost,i.symbol,i.name,i.asset_class,i.sector,i.unit,i.price
      FROM holdings h JOIN instruments i ON i.id=h.instrument_id
      WHERE h.team_id=? AND h.quantity>0 ORDER BY i.asset_class,i.name
    """, (team_id,)).fetchall()
    holdings = []
    invested_value = 0.0
    cost_basis = 0.0
    classes: dict[str, float] = {}
    for r in rows:
        current = r["quantity"] * r["price"]
        cost = r["quantity"] * r["avg_cost"]
        invested_value += current
        cost_basis += cost
        classes[r["asset_class"]] = classes.get(r["asset_class"], 0.0) + current
        holdings.append({
            "instrument_id": r["instrument_id"], "symbol": r["symbol"], "name": r["name"],
            "asset_class": r["asset_class"], "sector": r["sector"], "unit": r["unit"],
            "quantity": money(r["quantity"]), "avg_cost": money(r["avg_cost"]),
            "current_price": money(r["price"]), "market_value": money(current),
            "pnl": money(current-cost), "return_pct": round(((current/cost)-1)*100, 2) if cost else 0,
        })
    cash = float(account["cash"])
    gross_value = cash + invested_value
    net_value = gross_value - float(account["premium_due"])
    total_capital = float(account["own_capital"] + account["loan_principal"])
    all_assets = classes.copy()
    all_assets["Cash"] = cash
    tx = conn.execute("SELECT COUNT(*) n FROM transactions WHERE team_id=?", (team_id,)).fetchone()["n"]
    stage = conn.execute("SELECT number,title,status FROM stages ORDER BY number DESC LIMIT 1").fetchone()
    return {
        "team": {"id": team["id"], "code": team["code"], "name": team["name"], "username": team["username"]},
        "account": {
            "cash": money(cash), "own_capital": money(account["own_capital"]),
            "loan_principal": money(account["loan_principal"]), "loan_balance": money(account["loan_balance"]),
            "insurance_cover": money(account["insurance_cover"]), "premium_due": money(account["premium_due"]),
            "total_premium_paid": money(account["total_premium_paid"]), "settled": bool(account["settled"]),
            "initial_capital": money(total_capital), "gross_portfolio_value": money(gross_value),
            "portfolio_value": money(net_value), "invested_value": money(invested_value),
            "cost_basis": money(cost_basis), "unrealised_pnl": money(invested_value-cost_basis),
            "return_pct_vs_initial": round((net_value/total_capital-1)*100, 2) if total_capital else 0,
            "transactions_count": tx,
        },
        "holdings": holdings,
        "allocation": [{"asset_class": k, "value": money(v)} for k,v in all_assets.items()],
        "stage": dict(stage) if stage else None,
    }


class LoginBody(BaseModel):
    username: str = Field(min_length=2, max_length=50)
    password: str = Field(min_length=6, max_length=200)


class ChangePasswordBody(BaseModel):
    current_password: str
    new_password: str = Field(min_length=10, max_length=200)


class CreateTeamBody(BaseModel):
    name: str = Field(min_length=2, max_length=80)
    username: str = Field(min_length=3, max_length=40, pattern=r"^[A-Za-z0-9_.-]+$")
    password: str = Field(min_length=8, max_length=100)


class TradeBody(BaseModel):
    instrument_id: str
    side: str
    quantity: float = Field(gt=0, le=1_000_000_000)


class InstrumentEditBody(BaseModel):
    return_pct: float | None = Field(default=None, ge=-100, le=1000)
    direct_price: float | None = Field(default=None, gt=0, le=1_000_000_000)
    active: bool | None = None


class CreateInstrumentBody(BaseModel):
    symbol: str = Field(min_length=2, max_length=24, pattern=r"^[A-Za-z0-9._-]+$")
    name: str = Field(min_length=2, max_length=100)
    asset_class: str = Field(pattern=r"^(Equity|Bond|Commodity|Other)$")
    sector: str = Field(min_length=2, max_length=60)
    unit: str = Field(min_length=1, max_length=40)
    initial_price: float = Field(gt=0, le=1_000_000_000)


class ScenarioBody(BaseModel):
    name: str = Field(min_length=2, max_length=100)
    impacts: dict[str, float]


class SettingsBody(BaseModel):
    premium_rate_pct: float = Field(ge=0, le=100)
    insurance_cover: float = Field(gt=0, le=1_000_000_000)
    max_years: int = Field(ge=1, le=10)
    leaderboard_visible: bool


class AnnouncementBody(BaseModel):
    title: str = Field(min_length=2, max_length=100)
    body: str = Field(min_length=2, max_length=2000)


@app.get("/")
def home() -> FileResponse:
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/player")
def player_page() -> FileResponse:
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/admin")
def admin_page() -> FileResponse:
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/api/health")
def health() -> dict[str, Any]:
    return {"status": "ok", "application": "FinWiz Live Simulation", "version": "1.0.0"}


@app.post("/api/login")
def login(body: LoginBody) -> dict[str, Any]:
    conn = db()
    user = conn.execute("SELECT * FROM users WHERE username=?", (body.username.strip(),)).fetchone()
    if not user or not verify_password(body.password, user["password_hash"]):
        conn.close()
        raise HTTPException(401, "Incorrect username or password")
    token = secrets.token_urlsafe(36)
    expires = (datetime.now(timezone.utc) + timedelta(hours=18)).isoformat()
    conn.execute("INSERT INTO sessions(token,user_id,expires_at) VALUES(?,?,?)", (token, user["id"], expires))
    conn.commit()
    role = user["role"]
    conn.close()
    return {"token": token, "role": role, "username": user["username"], "redirect": "/admin" if role == "admin" else "/player"}


@app.get("/api/me")
def me(user: sqlite3.Row = Depends(bearer_user)) -> dict[str, Any]:
    return {"username": user["username"], "role": user["role"], "team_id": user["team_id"]}


@app.post("/api/logout")
def logout(authorization: str | None = Header(default=None)) -> dict[str, bool]:
    token = authorization.removeprefix("Bearer ") if authorization else None
    if token:
        conn = db()
        conn.execute("DELETE FROM sessions WHERE token=?", (token,))
        conn.commit()
        conn.close()
    return {"ok": True}


@app.post("/api/change-password")
def change_password(body: ChangePasswordBody, user: sqlite3.Row = Depends(bearer_user)) -> dict[str, bool]:
    if not verify_password(body.current_password, user["password_hash"]):
        raise HTTPException(400, "Current password is incorrect")
    conn = db()
    conn.execute("UPDATE users SET password_hash=? WHERE id=?", (hash_password(body.new_password), user["id"]))
    audit(conn, user["username"], "password_changed", "User changed their password")
    conn.commit()
    conn.close()
    return {"ok": True}


@app.get("/api/player/dashboard")
def player_dashboard(user: sqlite3.Row = Depends(player_user)) -> dict[str, Any]:
    conn = db()
    snap = portfolio_snapshot(conn, user["team_id"])
    instruments = conn.execute("SELECT id,symbol,name,asset_class,sector,unit,price,active FROM instruments WHERE active=1 ORDER BY asset_class,name").fetchall()
    tx = conn.execute("""
      SELECT t.id,t.side,t.quantity,t.price,t.gross,t.created_at,i.symbol,i.name
      FROM transactions t JOIN instruments i ON i.id=t.instrument_id
      WHERE t.team_id=? ORDER BY t.created_at DESC LIMIT 100
    """, (user["team_id"],)).fetchall()
    anns = conn.execute("SELECT title,body,created_at FROM announcements ORDER BY created_at DESC LIMIT 15").fetchall()
    stage = conn.execute("SELECT * FROM stages ORDER BY number DESC LIMIT 1").fetchone()
    leaderboard_on = setting(conn, "leaderboard_visible") == "1"
    updates = conn.execute("SELECT scenario_name,stage_number,created_at FROM market_updates ORDER BY created_at DESC LIMIT 10").fetchall()
    snap.update({
        "instruments": [dict(x) for x in instruments],
        "transactions": [dict(x) for x in tx],
        "announcements": [dict(x) for x in anns],
        "market_updates": [dict(x) for x in updates],
        "leaderboard_visible": leaderboard_on,
        "stage": {"number": stage["number"], "title": stage["title"], "status": stage["status"], "opened_at": stage["opened_at"], "closed_at": stage["closed_at"], "published_at": stage["published_at"]} if stage else None,
    })
    conn.close()
    return snap


@app.post("/api/player/trade")
async def trade(body: TradeBody, user: sqlite3.Row = Depends(player_user)) -> dict[str, Any]:
    side = body.side.lower()
    if side not in ("buy", "sell"):
        raise HTTPException(400, "Side must be buy or sell")
    conn = db()
    conn.execute("BEGIN IMMEDIATE")
    stage = conn.execute("SELECT * FROM stages ORDER BY number DESC LIMIT 1").fetchone()
    if not stage or stage["status"] != "open":
        conn.rollback(); conn.close()
        raise HTTPException(409, "Trading is currently closed")
    account = conn.execute("SELECT * FROM accounts WHERE team_id=?", (user["team_id"],)).fetchone()
    if account["settled"]:
        conn.rollback(); conn.close()
        raise HTTPException(409, "This team's account has been settled")
    if side == "buy" and account["premium_due"] > 0.005:
        conn.rollback(); conn.close()
        raise HTTPException(409, "Unpaid insurance premium is outstanding. Sell assets to clear it before buying.")
    inst = conn.execute("SELECT * FROM instruments WHERE id=? AND active=1", (body.instrument_id,)).fetchone()
    if not inst:
        conn.rollback(); conn.close()
        raise HTTPException(404, "Instrument not found or unavailable")
    qty = round(float(body.quantity), 4)
    price = float(inst["price"])
    gross = money(qty * price)
    holding = conn.execute("SELECT * FROM holdings WHERE team_id=? AND instrument_id=?", (user["team_id"], inst["id"])).fetchone()
    if side == "buy":
        if gross > account["cash"] + 0.005:
            conn.rollback(); conn.close()
            raise HTTPException(400, f"Insufficient cash. Available: ₹{account['cash']:,.2f}")
        old_qty = float(holding["quantity"]) if holding else 0.0
        old_cost = float(holding["avg_cost"]) if holding else 0.0
        new_qty = old_qty + qty
        new_avg = ((old_qty * old_cost) + gross) / new_qty
        conn.execute("INSERT INTO holdings(team_id,instrument_id,quantity,avg_cost) VALUES(?,?,?,?) ON CONFLICT(team_id,instrument_id) DO UPDATE SET quantity=excluded.quantity,avg_cost=excluded.avg_cost", (user["team_id"], inst["id"], new_qty, new_avg))
        conn.execute("UPDATE accounts SET cash=cash-? WHERE team_id=?", (gross, user["team_id"]))
    else:
        if not holding or float(holding["quantity"]) + 0.00001 < qty:
            conn.rollback(); conn.close()
            raise HTTPException(400, "You do not hold enough units to sell")
        remaining = float(holding["quantity"]) - qty
        if remaining < 0.00001:
            conn.execute("DELETE FROM holdings WHERE team_id=? AND instrument_id=?", (user["team_id"], inst["id"]))
        else:
            conn.execute("UPDATE holdings SET quantity=? WHERE team_id=? AND instrument_id=?", (remaining, user["team_id"], inst["id"]))
        # Proceeds first clear any unpaid annual insurance premium; remainder becomes cash.
        due = float(account["premium_due"])
        applied = min(due, gross)
        conn.execute("UPDATE accounts SET premium_due=premium_due-?,cash=cash+?,total_premium_paid=total_premium_paid+? WHERE team_id=?", (applied, gross-applied, applied, user["team_id"]))
    conn.execute("INSERT INTO transactions(id,team_id,instrument_id,side,quantity,price,gross,created_at) VALUES(?,?,?,?,?,?,?,?)", (str(uuid.uuid4()), user["team_id"], inst["id"], side.upper(), qty, price, gross, now_iso()))
    audit(conn, user["username"], "trade", f"{side.upper()} {qty} {inst['symbol']} at {price:.4f}; gross ₹{gross:.2f}")
    conn.commit()
    snap = portfolio_snapshot(conn, user["team_id"])
    conn.close()
    await hub.broadcast("trade")
    return {"ok": True, "message": f"{side.title()} order executed", "portfolio": snap}


@app.get("/api/player/leaderboard")
def player_leaderboard(user: sqlite3.Row = Depends(player_user)) -> dict[str, Any]:
    conn = db()
    if setting(conn, "leaderboard_visible") != "1":
        conn.close()
        return {"visible": False, "rows": []}
    teams = conn.execute("SELECT id,name,code FROM teams WHERE status='active' ORDER BY name").fetchall()
    rows = []
    for team in teams:
        snap = portfolio_snapshot(conn, team["id"])
        rows.append({"team": team["name"], "code": team["code"], "portfolio_value": snap["account"]["portfolio_value"], "return_pct": snap["account"]["return_pct_vs_initial"]})
    rows.sort(key=lambda x: x["portfolio_value"], reverse=True)
    for i, r in enumerate(rows, 1): r["rank"] = i
    conn.close()
    return {"visible": True, "rows": rows}


@app.get("/api/admin/overview")
def admin_overview(user: sqlite3.Row = Depends(admin_user)) -> dict[str, Any]:
    conn = db()
    teams_count = conn.execute("SELECT COUNT(*) n FROM teams").fetchone()["n"]
    stage = conn.execute("SELECT * FROM stages ORDER BY number DESC LIMIT 1").fetchone()
    assets = conn.execute("SELECT COUNT(*) n FROM instruments").fetchone()["n"]
    premium_rate = float(setting(conn, "premium_rate_pct"))
    cover = float(setting(conn, "insurance_cover"))
    account_rows = conn.execute("SELECT * FROM accounts").fetchall()
    cash = sum(float(x["cash"]) for x in account_rows)
    premium_due = sum(float(x["premium_due"]) for x in account_rows)
    premium_collected = sum(float(x["total_premium_paid"]) for x in account_rows)
    total_value = 0.0
    for t in conn.execute("SELECT id FROM teams").fetchall():
        total_value += portfolio_snapshot(conn, t["id"])["account"]["portfolio_value"]
    recent = conn.execute("SELECT actor,action,detail,created_at FROM audit_logs ORDER BY id DESC LIMIT 12").fetchall()
    result = {
        "teams_count": teams_count, "assets_count": assets, "total_cash": money(cash),
        "total_portfolio_value": money(total_value), "premium_due": money(premium_due),
        "premium_collected": money(premium_collected),
        "stage": dict(stage) if stage else None,
        "premium_rate_pct": premium_rate, "insurance_cover": cover,
        "annual_premium": money(cover*premium_rate/100),
        "max_years": int(setting(conn, "max_years")),
        "leaderboard_visible": setting(conn, "leaderboard_visible") == "1",
        "recent_activity": [dict(x) for x in recent],
    }
    conn.close()
    return result


@app.get("/api/admin/teams")
def admin_teams(user: sqlite3.Row = Depends(admin_user)) -> dict[str, Any]:
    conn = db()
    teams = conn.execute("SELECT * FROM teams ORDER BY created_at DESC").fetchall()
    rows = []
    for t in teams:
        snap = portfolio_snapshot(conn, t["id"])
        settle = conn.execute("SELECT * FROM settlements WHERE team_id=?", (t["id"],)).fetchone()
        rows.append({**dict(t), "account": snap["account"], "settlement": dict(settle) if settle else None})
    conn.close()
    return {"teams": rows}


@app.post("/api/admin/teams")
async def create_team(body: CreateTeamBody, user: sqlite3.Row = Depends(admin_user)) -> dict[str, Any]:
    conn = db()
    username = body.username.strip()
    if conn.execute("SELECT 1 FROM users WHERE username=?", (username,)).fetchone():
        conn.close(); raise HTTPException(409, "Username already exists")
    if conn.execute("SELECT 1 FROM teams WHERE name=? COLLATE NOCASE", (body.name.strip(),)).fetchone():
        conn.close(); raise HTTPException(409, "A team with this name already exists")
    stage = conn.execute("SELECT number FROM stages ORDER BY number DESC LIMIT 1").fetchone()
    stage_num = int(stage["number"] if stage else 1)
    team_id = str(uuid.uuid4())
    code = f"FW-{conn.execute('SELECT COUNT(*) n FROM teams').fetchone()['n']+1:03d}"
    own = float(setting(conn, "own_capital")); loan = float(setting(conn, "loan_principal")); cover = float(setting(conn, "insurance_cover")); rate = float(setting(conn, "premium_rate_pct"))
    premium = money(cover*rate/100)
    # A new team joins the current year, so it pays that year's premium once at account opening.
    cash = own + loan
    paid = min(cash, premium)
    due = max(0.0, premium-paid)
    cash -= paid
    conn.execute("INSERT INTO teams(id,code,name,username,status,created_at) VALUES(?,?,?,?,?,?)", (team_id, code, body.name.strip(), username, "active", now_iso()))
    conn.execute("INSERT INTO users(id,username,password_hash,role,team_id,created_at) VALUES(?,?,?,?,?,?)", (str(uuid.uuid4()), username, hash_password(body.password), "player", team_id, now_iso()))
    conn.execute("INSERT INTO accounts(team_id,cash,own_capital,loan_principal,loan_balance,insurance_cover,premium_due,total_premium_paid) VALUES(?,?,?,?,?,?,?,?)", (team_id, cash, own, loan, loan, cover, due, paid))
    audit(conn, user["username"], "team_created", f"Created {body.name.strip()} ({code}); initial capital ₹{own+loan:,.0f}; Year {stage_num} premium ₹{premium:,.0f}")
    conn.commit()
    snap = portfolio_snapshot(conn, team_id)
    conn.close()
    await hub.broadcast("team_created")
    return {"ok": True, "team": {"id": team_id, "code": code, "name": body.name.strip(), "username": username}, "account": snap["account"], "annual_premium": premium}


@app.get("/api/admin/instruments")
def admin_instruments(user: sqlite3.Row = Depends(admin_user)) -> dict[str, Any]:
    conn = db()
    rows = conn.execute("SELECT * FROM instruments ORDER BY asset_class,name").fetchall()
    conn.close()
    return {"instruments": [dict(x) for x in rows]}


@app.post("/api/admin/instruments")
async def create_instrument(body: CreateInstrumentBody, user: sqlite3.Row = Depends(admin_user)) -> dict[str, Any]:
    conn = db()
    symbol = body.symbol.strip().upper()
    if conn.execute("SELECT 1 FROM instruments WHERE symbol=?", (symbol,)).fetchone():
        conn.close(); raise HTTPException(409, "An instrument with this symbol already exists")
    instrument_id = str(uuid.uuid4())
    conn.execute(
        "INSERT INTO instruments(id,symbol,name,asset_class,sector,unit,price) VALUES(?,?,?,?,?,?,?)",
        (instrument_id, symbol, body.name.strip(), body.asset_class, body.sector.strip(), body.unit.strip(), body.initial_price),
    )
    audit(conn, user["username"], "instrument_created", f"Added {symbol} ({body.asset_class}) at simulated price ₹{body.initial_price:.4f}")
    conn.commit()
    row = conn.execute("SELECT * FROM instruments WHERE id=?", (instrument_id,)).fetchone()
    conn.close()
    await hub.broadcast("instrument_created")
    return {"ok": True, "instrument": dict(row)}


@app.patch("/api/admin/instruments/{instrument_id}")
async def edit_instrument(instrument_id: str, body: InstrumentEditBody, user: sqlite3.Row = Depends(admin_user)) -> dict[str, Any]:
    if body.return_pct is None and body.direct_price is None and body.active is None:
        raise HTTPException(400, "Provide a return, direct price or availability change")
    if body.return_pct is not None and body.direct_price is not None:
        raise HTTPException(400, "Choose percentage change or direct price, not both")
    conn = db()
    row = conn.execute("SELECT * FROM instruments WHERE id=?", (instrument_id,)).fetchone()
    if not row:
        conn.close(); raise HTTPException(404, "Instrument not found")
    if body.return_pct is not None:
        conn.execute("UPDATE instruments SET pending_return_pct=?,pending_price=NULL WHERE id=?", (body.return_pct, instrument_id))
        detail = f"Draft return for {row['symbol']} set to {body.return_pct:.2f}%"
    elif body.direct_price is not None:
        conn.execute("UPDATE instruments SET pending_price=?,pending_return_pct=0 WHERE id=?", (body.direct_price, instrument_id))
        detail = f"Draft price for {row['symbol']} set to ₹{body.direct_price:.4f}"
    else:
        detail = f"Availability for {row['symbol']} set to {body.active}"
    if body.active is not None:
        conn.execute("UPDATE instruments SET active=? WHERE id=?", (1 if body.active else 0, instrument_id))
    audit(conn, user["username"], "asset_draft_edited", detail)
    conn.commit()
    updated = conn.execute("SELECT * FROM instruments WHERE id=?", (instrument_id,)).fetchone()
    conn.close()
    await hub.broadcast("asset_draft")
    return {"ok": True, "instrument": dict(updated)}


@app.post("/api/admin/scenario")
async def apply_scenario(body: ScenarioBody, user: sqlite3.Row = Depends(admin_user)) -> dict[str, Any]:
    allowed = {"Equity", "Bond", "Commodity", "Other", "Cash"}
    if not body.impacts or any(k not in allowed for k in body.impacts):
        raise HTTPException(400, "Impacts must use Equity, Bond, Commodity or Cash")
    conn = db()
    touched = 0
    for asset_class, pct in body.impacts.items():
        if pct < -100 or pct > 1000:
            conn.close(); raise HTTPException(400, f"Invalid movement for {asset_class}")
        conn.execute("UPDATE instruments SET pending_return_pct=?,pending_price=NULL WHERE asset_class=?", (pct, asset_class))
        touched += conn.execute("SELECT changes()").fetchone()[0]
    audit(conn, user["username"], "scenario_applied", f"Draft scenario '{body.name}' set class movements: {json.dumps(body.impacts)}; instruments affected: {touched}")
    conn.commit(); conn.close()
    await hub.broadcast("scenario_draft")
    return {"ok": True, "scenario": body.name, "instruments_affected": touched}


@app.get("/api/admin/market-preview")
def market_preview(user: sqlite3.Row = Depends(admin_user)) -> dict[str, Any]:
    conn = db()
    rows = conn.execute("SELECT * FROM instruments ORDER BY asset_class,name").fetchall()
    preview = []
    for r in rows:
        if r["pending_price"] is not None:
            new_price = float(r["pending_price"])
            pct = (new_price/float(r["price"])-1)*100 if r["price"] else 0
            method = "Direct price"
        else:
            pct = float(r["pending_return_pct"])
            new_price = float(r["price"])*(1+pct/100)
            method = "Percentage"
        preview.append({"id": r["id"], "symbol": r["symbol"], "name": r["name"], "asset_class": r["asset_class"], "old_price": money(r["price"]), "new_price": money(new_price), "return_pct": round(pct,2), "method": method, "active": bool(r["active"])})
    team_values = []
    for t in conn.execute("SELECT id,name FROM teams ORDER BY name").fetchall():
        snap = portfolio_snapshot(conn, t["id"])
        current = snap["account"]["portfolio_value"]
        holding_map = {h["instrument_id"]: h["quantity"] for h in snap["holdings"]}
        projected = float(snap["account"]["cash"])-float(snap["account"]["premium_due"])
        for item in preview:
            projected += holding_map.get(item["id"],0)*item["new_price"]
        team_values.append({"team": t["name"], "current_value": money(current), "projected_value": money(projected), "change": money(projected-current)})
    stage = conn.execute("SELECT * FROM stages ORDER BY number DESC LIMIT 1").fetchone()
    conn.close()
    return {"assets": preview, "teams": team_values, "stage": dict(stage) if stage else None}


@app.post("/api/admin/publish")
async def publish_market(body: dict[str, str] | None = None, user: sqlite3.Row = Depends(admin_user)) -> dict[str, Any]:
    scenario_name = (body or {}).get("scenario_name", "Market update")[:100]
    conn = db(); conn.execute("BEGIN IMMEDIATE")
    stage = conn.execute("SELECT * FROM stages ORDER BY number DESC LIMIT 1").fetchone()
    if not stage or stage["status"] != "closed":
        conn.rollback(); conn.close(); raise HTTPException(409, "Close the trading window before publishing market movements")
    rows = conn.execute("SELECT * FROM instruments").fetchall()
    update_rows = []
    for r in rows:
        old = float(r["price"])
        if r["pending_price"] is not None:
            new = float(r["pending_price"])
        else:
            new = old*(1+float(r["pending_return_pct"])/100)
        new = max(0.0001, round(new, 4))
        pct = (new/old-1)*100 if old else 0
        conn.execute("UPDATE instruments SET price=?,pending_price=NULL,pending_return_pct=0 WHERE id=?", (new, r["id"]))
        if abs(new-old) > 0.0000001:
            conn.execute("INSERT INTO price_history(id,instrument_id,stage_number,old_price,new_price,return_pct,created_at) VALUES(?,?,?,?,?,?,?)", (str(uuid.uuid4()), r["id"], stage["number"], old, new, pct, now_iso()))
        update_rows.append({"symbol": r["symbol"], "old": old, "new": new, "return_pct": pct})
    conn.execute("UPDATE stages SET status='published',published_at=? WHERE id=?", (now_iso(), stage["id"]))
    conn.execute("INSERT INTO market_updates(id,stage_number,scenario_name,update_json,created_at,actor) VALUES(?,?,?,?,?,?)", (str(uuid.uuid4()), stage["number"], scenario_name, json.dumps(update_rows), now_iso(), user["username"]))
    audit(conn, user["username"], "market_published", f"Published '{scenario_name}' for Year {stage['number']}; {len(update_rows)} instruments processed")
    conn.commit(); conn.close()
    await hub.broadcast("market_published")
    return {"ok": True, "message": f"Market movements published for Year {stage['number']}", "instruments_processed": len(update_rows)}


@app.post("/api/admin/stage/open")
async def open_trading(user: sqlite3.Row = Depends(admin_user)) -> dict[str, Any]:
    conn = db(); conn.execute("BEGIN IMMEDIATE")
    stage = conn.execute("SELECT * FROM stages ORDER BY number DESC LIMIT 1").fetchone()
    if not stage or stage["status"] != "closed":
        conn.rollback(); conn.close(); raise HTTPException(409, "The current stage is not ready to open")
    conn.execute("UPDATE stages SET status='open',opened_at=?,closed_at=NULL WHERE id=?", (now_iso(), stage["id"]))
    audit(conn, user["username"], "trading_opened", f"Opened trading for Year {stage['number']}")
    conn.commit(); conn.close(); await hub.broadcast("stage_opened")
    return {"ok": True, "message": f"Trading opened for Year {stage['number']}"}


@app.post("/api/admin/stage/close")
async def close_trading(user: sqlite3.Row = Depends(admin_user)) -> dict[str, Any]:
    conn = db(); conn.execute("BEGIN IMMEDIATE")
    stage = conn.execute("SELECT * FROM stages ORDER BY number DESC LIMIT 1").fetchone()
    if not stage or stage["status"] != "open":
        conn.rollback(); conn.close(); raise HTTPException(409, "Trading is not open")
    conn.execute("UPDATE stages SET status='closed',closed_at=? WHERE id=?", (now_iso(), stage["id"]))
    audit(conn, user["username"], "trading_closed", f"Closed trading for Year {stage['number']}")
    conn.commit(); conn.close(); await hub.broadcast("stage_closed")
    return {"ok": True, "message": f"Trading closed for Year {stage['number']}"}


@app.post("/api/admin/stage/advance")
async def advance_stage(user: sqlite3.Row = Depends(admin_user)) -> dict[str, Any]:
    conn = db(); conn.execute("BEGIN IMMEDIATE")
    current = conn.execute("SELECT * FROM stages ORDER BY number DESC LIMIT 1").fetchone()
    max_years = int(setting(conn, "max_years"))
    if not current or current["status"] != "published":
        conn.rollback(); conn.close(); raise HTTPException(409, "Publish the current year's market update before advancing")
    if int(current["number"]) >= max_years:
        conn.rollback(); conn.close(); raise HTTPException(409, "The final year has been reached. Use maturity settlement instead.")
    next_year = int(current["number"])+1
    cover = float(setting(conn, "insurance_cover")); rate = float(setting(conn, "premium_rate_pct")); premium = money(cover*rate/100)
    team_accounts = conn.execute("SELECT * FROM accounts WHERE settled=0").fetchall()
    for acc in team_accounts:
        paid = min(float(acc["cash"]), premium)
        due = premium-paid
        conn.execute("UPDATE accounts SET cash=cash-?,premium_due=premium_due+?,total_premium_paid=total_premium_paid+? WHERE team_id=?", (paid,due,paid,acc["team_id"]))
    conn.execute("INSERT INTO stages(id,number,title,status) VALUES(?,?,?,?)", (str(uuid.uuid4()), next_year, f"Year {next_year}", "closed"))
    audit(conn, user["username"], "stage_advanced", f"Advanced to Year {next_year}; annual premium ₹{premium:,.2f} charged/accrued for {len(team_accounts)} teams")
    conn.commit(); conn.close(); await hub.broadcast("stage_advanced")
    return {"ok": True, "message": f"Advanced to Year {next_year}. Annual premium of ₹{premium:,.0f} has been charged or accrued for each team.", "annual_premium": premium}


@app.post("/api/admin/settings")
async def update_settings(body: SettingsBody, user: sqlite3.Row = Depends(admin_user)) -> dict[str, Any]:
    conn = db(); conn.execute("BEGIN IMMEDIATE")
    set_setting(conn, "premium_rate_pct", body.premium_rate_pct)
    set_setting(conn, "insurance_cover", body.insurance_cover)
    set_setting(conn, "max_years", body.max_years)
    set_setting(conn, "leaderboard_visible", "1" if body.leaderboard_visible else "0")
    # Update policy cover on existing team accounts; previously paid premiums are not retroactively changed.
    conn.execute("UPDATE accounts SET insurance_cover=? WHERE settled=0", (body.insurance_cover,))
    annual = money(body.insurance_cover*body.premium_rate_pct/100)
    audit(conn, user["username"], "policy_settings_updated", f"Premium rate {body.premium_rate_pct:.2f}% ; cover ₹{body.insurance_cover:,.0f}; annual premium ₹{annual:,.0f}; max years {body.max_years}")
    conn.commit(); conn.close(); await hub.broadcast("policy_settings")
    return {"ok": True, "annual_premium": annual}


@app.post("/api/admin/announcements")
async def create_announcement(body: AnnouncementBody, user: sqlite3.Row = Depends(admin_user)) -> dict[str, Any]:
    conn = db(); conn.execute("INSERT INTO announcements(id,title,body,created_at,actor) VALUES(?,?,?,?,?)", (str(uuid.uuid4()), body.title, body.body, now_iso(), user["username"]))
    audit(conn, user["username"], "announcement_published", body.title)
    conn.commit(); conn.close(); await hub.broadcast("announcement")
    return {"ok": True}


@app.get("/api/admin/audit")
def admin_audit(user: sqlite3.Row = Depends(admin_user)) -> dict[str, Any]:
    conn = db(); rows = conn.execute("SELECT * FROM audit_logs ORDER BY id DESC LIMIT 200").fetchall(); conn.close()
    return {"logs": [dict(x) for x in rows]}


@app.get("/api/admin/leaderboard")
def admin_leaderboard(user: sqlite3.Row = Depends(admin_user)) -> dict[str, Any]:
    conn = db(); teams = conn.execute("SELECT id,name,code FROM teams ORDER BY name").fetchall(); rows = []
    for t in teams:
        snap = portfolio_snapshot(conn, t["id"])
        rows.append({"team":t["name"],"code":t["code"],"cash":snap["account"]["cash"],"portfolio_value":snap["account"]["portfolio_value"],"return_pct":snap["account"]["return_pct_vs_initial"],"premium_paid":snap["account"]["total_premium_paid"]})
    rows.sort(key=lambda x:x["portfolio_value"],reverse=True)
    for i,r in enumerate(rows,1): r["rank"]=i
    conn.close(); return {"rows":rows}


@app.post("/api/admin/settle")
async def settle_all(user: sqlite3.Row = Depends(admin_user)) -> dict[str, Any]:
    conn = db(); conn.execute("BEGIN IMMEDIATE")
    stage = conn.execute("SELECT * FROM stages ORDER BY number DESC LIMIT 1").fetchone()
    max_years = int(setting(conn,"max_years"))
    if not stage or stage["status"] != "published" or int(stage["number"]) != max_years:
        conn.rollback(); conn.close(); raise HTTPException(409, "Settlement is available only after the final year's market update is published")
    settled=[]
    teams=conn.execute("SELECT id,name FROM teams WHERE status='active'").fetchall()
    for t in teams:
        acc=conn.execute("SELECT * FROM accounts WHERE team_id=?",(t["id"],)).fetchone()
        if acc["settled"]: continue
        holding_value=conn.execute("SELECT COALESCE(SUM(h.quantity*i.price),0) v FROM holdings h JOIN instruments i ON i.id=h.instrument_id WHERE h.team_id=?",(t["id"],)).fetchone()["v"]
        gross=float(acc["cash"])+float(holding_value)
        premium_due=float(acc["premium_due"])
        net=max(0.0,gross-premium_due)
        loan=float(acc["loan_balance"])
        repayment=min(net,loan)
        shortfall=max(0.0,loan-repayment)
        payout=min(shortfall,float(acc["insurance_cover"]))
        lender_shortfall=max(0.0,shortfall-payout)
        residual=max(0.0,net-loan)
        conn.execute("INSERT INTO settlements(team_id,gross_portfolio,premiums_due,net_assets,loan_repaid,insurer_payout,residual_equity,lender_shortfall,created_at) VALUES(?,?,?,?,?,?,?,?,?)",(t["id"],gross,premium_due,net,repayment,payout,residual,lender_shortfall,now_iso()))
        conn.execute("UPDATE accounts SET settled=1,loan_balance=0,cash=0,premium_due=0 WHERE team_id=?",(t["id"],))
        conn.execute("DELETE FROM holdings WHERE team_id=?",(t["id"],))
        settled.append({"team":t["name"],"gross_portfolio":money(gross),"premium_due":money(premium_due),"loan_repaid":money(repayment),"insurer_payout":money(payout),"residual_equity":money(residual),"lender_shortfall":money(lender_shortfall)})
    audit(conn,user["username"],"maturity_settlement",f"Settled {len(settled)} teams at maturity")
    conn.commit(); conn.close(); await hub.broadcast("settlement")
    return {"ok":True,"settlements":settled}


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket, token: str = "") -> None:
    user = get_user(token)
    await hub.connect(websocket)
    try:
        await websocket.send_text(json.dumps({"event":"connected","role":user["role"]}))
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        hub.disconnect(websocket)
    except Exception:
        hub.disconnect(websocket)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=PORT, reload=False)
