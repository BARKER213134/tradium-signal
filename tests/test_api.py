from fastapi.testclient import TestClient

import database
import admin as admin_module
from database import Signal, utcnow
from config import ADMIN_USERNAME, ADMIN_PASSWORD


def _login(c: TestClient):
    r = c.post("/login", data={"username": ADMIN_USERNAME, "password": ADMIN_PASSWORD},
               follow_redirects=False)
    assert r.status_code in (302, 303)


# Ensure admin uses our test DB-session override
client = TestClient(admin_module.app)
_login(client)


def _seed():
    s = database.SessionLocal()
    try:
        s.query(Signal).delete()
        s.add_all([
            Signal(message_id=1, pair="BTC/USDT", direction="LONG", entry=60000,
                   tp1=62000, sl=59000, status="СЛЕЖУ", received_at=utcnow()),
            Signal(message_id=2, pair="ETH/USDT", direction="LONG", entry=3000,
                   tp1=3100, sl=2950, dca4=2980, status="ОТКРЫТ",
                   received_at=utcnow()),
            Signal(message_id=3, pair="SOL/USDT", direction="SHORT", entry=150,
                   tp1=140, sl=155, status="TP", result="TP", pnl_percent=6.6,
                   received_at=utcnow()),
        ])
        s.commit()
    finally:
        s.close()


def test_unauthorized_html_redirects_to_login():
    no_auth = TestClient(admin_module.app)
    r = no_auth.get("/signals", follow_redirects=False)
    assert r.status_code == 303
    assert "/login" in r.headers["location"]


def test_unauthorized_api_returns_401():
    no_auth = TestClient(admin_module.app)
    r = no_auth.get("/api/signals-live")
    assert r.status_code == 401


def test_login_bad_password():
    c = TestClient(admin_module.app)
    r = c.post("/login", data={"username": ADMIN_USERNAME, "password": "wrong"},
               follow_redirects=False)
    assert r.status_code == 401


def test_login_page_open():
    c = TestClient(admin_module.app)
    assert c.get("/login").status_code == 200


def test_root_redirects():
    r = client.get("/", follow_redirects=False)
    assert r.status_code in (302, 307)
    assert "/signals" in r.headers["location"]


def test_signals_page_renders():
    _seed()
    r = client.get("/signals")
    assert r.status_code == 200


def test_signals_tab_filter_dca4():
    _seed()
    r = client.get("/signals?tab=dca4")
    assert r.status_code == 200
    # ETH is in ОТКРЫТ state
    assert b"ETH" in r.content or b"eth" in r.content.lower()


def test_api_signals_live_structure():
    _seed()
    r = client.get("/api/signals-live?tab=tradium")
    assert r.status_code == 200
    data = r.json()
    assert "stats" in data and "signals" in data
    for key in ("total", "watching", "dca4", "tp", "sl", "win_rate"):
        assert key in data["stats"]
    # tradium tab => only СЛЕЖУ
    assert all(s["status"] == "СЛЕЖУ" for s in data["signals"])


def test_api_signal_one_404():
    r = client.get("/api/signal/99999")
    assert r.status_code == 404


def test_api_signal_one_ok():
    _seed()
    s = database.SessionLocal()
    try:
        sid = s.query(Signal).first().id
    finally:
        s.close()
    r = client.get(f"/api/signal/{sid}")
    assert r.status_code == 200
    assert r.json()["id"] == sid


def test_chart_404_missing_path():
    _seed()
    s = database.SessionLocal()
    try:
        sid = s.query(Signal).first().id
    finally:
        s.close()
    r = client.get(f"/chart/{sid}")
    assert r.status_code == 404
