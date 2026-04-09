"""Shared fixtures. Uses mongomock for isolated test DB."""
import os
import sys
import pathlib

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

os.environ.setdefault("ANTHROPIC_API_KEY", "test-key")
os.environ.setdefault("MONGO_URL", "mongodb://fake/")
os.environ.setdefault("MONGO_DB", "tradium_test")

import mongomock  # noqa: E402
import pytest  # noqa: E402

import database  # noqa: E402
# Patch the symbol that database actually uses
database.MongoClient = mongomock.MongoClient
database._client = None
database._db = None
database.init_db()


@pytest.fixture
def db_session():
    s = database.SessionLocal()
    try:
        yield s
    finally:
        s.close()


@pytest.fixture
def clean_db():
    database._signals().delete_many({})
    database._counters().delete_many({})
    yield


class FakeBot:
    def __init__(self):
        self.messages = []
        self.photos = []

    async def send_message(self, chat_id, text, **kwargs):
        self.messages.append((chat_id, text, kwargs))

    async def send_photo(self, chat_id, photo, caption=None, **kwargs):
        self.photos.append((chat_id, photo, caption, kwargs))


@pytest.fixture
def fake_bot():
    return FakeBot()
