#!/usr/bin/env python3
"""Quick endpoint test"""

import sys
try:
    from starlette.testclient import TestClient
    from admin import app

    client = TestClient(app)

    endpoints = [
        ('GET', '/'),
        ('GET', '/api/header-data'),
        ('GET', '/api/paper/status'),
        ('GET', '/api/paper/history'),
    ]

    print("Testing endpoints:")
    for method, path in endpoints:
        try:
            if method == 'GET':
                r = client.get(path)
            print(f"{method} {path:30} -> {r.status_code}")
        except Exception as e:
            print(f"{method} {path:30} -> ERROR: {type(e).__name__}: {str(e)[:80]}")
except Exception as e:
    print(f"FATAL: {type(e).__name__}: {e}")
    sys.exit(1)
