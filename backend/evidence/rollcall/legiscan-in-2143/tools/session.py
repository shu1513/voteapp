"""Which Indiana session the tools in this directory work on.

The tools are shared across Indiana's sessions: only the dataset directory and the year
segment of an iga.in.gov URL differ. Set IN_SESSION to switch; the default is the 2025
session this directory was written for.

    IN_SESSION=2234 python3 memberchk.py rolls.json
"""
import os

SESSIONS = {
    "2143": {"dir": "/Users/shu/legiscan-data/in-2143/IN/2025-2025_Regular_Session", "year": "2025"},
    "2234": {"dir": "/Users/shu/legiscan-data/in-2234/IN/2026-2026_Regular_Session", "year": "2026"},
}

SESSION = os.environ.get("IN_SESSION", "2143")
if SESSION not in SESSIONS:
    raise SystemExit(f"IN_SESSION must be one of {sorted(SESSIONS)}, got {SESSION!r}")

DATASET = SESSIONS[SESSION]["dir"]
YEAR = SESSIONS[SESSION]["year"]
BILLDIR = f"{DATASET}/bill"
VOTEDIR = f"{DATASET}/vote"
PEOPLEDIR = f"{DATASET}/people"
