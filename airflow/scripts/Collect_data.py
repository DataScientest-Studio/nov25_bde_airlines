import os
import time
from datetime import datetime, timezone, timedelta

import requests
from pymongo import MongoClient

# =====================
# ENV
# =====================
MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongodb:27017/")
MONGO_DB = os.getenv("MONGO_DB", "DST_AIRLINES")
API_KEY = os.getenv("AIRLABS_API_KEY")

SLEEP = float(os.getenv("AIRLABS_SLEEP_SEC", "1.0"))
TIMEOUT = int(os.getenv("AIRLABS_TIMEOUT_SEC", "15"))

# Option: ne refresh airports/airlines si déjà collectés dans les X heures
REFRESH_REF_HOURS = int(os.getenv("REFRESH_REF_HOURS", "24"))

if not API_KEY or API_KEY == "CHANGE_ME":
    raise ValueError("AIRLABS_API_KEY est manquante/invalide. Vérifie ton .env.")

# =====================
# Mongo
# =====================
client = MongoClient(MONGO_URI)
db = client[MONGO_DB]

flights_col = db["flights_raw"]
delays_col = db["delays_raw"]
airports_col = db["airports_raw"]
airlines_col = db["airlines_raw"]

# =====================
# Airlabs endpoints
# =====================
BASE = "https://airlabs.co/api/v9"
FLIGHTS_URL = f"{BASE}/flights"
DELAYS_URL = f"{BASE}/delays"
AIRPORTS_URL = f"{BASE}/airports"
AIRLINES_URL = f"{BASE}/airlines"

AIRPORTS = [
    "CDG", "JFK", "LHR", "FRA", "DXB",
    "HND", "SIN", "SYD", "ORD", "ATL",
    "IST", "MAD", "AMS", "ALG", "DIA"
]

# =====================
# HTTP session
# =====================
session = requests.Session()


def now_utc():
    return datetime.now(timezone.utc)


def get_json(url, params, timeout=TIMEOUT):
    """Retourne le JSON dict ou None (sans crasher)."""
    try:
        r = session.get(url, params=params, timeout=timeout)
        r.raise_for_status()
        data = r.json()

        # Airlabs errors even if HTTP 200
        if isinstance(data, dict) and "response" not in data and (data.get("code") or data.get("message") or data.get("error")):
            print(f"❌ API error {url}: {data}")
            return data  # on renvoie quand même data pour détecter quota

        return data
    except Exception as e:
        print(f"❌ HTTP error {url}: {e}")
        return None


def normalize_response(data):
    """data['response'] peut être list, dict ou vide -> retourne list."""
    if not data:
        return []
    resp = data.get("response")
    if isinstance(resp, list):
        return resp
    if isinstance(resp, dict):
        return [resp]
    return []


def is_quota_error(data):
    """Détecte les erreurs de quota Airlabs (formats possibles)."""
    if not isinstance(data, dict):
        return False
    # parfois: {"error": {"code": "month_limit_exceeded", ...}}
    err = data.get("error")
    if isinstance(err, dict) and err.get("code") in {"month_limit_exceeded", "day_limit_exceeded", "limit_exceeded"}:
        return True
    # parfois: {"code": "...", "message": "..."}
    if data.get("code") in {"month_limit_exceeded", "day_limit_exceeded", "limit_exceeded"}:
        return True
    msg = (data.get("message") or "").lower()
    if "limit" in msg and "exceed" in msg:
        return True
    return False


def recently_collected(meta, hours=24):
    """meta = doc['_metadata'] ; retourne True si collected_at < hours."""
    if not isinstance(meta, dict):
        return False
    ca = meta.get("collected_at")
    if not ca:
        return False
    # Mongo stocke souvent datetime -> ok
    try:
        return ca >= (now_utc() - timedelta(hours=hours))
    except Exception:
        return False


# ✅ On stocke uniquement les airlines vues sur les vols depuis tes aéroports
airlines_from_my_airports = set()

print("✈️ COLLECTE DES VOLS")

for dep in AIRPORTS:
    print(f"📡 Flights depuis {dep}")
    data = get_json(FLIGHTS_URL, {"api_key": API_KEY, "dep_iata": dep}, timeout=TIMEOUT + 5)
    flights = normalize_response(data)

    if not flights:
        msg = (data.get("error") or data.get("message")) if isinstance(data, dict) else ""
        print(f"⚠️ vide/erreur flights {dep}: {msg}")
        time.sleep(SLEEP)
        continue

    for flight in flights:
        # capture des airlines (uniquement depuis tes AIRPORTS)
        iata = flight.get("airline_iata")
        if isinstance(iata, str):
            iata = iata.strip().upper()
            if len(iata) == 2 and iata.isalnum():
                airlines_from_my_airports.add(iata)

        flight["_metadata"] = {"airport": dep, "collected_at": now_utc(), "source": "airlabs_flights"}
        flights_col.update_one(
            {"flight_iata": flight.get("flight_iata"), "dep_time_utc": flight.get("dep_time_utc")},
            {"$set": flight},
            upsert=True,
        )

    print(f"✅ {dep} vols collectés ({len(flights)}) | airlines_detectées={len(airlines_from_my_airports)}")
    time.sleep(SLEEP)

print("\n⏱️ COLLECTE DES RETARDS")

for dep in AIRPORTS:
    print(f"📡 Delays depuis {dep}")
    data = get_json(DELAYS_URL, {"api_key": API_KEY, "dep_iata": dep, "type": "departures"}, timeout=TIMEOUT + 10)
    delays = normalize_response(data)

    if not delays:
        msg = (data.get("error") or data.get("message")) if isinstance(data, dict) else ""
        print(f"⚠️ vide/erreur delays {dep}: {msg}")
        time.sleep(SLEEP)
        continue

    for delay in delays:
        delay["_metadata"] = {"airport": dep, "collected_at": now_utc(), "source": "airlabs_delays"}
        delays_col.update_one(
            {"flight_iata": delay.get("flight_iata"), "dep_time_utc": delay.get("dep_time_utc")},
            {"$set": delay},
            upsert=True,
        )

        # au cas où delays contient airline_iata
        iata = delay.get("airline_iata")
        if isinstance(iata, str):
            iata = iata.strip().upper()
            if len(iata) == 2 and iata.isalnum():
                airlines_from_my_airports.add(iata)

    print(f"✅ {dep} retards collectés ({len(delays)}) | airlines_detectées={len(airlines_from_my_airports)}")
    time.sleep(SLEEP)

print("\n🛫 COLLECTE DES AÉROPORTS (14/15) — avec cache 24h par défaut")

for code in AIRPORTS:
    # cache: si airport déjà collecté récemment -> skip
    existing = airports_col.find_one({"iata_code": code}, {"_metadata": 1})
    if existing and recently_collected(existing.get("_metadata"), hours=REFRESH_REF_HOURS):
        print(f"⏩ Airport {code} déjà à jour (<{REFRESH_REF_HOURS}h), skip")
        continue

    print(f"📡 Airport {code}")
    data = get_json(AIRPORTS_URL, {"api_key": API_KEY, "iata_code": code})
    if is_quota_error(data):
        print("🛑 Quota Airlabs dépassé -> arrêt collecte airports.")
        break

    airports = normalize_response(data)

    if not airports:
        msg = (data.get("error") or data.get("message")) if isinstance(data, dict) else ""
        print(f"⚠️ vide/erreur airport {code}: {msg}")
        time.sleep(SLEEP)
        continue

    airport = airports[0]
    airport["_metadata"] = {"collected_at": now_utc(), "source": "airlabs_airports"}

    airports_col.update_one(
        {"iata_code": airport.get("iata_code") or code},
        {"$set": airport},
        upsert=True,
    )

    print(f"✅ airport {code} upsert")
    time.sleep(SLEEP)

print("\n🏢 COLLECTE DES COMPAGNIES (uniquement vols depuis tes aéroports) — avec cache")

airlines_iata = sorted(airlines_from_my_airports)
print(f"🔎 {len(airlines_iata)} compagnies détectées (depuis AIRPORTS)")

# ✅ cache: déjà présentes en Mongo -> pas de requêtes API
existing_airlines = set(airlines_col.distinct("iata_code"))
to_fetch = [a for a in airlines_iata if a not in existing_airlines]
print(f"🧠 déjà en base={len(existing_airlines)} | à récupérer={len(to_fetch)}")

for iata in to_fetch:
    # cache TTL: si déjà collectée récemment (au cas où elle existe mais sans distinct correct)
    existing = airlines_col.find_one({"iata_code": iata}, {"_metadata": 1})
    if existing and recently_collected(existing.get("_metadata"), hours=REFRESH_REF_HOURS):
        print(f"⏩ Airline {iata} déjà à jour (<{REFRESH_REF_HOURS}h), skip")
        continue

    print(f"📡 Airline {iata}")
    data = get_json(AIRLINES_URL, {"api_key": API_KEY, "iata_code": iata})
    if is_quota_error(data):
        print("🛑 Quota Airlabs dépassé -> arrêt collecte airlines.")
        break

    airlines = normalize_response(data)

    if not airlines:
        msg = (data.get("error") or data.get("message")) if isinstance(data, dict) else ""
        print(f"⚠️ vide/erreur airline {iata}: {msg}")
        time.sleep(SLEEP)
        continue

    airline = airlines[0]
    airline["_metadata"] = {"collected_at": now_utc(), "source": "airlabs_airlines"}

    airlines_col.update_one(
        {"iata_code": airline.get("iata_code") or iata},
        {"$set": airline},
        upsert=True,
    )

    print(f"✅ airline {iata} upsert")
    time.sleep(SLEEP)

print("\n🎯 COLLECTE TERMINÉE (FLIGHTS + DELAYS + AIRPORTS + AIRLINES limitées)")
client.close()
session.close()
