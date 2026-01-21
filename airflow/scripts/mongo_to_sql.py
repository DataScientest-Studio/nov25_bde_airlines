import os
from datetime import datetime

from pymongo import MongoClient
import psycopg2


# =====================
# ENV VARS
# =====================
MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongodb:27017/")
MONGO_DB = os.getenv("MONGO_DB", "DST_AIRLINES")

PG_HOST = os.getenv("PG_HOST", "postgres_data")
PG_DB = os.getenv("PG_DB", "flight")
PG_USER = os.getenv("PG_USER", "postgres")
PG_PASSWORD = os.getenv("PG_PASSWORD", "postgres")
PG_PORT = int(os.getenv("PG_PORT", "5432"))

# Collections Mongo
COL_DELAYS = os.getenv("MONGO_DELAYS_COL", "delays_raw")
COL_AIRPORTS = os.getenv("MONGO_AIRPORTS_COL", "airports_raw")
COL_AIRLINES = os.getenv("MONGO_AIRLINES_COL", "airlines_raw")


def pg_connect():
    return psycopg2.connect(
        host=PG_HOST,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD,
        port=PG_PORT,
    )


def to_int(v):
    try:
        if v is None:
            return None
        return int(v)
    except Exception:
        return None


def upsert_airport(cur, iata, icao, name):
    """Upsert airports by iata_code, return airport_id."""
    if not iata:
        return None
    cur.execute(
        """
        INSERT INTO airports (iata_code, icao_code, name)
        VALUES (%s, %s, %s)
        ON CONFLICT (iata_code) DO UPDATE
        SET
          icao_code = COALESCE(EXCLUDED.icao_code, airports.icao_code),
          name      = COALESCE(EXCLUDED.name, airports.name)
        RETURNING airport_id
        """,
        (iata, icao, name),
    )
    return cur.fetchone()[0]


def upsert_airline(cur, iata, icao, name):
    """Upsert airlines by iata_code (must be UNIQUE), return airline_id."""
    if not iata:
        return None
    cur.execute(
        """
        INSERT INTO airlines (iata_code, icao_code, name)
        VALUES (%s, %s, %s)
        ON CONFLICT (iata_code) DO UPDATE
        SET
          icao_code = COALESCE(EXCLUDED.icao_code, airlines.icao_code),
          name      = COALESCE(EXCLUDED.name, airlines.name)
        RETURNING airline_id
        """,
        (iata, icao, name),
    )
    return cur.fetchone()[0]


def get_or_create_flight(cur, flight_iata, flight_icao, flight_number, airline_id, dep_airport_id, arr_airport_id):
    """
    Flights: on garde une logique simple:
    - si déjà existant pour (flight_iata, dep_airport_id, arr_airport_id), on réutilise.
    - sinon on crée.
    """
    cur.execute(
        """
        SELECT flight_id
        FROM flights
        WHERE flight_iata = %s
          AND dep_airport_id = %s
          AND arr_airport_id = %s
        LIMIT 1
        """,
        (flight_iata, dep_airport_id, arr_airport_id),
    )
    row = cur.fetchone()
    if row:
        return row[0]

    cur.execute(
        """
        INSERT INTO flights
          (flight_iata, flight_icao, flight_number, airline_id, dep_airport_id, arr_airport_id)
        VALUES
          (%s,%s,%s,%s,%s,%s)
        RETURNING flight_id
        """,
        (flight_iata, flight_icao, flight_number, airline_id, dep_airport_id, arr_airport_id),
    )
    return cur.fetchone()[0]


def normalize_delay_fields(doc):
    """
    Airlabs renvoie parfois:
    - dep_delay / arr_delay / total_delay
    ou:
    - dep_delayed / arr_delayed / delayed
    """
    dep_delay = doc.get("dep_delay")
    arr_delay = doc.get("arr_delay")
    total_delay = doc.get("total_delay")

    if dep_delay is None:
        dep_delay = doc.get("dep_delayed")
    if arr_delay is None:
        arr_delay = doc.get("arr_delayed")
    if total_delay is None:
        total_delay = doc.get("delayed")

    return to_int(dep_delay), to_int(arr_delay), to_int(total_delay)


def main():
    # Mongo
    mongo = MongoClient(MONGO_URI)
    db = mongo[MONGO_DB]
    delays_col = db[COL_DELAYS]
    airports_col = db[COL_AIRPORTS]
    airlines_col = db[COL_AIRLINES]

    # Postgres
    conn = pg_connect()
    cur = conn.cursor()

    ok_airports = 0
    ok_airlines = 0
    ok_delays = 0
    err = 0

    # =========================
    # 1) Sync airports_raw -> airports
    # =========================
    print("🛫 Sync airports_raw -> airports")
    for a in airports_col.find():
        try:
            iata = a.get("iata_code")
            icao = a.get("icao_code")
            name = a.get("name")  # Airlabs airports endpoint renvoie généralement "name"
            if not iata:
                continue

            upsert_airport(cur, iata, icao, name)
            conn.commit()
            ok_airports += 1
        except Exception as e:
            conn.rollback()
            err += 1
            print("❌ airport sync error:", e)

    print(f"✅ airports sync: {ok_airports} upserts")

    # =========================
    # 2) Sync airlines_raw -> airlines
    # =========================
    print("\n🏢 Sync airlines_raw -> airlines")
    for a in airlines_col.find():
        try:
            iata = a.get("iata_code")
            icao = a.get("icao_code")
            name = a.get("name")  # Airlabs airlines endpoint renvoie généralement "name"
            if not iata:
                continue

            upsert_airline(cur, iata, icao, name)
            conn.commit()
            ok_airlines += 1
        except Exception as e:
            conn.rollback()
            err += 1
            print("❌ airline sync error:", e)

    print(f"✅ airlines sync: {ok_airlines} upserts")

    # =========================
    # 3) delays_raw -> flights + flight_delays
    # =========================
    print("\n⏱️ ETL delays_raw -> flights + flight_delays")
    for d in delays_col.find():
        try:
            # airports ids (créés via sync airports, mais on sécurise)
            dep_airport_id = upsert_airport(cur, d.get("dep_iata"), d.get("dep_icao"), None)
            arr_airport_id = upsert_airport(cur, d.get("arr_iata"), d.get("arr_icao"), None)

            # airline id (créé via sync airlines, mais on sécurise)
            airline_id = upsert_airline(cur, d.get("airline_iata"), d.get("airline_icao"), None)

            flight_id = get_or_create_flight(
                cur,
                d.get("flight_iata"),
                d.get("flight_icao"),
                d.get("flight_number"),
                airline_id,
                dep_airport_id,
                arr_airport_id,
            )

            dep_delay, arr_delay, total_delay = normalize_delay_fields(d)
            collected_at = (d.get("_metadata") or {}).get("collected_at")

            # IMPORTANT: doit exister / être parsable sinon on ignore
            dep_time_utc = d.get("dep_time_utc")
            if not dep_time_utc:
                continue

            cur.execute(
                """
                INSERT INTO flight_delays
                (flight_id, status,
                 dep_time_utc, dep_estimated_utc,
                 arr_time_utc, arr_estimated_utc,
                 dep_delay, arr_delay, total_delay,
                 duration, collected_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (flight_id, dep_time_utc) DO UPDATE
                SET
                  status = EXCLUDED.status,
                  dep_estimated_utc = EXCLUDED.dep_estimated_utc,
                  arr_time_utc = EXCLUDED.arr_time_utc,
                  arr_estimated_utc = EXCLUDED.arr_estimated_utc,
                  dep_delay = EXCLUDED.dep_delay,
                  arr_delay = EXCLUDED.arr_delay,
                  total_delay = EXCLUDED.total_delay,
                  duration = EXCLUDED.duration,
                  collected_at = EXCLUDED.collected_at
                """,
                (
                    flight_id,
                    d.get("status"),
                    d.get("dep_time_utc"),
                    d.get("dep_estimated_utc"),
                    d.get("arr_time_utc"),
                    d.get("arr_estimated_utc"),
                    dep_delay,
                    arr_delay,
                    total_delay,
                    to_int(d.get("duration")),
                    collected_at,
                ),
            )

            conn.commit()
            ok_delays += 1

        except Exception as e:
            conn.rollback()
            err += 1
            print("❌ delays ETL error:", e)

    cur.close()
    conn.close()
    mongo.close()

    print("\n🎉 ETL terminé")
    print(f"  airports upserts: {ok_airports}")
    print(f"  airlines upserts: {ok_airlines}")
    print(f"  delays upserts  : {ok_delays}")
    print(f"  errors          : {err}")


if __name__ == "__main__":
    main()
