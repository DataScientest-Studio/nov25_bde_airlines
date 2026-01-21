BEGIN;

-- =========================
-- Airports
-- =========================
CREATE TABLE IF NOT EXISTS airports (
  airport_id  SERIAL PRIMARY KEY,
  iata_code   VARCHAR(8) NOT NULL UNIQUE,
  icao_code   VARCHAR(8),
  name        TEXT
);

CREATE INDEX IF NOT EXISTS idx_airports_iata ON airports(iata_code);

-- =========================
-- Airlines
-- =========================
CREATE TABLE IF NOT EXISTS airlines (
  airline_id  SERIAL PRIMARY KEY,
  iata_code   VARCHAR(8) NOT NULL UNIQUE,
  icao_code   VARCHAR(8),
  name        TEXT
);

CREATE INDEX IF NOT EXISTS idx_airlines_iata ON airlines(iata_code);

-- =========================
-- Flights
-- =========================
CREATE TABLE IF NOT EXISTS flights (
  flight_id        SERIAL PRIMARY KEY,
  flight_iata      VARCHAR(32),
  flight_icao      VARCHAR(32),
  flight_number    VARCHAR(32),
  airline_id       INT REFERENCES airlines(airline_id),
  dep_airport_id   INT REFERENCES airports(airport_id),
  arr_airport_id   INT REFERENCES airports(airport_id)
);

-- Pour accélérer get_or_create_flight
CREATE INDEX IF NOT EXISTS idx_flights_keys
  ON flights(flight_iata, dep_airport_id, arr_airport_id);

-- =========================
-- Flight delays
-- =========================
CREATE TABLE IF NOT EXISTS flight_delays (
  delay_id            SERIAL PRIMARY KEY,
  flight_id           INT NOT NULL REFERENCES flights(flight_id),
  status              VARCHAR(32),

  -- UTC -> TIMESTAMPTZ recommandé
  dep_time_utc        TIMESTAMPTZ NULL,
  dep_estimated_utc   TIMESTAMPTZ NULL,
  arr_time_utc        TIMESTAMPTZ NULL,
  arr_estimated_utc   TIMESTAMPTZ NULL,

  dep_delay           INT NULL,
  arr_delay           INT NULL,
  total_delay         INT NULL,
  duration            INT NULL,
  collected_at        TIMESTAMPTZ NULL,

  CONSTRAINT uq_flight_delays_flight_dep UNIQUE (flight_id, dep_time_utc)
);

-- Index perf dashboard / requêtes
CREATE INDEX IF NOT EXISTS idx_delays_flight_time
  ON flight_delays(flight_id, dep_time_utc);

CREATE INDEX IF NOT EXISTS idx_delays_dep_time
  ON flight_delays(dep_time_utc);

CREATE INDEX IF NOT EXISTS idx_delays_collected_at
  ON flight_delays(collected_at);

CREATE INDEX IF NOT EXISTS idx_delays_total_delay
  ON flight_delays(total_delay);

COMMIT;
