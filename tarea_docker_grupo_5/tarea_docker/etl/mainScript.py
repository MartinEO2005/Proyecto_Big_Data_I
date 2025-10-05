import os, time, pymysql
from datetime import date, timedelta, datetime
from zoneinfo import ZoneInfo

import pandas as pd
import numpy as np
import requests_cache
from retry_requests import retry
import openmeteo_requests

DB_HOST = os.getenv("DB_HOST", "db")
DB_PORT = int(os.getenv("DB_PORT", "3306"))
DB_NAME = os.getenv("DB_NAME", "weather")
DB_USER = os.getenv("DB_USER", "appuser")
DB_PASSWORD = os.getenv("DB_PASSWORD", "app_password")

CITY_LAT = float(os.getenv("CITY_LAT", "40.3581"))
CITY_LON = float(os.getenv("CITY_LON", "-3.9043"))
TIMEZONE = os.getenv("TIMEZONE", "Europe/Madrid")

ARCHIVE_API = "https://archive-api.open-meteo.com/v1/archive"

def log(m): print(f"[ETL] {m}", flush=True)

def db_connect(retries=20, delay=2):
    for i in range(retries):
        try:
            return pymysql.connect(
                host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASSWORD,
                database=DB_NAME, charset="utf8mb4", autocommit=True
            )
        except Exception as e:
            log(f"DB attempt {i+1}/{retries} failed: {e}"); time.sleep(delay)
    raise RuntimeError("DB not reachable")

def ensure_table(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS info_meteorologica (
                date DATE PRIMARY KEY,
                tmax DOUBLE,
                tmin DOUBLE,
                precipitation_sum DOUBLE,
                weather_code INT
            )
        """)
    log("Tabla OK")

def compute_target_dates():
    tz = ZoneInfo(TIMEZONE)
    today_local = datetime.now(tz).date()
    # Últimos 7 días, EXCLUYENDO hoy (hoy puede estar incompleto)
    days = [today_local - timedelta(days=i) for i in range(1, 8)]
    return sorted(days)

def dates_needing_update(conn, target_dates):
    """Devuelve fechas que faltan O que tienen algún campo NULL (auto-reparación)."""
    if not target_dates: return []
    with conn.cursor() as cur:
        placeholders = ",".join(["%s"] * len(target_dates))
        cur.execute(f"""
            SELECT date
            FROM info_meteorologica
            WHERE date IN ({placeholders})
              AND tmax IS NOT NULL
              AND tmin IS NOT NULL
              AND precipitation_sum IS NOT NULL
              AND weather_code IS NOT NULL
        """, target_dates)
        complete = {row[0] for row in cur.fetchall()}
    return [d for d in target_dates if d not in complete]

def fetch_open_meteo_daily(start_date: date, end_date: date):
    # Cliente con cache y reintentos
    cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.3)
    client = openmeteo_requests.Client(session=retry_session)

    params = {
        "latitude": CITY_LAT,
        "longitude": CITY_LON,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "daily": ["temperature_2m_max", "temperature_2m_min", "precipitation_sum", "weathercode"],
        "timezone": TIMEZONE
    }
    log(f"Llamando Open-Meteo (archive) con {params}")
    responses = client.weather_api(ARCHIVE_API, params=params)
    return responses[0]  # una localización

def map_response_to_rows(response, filter_dates: set[date]):
    """
    Mapea respuesta daily del cliente openmeteo_requests -> filas (date, tmax, tmin, precip, code).
    Evita insertar filas con cualquier valor NaN/None.
    """

    daily = response.Daily()
    # El orden de variables debe coincidir con params["daily"]
    tmax = daily.Variables(0).ValuesAsNumpy()
    tmin = daily.Variables(1).ValuesAsNumpy()
    prcp = daily.Variables(2).ValuesAsNumpy()
    code = daily.Variables(3).ValuesAsNumpy()

    start = pd.to_datetime(daily.Time(), unit="s", utc=True)
    end = pd.to_datetime(daily.TimeEnd(), unit="s", utc=True)
    dates = pd.date_range(start=start, end=end, freq=pd.Timedelta(seconds=daily.Interval()), inclusive="left")
    # A fecha local (sin hora)
    dates_local = dates.tz_convert(TIMEZONE).date

    rows = []
    for d, a, b, c, e in zip(dates_local, tmax, tmin, prcp, code):
        if d not in filter_dates:
            continue
        # Si cualquiera es NaN/None, saltamos esta fila
        if any(pd.isna(v) for v in (a, b, c, e)):
            continue
        try:
            rows.append((d, float(a), float(b), float(c), int(e)))
        except Exception:
            # En caso de tipo raro, no insertamos esa fila
            continue
    return rows


def upsert(conn, rows):
    if not rows: return 0
    with conn.cursor() as cur:
        cur.executemany("""
            INSERT INTO info_meteorologica (date, tmax, tmin, precipitation_sum, weather_code)
            VALUES (%s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
              tmax=VALUES(tmax), tmin=VALUES(tmin),
              precipitation_sum=VALUES(precipitation_sum),
              weather_code=VALUES(weather_code)
        """, rows)
    return len(rows)

def main():
    log("Inicio ETL")
    conn = db_connect()
    try:
        ensure_table(conn)

        targets = compute_target_dates()
        to_fix = dates_needing_update(conn, targets)
        log(f"Fechas a insertar/actualizar: {', '.join(map(str, to_fix)) or 'ninguna'}")
        if not to_fix:
            log("Nada que hacer. Fin."); return

        start_date, end_date = min(to_fix), max(to_fix)
        response = fetch_open_meteo_daily(start_date, end_date)
        rows = map_response_to_rows(response, set(to_fix))
        inserted = upsert(conn, rows)
        log(f"Filas insertadas/actualizadas: {inserted}")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
