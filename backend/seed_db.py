"""
Генератор тестовой базы Drivee.
Запуск: python seed_db.py
Создаёт data/drivee.db с ~50 000 заказов за последние 90 дней.
"""
import sqlite3
import random
import uuid
from datetime import datetime, timedelta
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "data" / "drivee.db"
DB_PATH.parent.mkdir(exist_ok=True)

random.seed(42)

CITIES = [
    ("Москва", 1.0, 12600000),
    ("Санкт-Петербург", 0.45, 5400000),
    ("Новосибирск", 0.12, 1620000),
    ("Екатеринбург", 0.10, 1490000),
    ("Казань", 0.09, 1250000),
    ("Краснодар", 0.08, 970000),
    ("Нижний Новгород", 0.07, 1240000),
    ("Ростов-на-Дону", 0.06, 1130000),
]

CAR_CLASSES = [("economy", 0.55, 350), ("comfort", 0.30, 500),
                ("business", 0.10, 900), ("minivan", 0.05, 700)]
PAYMENT_METHODS = [("card", 0.65), ("cash", 0.15), ("corporate", 0.12), ("wallet", 0.08)]


def weighted_choice(options):
    """options = [(value, weight, ...), ...] — выбирает по весу."""
    total = sum(o[1] for o in options)
    r = random.uniform(0, total)
    acc = 0
    for o in options:
        acc += o[1]
        if r <= acc:
            return o
    return options[-1]


def main():
    if DB_PATH.exists():
        DB_PATH.unlink()
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()

    # --- Схема ---
    cur.executescript("""
        CREATE TABLE cities (
            city_id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            population INTEGER
        );
        CREATE TABLE drivers (
            driver_id TEXT PRIMARY KEY,
            registered_at TIMESTAMP,
            city_id INTEGER,
            avg_rating REAL,
            total_rides INTEGER,
            FOREIGN KEY (city_id) REFERENCES cities(city_id)
        );
        CREATE TABLE clients (
            client_id TEXT PRIMARY KEY,
            registered_at TIMESTAMP,
            city_id INTEGER,
            is_corporate INTEGER DEFAULT 0,
            FOREIGN KEY (city_id) REFERENCES cities(city_id)
        );
        CREATE TABLE orders (
            order_id TEXT PRIMARY KEY,
            created_at TIMESTAMP NOT NULL,
            completed_at TIMESTAMP,
            status TEXT NOT NULL,
            city_id INTEGER NOT NULL,
            driver_id TEXT,
            client_id TEXT NOT NULL,
            car_class TEXT,
            payment_method TEXT,
            price_rub REAL,
            distance_km REAL,
            duration_min INTEGER,
            rating INTEGER,
            pickup_wait_sec INTEGER,
            surge_multiplier REAL DEFAULT 1.0,
            promo_code TEXT,
            FOREIGN KEY (city_id) REFERENCES cities(city_id)
        );
        CREATE INDEX idx_orders_created ON orders(created_at);
        CREATE INDEX idx_orders_city ON orders(city_id);
        CREATE INDEX idx_orders_status ON orders(status);
    """)

    # --- Города ---
    city_ids = {}
    for i, (name, _, pop) in enumerate(CITIES, start=1):
        cur.execute("INSERT INTO cities VALUES (?, ?, ?)", (i, name, pop))
        city_ids[name] = i

    # --- Водители (по 50-500 на город пропорционально размеру) ---
    drivers_by_city = {}
    now = datetime.now()
    for name, weight, _ in CITIES:
        n_drivers = int(50 + weight * 450)
        drivers_by_city[city_ids[name]] = []
        for _ in range(n_drivers):
            did = str(uuid.uuid4())
            reg = now - timedelta(days=random.randint(30, 730))
            rating = round(random.uniform(4.2, 5.0), 2)
            cur.execute(
                "INSERT INTO drivers VALUES (?, ?, ?, ?, ?)",
                (did, reg.isoformat(), city_ids[name], rating, 0),
            )
            drivers_by_city[city_ids[name]].append(did)

    # --- Клиенты ---
    clients_by_city = {}
    for name, weight, _ in CITIES:
        n_clients = int(200 + weight * 2000)
        clients_by_city[city_ids[name]] = []
        for _ in range(n_clients):
            cid = str(uuid.uuid4())
            reg = now - timedelta(days=random.randint(1, 500))
            is_corp = 1 if random.random() < 0.08 else 0
            cur.execute(
                "INSERT INTO clients VALUES (?, ?, ?, ?)",
                (cid, reg.isoformat(), city_ids[name], is_corp),
            )
            clients_by_city[city_ids[name]].append(cid)

    # --- Заказы за 90 дней ---
    orders = []
    base_daily = 400  # базовое число заказов в день по всей сети

    for day_offset in range(90):
        day = now - timedelta(days=90 - day_offset)

        # Недельная сезонность: пятница/суббота +30%, воскресенье -10%
        dow = day.weekday()
        seasonal = {0: 1.0, 1: 1.02, 2: 1.05, 3: 1.1, 4: 1.3, 5: 1.25, 6: 0.9}[dow]

        # Небольшая растущая тенденция
        trend = 1 + day_offset * 0.002

        daily_total = int(base_daily * seasonal * trend)

        for _ in range(daily_total):
            # Время суток — два пика
            hour_r = random.random()
            if hour_r < 0.25:
                hour = random.choice([7, 8, 9])  # утренний пик
            elif hour_r < 0.55:
                hour = random.choice([17, 18, 19, 20])  # вечерний пик
            else:
                hour = random.randint(0, 23)
            minute = random.randint(0, 59)
            created = day.replace(hour=hour, minute=minute, second=random.randint(0, 59))

            city_name, city_weight, _ = weighted_choice(CITIES)
            city_id = city_ids[city_name]

            cc_name, _, base_price = weighted_choice(CAR_CLASSES)
            pm_name, _ = weighted_choice(PAYMENT_METHODS)

            # surge в пиковые часы
            surge = 1.0
            if hour in (8, 9, 18, 19, 20) and random.random() < 0.25:
                surge = round(random.uniform(1.2, 2.0), 1)

            # Статус (в разных городах разная доля отмен для реалистичности)
            city_cancel_rate = {
                "Москва": 0.14, "Санкт-Петербург": 0.12, "Новосибирск": 0.18,
                "Екатеринбург": 0.15, "Казань": 0.11, "Краснодар": 0.16,
                "Нижний Новгород": 0.17, "Ростов-на-Дону": 0.19,
            }.get(city_name, 0.15)

            r = random.random()
            if r < city_cancel_rate * 0.6:
                status = "cancelled_by_client"
            elif r < city_cancel_rate:
                status = "cancelled_by_driver"
            elif r < city_cancel_rate + 0.03:
                status = "no_cars"
            else:
                status = "completed"

            # Заполнение полей
            driver_id = random.choice(drivers_by_city[city_id]) if status != "no_cars" else None
            client_id = random.choice(clients_by_city[city_id])

            price = None
            completed = None
            rating = None
            distance = None
            duration = None

            pickup_wait = random.randint(60, 300) if status != "no_cars" else None
            if surge > 1 and pickup_wait:
                pickup_wait = int(pickup_wait * 1.5)

            if status == "completed":
                distance = round(random.uniform(1.5, 25.0), 1)
                base = base_price + distance * 25
                price = round(base * surge, 2)
                duration = int(distance * random.uniform(2.5, 4.5))
                completed = created + timedelta(minutes=duration + random.randint(2, 6))
                rating = random.choices([5, 4, 3, 2, 1], weights=[75, 15, 6, 3, 1])[0]

            promo = f"PROMO{random.randint(100, 999)}" if random.random() < 0.05 else None

            orders.append((
                str(uuid.uuid4()),
                created.isoformat(),
                completed.isoformat() if completed else None,
                status,
                city_id,
                driver_id,
                client_id,
                cc_name,
                pm_name,
                price,
                distance,
                duration,
                rating,
                pickup_wait,
                surge,
                promo,
            ))

    cur.executemany(
        "INSERT INTO orders VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        orders,
    )

    con.commit()
    n_orders = cur.execute("SELECT COUNT(*) FROM orders").fetchone()[0]
    n_completed = cur.execute(
        "SELECT COUNT(*) FROM orders WHERE status='completed'"
    ).fetchone()[0]
    n_cancelled = cur.execute(
        "SELECT COUNT(*) FROM orders WHERE status LIKE 'cancelled%'"
    ).fetchone()[0]
    n_drivers = cur.execute("SELECT COUNT(*) FROM drivers").fetchone()[0]
    n_clients = cur.execute("SELECT COUNT(*) FROM clients").fetchone()[0]
    con.close()

    print(f"База создана: {DB_PATH}")
    print(f"  Заказов: {n_orders:,} (завершённых {n_completed:,}, отменённых {n_cancelled:,})")
    print(f"  Водителей: {n_drivers:,}, клиентов: {n_clients:,}")
    print(f"  Городов: {len(CITIES)}")
    print(f"  Период: последние 90 дней")


if __name__ == "__main__":
    main()
