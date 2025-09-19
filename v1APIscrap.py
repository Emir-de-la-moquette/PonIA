import requests
import sqlite3
import time
from datetime import datetime, timedelta

# === CONFIG ===
BASE = "https://offline.turfinfo.api.pmu.fr/rest/client/1"
DB_NAME = "pmu_full.db"
START_DATE = datetime(2020, 1, 1)
END_DATE = datetime.today() - timedelta(days=1)
SLEEP_BASE = 0.1  # secondes entre requêtes

# === SQLite setup ===
conn = sqlite3.connect(DB_NAME)
cur = conn.cursor()

# Table courses
cur.execute("""
CREATE TABLE IF NOT EXISTS courses (
    course_id INTEGER PRIMARY KEY AUTOINCREMENT,
    date TEXT,
    reunion TEXT,
    course_externe TEXT,         -- ex: "C1"
    libelle TEXT,
    hippodrome TEXT,
    discipline TEXT,
    distance INTEGER,
    nombre_declares INTEGER
)
""")

# Table horses
cur.execute("""
CREATE TABLE IF NOT EXISTS horses (
    horse_id INTEGER PRIMARY KEY AUTOINCREMENT,
    numPmu INTEGER UNIQUE,
    nom TEXT,
    age INTEGER,
    sexe TEXT
)
""")

# Table trainers
cur.execute("""
CREATE TABLE IF NOT EXISTS trainers (
    trainer_id INTEGER PRIMARY KEY AUTOINCREMENT,
    nom TEXT UNIQUE
)
""")

# Table drivers
cur.execute("""
CREATE TABLE IF NOT EXISTS drivers (
    driver_id INTEGER PRIMARY KEY AUTOINCREMENT,
    nom TEXT UNIQUE
)
""")

# Table participants (liaison course <-> cheval)
cur.execute("""
CREATE TABLE IF NOT EXISTS participants (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    course_id INTEGER,
    horse_id INTEGER,
    trainer_id INTEGER,
    driver_id INTEGER,
    ordreArrivee INTEGER,
    temps TEXT,
    rapport_direct REAL,
    rapport_ref REAL,
    FOREIGN KEY(course_id) REFERENCES courses(course_id),
    FOREIGN KEY(horse_id) REFERENCES horses(horse_id),
    FOREIGN KEY(trainer_id) REFERENCES trainers(trainer_id),
    FOREIGN KEY(driver_id) REFERENCES drivers(driver_id)
)
""")

# Table pour reprendre
cur.execute("""
CREATE TABLE IF NOT EXISTS progress (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    last_date TEXT
)
""")

cur.execute("INSERT OR IGNORE INTO progress (id, last_date) VALUES (1, NULL)")
conn.commit()

# === Fonctions utilitaires ===

def safe_get(url):
    try:
        r = requests.get(url, timeout=15)
        if r.status_code == 200:
            return r.json()
        else:
            print(f"[WARN] {url} -> {r.status_code}")
            return None
    except Exception as e:
        print(f"[ERROR] {url} -> {e}")
        return None

def safe_name(field):
    if not field:
        return None
    if isinstance(field, dict):
        return field.get("nom") or field.get("name")
    if isinstance(field, str):
        return field
    return None

def get_or_create_trainer(name):
    if not name:
        return None
    cur.execute("SELECT trainer_id FROM trainers WHERE nom = ?", (name,))
    row = cur.fetchone()
    if row:
        return row[0]
    cur.execute("INSERT INTO trainers (nom) VALUES (?)", (name,))
    conn.commit()
    return cur.lastrowid

def get_or_create_driver(name):
    if not name:
        return None
    cur.execute("SELECT driver_id FROM drivers WHERE nom = ?", (name,))
    row = cur.fetchone()
    if row:
        return row[0]
    cur.execute("INSERT INTO drivers (nom) VALUES (?)", (name,))
    conn.commit()
    return cur.lastrowid

def get_or_create_horse(numPmu, nom, age, sexe):
    if numPmu is None:
        # S'il n'y a pas de numPmu, on peut tenter par nom, mais attention aux doublons
        cur.execute("SELECT horse_id FROM horses WHERE nom = ? AND numPmu IS NULL", (nom,))
    else:
        cur.execute("SELECT horse_id FROM horses WHERE numPmu = ?", (numPmu,))
    row = cur.fetchone()
    if row:
        return row[0]
    cur.execute("""
        INSERT INTO horses (numPmu, nom, age, sexe) 
        VALUES (?, ?, ?, ?)
    """, (numPmu, nom, age, sexe))
    conn.commit()
    return cur.lastrowid

def process_date(date_str):
    print(f"\n=== Traitement du {date_str} ===")

    prog_url = f"{BASE}/programme/{date_str}"
    data = safe_get(prog_url)
    if not data or "programme" not in data:
        print(f"[NO DATA] {date_str} : impossible de récupérer le programme")
        return

    reunions = data["programme"].get("reunions", [])
    if not reunions:
        print(f"[NO REUNION] {date_str} : aucune réunion trouvée")
        return

    for reunion in reunions:
        r_code = f"R{reunion['numOfficiel']}"
        hippodrome = reunion.get("hippodrome", {}).get("libelleLong", "?")
        print(f"-- Réunion {r_code} ({hippodrome}) --")

        courses = reunion.get("courses", [])
        if not courses:
            print(f"[NO COURSE] {date_str} {r_code} : aucune course trouvée")
            continue

        for course in courses:
            c_code = f"C{course['numExterne']}"
            print(f"   > Course {c_code} : {course.get('libelle', '???')}")

            # Insérer la course
            cur.execute("""
                INSERT INTO courses 
                (date, reunion, course_externe, libelle, hippodrome, discipline, distance, nombre_declares)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                date_str,
                r_code,
                c_code,
                course.get("libelle"),
                hippodrome,
                course.get("discipline"),
                course.get("distance"),
                course.get("nombreDeclaresPartants")
            ))
            course_db_id = cur.lastrowid
            conn.commit()

            # Récupérer les participants
            part_url = f"{BASE}/programme/{date_str}/{r_code}/{c_code}/participants"
            part_data = safe_get(part_url)
            if not part_data or "participants" not in part_data:
                print("      [NO PARTICIPANTS]")
                continue

            for p in part_data["participants"]:
                numPmu = p.get("numPmu")
                nom_h = p.get("nom")
                age = p.get("age")
                sexe = p.get("sexe")

                horse_db_id = get_or_create_horse(numPmu, nom_h, age, sexe)

                name_trainer = safe_name(p.get("entraineur"))
                trainer_db_id = get_or_create_trainer(name_trainer)

                name_driver = safe_name(p.get("driver"))
                driver_db_id = get_or_create_driver(name_driver)

                ordre = p.get("ordreArrivee")
                temps = p.get("tempsObtenu")
                rap_direct = (p.get("dernierRapportDirect") or {}).get("rapport")
                rap_ref = (p.get("dernierRapportReference") or {}).get("rapport")

                cur.execute("""
                    INSERT INTO participants
                    (course_id, horse_id, trainer_id, driver_id, ordreArrivee, temps, rapport_direct, rapport_ref)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    course_db_id, horse_db_id, trainer_db_id, driver_db_id, ordre, temps, rap_direct, rap_ref
                ))
            conn.commit()
            print(f"      [OK] {len(part_data['participants'])} participants traités")

            time.sleep(SLEEP_BASE)

    # fin réunions ce jour
    time.sleep(SLEEP_BASE * 2)


def main():
    # reprise automatique
    cur.execute("SELECT last_date FROM progress WHERE id = 1")
    row = cur.fetchone()
    if row and row[0]:
        d = datetime.strptime(row[0], "%d%m%Y") + timedelta(days=1)
        print(f"Reprise depuis le {d.strftime('%d/%m/%Y')}")
    else:
        d = START_DATE
        print(f"Démarrage depuis le {START_DATE.strftime('%d/%m/%Y')}")

    while d <= END_DATE:
        date_str = d.strftime("%d%m%Y")
        process_date(date_str)

        # mettre à jour progrès
        cur.execute("UPDATE progress SET last_date = ? WHERE id = 1", (date_str,))
        conn.commit()

        d += timedelta(days=1)
    conn.close()
    print("Import complet terminé ✅")

if __name__ == "__main__":
    main()
