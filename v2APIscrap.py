import requests
import sqlite3
import time
from datetime import datetime, timedelta

BASE = "https://offline.turfinfo.api.pmu.fr/rest/client/1"
DB_FILE = "pmu.sqlite"

# Delai de base pour API calls
BASE_SLEEP = 0.01

conn = sqlite3.connect(DB_FILE)
cur = conn.cursor()

# -------------------- CREATE TABLES -------------------- #
cur.executescript("""
CREATE TABLE IF NOT EXISTS hippodromes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    code TEXT UNIQUE,
    libelleCourt TEXT,
    libelleLong TEXT
);

CREATE TABLE IF NOT EXISTS courses (
    course_id INTEGER PRIMARY KEY AUTOINCREMENT,
    date TEXT,
    reunion TEXT,
    course_externe TEXT,
    libelle TEXT,
    hippodrome_id INTEGER,
    discipline TEXT,
    distance INTEGER,
    heure_depart INTEGER,
    nombre_declares INTEGER,
    FOREIGN KEY(hippodrome_id) REFERENCES hippodromes(id)
);

CREATE TABLE IF NOT EXISTS horses (
    horse_id INTEGER PRIMARY KEY AUTOINCREMENT,
    numPmu INTEGER UNIQUE,
    nom TEXT,
    age INTEGER,
    sexe TEXT
);

CREATE TABLE IF NOT EXISTS trainers (
    trainer_id INTEGER PRIMARY KEY AUTOINCREMENT,
    nom TEXT UNIQUE
);

CREATE TABLE IF NOT EXISTS drivers (
    driver_id INTEGER PRIMARY KEY AUTOINCREMENT,
    nom TEXT UNIQUE
);

CREATE TABLE IF NOT EXISTS participants (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    course_id INTEGER,
    horse_id INTEGER,
    trainer_id INTEGER,
    driver_id INTEGER,
    ordreArrivee INTEGER,
    temps INTEGER,
    rapport_direct REAL,
    rapport_ref REAL,
    courses_courues INTEGER,
    courses_gagnees INTEGER,
    courses_placees INTEGER,
    distance_reelle INTEGER,
    disqualifie BOOLEAN,
    FOREIGN KEY(course_id) REFERENCES courses(course_id),
    FOREIGN KEY(horse_id) REFERENCES horses(horse_id),
    FOREIGN KEY(trainer_id) REFERENCES trainers(trainer_id),
    FOREIGN KEY(driver_id) REFERENCES drivers(driver_id)
);
""")
conn.commit()

# -------------------- HELPERS -------------------- #
def safe_get(url):
    try:
        r = requests.get(url)
        if r.status_code == 200:
            return r.json()
        else:
            print(f"[ERROR] {url} returned {r.status_code}")
            return None
    except Exception as e:
        print(f"[EXCEPTION] {url} -> {e}")
        return None

def get_or_create_horse(numPmu, nom, age, sexe):
    cur.execute("SELECT horse_id FROM horses WHERE numPmu=?", (numPmu,))
    res = cur.fetchone()
    if res:
        return res[0]
    cur.execute("INSERT INTO horses (numPmu, nom, age, sexe) VALUES (?, ?, ?, ?)",
                (numPmu, nom, age, sexe))
    conn.commit()
    return cur.lastrowid

def get_or_create_trainer(nom):
    cur.execute("SELECT trainer_id FROM trainers WHERE nom=?", (nom,))
    res = cur.fetchone()
    if res:
        return res[0]
    cur.execute("INSERT OR IGNORE INTO trainers (nom) VALUES (?)", (nom,))
    conn.commit()
    return cur.lastrowid

def get_or_create_driver(nom):
    cur.execute("SELECT driver_id FROM drivers WHERE nom=?", (nom,))
    res = cur.fetchone()
    if res:
        return res[0]
    cur.execute("INSERT OR IGNORE INTO drivers (nom) VALUES (?)", (nom,))
    conn.commit()
    return cur.lastrowid

def get_or_create_hippodrome(code, libCourt, libLong):
    cur.execute("SELECT id FROM hippodromes WHERE code=?", (code,))
    res = cur.fetchone()
    if res:
        return res[0]
    cur.execute("INSERT INTO hippodromes (code, libelleCourt, libelleLong) VALUES (?, ?, ?)",
                (code, libCourt, libLong))
    conn.commit()
    return cur.lastrowid

# -------------------- MAIN PROCESS -------------------- #
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
        numR = f"{reunion['numOfficiel']}"
        hippo = reunion.get("hippodrome", {})
        hipp_id = get_or_create_hippodrome(
            hippo.get("code"),
            hippo.get("libelleCourt"),
            hippo.get("libelleLong")
        )
        print(f"-- Réunion R{numR} ({hippo.get('libelleCourt')}) --")

        courses = reunion.get("courses", [])
        for course in courses:
            course_id_externe = course.get("numExterne")
            libelle = course.get("libelle")
            discipline = course.get("discipline")
            distance = course.get("distance")
            heure_depart = course.get("heureDepart")
            nombre_declares = course.get("nombreDeclaresPartants")

            cur.execute("""INSERT INTO courses
                (date, reunion, course_externe, libelle, hippodrome_id,
                discipline, distance, heure_depart, nombre_declares)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (date_str, numR, course_id_externe, libelle, hipp_id,
                discipline, distance, heure_depart, nombre_declares)
            )
            conn.commit()
            course_db_id = cur.lastrowid

            print(f"   > Course {course.get('numExterne')} : {libelle}")

            # Participants
            participants_url = f"{BASE}/programme/{date_str}/R{numR}/C{course_id_externe}/participants"
            p_data = safe_get(participants_url)
            if not p_data:
                print(f"   [NO PARTICIPANTS] Course {course_id_externe}")
                continue

            for p in p_data.get("participants", []):
                horse_id = get_or_create_horse(p.get("numPmu"), p.get("nom"), p.get("age"), p.get("sexe"))
                trainer_id = get_or_create_trainer(p.get("entraineur"))
                driver_id = get_or_create_driver(p.get("driver"))

                ordre = p.get("ordreArrivee")
                temps = p.get("tempsObtenu")
                rap_direct = (p.get("dernierRapportDirect") or {}).get("rapport")
                rap_ref = (p.get("dernierRapportReference") or {}).get("rapport")
                courses_courues = p.get("nombreCourses")
                courses_gagnees = p.get("nombreVictoires")
                courses_placees = p.get("nombrePlaces")
                distance_reelle = p.get("handicapDistance")
                disqualifie = p.get("incident") == "DISQUALIFIE_POUR_ALLURE_IRREGULIERE"

                cur.execute("""
                    INSERT INTO participants
                    (course_id, horse_id, trainer_id, driver_id, ordreArrivee, temps,
                     rapport_direct, rapport_ref, courses_courues, courses_gagnees,
                     courses_placees, distance_reelle, disqualifie)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    course_db_id, horse_id, trainer_id, driver_id,
                    ordre, temps, rap_direct, rap_ref, courses_courues, courses_gagnees,
                    courses_placees, distance_reelle, disqualifie
                ))
            conn.commit()
            time.sleep(BASE_SLEEP)

# -------------------- LOOP OVER DATES -------------------- #
def main():
    start_date = datetime(2020, 1, 1)
    end_date = datetime.now() - timedelta(days=1)

    delta = timedelta(days=1)
    current = start_date
    while current <= end_date:
        date_str = current.strftime("%d%m%Y")
        process_date(date_str)
        time.sleep(BASE_SLEEP * 2)  # délai plus long entre les jours
        current += delta

if __name__ == "__main__":
    main()
