CREATE TABLE IF NOT EXISTS hippodromes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    code TEXT UNIQUE,
    libelleCourt TEXT,
    libelleLong TEXT,
    ville TEXT,
    types_piste TEXT,
    terrains TEXT,
    longueurs_pistes TEXT,
    corde TEXT,
);

CREATE TABLE IF NOT EXISTS courses (
    course_id INTEGER PRIMARY KEY AUTOINCREMENT,
    date TEXT,
    categorie TEXT,
    course_externe TEXT,
    libelle TEXT,
    hippodrome_id INTEGER,
    terrain_id INTEGER,
    discipline TEXT,
    specialite TEXT,
    distance INTEGER,
    heure_depart INTEGER,
    duree INTEGER,
    nombre_declares INTEGER,
    FOREIGN KEY(hippodrome_id) REFERENCES hippodromes(id)
    FOREIGN KEY(terrain_id) REFERENCES terrain(id)
);

CREATE TABLE IF NOT EXISTS horses (
    horse_id INTEGER PRIMARY KEY AUTOINCREMENT,
    nom TEXT UNIQUE,
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

CREATE TABLE IF NOT EXISTS terrain (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    type TEXT,
    etat TEXT
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