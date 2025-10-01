import sqlite3
import requests
import json

# --- Configuration ---
DATABASE_FILE = 'pmu.sqlite'
FEDERATION_CODES = ['haute-normandie']  # Liste brute des fédérations à traiter
BASE_API_URL = 'https://regions-api.equidia.fr'

# --- Fonctions d'aide à l'extraction ---

def extract_disciplines(discipline_data):
    """Extrait les disciplines à partir de l'objet 'discipline' de l'API."""
    disciplines = []
    if discipline_data.get('flat'):
        disciplines.append('Plat')
    if discipline_data.get('harnessTrot'):
        disciplines.append('Trot Attelé')
    if discipline_data.get('mountedTrot'):
        disciplines.append('Trot Monté')
    if discipline_data.get('barrierStChase'):
        disciplines.append('Obstacle')
    return ', '.join(disciplines)

def extract_track_lengths(distance_data):
    """Extrait et formate les longueurs de pistes à partir de l'objet 'distance' de l'API."""
    lengths = []
    if distance_data.get('gallop'):
        lengths.append(f"Plat: {', '.join(map(str, distance_data['gallop']))}m")
    if distance_data.get('trot'):
        lengths.append(f"Trot: {', '.join(map(str, distance_data['trot']))}m")
    if distance_data.get('hurle'):
        lengths.append(f"Haies: {', '.join(map(str, distance_data['hurle']))}m")
    if distance_data.get('steeple'):
        lengths.append(f"Steeple: {', '.join(map(str, distance_data['steeple']))}m")
    return '; '.join(lengths)

def extract_corde(string_data):
    """Détermine la corde (main) de la piste."""
    if string_data.get('left'):
        return 'Gauche'
    elif string_data.get('right'):
        return 'Droite'
    return 'Non spécifié'

# --- Fonctions principales de la BD ---

def setup_database(conn):
    """Ajoute les colonnes nécessaires à la table hippodromes si elles n'existent pas."""
    print("Vérification et ajout des colonnes à la table hippodromes...")
    cursor = conn.cursor()
    
    # Liste des colonnes à ajouter (colonne, type)
    new_columns = [
        ('ville', 'TEXT'),
        ('types_piste', 'TEXT'),
        ('longueurs_pistes', 'TEXT'),
        ('corde', 'TEXT'),
    ]

    try:
        # Vérification simple par un SELECT et gestion de l'erreur
        cursor.execute("SELECT ville, types_piste, longueurs_pistes, corde FROM hippodromes LIMIT 1")
    except sqlite3.OperationalError:
        # Si une colonne n'existe pas, OperationalError est levée
        # Nous allons donc ajouter chaque colonne manquante individuellement
        
        # Récupérer la liste des colonnes existantes pour éviter les erreurs
        cursor.execute("PRAGMA table_info(hippodromes)")
        existing_columns = [info[1] for info in cursor.fetchall()]
        
        for col_name, col_type in new_columns:
            if col_name not in existing_columns:
                try:
                    cursor.execute(f"ALTER TABLE hippodromes ADD COLUMN {col_name} {col_type}")
                    print(f"  -> Colonne '{col_name}' ajoutée.")
                except sqlite3.OperationalError as e:
                    print(f"  -> Erreur lors de l'ajout de la colonne '{col_name}': {e}")
                    
    conn.commit()
    print("Mise en place de la BD terminée.")
    
def get_hippodrome_details(slug):
    """Récupère les informations détaillées d'un hippodrome via l'API 2."""
    print("get_hippodrome_detail")
    url = f"{BASE_API_URL}/race-courses/{slug}"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        return response.json().get('raceCourse')
    except requests.RequestException as e:
        print(f"Erreur lors de la récupération des détails pour le slug '{slug}': {e}")
        return None

def process_federation_data(conn, fed_code):
    """
    Récupère la liste des hippodromes de la fédération,
    les insère ou les met à jour.
    """
    url = f"{BASE_API_URL}/federation/{fed_code}"
    print(f"\n--- Traitement de la fédération : {fed_code} ---")
    
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        race_courses = data.get('raceCourses')
        print(race_courses)
    except requests.RequestException as e:
        print(f"Erreur lors de la récupération de la fédération '{fed_code}': {e}")
        return

    for rc in race_courses:
        code = rc['code']
        name = rc['name']
        slug = rc['slug']
        place = rc['place']
        
        print(f"  -> Traitement de l'hippodrome : {code} - {name}...")

        cursor = conn.cursor()
        
        # 1. Insertion si l'hippodrome n'existe pas
        cursor.execute("SELECT 1 FROM hippodromes WHERE code = ?", (code,))
        if cursor.fetchone() is None:
            # Insertion initiale avec les données de base (JSON 1)
            cursor.execute(
                "INSERT INTO hippodromes (code, libelleCourt, libelleLong, ville) VALUES (?, ?, ?, ?)",
                (code, name, name, place) # On utilise 'name' pour libelleLong par défaut
            )
            print(f"    [INSERTION] Nouvel hippodrome ajouté : {code}")
        else:
            print(f"    [EXISTANT] Hippodrome déjà présent : {code}")
        
        # 2. Enrichissement (Mise à jour) avec les détails (JSON 2)
        details = get_hippodrome_details(slug)
        
        if details:
            # Extraction et formatage des données
            ville = details.get('address', {}).get('city', place) # Utilise la ville de l'adresse si dispo, sinon 'place'
            types_piste = extract_disciplines(details.get('discipline', {}))
            longueurs_pistes = extract_track_lengths(details.get('distance', {}))
            corde = extract_corde(details.get('string', {}))
            
            # Mise à jour dans la BD
            cursor.execute(
                """
                UPDATE hippodromes
                SET ville = ?, types_piste = ?, longueurs_pistes = ?, corde = ?
                WHERE code = ?
                """,
                (ville, types_piste, longueurs_pistes, corde, code)
            )
            print(f"    [MISE À JOUR] Détails enrichis pour : {code} (Ville: {ville}, Corde: {corde})")
        else:
            print(f"    [ÉCHEC] Impossible de récupérer les détails pour : {code}")
            
        conn.commit()

# --- Exécution principale ---

def main():
    try:
        # Connexion à la base de données SQLite
        conn = sqlite3.connect(DATABASE_FILE)
        
        # Étape 1 : Vérifier et mettre à jour la structure de la BD
        setup_database(conn)
        
        # Étape 2 : Parcourir chaque fédération pour récupérer et traiter les données
        for fed_code in FEDERATION_CODES:
            process_federation_data(conn, fed_code)
            
        print("\n✅ Traitement de toutes les fédérations terminé avec succès.")
        
    except sqlite3.Error as e:
        print(f"\n❌ Erreur SQLite : {e}")
    except Exception as e:
        print(f"\n❌ Une erreur inattendue est survenue : {e}")
    finally:
        if 'conn' in locals() and conn:
            conn.close()
            print("Connexion à la base de données fermée.")

if __name__ == "__main__":
    main()