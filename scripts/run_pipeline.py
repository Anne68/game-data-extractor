# scripts/run_pipeline.py - VERSION FINALE CORRIGÉE
import os
import time
from datetime import datetime
import pandas as pd
import requests
import mysql.connector
from mysql.connector import Error

# =========================
#   CONFIG / DB CONNECTION
# =========================
RAWG_API_KEY = os.getenv("RAWG_API_KEY")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = int(os.getenv("DB_PORT", "3306"))
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")
SCRAPE_LIMIT = int(os.getenv("SCRAPE_LIMIT", "20"))

# Validation
if not RAWG_API_KEY:
    raise RuntimeError("RAWG_API_KEY manquant.")
for k, v in [("DB_HOST", DB_HOST), ("DB_USER", DB_USER), ("DB_PASSWORD", DB_PASSWORD), ("DB_NAME", DB_NAME)]:
    if not v:
        raise RuntimeError(f"Config DB incomplète: variable {k} manquante.")

print(f"🔧 Connexion: {DB_USER}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

def get_mysql_connection():
    """Récupère une connexion MySQL directe"""
    try:
        conn = mysql.connector.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME,
            charset='utf8mb4',
            autocommit=True
        )
        
        if conn.is_connected():
            return conn
        else:
            return None
            
    except Error as e:
        print(f"❌ Erreur connexion MySQL: {e}")
        return None

# Test de connexion
print("🧪 Test de connexion MySQL...")
test_conn = get_mysql_connection()
if test_conn:
    cursor = test_conn.cursor()
    cursor.execute("SELECT VERSION()")
    version = cursor.fetchone()
    print(f"✅ MySQL connecté - Version: {version[0]}")
    cursor.close()
    test_conn.close()
else:
    raise RuntimeError("Impossible de se connecter à MySQL")

# =========================
#   SETUP TABLE API_STATE
# =========================
def ensure_api_state_table():
    """S'assure que la table api_state existe avec toutes les colonnes"""
    conn = get_mysql_connection()
    if not conn:
        return False
    
    try:
        cursor = conn.cursor()
        
        # Créer la table si elle n'existe pas
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS api_state (
                id INT PRIMARY KEY,
                last_page INT NOT NULL DEFAULT 65,
                last_extraction DATETIME,
                total_games_extracted INT DEFAULT 0
            )
        """)
        
        # Vérifier les colonnes existantes
        cursor.execute("DESCRIBE api_state")
        columns = [col[0] for col in cursor.fetchall()]
        
        # Ajouter les colonnes manquantes
        if 'last_extraction' not in columns:
            cursor.execute("ALTER TABLE api_state ADD COLUMN last_extraction DATETIME")
            print("✅ Colonne last_extraction ajoutée")
        
        if 'total_games_extracted' not in columns:
            cursor.execute("ALTER TABLE api_state ADD COLUMN total_games_extracted INT DEFAULT 0")
            print("✅ Colonne total_games_extracted ajoutée")
        
        # Initialiser si vide
        cursor.execute("SELECT COUNT(*) FROM api_state")
        if cursor.fetchone()[0] == 0:
            cursor.execute("INSERT INTO api_state (id, last_page, total_games_extracted) VALUES (1, 64, 0)")
            print("✅ État initial créé")
        
        cursor.close()
        return True
        
    except Error as e:
        print(f"❌ Erreur setup api_state: {e}")
        return False
    finally:
        conn.close()

# =========================
#   GESTION SÉQUENTIELLE
# =========================
def get_next_page_to_extract():
    """Détermine la prochaine page à extraire"""
    conn = get_mysql_connection()
    if not conn:
        return 65
    
    try:
        cursor = conn.cursor()
        
        cursor.execute("SELECT last_page, total_games_extracted FROM api_state WHERE id = 1")
        result = cursor.fetchone()
        
        if result:
            last_page, total_extracted = result
            next_page = last_page + 1
            print(f"📋 Reprise depuis la page {next_page} ({total_extracted} jeux extraits au total)")
        else:
            next_page = 65
            cursor.execute("INSERT INTO api_state (id, last_page, total_games_extracted) VALUES (1, 64, 0)")
            print(f"🆕 Première extraction, démarrage page {next_page}")
        
        cursor.close()
        return next_page
        
    except Error as e:
        print(f"❌ Erreur récupération état: {e}")
        return 65
    finally:
        conn.close()

def update_extraction_state(last_page, games_extracted):
    """Met à jour l'état d'extraction"""
    conn = get_mysql_connection()
    if not conn:
        return False
    
    try:
        cursor = conn.cursor()
        
        cursor.execute("""
            UPDATE api_state 
            SET last_page = %s, 
                last_extraction = NOW(), 
                total_games_extracted = total_games_extracted + %s
            WHERE id = 1
        """, (last_page, games_extracted))
        
        print(f"✅ État mis à jour: page {last_page}, +{games_extracted} jeux")
        cursor.close()
        return True
        
    except Error as e:
        print(f"❌ Erreur mise à jour état: {e}")
        return False
    finally:
        conn.close()

def fetch_exactly_50_games():
    """Récupère exactement 50 nouveaux jeux"""
    start_page = get_next_page_to_extract()
    
    base = "https://api.rawg.io/api/games"
    headers = {"Accept": "application/json"}
    
    all_games = []
    current_page = start_page
    target_games = 50
    
    print(f"🎯 Objectif: récupérer {target_games} jeux depuis la page {start_page}")
    
    while len(all_games) < target_games:
        try:
            params = {
                "key": RAWG_API_KEY, 
                "ordering": "id",
                "page_size": 40,
                "page": current_page
            }
            
            print(f"📄 Traitement page {current_page}...")
            r = requests.get(base, params=params, headers=headers, timeout=20)
            r.raise_for_status()
            
            data = r.json()
            page_results = data.get("results", [])
            
            if not page_results:
                print(f"⚠️ Aucun résultat page {current_page}, arrêt")
                break
            
            for g in page_results:
                if len(all_games) >= target_games:
                    break
                    
                game_data = {
                    "game_id_rawg": g.get("id"),
                    "title": g.get("name"),
                    "release_date": g.get("released"),
                    "genres": ", ".join([x["name"] for x in g.get("genres", [])]) if g.get("genres") else None,
                    "platforms": ", ".join([p["platform"]["name"] for p in g.get("platforms", [])]) if g.get("platforms") else None,
                    "rating": g.get("rating"),
                    "metacritic": g.get("metacritic"),
                    "last_update": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                }
                
                all_games.append(game_data)
            
            print(f"  → {len(page_results)} jeux sur la page, total: {len(all_games)}")
            
            current_page += 1
            time.sleep(0.8)
            
            if current_page - start_page > 10:
                print("⚠️ Limite de pages atteinte")
                break
                
        except Exception as e:
            print(f"❌ Erreur page {current_page}: {e}")
            break
    
    if all_games:
        games_added = len(all_games)
        last_processed_page = current_page - 1
        update_extraction_state(last_processed_page, games_added)
        
        print(f"✅ Extraction terminée: {games_added} jeux récupérés")
        
        if len(all_games) >= 3:
            print("🎮 Premiers jeux extraits:")
            for i in range(min(3, len(all_games))):
                game = all_games[i]
                print(f"  - {game['title']} (ID: {game['game_id_rawg']})")
    
    return pd.DataFrame(all_games)

def show_extraction_status():
    """Affiche l'état d'extraction"""
    conn = get_mysql_connection()
    if not conn:
        return
    
    try:
        cursor = conn.cursor()
        
        cursor.execute("SELECT last_page, last_extraction, total_games_extracted FROM api_state WHERE id = 1")
        result = cursor.fetchone()
        
        if result:
            last_page, last_extraction, total_extracted = result
            print(f"📊 État extraction séquentielle:")
            print(f"  • Dernière page traitée: {last_page}")
            print(f"  • Prochaine page: {last_page + 1}")  
            print(f"  • Total jeux extraits: {total_extracted}")
            print(f"  • Dernière extraction: {last_extraction}")
            
            estimated_min_id = (last_page - 1) * 40
            estimated_max_id = last_page * 40
            print(f"  • IDs approximatifs traités: {estimated_min_id}-{estimated_max_id}")
        else:
            print("📊 Aucun état d'extraction trouvé")
            
        cursor.close()
        
    except Error as e:
        print(f"❌ Erreur affichage état: {e}")
    finally:
        conn.close()

def get_database_stats():
    """Statistiques base de données"""
    conn = get_mysql_connection()
    if not conn:
        return
    
    try:
        cursor = conn.cursor()
        
        cursor.execute("SELECT COUNT(*) FROM games")
        total_games = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(DISTINCT game_id_rawg) FROM games")
        unique_games = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM best_price_pc")
        total_prices = cursor.fetchone()[0]
        
        cursor.execute("SELECT MAX(last_update) FROM games")
        last_update = cursor.fetchone()[0]
        
        print(f"\n📊 Statistiques base de données:")
        print(f"  🎮 Jeux: {total_games} total, {unique_games} uniques")
        print(f"  💰 Prix: {total_prices} total")
        print(f"  📅 Dernière MAJ: {last_update}")
        
        if total_games != unique_games:
            print(f"  ⚠️ {total_games - unique_games} doublons détectés")
        
        cursor.close()
        
    except Error as e:
        print(f"❌ Erreur stats: {e}")
    finally:
        conn.close()

def upsert_games(df: pd.DataFrame) -> int:
    """Insert/Update games - VERSION CORRIGÉE"""
    if df.empty:
        return 0
        
    conn = get_mysql_connection()
    if not conn:
        return 0
    
    inserted = 0
    updated = 0
    try:
        cursor = conn.cursor()
        
        for _, r in df.iterrows():
            cursor.execute("SELECT COUNT(*) FROM games WHERE game_id_rawg = %s", (r["game_id_rawg"],))
            exists = cursor.fetchone()[0] > 0
            
            # REQUÊTE CORRIGÉE sans background_image
            query = """
                INSERT INTO games (game_id_rawg, title, release_date, genres, platforms, rating, metacritic, last_update)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                  title=VALUES(title),
                  release_date=VALUES(release_date),
                  genres=VALUES(genres),
                  platforms=VALUES(platforms),
                  rating=VALUES(rating),
                  metacritic=VALUES(metacritic),
                  last_update=VALUES(last_update)
            """
            
            # VALUES CORRIGÉES - 8 paramètres au lieu de 9
            values = (
                r["game_id_rawg"], r["title"], r["release_date"],
                r["genres"], r["platforms"], r["rating"],
                r["metacritic"], r["last_update"]
            )
            
            cursor.execute(query, values)
            
            if exists:
                updated += 1
            else:
                inserted += 1
        
        cursor.close()
        print(f"✅ Jeux traités: {inserted} nouveaux, {updated} mis à jour")
        return inserted
        
    except Error as e:
        print(f"❌ Erreur upsert: {e}")
        return 0
    finally:
        conn.close()

# =========================
#   MAIN PRINCIPAL
# =========================
def main():
    print("🚀 Pipeline d'extraction séquentielle - 50 jeux par run")
    
    try:
        # 0. S'assurer que la table api_state est correcte
        ensure_api_state_table()
        
        # 1. État actuel
        show_extraction_status()
        
        # 2. Statistiques
        get_database_stats()
        
        # 3. Extraction de 50 jeux
        print("\n🎯 Extraction séquentielle de 50 nouveaux jeux...")
        new_games = fetch_exactly_50_games()
        
        if not new_games.empty:
            print(f"✅ {len(new_games)} jeux récupérés pour insertion")
            
            inserted = upsert_games(new_games)
            print(f"✅ {inserted} NOUVEAUX jeux ajoutés en base")
        else:
            print("❌ Aucun jeu récupéré")
            return
        
        # 4. État final
        print("\n" + "="*60)
        show_extraction_status()
        get_database_stats()
        
        print(f"\n🎉 Extraction séquentielle terminée!")
        print(f"📊 Résumé: {len(new_games)} nouveaux jeux ajoutés")
        
    except KeyboardInterrupt:
        print("\n⏹️ Arrêt demandé")
    except Exception as e:
        print(f"\n❌ Erreur: {e}")
        raise

if __name__ == "__main__":
    main()
