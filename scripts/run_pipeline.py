# scripts/run_pipeline.py - VERSION AVEC EXTRACTION SÉQUENTIELLE DE 50 JEUX
import os
import time
from datetime import datetime
import pandas as pd
import requests
import mysql.connector
from mysql.connector import Error

# Selenium pour DLCompare
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# =========================
#   CONFIG / DB CONNECTION
# =========================
# Variables d'environnement
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

# Test de connexion au démarrage
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
#   GESTION SÉQUENTIELLE DES EXTRACTIONS
# =========================

def get_next_page_to_extract():
    """Détermine la prochaine page à extraire depuis l'API RAWG"""
    conn = get_mysql_connection()
    if not conn:
        return 65  # Page de départ par défaut (ID ~2561)
    
    try:
        cursor = conn.cursor()
        
        # Vérifier si la table api_state existe, sinon la créer
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS api_state (
                id INT PRIMARY KEY,
                last_page INT NOT NULL DEFAULT 65,
                last_extraction DATETIME,
                total_games_extracted INT DEFAULT 0
            )
        """)
        
        # Récupérer l'état actuel
        cursor.execute("SELECT last_page, total_games_extracted FROM api_state WHERE id = 1")
        result = cursor.fetchone()
        
        if result:
            last_page, total_extracted = result
            next_page = last_page + 1
            print(f"📋 Reprise depuis la page {next_page} ({total_extracted} jeux extraits au total)")
        else:
            # Première exécution, commencer à la page 65 (environ ID 2561)
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
    """Met à jour l'état d'extraction après traitement"""
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
    """Récupère exactement 50 nouveaux jeux depuis la dernière position"""
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
                "ordering": "id",  # Tri par ID pour ordre séquentiel
                "page_size": 40,   # Maximum par page
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
            
            # Ajouter les jeux de cette page
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
                    #"background_image": g.get("background_image"),
                    "last_update": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                }
                
                all_games.append(game_data)
            
            print(f"  → {len(page_results)} jeux sur la page, total: {len(all_games)}")
            
            # Passer à la page suivante
            current_page += 1
            time.sleep(0.8)  # Respect du rate limiting
            
            # Sécurité: éviter les boucles infinies
            if current_page - start_page > 10:
                print("⚠️ Limite de pages atteinte")
                break
                
        except Exception as e:
            print(f"❌ Erreur page {current_page}: {e}")
            break
    
    # Mettre à jour l'état
    if all_games:
        games_added = len(all_games)
        last_processed_page = current_page - 1
        update_extraction_state(last_processed_page, games_added)
        
        print(f"✅ Extraction terminée: {games_added} jeux récupérés")
        
        # Afficher quelques exemples
        if len(all_games) >= 3:
            print("🎮 Premiers jeux extraits:")
            for i in range(min(3, len(all_games))):
                game = all_games[i]
                print(f"  - {game['title']} (ID: {game['game_id_rawg']})")
    
    return pd.DataFrame(all_games)

def show_extraction_status():
    """Affiche l'état actuel de l'extraction séquentielle"""
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
            
            # Estimer l'ID approximatif
            estimated_min_id = (last_page - 1) * 40
            estimated_max_id = last_page * 40
            print(f"  • IDs approximatifs traités: {estimated_min_id}-{estimated_max_id}")
        else:
            print("📊 Aucun état d'extraction trouvé (première fois)")
            
        cursor.close()
        
    except Error as e:
        print(f"❌ Erreur affichage état: {e}")
    finally:
        conn.close()

# =========================
#   GESTION DES DOUBLONS (simplifiée)
# =========================
def get_database_stats():
    """Affiche les statistiques de la base de données"""
    conn = get_mysql_connection()
    if not conn:
        return
    
    try:
        cursor = conn.cursor()
        
        # Stats games
        cursor.execute("SELECT COUNT(*) FROM games")
        total_games = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(DISTINCT game_id_rawg) FROM games")
        unique_games = cursor.fetchone()[0]
        
        # Stats prices
        cursor.execute("SELECT COUNT(*) FROM best_price_pc")
        total_prices = cursor.fetchone()[0]
        
        # Dernière mise à jour
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

# =========================
#   INSERTION EN BASE
# =========================
def upsert_games(df: pd.DataFrame) -> int:
    """Insert/Update games avec comptage précis"""
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
            # Vérifier si le jeu existe déjà
            cursor.execute("SELECT COUNT(*) FROM games WHERE game_id_rawg = %s", (r["game_id_rawg"],))
            exists = cursor.fetchone()[0] > 0
            
            query = """
                INSERT INTO games (game_id_rawg, title, release_date, genres, platforms, rating, metacritic, background_image, last_update)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                  title=VALUES(title),
                  release_date=VALUES(release_date),
                  genres=VALUES(genres),
                  platforms=VALUES(platforms),
                  rating=VALUES(rating),
                  metacritic=VALUES(metacritic),
                  background_image=VALUES(background_image),
                  last_update=VALUES(last_update)
            """
            
            values = (
                r["game_id_rawg"], r["title"], r["release_date"],
                r["genres"], r["platforms"], r["rating"],
                r["metacritic"], r["background_image"], r["last_update"]
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

def fetch_games_to_price(limit: int = None) -> pd.DataFrame:
    """Récupère les jeux à pricer"""
    if limit is None:
        limit = SCRAPE_LIMIT
        
    conn = get_mysql_connection()
    if not conn:
        return pd.DataFrame(columns=['game_id_rawg', 'title'])
    
    try:
        query = """
            SELECT DISTINCT g.game_id_rawg, g.title
            FROM games g
            LEFT JOIN best_price_pc b ON b.game_id_rawg = g.game_id_rawg
            WHERE b.last_update IS NULL OR b.last_update < (NOW() - INTERVAL 7 DAY)
            ORDER BY g.last_update DESC
            LIMIT %s
        """
        
        cursor = conn.cursor()
        cursor.execute(query, (limit,))
        
        results = cursor.fetchall()
        df = pd.DataFrame(results, columns=['game_id_rawg', 'title'])
        
        cursor.close()
        return df
        
    except Error as e:
        print(f"❌ Erreur récupération jeux: {e}")
        return pd.DataFrame(columns=['game_id_rawg', 'title'])
    finally:
        conn.close()

# =========================
#   SCRAPING (inchangé mais optionnel)
# =========================
def scrape_best_prices(games_df: pd.DataFrame) -> pd.DataFrame:
    """Scrape les prix avec Selenium"""
    if games_df.empty:
        print("✅ Aucun jeu à mettre à jour (prix).")
        return pd.DataFrame()

    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    prefs = {"profile.managed_default_content_settings.images": 2}
    opts.add_experimental_option("prefs", prefs)

    driver = webdriver.Chrome(options=opts)
    driver.set_page_load_timeout(12)

    rows = []
    for _, row in games_df.iterrows():
        title = row["title"]
        gid = int(row["game_id_rawg"])
        print(f"🔎 {title}")

        search_url = f"https://www.dlcompare.fr/search?q={title.replace(' ', '+')}#all"
        best_price_pc = None
        best_shop_pc = None
        url_pc = None

        try:
            driver.get(search_url)
            link = WebDriverWait(driver, 4).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, ".name.clickable"))
            )
            link.click()

            url_pc = driver.current_url.split("#")[0] + "#pc"
            driver.get(url_pc)

            try:
                price_el = WebDriverWait(driver, 4).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, ".lowPrice"))
                )
                best_price_pc = price_el.text.strip()
            except Exception:
                pass

            try:
                shop_el = WebDriverWait(driver, 3).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "a.shop > span"))
                )
                best_shop_pc = shop_el.text.strip()
            except Exception:
                pass

        except Exception as e:
            print(f"⚠️ Pas de résultat pour {title}: {e}")

        rows.append({
            "game_id_rawg": gid,
            "title": title,
            "best_price_PC": best_price_pc,
            "best_shop_PC": best_shop_pc,
            "site_url_PC": url_pc,
            "last_update": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        })

    driver.quit()
    return pd.DataFrame(rows)

def save_prices_to_mysql(df_prices: pd.DataFrame) -> int:
    """Sauvegarde les prix"""
    if df_prices.empty:
        return 0
        
    conn = get_mysql_connection()
    if not conn:
        return 0
    
    saved = 0
    try:
        cursor = conn.cursor()
        
        for _, r in df_prices.iterrows():
            query = """
                INSERT INTO best_price_pc (game_id_rawg, title, best_price_PC, best_shop_PC, site_url_PC, last_update)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                  title=VALUES(title),
                  best_price_PC=VALUES(best_price_PC),
                  best_shop_PC=VALUES(best_shop_PC),
                  site_url_PC=VALUES(site_url_PC),
                  last_update=VALUES(last_update)
            """
            
            values = (
                int(r["game_id_rawg"]), r["title"], r["best_price_PC"],
                r["best_shop_PC"], r["site_url_PC"], r["last_update"]
            )
            
            cursor.execute(query, values)
            saved += 1
        
        cursor.close()
        return saved
        
    except Error as e:
        print(f"❌ Erreur sauvegarde prix: {e}")
        return 0
    finally:
        conn.close()

# =========================
#   MAIN AVEC EXTRACTION SÉQUENTIELLE
# =========================
def main():
    print("🚀 Pipeline d'extraction séquentielle - 50 jeux par run")
    
    try:
        # 1. Afficher l'état actuel
        show_extraction_status()
        
        # 2. Statistiques base
        get_database_stats()
        
        # 3. Extraction de exactement 50 nouveaux jeux
        print("\n🎯 Extraction séquentielle de 50 nouveaux jeux...")
        new_games = fetch_exactly_50_games()
        
        if not new_games.empty:
            print(f"✅ {len(new_games)} jeux récupérés pour insertion")
            
            # 4. Insertion en base
            inserted = upsert_games(new_games)
            print(f"✅ {inserted} NOUVEAUX jeux ajoutés en base")
        else:
            print("❌ Aucun jeu récupéré")
            return
        
        # 5. Scraping prix (optionnel et limité)
        user_input = input("\n❓ Scraper quelques prix ? (o/N): ")
        if user_input.lower() in ['o', 'oui', 'y', 'yes']:
            print("\n📋 Sélection de 5 jeux pour scraping prix...")
            to_price = fetch_games_to_price(limit=5)
            
            if not to_price.empty:
                print(f"🔍 Scraping {len(to_price)} jeux...")
                df_prices = scrape_best_prices(to_price)
                saved = save_prices_to_mysql(df_prices)
                print(f"💾 {saved} prix sauvegardés")
        else:
            print("⏩ Scraping prix ignoré")
        
        # 6. État final
        print("\n" + "="*60)
        show_extraction_status()
        get_database_stats()
        
        print(f"\n🎉 Extraction séquentielle terminée!")
        print(f"📊 Résumé: {len(new_games)} nouveaux jeux ajoutés (ID séquentiel)")
        
    except KeyboardInterrupt:
        print("\n⏹️ Arrêt demandé")
    except Exception as e:
        print(f"\n❌ Erreur: {e}")
        raise

if __name__ == "__main__":
    main()
