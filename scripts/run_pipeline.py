# scripts/run_pipeline.py - VERSION AVEC GESTION DES DOUBLONS
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
#   GESTION DES DOUBLONS
# =========================
def setup_unique_constraints():
    """Configure les contraintes uniques pour éviter les doublons"""
    conn = get_mysql_connection()
    if not conn:
        return False
    
    try:
        cursor = conn.cursor()
        
        # Vérifier si la contrainte unique existe déjà sur games
        cursor.execute("""
            SELECT COUNT(*) FROM information_schema.table_constraints 
            WHERE constraint_schema = %s 
            AND table_name = 'games' 
            AND constraint_name = 'unique_game_id'
        """, (DB_NAME,))
        
        constraint_exists = cursor.fetchone()[0] > 0
        
        if not constraint_exists:
            print("🔧 Ajout de la contrainte unique sur games.game_id_rawg...")
            try:
                cursor.execute("ALTER TABLE games ADD CONSTRAINT unique_game_id UNIQUE (game_id_rawg)")
                print("✅ Contrainte unique ajoutée sur games")
            except Error as e:
                if "Duplicate entry" in str(e):
                    print("⚠️ Doublons détectés, nettoyage nécessaire avant contrainte")
                    return False
                else:
                    print(f"❌ Erreur ajout contrainte: {e}")
        else:
            print("✅ Contrainte unique déjà présente sur games")
        
        # Même chose pour best_price_pc
        cursor.execute("""
            SELECT COUNT(*) FROM information_schema.table_constraints 
            WHERE constraint_schema = %s 
            AND table_name = 'best_price_pc' 
            AND constraint_name = 'unique_price_game'
        """, (DB_NAME,))
        
        price_constraint_exists = cursor.fetchone()[0] > 0
        
        if not price_constraint_exists:
            print("🔧 Ajout de la contrainte unique sur best_price_pc.game_id_rawg...")
            try:
                cursor.execute("ALTER TABLE best_price_pc ADD CONSTRAINT unique_price_game UNIQUE (game_id_rawg)")
                print("✅ Contrainte unique ajoutée sur best_price_pc")
            except Error as e:
                if "Duplicate entry" in str(e):
                    print("⚠️ Doublons détectés dans best_price_pc")
        else:
            print("✅ Contrainte unique déjà présente sur best_price_pc")
        
        cursor.close()
        return True
        
    except Error as e:
        print(f"❌ Erreur setup contraintes: {e}")
        return False
    finally:
        conn.close()

def check_duplicates():
    """Vérifie et affiche les doublons existants"""
    conn = get_mysql_connection()
    if not conn:
        return
    
    try:
        cursor = conn.cursor()
        
        print("\n🔍 Vérification des doublons...")
        
        # Doublons dans games
        cursor.execute("""
            SELECT game_id_rawg, COUNT(*) as count 
            FROM games 
            GROUP BY game_id_rawg 
            HAVING count > 1
            ORDER BY count DESC
            LIMIT 10
        """)
        
        game_duplicates = cursor.fetchall()
        if game_duplicates:
            print(f"⚠️ {len(game_duplicates)} jeux en doublon détectés:")
            for game_id, count in game_duplicates[:5]:
                cursor.execute("SELECT title FROM games WHERE game_id_rawg = %s LIMIT 1", (game_id,))
                title = cursor.fetchone()[0]
                print(f"  - {title} (ID: {game_id}) : {count} exemplaires")
        else:
            print("✅ Aucun doublon dans games")
        
        # Doublons dans best_price_pc
        cursor.execute("""
            SELECT game_id_rawg, COUNT(*) as count 
            FROM best_price_pc 
            GROUP BY game_id_rawg 
            HAVING count > 1
            ORDER BY count DESC
            LIMIT 10
        """)
        
        price_duplicates = cursor.fetchall()
        if price_duplicates:
            print(f"⚠️ {len(price_duplicates)} prix en doublon détectés:")
            for game_id, count in price_duplicates[:5]:
                cursor.execute("SELECT title FROM best_price_pc WHERE game_id_rawg = %s LIMIT 1", (game_id,))
                title = cursor.fetchone()[0]
                print(f"  - {title} (ID: {game_id}) : {count} exemplaires")
        else:
            print("✅ Aucun doublon dans best_price_pc")
        
        cursor.close()
        return len(game_duplicates), len(price_duplicates)
        
    except Error as e:
        print(f"❌ Erreur vérification doublons: {e}")
        return 0, 0
    finally:
        conn.close()

def remove_duplicates():
    """Supprime les doublons en gardant la version la plus récente"""
    conn = get_mysql_connection()
    if not conn:
        return False
    
    try:
        cursor = conn.cursor()
        
        print("\n🧹 Nettoyage des doublons...")
        
        # Supprimer doublons games (garder le plus récent)
        cursor.execute("""
            DELETE g1 FROM games g1 
            INNER JOIN games g2 
            WHERE g1.game_id_rawg = g2.game_id_rawg 
            AND g1.last_update < g2.last_update
        """)
        
        games_cleaned = cursor.rowcount
        print(f"🗑️ {games_cleaned} doublons supprimés dans games")
        
        # Supprimer doublons best_price_pc (garder le plus récent)
        cursor.execute("""
            DELETE p1 FROM best_price_pc p1 
            INNER JOIN best_price_pc p2 
            WHERE p1.game_id_rawg = p2.game_id_rawg 
            AND p1.last_update < p2.last_update
        """)
        
        prices_cleaned = cursor.rowcount
        print(f"🗑️ {prices_cleaned} doublons supprimés dans best_price_pc")
        
        cursor.close()
        
        print(f"✅ Nettoyage terminé: {games_cleaned + prices_cleaned} doublons supprimés")
        return True
        
    except Error as e:
        print(f"❌ Erreur suppression doublons: {e}")
        return False
    finally:
        conn.close()

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
        
        cursor.execute("SELECT COUNT(DISTINCT game_id_rawg) FROM best_price_pc")
        unique_prices = cursor.fetchone()[0]
        
        # Dernière mise à jour
        cursor.execute("SELECT MAX(last_update) FROM games")
        last_update = cursor.fetchone()[0]
        
        print(f"\n📊 Statistiques base de données:")
        print(f"  🎮 Jeux: {total_games} total, {unique_games} uniques")
        print(f"  💰 Prix: {total_prices} total, {unique_prices} uniques")
        print(f"  📅 Dernière MAJ: {last_update}")
        
        if total_games != unique_games:
            print(f"  ⚠️ {total_games - unique_games} doublons détectés dans games")
        if total_prices != unique_prices:
            print(f"  ⚠️ {total_prices - unique_prices} doublons détectés dans best_price_pc")
        
        cursor.close()
        
    except Error as e:
        print(f"❌ Erreur stats: {e}")
    finally:
        conn.close()

# =========================
#   FONCTIONS PRINCIPALES (avec gestion doublons améliorée)
# =========================
def fetch_new_games_from_rawg(page_size=40, pages=2):
    """Récupère des jeux depuis RAWG"""
    base = "https://api.rawg.io/api/games"
    headers = {"Accept": "application/json"}
    params_base = {"key": RAWG_API_KEY, "ordering": "-added", "page_size": page_size}

    rows = []
    for page in range(1, pages + 1):
        params = dict(params_base, page=page)
        r = requests.get(base, params=params, headers=headers, timeout=20)
        r.raise_for_status()
        data = r.json()
        for g in data.get("results", []):
            rows.append({
                "game_id_rawg": g.get("id"),
                "title": g.get("name"),
                "release_date": g.get("released"),
                "genres": ", ".join([x["name"] for x in g.get("genres", [])]) if g.get("genres") else None,
                "platforms": ", ".join([p["platform"]["name"] for p in g.get("platforms", [])]) if g.get("platforms") else None,
                "rating": g.get("rating"),
                "metacritic": g.get("metacritic"),
                "background_image": g.get("background_image"),
                "last_update": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            })
        time.sleep(0.8)
    return pd.DataFrame(rows)

def upsert_games(df: pd.DataFrame) -> int:
    """Insert/Update games avec gestion avancée des doublons"""
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
            cursor.execute("SELECT last_update FROM games WHERE game_id_rawg = %s", (r["game_id_rawg"],))
            existing = cursor.fetchone()
            
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
            
            if existing:
                updated += 1
            else:
                inserted += 1
        
        cursor.close()
        print(f"✅ Jeux traités: {inserted} nouveaux, {updated} mis à jour")
        return inserted + updated
        
    except Error as e:
        print(f"❌ Erreur upsert: {e}")
        return 0
    finally:
        conn.close()

def fetch_games_to_price(limit: int = None) -> pd.DataFrame:
    """Récupère les jeux à pricer (sans doublons)"""
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
#   SCRAPING ET SAUVEGARDE (inchangé)
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
    """Sauvegarde les prix avec mysql-connector direct"""
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
#   MAIN AVEC GESTION DOUBLONS
# =========================
def main():
    print("🚀 Démarrage du pipeline d'extraction avec gestion des doublons")
    
    try:
        # 1. Statistiques initiales
        get_database_stats()
        
        # 2. Vérification des doublons
        game_dups, price_dups = check_duplicates()
        
        # 3. Nettoyage si nécessaire
        if game_dups > 0 or price_dups > 0:
            user_input = input("\n❓ Nettoyer les doublons maintenant ? (o/N): ")
            if user_input.lower() in ['o', 'oui', 'y', 'yes']:
                remove_duplicates()
        
        # 4. Configuration des contraintes uniques
        setup_unique_constraints()
        
        # 5. Extraction normale
        print("\n🧲 Récupération de nouveaux jeux via RAWG…")
        new_games = fetch_new_games_from_rawg(page_size=40, pages=2)
        print(f"→ récupérés: {len(new_games)}")
        
        if not new_games.empty:
            n = upsert_games(new_games)
            print(f"→ traités: {n}")
        else:
            print("⚠️ Aucun jeu récupéré")
            n = 0

        print("\n📋 Sélection des jeux à scrapper (prix)…")
        to_price = fetch_games_to_price(limit=SCRAPE_LIMIT)
        print(f"→ à traiter: {len(to_price)}")

        if not to_price.empty:
            print("\n🔍 Scraping DLCompare (PC)…")
            df_prices = scrape_best_prices(to_price)
            print(f"→ scrapés: {len(df_prices)}")

            print("\n💾 Enregistrement des prix…")
            saved = save_prices_to_mysql(df_prices)
            print(f"→ lignes enregistrées/MAJ: {saved}")
        else:
            print("✅ Aucun jeu nécessite une mise à jour des prix")
            saved = 0

        # 6. Statistiques finales
        print("\n" + "="*50)
        get_database_stats()
        
        print(f"\n🎉 Pipeline terminé avec succès!")
        print(f"📊 Résumé: {n} jeux traités, {saved} prix mis à jour")
        
    except KeyboardInterrupt:
        print("\n⏹️ Arrêt demandé par l'utilisateur")
    except Exception as e:
        print(f"\n❌ Erreur dans le pipeline: {e}")
        raise

if __name__ == "__main__":
    main()
