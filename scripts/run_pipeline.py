# scripts/run_pipeline.py - VERSION CORRIGÉE
import os
import time
import urllib.parse
from datetime import datetime

import pandas as pd
import requests

from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from sqlalchemy.exc import SQLAlchemyError

# Selenium (pour la partie DLCompare)
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


# =========================
#   CONFIG / DB ENGINE - VERSION CORRIGÉE
# =========================
# Variables d'environnement requises
RAWG_API_KEY = os.getenv("RAWG_API_KEY")

DB_HOST = os.getenv("DB_HOST")
DB_PORT = int(os.getenv("DB_PORT", "3306"))
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")

# Limite de scraping par run (peut être ajustée via ENV)
SCRAPE_LIMIT = int(os.getenv("SCRAPE_LIMIT", "20"))

# Garde-fous
if not RAWG_API_KEY:
    raise RuntimeError("RAWG_API_KEY manquant.")

for k, v in [("DB_HOST", DB_HOST), ("DB_USER", DB_USER), ("DB_PASSWORD", DB_PASSWORD), ("DB_NAME", DB_NAME)]:
    if not v:
        raise RuntimeError(f"Config DB incomplète: variable {k} manquante.")

# MÉTHODE CORRIGÉE : Gérer les mots de passe avec caractères spéciaux
print(f"🔧 Connexion à {DB_HOST} en tant que {DB_USER}")

def create_db_engine():
    """Crée l'engine SQLAlchemy avec gestion des caractères spéciaux"""
    try:
        # Méthode 1: URL.create() (recommandée pour les mots de passe avec @)
        print("🧪 Tentative connexion méthode URL.create()...")
        db_url = URL.create(
            drivername="mysql+mysqlconnector",
            username=DB_USER,
            password=DB_PASSWORD,  # SQLAlchemy encode automatiquement
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            query={"charset": "utf8mb4"},
        )
        
        engine = create_engine(db_url, pool_pre_ping=True, pool_recycle=3600)
        
        # Test de connexion
        with engine.connect() as test_conn:
            test_conn.execute(text("SELECT 1"))
        
        print("✅ Connexion MySQL établie (méthode URL.create)")
        return engine
        
    except Exception as e:
        print(f"❌ Méthode URL.create échouée: {e}")
        print("🔧 Tentative avec encodage manuel...")
        
        try:
            # Méthode 2: Encodage manuel du mot de passe
            encoded_password = urllib.parse.quote_plus(DB_PASSWORD)
            connection_string = f"mysql+mysqlconnector://{DB_USER}:{encoded_password}@{DB_HOST}:{DB_PORT}/{DB_NAME}?charset=utf8mb4"
            
            engine = create_engine(connection_string, pool_pre_ping=True, pool_recycle=3600)
            
            # Test de connexion
            with engine.connect() as test_conn:
                test_conn.execute(text("SELECT 1"))
            
            print("✅ Connexion MySQL établie (encodage manuel)")
            return engine
            
        except Exception as e2:
            print(f"❌ Méthode encodage manuel échouée: {e2}")
            
            # Méthode 3: Afficher des infos de debug
            print("\n🔍 Informations de debug:")
            print(f"  - Host: {DB_HOST}")
            print(f"  - Port: {DB_PORT}")
            print(f"  - User: {DB_USER}")
            print(f"  - Database: {DB_NAME}")
            print(f"  - Password length: {len(DB_PASSWORD) if DB_PASSWORD else 0}")
            print(f"  - Password contains @: {'@' in DB_PASSWORD if DB_PASSWORD else False}")
            
            # Suggestion de test direct
            print("\n💡 Testez la connexion directe avec:")
            print("python3 -c \"import mysql.connector; conn = mysql.connector.connect(")
            print(f"    host='{DB_HOST}', port={DB_PORT}, user='{DB_USER}',")
            print("    password='[VOTRE_MOT_DE_PASSE]', database='{DB_NAME}'); print('OK')\"")
            
            raise RuntimeError(f"Impossible de se connecter à MySQL. Dernière erreur: {e2}")

# Créer l'engine
engine = create_db_engine()


# =========================
#   RAWG: fetch new games (INCHANGÉ)
# =========================
def fetch_new_games_from_rawg(page_size=40, pages=2):
    """
    Récupère des jeux récents/populaires.
    Heuristique: tri par -added (les plus ajoutés récemment).
    """
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
        time.sleep(0.8)  # petite pause pour éviter le rate-limit
    return pd.DataFrame(rows)


def upsert_games(df: pd.DataFrame) -> int:
    if df.empty:
        return 0
    inserted = 0
    
    try:
        with engine.begin() as conn:
            for _, r in df.iterrows():
                conn.execute(text("""
                    INSERT INTO games (game_id_rawg, title, release_date, genres, platforms, rating, metacritic, background_image, last_update)
                    VALUES (:id, :title, :rd, :genres, :plats, :rating, :mc, :bg, :lu)
                    ON DUPLICATE KEY UPDATE
                      title=VALUES(title),
                      release_date=VALUES(release_date),
                      genres=VALUES(genres),
                      platforms=VALUES(platforms),
                      rating=VALUES(rating),
                      metacritic=VALUES(metacritic),
                      background_image=VALUES(background_image),
                      last_update=VALUES(last_update)
                """), {
                    "id": r["game_id_rawg"],
                    "title": r["title"],
                    "rd": r["release_date"],
                    "genres": r["genres"],
                    "plats": r["platforms"],
                    "rating": r["rating"],
                    "mc": r["metacritic"],
                    "bg": r["background_image"],
                    "lu": r["last_update"],
                })
                inserted += 1
                
        print(f"✅ {inserted} jeux traités avec succès")
        return inserted
        
    except Exception as e:
        print(f"❌ Erreur lors de l'insertion des jeux: {e}")
        return 0


# =======================================
#   DB -> liste des jeux à pricifier (INCHANGÉ)
# =======================================
def fetch_games_to_price(limit: int = None) -> pd.DataFrame:
    """Jeux sans prix ou prix vieux de >7 jours."""
    if limit is None:
        limit = SCRAPE_LIMIT
    q = text("""
        SELECT g.game_id_rawg, g.title
        FROM games g
        LEFT JOIN best_price_pc b ON b.game_id_rawg = g.game_id_rawg
        WHERE b.last_update IS NULL OR b.last_update < (NOW() - INTERVAL 7 DAY)
        ORDER BY g.last_update DESC
        LIMIT :lim
    """)
    
    try:
        with engine.begin() as conn:
            df = pd.read_sql(q, conn, params={"lim": int(limit)})
        return df
    except Exception as e:
        print(f"❌ Erreur récupération jeux à pricer: {e}")
        return pd.DataFrame(columns=['game_id_rawg', 'title'])


# =======================================
#   DLCompare: scrape best price (PC) (INCHANGÉ)
# =======================================
def scrape_best_prices(games_df: pd.DataFrame) -> pd.DataFrame:
    if games_df.empty:
        print("✅ Aucun jeu à mettre à jour (prix).")
        return pd.DataFrame()

    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    # réduire légèrement le coût images
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

            # utiliser un sélecteur CSS (multi-classes)
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
            print(f"⚠️ Pas de résultat fiable pour {title}: {e}")

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


# =======================================
#   Save prices -> MySQL (CORRIGÉ)
# =======================================
def save_prices_to_mysql(df_prices: pd.DataFrame) -> int:
    if df_prices.empty:
        return 0
    saved = 0
    
    try:
        with engine.begin() as conn:
            for _, r in df_prices.iterrows():
                conn.execute(text("""
                    INSERT INTO best_price_pc (game_id_rawg, title, best_price_PC, best_shop_PC, site_url_PC, last_update)
                    VALUES (:id, :title, :bp, :bs, :url, :lu)
                    ON DUPLICATE KEY UPDATE
                      title=VALUES(title),
                      best_price_PC=VALUES(best_price_PC),
                      best_shop_PC=VALUES(best_shop_PC),
                      site_url_PC=VALUES(site_url_PC),
                      last_update=VALUES(last_update)
                """), {
                    "id": int(r["game_id_rawg"]),
                    "title": r["title"],
                    "bp": r["best_price_PC"],
                    "bs": r["best_shop_PC"],
                    "url": r["site_url_PC"],
                    "lu": r["last_update"],
                })
                saved += 1
        
        print(f"✅ {saved} prix traités avec succès")
        return saved
        
    except Exception as e:
        print(f"❌ Erreur lors de la sauvegarde des prix: {e}")
        return 0


# =======================================
#   Main (AVEC GESTION D'ERREUR AMÉLIORÉE)
# =======================================
def main():
    print("🚀 Démarrage du pipeline d'extraction")
    print(f"🔧 Base de données: {DB_USER}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
    print(f"🔑 API RAWG: {'✓' if RAWG_API_KEY else '✗'}")
    
    try:
        print("\n🧲 Récupération de nouveaux jeux via RAWG…")
        new_games = fetch_new_games_from_rawg(page_size=40, pages=2)  # ~80 jeux
        print(f"→ récupérés: {len(new_games)}")
        
        if not new_games.empty:
            n = upsert_games(new_games)
            print(f"→ upsert en base: {n}")
        else:
            print("⚠️ Aucun jeu récupéré de RAWG")
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

        print(f"\n🎉 Pipeline terminé avec succès!")
        print(f"📊 Résumé: {n} jeux ajoutés/mis à jour, {saved} prix traités")
        
    except KeyboardInterrupt:
        print("\n⏹️ Arrêt demandé par l'utilisateur")
    except Exception as e:
        print(f"\n❌ Erreur dans le pipeline: {e}")
        print("🔍 Vérifiez:")
        print("  - La connexion internet")
        print("  - Les variables d'environnement")
        print("  - La connexion à la base de données")
        print("  - La clé API RAWG")
        raise


if __name__ == "__main__":
    main()
