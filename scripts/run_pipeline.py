# scripts/run_pipeline.py
import os
import time
import json
import math
from datetime import datetime, timedelta

import pandas as pd
import requests
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

# --- Selenium
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# =========================
#   CONFIG / DB ENGINE
# =========================
RAWG_API_KEY = os.getenv("RAWG_API_KEY")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = int(os.getenv("DB_PORT", "3306"))
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME", "anne_games_db")
SCRAPE_LIMIT = int(os.getenv("SCRAPE_LIMIT", "20"))  # limite « sécurité »

if not RAWG_API_KEY:
    raise RuntimeError("RAWG_API_KEY manquant.")

if not all([DB_HOST, DB_USER, DB_PASSWORD, DB_NAME]):
    raise RuntimeError("Config DB incomplète (DB_HOST/DB_USER/DB_PASSWORD/DB_NAME).")

engine = create_engine(
    f"mysql+mysqlconnector://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}?charset=utf8mb4",
    pool_pre_ping=True,
    pool_recycle=3600,
)

# =========================
#   RAWG: fetch new games
# =========================
def fetch_new_games_from_rawg(page_size=40, pages=2):
    """
    Récupère des jeux récents/populaires.
    Heuristique: on prend la sortie triée par -added (les plus ajoutés récemment).
    Ajuste si tu veux -released ou un filtre de dates.
    """
    base = "https://api.rawg.io/api/games"
    headers = {"Accept": "application/json"}
    params_base = {
        "key": RAWG_API_KEY,
        "ordering": "-added",
        "page_size": page_size,
    }

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
        # pause légère pour éviter rate-limit
        time.sleep(0.8)
    return pd.DataFrame(rows)

def upsert_games(df: pd.DataFrame):
    if df.empty:
        return 0
    inserted = 0
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
    return inserted

# =======================================
#   DB -> liste des jeux à pricifier
# =======================================
def fetch_games_to_price(limit: int = SCRAPE_LIMIT) -> pd.DataFrame:
    """
    Récupère les jeux à scrapper (ceux qui n’ont pas encore de best_price_pc,
    ou dont la MAJ prix date de > 7 jours). Ajuste la fenêtre selon ton besoin.
    """
    q = text("""
        SELECT g.game_id_rawg, g.title
        FROM games g
        LEFT JOIN best_price_pc b ON b.game_id_rawg = g.game_id_rawg
        WHERE b.last_update IS NULL OR b.last_update < (NOW() - INTERVAL 7 DAY)
        ORDER BY g.last_update DESC
        LIMIT :lim
    """)
    with engine.begin() as conn:
        df = pd.read_sql(q, conn, params={"lim": limit})
    return df

# =======================================
#   DLCompare: scrape best price (PC)
# =======================================
def scrape_best_prices(games_df: pd.DataFrame) -> pd.DataFrame:
    """
    Pour chaque titre, cherche sur DLCompare, clique le premier résultat,
    passe en onglet #pc, lit le meilleur prix & shop.
    """
    if games_df.empty:
        print("✅ Aucun jeu à mettre à jour (prix).")
        return pd.DataFrame()

    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    # (optionnel) réduire le coût images
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

            # ATTENTION: utiliser CSS selector pour sélection multi-classes
            # L’élément de résultat cliquable a souvent les classes: "name clickable"
            link = WebDriverWait(driver, 4).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, ".name.clickable"))
            )
            link.click()

            # forcer l’onglet PC
            url_pc = driver.current_url.split("#")[0] + "#pc"
            driver.get(url_pc)

            # lecture du meilleur prix – les pages DLCompare exposent souvent .lowPrice
            try:
                price_el = WebDriverWait(driver, 4).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, ".lowPrice"))
                )
                best_price_pc = price_el.text.strip()
            except Exception:
                pass

            # boutique
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
#   Save prices -> MySQL
# =======================================
def save_prices_to_mysql(df_prices: pd.DataFrame) -> int:
    if df_prices.empty:
        return 0
    saved = 0
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
    return saved

# =======================================
#   Main
# =======================================
def main():
    print("🧲 Récupération de nouveaux jeux via RAWG…")
    new_games = fetch_new_games_from_rawg(page_size=40, pages=2)  # ~80 jeux
    print(f"→ récupérés: {len(new_games)}")
    n = upsert_games(new_games)
    print(f"→ upsert en base: {n}")

    print("\n📋 Sélection des jeux à scrapper (prix)…")
    to_price = fetch_games_to_price(limit=SCRAPE_LIMIT)
    print(f"→ à traiter: {len(to_price)}")

    print("\n🔍 Scraping DLCompare (PC)…")
    df_prices = scrape_best_prices(to_price)
    print(f"→ scrapés: {len(df_prices)}")

    print("\n💾 Enregistrement des prix…")
    saved = save_prices_to_mysql(df_prices)
    print(f"→ lignes enregistrées/MAJ: {saved}")

    print("\n🎉 Terminé.")

if __name__ == "__main__":
    main()
