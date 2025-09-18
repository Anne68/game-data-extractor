"""
💰 Module de scraping des prix avec Selenium - Version corrigée basée sur le notebook fonctionnel
"""

import pandas as pd
import time
import logging
import os
import sys
from datetime import datetime
from typing import Dict, Any, List, Optional
from pathlib import Path

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException

sys.path.insert(0, str(Path(__file__).parent.parent))
logger = logging.getLogger(__name__)

class PriceScraper:
    """Scraper de prix avec Selenium pour DLCompare - Version notebook corrigée"""
    
    def __init__(self):
        try:
            from utils.config import ConfigManager
            config = ConfigManager()
            scraping_config = config.get_scraping_config()
        except ImportError:
            scraping_config = self._get_config_from_env()
        
        self.enabled = scraping_config.get('enabled', True)
        self.max_games = scraping_config.get('max_games_per_session', 10)  # Réduit pour la stabilité
        self.delay = scraping_config.get('delay_between_requests', 3)
        self.headless = scraping_config.get('headless', True)
        
        logger.info(f"PriceScraper Selenium initialisé - Enabled: {self.enabled}, Headless: {self.headless}")
    
    def _get_config_from_env(self) -> Dict[str, Any]:
        return {
            'enabled': os.getenv('SCRAPING_ENABLED', 'true').lower() == 'true',
            'max_games_per_session': int(os.getenv('MAX_GAMES_SCRAPING', '10')),
            'delay_between_requests': float(os.getenv('SCRAPING_DELAY', '3.0')),
            'headless': os.getenv('HEADLESS_MODE', 'true').lower() == 'true'
        }
    
    def _setup_selenium_driver(self):
        """Configure le driver Selenium Chrome - Exactement comme dans le notebook"""
        options = Options()
        
        if self.headless:
            options.add_argument('--headless')
        
        # Configuration exacte du notebook fonctionnel
        options.add_argument('--disable-blink-features=AutomationControlled')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-gpu')
        options.add_argument('--disable-extensions')
        options.add_argument('--disable-images')  # Accélérer le chargement
        options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
        
        # Désactiver les logs pour éviter le spam
        options.add_experimental_option('excludeSwitches', ['enable-logging'])
        options.add_experimental_option('useAutomationExtension', False)
        options.add_argument('--log-level=3')  # Supprimer les warnings
        
        try:
            driver = webdriver.Chrome(options=options)
            driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            driver.set_page_load_timeout(30)
            return driver
        except Exception as e:
            logger.error(f"Erreur création driver Selenium: {e}")
            return None
    
    def scrape_prices(self, games_df: pd.DataFrame) -> pd.DataFrame:
        """Scrape les prix depuis DLCompare - Logic exacte du notebook"""
        if not self.enabled:
            logger.info("Scraping désactivé")
            return pd.DataFrame(columns=['game_id_rawg', 'title', 'platform', 'price', 'shop', 'url', 'last_update'])
        
        if games_df.empty:
            logger.info("Aucun jeu à scraper")
            return pd.DataFrame(columns=['game_id_rawg', 'title', 'platform', 'price', 'shop', 'url', 'last_update'])
        
        logger.info(f"🔍 Scraping des prix pour {len(games_df)} jeux depuis DLCompare")
        
        driver = self._setup_selenium_driver()
        if not driver:
            logger.error("Impossible d'initialiser Selenium")
            return pd.DataFrame()
        
        updated_prices = []
        
        try:
            # Boucle principale - structure exacte du notebook
            for _, game_row in games_df.head(min(self.max_games, len(games_df))).iterrows():
                title = game_row.get('title', '').strip()
                game_id = game_row.get('game_id_rawg')
                
                if not title:
                    continue
                
                logger.info(f"🎮 Recherche du prix pour {title}...")
                
                try:
                    # Structure exacte du notebook fonctionnel
                    search_url = f"https://www.dlcompare.fr/search?q={title.replace(' ', '+')}#all"
                    driver.get(search_url)
                    time.sleep(2)  # Attendre le chargement
                    
                    try:
                        # SÉLECTEUR EXACT DU NOTEBOOK - c'est crucial !
                        game_element = WebDriverWait(driver, 10).until(
                            EC.element_to_be_clickable((By.CLASS_NAME, "name.clickable"))
                        )
                        game_element.click()
                        
                        # Navigation vers la section PC - comme dans le notebook
                        url_pc = driver.current_url + "#pc"
                        driver.get(url_pc)
                        time.sleep(2)
                        
                        best_price_pc = None
                        best_shop_pc = None
                        
                        try:
                            # SÉLECTEURS EXACTS DU NOTEBOOK - ne pas changer !
                            price_element = WebDriverWait(driver, 5).until(
                                EC.presence_of_element_located((By.CLASS_NAME, "lowPrice"))
                            )
                            best_price_pc = price_element.text.strip()
                            
                            shop_element = WebDriverWait(driver, 5).until(
                                EC.presence_of_element_located((By.CSS_SELECTOR, "a.shop > span"))
                            )
                            best_shop_pc = shop_element.text.strip()
                            
                        except TimeoutException:
                            logger.warning(f"Aucun prix trouvé pour {title} sur PC")
                        
                        # Ajouter les résultats - structure exacte du notebook
                        last_update = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        updated_prices.append({
                            'game_id_rawg': game_id,
                            'title': title,
                            'platform': 'PC',
                            'price': best_price_pc,
                            'shop': best_shop_pc,
                            'url': url_pc,
                            'last_update': last_update
                        })
                        
                        if best_price_pc:
                            logger.info(f"✅ Prix trouvé: {best_price_pc} chez {best_shop_pc}")
                        
                    except TimeoutException:
                        logger.warning(f"Aucun résultat fiable pour {title}")
                        updated_prices.append({
                            'game_id_rawg': game_id,
                            'title': title,
                            'platform': 'PC',
                            'price': None,
                            'shop': None,
                            'url': None,
                            'last_update': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        })
                        
                except Exception as e:
                    logger.error(f"❌ Erreur pour {title}: {e}")
                    updated_prices.append({
                        'game_id_rawg': game_id,
                        'title': title,
                        'platform': 'PC',
                        'price': None,
                        'shop': None,
                        'url': None,
                        'last_update': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    })
                
                # Délai entre les jeux pour éviter la détection
                time.sleep(self.delay)
                
        finally:
            driver.quit()
            logger.info("🔒 Driver Selenium fermé")
        
        logger.info(f"🎯 Scraping terminé: {len([p for p in updated_prices if p.get('price')])} prix récupérés sur {len(updated_prices)} jeux")
        return pd.DataFrame(updated_prices)
    
    def fetch_games_from_db(self):
        """Récupérer les jeux depuis la base - fonction du notebook"""
        try:
            from extractor.database import DatabaseManager
            db = DatabaseManager()
            
            conn = db.get_connection()
            if not conn:
                return pd.DataFrame()
            
            cursor = conn.cursor()
            
            # Récupérer les jeux sans prix ou avec prix ancien
            query = """
                SELECT DISTINCT g.game_id_rawg, g.title 
                FROM games g
                LEFT JOIN best_price_pc p ON g.game_id_rawg = p.game_id_rawg
                WHERE p.game_id_rawg IS NULL 
                   OR p.last_update < DATE_SUB(NOW(), INTERVAL 7 DAY)
                ORDER BY g.rating DESC
                LIMIT %s
            """
            
            cursor.execute(query, (self.max_games,))
            games = cursor.fetchall()
            conn.close()
            
            return pd.DataFrame(games, columns=["game_id_rawg", "title"])
            
        except Exception as e:
            logger.error(f"Erreur récupération jeux: {e}")
            return pd.DataFrame()
    
    def save_prices_to_mysql(self, df_prices):
        """Sauvegarder les prix en MySQL - fonction du notebook"""
        if df_prices.empty:
            return
        
        try:
            from extractor.database import DatabaseManager
            db = DatabaseManager()
            return db.save_prices(df_prices)
            
        except Exception as e:
            logger.error(f"Erreur sauvegarde prix: {e}")
            return False
    
    def test_scraping(self, test_games: List[Dict[str, str]] = None) -> bool:
        """Test du scraping - version notebook"""
        logger.info("🧪 Test de scraping Selenium")
        
        if not test_games:
            test_games = [{'game_id_rawg': 12345, 'title': 'Cyberpunk 2077'}]
        
        test_df = pd.DataFrame(test_games)
        results = self.scrape_prices(test_df)
        
        success = not results.empty and len([r for r in results.to_dict('records') if r.get('price')]) > 0
        
        if success:
            logger.info("✅ Test Selenium réussi")
            for _, row in results.iterrows():
                if row['price']:
                    logger.info(f"   Prix test: {row['title']} = {row['price']} chez {row['shop']}")
        else:
            logger.warning("❌ Test Selenium échoué")
            
        return success

# ============================ #
# 🚀  Fonction principale      #
# ============================ #
def main():
    """Fonction principale pour tester le scraper"""
    scraper = PriceScraper()
    
    # Test avec un jeu connu
    if scraper.test_scraping():
        print("✅ Scraper fonctionnel")
        
        # Scraper quelques jeux réels
        games_df = scraper.fetch_games_from_db()
        if not games_df.empty:
            print(f"📋 {len(games_df)} jeux trouvés pour scraping")
            prices_df = scraper.scrape_prices(games_df)
            
            if not prices_df.empty:
                success = scraper.save_prices_to_mysql(prices_df)
                if success:
                    print(f"✅ {len(prices_df)} prix sauvegardés en base")
                else:
                    print("❌ Erreur sauvegarde")
        else:
            print("❌ Aucun jeu trouvé en base")
    else:
        print("❌ Test scraper échoué")

if __name__ == "__main__":
    main()
