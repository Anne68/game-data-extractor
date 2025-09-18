"""
💰 Module de scraping des prix avec Selenium - Version anti-crash
"""

import pandas as pd
import time
import logging
import os
import sys
import re
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
    """Scraper de prix avec Selenium - Version robuste anti-crash"""
    
    def __init__(self):
        try:
            from utils.config import ConfigManager
            config = ConfigManager()
            scraping_config = config.get_scraping_config()
        except ImportError:
            scraping_config = self._get_config_from_env()
        
        self.enabled = scraping_config.get('enabled', True)
        self.max_games = scraping_config.get('max_games_per_session', 5)  # Réduit pour stabilité
        self.delay = scraping_config.get('delay_between_requests', 4)
        self.headless = scraping_config.get('headless', True)
        self.restart_interval = 3  # Redémarrer Chrome tous les 3 jeux
        
        # Jeux problématiques à éviter
        self.problematic_keywords = [
            'KINGDOM HEARTS', 'Final Fantasy XV', 'Call of Duty', 'Assassin\'s Creed',
            'HD 1.5', '+2.5', 'ReMIX', 'Collection', 'Anthology'
        ]
        
        logger.info(f"PriceScraper anti-crash initialisé - Enabled: {self.enabled}, Max: {self.max_games}")
    
    def _get_config_from_env(self) -> Dict[str, Any]:
        return {
            'enabled': os.getenv('SCRAPING_ENABLED', 'true').lower() == 'true',
            'max_games_per_session': int(os.getenv('MAX_GAMES_SCRAPING', '5')),
            'delay_between_requests': float(os.getenv('SCRAPING_DELAY', '4.0')),
            'headless': os.getenv('HEADLESS_MODE', 'true').lower() == 'true'
        }
    
    def _is_problematic_game(self, title: str) -> bool:
        """Vérifie si un jeu est connu pour poser des problèmes"""
        title_upper = title.upper()
        return any(keyword.upper() in title_upper for keyword in self.problematic_keywords)
    
    def _clean_game_title(self, title: str) -> str:
        """Nettoie le titre du jeu pour éviter les problèmes de recherche"""
        # Supprimer les caractères problématiques
        cleaned = re.sub(r'[^\w\s-]', '', title)
        # Supprimer les versions/éditions
        cleaned = re.sub(r'\b(HD|4K|Remaster|Edition|Collection|Anthology|ReMIX)\b', '', cleaned, flags=re.IGNORECASE)
        # Supprimer les numéros de version complexes
        cleaned = re.sub(r'\b\d+\.\d+\b', '', cleaned)
        # Nettoyer les espaces multiples
        cleaned = re.sub(r'\s+', ' ', cleaned).strip()
        
        # Limiter la longueur pour éviter les URLs trop longues
        if len(cleaned) > 50:
            words = cleaned.split()
            cleaned = ' '.join(words[:4])  # Prendre seulement les 4 premiers mots
        
        return cleaned
    
    def _setup_selenium_driver(self):
        """Configure le driver Selenium avec timeouts courts"""
        options = Options()
        
        if self.headless:
            options.add_argument('--headless=new')  # Nouveau mode headless plus stable
        
        # Configuration robuste anti-crash
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-gpu')
        options.add_argument('--disable-extensions')
        options.add_argument('--disable-images')
        options.add_argument('--disable-javascript')  # Désactiver JS pour accélérer
        options.add_argument('--disable-plugins')
        options.add_argument('--disable-background-timer-throttling')
        options.add_argument('--disable-backgrounding-occluded-windows')
        options.add_argument('--disable-renderer-backgrounding')
        options.add_argument('--disable-features=TranslateUI')
        options.add_argument('--disable-ipc-flooding-protection')
        
        # Memory et performance
        options.add_argument('--memory-pressure-off')
        options.add_argument('--max_old_space_size=4096')
        
        # User agent simple
        options.add_argument('--user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36')
        
        # Désactiver les logs pour éviter le spam
        options.add_experimental_option('excludeSwitches', ['enable-logging'])
        options.add_experimental_option('useAutomationExtension', False)
        options.add_argument('--log-level=3')
        options.add_argument('--silent')
        
        try:
            driver = webdriver.Chrome(options=options)
            
            # Timeouts très courts pour éviter les blocages
            driver.set_page_load_timeout(10)  # Maximum 10 secondes
            driver.implicitly_wait(3)
            
            # Masquer l'automation
            driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            
            return driver
        except Exception as e:
            logger.error(f"Erreur création driver: {e}")
            return None
    
    def scrape_prices(self, games_df: pd.DataFrame) -> pd.DataFrame:
        """Scrape les prix avec gestion robuste des erreurs"""
        if not self.enabled:
            logger.info("Scraping désactivé")
            return pd.DataFrame(columns=['game_id_rawg', 'title', 'platform', 'price', 'shop', 'url', 'last_update'])
        
        if games_df.empty:
            logger.info("Aucun jeu à scraper")
            return pd.DataFrame(columns=['game_id_rawg', 'title', 'platform', 'price', 'shop', 'url', 'last_update'])
        
        logger.info(f"🔍 Scraping robuste pour {len(games_df)} jeux")
        
        updated_prices = []
        driver = None
        games_processed = 0
        
        try:
            for index, game_row in games_df.head(self.max_games).iterrows():
                title = game_row.get('title', '').strip()
                game_id = game_row.get('game_id_rawg')
                
                if not title:
                    continue
                
                # Skip les jeux problématiques
                if self._is_problematic_game(title):
                    logger.warning(f"⚠️ Jeu ignoré (problématique): {title}")
                    updated_prices.append({
                        'game_id_rawg': game_id,
                        'title': title,
                        'platform': 'PC',
                        'price': None,
                        'shop': None,
                        'url': None,
                        'last_update': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    })
                    continue
                
                # Redémarrer le driver périodiquement
                if games_processed % self.restart_interval == 0:
                    if driver:
                        try:
                            driver.quit()
                        except:
                            pass
                        time.sleep(2)
                    
                    driver = self._setup_selenium_driver()
                    if not driver:
                        logger.error("Impossible de créer le driver")
                        break
                    
                    logger.info(f"🔄 Driver redémarré (jeu {games_processed + 1})")
                
                logger.info(f"🎮 [{games_processed + 1}/{self.max_games}] Recherche: {title}")
                
                try:
                    price_info = self._scrape_single_game(driver, title, game_id)
                    updated_prices.append(price_info)
                    
                    if price_info.get('price'):
                        logger.info(f"✅ Prix trouvé: {price_info['price']} chez {price_info['shop']}")
                    else:
                        logger.info(f"❌ Aucun prix pour {title}")
                        
                except Exception as e:
                    logger.error(f"❌ Erreur {title}: {str(e)[:100]}")
                    updated_prices.append({
                        'game_id_rawg': game_id,
                        'title': title,
                        'platform': 'PC',
                        'price': None,
                        'shop': None,
                        'url': None,
                        'last_update': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    })
                
                games_processed += 1
                
                # Pause entre les jeux
                if games_processed < self.max_games:
                    time.sleep(self.delay)
                
        except KeyboardInterrupt:
            logger.info("Interruption utilisateur")
        except Exception as e:
            logger.error(f"Erreur générale: {e}")
        finally:
            if driver:
                try:
                    driver.quit()
                    logger.info("🔒 Driver fermé")
                except:
                    pass
        
        successful_prices = len([p for p in updated_prices if p.get('price')])
        logger.info(f"🎯 Terminé: {successful_prices}/{len(updated_prices)} prix récupérés")
        
        return pd.DataFrame(updated_prices)
    
    def _scrape_single_game(self, driver, title: str, game_id: int) -> Dict[str, Any]:
        """Scrape un seul jeu avec timeouts courts"""
        base_result = {
            'game_id_rawg': game_id,
            'title': title,
            'platform': 'PC',
            'price': None,
            'shop': None,
            'url': None,
            'last_update': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        
        try:
            # Nettoyer le titre pour la recherche
            clean_title = self._clean_game_title(title)
            search_url = f"https://www.dlcompare.fr/search?q={clean_title.replace(' ', '+')}#all"
            
            logger.debug(f"🔗 Recherche: {search_url}")
            
            # Charger la page avec timeout court
            try:
                driver.get(search_url)
                time.sleep(2)
            except Exception as e:
                logger.debug(f"Timeout chargement page: {e}")
                return base_result
            
            # Chercher le jeu (timeout très court)
            try:
                game_element = WebDriverWait(driver, 5).until(
                    EC.element_to_be_clickable((By.CLASS_NAME, "name.clickable"))
                )
                game_element.click()
                time.sleep(2)
            except TimeoutException:
                logger.debug(f"Jeu non trouvé dans les résultats: {clean_title}")
                return base_result
            
            # Aller à la section PC
            try:
                current_url = driver.current_url
                pc_url = current_url + "#pc"
                driver.get(pc_url)
                time.sleep(1)
                
                base_result['url'] = pc_url
            except Exception as e:
                logger.debug(f"Erreur navigation PC: {e}")
                return base_result
            
            # Extraire le prix (timeout court)
            try:
                price_element = WebDriverWait(driver, 3).until(
                    EC.presence_of_element_located((By.CLASS_NAME, "lowPrice"))
                )
                price = price_element.text.strip()
                base_result['price'] = price if price else None
                
                # Extraire la boutique
                try:
                    shop_element = driver.find_element(By.CSS_SELECTOR, "a.shop > span")
                    shop = shop_element.text.strip()
                    base_result['shop'] = shop if shop else "DLCompare"
                except:
                    base_result['shop'] = "DLCompare"
                    
            except TimeoutException:
                logger.debug(f"Prix non trouvé pour: {clean_title}")
            
            return base_result
            
        except Exception as e:
            logger.debug(f"Erreur scraping {title}: {e}")
            return base_result
    
    def fetch_games_from_db(self):
        """Récupérer les jeux prioritaires depuis la base"""
        try:
            from extractor.database import DatabaseManager
            db = DatabaseManager()
            
            conn = db.get_connection()
            if not conn:
                return pd.DataFrame()
            
            cursor = conn.cursor()
            
            # Récupérer les jeux sans prix, en évitant les problématiques
            query = """
                SELECT DISTINCT g.game_id_rawg, g.title 
                FROM games g
                LEFT JOIN best_price_pc p ON g.game_id_rawg = p.game_id_rawg
                WHERE p.game_id_rawg IS NULL 
                   AND g.title NOT LIKE '%KINGDOM HEARTS%'
                   AND g.title NOT LIKE '%ReMIX%'
                   AND g.title NOT LIKE '%HD 1.5%'
                   AND g.title NOT LIKE '%Collection%'
                   AND LENGTH(g.title) < 50
                ORDER BY g.rating DESC
                LIMIT %s
            """
            
            cursor.execute(query, (self.max_games * 2,))  # Récupérer plus pour avoir des alternatives
            games = cursor.fetchall()
            conn.close()
            
            df = pd.DataFrame(games, columns=["game_id_rawg", "title"])
            logger.info(f"📋 {len(df)} jeux récupérés de la base (filtré)")
            return df
            
        except Exception as e:
            logger.error(f"Erreur récupération jeux: {e}")
            return pd.DataFrame()
    
    def test_scraping(self) -> bool:
        """Test rapide du scraping"""
        logger.info("🧪 Test scraping rapide")
        
        test_games = [
            {'game_id_rawg': 1, 'title': 'Cyberpunk 2077'},
            {'game_id_rawg': 2, 'title': 'The Witcher 3'}
        ]
        
        test_df = pd.DataFrame(test_games)
        results = self.scrape_prices(test_df)
        
        success = len([r for r in results.to_dict('records') if r.get('price')]) > 0
        
        if success:
            logger.info("✅ Test réussi")
        else:
            logger.warning("❌ Test échoué")
            
        return success

# Fonction principale pour test
def main():
    scraper = PriceScraper()
    
    if scraper.test_scraping():
        print("✅ Test OK - Lancement scraping réel")
        
        games_df = scraper.fetch_games_from_db()
        if not games_df.empty:
            prices_df = scraper.scrape_prices(games_df)
            print(f"📊 Résultats: {len(prices_df)} jeux traités")
            
            # Afficher les prix trouvés
            successful = prices_df[prices_df['price'].notna()]
            if not successful.empty:
                print("\n💰 Prix récupérés:")
                for _, row in successful.iterrows():
                    print(f"  - {row['title']}: {row['price']} ({row['shop']})")
        else:
            print("❌ Aucun jeu trouvé en base")
    else:
        print("❌ Test échoué")

if __name__ == "__main__":
    main()
