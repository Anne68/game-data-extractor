"""
💰 Module de scraping des prix avec Selenium - Version complète inspirée du notebook
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
    """Scraper de prix avec Selenium pour DLCompare - Inspiré du notebook Jupyter"""
    
    def __init__(self):
        try:
            from utils.config import ConfigManager
            config = ConfigManager()
            scraping_config = config.get_scraping_config()
        except ImportError:
            scraping_config = self._get_config_from_env()
        
        self.enabled = scraping_config.get('enabled', True)
        self.max_games = scraping_config.get('max_games_per_session', 20)
        self.delay = scraping_config.get('delay_between_requests', 3)
        self.headless = scraping_config.get('headless', True)
        
        logger.info(f"PriceScraper Selenium initialisé - Enabled: {self.enabled}, Headless: {self.headless}")
    
    def _get_config_from_env(self) -> Dict[str, Any]:
        return {
            'enabled': os.getenv('SCRAPING_ENABLED', 'true').lower() == 'true',
            'max_games_per_session': int(os.getenv('MAX_GAMES_SCRAPING', '20')),
            'delay_between_requests': float(os.getenv('SCRAPING_DELAY', '3.0')),
            'headless': os.getenv('HEADLESS_MODE', 'true').lower() == 'true'
        }
    
    def _setup_selenium_driver(self):
        """Configure le driver Selenium Chrome - Inspiré du notebook"""
        options = Options()
        
        if self.headless:
            options.add_argument('--headless')
        
        # Options anti-détection (inspirées du notebook)
        options.add_argument('--disable-blink-features=AutomationControlled')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-gpu')
        options.add_argument('--disable-extensions')
        options.add_argument('--disable-plugins')
        options.add_argument('--disable-images')  # Accélérer le chargement
        options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
        
        # Désactiver les logs
        options.add_experimental_option('excludeSwitches', ['enable-logging'])
        options.add_experimental_option('useAutomationExtension', False)
        
        try:
            driver = webdriver.Chrome(options=options)
            driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            return driver
        except Exception as e:
            logger.error(f"Erreur création driver Selenium: {e}")
            return None
    
    def scrape_prices(self, games_df: pd.DataFrame) -> pd.DataFrame:
        """Scrape les prix depuis DLCompare avec Selenium - Structure du notebook"""
        if not self.enabled:
            logger.info("Scraping désactivé")
            return pd.DataFrame(columns=['game_id_rawg', 'title', 'platform', 'price', 'shop', 'url', 'last_update'])
        
        if games_df.empty:
            logger.info("Aucun jeu à scraper")
            return pd.DataFrame(columns=['game_id_rawg', 'title', 'platform', 'price', 'shop', 'url', 'last_update'])
        
        logger.info(f"🔍 Scraping Selenium des prix pour {len(games_df)} jeux depuis DLCompare")
        
        driver = self._setup_selenium_driver()
        if not driver:
            logger.error("Impossible d'initialiser Selenium")
            return pd.DataFrame()
        
        updated_prices = []
        
        try:
            for index, game in games_df.head(min(self.max_games, len(games_df))).iterrows():
                game_title = game.get('title', '').strip()
                game_id = game.get('game_id_rawg')
                
                if not game_title:
                    continue
                
                logger.info(f"🎮 Recherche prix pour: {game_title}")
                
                try:
                    price_info = self._scrape_game_price_selenium(driver, game_title, game_id)
                    
                    if price_info:
                        updated_prices.append(price_info)
                        logger.info(f"✅ Prix trouvé: {price_info['price']} chez {price_info['shop']}")
                    else:
                        logger.warning(f"❌ Aucun prix trouvé pour {game_title}")
                        # Ajouter une entrée vide pour traçabilité
                        updated_prices.append({
                            'game_id_rawg': game_id,
                            'title': game_title,
                            'platform': 'PC',
                            'price': None,
                            'shop': None,
                            'url': None,
                            'last_update': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        })
                    
                except Exception as e:
                    logger.error(f"❌ Erreur scraping {game_title}: {e}")
                    updated_prices.append({
                        'game_id_rawg': game_id,
                        'title': game_title,
                        'platform': 'PC',
                        'price': None,
                        'shop': None,
                        'url': None,
                        'last_update': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    })
                
                # Délai entre les jeux
                time.sleep(self.delay)
                
        finally:
            driver.quit()
            logger.info("🔒 Driver Selenium fermé")
        
        logger.info(f"🎯 Scraping terminé: {len([p for p in updated_prices if p['price']])} prix récupérés sur {len(updated_prices)} jeux")
        return pd.DataFrame(updated_prices)
    
    def _scrape_game_price_selenium(self, driver, game_title: str, game_id: int) -> Optional[Dict[str, Any]]:
        """Scrape le prix d'un jeu spécifique - Méthode inspirée du notebook"""
        try:
            # URL de recherche DLCompare (structure du notebook)
            search_term = game_title.replace(' ', '+').replace(':', '').replace('(', '').replace(')', '')
            search_url = f"https://www.dlcompare.fr/search?q={search_term}#all"
            
            logger.debug(f"🔗 URL recherche: {search_url}")
            
            driver.get(search_url)
            time.sleep(2)  # Attendre le chargement initial
            
            # Chercher le premier jeu dans les résultats (logique du notebook)
            try:
                # Attendre que les résultats se chargent
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CLASS_NAME, "name"))
                )
                
                # Cliquer sur le premier jeu trouvé (comme dans le notebook)
                game_element = WebDriverWait(driver, 5).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, ".name.clickable, .name a, [class*='game-title'] a"))
                )
                
                game_element.click()
                time.sleep(3)  # Attendre la redirection
                
                # Aller à la section PC (structure du notebook)
                current_url = driver.current_url
                if "#pc" not in current_url:
                    pc_url = current_url.rstrip('/') + "#pc"
                    driver.get(pc_url)
                    time.sleep(2)
                
                # Extraire le meilleur prix (logique du notebook)
                best_price_info = self._extract_best_price_selenium(driver, current_url)
                
                if best_price_info:
                    return {
                        'game_id_rawg': game_id,
                        'title': game_title,
                        'platform': 'PC',
                        'price': best_price_info['price'],
                        'shop': best_price_info['shop'],
                        'url': current_url,
                        'last_update': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    }
                
                return None
                
            except TimeoutException:
                logger.debug(f"❌ Timeout: Aucun résultat trouvé pour {game_title}")
                return None
            except Exception as e:
                logger.debug(f"❌ Erreur navigation pour {game_title}: {e}")
                return None
                
        except Exception as e:
            logger.error(f"❌ Erreur générale scraping {game_title}: {e}")
            return None
    
    def _extract_best_price_selenium(self, driver, game_url: str) -> Optional[Dict[str, str]]:
        """Extrait le meilleur prix depuis la page du jeu - Inspiré du notebook"""
        try:
            best_price = None
            best_shop = None
            
            # Sélecteurs multiples pour les prix (basés sur l'observation DLCompare)
            price_selectors = [
                ".lowPrice",
                ".price.lowest",
                ".best-price",
                "[class*='price'][class*='low']",
                ".price-value",
                ".offer-price"
            ]
            
            # Sélecteurs multiples pour les boutiques
            shop_selectors = [
                "a.shop > span",
                ".shop-name",
                ".retailer-name",
                "[class*='shop'] span",
                ".vendor-name"
            ]
            
            # Chercher le prix le plus bas
            for price_selector in price_selectors:
                try:
                    price_element = WebDriverWait(driver, 3).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, price_selector))
                    )
                    best_price = price_element.text.strip()
                    if best_price and ('€' in best_price or '$' in best_price):
                        logger.debug(f"💰 Prix trouvé avec sélecteur {price_selector}: {best_price}")
                        break
                except:
                    continue
            
            # Chercher le nom de la boutique
            for shop_selector in shop_selectors:
                try:
                    shop_element = driver.find_element(By.CSS_SELECTOR, shop_selector)
                    best_shop = shop_element.text.strip()
                    if best_shop:
                        logger.debug(f"🏪 Boutique trouvée avec sélecteur {shop_selector}: {best_shop}")
                        break
                except:
                    continue
            
            # Fallback: chercher dans tout le DOM
            if not best_price:
                try:
                    # Recherche générique des prix dans le texte
                    page_source = driver.page_source.lower()
                    import re
                    price_matches = re.findall(r'(\d+[,.]?\d*)\s*€', page_source)
                    if price_matches:
                        # Prendre le premier prix trouvé
                        best_price = f"{price_matches[0]}€"
                        logger.debug(f"💰 Prix trouvé via regex: {best_price}")
                except:
                    pass
            
            if not best_shop:
                # Boutiques connues à chercher dans le texte
                known_shops = ['Steam', 'Epic Games Store', 'GOG', 'Origin', 'Uplay', 'Microsoft Store', 'Kinguin', 'G2A', 'CDKeys']
                page_text = driver.page_source.lower()
                for shop in known_shops:
                    if shop.lower() in page_text:
                        best_shop = shop
                        break
                
                if not best_shop:
                    best_shop = "DLCompare"
            
            if best_price:
                return {
                    'price': best_price,
                    'shop': best_shop or "DLCompare"
                }
            
            return None
            
        except Exception as e:
            logger.error(f"❌ Erreur extraction prix: {e}")
            return None
    
    def test_scraping(self, test_games: List[Dict[str, str]]) -> bool:
        """Test le scraping avec Selenium - Version notebook"""
        logger.info("🧪 Test de scraping Selenium")
        
        test_df = pd.DataFrame([{
            'game_id_rawg': 12345,
            'title': 'Cyberpunk 2077'
        }])
        
        results = self.scrape_prices(test_df)
        success = not results.empty and len(results) > 0
        
        if success:
            logger.info("✅ Test Selenium réussi")
        else:
            logger.warning("❌ Test Selenium échoué")
            
        return success
