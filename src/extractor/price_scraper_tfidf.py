
"""
💰 Price Scraper avec matching TF-IDF amélioré
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
from selenium.common.exceptions import TimeoutException, NoSuchElementException

sys.path.insert(0, str(Path(__file__).parent.parent))
logger = logging.getLogger(__name__)

class PriceScraperTFIDF:
    """Scraper de prix avec matching TF-IDF intelligent"""
    
    def __init__(self):
        try:
            from utils.config import ConfigManager
            config = ConfigManager()
            scraping_config = config.get_scraping_config()
        except ImportError:
            scraping_config = self._get_config_from_env()
        
        self.enabled = scraping_config.get('enabled', True)
        self.max_games = scraping_config.get('max_games_per_session', 10)
        self.delay = scraping_config.get('delay_between_requests', 3)
        self.headless = scraping_config.get('headless', True)
        
        # Initialiser le matcher TF-IDF
        from utils.text_similarity import GameTitleMatcher
        self.title_matcher = GameTitleMatcher(similarity_threshold=0.6)
        
        logger.info(f"PriceScraperTFIDF initialisé - TF-IDF threshold: 0.6")
    
    def _get_config_from_env(self) -> Dict[str, Any]:
        return {
            'enabled': os.getenv('SCRAPING_ENABLED', 'true').lower() == 'true',
            'max_games_per_session': int(os.getenv('MAX_GAMES_SCRAPING', '10')),
            'delay_between_requests': float(os.getenv('SCRAPING_DELAY', '3.0')),
            'headless': os.getenv('HEADLESS_MODE', 'true').lower() == 'true'
        }
    
    def _setup_driver(self):
        """Configure le driver Selenium"""
        options = Options()
        
        if self.headless:
            options.add_argument('--headless=new')
        
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-gpu')
        options.add_argument('--disable-extensions')
        options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36')
        
        try:
            driver = webdriver.Chrome(options=options)
            driver.set_page_load_timeout(15)
            driver.implicitly_wait(5)
            return driver
        except Exception as e:
            logger.error(f"Erreur création driver: {e}")
            return None
    
    def _search_and_find_best_match(self, driver, search_title: str) -> Tuple[Optional[str], float]:
        """
        Recherche et trouve le meilleur match avec TF-IDF
        
        Returns:
            Tuple (URL du jeu trouvé, score de similarité)
        """
        try:
            # Nettoyer le titre pour la recherche
            clean_search = self.title_matcher.normalize_title(search_title)
            search_url = f"https://www.dlcompare.fr/search?q={clean_search.replace(' ', '+')}"
            
            logger.info(f"🔍 Recherche TF-IDF: '{search_title}' -> '{clean_search}'")
            
            driver.get(search_url)
            time.sleep(2)
            
            # Récupérer tous les jeux trouvés
            game_elements = driver.find_elements(By.CSS_SELECTOR, ".search-result .game-item")
            
            if not game_elements:
                logger.debug("Aucun jeu trouvé dans les résultats")
                return None, 0.0
            
            # Extraire les titres et URLs
            candidates = []
            for element in game_elements[:10]:  # Limiter aux 10 premiers
                try:
                    title_element = element.find_element(By.CSS_SELECTOR, ".name, .title, h3, h4")
                    url_element = element.find_element(By.CSS_SELECTOR, "a")
                    
                    candidate_title = title_element.text.strip()
                    candidate_url = url_element.get_attribute('href')
                    
                    if candidate_title and candidate_url:
                        candidates.append((candidate_title, candidate_url))
                        
                except Exception as e:
                    logger.debug(f"Erreur extraction candidat: {e}")
                    continue
            
            if not candidates:
                logger.debug("Aucun candidat valide trouvé")
                return None, 0.0
            
            # Utiliser TF-IDF pour trouver le meilleur match
            candidate_titles = [title for title, _ in candidates]
            best_idx, similarity_score = self.title_matcher.find_best_match(search_title, candidate_titles)
            
            if best_idx is not None:
                best_title, best_url = candidates[best_idx]
                logger.info(f"✅ Meilleur match TF-IDF: '{best_title}' (score: {similarity_score:.3f})")
                return best_url, similarity_score
            else:
                logger.info(f"❌ Aucun match suffisant (meilleur score: {similarity_score:.3f})")
                return None, similarity_score
                
        except Exception as e:
            logger.error(f"Erreur recherche TF-IDF: {e}")
            return None, 0.0
    
    def _extract_price_from_page(self, driver, game_url: str) -> Dict[str, Any]:
        """Extrait le prix depuis la page du jeu"""
        try:
            # Aller sur la page PC du jeu
            pc_url = f"{game_url}#pc"
            driver.get(pc_url)
            time.sleep(2)
            
            # Extraire le prix
            try:
                price_element = WebDriverWait(driver, 5).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, ".lowPrice, .best-price, .price"))
                )
                price = price_element.text.strip()
                
                # Extraire la boutique
                try:
                    shop_element = driver.find_element(By.CSS_SELECTOR, ".shop span, .merchant, .store-name")
                    shop = shop_element.text.strip()
                except:
                    shop = "DLCompare"
                
                return {
                    'price': price if price else None,
                    'shop': shop,
                    'url': pc_url
                }
                
            except TimeoutException:
                logger.debug("Prix non trouvé sur la page")
                return {'price': None, 'shop': None, 'url': pc_url}
                
        except Exception as e:
            logger.error(f"Erreur extraction prix: {e}")
            return {'price': None, 'shop': None, 'url': None}
    
    def scrape_prices(self, games_df: pd.DataFrame) -> pd.DataFrame:
        """Scrape les prix avec matching TF-IDF"""
        if not self.enabled or games_df.empty:
            return pd.DataFrame(columns=['game_id_rawg', 'title', 'platform', 'price', 'shop', 'url', 'last_update', 'similarity_score'])
        
        logger.info(f"🔍 Scraping TF-IDF pour {len(games_df)} jeux")
        
        results = []
        driver = None
        
        try:
            driver = self._setup_driver()
            if not driver:
                logger.error("Impossible de créer le driver")
                return pd.DataFrame(columns=['game_id_rawg', 'title', 'platform', 'price', 'shop', 'url', 'last_update', 'similarity_score'])
            
            for index, game_row in games_df.head(self.max_games).iterrows():
                title = game_row.get('title', '').strip()
                game_id = game_row.get('game_id_rawg')
                
                if not title:
                    continue
                
                logger.info(f"🎮 [{index + 1}/{len(games_df)}] Traitement: {title}")
                
                try:
                    # Rechercher avec TF-IDF
                    game_url, similarity_score = self._search_and_find_best_match(driver, title)
                    
                    base_result = {
                        'game_id_rawg': game_id,
                        'title': title,
                        'platform': 'PC',
                        'price': None,
                        'shop': None,
                        'url': None,
                        'last_update': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        'similarity_score': similarity_score
                    }
                    
                    if game_url and similarity_score >= self.title_matcher.similarity_threshold:
                        # Extraire le prix
                        price_info = self._extract_price_from_page(driver, game_url)
                        base_result.update(price_info)
                        
                        if price_info.get('price'):
                            logger.info(f"✅ Prix trouvé: {price_info['price']} chez {price_info['shop']} (similarité: {similarity_score:.3f})")
                        else:
                            logger.info(f"⚠️ Jeu trouvé mais prix indisponible (similarité: {similarity_score:.3f})")
                    else:
                        logger.info(f"❌ Aucun match suffisant pour '{title}' (similarité: {similarity_score:.3f})")
                    
                    results.append(base_result)
                    
                except Exception as e:
                    logger.error(f"❌ Erreur pour {title}: {e}")
                    results.append({
                        'game_id_rawg': game_id,
                        'title': title,
                        'platform': 'PC',
                        'price': None,
                        'shop': None,
                        'url': None,
                        'last_update': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        'similarity_score': 0.0
                    })
                
                # Pause entre les jeux
                if index < len(games_df) - 1:
                    time.sleep(self.delay)
                    
        except Exception as e:
            logger.error(f"Erreur générale scraping: {e}")
            
        finally:
            if driver:
                try:
                    driver.quit()
                except:
                    pass
        
        # Statistiques finales
        successful = len([r for r in results if r.get('price')])
        avg_similarity = sum(r.get('similarity_score', 0) for r in results) / max(len(results), 1)
        
        logger.info(f"🎯 Scraping TF-IDF terminé: {successful}/{len(results)} prix trouvés (similarité moyenne: {avg_similarity:.3f})")
        
        return pd.DataFrame(results)
    
    def test_tfidf_matching(self):
        """Test du matching TF-IDF"""
        test_games = [
            {'game_id_rawg': 1, 'title': 'Cyberpunk 2077'},
            {'game_id_rawg': 2, 'title': 'The Witcher 3 Wild Hunt'},
            {'game_id_rawg': 3, 'title': 'Red Dead Redemption 2'}
        ]
        
        logger.info("🧪 Test du matching TF-IDF")
        test_df = pd.DataFrame(test_games)
        results = self.scrape_prices(test_df)
        
        print("\n📊 Résultats du test TF-IDF:")
        for _, row in results.iterrows():
            print(f"• {row['title']}: {row['price']} (similarité: {row['similarity_score']:.3f})")
        
        return not results.empty

# Fonction principale pour test
def main():
    scraper = PriceScraperTFIDF()
    
    # Test du matching TF-IDF
    scraper.test_tfidf_matching()

if __name__ == "__main__":
    main()
