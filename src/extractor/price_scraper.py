"""
💰 Price Scraper RÉEL avec TF-IDF pour DLCompare
"""

import pandas as pd
import time
import logging
import os
import sys
import re
from datetime import datetime
from typing import Dict, Any, List, Optional, Tuple
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
logger = logging.getLogger(__name__)

# Import Selenium
try:
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.common.exceptions import TimeoutException, NoSuchElementException
    SELENIUM_AVAILABLE = True
except ImportError:
    SELENIUM_AVAILABLE = False
    logger.error("❌ Selenium requis pour le vrai scraping")

# Import TF-IDF
try:
    from sklearn.feature_extraction.text import TfidfVectorizer
    from sklearn.metrics.pairwise import cosine_similarity
    import numpy as np
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False

class GameTitleMatcher:
    """Matcher TF-IDF pour titres de jeux"""
    
    def __init__(self, similarity_threshold: float = 0.6):
        self.similarity_threshold = similarity_threshold
        
        if SKLEARN_AVAILABLE:
            self.vectorizer = TfidfVectorizer(
                lowercase=True,
                stop_words='english',
                ngram_range=(1, 2),
                max_features=1000,
                token_pattern=r'\b[a-zA-Z][a-zA-Z0-9]*\b'
            )
            self.enabled = True
        else:
            self.enabled = False
    
    def normalize_title(self, title: str) -> str:
        """Normalise un titre de jeu"""
        if not title:
            return ""
        
        normalized = title.lower()
        
        # Supprimer éditions et versions
        normalized = re.sub(r'\b(goty|ultimate|deluxe|premium|collector|special|limited)\b', '', normalized)
        normalized = re.sub(r'\b(edition|version|remaster|hd|4k|enhanced|definitive)\b', '', normalized)
        normalized = re.sub(r'\b(pack|bundle|collection|anthology)\b', '', normalized)
        
        # Supprimer plateformes
        normalized = re.sub(r'\b(pc|ps4|ps5|xbox|nintendo|switch|steam)\b', '', normalized)
        normalized = re.sub(r'\(\d{4}\)', '', normalized)
        
        # Nettoyer
        normalized = re.sub(r'[^\w\s]', ' ', normalized)
        normalized = re.sub(r'\s+', ' ', normalized).strip()
        
        return normalized
    
    def find_best_match(self, search_title: str, candidate_titles: List[str]) -> Tuple[Optional[int], float]:
        """Trouve le meilleur match avec TF-IDF"""
        if not search_title or not candidate_titles:
            return None, 0.0
        
        if not self.enabled:
            return self._simple_match(search_title, candidate_titles)
        
        normalized_search = self.normalize_title(search_title)
        normalized_candidates = [self.normalize_title(title) for title in candidate_titles]
        
        valid_candidates = [(i, title) for i, title in enumerate(normalized_candidates) if title.strip()]
        
        if not valid_candidates:
            return None, 0.0
        
        try:
            all_texts = [normalized_search] + [title for _, title in valid_candidates]
            tfidf_matrix = self.vectorizer.fit_transform(all_texts)
            similarities = cosine_similarity(tfidf_matrix[0:1], tfidf_matrix[1:]).flatten()
            
            best_idx = np.argmax(similarities)
            best_score = similarities[best_idx]
            original_idx = valid_candidates[best_idx][0]
            
            if best_score >= self.similarity_threshold:
                return original_idx, best_score
            else:
                return None, best_score
                
        except Exception as e:
            logger.error(f"Erreur TF-IDF: {e}")
            return self._simple_match(search_title, candidate_titles)
    
    def _simple_match(self, search_title: str, candidate_titles: List[str]) -> Tuple[Optional[int], float]:
        """Matching simple par mots-clés"""
        normalized_search = self.normalize_title(search_title).lower()
        search_words = set(normalized_search.split())
        
        best_score = 0.0
        best_idx = None
        
        for i, candidate in enumerate(candidate_titles):
            normalized_candidate = self.normalize_title(candidate).lower()
            candidate_words = set(normalized_candidate.split())
            
            if search_words and candidate_words:
                intersection = len(search_words.intersection(candidate_words))
                union = len(search_words.union(candidate_words))
                score = intersection / union if union > 0 else 0.0
                
                if score > best_score:
                    best_score = score
                    best_idx = i
        
        if best_score >= 0.3:
            return best_idx, best_score
        else:
            return None, best_score

class RealPriceScraper:
    """Scraper RÉEL pour DLCompare avec TF-IDF"""
    
    def __init__(self):
        self.enabled = SELENIUM_AVAILABLE
        self.max_games = 5  # Commencer petit
        self.delay = 3
        self.headless = True
        self.title_matcher = GameTitleMatcher(similarity_threshold=0.6)
        
        if not SELENIUM_AVAILABLE:
            logger.error("❌ Selenium requis pour le scraping réel")
        
        logger.info(f"RealPriceScraper - TF-IDF: {'✅' if self.title_matcher.enabled else '❌'}")
    
    def _setup_driver(self):
        """Configure Chrome pour DLCompare"""
        if not SELENIUM_AVAILABLE:
            return None
        
        options = Options()
        
        if self.headless:
            options.add_argument('--headless=new')
        
        # Configuration robuste
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-gpu')
        options.add_argument('--disable-extensions')
        options.add_argument('--disable-images')
        options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36')
        
        try:
            driver = webdriver.Chrome(options=options)
            driver.set_page_load_timeout(20)
            driver.implicitly_wait(5)
            return driver
        except Exception as e:
            logger.error(f"Erreur création driver: {e}")
            return None
    
    def _search_game_on_dlcompare(self, driver, title: str) -> Tuple[Optional[str], float]:
        """Recherche un jeu sur DLCompare et trouve le meilleur match"""
        try:
            # Nettoyer le titre pour la recherche
            clean_title = self.title_matcher.normalize_title(title)
            search_query = clean_title.replace(' ', '+')
            search_url = f"https://www.dlcompare.fr/search?q={search_query}"
            
            logger.info(f"🔍 Recherche: {title} -> {search_url}")
            
            driver.get(search_url)
            time.sleep(3)
            
            # Chercher les résultats de jeux
            try:
                # Différents sélecteurs possibles pour DLCompare
                game_elements = driver.find_elements(By.CSS_SELECTOR, ".search-result, .game-item, .product-item")
                
                if not game_elements:
                    # Essayer d'autres sélecteurs
                    game_elements = driver.find_elements(By.CSS_SELECTOR, "h3 a, .title a, .name a")
                
                if not game_elements:
                    logger.warning(f"Aucun résultat trouvé pour: {title}")
                    return None, 0.0
                
                # Extraire les titres et URLs
                candidates = []
                for element in game_elements[:10]:  # Limiter aux 10 premiers
                    try:
                        if element.tag_name == 'a':
                            game_title = element.text.strip()
                            game_url = element.get_attribute('href')
                        else:
                            link = element.find_element(By.TAG_NAME, 'a')
                            game_title = link.text.strip()
                            game_url = link.get_attribute('href')
                        
                        if game_title and game_url:
                            candidates.append((game_title, game_url))
                            
                    except Exception as e:
                        logger.debug(f"Erreur extraction candidat: {e}")
                        continue
                
                if not candidates:
                    logger.warning(f"Aucun candidat valide pour: {title}")
                    return None, 0.0
                
                # Utiliser TF-IDF pour trouver le meilleur match
                candidate_titles = [title for title, _ in candidates]
                best_idx, similarity_score = self.title_matcher.find_best_match(title, candidate_titles)
                
                if best_idx is not None:
                    best_title, best_url = candidates[best_idx]
                    logger.info(f"✅ Match trouvé: '{best_title}' (score: {similarity_score:.3f})")
                    return best_url, similarity_score
                else:
                    logger.info(f"❌ Aucun match suffisant (meilleur score: {similarity_score:.3f})")
                    return None, similarity_score
                    
            except Exception as e:
                logger.error(f"Erreur extraction résultats: {e}")
                return None, 0.0
                
        except Exception as e:
            logger.error(f"Erreur recherche DLCompare: {e}")
            return None, 0.0
    
    def _extract_price_from_game_page(self, driver, game_url: str) -> Dict[str, Any]:
        """Extrait le prix depuis la page du jeu"""
        try:
            # Aller sur la page PC du jeu
            if '#pc' not in game_url:
                pc_url = f"{game_url}#pc"
            else:
                pc_url = game_url
            
            driver.get(pc_url)
            time.sleep(2)
            
            # Chercher le prix (plusieurs sélecteurs possibles)
            price_selectors = [
                ".lowPrice",
                ".best-price",
                ".price-value",
                ".price",
                "[data-price]"
            ]
            
            price = None
            for selector in price_selectors:
                try:
                    price_element = WebDriverWait(driver, 5).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, selector))
                    )
                    price = price_element.text.strip()
                    if price:
                        break
                except TimeoutException:
                    continue
            
            # Chercher la boutique
            shop_selectors = [
                ".shop span",
                ".merchant",
                ".store-name",
                ".shop-name"
            ]
            
            shop = None
            for selector in shop_selectors:
                try:
                    shop_element = driver.find_element(By.CSS_SELECTOR, selector)
                    shop = shop_element.text.strip()
                    if shop:
                        break
                except NoSuchElementException:
                    continue
            
            if not shop:
                shop = "DLCompare"
            
            return {
                'price': price,
                'shop': shop,
                'url': pc_url
            }
            
        except Exception as e:
            logger.error(f"Erreur extraction prix: {e}")
            return {
                'price': None,
                'shop': None,
                'url': game_url
            }
    
    def scrape_prices(self, games_df: pd.DataFrame) -> pd.DataFrame:
        """Scraping RÉEL avec TF-IDF"""
        if not self.enabled or games_df.empty:
            logger.warning("Scraping désactivé ou aucun jeu")
            return pd.DataFrame(columns=['game_id_rawg', 'title', 'platform', 'price', 'shop', 'url', 'last_update', 'similarity_score'])
        
        logger.info(f"🔍 SCRAPING RÉEL pour {len(games_df)} jeux")
        
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
                
                logger.info(f"🎮 [{index + 1}/{len(games_df)}] Scraping: {title}")
                
                try:
                    # Rechercher le jeu
                    game_url, similarity_score = self._search_game_on_dlcompare(driver, title)
                    
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
                        price_info = self._extract_price_from_game_page(driver, game_url)
                        base_result.update(price_info)
                        
                        if price_info.get('price'):
                            logger.info(f"✅ Prix trouvé: {price_info['price']} chez {price_info['shop']}")
                        else:
                            logger.info(f"⚠️ Jeu trouvé mais prix indisponible")
                    else:
                        logger.info(f"❌ Aucun match suffisant")
                    
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
                    logger.info(f"⏸️ Pause de {self.delay}s...")
                    time.sleep(self.delay)
                    
        except Exception as e:
            logger.error(f"Erreur générale scraping: {e}")
            
        finally:
            if driver:
                try:
                    driver.quit()
                    logger.info("🔒 Driver fermé")
                except:
                    pass
        
        # Statistiques finales
        successful = len([r for r in results if r.get('price')])
        avg_similarity = sum(r.get('similarity_score', 0) for r in results) / max(len(results), 1)
        
        logger.info(f"🎯 Scraping RÉEL terminé: {successful}/{len(results)} prix trouvés (similarité moy: {avg_similarity:.3f})")
        
        return pd.DataFrame(results)

def main():
    """Test du scraper réel"""
    scraper = RealPriceScraper()
    
    import pandas as pd
    test_games = pd.DataFrame([
        {'game_id_rawg': 3498, 'title': 'Grand Theft Auto V'},
        {'game_id_rawg': 4200, 'title': 'Portal 2'},
        {'game_id_rawg': 28, 'title': 'Red Dead Redemption 2'}
    ])
    
    print("🧪 Test du scraper RÉEL")
    results = scraper.scrape_prices(test_games)
    
    print("\n📊 Résultats:")
    for _, row in results.iterrows():
        similarity = row.get('similarity_score', 0)
        price = row.get('price', 'N/A')
        shop = row.get('shop', 'N/A')
        
        quality = "🔥" if similarity >= 0.8 else "✅" if similarity >= 0.6 else "⚠️"
        
        print(f"{quality} {row['title']}")
        print(f"   Prix: {price} chez {shop}")
        print(f"   Similarité: {similarity:.3f}")
        print()

if __name__ == "__main__":
    main()
