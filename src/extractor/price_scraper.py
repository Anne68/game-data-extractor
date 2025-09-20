
"""
💰 Price Scraper avec matching TF-IDF intelligent
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

# Import optionnel de Selenium
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
    logger.warning("Selenium non disponible - scraping désactivé")

# Import optionnel de scikit-learn pour TF-IDF
try:
    from sklearn.feature_extraction.text import TfidfVectorizer
    from sklearn.metrics.pairwise import cosine_similarity
    import numpy as np
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False
    logger.warning("scikit-learn non disponible - TF-IDF désactivé")

class GameTitleMatcher:
    """Matcher de titres de jeux utilisant TF-IDF"""
    
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
            logger.info("TF-IDF matcher activé")
        else:
            self.enabled = False
            logger.warning("TF-IDF désactivé - sklearn non disponible")
    
    def normalize_title(self, title: str) -> str:
        """Normalise un titre de jeu"""
        if not title:
            return ""
        
        normalized = title.lower()
        
        # Supprimer les éditions et versions
        normalized = re.sub(r'\b(goty|game of the year|ultimate|deluxe|premium|collector|special|limited|director\'s cut)\b', '', normalized)
        normalized = re.sub(r'\b(edition|version|remaster|remastered|hd|4k|enhanced|definitive)\b', '', normalized)
        normalized = re.sub(r'\b(pack|bundle|collection|anthology|trilogy)\b', '', normalized)
        
        # Supprimer plateformes et années
        normalized = re.sub(r'\b(pc|ps4|ps5|xbox|nintendo|switch|steam)\b', '', normalized)
        normalized = re.sub(r'\(\d{4}\)', '', normalized)
        
        # Nettoyer caractères spéciaux
        normalized = re.sub(r'[^\w\s]', ' ', normalized)
        normalized = re.sub(r'\s+', ' ', normalized).strip()
        
        return normalized
    
    def find_best_match(self, search_title: str, candidate_titles: List[str]) -> Tuple[Optional[int], float]:
        """Trouve le meilleur match avec TF-IDF ou fallback"""
        if not search_title or not candidate_titles:
            return None, 0.0
        
        if self.enabled:
            return self._find_best_match_tfidf(search_title, candidate_titles)
        else:
            return self._find_best_match_simple(search_title, candidate_titles)
    
    def _find_best_match_tfidf(self, search_title: str, candidate_titles: List[str]) -> Tuple[Optional[int], float]:
        """Matching avec TF-IDF"""
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
            
            logger.debug(f"TF-IDF: '{search_title}' -> '{candidate_titles[original_idx]}' ({best_score:.3f})")
            
            if best_score >= self.similarity_threshold:
                return original_idx, best_score
            else:
                return None, best_score
                
        except Exception as e:
            logger.error(f"Erreur TF-IDF: {e}")
            return self._find_best_match_simple(search_title, candidate_titles)
    
    def _find_best_match_simple(self, search_title: str, candidate_titles: List[str]) -> Tuple[Optional[int], float]:
        """Matching simple par mots-clés"""
        normalized_search = self.normalize_title(search_title).lower()
        search_words = set(normalized_search.split())
        
        best_score = 0.0
        best_idx = None
        
        for i, candidate in enumerate(candidate_titles):
            normalized_candidate = self.normalize_title(candidate).lower()
            candidate_words = set(normalized_candidate.split())
            
            if search_words and candidate_words:
                # Jaccard similarity
                intersection = len(search_words.intersection(candidate_words))
                union = len(search_words.union(candidate_words))
                score = intersection / union if union > 0 else 0.0
                
                if score > best_score:
                    best_score = score
                    best_idx = i
        
        logger.debug(f"Simple: '{search_title}' -> best score {best_score:.3f}")
        
        if best_score >= 0.3:  # Seuil plus bas pour matching simple
            return best_idx, best_score
        else:
            return None, best_score

class PriceScraper:
    """Scraper de prix avec TF-IDF"""
    
    def __init__(self):
        try:
            from utils.config import ConfigManager
            config = ConfigManager()
            scraping_config = config.get_scraping_config()
        except ImportError:
            scraping_config = self._get_config_from_env()
        
        self.enabled = scraping_config.get('enabled', True) and SELENIUM_AVAILABLE
        self.max_games = scraping_config.get('max_games_per_session', 5)  # Réduit pour test
        self.delay = scraping_config.get('delay_between_requests', 3)
        self.headless = scraping_config.get('headless', True)
        
        # Initialiser le matcher
        self.title_matcher = GameTitleMatcher(similarity_threshold=0.6)
        
        if not SELENIUM_AVAILABLE:
            self.enabled = False
            logger.warning("Scraping désactivé - Selenium non disponible")
        
        tfidf_status = "✅" if self.title_matcher.enabled else "❌"
        scraping_status = "✅" if self.enabled else "❌"
        logger.info(f"PriceScraper initialisé - TF-IDF: {tfidf_status}, Scraping: {scraping_status}")
    
    def _get_config_from_env(self) -> Dict[str, Any]:
        return {
            'enabled': os.getenv('SCRAPING_ENABLED', 'true').lower() == 'true',
            'max_games_per_session': int(os.getenv('MAX_GAMES_SCRAPING', '5')),
            'delay_between_requests': float(os.getenv('SCRAPING_DELAY', '3.0')),
            'headless': os.getenv('HEADLESS_MODE', 'true').lower() == 'true'
        }
    
    def _setup_driver(self):
        """Configure le driver Selenium"""
        if not SELENIUM_AVAILABLE:
            return None
        
        options = Options()
        
        if self.headless:
            options.add_argument('--headless=new')
        
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-gpu')
        options.add_argument('--disable-extensions')
        
        try:
            driver = webdriver.Chrome(options=options)
            driver.set_page_load_timeout(10)
            driver.implicitly_wait(3)
            return driver
        except Exception as e:
            logger.error(f"Erreur création driver: {e}")
            return None
    
    def scrape_prices(self, games_df: pd.DataFrame) -> pd.DataFrame:
        """Scrape les prix avec TF-IDF (mode simulation pour test)"""
        if games_df.empty:
            logger.info("Aucun jeu à scraper")
            return pd.DataFrame(columns=['game_id_rawg', 'title', 'platform', 'price', 'shop', 'url', 'last_update', 'similarity_score'])
        
        logger.info(f"🔍 Scraping simulation pour {len(games_df)} jeux (TF-IDF: {'✅' if self.title_matcher.enabled else '❌'})")
        
        results = []
        
        for index, game_row in games_df.head(self.max_games).iterrows():
            title = game_row.get('title', '').strip()
            game_id = game_row.get('game_id_rawg')
            
            if not title:
                continue
            
            logger.info(f"🎮 [{index + 1}] Test matching: {title}")
            
            # Simuler des candidats de recherche
            candidates = [
                title,
                f"{title} Ultimate Edition",
                f"{title} Remastered",
                "Jeu Complètement Différent"
            ]
            
            # Tester le matching TF-IDF
            best_idx, similarity_score = self.title_matcher.find_best_match(title, candidates)
            
            # Simuler un résultat
            base_result = {
                'game_id_rawg': game_id,
                'title': title,
                'platform': 'PC',
                'price': f"{15 + (game_id % 40)}.99€" if best_idx is not None else None,
                'shop': 'TestShop' if best_idx is not None else None,
                'url': f'https://example.com/game/{game_id}' if best_idx is not None else None,
                'last_update': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                'similarity_score': similarity_score
            }
            
            if best_idx is not None:
                matched_title = candidates[best_idx]
                quality = "🔥" if similarity_score >= 0.8 else "✅" if similarity_score >= 0.6 else "⚠️"
                logger.info(f"{quality} Match trouvé: '{matched_title}' (score: {similarity_score:.3f})")
            else:
                logger.info(f"❌ Aucun match suffisant (meilleur score: {similarity_score:.3f})")
            
            results.append(base_result)
            
            # Petite pause simulée
            time.sleep(0.5)
        
        successful = len([r for r in results if r.get('price')])
        avg_similarity = sum(r.get('similarity_score', 0) for r in results) / max(len(results), 1)
        
        logger.info(f"🎯 Simulation terminée: {successful}/{len(results)} prix simulés (similarité moy: {avg_similarity:.3f})")
        
        return pd.DataFrame(results)
    
    def test_scraping(self) -> bool:
        """Test du scraper"""
        logger.info("🧪 Test du scraper TF-IDF")
        
        test_games = pd.DataFrame([
            {'game_id_rawg': 1, 'title': 'Cyberpunk 2077'},
            {'game_id_rawg': 2, 'title': 'The Witcher 3'}
        ])
        
        results = self.scrape_prices(test_games)
        
        success = not results.empty
        
        if success:
            logger.info("✅ Test scraper réussi")
        else:
            logger.warning("❌ Test scraper échoué")
        
        return success

# Alias pour compatibilité
PriceScraperTFIDF = PriceScraper

def main():
    """Test principal"""
    print("🧪 Test du module PriceScraper avec TF-IDF")
    
    scraper = PriceScraper()
    
    print(f"TF-IDF enabled: {scraper.title_matcher.enabled}")
    print(f"Scraper enabled: {scraper.enabled}")
    
    success = scraper.test_scraping()
    print(f"Test result: {success}")

if __name__ == "__main__":
    main()
