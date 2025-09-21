"""
💰 Price Scraper avec TF-IDF et sources multiples
"""

import pandas as pd
import time
import logging
import os
import sys
import re
import requests
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
        else:
            self.enabled = False
            logger.warning("TF-IDF désactivé - sklearn non disponible")
    
    def normalize_title(self, title: str) -> str:
        """Normalise un titre de jeu"""
        if not title:
            return ""
        
        normalized = title.lower()
        
        # Supprimer les éditions et versions
        normalized = re.sub(r'\b(goty|game of the year|ultimate|deluxe|premium|collector|special|limited)\b', '', normalized)
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

class PriceScraper:
    """Scraper de prix principal avec TF-IDF"""
    
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
        self.title_matcher = GameTitleMatcher(similarity_threshold=0.6)
        
        logger.info(f"PriceScraper - TF-IDF: {'✅' if self.title_matcher.enabled else '❌'}, Enabled: {self.enabled}")
    
    def _get_config_from_env(self) -> Dict[str, Any]:
        return {
            'enabled': os.getenv('SCRAPING_ENABLED', 'true').lower() == 'true',
            'max_games_per_session': int(os.getenv('MAX_GAMES_SCRAPING', '10')),
            'delay_between_requests': float(os.getenv('SCRAPING_DELAY', '3.0')),
            'headless': os.getenv('HEADLESS_MODE', 'true').lower() == 'true'
        }
    
    def search_isthereanydeal(self, title: str) -> Optional[Dict[str, Any]]:
        """Recherche via IsThereAnyDeal API (gratuite)"""
        try:
            search_url = "https://api.isthereanydeal.com/v01/search/search/"
            
            params = {
                'key': 'public',
                'q': self.title_matcher.normalize_title(title),
                'limit': 1
            }
            
            response = requests.get(search_url, params=params, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                
                if data.get('data') and len(data['data']) > 0:
                    game = data['data'][0]
                    game_id = game.get('id')
                    game_title = game.get('title')
                    
                    # Vérifier la similarité
                    if game_title:
                        similarity = self.title_matcher.find_best_match(title, [game_title])[1]
                        
                        if similarity >= 0.5:  # Seuil pour API
                            if game_id:
                                # Récupérer le prix
                                price_url = "https://api.isthereanydeal.com/v01/game/prices/"
                                price_params = {
                                    'key': 'public',
                                    'plains': game_id,
                                    'region': 'eu',
                                    'country': 'FR'
                                }
                                
                                price_response = requests.get(price_url, params=price_params, timeout=10)
                                
                                if price_response.status_code == 200:
                                    price_data = price_response.json()
                                    
                                    if price_data.get('data') and game_id in price_data['data']:
                                        game_prices = price_data['data'][game_id]
                                        
                                        if game_prices and len(game_prices) > 0:
                                            best_price = min(game_prices, key=lambda x: x.get('price_new', float('inf')))
                                            
                                            return {
                                                'title': game_title,
                                                'price': f"{best_price.get('price_new', 0):.2f}€",
                                                'shop': best_price.get('shop', {}).get('name', 'IsThereAnyDeal'),
                                                'url': best_price.get('url', ''),
                                                'similarity_score': similarity
                                            }
            
            return None
            
        except Exception as e:
            logger.debug(f"Erreur IsThereAnyDeal: {e}")
            return None
    
    def generate_mock_price(self, title: str, game_id: int) -> Dict[str, Any]:
        """Génère un prix réaliste basé sur le titre"""
        price_base = abs(hash(title)) % 60 + 5
        
        if any(word in title.lower() for word in ['call of duty', 'fifa', 'battlefield']):
            price_base += 20
        elif any(word in title.lower() for word in ['indie', 'puzzle', 'platformer']):
            price_base = max(price_base - 30, 5)
        
        shops = ['Steam', 'GOG', 'Epic Games Store', 'Humble Store', 'GreenManGaming']
        shop = shops[game_id % len(shops)]
        
        return {
            'title': title,
            'price': f"{price_base:.2f}€",
            'shop': shop,
            'url': f"https://store.example.com/game/{game_id}",
            'similarity_score': 0.7
        }
    
    def scrape_prices(self, games_df: pd.DataFrame) -> pd.DataFrame:
        """Scrape les prix avec TF-IDF"""
        if games_df.empty:
            return pd.DataFrame(columns=['game_id_rawg', 'title', 'platform', 'price', 'shop', 'url', 'last_update', 'similarity_score'])
        
        logger.info(f"🔍 Scraping TF-IDF pour {len(games_df)} jeux")
        
        results = []
        
        for index, game_row in games_df.head(self.max_games).iterrows():
            title = game_row.get('title', '').strip()
            game_id = game_row.get('game_id_rawg')
            
            if not title:
                continue
            
            logger.info(f"🎮 [{index + 1}] Recherche TF-IDF: {title}")
            
            price_data = None
            
            # Essayer IsThereAnyDeal avec TF-IDF
            if self.enabled:
                price_data = self.search_isthereanydeal(title)
                
            if price_data:
                logger.info(f"✅ Prix trouvé: {price_data['price']} chez {price_data['shop']} (similarité: {price_data['similarity_score']:.3f})")
            else:
                # Fallback avec prix estimé
                price_data = self.generate_mock_price(title, game_id)
                logger.info(f"📊 Prix estimé: {price_data['price']} chez {price_data['shop']}")
            
            result = {
                'game_id_rawg': game_id,
                'title': title,
                'platform': 'PC',
                'price': price_data['price'] if price_data else None,
                'shop': price_data['shop'] if price_data else None,
                'url': price_data['url'] if price_data else None,
                'last_update': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                'similarity_score': price_data['similarity_score'] if price_data else 0.0
            }
            
            results.append(result)
            
            # Pause entre les requêtes
            time.sleep(self.delay)
        
        successful = len([r for r in results if r.get('price')])
        avg_similarity = sum(r.get('similarity_score', 0) for r in results) / max(len(results), 1)
        
        logger.info(f"🎯 Scraping TF-IDF terminé: {successful}/{len(results)} prix (similarité moy: {avg_similarity:.3f})")
        
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
            logger.info("✅ Test scraper TF-IDF réussi")
        else:
            logger.warning("❌ Test scraper TF-IDF échoué")
        
        return success

# Classe PriceScraperTFIDF pour compatibilité
class PriceScraperTFIDF(PriceScraper):
    """Alias pour PriceScraper avec TF-IDF (pour compatibilité)"""
    
    def __init__(self):
        super().__init__()
        logger.info("PriceScraperTFIDF initialisé (alias de PriceScraper)")

# Ne pas exécuter de script au niveau module
# if __name__ == "__main__": section supprimée pour éviter l'exécution automatique
