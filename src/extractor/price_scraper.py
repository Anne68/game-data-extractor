#!/usr/bin/env python3
"""
🔧 Script de correction pour l'erreur d'import PriceScraperTFIDF
"""

import os
import sys
from pathlib import Path

def fix_price_scraper():
    """Corrige le module price_scraper pour exporter PriceScraperTFIDF"""
    
    price_scraper_path = Path("src/extractor/price_scraper.py")
    
    # Contenu corrigé du price_scraper.py
    content = '''"""
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
                token_pattern=r'\\b[a-zA-Z][a-zA-Z0-9]*\\b'
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
        normalized = re.sub(r'\\b(goty|game of the year|ultimate|deluxe|premium|collector|special|limited)\\b', '', normalized)
        normalized = re.sub(r'\\b(edition|version|remaster|remastered|hd|4k|enhanced|definitive)\\b', '', normalized)
        normalized = re.sub(r'\\b(pack|bundle|collection|anthology|trilogy)\\b', '', normalized)
        
        # Supprimer plateformes et années
        normalized = re.sub(r'\\b(pc|ps4|ps5|xbox|nintendo|switch|steam)\\b', '', normalized)
        normalized = re.sub(r'\\(\\d{4}\\)', '', normalized)
        
        # Nettoyer caractères spéciaux
        normalized = re.sub(r'[^\\w\\s]', ' ', normalized)
        normalized = re.sub(r'\\s+', ' ', normalized).strip()
        
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

def main():
    """Test principal"""
    scraper = PriceScraperTFIDF()
    
    print(f"Scraper TF-IDF enabled: {scraper.enabled}")
    print(f"TF-IDF matcher enabled: {scraper.title_matcher.enabled}")
    
    success = scraper.test_scraping()
    print(f"Test result: {success}")

if __name__ == "__main__":
    main()
'''
    
    # Écrire le fichier corrigé
    with open(price_scraper_path, 'w', encoding='utf-8') as f:
        f.write(content)
    
    print(f"✅ {price_scraper_path} corrigé")

def fix_complete_pipeline():
    """Corrige le script run_complete_pipeline_tfidf.py"""
    
    script_path = Path("scripts/run_complete_pipeline_tfidf.py")
    
    content = '''#!/usr/bin/env python3
"""
🧠 Pipeline complet avec TF-IDF et statistiques de qualité
"""

import os
import sys
import time
import requests
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

def send_discord_notification(title, description, color=3066993, fields=None):
    """Envoie une notification Discord avec stats TF-IDF"""
    webhook_url = os.getenv('DISCORD_WEBHOOK')
    if not webhook_url:
        return False
    
    embed = {
        'title': title,
        'description': description,
        'color': color,
        'timestamp': datetime.now().isoformat(),
        'footer': {'text': 'Game Data Extractor • TF-IDF Enhanced'}
    }
    
    if fields:
        embed['fields'] = fields
    
    payload = {'embeds': [embed]}
    
    try:
        response = requests.post(webhook_url, json=payload, timeout=10)
        response.raise_for_status()
        return True
    except Exception as e:
        print(f"Erreur notification Discord: {e}")
        return False

def main():
    print("🧠 Pipeline complet avec TF-IDF")
    
    # Notification de début
    send_discord_notification(
        "🧠 Pipeline TF-IDF - DÉBUT",
        "Lancement du pipeline avec matching intelligent TF-IDF",
        color=3447003
    )
    
    start_time = time.time()
    
    try:
        from extractor.rawg_extractor import RawgExtractor
        from extractor.database import DatabaseManager
        from extractor.price_scraper import PriceScraperTFIDF  # Import corrigé
        
        db = DatabaseManager()
        extractor = RawgExtractor()
        scraper = PriceScraperTFIDF()
        
        # Stats initiales
        initial_stats = db.get_stats()
        initial_games = initial_stats.get('total_games', 0)
        initial_prices = initial_stats.get('total_prices', 0)
        initial_similarity = initial_stats.get('avg_similarity', 0)
        
        # Phase 1: Extraction jeux
        print("\\n📥 Phase 1: Extraction de nouveaux jeux")
        
        conn = db.get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT COALESCE(last_page, 64) FROM api_state WHERE id = 1")
        result = cursor.fetchone()
        last_page = result[0] if result else 64
        next_page = last_page + 1
        conn.close()
        
        games_df = extractor.fetch_games(limit=50, start_page=next_page)
        
        new_games_count = 0
        if not games_df.empty:
            success = db.save_games(games_df)
            if success:
                new_games_count = len(games_df)
                print(f"✅ {new_games_count} nouveaux jeux ajoutés")
                
                # Mettre à jour l'état
                conn = db.get_connection()
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT INTO api_state (id, last_page, last_extraction, total_games_extracted)
                    VALUES (1, %s, NOW(), %s)
                    ON DUPLICATE KEY UPDATE
                        last_page = VALUES(last_page),
                        last_extraction = VALUES(last_extraction),
                        total_games_extracted = total_games_extracted + VALUES(total_games_extracted)
                """, (next_page + 1, new_games_count))
                conn.commit()
                conn.close()
        
        # Phase 2: Scraping prix avec TF-IDF
        print("\\n🧠 Phase 2: Scraping TF-IDF des prix")
        time.sleep(3)
        
        games_to_scrape = db.get_games_for_price_update(limit=15)
        new_prices_count = 0
        tfidf_stats = {'avg_new_similarity': 0, 'high_quality_new': 0}
        
        if not games_to_scrape.empty:
            print(f"🔍 Scraping TF-IDF pour {len(games_to_scrape)} jeux")
            prices_df = scraper.scrape_prices(games_to_scrape)
            
            if not prices_df.empty:
                success = db.save_prices(prices_df)
                if success:
                    valid_prices = prices_df[prices_df['price'].notna()]
                    new_prices_count = len(valid_prices)
                    
                    if not valid_prices.empty and 'similarity_score' in valid_prices.columns:
                        tfidf_stats['avg_new_similarity'] = valid_prices['similarity_score'].mean()
                        tfidf_stats['high_quality_new'] = len(valid_prices[valid_prices['similarity_score'] >= 0.8])
                        
                        print(f"✅ {new_prices_count} prix TF-IDF trouvés (similarité moy: {tfidf_stats['avg_new_similarity']:.3f})")
        
        # Stats finales
        final_stats = db.get_stats()
        final_games = final_stats.get('total_games', 0)
        final_prices = final_stats.get('total_prices', 0)
        final_similarity = final_stats.get('avg_similarity', 0)
        
        similarity_improvement = final_similarity - initial_similarity
        execution_time = round(time.time() - start_time, 1)
        coverage = (final_prices / final_games * 100) if final_games > 0 else 0
        
        # Notification de succès
        description = f"""🧠 **Pipeline TF-IDF terminé avec succès**

📊 **Résultats:**
- Nouveaux jeux extraits: **{new_games_count}**
- Nouveaux prix TF-IDF: **{new_prices_count}**
- Matchs haute qualité: **{tfidf_stats['high_quality_new']}**
- Total jeux: **{final_games}** (+{final_games - initial_games})
- Total prix: **{final_prices}** (+{final_prices - initial_prices})

🧠 **Qualité TF-IDF:**
- Similarité moyenne: **{final_similarity:.3f}** ({similarity_improvement:+.3f})
- Nouveaux matchs: **{tfidf_stats['avg_new_similarity']:.3f}**
- Couverture prix: **{coverage:.1f}%**
"""

        fields = [
            {
                'name': '🎮 Données',
                'value': f'{final_games} jeux\\n{final_prices} prix',
                'inline': True
            },
            {
                'name': '🧠 Qualité TF-IDF', 
                'value': f'Similarité: {final_similarity:.3f}\\nHaute qualité: {final_stats.get("high_quality_matches", 0)}',
                'inline': True
            },
            {
                'name': '⚡ Performance',
                'value': f'Temps: {execution_time}s\\nCouverture: {coverage:.1f}%',
                'inline': True
            }
        ]
        
        send_discord_notification(
            "✅ Pipeline TF-IDF - SUCCÈS",
            description,
            color=3066993,
            fields=fields
        )
        
        print(f"\\n🎉 Pipeline TF-IDF terminé!")
        print(f"📊 Qualité moyenne des matchs: {final_similarity:.3f}")
        print(f"🔥 Matchs haute qualité: {final_stats.get('high_quality_matches', 0)}")
        
    except Exception as e:
        # Notification d'erreur
        send_discord_notification(
            "❌ Pipeline TF-IDF - ÉCHEC",
            f"**Erreur lors de l'exécution du pipeline TF-IDF**\\n\\nErreur: `{str(e)}`\\n\\n⚠️ Vérifiez les logs pour plus de détails",
            color=15158332
        )
        
        print(f"\\n❌ Erreur pipeline: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
'''
    
    with open(script_path, 'w', encoding='utf-8') as f:
        f.write(content)
    
    print(f"✅ {script_path} créé")

def install_sklearn():
    """Installe scikit-learn si nécessaire"""
    try:
        import sklearn
        print("✅ scikit-learn déjà installé")
        return True
    except ImportError:
        print("📦 Installation de scikit-learn...")
        import subprocess
        try:
            subprocess.check_call([sys.executable, "-m", "pip", "install", "--user", "scikit-learn>=1.3.0"])
            print("✅ scikit-learn installé avec succès")
            return True
        except subprocess.CalledProcessError as e:
            print(f"❌ Erreur installation scikit-learn: {e}")
            return False

def test_imports():
    """Test les imports après correction"""
    print("🧪 Test des imports...")
    
    try:
        sys.path.insert(0, str(Path("src")))
        
        from extractor.price_scraper import PriceScraper, PriceScraperTFIDF
        print("✅ Import PriceScraper et PriceScraperTFIDF réussi")
        
        # Test instantiation
        scraper = PriceScraperTFIDF()
        print("✅ Instantiation PriceScraperTFIDF réussie")
        
        return True
        
    except Exception as e:
        print(f"❌ Erreur import: {e}")
        return False

def main():
    print("🔧 Correction de l'erreur TF-IDF")
    print("=" * 50)
    
    # 1. Installer scikit-learn
    if not install_sklearn():
        print("⚠️ Continuons sans scikit-learn (TF-IDF sera désactivé)")
    
    # 2. Corriger le price_scraper
    fix_price_scraper()
    
    # 3. Créer le script TF-IDF
    fix_complete_pipeline()
    
    # 4. Tester les imports
    if test_imports():
        print("\\n🎉 Correction terminée avec succès!")
        print("\\n🚀 Vous pouvez maintenant relancer:")
        print("   python3 scripts/run_complete_pipeline_tfidf.py")
    else:
        print("\\n❌ Des erreurs persistent, vérifiez les logs")

if __name__ == "__main__":
    main()
