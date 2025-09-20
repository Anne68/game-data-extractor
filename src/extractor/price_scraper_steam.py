
"""
💰 Price Scraper alternatif pour Steam Store
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

class SteamPriceScraper:
    """Scraper pour Steam Store via API"""
    
    def __init__(self):
        self.enabled = True
        self.max_games = 5
        self.delay = 2
        self.base_url = "https://store.steampowered.com/api"
        
    def normalize_title(self, title: str) -> str:
        """Normalise un titre pour la recherche Steam"""
        if not title:
            return ""
        
        normalized = title.lower()
        # Supprimer éditions
        normalized = re.sub(r'\b(goty|ultimate|deluxe|premium|collector)\b', '', normalized)
        normalized = re.sub(r'\b(edition|remaster|hd)\b', '', normalized)
        # Nettoyer
        normalized = re.sub(r'[^\w\s]', ' ', normalized)
        normalized = re.sub(r'\s+', ' ', normalized).strip()
        
        return normalized
    
    def search_steam_game(self, title: str) -> Optional[Dict[str, Any]]:
        """Recherche un jeu sur Steam"""
        try:
            # Utiliser l'API de recherche Steam (non officielle mais fonctionnelle)
            search_url = "https://steamcommunity.com/actions/SearchApps/"
            
            normalized_title = self.normalize_title(title)
            
            response = requests.get(search_url, params={
                'text': normalized_title,
                'max_results': 5
            }, timeout=10)
            
            if response.status_code == 200:
                results = response.json()
                
                if results and len(results) > 0:
                    # Prendre le premier résultat
                    first_result = results[0]
                    app_id = first_result.get('appid')
                    game_name = first_result.get('name')
                    
                    if app_id:
                        # Récupérer les détails du jeu
                        details_url = f"https://store.steampowered.com/api/appdetails"
                        details_response = requests.get(details_url, params={
                            'appids': app_id,
                            'cc': 'fr',  # France
                            'l': 'french'
                        }, timeout=10)
                        
                        if details_response.status_code == 200:
                            details_data = details_response.json()
                            app_data = details_data.get(str(app_id), {})
                            
                            if app_data.get('success') and app_data.get('data'):
                                game_data = app_data['data']
                                price_overview = game_data.get('price_overview')
                                
                                if price_overview:
                                    price = price_overview.get('final_formatted', 'N/A')
                                    original_price = price_overview.get('initial_formatted')
                                    discount = price_overview.get('discount_percent', 0)
                                    
                                    return {
                                        'title': game_data.get('name', game_name),
                                        'price': price,
                                        'original_price': original_price,
                                        'discount': discount,
                                        'url': f"https://store.steampowered.com/app/{app_id}",
                                        'shop': 'Steam',
                                        'similarity_score': 0.8  # Score fixe pour Steam
                                    }
                                else:
                                    # Jeu gratuit ou pas de prix
                                    is_free = game_data.get('is_free', False)
                                    return {
                                        'title': game_data.get('name', game_name),
                                        'price': 'Gratuit' if is_free else 'N/A',
                                        'original_price': None,
                                        'discount': 0,
                                        'url': f"https://store.steampowered.com/app/{app_id}",
                                        'shop': 'Steam',
                                        'similarity_score': 0.8
                                    }
            
            return None
            
        except Exception as e:
            logger.error(f"Erreur recherche Steam: {e}")
            return None
    
    def scrape_prices(self, games_df: pd.DataFrame) -> pd.DataFrame:
        """Scrape les prix Steam"""
        if games_df.empty:
            return pd.DataFrame(columns=['game_id_rawg', 'title', 'platform', 'price', 'shop', 'url', 'last_update', 'similarity_score'])
        
        logger.info(f"🔍 Scraping Steam pour {len(games_df)} jeux")
        
        results = []
        
        for index, game_row in games_df.head(self.max_games).iterrows():
            title = game_row.get('title', '').strip()
            game_id = game_row.get('game_id_rawg')
            
            if not title:
                continue
            
            logger.info(f"🎮 [{index + 1}] Steam: {title}")
            
            try:
                steam_data = self.search_steam_game(title)
                
                base_result = {
                    'game_id_rawg': game_id,
                    'title': title,
                    'platform': 'PC',
                    'price': None,
                    'shop': None,
                    'url': None,
                    'last_update': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    'similarity_score': 0.0
                }
                
                if steam_data:
                    base_result.update({
                        'price': steam_data['price'],
                        'shop': steam_data['shop'],
                        'url': steam_data['url'],
                        'similarity_score': steam_data['similarity_score']
                    })
                    
                    logger.info(f"✅ Prix Steam: {steam_data['price']}")
                else:
                    logger.info(f"❌ Pas trouvé sur Steam")
                
                results.append(base_result)
                
            except Exception as e:
                logger.error(f"❌ Erreur {title}: {e}")
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
            
            # Pause entre les requêtes
            time.sleep(self.delay)
        
        successful = len([r for r in results if r.get('price')])
        logger.info(f"🎯 Steam terminé: {successful}/{len(results)} prix trouvés")
        
        return pd.DataFrame(results)

def main():
    scraper = SteamPriceScraper()
    
    import pandas as pd
    test_games = pd.DataFrame([
        {'game_id_rawg': 3498, 'title': 'Grand Theft Auto V'},
        {'game_id_rawg': 4200, 'title': 'Portal 2'},
        {'game_id_rawg': 28, 'title': 'Red Dead Redemption 2'}
    ])
    
    results = scraper.scrape_prices(test_games)
    
    print("📊 Résultats Steam:")
    for _, row in results.iterrows():
        print(f"  {row['title']}: {row['price']} ({row['shop']})")

if __name__ == "__main__":
    main()
