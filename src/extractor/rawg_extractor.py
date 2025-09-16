"""
🎮 Module d'extraction des données depuis l'API RAWG.io
"""

import requests
import pandas as pd
import time
from datetime import datetime
from typing import Optional, Dict, Any
import logging

logger = logging.getLogger(__name__)

class RawgExtractor:
    """Extracteur de données RAWG.io"""
    
    def __init__(self, api_key: str = None):
        from ..utils.config import ConfigManager
        
        config = ConfigManager()
        api_config = config.get_api_config()
        
        self.api_key = api_key or api_config.get('rawg_api_key')
        self.base_url = "https://api.rawg.io/api"
        self.page_size = api_config.get('page_size', 40)
        self.rate_limit_delay = api_config.get('rate_limit_delay', 1)
        
        if not self.api_key:
            raise ValueError("Clé API RAWG requise")
    
    def fetch_games(self, limit: int = 500, start_page: int = 1) -> pd.DataFrame:
        """Récupère les jeux depuis l'API RAWG"""
        logger.info(f"Début extraction {limit} jeux depuis la page {start_page}")
        
        games = []
        page = start_page
        
        while len(games) < limit:
            try:
                url = f"{self.base_url}/games"
                params = {
                    'key': self.api_key,
                    'page_size': self.page_size,
                    'page': page
                }
                
                response = requests.get(url, params=params, timeout=30)
                response.raise_for_status()
                
                data = response.json()
                
                if 'results' not in data or not data['results']:
                    logger.warning(f"Aucun résultat page {page}")
                    break
                
                for game in data['results']:
                    if len(games) >= limit:
                        break
                    
                    games.append(self._parse_game(game))
                
                logger.info(f"Page {page} traitée, total: {len(games)} jeux")
                page += 1
                
                # Respect du rate limiting
                time.sleep(self.rate_limit_delay)
                
            except requests.exceptions.RequestException as e:
                logger.error(f"Erreur requête page {page}: {e}")
                break
            except Exception as e:
                logger.error(f"Erreur inattendue page {page}: {e}")
                break
        
        logger.info(f"Extraction terminée: {len(games)} jeux récupérés")
        return pd.DataFrame(games)
    
    def _parse_game(self, game_data: Dict[Any, Any]) -> Dict[str, Any]:
        """Parse les données d'un jeu depuis l'API"""
        return {
            'game_id_rawg': game_data.get('id'),
            'title': game_data.get('name'),
            'release_date': game_data.get('released'),
            'genres': ', '.join([g['name'] for g in game_data.get('genres', [])]),
            'platforms': ', '.join([p['platform']['name'] for p in game_data.get('platforms', [])]),
            'rating': game_data.get('rating'),
            'metacritic': game_data.get('metacritic'),
            'background_image': game_data.get('background_image'),
            'last_update': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
