"""
🎮 Module d'extraction des données depuis l'API RAWG.io
"""

import requests
import pandas as pd
import time
import os
import sys
from datetime import datetime
from typing import Optional, Dict, Any
from pathlib import Path
import logging

# Add src to Python path
sys.path.insert(0, str(Path(__file__).parent.parent))

logger = logging.getLogger(__name__)

class RawgExtractor:
    """Extracteur de données RAWG.io"""
    
    def __init__(self, api_key: str = None):
        try:
            from utils.config import ConfigManager
            config = ConfigManager()
            api_config = config.get_api_config()
        except ImportError:
            # Fallback to environment variables
            api_config = self._get_config_from_env()
        
        self.api_key = api_key or api_config.get('rawg_api_key') or os.getenv('RAWG_API_KEY')
        self.base_url = "https://api.rawg.io/api"
        self.page_size = api_config.get('page_size', 40)
        self.rate_limit_delay = api_config.get('rate_limit_delay', 1)
        
        if not self.api_key:
            raise ValueError("Clé API RAWG requise")
    
    def _get_config_from_env(self) -> Dict[str, Any]:
        """Récupère la configuration depuis les variables d'environnement"""
        return {
            'rawg_api_key': os.getenv('RAWG_API_KEY', ''),
            'page_size': int(os.getenv('PAGE_SIZE', '40')),
            'rate_limit_delay': float(os.getenv('RATE_LIMIT_DELAY', '1.0'))
        }
    
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
                
                logger.info(f"Requête page {page}...")
                response = requests.get(url, params=params, timeout=30)
                response.raise_for_status()
                
                data = response.json()
                
                if 'results' not in data or not data['results']:
                    logger.warning(f"Aucun résultat page {page}")
                    break
                
                for game in data['results']:
                    if len(games) >= limit:
                        break
                    
                    parsed_game = self._parse_game(game)
                    if parsed_game:
                        games.append(parsed_game)
                
                logger.info(f"Page {page} traitée, total: {len(games)} jeux")
                page += 1
                
                # Respect du rate limiting
                time.sleep(self.rate_limit_delay)
                
                # Éviter les boucles infinies
                if page > 100:  # Limite de sécurité
                    logger.warning("Limite de pages atteinte")
                    break
                
            except requests.exceptions.RequestException as e:
                logger.error(f"Erreur requête page {page}: {e}")
                break
            except Exception as e:
                logger.error(f"Erreur inattendue page {page}: {e}")
                break
        
        logger.info(f"Extraction terminée: {len(games)} jeux récupérés")
        
        if games:
            return pd.DataFrame(games)
        else:
            return pd.DataFrame(columns=['game_id_rawg', 'title', 'release_date', 'genres', 'platforms', 'rating', 'metacritic', 'background_image', 'last_update'])
    
    def _parse_game(self, game_data: Dict[Any, Any]) -> Optional[Dict[str, Any]]:
        """Parse les données d'un jeu depuis l'API"""
        try:
            return {
                'game_id_rawg': game_data.get('id'),
                'title': game_data.get('name', '').strip(),
                'release_date': game_data.get('released'),
                'genres': ', '.join([g.get('name', '') for g in game_data.get('genres', []) if g.get('name')]),
                'platforms': ', '.join([p.get('platform', {}).get('name', '') for p in game_data.get('platforms', []) if p.get('platform', {}).get('name')]),
                'rating': game_data.get('rating'),
                'metacritic': game_data.get('metacritic'),
                'background_image': game_data.get('background_image'),
                'last_update': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
        except Exception as e:
            logger.error(f"Erreur parsing jeu {game_data.get('id', 'unknown')}: {e}")
            return None
    
    def test_api_connection(self) -> bool:
        """Test la connexion à l'API RAWG"""
        try:
            url = f"{self.base_url}/games"
            params = {
                'key': self.api_key,
                'page_size': 1,
                'page': 1
            }
            
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            return 'results' in data and len(data['results']) > 0
            
        except Exception as e:
            logger.error(f"Test API RAWG échoué: {e}")
            return False
