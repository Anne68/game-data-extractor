cat > src/extractor/rawg_extractor.py << 'EOF'
"""
🎮 Module d'extraction RAWG.io
"""

import requests
import pandas as pd
import time
import os
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

class RawgExtractor:
    def __init__(self, api_key=None):
        self.api_key = api_key or os.getenv('RAWG_API_KEY', 'a596903618f14aeeb1fcbbb790180dd5')
        self.base_url = "https://api.rawg.io/api"
        self.page_size = 40
        self.rate_limit_delay = 1
    
    def fetch_games(self, limit=500, start_page=1):
        logger.info(f"Extraction {limit} jeux depuis page {start_page}")
        
        games = []
        page = start_page
        
        while len(games) < limit and page <= 10:  # Limite sécurité
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
                
                if not data.get('results'):
                    break
                
                for game in data['results']:
                    if len(games) >= limit:
                        break
                    
                    parsed = self._parse_game(game)
                    if parsed:
                        games.append(parsed)
                
                logger.info(f"Page {page}: {len(games)} jeux")
                page += 1
                time.sleep(self.rate_limit_delay)
                
            except Exception as e:
                logger.error(f"Erreur page {page}: {e}")
                break
        
        logger.info(f"Total: {len(games)} jeux")
        
        if games:
            return pd.DataFrame(games)
        else:
            return pd.DataFrame(columns=['game_id_rawg', 'title', 'release_date', 'genres', 'platforms', 'rating', 'metacritic', 'background_image', 'last_update'])
    
    def _parse_game(self, game_data):
        try:
            return {
                'game_id_rawg': game_data.get('id'),
                'title': game_data.get('name', '').strip(),
                'release_date': game_data.get('released'),
                'genres': ', '.join([g.get('name', '') for g in game_data.get('genres', [])]),
                'platforms': ', '.join([p.get('platform', {}).get('name', '') for p in game_data.get('platforms', [])]),
                'rating': game_data.get('rating'),
                'metacritic': game_data.get('metacritic'),
                'backgroun
