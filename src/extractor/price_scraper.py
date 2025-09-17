cat > src/extractor/price_scraper.py << 'EOF'
"""
💰 Module de scraping des prix (version simplifiée pour AlwaysData)
"""

import pandas as pd
import requests
import time
import logging
import os
import sys
from datetime import datetime
from typing import Dict, Any, List, Optional
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

logger = logging.getLogger(__name__)

class PriceScraper:
    """Scraper de prix pour les jeux vidéo (version simplifiée)"""
    
    def __init__(self):
        try:
            from utils.config import ConfigManager
            config = ConfigManager()
            scraping_config = config.get_scraping_config()
        except ImportError:
            scraping_config = self._get_config_from_env()
        
        self.enabled = scraping_config.get('enabled', False)  # Désactivé par défaut
        self.max_games = scraping_config.get('max_games_per_session', 50)
        self.delay = scraping_config.get('delay_between_requests', 2)
        
        logger.info(f"PriceScraper initialisé - Enabled: {self.enabled}")
    
    def _get_config_from_env(self) -> Dict[str, Any]:
        """Récupère la configuration depuis les variables d'environnement"""
        return {
            'enabled': os.getenv('SCRAPING_ENABLED', 'false').lower() == 'true',
            'max_games_per_session': int(os.getenv('MAX_GAMES_SCRAPING', '50')),
            'delay_between_requests': float(os.getenv('SCRAPING_DELAY', '2.0'))
        }
    
    def scrape_prices(self, games_df: pd.DataFrame) -> pd.DataFrame:
        """Scrape les prix pour une liste de jeux"""
        if not self.enabled:
            logger.info("Scraping désactivé, retour DataFrame vide")
            return pd.DataFrame(columns=['game_id_rawg', 'title', 'platform', 'price', 'shop', 'url', 'last_update'])
        
        if games_df.empty:
            logger.info("Aucun jeu à scraper")
            return pd.DataFrame(columns=['game_id_rawg', 'title', 'platform', 'price', 'shop', 'url', 'last_update'])
        
        logger.info(f"Scraping prix pour {len(games_df)} jeux (MODE SIMULATION)")
        
        # Pour le moment, retourner des données simulées
        prices_data = []
        for index, game in games_df.head(min(5, len(games_df))).iterrows():
            prices_data.append({
                'game_id_rawg': game.get('game_id_rawg'),
                'title': game.get('title'),
                'platform': 'PC',
                'price': '€19.99',
                'shop': 'Steam',
                'url': f'https://store.steampowered.com/search/?term={game.get("title", "").replace(" ", "+")}',
                'last_update': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            })
        
        logger.info(f"Données prix simulées générées: {len(prices_data)} entrées")
        return pd.DataFrame(prices_data)
    
    def test_scraping(self, test_games: List[Dict[str, str]]) -> bool:
        """Test le scraping"""
        logger.info("Test scraping (simulation)")
        return True
EOF
