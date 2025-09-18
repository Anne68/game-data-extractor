"""
💰 Module de scraping réel des prix depuis DLCompare
"""

import pandas as pd
import requests
import time
import logging
import os
import sys
import re
from datetime import datetime
from typing import Dict, Any, List, Optional
from pathlib import Path
from bs4 import BeautifulSoup
import urllib.parse

sys.path.insert(0, str(Path(__file__).parent.parent))
logger = logging.getLogger(__name__)

class PriceScraper:
    """Scraper de prix réel depuis DLCompare pour récupérer uniquement le meilleur prix"""
    
    def __init__(self):
        try:
            from utils.config import ConfigManager
            config = ConfigManager()
            scraping_config = config.get_scraping_config()
        except ImportError:
            scraping_config = self._get_config_from_env()
        
        self.enabled = scraping_config.get('enabled', True)
        self.max_games = scraping_config.get('max_games_per_session', 20)  # Réduit pour éviter la surcharge
        self.delay = scraping_config.get('delay_between_requests', 5)  # Augmenté pour éviter les blocages
        
        # Headers réalistes pour éviter la détection
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'fr-FR,fr;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Cache-Control': 'max-age=0'
        }
        
        self.session = requests.Session()
        self.session.headers.update(self.headers)
        
        logger.info(f"PriceScraper réel initialisé - Enabled: {self.enabled}")
    
    def _get_config_from_env(self) -> Dict[str, Any]:
        return {
            'enabled': os.getenv('SCRAPING_ENABLED', 'true').lower() == 'true',
            'max_games_per_session': int(os.getenv('MAX_GAMES_SCRAPING', '20')),
            'delay_between_requests': float(os.getenv('SCRAPING_DELAY', '5.0'))
        }
    
    def scrape_prices(self, games_df: pd.DataFrame) -> pd.DataFrame:
        """Scrape les prix réels depuis DLCompare et ne retourne que le meilleur prix par jeu"""
        if not self.enabled:
            logger.info("Scraping désactivé")
            return pd.DataFrame(columns=['game_id_rawg', 'title', 'platform', 'price', 'shop', 'url', 'last_update'])
        
        if games_df.empty:
            logger.info("Aucun jeu à scraper")
            return pd.DataFrame(columns=['game_id_rawg', 'title', 'platform', 'price', 'shop', 'url', 'last_update'])
        
        logger.info(f"🔍 Scraping RÉEL des prix pour {len(games_df)} jeux depuis DLCompare")
        
        prices_data = []
        
        for index, game in games_df.head(min(self.max_games, len(games_df))).iterrows():
            game_title = game.get('title', '').strip()
            game_id = game.get('game_id_rawg')
            
            if not game_title:
                continue
                
            logger.info(f"🎮 Recherche prix pour: {game_title}")
            
            try:
                best_price_info = self._get_best_price_from_dlcompare(game_title)
                
                if best_price_info:
                    prices_data.append({
                        'game_id_rawg': game_id,
                        'title': game_title,
                        'platform': 'PC',
                        'price': best_price_info['price'],
                        'shop': best_price_info['shop'],
                        'url': best_price_info['url'],
                        'last_update': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    })
                    logger.info(f"✅ Prix trouvé: {best_price_info['price']} chez {best_price_info['shop']}")
                else:
                    logger.warning(f"❌ Aucun prix trouvé pour {game_title}")
                    
            except Exception as e:
                logger.error(f"❌ Erreur scraping pour {game_title}: {e}")
            
            # Délai entre chaque jeu pour éviter les blocages
            time.sleep(self.delay)
        
        logger.info(f"🎯 Scraping terminé: {len(prices_data)} prix récupérés")
        return pd.DataFrame(prices_data)
    
    def _get_best_price_from_dlcompare(self, game_title: str) -> Optional[Dict[str, str]]:
        """Récupère le meilleur prix depuis DLCompare"""
        try:
            # Préparer la recherche
            search_term = self._clean_game_title_for_search(game_title)
            search_url = f"https://www.dlcompare.fr/jeux/search?q={urllib.parse.quote(search_term)}"
            
            logger.debug(f"🔗 URL de recherche: {search_url}")
            
            # Première requête : page de recherche
            response = self.session.get(search_url, timeout=15)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Chercher le premier résultat de jeu
            game_links = soup.find_all('a', href=re.compile(r'/jeux/\d+/'))
            
            if not game_links:
                logger.debug(f"❌ Aucun lien de jeu trouvé pour {game_title}")
                return None
            
            # Prendre le premier résultat
            first_game_link = game_links[0]
            game_url = "https://www.dlcompare.fr" + first_game_link.get('href')
            
            logger.debug(f"🎮 Page jeu trouvée: {game_url}")
            
            # Délai avant la page détaillée
            time.sleep(2)
            
            # Deuxième requête : page détaillée du jeu
            game_response = self.session.get(game_url, timeout=15)
            game_response.raise_for_status()
            
            game_soup = BeautifulSoup(game_response.content, 'html.parser')
            
            # Extraire le meilleur prix (méthode basée sur la structure DLCompare)
            best_price = self._extract_best_price_from_game_page(game_soup, game_url)
            
            return best_price
            
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Erreur réseau lors du scraping: {e}")
            return None
        except Exception as e:
            logger.error(f"❌ Erreur inattendue lors du scraping: {e}")
            return None
    
    def _clean_game_title_for_search(self, title: str) -> str:
        """Nettoie le titre du jeu pour optimiser la recherche"""
        # Supprimer les caractères spéciaux et les éditions
        title = re.sub(r'\(.*?\)', '', title)  # Supprimer ce qui est entre parenthèses
        title = re.sub(r'[™®©]', '', title)    # Supprimer les symboles de marque
        title = re.sub(r'\s+', ' ', title)     # Normaliser les espaces
        title = title.strip()
        
        # Supprimer les éditions courantes
        editions_to_remove = ['Game of the Year', 'GOTY', 'Deluxe', 'Premium', 'Gold', 'Ultimate', 'Enhanced', 'Remastered']
        for edition in editions_to_remove:
            title = re.sub(rf'\b{edition}\b', '', title, flags=re.IGNORECASE)
        
        return title.strip()
    
    def _extract_best_price_from_game_page(self, soup: BeautifulSoup, game_url: str) -> Optional[Dict[str, str]]:
        """Extrait le meilleur prix depuis la page détaillée du jeu"""
        try:
            # Chercher les offres de prix (structure basée sur l'image DLCompare)
            price_containers = soup.find_all(['div', 'span'], class_=re.compile(r'price|offer|deal'))
            
            if not price_containers:
                # Fallback: chercher tous les éléments contenant €
                price_containers = soup.find_all(text=re.compile(r'\d+[,.]?\d*\s*€'))
                
            best_price = None
            best_shop = None
            best_price_value = float('inf')
            
            for container in price_containers:
                # Extraire le prix
                if hasattr(container, 'text'):
                    price_text = container.text
                else:
                    price_text = str(container)
                
                price_match = re.search(r'(\d+[,.]?\d*)\s*€', price_text)
                if price_match:
                    price_str = price_match.group(1).replace(',', '.')
                    try:
                        price_value = float(price_str)
                        
                        if price_value < best_price_value and price_value > 0:
                            best_price_value = price_value
                            best_price = f"{price_str}€"
                            
                            # Chercher le nom de la boutique dans le contexte
                            shop_element = container.find_parent() if hasattr(container, 'find_parent') else None
                            if shop_element:
                                shop_text = shop_element.get_text()
                                # Extraire les noms de boutiques courantes
                                shops = ['Steam', 'Epic Games Store', 'GOG', 'Origin', 'Uplay', 'Microsoft Store', 'PlayStation Store', 'Nintendo eShop', 'Kinguin', 'G2A', 'CDKeys']
                                for shop in shops:
                                    if shop.lower() in shop_text.lower():
                                        best_shop = shop
                                        break
                            
                            if not best_shop:
                                best_shop = "DLCompare"  # Fallback
                                
                    except ValueError:
                        continue
            
            if best_price and best_price_value != float('inf'):
                return {
                    'price': best_price,
                    'shop': best_shop or "DLCompare",
                    'url': game_url
                }
            
            return None
            
        except Exception as e:
            logger.error(f"❌ Erreur extraction prix: {e}")
            return None
    
    def test_scraping(self, test_games: List[Dict[str, str]]) -> bool:
        """Test le scraping avec un jeu réel"""
        logger.info("🧪 Test de scraping réel")
        
        test_df = pd.DataFrame([{
            'game_id_rawg': 12345,
            'title': 'Cyberpunk 2077'
        }])
        
        results = self.scrape_prices(test_df)
        success = not results.empty and len(results) > 0
        
        if success:
            logger.info("✅ Test de scraping réussi")
        else:
            logger.warning("❌ Test de scraping échoué")
            
        return success
