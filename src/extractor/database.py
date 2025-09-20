"""
Module de gestion de la base de données MySQL - Version corrigée
"""

import mysql.connector
from mysql.connector import Error
import pandas as pd
import numpy as np
import logging
import os
from typing import Dict, Any

logger = logging.getLogger(__name__)

class DatabaseManager:
    def __init__(self):
        self.db_config = {
            "host": os.getenv("DB_HOST", "mysql-anne.alwaysdata.net"),
            "port": int(os.getenv("DB_PORT", "3306")),
            "user": os.getenv("DB_USER", "anne"),
            "password": os.getenv("DB_PASSWORD", "Vicky2@18"),
            "database": os.getenv("DB_NAME", "anne_games_db"),
            "charset": "utf8mb4"
        }
    
    def get_connection(self):
        try:
            return mysql.connector.connect(**self.db_config)
        except Error as e:
            logger.error(f"Erreur connexion MySQL: {e}")
            return None
    
    def test_connection(self):
        conn = self.get_connection()
        if conn and conn.is_connected():
            conn.close()
            return True
        return False
    
    def setup_tables(self):
        return True
    
    def save_games(self, games_df):
        if games_df.empty:
            return True
        
        conn = self.get_connection()
        if not conn:
            return False
        
        try:
            cursor = conn.cursor()
            games_df = games_df.replace({np.nan: None})
            
            insert_query = """
                INSERT INTO games (game_id_rawg, title, release_date, genres, platforms, 
                                 rating, metacritic, last_update)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    title = VALUES(title),
                    last_update = VALUES(last_update)
            """
            
            data = []
            for _, row in games_df.iterrows():
                data.append([
                    row.get('game_id_rawg'), row.get('title'), row.get('release_date'),
                    row.get('genres'), row.get('platforms'), row.get('rating'),
                    row.get('metacritic'), row.get('last_update')
                ])
            
            cursor.executemany(insert_query, data)
            conn.commit()
            logger.info(f"✅ {len(games_df)} jeux sauvegardés")
            return True
            
        except Error as e:
            logger.error(f"Erreur sauvegarde: {e}")
            return False
        finally:
            if conn.is_connected():
                cursor.close()
                conn.close()
    
    def get_games_for_price_update(self, limit=50):
        conn = self.get_connection()
        if not conn:
            return pd.DataFrame()
        
        try:
            query = """
                SELECT DISTINCT g.game_id_rawg, g.title
                FROM games g
                LEFT JOIN best_price_pc p ON g.game_id_rawg = p.game_id_rawg
                WHERE p.game_id_rawg IS NULL
                ORDER BY g.rating DESC
                LIMIT %s
            """
            return pd.read_sql(query, conn, params=[limit])
        except Error as e:
            logger.error(f"Erreur récupération: {e}")
            return pd.DataFrame()
        finally:
            if conn.is_connected():
                conn.close()
    
    def save_prices(self, prices_df):
        if prices_df.empty:
            return True
        
        conn = self.get_connection()
        if not conn:
            return False
        
        try:
            cursor = conn.cursor()
            prices_df = prices_df.replace({np.nan: None})
            
            insert_query = """
                INSERT INTO best_price_pc (title, best_price_PC, best_shop_PC, site_url_PC, last_update, game_id_rawg)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    best_price_PC = VALUES(best_price_PC),
                    best_shop_PC = VALUES(best_shop_PC),
                    last_update = VALUES(last_update)
            """
            
            data = []
            for _, row in prices_df.iterrows():
                data.append([
                    row.get('title'), row.get('price'), row.get('shop'),
                    row.get('url'), row.get('last_update'), row.get('game_id_rawg')
                ])
            
            cursor.executemany(insert_query, data)
            conn.commit()
            logger.info(f"✅ {len(prices_df)} prix sauvegardés")
            return True
            
        except Error as e:
            logger.error(f"Erreur sauvegarde prix: {e}")
            return False
        finally:
            if conn.is_connected():
                cursor.close()
                conn.close()
    
    def get_stats(self):
        conn = self.get_connection()
        if not conn:
            return {}
        
        try:
            cursor = conn.cursor()
            stats = {}
            
            cursor.execute("SELECT COUNT(*) FROM games")
            stats['total_games'] = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM best_price_pc")
            stats['total_prices'] = cursor.fetchone()[0]
            
            cursor.execute("SELECT MAX(last_update) FROM games")
            last_update = cursor.fetchone()[0]
            stats['last_extraction'] = last_update.strftime('%Y-%m-%d %H:%M:%S') if last_update else None
            
            return stats
        except Error as e:
            return {}
        finally:
            if conn.is_connected():
                cursor.close()
                conn.close()
    
    def get_detailed_stats(self):
        stats = self.get_stats()
        conn = self.get_connection()
        if not conn:
            return stats
        
        try:
            cursor = conn.cursor()
            
            cursor.execute("SELECT last_page FROM api_state WHERE id = 1")
            result = cursor.fetchone()
            stats['last_api_page'] = result[0] if result else 65
            
            cursor.execute("""
                SELECT COUNT(DISTINCT g.game_id_rawg)
                FROM games g
                LEFT JOIN best_price_pc p ON g.game_id_rawg = p.game_id_rawg
                WHERE p.game_id_rawg IS NULL
            """)
            stats['games_without_prices'] = cursor.fetchone()[0]
            
            stats['db_size_mb'] = 0
            stats['log_files'] = 0
            
            return stats
        except Error as e:
            return stats
        finally:
            if conn.is_connected():
                cursor.close()
                conn.close()
    
    def optimize_database(self):
        return True
