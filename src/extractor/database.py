"""
🗄️ Module de gestion de la base de données MySQL
"""

import mysql.connector
from mysql.connector import Error
import pandas as pd
import numpy as np
from typing import Optional, Dict, Any, List
import logging

logger = logging.getLogger(__name__)

class DatabaseManager:
    """Gestionnaire de base de données MySQL"""
    
    def __init__(self):
        from ..utils.config import ConfigManager
        
        config = ConfigManager()
        self.db_config = config.get_database_config()
        
        if not self.db_config:
            raise ValueError("Configuration base de données requise")
    
    def get_connection(self):
        """Établit une connexion à la base de données"""
        try:
            conn = mysql.connector.connect(**self.db_config)
            return conn
        except Error as e:
            logger.error(f"Erreur connexion MySQL: {e}")
            return None
    
    def test_connection(self) -> bool:
        """Test la connexion à la base de données"""
        conn = self.get_connection()
        if conn and conn.is_connected():
            conn.close()
            return True
        return False
    
    def setup_tables(self):
        """Crée les tables nécessaires"""
        conn = self.get_connection()
        if not conn:
            return False
        
        try:
            cursor = conn.cursor()
            
            # Table des jeux
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS games (
                    game_id_rawg INT PRIMARY KEY,
                    title VARCHAR(255) NOT NULL,
                    release_date DATE,
                    genres TEXT,
                    platforms TEXT,
                    rating FLOAT,
                    metacritic INT,
                    background_image TEXT,
                    last_update DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    INDEX idx_title (title),
                    INDEX idx_release_date (release_date)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """)
            
            # Table des prix
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS game_prices (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    game_id_rawg INT,
                    title VARCHAR(255),
                    platform VARCHAR(50),
                    price VARCHAR(20),
                    shop VARCHAR(100),
                    url TEXT,
                    last_update DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    FOREIGN KEY (game_id_rawg) REFERENCES games(game_id_rawg) ON DELETE CASCADE,
                    INDEX idx_game_platform (game_id_rawg, platform),
                    INDEX idx_last_update (last_update)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """)
            
            # Table de métadonnées système
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS system_meta (
                    key_name VARCHAR(100) PRIMARY KEY,
                    value TEXT,
                    last_update DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """)
            
            conn.commit()
            logger.info("✅ Tables créées/vérifiées avec succès")
            return True
            
        except Error as e:
            logger.error(f"Erreur création tables: {e}")
            conn.rollback()
            return False
        finally:
            if conn.is_connected():
                cursor.close()
                conn.close()
    
    def save_games(self, games_df: pd.DataFrame) -> bool:
        """Sauvegarde les jeux dans la base"""
        if games_df.empty:
            logger.info("Aucun jeu à sauvegarder")
            return True
        
        conn = self.get_connection()
        if not conn:
            return False
        
        try:
            cursor = conn.cursor()
            
            # Remplacer NaN par None
            games_df = games_df.replace({np.nan: None})
            
            insert_query = """
                INSERT INTO games (game_id_rawg, title, release_date, genres, platforms, 
                                 rating, metacritic, background_image, last_update)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    title = VALUES(title),
                    release_date = VALUES(release_date),
                    genres = VALUES(genres),
                    platforms = VALUES(platforms),
                    rating = VALUES(rating),
                    metacritic = VALUES(metacritic),
                    background_image = VALUES(background_image),
                    last_update = VALUES(last_update)
            """
            
            data = games_df.values.tolist()
            cursor.executemany(insert_query, data)
            
            conn.commit()
            logger.info(f"✅ {len(games_df)} jeux sauvegardés")
            return True
            
        except Error as e:
            logger.error(f"Erreur sauvegarde jeux: {e}")
            conn.rollback()
            return False
        finally:
            if conn.is_connected():
                cursor.close()
                conn.close()
    
    def get_games_for_price_update(self, limit: int = 50) -> pd.DataFrame:
        """Récupère les jeux à mettre à jour pour les prix"""
        conn = self.get_connection()
        if not conn:
            return pd.DataFrame()
        
        try:
            query = """
                SELECT g.game_id_rawg, g.title
                FROM games g
                LEFT JOIN game_prices p ON g.game_id_rawg = p.game_id_rawg
                WHERE p.last_update IS NULL 
                   OR p.last_update < DATE_SUB(NOW(), INTERVAL 7 DAY)
                ORDER BY g.rating DESC, g.metacritic DESC
                LIMIT %s
            """
            
            return pd.read_sql(query, conn, params=[limit])
            
        except Error as e:
            logger.error(f"Erreur récupération jeux: {e}")
            return pd.DataFrame()
        finally:
            if conn.is_connected():
                conn.close()
    
    def get_stats(self) -> Dict[str, Any]:
        """Récupère les statistiques de base"""
        conn = self.get_connection()
        if not conn:
            return {}
        
        try:
            cursor = conn.cursor()
            
            stats = {}
            
            # Nombre de jeux
            cursor.execute("SELECT COUNT(*) FROM games")
            stats['total_games'] = cursor.fetchone()[0]
            
            # Nombre de prix
            cursor.execute("SELECT COUNT(*) FROM game_prices")
            stats['total_prices'] = cursor.fetchone()[0]
            
            # Dernière extraction
            cursor.execute("SELECT MAX(last_update) FROM games")
            last_extraction = cursor.fetchone()[0]
            stats['last_extraction'] = last_extraction.strftime('%Y-%m-%d %H:%M:%S') if last_extraction else None
            
            return stats
            
        except Error as e:
            logger.error(f"Erreur récupération stats: {e}")
            return {}
        finally:
            if conn.is_connected():
                cursor.close()
                conn.close()
