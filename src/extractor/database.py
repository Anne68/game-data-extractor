cat > src/extractor/database.py << 'EOF'
"""
🗄️ Module de gestion de la base de données MySQL
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
            "user": os.getenv("DB_USER", "anne"),
            "password": os.getenv("DB_PASSWORD", "Vicky2@18"),
            "database": os.getenv("DB_NAME", "anne_games_db"),
            "charset": "utf8mb4"
        }
    
    def get_connection(self):
        try:
            return mysql.connector.connect(**self.db_config)
        except Error as e:
            logger.error(f"Erreur connexion: {e}")
            return None
    
    def test_connection(self) -> bool:
        conn = self.get_connection()
        if conn and conn.is_connected():
            conn.close()
            return True
        return False
    
    def setup_tables(self):
        conn = self.get_connection()
        if not conn:
            return False
        
        try:
            cursor = conn.cursor()
            
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
                    INDEX idx_title (title)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """)
            
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
                    INDEX idx_game_id (game_id_rawg)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """)
            
            conn.commit()
            logger.info("✅ Tables créées")
            return True
            
        except Error as e:
            logger.error(f"Erreur tables: {e}")
            return False
        finally:
            if conn.is_connected():
                cursor.close()
                conn.close()
    
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
                                 rating, metacritic, background_image, last_update)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    title = VALUES(title),
                    last_update = VALUES(last_update)
            """
            
            data = []
            for _, row in games_df.iterrows():
                data.append([
                    row.get('game_id_rawg'),
                    row.get('title'),
                    row.get('release_date'),
                    row.get('genres'),
                    row.get('platforms'),
                    row.get('rating'),
                    row.get('metacritic'),
                    row.get('background_image'),
                    row.get('last_update')
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
                SELECT game_id_rawg, title
                FROM games
                ORDER BY rating DESC
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
        return True  # Placeholder
    
    def get_stats(self):
        conn = self.get_connection()
        if not conn:
            return {}
        
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM games")
            total_games = cursor.fetchone()[0]
            
            return {'total_games': total_games, 'total_prices': 0}
        except Error as e:
            return {}
        finally:
            if conn.is_connected():
                cursor.close()
                conn.close()
    
    def get_detailed_stats(self):
        return self.get_stats()
    
    def optimize_database(self):
        return True
EOF
