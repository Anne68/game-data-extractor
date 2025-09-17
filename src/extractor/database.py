"""
🗄️ Module de gestion de la base de données MySQL - Adapté pour best_price_pc
"""

import mysql.connector
from mysql.connector import Error
import pandas as pd
import numpy as np
import logging
import os
from typing import Dict, Any
from pathlib import Path

logger = logging.getLogger(__name__)

class DatabaseManager:
    """Gestionnaire de base de données MySQL"""
    
    def __init__(self):
        self.db_config = {
            "host": os.getenv("DB_HOST", "mysql-anne.alwaysdata.net"),
            "port": int(os.getenv("DB_PORT", "3306")),
            "user": os.getenv("DB_USER", "anne"),
            "password": os.getenv("DB_PASSWORD", "Vicky2@18"),
            "database": os.getenv("DB_NAME", "anne_games_db"),
            "charset": "utf8mb4"
        }
        
        if not self.db_config['password']:
            raise ValueError("DB_PASSWORD requis dans les variables d'environnement")
    
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
        """Crée les tables nécessaires (utilise les tables existantes)"""
        conn = self.get_connection()
        if not conn:
            return False
        
        try:
            cursor = conn.cursor()
            
            # Vérifier que la table games existe
            cursor.execute("SHOW TABLES LIKE 'games'")
            if not cursor.fetchone():
                logger.error("Table 'games' n'existe pas")
                return False
            
            # Vérifier que la table best_price_pc existe
            cursor.execute("SHOW TABLES LIKE 'best_price_pc'")
            if not cursor.fetchone():
                logger.error("Table 'best_price_pc' n'existe pas")
                return False
            
            # Créer la table api_state si elle n'existe pas
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS api_state (
                    id INT PRIMARY KEY,
                    last_page INT DEFAULT 0
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """)
            
            # Insérer l'état initial si nécessaire
            cursor.execute("INSERT IGNORE INTO api_state (id, last_page) VALUES (1, 0)")
            
            conn.commit()
            logger.info("✅ Tables vérifiées/créées avec succès")
            return True
            
        except Error as e:
            logger.error(f"Erreur vérification tables: {e}")
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
            logger.error(f"❌ Erreur sauvegarde jeux: {e}")
            conn.rollback()
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
        # Version simplifiée qui fonctionne avec votre structure
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
        logger.error(f"Erreur récupération jeux pour prix: {e}")
        return pd.DataFrame()
    finally:
        if conn.is_connected():
            conn.close()
    
    def save_prices(self, prices_df: pd.DataFrame) -> bool:
        """Sauvegarde les prix dans best_price_pc (adapté à votre structure)"""
        if prices_df.empty:
            logger.info("Aucun prix à sauvegarder")
            return True
        
        conn = self.get_connection()
        if not conn:
            return False
        
        try:
            cursor = conn.cursor()
            
            # Adapter les données au format de best_price_pc
            prices_df = prices_df.replace({np.nan: None})
            
            # Insertion adaptée à votre structure best_price_pc
            insert_query = """
                INSERT INTO best_price_pc (title, best_price_PC, best_shop_PC, site_url_PC, last_update, game_id_rawg)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    best_price_PC = VALUES(best_price_PC),
                    best_shop_PC = VALUES(best_shop_PC),
                    site_url_PC = VALUES(site_url_PC),
                    last_update = VALUES(last_update)
            """
            
            data = []
            successful_inserts = 0
            
            for _, row in prices_df.iterrows():
                try:
                    data.append([
                        row.get('title'),
                        row.get('price'),  # best_price_PC
                        row.get('shop'),   # best_shop_PC
                        row.get('url'),    # site_url_PC
                        row.get('last_update'),
                        row.get('game_id_rawg')
                    ])
                    successful_inserts += 1
                except Exception as e:
                    logger.warning(f"Erreur préparation ligne prix: {e}")
                    continue
            
            if data:
                cursor.executemany(insert_query, data)
                conn.commit()
                logger.info(f"✅ {successful_inserts} prix sauvegardés dans best_price_pc")
                return True
            else:
                logger.warning("Aucune donnée prix valide à insérer")
                return False
                
        except Error as e:
            logger.error(f"❌ Erreur sauvegarde prix: {e}")
            conn.rollback()
            return False
        finally:
            if conn.is_connected():
                cursor.close()
                conn.close()
    
    def get_stats(self) -> Dict[str, Any]:
        """Récupère les statistiques de base (adapté pour best_price_pc)"""
        conn = self.get_connection()
        if not conn:
            return {}
        
        try:
            cursor = conn.cursor()
            
            stats = {}
            
            # Nombre de jeux
            cursor.execute("SELECT COUNT(*) FROM games")
            stats['total_games'] = cursor.fetchone()[0]
            
            # Nombre de prix dans best_price_pc
            cursor.execute("SELECT COUNT(*) FROM best_price_pc WHERE best_price_PC IS NOT NULL")
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
    
    def get_detailed_stats(self) -> Dict[str, Any]:
        """Récupère des statistiques détaillées (adapté pour best_price_pc)"""
        conn = self.get_connection()
        if not conn:
            return {}
        
        try:
            cursor = conn.cursor()
            stats = {}
            
            # Statistiques de base
            cursor.execute("SELECT COUNT(*) FROM games")
            stats['total_games'] = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM best_price_pc WHERE best_price_PC IS NOT NULL")
            stats['total_prices'] = cursor.fetchone()[0]
            
            # Dernière page API
            cursor.execute("SELECT last_page FROM api_state WHERE id = 1")
            result = cursor.fetchone()
            stats['last_api_page'] = result[0] if result else 0
            
            # Jeux sans prix
            cursor.execute("""
                SELECT COUNT(DISTINCT g.game_id_rawg)
                FROM games g
                LEFT JOIN best_price_pc p ON g.game_id_rawg = p.game_id_rawg
                WHERE p.game_id_rawg IS NULL
            """)
            stats['games_without_prices'] = cursor.fetchone()[0]
            
            # Dernière extraction
            cursor.execute("SELECT MAX(last_update) FROM games")
            last_update = cursor.fetchone()[0]
            stats['last_extraction'] = last_update.strftime('%Y-%m-%d %H:%M:%S') if last_update else None
            
            # Dernière mise à jour des prix
            cursor.execute("SELECT MAX(last_update) FROM best_price_pc")
            last_price_update = cursor.fetchone()[0]
            stats['last_price_update'] = last_price_update.strftime('%Y-%m-%d %H:%M:%S') if last_price_update else None
            
            # Boutiques les plus représentées
            cursor.execute("""
                SELECT best_shop_PC, COUNT(*) as count 
                FROM best_price_pc 
                WHERE best_shop_PC IS NOT NULL
                GROUP BY best_shop_PC 
                ORDER BY count DESC 
                LIMIT 10
            """)
            stats['top_shops'] = dict(cursor.fetchall())
            
            # Ajouts locaux
            stats['db_size_mb'] = 0
            stats['log_files'] = len(list(Path('logs').glob('*.log'))) if Path('logs').exists() else 0
            
            return stats
            
        except Error as e:
            logger.error(f"Erreur récupération stats détaillées: {e}")
            return {}
        finally:
            if conn.is_connected():
                cursor.close()
                conn.close()
    
    def optimize_database(self):
        """Optimise la base de données"""
        conn = self.get_connection()
        if not conn:
            return False
        
        try:
            cursor = conn.cursor()
            
            # Optimiser les tables principales
            cursor.execute("OPTIMIZE TABLE games")
            cursor.execute("OPTIMIZE TABLE best_price_pc")
            cursor.execute("OPTIMIZE TABLE api_state")
            
            logger.info("✅ Base de données optimisée")
            return True
            
        except Error as e:
            logger.error(f"Erreur optimisation DB: {e}")
            return False
        finally:
            if conn.is_connected():
                cursor.close()
                conn.close()
