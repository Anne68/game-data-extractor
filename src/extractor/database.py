"""
DatabaseManager amélioré avec support TF-IDF
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
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
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
    
    def save_prices(self, prices_df):
        """Sauvegarde les prix avec scores de similarité TF-IDF"""
        if prices_df.empty:
            return True
        
        conn = self.get_connection()
        if not conn:
            return False
        
        try:
            cursor = conn.cursor()
            prices_df = prices_df.replace({np.nan: None})
            
            # Vérifier si la colonne similarity_score existe
            cursor.execute("SHOW COLUMNS FROM best_price_pc LIKE 'similarity_score'")
            has_similarity_column = cursor.fetchone() is not None
            
            if has_similarity_column:
                insert_query = """
                    INSERT INTO best_price_pc (title, best_price_PC, best_shop_PC, site_url_PC, 
                                             last_update, game_id_rawg, similarity_score)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        best_price_PC = VALUES(best_price_PC),
                        best_shop_PC = VALUES(best_shop_PC),
                        last_update = VALUES(last_update),
                        similarity_score = VALUES(similarity_score)
                """
            else:
                # Version sans similarity_score pour compatibilité
                insert_query = """
                    INSERT INTO best_price_pc (title, best_price_PC, best_shop_PC, site_url_PC, 
                                             last_update, game_id_rawg)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        best_price_PC = VALUES(best_price_PC),
                        best_shop_PC = VALUES(best_shop_PC),
                        last_update = VALUES(last_update)
                """
            
            data = []
            for _, row in prices_df.iterrows():
                base_data = [
                    row.get('title'), row.get('price'), row.get('shop'),
                    row.get('url'), row.get('last_update'), row.get('game_id_rawg')
                ]
                
                if has_similarity_column:
                    base_data.append(row.get('similarity_score'))
                
                data.append(base_data)
            
            cursor.executemany(insert_query, data)
            conn.commit()
            
            # Statistiques de qualité
            if has_similarity_column and 'similarity_score' in prices_df.columns:
                avg_similarity = prices_df['similarity_score'].mean()
                high_quality = len(prices_df[prices_df['similarity_score'] >= 0.8])
                logger.info(f"✅ {len(prices_df)} prix sauvegardés (similarité moy: {avg_similarity:.3f}, {high_quality} haute qualité)")
            else:
                logger.info(f"✅ {len(prices_df)} prix sauvegardés")
            
            return True
            
        except Error as e:
            logger.error(f"Erreur sauvegarde prix: {e}")
            return False
        finally:
            if conn.is_connected():
                cursor.close()
                conn.close()
    
    def get_games_for_price_update(self, limit=50, min_similarity=None):
        """Récupère les jeux sans prix, avec option de filtrage par similarité"""
        conn = self.get_connection()
        if not conn:
            return pd.DataFrame()
        
        try:
            if min_similarity:
                # Exclure les jeux avec une mauvaise similarité précédente
                query = """
                    SELECT DISTINCT g.game_id_rawg, g.title
                    FROM games g
                    LEFT JOIN best_price_pc p ON g.game_id_rawg = p.game_id_rawg
                    WHERE p.game_id_rawg IS NULL 
                       OR (p.similarity_score IS NOT NULL AND p.similarity_score < %s)
                    ORDER BY g.rating DESC
                    LIMIT %s
                """
                return pd.read_sql(query, conn, params=[min_similarity, limit])
            else:
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
    
    def get_similarity_statistics(self):
        """Récupère les statistiques de similarité TF-IDF"""
        conn = self.get_connection()
        if not conn:
            return {}
        
        try:
            cursor = conn.cursor()
            stats = {}
            
            # Vérifier si la colonne existe
            cursor.execute("SHOW COLUMNS FROM best_price_pc LIKE 'similarity_score'")
            if not cursor.fetchone():
                return {'similarity_support': False}
            
            cursor.execute("""
                SELECT 
                    COUNT(*) as total_with_similarity,
                    AVG(similarity_score) as avg_similarity,
                    COUNT(CASE WHEN similarity_score >= 0.8 THEN 1 END) as high_quality,
                    COUNT(CASE WHEN similarity_score >= 0.6 AND similarity_score < 0.8 THEN 1 END) as medium_quality,
                    COUNT(CASE WHEN similarity_score < 0.6 THEN 1 END) as low_quality
                FROM best_price_pc 
                WHERE similarity_score IS NOT NULL
            """)
            
            result = cursor.fetchone()
            if result:
                stats.update({
                    'similarity_support': True,
                    'total_with_similarity': result[0],
                    'avg_similarity': float(result[1]) if result[1] else 0.0,
                    'high_quality_matches': result[2],
                    'medium_quality_matches': result[3],
                    'low_quality_matches': result[4]
                })
            
            return stats
            
        except Error as e:
            logger.error(f"Erreur stats similarité: {e}")
            return {'similarity_support': False}
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
            
            # Ajouter les stats de similarité
            similarity_stats = self.get_similarity_statistics()
            stats.update(similarity_stats)
            
            return stats
        except Error as e:
            return {}
        finally:
            if conn.is_connected():
                cursor.close()
                conn.close()

