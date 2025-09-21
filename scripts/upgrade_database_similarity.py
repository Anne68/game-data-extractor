#!/usr/bin/env python3
"""
🔧 Script de mise à jour de la base pour inclure les scores de similarité TF-IDF
"""

import os
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

def main():
    try:
        from extractor.database import DatabaseManager
        
        db = DatabaseManager()
        conn = db.get_connection()
        
        if not conn:
            print("❌ Impossible de se connecter à la base")
            return False
        
        try:
            cursor = conn.cursor()
            
            print("🔧 Mise à jour de la structure de base pour TF-IDF...")
            
            # Vérifier si la table api_state existe
            cursor.execute("SHOW TABLES LIKE 'api_state'")
            if not cursor.fetchone():
                print("📋 Création de la table api_state...")
                cursor.execute("""
                    CREATE TABLE api_state (
                        id INT PRIMARY KEY,
                        last_page INT NOT NULL DEFAULT 0,
                        last_extraction DATETIME,
                        total_games_extracted INT DEFAULT 0
                    )
                """)
                cursor.execute("INSERT INTO api_state (id, last_page, total_games_extracted) VALUES (1, 0, 0)")
                print("✅ Table api_state créée")
            
            # Ajouter la colonne similarity_score à best_price_pc
            cursor.execute("SHOW COLUMNS FROM best_price_pc LIKE 'similarity_score'")
            if not cursor.fetchone():
                cursor.execute("ALTER TABLE best_price_pc ADD COLUMN similarity_score DECIMAL(4,3) DEFAULT NULL")
                print("✅ Colonne similarity_score ajoutée à best_price_pc")
            else:
                print("ℹ️ Colonne similarity_score déjà présente")
            
            # Ajouter un index pour les requêtes de similarité
            cursor.execute("SHOW INDEX FROM best_price_pc WHERE Key_name = 'idx_similarity_score'")
            if not cursor.fetchone():
                cursor.execute("ALTER TABLE best_price_pc ADD INDEX idx_similarity_score (similarity_score)")
                print("✅ Index sur similarity_score créé")
            else:
                print("ℹ️ Index similarity_score déjà présent")
            
            # Vérifier les colonnes manquantes dans api_state
            cursor.execute("SHOW COLUMNS FROM api_state")
            columns = [col[0] for col in cursor.fetchall()]
            
            if 'last_extraction' not in columns:
                cursor.execute("ALTER TABLE api_state ADD COLUMN last_extraction DATETIME")
                print("✅ Colonne last_extraction ajoutée")
            
            if 'total_games_extracted' not in columns:
                cursor.execute("ALTER TABLE api_state ADD COLUMN total_games_extracted INT DEFAULT 0")
                print("✅ Colonne total_games_extracted ajoutée")
            
            conn.commit()
            
            # Afficher les statistiques
            cursor.execute("SELECT COUNT(*) FROM games")
            total_games = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM best_price_pc")
            total_prices = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM best_price_pc WHERE similarity_score IS NOT NULL")
            prices_with_similarity = cursor.fetchone()[0]
            
            print(f"\n📊 État de la base après mise à jour:")
            print(f"   🎮 Total jeux: {total_games}")
            print(f"   💰 Total prix: {total_prices}")
            print(f"   🧠 Prix avec TF-IDF: {prices_with_similarity}")
            
            conn.close()
            print("\n✅ Base de données mise à jour avec succès pour le TF-IDF")
            return True
            
        except Exception as e:
            print(f"❌ Erreur lors de la mise à jour: {e}")
            conn.rollback()
            return False
        finally:
            if conn.is_connected():
                conn.close()
                
    except Exception as e:
        print(f"❌ Erreur de connexion: {e}")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
