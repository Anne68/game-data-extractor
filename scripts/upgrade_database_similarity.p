"""
Script de mise à jour de la base pour inclure les scores de similarité
"""

import os
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

def main():
    from extractor.database import DatabaseManager
    
    db = DatabaseManager()
    conn = db.get_connection()
    
    if not conn:
        print("❌ Impossible de se connecter à la base")
        return
    
    try:
        cursor = conn.cursor()
        
        # Ajouter la colonne similarity_score à best_price_pc
        try:
            cursor.execute("ALTER TABLE best_price_pc ADD COLUMN similarity_score DECIMAL(4,3) DEFAULT NULL")
            print("✅ Colonne similarity_score ajoutée à best_price_pc")
        except Exception as e:
            if "Duplicate column name" in str(e):
                print("ℹ️ Colonne similarity_score déjà présente")
            else:
                print(f"Erreur ajout colonne: {e}")
        
        # Ajouter un index pour les requêtes de similarité
        try:
            cursor.execute("ALTER TABLE best_price_pc ADD INDEX idx_similarity_score (similarity_score)")
            print("✅ Index sur similarity_score créé")
        except Exception as e:
            if "Duplicate key name" in str(e):
                print("ℹ️ Index similarity_score déjà présent")
            else:
                print(f"Erreur création index: {e}")
        
        conn.commit()
        conn.close()
        
        print("✅ Base de données mise à jour pour le TF-IDF")
        
    except Exception as e:
        print(f"❌ Erreur mise à jour base: {e}")

if __name__ == "__main__":
    main()
EOF
