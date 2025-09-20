# Créer un nouveau script de pipeline amélioré
cat > scripts/run_complete_pipeline.py << 'EOF'
#!/usr/bin/env python3
"""
Pipeline complet : extraction jeux + scraping prix automatique
"""

import os
import sys
import time
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

def main():
    print("🚀 Pipeline complet : Jeux + Prix")
    
    try:
        # 1. Extraction des jeux
        print("\n📥 Phase 1 : Extraction de 50 nouveaux jeux")
        from extractor.rawg_extractor import RawgExtractor
        from extractor.database import DatabaseManager
        
        db = DatabaseManager()
        extractor = RawgExtractor()
        
        # Récupérer l'état actuel
        conn = db.get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT COALESCE(last_page, 64) FROM api_state WHERE id = 1")
        result = cursor.fetchone()
        last_page = result[0] if result else 64
        next_page = last_page + 1
        conn.close()
        
        print(f"Extraction depuis la page {next_page}")
        
        # Extraire 50 jeux (environ 1.5 pages)
        games_df = extractor.fetch_games(limit=50, start_page=next_page)
        
        if not games_df.empty:
            success = db.save_games(games_df)
            if success:
                print(f"✅ {len(games_df)} nouveaux jeux ajoutés")
                
                # Mettre à jour l'état
                new_last_page = next_page + 1
                conn = db.get_connection()
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT INTO api_state (id, last_page, last_extraction, total_games_extracted)
                    VALUES (1, %s, NOW(), %s)
                    ON DUPLICATE KEY UPDATE
                        last_page = VALUES(last_page),
                        last_extraction = VALUES(last_extraction),
                        total_games_extracted = total_games_extracted + VALUES(total_games_extracted)
                """, (new_last_page, len(games_df)))
                conn.commit()
                conn.close()
            else:
                print("❌ Erreur sauvegarde jeux")
                return
        else:
            print("❌ Aucun jeu extrait")
            return
        
        # 2. Pause entre les phases
        print("\n⏸️ Pause de 5 secondes...")
        time.sleep(5)
        
        # 3. Scraping des prix
        print("\n💰 Phase 2 : Scraping automatique des prix")
        from extractor.price_scraper import PriceScraper
        
        scraper = PriceScraper()
        
        # Récupérer les jeux sans prix (nouveaux en priorité)
        games_to_scrape = db.get_games_for_price_update(limit=10)
        
        if not games_to_scrape.empty:
            print(f"Scraping pour {len(games_to_scrape)} jeux")
            
            prices_df = scraper.scrape_prices(games_to_scrape)
            
            if not prices_df.empty:
                success = db.save_prices(prices_df)
                if success:
                    successful_prices = len([p for _, p in prices_df.iterrows() if p.get('price')])
                    print(f"✅ {successful_prices} prix récupérés et sauvegardés")
                else:
                    print("❌ Erreur sauvegarde prix")
            else:
                print("⚠️ Aucun prix récupéré")
        else:
            print("ℹ️ Aucun jeu trouvé pour le scraping")
        
        # 4. Statistiques finales
        print("\n📊 Statistiques finales :")
        stats = db.get_stats()
        print(f"  🎮 Total jeux : {stats.get('total_games', 0)}")
        print(f"  💰 Total prix : {stats.get('total_prices', 0)}")
        
        # Calculer le ratio
        total_games = stats.get('total_games', 0)
        total_prices = stats.get('total_prices', 0)
        if total_games > 0:
            ratio = (total_prices / total_games) * 100
            print(f"  📈 Couverture prix : {ratio:.1f}%")
        
        print("\n🎉 Pipeline complet terminé avec succès !")
        
    except Exception as e:
        print(f"\n❌ Erreur pipeline : {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
EOF

# Rendre le script exécutable
chmod +x scripts/run_complete_pipeline.py
