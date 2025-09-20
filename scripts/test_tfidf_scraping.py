"""
🧪 Test du scraping avec TF-IDF
"""

import os
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

def main():
    print("🧪 Test du scraping avec TF-IDF")
    
    # Test du module de similarité
    print("\n1. Test du module TF-IDF:")
    from utils.text_similarity import test_similarity
    test_similarity()
    
    # Test du scraper
    print("\n2. Test du scraper TF-IDF:")
    from extractor.price_scraper import PriceScraperTFIDF
    
    scraper = PriceScraperTFIDF()
    
    # Test avec quelques jeux connus
    import pandas as pd
    test_games = pd.DataFrame([
        {'game_id_rawg': 1, 'title': 'Cyberpunk 2077'},
        {'game_id_rawg': 2, 'title': 'The Witcher 3: Wild Hunt'},
        {'game_id_rawg': 3, 'title': 'Grand Theft Auto V'}
    ])
    
    print(f"Jeux de test: {len(test_games)}")
    for _, game in test_games.iterrows():
        print(f"  - {game['title']}")
    
    # Lancer le scraping test
    results = scraper.scrape_prices(test_games)
    
    print("\n📊 Résultats TF-IDF:")
    if not results.empty:
        for _, row in results.iterrows():
            similarity = row.get('similarity_score', 0)
            price = row.get('price', 'N/A')
            shop = row.get('shop', 'N/A')
            
            quality = "🔥" if similarity >= 0.8 else "✅" if similarity >= 0.6 else "⚠️" if similarity >= 0.4 else "❌"
            
            print(f"{quality} {row['title']}")
            print(f"   Similarité: {similarity:.3f}")
            print(f"   Prix: {price} chez {shop}")
            print()
    else:
        print("❌ Aucun résultat")
    
    # Test mise à jour base de données
    print("3. Test mise à jour base de données:")
    try:
        from extractor.database import DatabaseManager
        db = DatabaseManager()
        
        # Mettre à jour la base pour supporter TF-IDF
        import subprocess
        result = subprocess.run([sys.executable, 'scripts/upgrade_database_similarity.py'], 
                              capture_output=True, text=True)
        print(result.stdout)
        
        # Test sauvegarde avec similarité
        if not results.empty:
            success = db.save_prices(results)
            print(f"✅ Sauvegarde test: {'réussie' if success else 'échouée'}")
            
            # Afficher les stats de similarité
            stats = db.get_similarity_statistics()
            if stats.get('similarity_support'):
                print(f"📊 Stats similarité:")
                print(f"   Moyenne: {stats.get('avg_similarity', 0):.3f}")
                print(f"   Haute qualité (≥0.8): {stats.get('high_quality_matches', 0)}")
                print(f"   Qualité moyenne (0.6-0.8): {stats.get('medium_quality_matches', 0)}")
                print(f"   Faible qualité (<0.6): {stats.get('low_quality_matches', 0)}")
        
    except Exception as e:
        print(f"❌ Erreur test base: {e}")
    
    print("\n✅ Test TF-IDF terminé")

if __name__ == "__main__":
    main()
