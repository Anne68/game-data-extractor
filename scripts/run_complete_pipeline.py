#!/usr/bin/env python3
"""
🧠 Pipeline complet avec TF-IDF et statistiques de qualité
"""

import os
import sys
import time
import requests
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

def send_discord_notification(title, description, color=3066993, fields=None):
    """Envoie une notification Discord avec stats TF-IDF"""
    webhook_url = os.getenv('DISCORD_WEBHOOK')
    if not webhook_url:
        return False
    
    embed = {
        'title': title,
        'description': description,
        'color': color,
        'timestamp': datetime.now().isoformat(),
        'footer': {'text': 'Game Data Extractor • TF-IDF Enhanced'}
    }
    
    if fields:
        embed['fields'] = fields
    
    payload = {'embeds': [embed]}
    
    try:
        response = requests.post(webhook_url, json=payload, timeout=10)
        response.raise_for_status()
        return True
    except Exception as e:
        print(f"Erreur notification Discord: {e}")
        return False

def main():
    print("🧠 Pipeline complet avec TF-IDF")
    
    # Notification de début
    send_discord_notification(
        "🧠 Pipeline TF-IDF - DÉBUT",
        "Lancement du pipeline avec matching intelligent TF-IDF",
        color=3447003
    )
    
    start_time = time.time()
    
    try:
        from extractor.rawg_extractor import RawgExtractor
        from extractor.database import DatabaseManager
        from extractor.price_scraper import PriceScraperTFIDF  # Import corrigé
        
        db = DatabaseManager()
        extractor = RawgExtractor()
        scraper = PriceScraperTFIDF()
        
        # Stats initiales
        initial_stats = db.get_stats()
        initial_games = initial_stats.get('total_games', 0)
        initial_prices = initial_stats.get('total_prices', 0)
        initial_similarity = initial_stats.get('avg_similarity', 0)
        
        # Phase 1: Extraction jeux
        print("\n📥 Phase 1: Extraction de nouveaux jeux")
        
        conn = db.get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT COALESCE(last_page, 64) FROM api_state WHERE id = 1")
        result = cursor.fetchone()
        last_page = result[0] if result else 64
        next_page = last_page + 1
        conn.close()
        
        games_df = extractor.fetch_games(limit=50, start_page=next_page)
        
        new_games_count = 0
        if not games_df.empty:
            success = db.save_games(games_df)
            if success:
                new_games_count = len(games_df)
                print(f"✅ {new_games_count} nouveaux jeux ajoutés")
                
                # Mettre à jour l'état
                conn = db.get_connection()
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT INTO api_state (id, last_page, last_extraction, total_games_extracted)
                    VALUES (1, %s, NOW(), %s)
                    ON DUPLICATE KEY UPDATE
                        last_page = VALUES(last_page),
                        last_extraction = VALUES(last_extraction),
                        total_games_extracted = total_games_extracted + VALUES(total_games_extracted)
                """, (next_page + 1, new_games_count))
                conn.commit()
                conn.close()
        
        # Phase 2: Scraping prix avec TF-IDF
        print("\n🧠 Phase 2: Scraping TF-IDF des prix")
        time.sleep(3)
        
        games_to_scrape = db.get_games_for_price_update(limit=15)
        new_prices_count = 0
        tfidf_stats = {'avg_new_similarity': 0, 'high_quality_new': 0}
        
        if not games_to_scrape.empty:
            print(f"🔍 Scraping TF-IDF pour {len(games_to_scrape)} jeux")
            prices_df = scraper.scrape_prices(games_to_scrape)
            
            if not prices_df.empty:
                success = db.save_prices(prices_df)
                if success:
                    valid_prices = prices_df[prices_df['price'].notna()]
                    new_prices_count = len(valid_prices)
                    
                    if not valid_prices.empty and 'similarity_score' in valid_prices.columns:
                        tfidf_stats['avg_new_similarity'] = valid_prices['similarity_score'].mean()
                        tfidf_stats['high_quality_new'] = len(valid_prices[valid_prices['similarity_score'] >= 0.8])
                        
                        print(f"✅ {new_prices_count} prix TF-IDF trouvés (similarité moy: {tfidf_stats['avg_new_similarity']:.3f})")
        
        # Stats finales
        final_stats = db.get_stats()
        final_games = final_stats.get('total_games', 0)
        final_prices = final_stats.get('total_prices', 0)
        final_similarity = final_stats.get('avg_similarity', 0)
        
        similarity_improvement = final_similarity - initial_similarity
        execution_time = round(time.time() - start_time, 1)
        coverage = (final_prices / final_games * 100) if final_games > 0 else 0
        
        # Notification de succès
        description = f"""🧠 **Pipeline TF-IDF terminé avec succès**

📊 **Résultats:**
- Nouveaux jeux extraits: **{new_games_count}**
- Nouveaux prix TF-IDF: **{new_prices_count}**
- Matchs haute qualité: **{tfidf_stats['high_quality_new']}**
- Total jeux: **{final_games}** (+{final_games - initial_games})
- Total prix: **{final_prices}** (+{final_prices - initial_prices})

🧠 **Qualité TF-IDF:**
- Similarité moyenne: **{final_similarity:.3f}** ({similarity_improvement:+.3f})
- Nouveaux matchs: **{tfidf_stats['avg_new_similarity']:.3f}**
- Couverture prix: **{coverage:.1f}%**
"""

        fields = [
            {
                'name': '🎮 Données',
                'value': f'{final_games} jeux\n{final_prices} prix',
                'inline': True
            },
            {
                'name': '🧠 Qualité TF-IDF', 
                'value': f'Similarité: {final_similarity:.3f}\nHaute qualité: {final_stats.get("high_quality_matches", 0)}',
                'inline': True
            },
            {
                'name': '⚡ Performance',
                'value': f'Temps: {execution_time}s\nCouverture: {coverage:.1f}%',
                'inline': True
            }
        ]
        
        send_discord_notification(
            "✅ Pipeline TF-IDF - SUCCÈS",
            description,
            color=3066993,
            fields=fields
        )
        
        print(f"\n🎉 Pipeline TF-IDF terminé!")
        print(f"📊 Qualité moyenne des matchs: {final_similarity:.3f}")
        print(f"🔥 Matchs haute qualité: {final_stats.get('high_quality_matches', 0)}")
        
    except Exception as e:
        # Notification d'erreur
        send_discord_notification(
            "❌ Pipeline TF-IDF - ÉCHEC",
            f"**Erreur lors de l'exécution du pipeline TF-IDF**\n\nErreur: `{str(e)}`\n\n⚠️ Vérifiez les logs pour plus de détails",
            color=15158332
        )
        
        print(f"\n❌ Erreur pipeline: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
