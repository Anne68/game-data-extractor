"""
🔧 Activation du vrai scraping et nettoyage des données de test
"""

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
    
    cursor = conn.cursor()
    
    print("🧹 Nettoyage des données de test...")
    
    # Supprimer les entrées TestShop
    cursor.execute("DELETE FROM best_price_pc WHERE best_shop_PC = 'TestShop'")
    deleted_testshop = cursor.rowcount
    
    # Supprimer les URLs example.com
    cursor.execute("DELETE FROM best_price_pc WHERE site_url_PC LIKE '%example.com%'")
    deleted_example = cursor.rowcount
    
    # Supprimer les prix avec similarity_score exactement 0.7 (simulés)
    cursor.execute("DELETE FROM best_price_pc WHERE similarity_score = 0.7")
    deleted_simulated = cursor.rowcount
    
    conn.commit()
    
    print(f"✅ Nettoyage terminé:")
    print(f"   - TestShop supprimés: {deleted_testshop}")
    print(f"   - Example.com supprimés: {deleted_example}")
    print(f"   - Prix simulés supprimés: {deleted_simulated}")
    
    # Statistiques après nettoyage
    cursor.execute("SELECT COUNT(*) FROM best_price_pc")
    remaining_prices = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(DISTINCT best_shop_PC) FROM best_price_pc WHERE best_shop_PC IS NOT NULL")
    real_shops = cursor.fetchone()[0]
    
    print(f"\n📊 État après nettoyage:")
    print(f"   - Prix restants: {remaining_prices}")
    print(f"   - Vraies boutiques: {real_shops}")
    
    if real_shops > 0:
        cursor.execute("""
            SELECT best_shop_PC, COUNT(*) as count 
            FROM best_price_pc 
            WHERE best_shop_PC IS NOT NULL 
            GROUP BY best_shop_PC 
            ORDER BY count DESC 
            LIMIT 5
        """)
        
        print("\n🏪 Top boutiques réelles:")
        for shop, count in cursor.fetchall():
            print(f"   - {shop}: {count} prix")
    
    conn.close()
    
    print("\n🚀 Pour activer le vrai scraping:")
    print("   1. Modifiez src/extractor/price_scraper.py")
    print("   2. Supprimez le mode simulation")
    print("   3. Activez le vrai scraping DLCompare")

if __name__ == "__main__":
    main()
