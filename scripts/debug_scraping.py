# Créer un script de diagnostic du scraping

"""
Script de diagnostic du scraping
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

def main():
    print("🔍 Diagnostic du scraping")
    
    # 1. Test des imports
    try:
        from selenium import webdriver
        print("✅ Selenium importé")
    except ImportError as e:
        print(f"❌ Erreur import Selenium: {e}")
        return
    
    # 2. Test de Chrome
    try:
        from selenium.webdriver.chrome.options import Options
        options = Options()
        options.add_argument('--headless=new')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        
        driver = webdriver.Chrome(options=options)
        driver.get("https://www.google.com")
        print("✅ Chrome fonctionne")
        driver.quit()
    except Exception as e:
        print(f"❌ Erreur Chrome: {e}")
        return
    
    # 3. Test du scraper
    try:
        from extractor.price_scraper import PriceScraper
        scraper = PriceScraper()
        print(f"✅ PriceScraper créé - Enabled: {scraper.enabled}")
        
        # Test simple
        if scraper.test_scraping():
            print("✅ Test scraping réussi")
        else:
            print("❌ Test scraping échoué")
            
    except Exception as e:
        print(f"❌ Erreur PriceScraper: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
