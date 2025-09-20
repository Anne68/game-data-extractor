
#!/usr/bin/env python3
"""
🔍 Diagnostic du scraping DLCompare
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
import time

def setup_driver():
    options = Options()
    options.add_argument('--headless=new')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36')
    
    return webdriver.Chrome(options=options)

def test_dlcompare_search(driver, title):
    """Test une recherche sur DLCompare et affiche le HTML"""
    search_query = title.replace(' ', '+')
    search_url = f"https://www.dlcompare.fr/search?q={search_query}"
    
    print(f"🔍 Test pour: {title}")
    print(f"URL: {search_url}")
    
    driver.get(search_url)
    time.sleep(3)
    
    # Afficher le titre de la page
    print(f"Titre page: {driver.title}")
    
    # Chercher différents sélecteurs
    selectors_to_test = [
        ".search-result",
        ".game-item", 
        ".product-item",
        "h3 a",
        ".title a",
        ".name a",
        "a[href*='game']",
        ".game-card",
        "[data-game]"
    ]
    
    print("\n🎯 Test des sélecteurs:")
    found_any = False
    
    for selector in selectors_to_test:
        try:
            elements = driver.find_elements(By.CSS_SELECTOR, selector)
            print(f"  {selector}: {len(elements)} éléments")
            
            if elements:
                found_any = True
                # Afficher les premiers éléments
                for i, elem in enumerate(elements[:3]):
                    try:
                        text = elem.text.strip()[:50]
                        href = elem.get_attribute('href')
                        print(f"    [{i+1}] {text} -> {href}")
                    except:
                        print(f"    [{i+1}] Erreur lecture élément")
        except Exception as e:
            print(f"  {selector}: Erreur - {e}")
    
    if not found_any:
        print("\n❌ Aucun élément trouvé avec les sélecteurs testés")
        print("📄 Extrait du HTML de la page:")
        body_html = driver.find_element(By.TAG_NAME, "body").get_attribute('innerHTML')[:1000]
        print(body_html)
    
    print("\n" + "="*50)

def main():
    driver = setup_driver()
    
    try:
        # Test avec des jeux populaires
        test_games = [
            "Grand Theft Auto V",
            "Portal 2", 
            "Cyberpunk 2077"
        ]
        
        for game in test_games:
            test_dlcompare_search(driver, game)
            time.sleep(2)
            
    finally:
        driver.quit()

if __name__ == "__main__":
    main()
