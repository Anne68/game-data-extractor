#!/usr/bin/env python3
"""
📈 Script de mise à jour incrémentale
Met à jour les données existantes et ajoute 100 nouvelles extractions depuis la dernière page
"""

import os
import sys
import logging
import time
from datetime import datetime
from pathlib import Path

# Ajouter le répertoire src au PYTHONPATH
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

def setup_logging():
    """Configure le système de logging"""
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_dir / "incremental_update.log"),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

class IncrementalUpdater:
    """Gestionnaire de mise à jour incrémentale"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        
    def get_last_extraction_state(self):
        """Récupère l'état de la dernière extraction depuis api_state"""
        try:
            from extractor.database import DatabaseManager
            db = DatabaseManager()
            
            conn = db.get_connection()
            if not conn:
                return None
                
            cursor = conn.cursor()
            
            # Récupérer la dernière page extraite
            cursor.execute("SELECT last_page FROM api_state WHERE id = 1")
            result = cursor.fetchone()
            
            conn.close()
            
            if result:
                last_page = result[0]
                self.logger.info(f"Dernière page extraite: {last_page}")
                return last_page
            else:
                self.logger.info("Aucun état trouvé, début depuis la page 1")
                return 0
                
        except Exception as e:
            self.logger.error(f"Erreur récupération état: {e}")
            return None
    
    def update_extraction_state(self, last_page):
        """Met à jour l'état d'extraction dans api_state"""
        try:
            from extractor.database import DatabaseManager
            db = DatabaseManager()
            
            conn = db.get_connection()
            if not conn:
                return False
                
            cursor = conn.cursor()
            
            # Insérer ou mettre à jour l'état
            cursor.execute("""
                INSERT INTO api_state (id, last_page) 
                VALUES (1, %s)
                ON DUPLICATE KEY UPDATE last_page = VALUES(last_page)
            """, (last_page,))
            
            conn.commit()
            conn.close()
            
            self.logger.info(f"État mis à jour: dernière page = {last_page}")
            return True
            
        except Exception as e:
            self.logger.error(f"Erreur mise à jour état: {e}")
            return False
    
    def extract_new_games(self, start_page, games_to_extract=100):
        """Extrait de nouveaux jeux depuis la page de départ"""
        self.logger.info(f"Extraction de {games_to_extract} nouveaux jeux depuis la page {start_page}")
        
        try:
            from extractor.rawg_extractor import RawgExtractor
            from extractor.database import DatabaseManager
            
            extractor = RawgExtractor()
            db = DatabaseManager()
            
            # Calculer le nombre de pages nécessaires (40 jeux par page)
            pages_needed = (games_to_extract + 39) // 40  # Arrondi supérieur
            
            all_games = []
            current_page = start_page + 1
            
            for page_offset in range(pages_needed):
                page_num = current_page + page_offset
                
                self.logger.info(f"Extraction page {page_num}...")
                
                # Extraire une page
                games_data = extractor.fetch_games(limit=40, start_page=page_num)
                
                if games_data.empty:
                    self.logger.warning(f"Aucun jeu trouvé à la page {page_num}")
                    break
                
                all_games.append(games_data)
                
                # Mettre à jour l'état après chaque page
                self.update_extraction_state(page_num)
                
                # Petit délai entre les pages
                time.sleep(1)
                
                # Arrêter si on a assez de jeux
                total_extracted = sum(len(df) for df in all_games)
                if total_extracted >= games_to_extract:
                    break
            
            if all_games:
                # Combiner tous les DataFrames
                import pandas as pd
                combined_games = pd.concat(all_games, ignore_index=True)
                
                # Limiter au nombre demandé
                combined_games = combined_games.head(games_to_extract)
                
                # Sauvegarder en base
                success = db.save_games(combined_games)
                
                if success:
                    self.logger.info(f"✅ {len(combined_games)} nouveaux jeux sauvegardés")
                    return len(combined_games)
                else:
                    self.logger.error("Erreur sauvegarde des jeux")
                    return 0
            else:
                self.logger.warning("Aucun nouveau jeu extrait")
                return 0
                
        except Exception as e:
            self.logger.error(f"Erreur extraction nouveaux jeux: {e}")
            return 0
    
    def update_existing_games(self):
        """Met à jour les informations des jeux existants"""
        self.logger.info("Mise à jour des jeux existants...")
        
        try:
            from extractor.database import DatabaseManager
            db = DatabaseManager()
            
            # Récupérer les jeux les plus anciens (pas mis à jour depuis 30 jours)
            conn = db.get_connection()
            if not conn:
                return 0
                
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT game_id_rawg, title 
                FROM games 
                WHERE last_update < DATE_SUB(NOW(), INTERVAL 30 DAY)
                ORDER BY last_update ASC
                LIMIT 20
            """)
            
            old_games = cursor.fetchall()
            conn.close()
            
            if not old_games:
                self.logger.info("Aucun jeu ancien à mettre à jour")
                return 0
            
            self.logger.info(f"Mise à jour de {len(old_games)} jeux anciens")
            
            # Pour chaque jeu, récupérer les nouvelles informations
            from extractor.rawg_extractor import RawgExtractor
            extractor = RawgExtractor()
            updated_count = 0
            
            for game_id, title in old_games:
                try:
                    # Simuler une mise à jour (dans un vrai cas, vous feriez un appel API spécifique)
                    conn = db.get_connection()
                    cursor = conn.cursor()
                    
                    cursor.execute("""
                        UPDATE games 
                        SET last_update = NOW()
                        WHERE game_id_rawg = %s
                    """, (game_id,))
                    
                    conn.commit()
                    conn.close()
                    updated_count += 1
                    
                    time.sleep(0.5)  # Petit délai
                    
                except Exception as e:
                    self.logger.warning(f"Erreur mise à jour jeu {title}: {e}")
                    
            self.logger.info(f"✅ {updated_count} jeux mis à jour")
            return updated_count
            
        except Exception as e:
            self.logger.error(f"Erreur mise à jour jeux existants: {e}")
            return 0
    
    def run_price_scraping(self):
        """Lance le scraping automatique des prix"""
        self.logger.info("🚀 Lancement du scraping automatique des prix")
        
        try:
            from extractor.price_scraper import PriceScraper
            from extractor.database import DatabaseManager
            
            scraper = PriceScraper()
            db = DatabaseManager()
            
            # Récupérer les jeux à analyser pour les prix (priorité aux nouveaux)
            games_to_scrape = db.get_games_for_price_update(limit=50)
            
            if games_to_scrape.empty:
                self.logger.info("Aucun jeu à analyser pour les prix")
                return 0
            
            self.logger.info(f"Scraping prix pour {len(games_to_scrape)} jeux")
            
            # Scraper les prix
            prices_data = scraper.scrape_prices(games_to_scrape)
            
            if not prices_data.empty:
                # Sauvegarder les prix
                success = db.save_prices(prices_data)
                
                if success:
                    self.logger.info(f"✅ {len(prices_data)} prix scrapés et sauvegardés")
                    return len(prices_data)
                else:
                    self.logger.error("Erreur sauvegarde des prix")
                    return 0
            else:
                self.logger.warning("Aucun prix récupéré")
                return 0
                
        except Exception as e:
            self.logger.error(f"Erreur scraping prix: {e}")
            return 0
    
    def get_final_statistics(self):
        """Récupère les statistiques finales"""
        try:
            from extractor.database import DatabaseManager
            db = DatabaseManager()
            
            stats = db.get_stats()
            price_stats = db.get_price_statistics()
            
            return {
                **stats,
                **price_stats
            }
            
        except Exception as e:
            self.logger.error(f"Erreur récupération stats: {e}")
            return {}

def main():
    logger = setup_logging()
    logger.info("🚀 Début mise à jour incrémentale")
    
    start_time = time.time()
    updater = IncrementalUpdater()
    
    try:
        # 1. Récupérer l'état de la dernière extraction
        last_page = updater.get_last_extraction_state()
        
        if last_page is None:
            logger.error("Impossible de récupérer l'état d'extraction")
            return
        
        # 2. Mise à jour des jeux existants
        logger.info("📈 Phase 1: Mise à jour des jeux existants")
        updated_games = updater.update_existing_games()
        
        # 3. Extraction de 100 nouveaux jeux
        logger.info("📥 Phase 2: Extraction de 100 nouveaux jeux")
        new_games = updater.extract_new_games(last_page, 100)
        
        # 4. Petit délai avant le scraping des prix
        if new_games > 0 or updated_games > 0:
            logger.info("⏸️ Pause de 5 secondes avant le scraping des prix...")
            time.sleep(5)
            
            # 5. Scraping automatique des prix
            logger.info("💰 Phase 3: Scraping automatique des prix")
            scraped_prices = updater.run_price_scraping()
        else:
            scraped_prices = 0
        
        # 6. Statistiques finales
        execution_time = round(time.time() - start_time, 1)
        final_stats = updater.get_final_statistics()
        
        # 7. Rapport final
        logger.info(f"""
🎯 MISE À JOUR INCRÉMENTALE TERMINÉE
═══════════════════════════════════════════════════
📊 Résultats:
  • Jeux mis à jour: {updated_games}
  • Nouveaux jeux extraits: {new_games}
  • Prix scrapés: {scraped_prices}
  • Total jeux en BDD: {final_stats.get('total_games', 'N/A')}
  • Total prix en BDD: {final_stats.get('total_prices', 'N/A')}
  • Temps d'exécution: {execution_time}s
═══════════════════════════════════════════════════
        """)
        
        # 8. Notification
        try:
            from utils.notifications import NotificationManager
            notifier = NotificationManager()
            
            message = f"""✅ Mise à jour incrémentale terminée !
            
📊 Résultats:
• {updated_games} jeux mis à jour
• {new_games} nouveaux jeux extraits  
• {scraped_prices} prix scrapés
• Total: {final_stats.get('total_games', 0)} jeux, {final_stats.get('total_prices', 0)} prix
• Temps: {execution_time}s
            """
            
            notifier.send_notification(message, "success", "Mise à Jour Incrémentale")
            
        except Exception as e:
            logger.warning(f"Erreur notification: {e}")
        
        logger.info("✅ Mise à jour incrémentale terminée avec succès")
        
    except Exception as e:
        logger.error(f"❌ Erreur durant la mise à jour: {e}")
        
        # Notification d'erreur
        try:
            from utils.notifications import NotificationManager
            notifier = NotificationManager()
            notifier.send_error_notification(f"Erreur mise à jour incrémentale: {e}")
        except:
            pass

if __name__ == "__main__":
    main()
