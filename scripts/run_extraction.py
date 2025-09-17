#!/usr/bin/env python3
"""
Script principal d'extraction avec support de mise à jour incrémentale
"""

import os
import sys
import argparse
import logging
import time
from datetime import datetime
from pathlib import Path

# Ajouter le répertoire src au PYTHONPATH
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

def setup_logging(level="INFO"):
    """Configure le système de logging"""
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    
    logging.basicConfig(
        level=getattr(logging, level),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_dir / "extraction.log"),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

def run_incremental_update(logger, games_to_add=100):
    """Exécute une mise à jour incrémentale complète"""
    logger.info(f"Démarrage mise à jour incrémentale: {games_to_add} nouveaux jeux")
    
    try:
        # Import du script de mise à jour incrémentale
        import subprocess
        
        # Lancer le script de mise à jour incrémentale
        result = subprocess.run([
            sys.executable, 
            str(Path(__file__).parent / "incremental_update.py")
        ], capture_output=True, text=True)
        
        if result.returncode == 0:
            logger.info("✅ Mise à jour incrémentale terminée avec succès")
            logger.info(result.stdout)
            return True
        else:
            logger.error(f"❌ Erreur mise à jour incrémentale: {result.stderr}")
            return False
            
    except Exception as e:
        logger.error(f"❌ Erreur lancement mise à jour incrémentale: {e}")
        return False

def get_database_state(logger):
    """Récupère l'état actuel de la base de données"""
    try:
        from extractor.database import DatabaseManager
        db = DatabaseManager()
        
        conn = db.get_connection()
        if not conn:
            return None
            
        cursor = conn.cursor()
        
        # Statistiques générales
        stats = {}
        
        # Total jeux
        cursor.execute("SELECT COUNT(*) FROM games")
        stats['total_games'] = cursor.fetchone()[0]
        
        # Total prix
        cursor.execute("SELECT COUNT(*) FROM game_prices")
        stats['total_prices'] = cursor.fetchone()[0]
        
        # Dernière page API
        cursor.execute("SELECT last_page FROM api_state WHERE id = 1")
        result = cursor.fetchone()
        stats['last_api_page'] = result[0] if result else 0
        
        # Jeux sans prix
        cursor.execute("""
            SELECT COUNT(DISTINCT g.game_id_rawg)
            FROM games g
            LEFT JOIN game_prices p ON g.game_id_rawg = p.game_id_rawg
            WHERE p.game_id_rawg IS NULL
        """)
        stats['games_without_prices'] = cursor.fetchone()[0]
        
        # Dernière extraction
        cursor.execute("SELECT MAX(last_update) FROM games")
        last_update = cursor.fetchone()[0]
        stats['last_extraction'] = last_update.strftime('%Y-%m-%d %H:%M:%S') if last_update else None
        
        conn.close()
        return stats
        
    except Exception as e:
        logger.error(f"Erreur récupération état BDD: {e}")
        return None

def main():
    parser = argparse.ArgumentParser(description="Game Data Extraction Runner - Enhanced with Incremental Updates")
    
    # Options existantes
    parser.add_argument("--games-only", action="store_true", 
                       help="Extraire uniquement les jeux (sans prix)")
    parser.add_argument("--prices-only", action="store_true",
                       help="Scraper uniquement les prix")
    parser.add_argument("--maintenance", action="store_true",
                       help="Exécuter la maintenance")
    parser.add_argument("--full", action="store_true",
                       help="Extraction complète (jeux + prix automatique)")
    parser.add_argument("--auto", action="store_true",
                       help="Mode automatique: jeux puis prix")
    
    # Nouvelle option pour la mise à jour incrémentale
    parser.add_argument("--incremental", action="store_true",
                       help="Mise à jour incrémentale (mise à jour + 100 nouveaux jeux + prix)")
    parser.add_argument("--update", action="store_true",
                       help="Alias pour --incremental")
    
    parser.add_argument("--status", action="store_true",
                       help="Afficher le statut détaillé du système")
    parser.add_argument("--test", action="store_true",
                       help="Mode test avec limites réduites")
    parser.add_argument("--games-limit", type=int, default=500,
                       help="Limite du nombre de jeux à extraire")
    parser.add_argument("--prices-limit", type=int, default=50,
                       help="Limite du nombre de jeux pour le scraping prix")
    parser.add_argument("--log-level", default="INFO",
                       choices=["DEBUG", "INFO", "WARNING", "ERROR"],
                       help="Niveau de logging")
    
    args = parser.parse_args()
    
    # Configuration du logging
    logger = setup_logging(args.log_level)
    
    # Mode test : limites réduites
    if args.test:
        args.games_limit = min(args.games_limit, 10)
        args.prices_limit = min(args.prices_limit, 5)
        logger.info("Mode test activé (limites réduites)")
    
    # Début de l'exécution
    start_time = time.time()
    logger.info("Début de l'exécution du script d'extraction")
    
    success = True
    operations = []
    
    try:
        # Affichage du statut détaillé
        if args.status:
            db_state = get_database_state(logger)
            
            if db_state:
                print("📊 === STATUT DÉTAILLÉ DU SYSTÈME ===")
                print(f"🎮 Total jeux: {db_state['total_games']:,}")
                print(f"💰 Total prix: {db_state['total_prices']:,}")
                print(f"📄 Dernière page API: {db_state['last_api_page']}")
                print(f"🎯 Jeux sans prix: {db_state['games_without_prices']:,}")
                print(f"📅 Dernière extraction: {db_state['last_extraction']}")
                print(f"💾 Ratio prix/jeux: {db_state['total_prices']/max(db_state['total_games'],1):.1f}")
                
                # Recommandations
                print("\n💡 === RECOMMANDATIONS ===")
                if db_state['games_without_prices'] > 50:
                    print(f"🔍 Recommandé: Scraping prix ({db_state['games_without_prices']} jeux sans prix)")
                
                if db_state['total_games'] < 1000:
                    print("📈 Recommandé: Extraction incrémentale pour enrichir la base")
                    
                print("\n🚀 === COMMANDES SUGGESTIONS ===")
                print("• python3 scripts/run_extraction.py --incremental")
                print("• python3 scripts/run_extraction.py --prices-only")
            else:
                print("❌ Impossible de récupérer le statut")
            return
        
        # Mise à jour incrémentale
        if args.incremental or args.update:
            logger.info("🔄 Mode mise à jour incrémentale")
            if run_incremental_update(logger, args.games_limit):
                operations.append("✅ Mise à jour incrémentale")
            else:
                operations.append("❌ Mise à jour incrémentale")
                success = False
        
        # Autres modes (code existant...)
        elif args.games_only:
            # Code existant pour games-only
            from extractor.rawg_extractor import RawgExtractor
            from extractor.database import DatabaseManager
            
            extractor = RawgExtractor()
            db = DatabaseManager()
            
            games_data = extractor.fetch_games(limit=args.games_limit)
            if not games_data.empty:
                db.save_games(games_data)
                operations.append(f"✅ Extraction jeux ({len(games_data)})")
            else:
                operations.append("❌ Extraction jeux")
                success = False
        
        elif args.prices_only:
            # Code existant pour prices-only
            from extractor.price_scraper import PriceScraper
            from extractor.database import DatabaseManager
            
            scraper = PriceScraper()
            db = DatabaseManager()
            
            games_to_scrape = db.get_games_for_price_update(limit=args.prices_limit)
            if not games_to_scrape.empty:
                prices_data = scraper.scrape_prices(games_to_scrape)
                if not prices_data.empty:
                    db.save_prices(prices_data)
                    operations.append(f"✅ Scraping prix ({len(prices_data)})")
                else:
                    operations.append("❌ Scraping prix")
                    success = False
            else:
                operations.append("ℹ️ Aucun jeu à analyser pour les prix")
        
        elif args.maintenance:
            # Code maintenance existant
            from utils.maintenance import MaintenanceManager
            maintenance = MaintenanceManager()
            
            results = maintenance.run_full_maintenance()
            if results.get('success', False):
                operations.append("✅ Maintenance")
            else:
                operations.append("❌ Maintenance")
                success = False
        
        else:
            # Mode par défaut : mise à jour incrémentale
            logger.info("Mode par défaut: mise à jour incrémentale")
            if run_incremental_update(logger, 100):
                operations.append("✅ Mise à jour incrémentale (défaut)")
            else:
                operations.append("❌ Mise à jour incrémentale (défaut)")
                success = False
        
        # Calcul du temps d'exécution
        execution_time = time.time() - start_time
        
        # Message final
        status_msg = "succès" if success else "erreur"
        message = f"Extraction terminée avec {status_msg} en {execution_time:.1f}s"
        
        if operations:
            message += f"\nOpérations: {', '.join(operations)}"
        
        logger.info(message)
        
        # Notification
        try:
            from utils.notifications import NotificationManager
            notifier = NotificationManager()
            notifier.send_notification(message, "success" if success else "error")
        except Exception as e:
            logger.warning(f"Erreur notification: {e}")
        
        # Code de sortie
        sys.exit(0 if success else 1)
        
    except KeyboardInterrupt:
        logger.info("Interruption par l'utilisateur")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Erreur fatale: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
