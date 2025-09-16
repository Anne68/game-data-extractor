#!/usr/bin/env python3
"""
🎮 Script principal d'extraction de données
Gère l'extraction des jeux et des prix selon les options spécifiées
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

def run_games_extraction(logger, limit=500):
    """Exécute l'extraction des jeux"""
    logger.info(f"🎮 Début extraction des jeux (limite: {limit})")
    
    try:
        from extractor.rawg_extractor import RawgExtractor
        from extractor.database import DatabaseManager
        
        extractor = RawgExtractor()
        db = DatabaseManager()
        
        # Extraction des jeux
        games_data = extractor.fetch_games(limit=limit)
        
        if not games_data.empty:
            db.save_games(games_data)
            logger.info(f"✅ {len(games_data)} jeux extraits et sauvegardés")
            return True
        else:
            logger.warning("⚠️ Aucun nouveau jeu à extraire")
            return False
            
    except Exception as e:
        logger.error(f"❌ Erreur extraction jeux: {e}")
        return False

def run_prices_scraping(logger, limit=50):
    """Exécute le scraping des prix"""
    logger.info(f"💰 Début scraping des prix (limite: {limit})")
    
    try:
        from extractor.price_scraper import PriceScraper
        from extractor.database import DatabaseManager
        
        scraper = PriceScraper()
        db = DatabaseManager()
        
        # Récupérer les jeux à analyser
        games_to_scrape = db.get_games_for_price_update(limit=limit)
        
        if games_to_scrape.empty:
            logger.info("ℹ️ Aucun jeu à analyser pour les prix")
            return True
        
        # Scraping des prix
        prices_data = scraper.scrape_prices(games_to_scrape)
        
        if not prices_data.empty:
            db.save_prices(prices_data)
            logger.info(f"✅ {len(prices_data)} prix mis à jour")
            return True
        else:
            logger.warning("⚠️ Aucun prix récupéré")
            return False
            
    except Exception as e:
        logger.error(f"❌ Erreur scraping prix: {e}")
        return False

def run_maintenance(logger):
    """Exécute les tâches de maintenance"""
    logger.info("🧹 Début de la maintenance")
    
    try:
        from extractor.database import DatabaseManager
        from utils.maintenance import MaintenanceManager
        
        db = DatabaseManager()
        maintenance = MaintenanceManager()
        
        # Nettoyage des logs anciens
        maintenance.cleanup_old_logs(days=7)
        
        # Nettoyage des données obsolètes
        maintenance.cleanup_old_data(days=30)
        
        # Optimisation de la base de données
        db.optimize_database()
        
        # Statistiques
        stats = db.get_stats()
        logger.info(f"📊 Statistiques: {stats['total_games']} jeux, {stats['total_prices']} prix")
        
        logger.info("✅ Maintenance terminée")
        return True
        
    except Exception as e:
        logger.error(f"❌ Erreur maintenance: {e}")
        return False

def send_notification(logger, message, success=True):
    """Envoie une notification"""
    try:
        from utils.notifications import NotificationManager
        
        notifier = NotificationManager()
        notifier.send_notification(
            message=message,
            level="info" if success else "error"
        )
    except Exception as e:
        logger.warning(f"⚠️ Erreur notification: {e}")

def main():
    parser = argparse.ArgumentParser(description="Game Data Extraction Runner")
    parser.add_argument("--games-only", action="store_true", 
                       help="Extraire uniquement les jeux")
    parser.add_argument("--prices-only", action="store_true",
                       help="Scraper uniquement les prix")
    parser.add_argument("--maintenance", action="store_true",
                       help="Exécuter la maintenance")
    parser.add_argument("--full", action="store_true",
                       help="Extraction complète (jeux + prix)")
    parser.add_argument("--test", action="store_true",
                       help="Mode test avec limites réduites")
    parser.add_argument("--status", action="store_true",
                       help="Afficher le statut du système")
    parser.add_argument("--limit", type=int, default=500,
                       help="Limite du nombre d'éléments à traiter")
    parser.add_argument("--log-level", default="INFO",
                       choices=["DEBUG", "INFO", "WARNING", "ERROR"],
                       help="Niveau de logging")
    
    args = parser.parse_args()
    
    # Configuration du logging
    logger = setup_logging(args.log_level)
    
    # Mode test : limites réduites
    if args.test:
        args.limit = min(args.limit, 10)
        logger.info("🧪 Mode test activé (limites réduites)")
    
    # Début de l'exécution
    start_time = time.time()
    logger.info("🚀 Début de l'exécution du script d'extraction")
    
    success = True
    operations = []
    
    try:
        # Affichage du statut
        if args.status:
            from extractor.database import DatabaseManager
            db = DatabaseManager()
            stats = db.get_detailed_stats()
            
            print("📊 === STATUT DU SYSTÈME ===")
            print(f"🎮 Total jeux: {stats.get('total_games', 0):,}")
            print(f"💰 Total prix: {stats.get('total_prices', 0):,}")
            print(f"📅 Dernière extraction: {stats.get('last_extraction', 'N/A')}")
            print(f"💾 Taille DB: {stats.get('db_size_mb', 0):.1f} MB")
            print(f"📁 Logs: {stats.get('log_files', 0)} fichiers")
            return
        
        # Extraction des jeux
        if args.games_only or args.full:
            if run_games_extraction(logger, args.limit):
                operations.append("✅ Extraction jeux")
            else:
                operations.append("❌ Extraction jeux")
                success = False
        
        # Scraping des prix
        if args.prices_only or args.full:
            if run_prices_scraping(logger, args.limit):
                operations.append("✅ Scraping prix")
            else:
                operations.append("❌ Scraping prix")
                success = False
        
        # Maintenance
        if args.maintenance:
            if run_maintenance(logger):
                operations.append("✅ Maintenance")
            else:
                operations.append("❌ Maintenance")
                success = False
        
        # Si aucune option spécifiée, faire extraction jeux par défaut
        if not any([args.games_only, args.prices_only, args.maintenance, args.full]):
            logger.info("ℹ️ Aucune option spécifiée, extraction des jeux par défaut")
            if run_games_extraction(logger, args.limit):
                operations.append("✅ Extraction jeux (défaut)")
            else:
                operations.append("❌ Extraction jeux (défaut)")
                success = False
        
        # Calcul du temps d'exécution
        execution_time = time.time() - start_time
        
        # Message final
        status_msg = "succès" if success else "erreur"
        message = f"🎯 Extraction terminée avec {status_msg} en {execution_time:.1f}s"
        
        if operations:
            message += f"\n📋 Opérations: {', '.join(operations)}"
        
        logger.info(message)
        
        # Notification
        send_notification(logger, message, success)
        
        # Code de sortie
        sys.exit(0 if success else 1)
        
    except KeyboardInterrupt:
        logger.info("⏹️ Interruption par l'utilisateur")
        sys.exit(1)
    except Exception as e:
        logger.error(f"❌ Erreur fatale: {e}")
        send_notification(logger, f"❌ Erreur fatale: {e}", False)
        sys.exit(1)

if __name__ == "__main__":
    main()
