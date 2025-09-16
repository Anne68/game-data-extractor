#!/usr/bin/env python3
"""
🚀 Script de setup automatique pour AlwaysData
Configure l'environnement, la base de données et les dépendances
"""

import os
import sys
import subprocess
import argparse
import json
from pathlib import Path

def run_command(cmd, check=True):
    """Exécute une commande système"""
    print(f"🔧 Exécution: {cmd}")
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if check and result.returncode != 0:
        print(f"❌ Erreur: {result.stderr}")
        sys.exit(1)
    return result

def setup_environment():
    """Configure l'environnement Python"""
    print("🐍 Configuration de l'environnement Python...")
    
    # Installer les dépendances
    run_command("python3 -m pip install --user --upgrade pip")
    run_command("python3 -m pip install --user -r requirements.txt")
    
    # Créer les répertoires nécessaires
    os.makedirs("logs", exist_ok=True)
    os.makedirs("data", exist_ok=True)
    os.makedirs("config", exist_ok=True)
    
    print("✅ Environnement configuré")

def setup_database():
    """Configure la base de données"""
    print("🗄️ Configuration de la base de données...")
    
    try:
        import mysql.connector
        from src.extractor.database import DatabaseManager
        
        db = DatabaseManager()
        db.setup_tables()
        print("✅ Base de données configurée")
    except Exception as e:
        print(f"❌ Erreur de configuration DB: {e}")
        return False
    return True

def setup_cron():
    """Configure les tâches cron"""
    print("⏰ Configuration des tâches cron...")
    
    cron_jobs = [
        "0 6 * * * cd ~/game-extraction && python3 scripts/run_extraction.py --games-only",
        "0 18 * * * cd ~/game-extraction && python3 scripts/run_extraction.py --games-only", 
        "0 12 * * * cd ~/game-extraction && python3 scripts/run_extraction.py --prices-only",
        "0 2 * * 0 cd ~/game-extraction && python3 scripts/run_extraction.py --maintenance"
    ]
    
    # Écrire le fichier crontab
    with open("/tmp/game_extraction_cron", "w") as f:
        for job in cron_jobs:
            f.write(f"{job}\n")
    
    run_command("crontab /tmp/game_extraction_cron")
    run_command("rm /tmp/game_extraction_cron")
    
    print("✅ Tâches cron configurées")

def main():
    parser = argparse.ArgumentParser(description="Setup AlwaysData environment")
    parser.add_argument("--auto-deploy", action="store_true", 
                       help="Déploiement automatique complet")
    parser.add_argument("--skip-cron", action="store_true",
                       help="Ignorer la configuration cron")
    
    args = parser.parse_args()
    
    print("🚀 Début du setup AlwaysData...")
    
    # Setup étape par étape
    setup_environment()
    
    if setup_database():
        print("✅ Base de données OK")
    else:
        print("⚠️ Problème avec la base de données")
    
    if not args.skip_cron:
        setup_cron()
    
    print("🎉 Setup terminé avec succès !")

if __name__ == "__main__":
    main()
