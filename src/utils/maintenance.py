
"""
🧹 Module de maintenance du système
"""

import os
import sys
import logging
import shutil
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any

sys.path.insert(0, str(Path(__file__).parent.parent))

logger = logging.getLogger(__name__)

class MaintenanceManager:
    """Gestionnaire des tâches de maintenance"""
    
    def __init__(self):
        self.log_dir = Path("logs")
        self.data_dir = Path("data")
        self.backup_dir = Path("backups")
        
        # Créer les répertoires s'ils n'existent pas
        self.log_dir.mkdir(exist_ok=True)
        self.data_dir.mkdir(exist_ok=True)
        self.backup_dir.mkdir(exist_ok=True)
    
    def cleanup_old_logs(self, days: int = 7) -> Dict[str, Any]:
        """Nettoie les anciens fichiers de log"""
        logger.info(f"Nettoyage des logs plus anciens que {days} jours")
        
        cutoff_date = datetime.now() - timedelta(days=days)
        files_removed = 0
        space_freed = 0
        
        try:
            for log_file in self.log_dir.glob("*.log*"):
                if log_file.is_file():
                    file_modified = datetime.fromtimestamp(log_file.stat().st_mtime)
                    
                    if file_modified < cutoff_date:
                        file_size = log_file.stat().st_size
                        log_file.unlink()
                        files_removed += 1
                        space_freed += file_size
        
        except Exception as e:
            logger.error(f"Erreur nettoyage logs: {e}")
        
        result = {
            'files_removed': files_removed,
            'space_freed_mb': round(space_freed / 1024 / 1024, 2)
        }
        
        logger.info(f"Nettoyage terminé: {files_removed} fichiers, {result['space_freed_mb']} MB")
        return result
    
    def cleanup_old_data(self, days: int = 30) -> Dict[str, Any]:
        """Nettoie les anciennes données temporaires"""
        logger.info(f"Nettoyage des données temporaires plus anciennes que {days} jours")
        
        cutoff_date = datetime.now() - timedelta(days=days)
        files_removed = 0
        space_freed = 0
        
        try:
            for data_file in self.data_dir.glob("*"):
                if data_file.is_file():
                    file_modified = datetime.fromtimestamp(data_file.stat().st_mtime)
                    
                    if file_modified < cutoff_date:
                        file_size = data_file.stat().st_size
                        data_file.unlink()
                        files_removed += 1
                        space_freed += file_size
        
        except Exception as e:
            logger.error(f"Erreur nettoyage données: {e}")
        
        result = {
            'files_removed': files_removed,
            'space_freed_mb': round(space_freed / 1024 / 1024, 2)
        }
        
        return result
EOF

echo "✅ Modules Python mis à jour avec succès"

# 5. Rendre les scripts exécutables
echo "🔧 Configuration des permissions..."
chmod +x scripts/*.py
chmod +x scripts/*.sh 2>/dev/null || true

# 6. Créer un fichier de variables d'environnement pour les tests
echo "⚙️ Création du fichier d'environnement de test..."
cat > .env.test << 'EOF'
# Variables d'environnement pour les tests locaux
DB_HOST=mysql-anne.alwaysdata.net
DB_USER=anne
DB_PASSWORD=Vicky2@18
DB_NAME=anne_games_db
RAWG_API_KEY=a596903618f14aeeb1fcbbb790180dd5
LOG_LEVEL=INFO
HEADLESS_MODE=true
SCRAPING_ENABLED=false
DISCORD_WEBHOOK=https://discordapp.com/api/webhooks/1417424556783697950/mQR0dloyGcQr27snqvFEhKntCoFO1aLXPBKiMqZkpy_NieDq9ve2uPLO_sYYcqw7vOAc
NOTIFICATION_EMAIL=vicky69200@gmail.com
EOF

echo "🧪 Test rapide des imports..."
python3 -c "
import sys
sys.path.insert(0, 'src')
try:
    from extractor.database import DatabaseManager
    from extractor.rawg_extractor import RawgExtractor
    from extractor.price_scraper import PriceScraper
    from utils.config import ConfigManager
    from utils.notifications import NotificationManager
    print('✅ Tous les imports fonctionnent correctement')
except ImportError as e:
    print(f'❌ Erreur import: {e}')
    sys.exit(1)
"

echo ""
echo "🎉 Correction terminée avec succès !"
echo ""
echo "📋 Actions effectuées:"
echo "• ✅ Modules Python corrigés avec gestion des imports"
echo "• ✅ Répertoires manquants créés"
echo "• ✅ Scripts rendus exécutables"
echo "• ✅ Fichier .env.test créé"
echo ""
echo "🚀 Vous pouvez maintenant tester avec:"
echo "   python3 scripts/quick_test.py"
echo "   python3 scripts/run_extraction.py --games-only --limit 10"
