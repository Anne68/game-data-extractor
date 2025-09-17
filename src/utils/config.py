"""
📢 Module de gestion des notifications
"""

import os
import logging
from datetime import datetime
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)

try:
    import requests
    REQUESTS_AVAILABLE = True
except ImportError:
    REQUESTS_AVAILABLE = False
    logger.warning("Module requests non disponible")

class NotificationManager:
    """Gestionnaire de notifications"""
    
    def __init__(self):
        # Configuration directe depuis les variables d'environnement pour éviter les imports circulaires
        self.discord_webhook = os.getenv('DISCORD_WEBHOOK')
        self.email_enabled = os.getenv('EMAIL_ENABLED', 'false').lower() == 'true'
        recipients_str = os.getenv('NOTIFICATION_EMAIL', '')
        self.recipients = [email.strip() for email in recipients_str.split(',') if email.strip()]
    
    def send_notification(self, message: str, level: str = "info", title: str = None) -> bool:
        """Envoie une notification via Discord et/ou email"""
        success = True
        
        # Notification Discord
        if self.discord_webhook and REQUESTS_AVAILABLE:
            success &= self._send_discord_notification(message, level, title)
        else:
            # Log local si Discord non disponible
            logger.info(f"Notification {level}: {message}")
        
        return success
    
    def _send_discord_notification(self, message: str, level: str, title: str = None) -> bool:
        """Envoie une notification Discord"""
        try:
            # Emoji selon le niveau
            emoji_map = {
                'info': '📊',
                'success': '✅',
                'warning': '⚠️',
                'error': '❌',
                'debug': '🔍'
            }
            
            emoji = emoji_map.get(level, '📢')
            
            # Couleur selon le niveau
            color_map = {
                'info': 3447003,      # Bleu
                'success': 65280,     # Vert
                'warning': 16776960,  # Jaune
                'error': 16711680,    # Rouge
                'debug': 8421504      # Gris
            }
            
            color = color_map.get(level, 3447003)
            
            # Titre par défaut
            if not title:
                title = f"{emoji} Game Data Extractor - {level.title()}"
            
            # Payload Discord
            payload = {
                "embeds": [{
                    "title": title,
                    "description": message,
                    "color": color,
                    "timestamp": datetime.now().isoformat(),
                    "footer": {
                        "text": "Game Data Extractor"
                    }
                }]
            }
            
            response = requests.post(
                self.discord_webhook,
                json=payload,
                timeout=10
            )
            response.raise_for_status()
            
            logger.info("Notification Discord envoyée avec succès")
            return True
            
        except Exception as e:
            logger.error(f"Erreur envoi notification Discord: {e}")
            return False
    
    def send_start_notification(self):
        """Notification de début d'extraction"""
        message = "🚀 Début de l'extraction automatique de données"
        self.send_notification(message, "info", "Extraction Started")
    
    def send_success_notification(self, stats: Dict[str, Any]):
        """Notification de succès avec statistiques"""
        message = f"""✅ Extraction terminée avec succès !
        
📊 Statistiques:
• Jeux extraits: {stats.get('games_extracted', 0)}
• Prix scrapés: {stats.get('prices_scraped', 0)}
• Temps d'exécution: {stats.get('execution_time', 'N/A')}
• Total jeux DB: {stats.get('total_games', 0)}
• Total prix DB: {stats.get('total_prices', 0)}
        """
        self.send_notification(message, "success", "Extraction Completed")
    
    def send_error_notification(self, error: str):
        """Notification d'erreur"""
        message = f"""❌ Erreur lors de l'extraction !
        
Erreur: {error}
Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
        
Veuillez vérifier les logs pour plus de détails.
        """
        self.send_notification(message, "error", "Extraction Failed")
    
    def send_warning_notification(self, warning: str):
        """Notification d'avertissement"""
        message = f"""⚠️ Avertissement durant l'extraction
        
Avertissement: {warning}
Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
        """
        self.send_notification(message, "warning", "Extraction Warning")
