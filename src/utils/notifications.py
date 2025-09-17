
"""
📢 Module de gestion des notifications
"""

import os
import sys
import logging
from datetime import datetime
from typing import Dict, Any, Optional
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

logger = logging.getLogger(__name__)

try:
    import requests
    REQUESTS_AVAILABLE = True
except ImportError:
    REQUESTS_AVAILABLE = False

class NotificationManager:
    """Gestionnaire de notifications"""
    
    def __init__(self):
        try:
            from utils.config import ConfigManager
            config = ConfigManager()
            notifications_config = config.get_notifications_config()
        except ImportError:
            notifications_config = self._get_config_from_env()
        
        self.discord_webhook = notifications_config.get('discord_webhook')
        self.email_enabled = notifications_config.get('email_enabled', False)
        self.recipients = notifications_config.get('recipients', [])
    
    def _get_config_from_env(self) -> Dict[str, Any]:
        """Récupère la configuration depuis les variables d'environnement"""
        return {
            'discord_webhook': os.getenv('DISCORD_WEBHOOK'),
            'email_enabled': os.getenv('EMAIL_ENABLED', 'false').lower() == 'true',
            'recipients': os.getenv('NOTIFICATION_EMAIL', '').split(',') if os.getenv('NOTIFICATION_EMAIL') else []
        }
    
    def send_notification(self, message: str, level: str = "info", title: str = None) -> bool:
        """Envoie une notification"""
        success = True
        
        if self.discord_webhook and REQUESTS_AVAILABLE:
            success &= self._send_discord_notification(message, level, title)
        
        return success
    
    def _send_discord_notification(self, message: str, level: str, title: str = None) -> bool:
        """Envoie une notification Discord"""
        try:
            emoji_map = {
                'info': '📊', 'success': '✅', 'warning': '⚠️', 'error': '❌', 'debug': '🔍'
            }
            
            emoji = emoji_map.get(level, '📢')
            
            if not title:
                title = f"{emoji} Game Data Extractor - {level.title()}"
            
            payload = {
                "embeds": [{
                    "title": title,
                    "description": message,
                    "timestamp": datetime.now().isoformat(),
                    "footer": {"text": "Game Data Extractor"}
                }]
            }
            
            response = requests.post(self.discord_webhook, json=payload, timeout=10)
            response.raise_for_status()
            
            logger.info("Notification Discord envoyée")
            return True
            
        except Exception as e:
            logger.error(f"Erreur notification Discord: {e}")
            return False
