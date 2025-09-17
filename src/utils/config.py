"""
⚙️ Module de gestion de la configuration
"""

import os
import json
from pathlib import Path
from typing import Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)

class ConfigManager:
    """Gestionnaire de configuration"""
    
    def __init__(self, config_file: str = "config/config.json"):
        self.config_file = Path(config_file)
        self.config_data = self._load_config()
    
    def _load_config(self) -> Dict[str, Any]:
        """Charge la configuration depuis le fichier JSON"""
        if self.config_file.exists():
            try:
                with open(self.config_file, 'r', encoding='utf-8') as f:
                    config = json.load(f)
                    
                # Remplacer les placeholders par les variables d'environnement
                config = self._replace_env_vars(config)
                return config
                
            except Exception as e:
                logger.warning(f"Erreur lecture config: {e}")
        
        # Configuration par défaut depuis les variables d'environnement
        return self._get_default_config()
    
    def _replace_env_vars(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Remplace les valeurs par les variables d'environnement"""
        if isinstance(config, dict):
            for key, value in config.items():
                if isinstance(value, dict):
                    config[key] = self._replace_env_vars(value)
                elif isinstance(value, str):
                    if "YOUR_PASSWORD_HERE" in value:
                        config[key] = os.getenv("DB_PASSWORD", value)
                    elif value.startswith('${') and value.endswith('}'):
                        env_var = value[2:-1]  # Enlever ${ et }
                        config[key] = os.getenv(env_var, value)
        return config
    
    def _get_default_config(self) -> Dict[str, Any]:
        """Configuration par défaut depuis les variables d'environnement"""
        return {
            "database": {
                "host": os.getenv("DB_HOST", "localhost"),
                "port": int(os.getenv("DB_PORT", "3306")),
                "user": os.getenv("DB_USER", "root"),
                "password": os.getenv("DB_PASSWORD", ""),
                "database": os.getenv("DB_NAME", "games_db"),
                "charset": "utf8mb4"
            },
            "api": {
                "rawg_api_key": os.getenv("RAWG_API_KEY", ""),
                "games_per_extraction": int(os.getenv("GAMES_PER_EXTRACTION", "500")),
                "page_size": int(os.getenv("PAGE_SIZE", "40")),
                "rate_limit_delay": float(os.getenv("RATE_LIMIT_DELAY", "1.0")),
                "max_retries": int(os.getenv("MAX_RETRIES", "3"))
            },
            "scraping": {
                "enabled": os.getenv("SCRAPING_ENABLED", "true").lower() == "true",
                "headless": os.getenv("HEADLESS_MODE", "true").lower() == "true",
                "max_games_per_session": int(os.getenv("MAX_GAMES_SCRAPING", "50")),
                "delay_between_requests": float(os.getenv("SCRAPING_DELAY", "2.0")),
                "retry_attempts": int(os.getenv("SCRAPING_RETRIES", "3")),
                "timeout": int(os.getenv("SCRAPING_TIMEOUT", "30"))
            },
            "notifications": {
                "discord_webhook": os.getenv("DISCORD_WEBHOOK", ""),
                "email_enabled": os.getenv("EMAIL_ENABLED", "false").lower() == "true",
                "smtp_server": os.getenv("SMTP_SERVER", "smtp.gmail.com"),
                "smtp_port": int(os.getenv("SMTP_PORT", "587")),
                "smtp_user": os.getenv("SMTP_USER", ""),
                "smtp_password": os.getenv("SMTP_PASSWORD", ""),
                "recipients": [email.strip() for email in os.getenv("NOTIFICATION_EMAIL", "").split(",") if email.strip()]
            },
            "logging": {
                "level": os.getenv("LOG_LEVEL", "INFO"),
                "log_file": os.getenv("LOG_FILE", "logs/extraction.log"),
                "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                "max_log_files": int(os.getenv("MAX_LOG_FILES", "7"))
            },
            "maintenance": {
                "cleanup_days": int(os.getenv("CLEANUP_DAYS", "30")),
                "backup_retention_days": int(os.getenv("BACKUP_RETENTION_DAYS", "7"))
            }
        }
    
    def get_database_config(self) -> Dict[str, Any]:
        """Récupère la configuration de base de données"""
        return self.config_data.get("database", {})
    
    def get_api_config(self) -> Dict[str, Any]:
        """Récupère la configuration API"""
        return self.config_data.get("api", {})
    
    def get_scraping_config(self) -> Dict[str, Any]:
        """Récupère la configuration scraping"""
        return self.config_data.get("scraping", {})
    
    def get_notifications_config(self) -> Dict[str, Any]:
        """Récupère la configuration des notifications"""
        return self.config_data.get("notifications", {})
    
    def get_logging_config(self) -> Dict[str, Any]:
        """Récupère la configuration logging"""
        return self.config_data.get("logging", {})
    
    def get_maintenance_config(self) -> Dict[str, Any]:
        """Récupère la configuration de maintenance"""
        return self.config_data.get("maintenance", {})
