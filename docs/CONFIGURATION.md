# 🔧 Configuration Avancée

## Structure de Configuration

Le système utilise plusieurs niveaux de configuration :

1. **Variables d'environnement** (.env)
2. **Fichier JSON** (config/config.json)  
3. **Secrets GitHub** (pour le déploiement)

## Configuration Complète

### config/config.json
```json
{
  "database": {
    "host": "mysql-anne.alwaysdata.net",
    "port": 3306,
    "user": "anne",
    "password": "SERA_REMPLACE_PAR_ENV",
    "database": "Vicky2@18",
    "charset": "utf8mb4"
  },
  "api": {
    "rawg_api_key": "a596903618f14aeeb1fcbbb790180dd5",
    "games_per_extraction": 500,
    "page_size": 40,
    "rate_limit_delay": 1,
    "max_retries": 3
  },
  "scraping": {
    "enabled": true,
    "max_games_per_session": 50,
    "delay_between_requests": 2,
    "retry_attempts": 3,
    "headless": true,
    "timeout": 30
  },
  "scheduling": {
    "games_extraction_hours": ["06:00", "18:00"],
    "prices_scraping_hours": ["12:00"],
    "maintenance_hours": ["02:00"],
    "timezone": "Europe/Paris"
  },
  "notifications": {
    "discord_webhook": "https://discordapp.com/api/webhooks/1417424556783697950/mQR0dloyGcQr27snqvFEhKntCoFO1aLXPBKiMqZkpy_NieDq9ve2uPLO_sYYcqw7vOAc",
    "email_enabled": true,
    "smtp_server": "smtp.gmail.com",
    "smtp_port": 587,
    "recipients": ["vicky69200@gmail.com"]
  },
  "logging": {
    "level": "INFO",
    "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    "max_log_files": 7,
    "log_file": "logs/extraction.log"
  }
}
```

## Personnalisation

### Fréquence d'extraction
Modifiez les cron dans `.github/workflows/scheduled-extraction.yml` :

```yaml
on:
  schedule:
    - cron: '0 6 * * *'   # Tous les jours à 8h Paris
    - cron: '0 18 * * *'  # Tous les jours à 20h Paris  
    - cron: '0 12 * * *'  # Tous les jours à 14h Paris
```

### Limites d'extraction
Dans `config/config.json` :

```json
{
  "api": {
    "games_per_extraction": 1000,  // Plus de jeux par session
    "page_size": 40,               // Taille des pages RAWG
    "rate_limit_delay": 0.5        // Délai plus court
  }
}
```
