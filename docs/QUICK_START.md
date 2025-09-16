# 🚀 Guide de Démarrage Rapide

## Prérequis

- Compte GitHub (gratuit)
- Compte AlwaysData avec base MySQL
- Clé API RAWG (gratuite sur rawg.io)

## Installation en 5 minutes

### 1. Fork ce repository
```bash
# Via l'interface GitHub ou :
gh repo fork Anne68/game-data-extractor
```

### 2. Configurer les secrets GitHub
Dans votre fork > **Settings > Secrets and variables > Actions** :

- `ALWAYSDATA_HOST` : ssh-votre-compte.alwaysdata.net
- `ALWAYSDATA_USERNAME` : votre-username  
- `ALWAYSDATA_SSH_KEY` : votre clé SSH privée
- `DB_HOST` : mysql-votre-compte.alwaysdata.net
- `DB_USER` : votre-db-user
- `DB_PASSWORD` : votre-mot-de-passe
- `DB_NAME` : votre-games-db
- `RAWG_API_KEY` : votre-cle-api-rawg

### 3. Premier déploiement
```bash
git clone https://github.com/VOTRE-USERNAME/game-data-extractor.git
cd game-data-extractor
git add .
git commit -m "🚀 Initial setup"
git push origin main
```

**Le déploiement se lance automatiquement !**

### 4. Vérifier le déploiement
- Aller dans **Actions** sur GitHub
- Vérifier que le workflow "Deploy to AlwaysData" est ✅
- Les extractions se lancent automatiquement selon la planification

## Première extraction manuelle
```bash
# Via GitHub Actions
Actions > Scheduled Data Extraction > Run workflow > games

# Ou via SSH sur AlwaysData
ssh votre-username@ssh-votre-compte.alwaysdata.net
cd ~/game-extraction  
python3 scripts/run_extraction.py --games-only --limit 10
```

## Félicitations ! 🎉

Votre système d'extraction est opérationnel !
