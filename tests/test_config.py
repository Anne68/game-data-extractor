"""
Tests pour le module de configuration
"""

import pytest
import os
from unittest.mock import patch, mock_open
from src.utils.config import ConfigManager

class TestConfigManager:
    
    @patch.dict(os.environ, {
        'DB_HOST': 'test-host',
        'DB_USER': 'test-user',
        'RAWG_API_KEY': 'test-key'
    })
    def test_config_from_env_vars(self):
        """Test de chargement depuis les variables d'environnement"""
        config = ConfigManager()
        
        db_config = config.get_database_config()
        api_config = config.get_api_config()
        
        assert db_config['host'] == 'test-host'
        assert db_config['user'] == 'test-user'
        assert api_config['rawg_api_key'] == 'test-key'
    
    @patch("pathlib.Path.exists")
    @patch("builtins.open", new_callable=mock_open, read_data='{"database": {"host": "json-host"}}')
    def test_config_from_json_file(self, mock_file, mock_exists):
        """Test de chargement depuis un fichier JSON"""
        mock_exists.return_value = True
        
        config = ConfigManager()
        db_config = config.get_database_config()
        
        assert db_config['host'] == 'json-host'
    
    def test_default_values(self):
        """Test des valeurs par défaut"""
        config = ConfigManager()
        
        api_config = config.get_api_config()
        scraping_config = config.get_scraping_config()
        
        assert api_config['page_size'] == 40
        assert scraping_config['enabled'] == True
