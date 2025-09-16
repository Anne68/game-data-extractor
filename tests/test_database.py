"""
Tests pour le module de base de données
"""

import pytest
from unittest.mock import Mock, patch
import pandas as pd
from src.extractor.database import DatabaseManager

class TestDatabaseManager:
    
    @patch('src.extractor.database.mysql.connector.connect')
    def test_connection_success(self, mock_connect):
        """Test de connexion réussie"""
        mock_conn = Mock()
        mock_conn.is_connected.return_value = True
        mock_connect.return_value = mock_conn
        
        with patch('src.utils.config.ConfigManager') as mock_config_class:
            mock_config = mock_config_class.return_value
            mock_config.get_database_config.return_value = {
                'host': 'localhost',
                'user': 'test',
                'password': 'test',
                'database': 'test_db'
            }
            
            db = DatabaseManager()
            assert db.test_connection() == True
    
    @patch('src.extractor.database.mysql.connector.connect')
    def test_connection_failure(self, mock_connect):
        """Test d'échec de connexion"""
        mock_connect.side_effect = Exception("Connection failed")
        
        with patch('src.utils.config.ConfigManager') as mock_config_class:
            mock_config = mock_config_class.return_value
            mock_config.get_database_config.return_value = {
                'host': 'localhost',
                'user': 'test',
                'password': 'test',
                'database': 'test_db'
            }
            
            db = DatabaseManager()
            assert db.test_connection() == False
    
    @patch('src.extractor.database.DatabaseManager.get_connection')
    def test_save_games_empty_dataframe(self, mock_get_conn):
        """Test de sauvegarde avec DataFrame vide"""
        mock_get_conn.return_value = None
        
        with patch('src.utils.config.ConfigManager'):
            db = DatabaseManager()
            result = db.save_games(pd.DataFrame())
            
            assert result == True  # Devrait retourner True pour DataFrame vide
