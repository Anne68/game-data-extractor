cat > src/utils/text_similarity.py << 'EOF'
"""
🔍 Module de similarité de texte avec TF-IDF
"""

import re
import logging
from typing import List, Tuple, Optional
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import numpy as np

logger = logging.getLogger(__name__)

class GameTitleMatcher:
    """Matcher de titres de jeux utilisant TF-IDF"""
    
    def __init__(self, similarity_threshold: float = 0.6):
        self.similarity_threshold = similarity_threshold
        self.vectorizer = TfidfVectorizer(
            lowercase=True,
            stop_words='english',  # Mots vides en anglais
            ngram_range=(1, 2),    # Unigrammes et bigrammes
            max_features=1000,     # Limiter le vocabulaire
            token_pattern=r'\b[a-zA-Z][a-zA-Z0-9]*\b'  # Mots alphanumériques
        )
        
    def normalize_title(self, title: str) -> str:
        """Normalise un titre de jeu pour la comparaison"""
        if not title:
            return ""
        
        # Convertir en minuscules
        normalized = title.lower()
        
        # Supprimer les caractères spéciaux et éditions
        normalized = re.sub(r'\b(goty|game of the year|ultimate|deluxe|premium|collector|special|limited|director\'s cut)\b', '', normalized)
        normalized = re.sub(r'\b(edition|version|remaster|remastered|hd|4k|enhanced|definitive)\b', '', normalized)
        normalized = re.sub(r'\b(pack|bundle|collection|anthology|trilogy|saga)\b', '', normalized)
        
        # Supprimer les numéros de version
        normalized = re.sub(r'\b(v\d+\.\d+|version\s+\d+)\b', '', normalized)
        
        # Supprimer les plateformes
        normalized = re.sub(r'\b(pc|ps4|ps5|xbox|nintendo|switch|steam)\b', '', normalized)
        
        # Supprimer l'année entre parenthèses
        normalized = re.sub(r'\(\d{4}\)', '', normalized)
        
        # Remplacer les caractères spéciaux par des espaces
        normalized = re.sub(r'[^\w\s]', ' ', normalized)
        
        # Nettoyer les espaces multiples
        normalized = re.sub(r'\s+', ' ', normalized).strip()
        
        return normalized
    
    def find_best_match(self, search_title: str, candidate_titles: List[str]) -> Tuple[Optional[int], float]:
        """
        Trouve le meilleur match parmi les candidats
        
        Args:
            search_title: Titre recherché
            candidate_titles: Liste des titres candidats
            
        Returns:
            Tuple (index du meilleur match, score de similarité)
        """
        if not search_title or not candidate_titles:
            return None, 0.0
        
        # Normaliser tous les titres
        normalized_search = self.normalize_title(search_title)
        normalized_candidates = [self.normalize_title(title) for title in candidate_titles]
        
        # Filtrer les titres vides
        valid_candidates = [(i, title) for i, title in enumerate(normalized_candidates) if title.strip()]
        
        if not valid_candidates:
            return None, 0.0
        
        try:
            # Préparer les textes pour TF-IDF
            all_texts = [normalized_search] + [title for _, title in valid_candidates]
            
            # Calculer les vecteurs TF-IDF
            tfidf_matrix = self.vectorizer.fit_transform(all_texts)
            
            # Calculer la similarité cosinus
            similarities = cosine_similarity(tfidf_matrix[0:1], tfidf_matrix[1:]).flatten()
            
            # Trouver le meilleur score
            best_idx = np.argmax(similarities)
            best_score = similarities[best_idx]
            
            # Récupérer l'index original
            original_idx = valid_candidates[best_idx][0]
            
            logger.debug(f"Recherche: '{search_title}' -> '{candidate_titles[original_idx]}' (score: {best_score:.3f})")
            
            # Retourner seulement si le score dépasse le seuil
            if best_score >= self.similarity_threshold:
                return original_idx, best_score
            else:
                logger.debug(f"Score trop faible ({best_score:.3f} < {self.similarity_threshold})")
                return None, best_score
                
        except Exception as e:
            logger.error(f"Erreur calcul similarité: {e}")
            return None, 0.0
    
    def get_similarity_score(self, title1: str, title2: str) -> float:
        """Calcule le score de similarité entre deux titres"""
        normalized1 = self.normalize_title(title1)
        normalized2 = self.normalize_title(title2)
        
        if not normalized1 or not normalized2:
            return 0.0
        
        try:
            tfidf_matrix = self.vectorizer.fit_transform([normalized1, normalized2])
            similarity = cosine_similarity(tfidf_matrix[0:1], tfidf_matrix[1:2])[0][0]
            return float(similarity)
        except:
            return 0.0
    
    def is_similar_enough(self, title1: str, title2: str) -> bool:
        """Vérifie si deux titres sont suffisamment similaires"""
        return self.get_similarity_score(title1, title2) >= self.similarity_threshold

# Tests unitaires
def test_similarity():
    """Tests de la similarité"""
    matcher = GameTitleMatcher(similarity_threshold=0.5)
    
    test_cases = [
        ("Cyberpunk 2077", "Cyberpunk 2077 Ultimate Edition", True),
        ("The Witcher 3", "The Witcher III Wild Hunt", True),
        ("FIFA 23", "FIFA 24", False),
        ("Call of Duty Modern Warfare", "Call of Duty Black Ops", False),
        ("Grand Theft Auto V", "GTA 5", True),
    ]
    
    print("🧪 Tests de similarité TF-IDF:")
    for title1, title2, expected in test_cases:
        score = matcher.get_similarity_score(title1, title2)
        is_similar = matcher.is_similar_enough(title1, title2)
        status = "✅" if is_similar == expected else "❌"
        print(f"{status} '{title1}' vs '{title2}': {score:.3f} -> {is_similar}")

if __name__ == "__main__":
    test_similarity()
EOF
