# =========================
#   CONFIG / DB ENGINE - VERSION CORRIGÉE
# =========================
import urllib.parse

# Variables d'environnement requises
RAWG_API_KEY = os.getenv("RAWG_API_KEY")

DB_HOST = os.getenv("DB_HOST")
DB_PORT = int(os.getenv("DB_PORT", "3306"))
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")

# Limite de scraping par run (peut être ajustée via ENV)
SCRAPE_LIMIT = int(os.getenv("SCRAPE_LIMIT", "20"))

# Garde-fous
if not RAWG_API_KEY:
    raise RuntimeError("RAWG_API_KEY manquant.")

for k, v in [("DB_HOST", DB_HOST), ("DB_USER", DB_USER), ("DB_PASSWORD", DB_PASSWORD), ("DB_NAME", DB_NAME)]:
    if not v:
        raise RuntimeError(f"Config DB incomplète: variable {k} manquante.")

# MÉTHODE CORRIGÉE : Utiliser URL.create() au lieu de string interpolation
print(f"🔧 Connexion à {DB_HOST} en tant que {DB_USER}")

try:
    # Méthode recommandée pour les mots de passe avec caractères spéciaux
    db_url = URL.create(
        drivername="mysql+mysqlconnector",
        username=DB_USER,
        password=DB_PASSWORD,  # SQLAlchemy encode automatiquement
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME,
        query={"charset": "utf8mb4"},
    )
    
    engine = create_engine(db_url, pool_pre_ping=True, pool_recycle=3600)
    
    # Test de connexion
    with engine.connect() as test_conn:
        test_conn.execute(text("SELECT 1"))
    
    print("✅ Connexion MySQL établie")
    
except Exception as e:
    print(f"❌ Erreur connexion MySQL: {e}")
    print("🔧 Tentative avec encodage manuel...")
    
    # Fallback: encodage manuel du mot de passe
    try:
        encoded_password = urllib.parse.quote_plus(DB_PASSWORD)
        connection_string = f"mysql+mysqlconnector://{DB_USER}:{encoded_password}@{DB_HOST}:{DB_PORT}/{DB_NAME}?charset=utf8mb4"
        engine = create_engine(connection_string, pool_pre_ping=True, pool_recycle=3600)
        
        # Test de connexion
        with engine.connect() as test_conn:
            test_conn.execute(text("SELECT 1"))
        
        print("✅ Connexion MySQL établie (méthode fallback)")
        
    except Exception as e2:
        raise RuntimeError(f"Impossible de se connecter à MySQL: {e2}")
