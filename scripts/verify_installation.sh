#!/bin/bash

echo "🔍 Vérification de l'installation..."

# Vérifier la structure
echo "📁 Structure des dossiers:"
find . -type d -name ".git" -prune -o -type d -print | head -20

# Vérifier les fichiers principaux
echo -e "\n📄 Fichiers principaux:"
ls -la *.md *.yml *.txt *.json 2>/dev/null || true

# Vérifier les workflows GitHub
echo -e "\n⚡ Workflows GitHub Actions:"
ls -la .github/workflows/ 2>/dev/null || echo "Aucun workflow trouvé"

# Vérifier les scripts
echo -e "\n🔧 Scripts disponibles:"
ls -la scripts/ 2>/dev/null || echo "Aucun script trouvé"

# Vérifier la configuration Python
echo -e "\n🐍 Configuration Python:"
if command -v python3 &> /dev/null; then
    echo "✅ Python3 disponible: $(python3 --version)"
else
    echo "❌ Python3 non trouvé"
fi

echo -e "\n✅ Vérification terminée !"
