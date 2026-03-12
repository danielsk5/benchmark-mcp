#!/bin/bash
# Atualiza benchmark.db e faz deploy no Fly.io
# Uso: ./deploy.sh [--supabase]
#   --supabase   também sincroniza para o Supabase após o deploy

set -e

SRC=~/data/benchmark/benchmark.db
DST=~/Documents/GitHub/benchmark-mcp/data/benchmark.db

echo "=== benchmark-mcp deploy ==="
echo ""

# Verifica se a fonte existe
if [ ! -f "$SRC" ]; then
  echo "ERRO: $SRC não encontrado."
  exit 1
fi

# Copia banco atualizado
echo "1. Copiando benchmark.db..."
cp "$SRC" "$DST"
echo "   $(du -sh $DST | cut -f1) copiados"

# Deploy Fly.io
echo ""
echo "2. Deploy Fly.io..."
cd ~/Documents/GitHub/benchmark-mcp
fly deploy

# Supabase sync (opcional)
if [[ "$1" == "--supabase" ]]; then
  echo ""
  echo "3. Sincronizando Supabase..."
  python3 ~/Documents/GitHub/benchmark-mcp/insert_data.py
fi

echo ""
echo "Deploy concluído."
