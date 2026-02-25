# Benchmark Shoppings BR — MCP Server

MCP Server público com dados de IR de 5 grupos de shoppings brasileiros.

## Empresas
- Iguatemi (IGTI11) — FY2025 vs FY2024
- Multiplan (MULT3) — FY2025 vs FY2024  
- Allos (ALOS3) — LTM 3Q25 vs LTM 3Q24
- XP Malls (XPML11) — LTM 3Q25 vs LTM 3Q24
- JHSF (JHSF3) — LTM 3Q25 vs LTM 3Q24

## Tools disponíveis
- `listar_entidades` — resumo das 5 empresas
- `ranking_ativos` — ranking por vendas/m², aluguel/m², ocupação ou yield
- `detalhe_ativo` — dados de um shopping específico
- `portfolio_comparativo` — período atual vs anterior
- `query_sql` — SQL livre (read-only)
- `schema_banco` — schema completo

## Conectar ao Claude
Adicionar em Settings → Integrations → Add MCP Server:
`https://SEU-DEPLOY.railway.app/mcp`

## Atualizar dados
1. Rodar parsers localmente
2. `cp benchmark.db ~/Documents/GitHub/benchmark-mcp/data/`
3. `git add data/benchmark.db && git commit -m "update benchmark" && git push`
4. Railway redeploy automático
