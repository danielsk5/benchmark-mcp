"""Benchmark Shoppings BR — MCP Server

Expõe dados públicos de IR de 8 entidades de shoppings brasileiros:
Iguatemi (IGTI11), Multiplan (MULT3), Allos (ALOS3), XP Malls (XPML11), JHSF (JHSF3),
Hedge Brasil Shopping (HGBS11), Vinci Shopping Centers (VISC11), BTG Pactual Malls (BPML11)

Períodos:
- Iguatemi/Multiplan: FY2025 vs FY2024 (ano encerrado)
- Demais: LTM 3Q25 vs LTM 3Q24 (soma últimos 4 trimestres)

Transporte: streamable-http com OAuth 2.1 (quando MCP_OAUTH_PASSWORD definido)

Convenção stake_pct (entity_asset_stakes):
- Todos os grupos: stake_pct armazenado como % (0.0–100.0)
- Zeros (stake_pct = 0.0) indicam ativos sem participação mapeada — tratar como NULL
- Colunas _100pct = 100% do shopping (fonte IR). Colunas _empresa = proporcional à participação.
"""

import os
import sqlite3
import logging
from pathlib import Path
from contextlib import contextmanager

from mcp.server.fastmcp import FastMCP
from mcp.server.fastmcp.server import AuthSettings
from mcp.server.auth.routes import ClientRegistrationOptions, RevocationOptions
from oauth_provider import BenchmarkOAuthProvider, MCP_OAUTH_PASSWORD, SERVER_URL, verify_password

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("benchmark-mcp")

DB_PATH = os.getenv("DB_PATH", str(Path(__file__).parent / "data" / "benchmark.db"))
PORT = int(os.getenv("PORT", "8000"))

oauth_provider = BenchmarkOAuthProvider() if MCP_OAUTH_PASSWORD else None
auth_settings = AuthSettings(
    issuer_url=SERVER_URL,
    resource_server_url=SERVER_URL,
    client_registration_options=ClientRegistrationOptions(enabled=True),
    revocation_options=RevocationOptions(enabled=True),
) if MCP_OAUTH_PASSWORD else None

mcp = FastMCP(
    name="benchmark-shoppings-br",
    auth_server_provider=oauth_provider,
    auth=auth_settings,
    instructions="""Banco de dados de benchmark de shoppings brasileiros com dados públicos de IR.
8 entidades: Iguatemi (IGTI11), Multiplan (MULT3), Allos (ALOS3), XP Malls (XPML11), JHSF (JHSF3),
Hedge Brasil (HGBS11), Vinci Shoppings (VISC11), BTG Pactual Malls (BPML11).
Métricas por ativo: vendas/m², aluguel/m², ocupação, yield receita/venda.
Período ref: IGTI11/MULT3 = FY2025; demais = LTM 3Q25.
Todos os valores monetários em R$ mil. Métricas /m² em R$/m²/ano.

METODOLOGIA DE /m² — CRÍTICO:
- asset_metrics.sales_psqm e rent_psqm = R$/m²/quarter (valor bruto do trimestre, sem anualização).
- Para valores anuais (R$/m²/ano LTM), USE SEMPRE v_asset_ltm.sales_psqm / v_asset_ltm.rent_psqm,
  que calcula LTM real (soma 4 quarters / ABL).
- Todos os tools analytics (ranking_ativos, detalhe_ativo, comparar_ativos, peer_group, gap_analysis,
  scatter_data, comparar_mercado) já usam v_asset_ltm — retornam R$/m²/ano correto.
- NUNCA some os valores /m² de 4 trimestres nem tire média deles. Para totais anuais, some sales_total.

Convenção stake_pct: % (0.0–100.0) para todos os grupos.
Colunas _100pct = 100% do shopping (fonte IR) — USE SEMPRE ESTAS para "vendas total" ou "receita total".
Colunas _empresa = proporcional à participação da entidade no ativo.

Tools disponíveis (19):
- listar_entidades: lista as 8 entidades
- ranking_ativos: ranking por métrica
- detalhe_ativo: detalhe completo de um ativo
- portfolio_comparativo: período atual vs anterior (apenas IGTI11/MULT3/ALOS3/XPML11/JHSF3)
- serie_historica: evolução trimestral de um ativo (usa v_asset_ltm — LTM correto)
- historico_anual: resumo ano a ano com vendas totais e /m² LTM (use para "histórico de vendas")
- comparar_ativos: side-by-side de 2–3 ativos
- top_movers: maiores altas/quedas YoY (usa v_asset_ltm — LTM correto)
- resumo_mercado: médias por estado ou categoria
- peer_group: encontra peers de um ativo referência
- concentracao_portfolio: HHI e concentração de receita
- gap_analysis: gaps de um ativo vs média da categoria
- scatter_data: pares (x,y) para gráficos
- mix_lojas: breakdown de segmentos de um shopping (n_lojas, ABL, %) — 9 shoppings disponíveis
- comparar_mix: side-by-side de 2 shoppings por segmento com delta ABL%
- buscar_loja: busca lojas por nome/segmento/shopping (1745 lojas, 16 segmentos)
- mix_por_entidade: mix agregado por operadora — igti11 (4 shoppings), mult3 (2), jhsf3 (2)
- query_sql: SQL livre read-only (max 500 linhas) — v_asset_ltm disponível
- schema_banco: schema completo

Store Mix disponível (1745 lojas, 16 segmentos):
- Iguatemi (igti11): Iguatemi SP, Iguatemi Brasília, JK Iguatemi, Leblon
- Multiplan (mult3): Village Mall, Park Shopping Barigui
- JHSF (jhsf3): Cidade Jardim, CJ Shops
- Pátio Batel: presente no store_mix mas fora das entidades de benchmark""",
    host="0.0.0.0",
    port=PORT,
    streamable_http_path="/mcp",
)


@contextmanager
def get_db():
    if not Path(DB_PATH).exists():
        raise FileNotFoundError(f"benchmark.db não encontrado: {DB_PATH}")
    conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro&immutable=1", uri=True)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()


def rows_to_dicts(cursor) -> list[dict]:
    cols = [d[0] for d in cursor.description]
    return [dict(zip(cols, row)) for row in cursor.fetchall()]


# ── CONSTANTS & VALIDATION ─────────────────────────────────────────────────

ENTITY_PERIODS = {
    "igti11": ("4Q25", "FY2025"),
    "mult3":  ("4Q25", "FY2025"),
    "alos3":  ("3Q25", "LTM 3Q25"),
    "xpml11": ("3Q25", "LTM 3Q25"),
    "jhsf3":  ("3Q25", "LTM 3Q25"),
    "hgbs11": ("3Q25", "LTM 3Q25"),
    "visc11": ("3Q25", "LTM 3Q25"),
    "bpml11": ("3Q25", "LTM 3Q25"),
}

VALID_ENTITIES = set(ENTITY_PERIODS.keys())
VALID_STATES = {
    "AL", "AM", "BA", "CE", "DF", "ES", "GO", "MA", "MG", "MS",
    "MT", "PA", "PE", "PR", "RJ", "RN", "RS", "SC", "SP", "TO",
}
VALID_CATEGORIES = {"premium", "regional", "outlet", "power-center", "other"}

CURRENT_QUARTER_FILTER = """(
    (am.entity_id IN ('igti11','mult3') AND am.quarter = '4Q25')
    OR (am.entity_id IN ('alos3','xpml11','jhsf3','hgbs11','visc11','bpml11') AND am.quarter = '3Q25')
)"""

METRIC_MAP = {
    "vendas_m2":  "am.sales_psqm",
    "aluguel_m2": "am.rent_psqm",
    "ocupacao":   "am.occupancy_rate",
    "yield_pct":  "ROUND(am.rent_psqm * 100.0 / NULLIF(am.sales_psqm,0), 2)",
    "sss":        "am.sss",
    "noi":        "am.noi",
    "noi_margin": "am.noi_margin",
}

QUARTER_ORDER = "CAST('20' || SUBSTR(am.quarter, 3) AS INTEGER) * 10 + CAST(SUBSTR(am.quarter, 1, 1) AS INTEGER)"


def _validate_entity(empresa: str) -> str:
    empresa = empresa.strip().lower()
    if empresa and empresa not in VALID_ENTITIES:
        raise ValueError(f"Empresa inválida: {empresa}. Válidas: {', '.join(sorted(VALID_ENTITIES))}")
    return empresa


def _validate_state(estado: str) -> str:
    estado = estado.strip().upper()
    if estado and estado not in VALID_STATES:
        raise ValueError(f"Estado inválido: {estado}. Válidos: {', '.join(sorted(VALID_STATES))}")
    return estado


def _validate_category(categoria: str) -> str:
    categoria = categoria.strip().lower()
    if categoria and categoria not in VALID_CATEGORIES:
        raise ValueError(f"Categoria inválida: {categoria}. Válidas: {', '.join(sorted(VALID_CATEGORIES))}")
    return categoria


def _validate_metric(metrica: str) -> str:
    metrica = metrica.strip().lower()
    if metrica not in METRIC_MAP:
        raise ValueError(f"Métrica inválida: {metrica}. Válidas: {', '.join(sorted(METRIC_MAP.keys()))}")
    return METRIC_MAP[metrica]


# ── EXISTING TOOLS (with parameterized queries) ───────────────────────────

@mcp.tool()
def listar_entidades() -> list[dict]:
    """Lista as 8 entidades no banco com cobertura e período de referência."""
    with get_db() as conn:
        rows = conn.execute("""
            SELECT e.id, e.name as nome, e.ticker, e.type as categoria,
                   COUNT(DISTINCT am.asset_id) as n_ativos,
                   MIN(pm.quarter) as periodo_ini,
                   MAX(pm.quarter) as periodo_fim
            FROM entities e
            LEFT JOIN asset_metrics am ON am.entity_id = e.id AND am.period_type='quarter'
            LEFT JOIN portfolio_metrics pm ON pm.entity_id = e.id AND pm.period_type='quarter'
            GROUP BY e.id ORDER BY e.name
        """).fetchall()
        result = [dict(r) for r in rows]
    for r in result:
        r["periodo_ref"] = ENTITY_PERIODS.get(r["id"], ("?", "?"))[1]
    return result


@mcp.tool()
def ranking_ativos(
    metrica: str = "vendas_m2",
    top_n: int = 30,
    empresa: str = "",
    estado: str = "",
) -> list[dict]:
    """Ranking de ativos por métrica.

    metrica: vendas_m2 | aluguel_m2 | ocupacao | yield_pct | sss | noi | noi_margin
    empresa: filtro por entity_id (igti11, mult3, alos3, xpml11, jhsf3, hgbs11, visc11, bpml11) — vazio = todas
    estado: filtro por UF (ex: PR, SP, RJ) — vazio = todos
    top_n: quantos retornar (max 100)
    """
    top_n = min(int(top_n), 100)
    metric_sql = _validate_metric(metrica)
    empresa = _validate_entity(empresa)
    estado = _validate_state(estado)

    filters = [CURRENT_QUARTER_FILTER, f"{metric_sql} IS NOT NULL"]
    params = []
    if empresa:
        filters.append("am.entity_id = ?")
        params.append(empresa)
    if estado:
        filters.append("a.state = ?")
        params.append(estado)

    where = " AND ".join(filters)

    with get_db() as conn:
        cur = conn.execute(f"""
            SELECT
                ROW_NUMBER() OVER (ORDER BY {metric_sql} DESC) as rank,
                e.name as empresa, e.ticker,
                a.name as ativo, a.city as cidade, a.state as estado,
                am.sales_psqm as vendas_m2,
                am.rent_psqm as aluguel_m2,
                ROUND(am.rent_psqm * 100.0 / NULLIF(am.sales_psqm,0), 2) as yield_pct,
                am.occupancy_rate as ocupacao_pct,
                am.abl_total_sqm as abl_m2,
                ROUND(am.sales_total / 1000.0, 1) as vendas_100pct_r_mi,
                CASE WHEN am.stake_pct > 0.0
                     THEN ROUND(am.sales_total * am.stake_pct / 100000.0, 1)
                     ELSE NULL END as vendas_empresa_r_mi,
                CASE WHEN am.stake_pct > 0.0
                     THEN ROUND(am.stake_pct, 2)
                     ELSE NULL END as participacao_pct,
                CASE am.entity_id
                    WHEN 'igti11' THEN 'FY2025' WHEN 'mult3' THEN 'FY2025'
                    ELSE 'LTM 3Q25' END as periodo_ref
            FROM v_asset_ltm am
            JOIN assets a ON a.id = am.asset_id
            JOIN entities e ON e.id = am.entity_id
            WHERE {where}
            ORDER BY {metric_sql} DESC
            LIMIT ?
        """, params + [top_n])
        return rows_to_dicts(cur)


@mcp.tool()
def detalhe_ativo(nome_ativo: str) -> list[dict]:
    """Detalhe completo de um ativo específico (busca por nome parcial).

    Retorna dados do período de referência mais recente disponível.
    Exemplo: nome_ativo='Village' retorna VillageMall.
    """
    with get_db() as conn:
        cur = conn.execute(f"""
            SELECT
                e.name as empresa, e.ticker,
                a.name as ativo, a.city as cidade, a.state as estado,
                CASE am.entity_id WHEN 'igti11' THEN 'FY2025' WHEN 'mult3' THEN 'FY2025'
                    ELSE 'LTM 3Q25' END as periodo_ref,
                am.sales_psqm as vendas_m2, am.rent_psqm as aluguel_m2,
                ROUND(am.rent_psqm * 100.0 / NULLIF(am.sales_psqm,0), 2) as yield_pct,
                am.occupancy_rate as ocupacao_pct, am.sss as sss_pct,
                am.sales_total as vendas_100pct_r_mil,
                CASE WHEN am.stake_pct > 0.0
                     THEN ROUND(am.sales_total * am.stake_pct / 100.0, 0)
                     ELSE NULL END as vendas_empresa_r_mil,
                am.noi as noi_100pct_r_mil,
                CASE WHEN am.stake_pct > 0.0
                     THEN ROUND(am.noi * am.stake_pct / 100.0, 0)
                     ELSE NULL END as noi_empresa_r_mil,
                am.rent_total as aluguel_100pct_r_mil,
                CASE WHEN am.stake_pct > 0.0
                     THEN ROUND(am.rent_total * am.stake_pct / 100.0, 0)
                     ELSE NULL END as aluguel_empresa_r_mil,
                am.abl_total_sqm as abl_total_m2, am.abl_own_sqm as abl_propria_m2,
                CASE WHEN am.stake_pct > 0.0
                     THEN ROUND(am.stake_pct, 4)
                     ELSE NULL END as participacao_pct
            FROM v_asset_ltm am
            JOIN assets a ON a.id = am.asset_id
            JOIN entities e ON e.id = am.entity_id
            WHERE {CURRENT_QUARTER_FILTER}
            AND LOWER(a.name) LIKE LOWER(?)
            ORDER BY am.sales_psqm DESC NULLS LAST
        """, [f"%{nome_ativo}%"])
        return rows_to_dicts(cur)


@mcp.tool()
def portfolio_comparativo(empresa: str = "") -> list[dict]:
    """Comparativo de portfólio: período atual vs anterior por empresa.

    empresa: igti11 | mult3 | alos3 | xpml11 | jhsf3 — vazio = todas
    (HGBS11/VISC11/BPML11 não têm portfolio_metrics — use ranking_ativos ou query_sql para esses)
    Iguatemi/Multiplan: FY2025 vs FY2024
    Allos/XP Malls/JHSF: LTM 3Q25 vs LTM 3Q24
    """
    empresa = _validate_entity(empresa)

    fy_params = []
    ltm_params = []

    if empresa:
        fy_entity_filter = "pm.entity_id = ? AND"
        ltm_entity_filter = "pm.entity_id = ? AND"
        fy_params.append(empresa)
        ltm_params.append(empresa)
    else:
        fy_entity_filter = "pm.entity_id IN ('igti11','mult3') AND"
        ltm_entity_filter = "pm.entity_id IN ('alos3','xpml11','jhsf3') AND"

    with get_db() as conn:
        cur_fy = conn.execute(f"""
            SELECT e.name as empresa, e.ticker,
                pm.quarter as periodo,
                ROUND(pm.sales_total/1000000.0, 3) as vendas_r_bi,
                ROUND(pm.noi/1000.0, 1) as noi_r_mi,
                ROUND(pm.ebitda/1000.0, 1) as ebitda_r_mi,
                pm.ebitda_margin as ebitda_margin_pct,
                ROUND(pm.ffo_margin*100.0, 2) as ffo_margin_pct,
                pm.sss as sss_pct, pm.ssr as ssr_pct,
                pm.occupancy_rate as ocupacao_pct,
                ROUND(pm.abl_own_sqm/1000.0, 1) as abl_propria_mil_m2,
                pm.total_assets as n_shoppings,
                pm.rent_psqm as aluguel_m2_portfolio
            FROM portfolio_metrics pm
            JOIN entities e ON e.id = pm.entity_id
            WHERE {fy_entity_filter} pm.quarter IN ('FY25','FY24') AND pm.period_type='annual'
            ORDER BY e.name, pm.quarter
        """, fy_params)
        fy_rows = rows_to_dicts(cur_fy)

        cur_ltm = conn.execute(f"""
            SELECT e.name as empresa, e.ticker,
                CASE WHEN pm.quarter IN ('4Q24','1Q25','2Q25','3Q25') THEN 'LTM_3Q25'
                     ELSE 'LTM_3Q24' END as periodo,
                ROUND(SUM(pm.sales_total)/1000000.0, 3) as vendas_r_bi,
                ROUND(SUM(pm.noi)/1000.0, 1) as noi_r_mi,
                ROUND(SUM(pm.ebitda)/1000.0, 1) as ebitda_r_mi,
                ROUND(AVG(pm.ebitda_margin), 2) as ebitda_margin_pct,
                ROUND(AVG(pm.ffo_margin)*100.0, 2) as ffo_margin_pct,
                ROUND(AVG(pm.sss), 2) as sss_pct,
                ROUND(AVG(pm.ssr), 2) as ssr_pct,
                ROUND(AVG(pm.occupancy_rate), 2) as ocupacao_pct,
                ROUND(AVG(pm.abl_own_sqm)/1000.0, 1) as abl_propria_mil_m2,
                ROUND(AVG(pm.total_assets), 0) as n_shoppings,
                ROUND(AVG(pm.rent_psqm), 2) as aluguel_m2_portfolio
            FROM portfolio_metrics pm
            JOIN entities e ON e.id = pm.entity_id
            WHERE {ltm_entity_filter}
                pm.quarter IN ('4Q23','1Q24','2Q24','3Q24','4Q24','1Q25','2Q25','3Q25')
                AND pm.period_type='quarter'
            GROUP BY pm.entity_id, periodo
            ORDER BY e.name, periodo
        """, ltm_params)
        ltm_rows = rows_to_dicts(cur_ltm)

    return fy_rows + ltm_rows


# ── TIER 1: CORE ANALYTICS ────────────────────────────────────────────────

@mcp.tool()
def serie_historica(
    nome_ativo: str,
    metrica: str = "vendas_m2",
    n_quarters: int = 12,
) -> list[dict]:
    """Série histórica trimestral de um ativo — vendas/m² e aluguel/m² em LTM (últimos 12 meses).

    nome_ativo: busca parcial (ex: 'Village', 'Iguatemi SP', 'Barigui')
    metrica: vendas_m2 | aluguel_m2 | ocupacao | yield_pct | sss | noi | noi_margin
    n_quarters: quantos trimestres retornar (max 44, default 12)

    IMPORTANTE: vendas_m2 e aluguel_m2 retornados são LTM (soma 4 quarters reais / ABL),
    não Q×4. Isso garante comparabilidade histórica correta sem distorção sazonal.
    Campo psqm_methodology indica 'LTM' (4 quarters) ou 'partial_NQ' (início da série).

    Retorna em ordem cronológica (mais antigo primeiro).
    """
    n_quarters = min(int(n_quarters), 44)
    # Remap metric SQL to use v_asset_ltm columns (LTM-correct)
    metric_map_ltm = {
        "vendas_m2":  "am.sales_psqm",
        "aluguel_m2": "am.rent_psqm",
        "ocupacao":   "am.occupancy_rate",
        "yield_pct":  "ROUND(am.rent_psqm * 100.0 / NULLIF(am.sales_psqm,0), 2)",
        "sss":        "am.sss",
        "noi":        "am.noi",
        "noi_margin": "am.noi_margin",
    }
    metrica_clean = metrica.strip().lower()
    if metrica_clean not in metric_map_ltm:
        raise ValueError(f"Métrica inválida: {metrica_clean}. Válidas: {', '.join(sorted(metric_map_ltm.keys()))}")
    metric_sql = metric_map_ltm[metrica_clean]

    with get_db() as conn:
        cur = conn.execute(f"""
            SELECT
                a.name as ativo, e.name as empresa,
                am.quarter as trimestre,
                {metric_sql} as valor,
                am.sales_psqm as vendas_m2,
                am.rent_psqm as aluguel_m2,
                am.occupancy_rate as ocupacao_pct,
                am.sss as sss_pct,
                am.psqm_methodology,
                am.ltm_quarters_count
            FROM v_asset_ltm am
            JOIN assets a ON a.id = am.asset_id
            JOIN entities e ON e.id = am.entity_id
            WHERE LOWER(a.name) LIKE LOWER(?)
            AND {metric_sql} IS NOT NULL
            ORDER BY am.quarter_order DESC
            LIMIT ?
        """, [f"%{nome_ativo}%", n_quarters])
        rows = rows_to_dicts(cur)

    return list(reversed(rows))


@mcp.tool()
def historico_anual(
    nome_ativo: str,
    ano_ini: int = 2015,
    ano_fim: int = 2025,
) -> list[dict]:
    """Resumo ano a ano de um ativo: vendas totais, vendas/m² e aluguel/m² em LTM.

    nome_ativo: busca parcial (ex: 'Barigui', 'Village', 'Iguatemi SP')
    ano_ini: ano de início (default 2015)
    ano_fim: ano de fim (default 2025)

    Metodologia: agrega os 4 trimestres de cada ano civil (1Q-4Q).
    - vendas_total_r_mi: soma real dos 4 quarters em R$ MM (100% do ativo)
    - vendas_psqm: LTM ao final do ano (4Q do ano), em R$/m²/ano
    - receita_psqm: idem para receita de aluguel
    - abl_4q: ABL total no 4Q do ano em m²
    Retorna cronológico do mais antigo ao mais recente.
    """
    ano_ini = int(ano_ini)
    ano_fim = int(ano_fim)

    with get_db() as conn:
        cur = conn.execute("""
            SELECT
                '20' || SUBSTR(am.quarter, 3) AS ano,
                ROUND(SUM(am.sales_total) / 1000.0, 1) AS vendas_total_r_mi,
                ROUND(SUM(am.rent_total) / 1000.0, 2) AS receita_total_r_mi,
                MAX(CASE WHEN am.quarter LIKE '4Q%' THEN am.abl_total_sqm END) AS abl_4q_m2,
                -- LTM ao final do ano = valor 4Q da view (já é LTM correto)
                MAX(CASE WHEN am.quarter LIKE '4Q%' THEN am.sales_psqm END) AS vendas_psqm,
                MAX(CASE WHEN am.quarter LIKE '4Q%' THEN am.rent_psqm END) AS receita_psqm,
                MAX(CASE WHEN am.quarter LIKE '4Q%' THEN am.psqm_methodology END) AS metodologia,
                COUNT(DISTINCT am.quarter) AS quarters_disponiveis
            FROM v_asset_ltm am
            JOIN assets a ON a.id = am.asset_id
            WHERE LOWER(a.name) LIKE LOWER(?)
            AND CAST('20' || SUBSTR(am.quarter, 3) AS INTEGER) BETWEEN ? AND ?
            GROUP BY am.entity_id, '20' || SUBSTR(am.quarter, 3)
            HAVING COUNT(DISTINCT am.quarter) = 4
            ORDER BY ano
        """, [f"%{nome_ativo}%", ano_ini, ano_fim])
        rows = rows_to_dicts(cur)

    # Add YoY columns
    for i, row in enumerate(rows):
        if i == 0:
            row["vendas_total_yoy_pct"] = None
            row["vendas_psqm_yoy_pct"] = None
        else:
            prev = rows[i - 1]
            def yoy(cur_val, prev_val):
                if prev_val and prev_val != 0 and cur_val is not None:
                    return round((cur_val - prev_val) * 100.0 / abs(prev_val), 1)
                return None
            row["vendas_total_yoy_pct"] = yoy(row["vendas_total_r_mi"], prev["vendas_total_r_mi"])
            row["vendas_psqm_yoy_pct"] = yoy(row["vendas_psqm"], prev["vendas_psqm"])

    return rows


@mcp.tool()
def comparar_ativos(
    ativo1: str,
    ativo2: str,
    ativo3: str = "",
) -> list[dict]:
    """Comparação side-by-side de 2 ou 3 ativos no período atual.

    ativo1, ativo2, ativo3: busca parcial por nome (ex: 'Village', 'Iguatemi SP')
    Retorna todas as métricas disponíveis para comparação direta.
    """
    nomes = [ativo1, ativo2]
    if ativo3:
        nomes.append(ativo3)

    results = []
    with get_db() as conn:
        for nome in nomes:
            cur = conn.execute(f"""
                SELECT
                    a.name as ativo, e.name as empresa, e.ticker,
                    a.city as cidade, a.state as estado, a.category as categoria,
                    CASE am.entity_id WHEN 'igti11' THEN 'FY2025' WHEN 'mult3' THEN 'FY2025'
                        ELSE 'LTM 3Q25' END as periodo_ref,
                    am.sales_psqm as vendas_m2,
                    am.rent_psqm as aluguel_m2,
                    ROUND(am.rent_psqm * 100.0 / NULLIF(am.sales_psqm,0), 2) as yield_pct,
                    am.occupancy_rate as ocupacao_pct,
                    am.sss as sss_pct,
                    am.noi as noi_r_mil,
                    am.noi_margin as noi_margin_pct,
                    am.occ_cost_pct as custo_ocupacao_pct,
                    am.default_rate as inadimplencia_pct,
                    am.abl_total_sqm as abl_total_m2,
                    ROUND(am.sales_total / 1000.0, 1) as vendas_100pct_r_mi,
                    CASE WHEN am.stake_pct > 0.0
                         THEN ROUND(am.stake_pct, 2)
                         ELSE NULL END as participacao_pct
                FROM v_asset_ltm am
                JOIN assets a ON a.id = am.asset_id
                JOIN entities e ON e.id = am.entity_id
                WHERE {CURRENT_QUARTER_FILTER}
                AND LOWER(a.name) LIKE LOWER(?)
                ORDER BY am.sales_psqm DESC NULLS LAST
                LIMIT 1
            """, [f"%{nome}%"])
            rows = rows_to_dicts(cur)
            if rows:
                results.append(rows[0])

    return results


@mcp.tool()
def top_movers(
    metrica: str = "vendas_m2",
    direcao: str = "alta",
    top_n: int = 15,
    empresa: str = "",
    estado: str = "",
) -> list[dict]:
    """Maiores altas ou quedas YoY (year-over-year) por ativo.

    metrica: vendas_m2 | aluguel_m2 | ocupacao | yield_pct | sss | noi
    direcao: alta | queda
    top_n: quantos retornar (max 50)
    empresa: filtro por entity_id — vazio = todas
    estado: filtro por UF — vazio = todos

    Compara trimestre mais recente vs mesmo trimestre do ano anterior.
    """
    top_n = min(int(top_n), 50)
    metric_sql = _validate_metric(metrica)
    empresa = _validate_entity(empresa)
    estado = _validate_state(estado)

    if direcao not in ("alta", "queda"):
        raise ValueError("direcao deve ser 'alta' ou 'queda'")

    order = "DESC" if direcao == "alta" else "ASC"

    # Use v_asset_ltm so YoY comparison is LTM vs LTM (not Q×4 vs LTM)
    metric_cur = metric_sql.replace("am.", "cur.")
    metric_prev = metric_sql.replace("am.", "prev.")

    current_prior = """(
        (cur.entity_id IN ('igti11','mult3') AND cur.quarter = '4Q25' AND prev.quarter = '4Q24')
        OR (cur.entity_id IN ('alos3','xpml11','jhsf3','hgbs11','visc11','bpml11') AND cur.quarter = '3Q25' AND prev.quarter = '3Q24')
    )"""

    extra_filters = []
    params = []
    if empresa:
        extra_filters.append("cur.entity_id = ?")
        params.append(empresa)
    if estado:
        extra_filters.append("a.state = ?")
        params.append(estado)

    extra_where = (" AND " + " AND ".join(extra_filters)) if extra_filters else ""

    with get_db() as conn:
        cur = conn.execute(f"""
            SELECT
                a.name as ativo, e.name as empresa, a.state as estado,
                cur.quarter as trimestre_atual,
                prev.quarter as trimestre_anterior,
                {metric_cur} as valor_atual,
                {metric_prev} as valor_anterior,
                ROUND({metric_cur} - {metric_prev}, 2) as variacao_abs,
                CASE WHEN {metric_prev} IS NOT NULL AND {metric_prev} != 0
                     THEN ROUND(({metric_cur} - {metric_prev}) * 100.0 / ABS({metric_prev}), 2)
                     ELSE NULL END as variacao_pct
            FROM v_asset_ltm cur
            JOIN v_asset_ltm prev
                ON prev.asset_id = cur.asset_id AND prev.entity_id = cur.entity_id
            JOIN assets a ON a.id = cur.asset_id
            JOIN entities e ON e.id = cur.entity_id
            WHERE {current_prior}
            AND {metric_cur} IS NOT NULL
            AND {metric_prev} IS NOT NULL
            {extra_where}
            ORDER BY variacao_pct {order}
            LIMIT ?
        """, params + [top_n])
        return rows_to_dicts(cur)


@mcp.tool()
def resumo_mercado(
    estado: str = "",
    categoria: str = "",
) -> list[dict]:
    """Resumo agregado do mercado por estado e/ou categoria.

    estado: filtro por UF (ex: PR, SP) — vazio = todos
    categoria: premium | regional | outlet | power-center | other — vazio = todas

    Retorna: n_ativos, médias de vendas/m², aluguel/m², ocupação, yield, e valores min/max.
    Agrupamento: por categoria se filtrou estado; por estado se filtrou categoria; por estado se sem filtro.
    """
    estado = _validate_state(estado)
    categoria = _validate_category(categoria)

    filters = [CURRENT_QUARTER_FILTER, "am.sales_psqm IS NOT NULL"]
    params = []

    group_col = "a.state"
    if estado:
        filters.append("a.state = ?")
        params.append(estado)
        group_col = "a.category"
    if categoria:
        filters.append("a.category = ?")
        params.append(categoria)
        if not estado:
            group_col = "a.state"

    where = " AND ".join(filters)

    with get_db() as conn:
        cur = conn.execute(f"""
            SELECT
                {group_col} as agrupamento,
                COUNT(*) as n_ativos,
                ROUND(AVG(am.sales_psqm), 0) as media_vendas_m2,
                ROUND(MIN(am.sales_psqm), 0) as min_vendas_m2,
                ROUND(MAX(am.sales_psqm), 0) as max_vendas_m2,
                ROUND(AVG(am.rent_psqm), 0) as media_aluguel_m2,
                ROUND(AVG(am.occupancy_rate), 2) as media_ocupacao_pct,
                ROUND(AVG(CASE WHEN am.sales_psqm > 0
                          THEN am.rent_psqm * 100.0 / am.sales_psqm END), 2) as media_yield_pct,
                ROUND(AVG(am.sss), 2) as media_sss_pct,
                ROUND(SUM(am.sales_total) / 1000.0, 1) as vendas_totais_r_mi
            FROM v_asset_ltm am
            JOIN assets a ON a.id = am.asset_id
            WHERE {where}
            GROUP BY {group_col}
            ORDER BY media_vendas_m2 DESC
        """, params)
        return rows_to_dicts(cur)


# ── TIER 2: STRATEGIC ANALYTICS ───────────────────────────────────────────

@mcp.tool()
def peer_group(
    nome_ativo: str,
    metrica: str = "vendas_m2",
    raio_pct: float = 20.0,
) -> list[dict]:
    """Encontra ativos comparáveis (peers) a um ativo de referência.

    nome_ativo: busca parcial (ex: 'Batel', 'Village')
    metrica: vendas_m2 | aluguel_m2 | ocupacao | yield_pct
    raio_pct: tolerância em % (ex: 20 = ±20% do valor de referência). Min 5, max 50.

    Retorna o ativo de referência (is_ref=1) e os peers dentro do raio,
    com delta_pct_vs_ref mostrando a distância percentual.
    """
    metric_sql = _validate_metric(metrica)
    raio_pct = min(max(float(raio_pct), 5.0), 50.0)

    with get_db() as conn:
        ref = conn.execute(f"""
            SELECT am.asset_id, {metric_sql} as ref_value
            FROM v_asset_ltm am
            JOIN assets a ON a.id = am.asset_id
            WHERE {CURRENT_QUARTER_FILTER}
            AND LOWER(a.name) LIKE LOWER(?)
            AND {metric_sql} IS NOT NULL
            ORDER BY am.sales_psqm DESC NULLS LAST
            LIMIT 1
        """, [f"%{nome_ativo}%"]).fetchone()

        if not ref:
            raise ValueError(f"Ativo '{nome_ativo}' não encontrado ou sem dados para métrica")

        ref_value = ref["ref_value"]
        ref_asset_id = ref["asset_id"]
        lower = ref_value * (1 - raio_pct / 100.0)
        upper = ref_value * (1 + raio_pct / 100.0)

        cur = conn.execute(f"""
            SELECT
                a.name as ativo, e.name as empresa, a.state as estado,
                a.category as categoria,
                {metric_sql} as valor,
                am.sales_psqm as vendas_m2,
                am.rent_psqm as aluguel_m2,
                am.occupancy_rate as ocupacao_pct,
                ROUND(am.rent_psqm * 100.0 / NULLIF(am.sales_psqm,0), 2) as yield_pct,
                CASE WHEN am.asset_id = ? THEN 1 ELSE 0 END as is_ref,
                ROUND(({metric_sql} - ?) * 100.0 / ABS(?), 2) as delta_pct_vs_ref
            FROM v_asset_ltm am
            JOIN assets a ON a.id = am.asset_id
            JOIN entities e ON e.id = am.entity_id
            WHERE {CURRENT_QUARTER_FILTER}
            AND {metric_sql} BETWEEN ? AND ?
            ORDER BY is_ref DESC, {metric_sql} DESC
        """, [ref_asset_id, ref_value, ref_value, lower, upper])
        return rows_to_dicts(cur)


@mcp.tool()
def concentracao_portfolio(empresa: str) -> dict:
    """Análise de concentração do portfólio de uma empresa.

    empresa: igti11 | mult3 | alos3 | xpml11 | jhsf3 | hgbs11 | visc11 | bpml11 (obrigatório)

    Retorna:
    - top_5_ativos com % da receita total
    - hhi (Herfindahl-Hirschman): <1500 diversificado, 1500-2500 moderado, >2500 concentrado
    - pct_top_1, pct_top_3, pct_top_5
    """
    empresa = _validate_entity(empresa)
    if not empresa:
        raise ValueError("empresa é obrigatório para análise de concentração")

    quarter = ENTITY_PERIODS[empresa][0]

    with get_db() as conn:
        rows = conn.execute("""
            SELECT a.name as ativo, a.state as estado,
                   am.sales_total as vendas_r_mil,
                   eas.abl_total_sqm as abl_m2
            FROM asset_metrics am
            JOIN assets a ON a.id = am.asset_id
            JOIN entity_asset_stakes eas
                ON eas.asset_id = am.asset_id AND eas.entity_id = am.entity_id
                AND eas.quarter = am.quarter
            WHERE am.entity_id = ? AND am.quarter = ?
            AND am.period_type = 'quarter'
            AND am.sales_total IS NOT NULL AND am.sales_total > 0
            ORDER BY am.sales_total DESC
        """, [empresa, quarter]).fetchall()

        if not rows:
            return {"empresa": empresa, "erro": "Sem dados de vendas para este período"}

        assets = [dict(r) for r in rows]
        total = sum(a["vendas_r_mil"] for a in assets)

        for a in assets:
            a["pct_receita"] = round(a["vendas_r_mil"] * 100.0 / total, 2)

        shares = [a["pct_receita"] for a in assets]
        hhi = round(sum(s ** 2 for s in shares), 0)

        return {
            "empresa": empresa,
            "periodo": ENTITY_PERIODS[empresa][1],
            "n_ativos": len(assets),
            "vendas_total_r_mil": round(total, 0),
            "hhi": hhi,
            "hhi_classificacao": "diversificado" if hhi < 1500 else ("moderado" if hhi < 2500 else "concentrado"),
            "pct_top_1": shares[0] if len(shares) >= 1 else None,
            "pct_top_3": round(sum(shares[:3]), 2) if len(shares) >= 3 else None,
            "pct_top_5": round(sum(shares[:5]), 2) if len(shares) >= 5 else None,
            "top_5_ativos": assets[:5],
        }


@mcp.tool()
def gap_analysis(
    nome_ativo: str,
    categoria_benchmark: str = "",
) -> dict:
    """Análise de gaps: compara um ativo contra a média da sua categoria.

    nome_ativo: busca parcial (ex: 'Batel', 'Village')
    categoria_benchmark: premium | regional | outlet | power-center — vazio = usa categoria do ativo

    Retorna cada métrica com: valor do ativo, média da categoria, delta, delta %,
    e classificação (acima | abaixo | na_media ±5%).
    """
    categoria_benchmark = _validate_category(categoria_benchmark)

    with get_db() as conn:
        ref = conn.execute(f"""
            SELECT a.name, a.category,
                   am.sales_psqm, am.rent_psqm, am.occupancy_rate,
                   am.sss, am.noi_margin
            FROM v_asset_ltm am
            JOIN assets a ON a.id = am.asset_id
            WHERE {CURRENT_QUARTER_FILTER}
            AND LOWER(a.name) LIKE LOWER(?)
            ORDER BY am.sales_psqm DESC NULLS LAST
            LIMIT 1
        """, [f"%{nome_ativo}%"]).fetchone()

        if not ref:
            raise ValueError(f"Ativo '{nome_ativo}' não encontrado")

        cat = categoria_benchmark or ref["category"]

        avgs = conn.execute(f"""
            SELECT
                ROUND(AVG(am.sales_psqm), 0) as avg_vendas_m2,
                ROUND(AVG(am.rent_psqm), 0) as avg_aluguel_m2,
                ROUND(AVG(am.occupancy_rate), 2) as avg_ocupacao,
                ROUND(AVG(CASE WHEN am.sales_psqm > 0
                          THEN am.rent_psqm * 100.0 / am.sales_psqm END), 2) as avg_yield,
                ROUND(AVG(am.sss), 2) as avg_sss,
                ROUND(AVG(am.noi_margin), 2) as avg_noi_margin,
                COUNT(*) as n_ativos_categoria
            FROM v_asset_ltm am
            JOIN assets a ON a.id = am.asset_id
            WHERE {CURRENT_QUARTER_FILTER}
            AND a.category = ?
            AND am.sales_psqm IS NOT NULL
        """, [cat]).fetchone()

        yield_ref = round(ref["rent_psqm"] * 100.0 / ref["sales_psqm"], 2) if ref["sales_psqm"] else None

        metrics = [
            ("vendas_m2", ref["sales_psqm"], avgs["avg_vendas_m2"], "R$/m²/ano"),
            ("aluguel_m2", ref["rent_psqm"], avgs["avg_aluguel_m2"], "R$/m²/ano"),
            ("ocupacao", ref["occupancy_rate"], avgs["avg_ocupacao"], "%"),
            ("yield_pct", yield_ref, avgs["avg_yield"], "%"),
            ("sss", ref["sss"], avgs["avg_sss"], "%"),
            ("noi_margin", ref["noi_margin"], avgs["avg_noi_margin"], "%"),
        ]

        gaps = []
        for nome_metrica, valor, media, unidade in metrics:
            if valor is None or media is None:
                continue
            delta = round(valor - media, 2)
            delta_pct = round(delta * 100.0 / abs(media), 2) if media != 0 else None
            classificacao = "na_media" if abs(delta_pct or 0) < 5 else ("acima" if delta > 0 else "abaixo")
            gaps.append({
                "metrica": nome_metrica,
                "valor_ativo": valor,
                "media_categoria": media,
                "delta": delta,
                "delta_pct": delta_pct,
                "classificacao": classificacao,
                "unidade": unidade,
            })

    return {
        "ativo": ref["name"],
        "categoria_benchmark": cat,
        "n_ativos_na_categoria": avgs["n_ativos_categoria"],
        "gaps": gaps,
    }


@mcp.tool()
def scatter_data(
    eixo_x: str = "vendas_m2",
    eixo_y: str = "aluguel_m2",
    categoria: str = "",
    estado: str = "",
) -> list[dict]:
    """Dados para gráfico de dispersão (scatter plot).

    eixo_x: vendas_m2 | aluguel_m2 | ocupacao | yield_pct | sss | noi | noi_margin
    eixo_y: vendas_m2 | aluguel_m2 | ocupacao | yield_pct | sss | noi | noi_margin
    categoria: filtro — vazio = todas
    estado: filtro — vazio = todos

    Retorna pares (x, y) com label e metadata para montar gráfico.
    """
    x_sql = _validate_metric(eixo_x)
    y_sql = _validate_metric(eixo_y)
    categoria = _validate_category(categoria)
    estado = _validate_state(estado)

    filters = [CURRENT_QUARTER_FILTER, f"{x_sql} IS NOT NULL", f"{y_sql} IS NOT NULL"]
    params = []
    if categoria:
        filters.append("a.category = ?")
        params.append(categoria)
    if estado:
        filters.append("a.state = ?")
        params.append(estado)

    where = " AND ".join(filters)

    with get_db() as conn:
        cur = conn.execute(f"""
            SELECT
                a.name as label,
                e.name as empresa,
                a.state as estado,
                a.category as categoria,
                {x_sql} as x,
                {y_sql} as y
            FROM v_asset_ltm am
            JOIN assets a ON a.id = am.asset_id
            JOIN entities e ON e.id = am.entity_id
            WHERE {where}
            ORDER BY {x_sql} DESC
        """, params)
        return rows_to_dicts(cur)


# ── STORE MIX TOOLS ────────────────────────────────────────────────────────

VALID_SHOPPINGS = {
    "CJ Shops", "Cidade Jardim", "Iguatemi Brasília", "Iguatemi São Paulo",
    "JK Iguatemi", "Leblon", "Park Shopping Barigui", "Pátio Batel", "Village Mall",
}

# Mapeamento: nome do shopping no store_mix → entity_id no benchmark.db
# Pátio Batel não está nas entidades do benchmark (é o nosso ativo)
SHOPPING_ENTITY_MAP: dict[str, str] = {
    "Iguatemi São Paulo":  "igti11",
    "Iguatemi Brasília":   "igti11",
    "JK Iguatemi":         "igti11",
    "Leblon":              "igti11",   # Iguatemi Leblon
    "Village Mall":        "mult3",
    "Park Shopping Barigui": "mult3",
    "Cidade Jardim":       "jhsf3",
    "CJ Shops":            "jhsf3",   # CJ Shops faz parte do complexo Cidade Jardim (JHSF)
    "Pátio Batel":         None,      # nosso shopping — fora do benchmark
}

# Shoppings agrupados por entidade
ENTITY_SHOPPINGS: dict[str, list[str]] = {}
for _sh, _ent in SHOPPING_ENTITY_MAP.items():
    if _ent:
        ENTITY_SHOPPINGS.setdefault(_ent, []).append(_sh)

VALID_SEGMENTS = {
    "Alimentação", "Artigos Diversos", "Artigos Esportivos", "Artigos para o Lar",
    "Calçados", "Conveniência/Serviços", "Eletrodomésticos e Eletroeletrônicos",
    "Entretenimento", "Hipermercado/Supermercado/Atacado", "Livrarias",
    "Loja de Departamentos", "Perfumaria, Maquiagem e Cosméticos",
    "Relojoarias, Joalherias e Bijouterias", "Telefonia e Acessórios",
    "Vestuário", "Óticas",
}


def _validate_shopping(shopping: str) -> str:
    shopping = shopping.strip()
    if shopping and shopping not in VALID_SHOPPINGS:
        raise ValueError(
            f"Shopping inválido: '{shopping}'. Válidos: {', '.join(sorted(VALID_SHOPPINGS))}"
        )
    return shopping


def _validate_segment(segmento: str) -> str:
    segmento = segmento.strip()
    if segmento and segmento not in VALID_SEGMENTS:
        raise ValueError(
            f"Segmento inválido: '{segmento}'. Válidos: {', '.join(sorted(VALID_SEGMENTS))}"
        )
    return segmento


@mcp.tool()
def mix_lojas(
    shopping: str = "",
    segmento: str = "",
) -> dict:
    """Breakdown do mix de lojas por segmento.

    shopping: nome exato (ex: 'Pátio Batel', 'Village Mall') — vazio = todos os 9 shoppings
    segmento: filtro por segmento — vazio = todos

    Retorna: por segmento → n_lojas, n_lojas_pct, subsegmentos.
    Também retorna totais e lista dos shoppings no escopo.
    """
    shopping = _validate_shopping(shopping)
    segmento = _validate_segment(segmento)

    filters = ["segment IS NOT NULL"]
    params: list = []
    if shopping:
        filters.append("shopping = ?")
        params.append(shopping)
    if segmento:
        filters.append("segment = ?")
        params.append(segmento)

    where = " AND ".join(filters)

    with get_db() as conn:
        n_total = conn.execute(
            f"SELECT COUNT(*) FROM store_mix WHERE {where}", params
        ).fetchone()[0] or 1

        segs = conn.execute(f"""
            SELECT
                segment,
                COUNT(*) as n_lojas,
                GROUP_CONCAT(DISTINCT subsegment) as subsegmentos
            FROM store_mix
            WHERE {where}
            GROUP BY segment
            ORDER BY n_lojas DESC
        """, params).fetchall()

        breakdown = [
            {
                "segmento": s["segment"],
                "n_lojas": s["n_lojas"],
                "n_lojas_pct": round(s["n_lojas"] * 100 / n_total, 1),
                "subsegmentos": sorted(set(
                    x.strip() for x in (s["subsegmentos"] or "").split(",") if x.strip()
                )),
            }
            for s in segs
        ]

        shoppings_in_scope = conn.execute(f"""
            SELECT shopping, COUNT(*) as n_lojas
            FROM store_mix WHERE {where}
            GROUP BY shopping ORDER BY n_lojas DESC
        """, params).fetchall()

    return {
        "filtro_shopping": shopping or "todos",
        "filtro_segmento": segmento or "todos",
        "n_lojas_total": n_total,
        "shoppings": [dict(s) for s in shoppings_in_scope],
        "por_segmento": breakdown,
    }


@mcp.tool()
def comparar_mix(
    shopping_a: str,
    shopping_b: str,
) -> dict:
    """Compara o mix de lojas de dois shoppings lado a lado por segmento.

    shopping_a, shopping_b: nomes exatos (ex: 'Pátio Batel', 'Village Mall')

    Retorna para cada segmento: n_lojas e abl_pct de cada shopping,
    delta_abl_pct (A − B) e delta_lojas (A − B).
    Útil para identificar onde cada shopping é mais forte ou mais fraco.
    """
    shopping_a = _validate_shopping(shopping_a)
    shopping_b = _validate_shopping(shopping_b)
    if not shopping_a or not shopping_b:
        raise ValueError("shopping_a e shopping_b são obrigatórios.")

    with get_db() as conn:
        def get_mix(sh: str) -> dict:
            n_tot = conn.execute(
                "SELECT COUNT(*) FROM store_mix WHERE shopping=? AND segment IS NOT NULL",
                [sh]
            ).fetchone()[0] or 1
            rows = conn.execute("""
                SELECT segment, COUNT(*) as n_lojas
                FROM store_mix WHERE shopping=? AND segment IS NOT NULL
                GROUP BY segment
            """, [sh]).fetchall()
            return {
                "n_total": n_tot,
                "segments": {
                    r["segment"]: {
                        "n_lojas": r["n_lojas"],
                        "n_lojas_pct": round(r["n_lojas"] * 100 / n_tot, 1),
                    }
                    for r in rows
                },
            }

        mix_a = get_mix(shopping_a)
        mix_b = get_mix(shopping_b)

    all_segs = sorted(set(mix_a["segments"]) | set(mix_b["segments"]))
    comparison = []
    for seg in all_segs:
        a = mix_a["segments"].get(seg, {"n_lojas": 0, "n_lojas_pct": 0.0})
        b = mix_b["segments"].get(seg, {"n_lojas": 0, "n_lojas_pct": 0.0})
        comparison.append({
            "segmento": seg,
            f"{shopping_a}_n_lojas": a["n_lojas"],
            f"{shopping_a}_pct": a["n_lojas_pct"],
            f"{shopping_b}_n_lojas": b["n_lojas"],
            f"{shopping_b}_pct": b["n_lojas_pct"],
            "delta_n_lojas": a["n_lojas"] - b["n_lojas"],
            "delta_pct": round(a["n_lojas_pct"] - b["n_lojas_pct"], 1),
        })

    comparison.sort(key=lambda x: abs(x["delta_pct"]), reverse=True)

    return {
        "shopping_a": shopping_a,
        "shopping_b": shopping_b,
        "resumo_a": {"n_lojas": mix_a["n_total"]},
        "resumo_b": {"n_lojas": mix_b["n_total"]},
        "comparacao_por_segmento": comparison,
    }


@mcp.tool()
def buscar_loja(
    nome: str = "",
    segmento: str = "",
    shopping: str = "",
    top_n: int = 50,
) -> list[dict]:
    """Busca lojas no store_mix por nome, segmento e/ou shopping.

    nome: busca parcial case-insensitive no nome da loja (ex: 'zara', 'starbucks')
    segmento: filtro exato por segmento (ex: 'Vestuário', 'Alimentação')
    shopping: filtro exato por shopping (ex: 'Pátio Batel', 'Village Mall')
    top_n: máximo de resultados (default 50, max 200)

    Retorna: lista de lojas com shopping, segmento, subsegmento, tipo e andar.
    Se nome vazio e segmento vazio, retorna as top_n primeiras lojas por ordem alfabética.
    """
    segmento = _validate_segment(segmento)
    shopping = _validate_shopping(shopping)
    top_n = min(int(top_n), 200)

    filters: list[str] = []
    params: list = []

    if nome:
        filters.append("UPPER(store_name) LIKE UPPER(?)")
        params.append(f"%{nome}%")
    if segmento:
        filters.append("segment = ?")
        params.append(segmento)
    if shopping:
        filters.append("shopping = ?")
        params.append(shopping)

    where = ("WHERE " + " AND ".join(filters)) if filters else ""

    with get_db() as conn:
        cur = conn.execute(f"""
            SELECT
                shopping,
                store_name as loja,
                segment as segmento,
                subsegment as subsegmento,
                store_type as tipo,
                floor as andar,
                CASE WHEN is_patio_batel=1 THEN 'sim' ELSE 'nao' END as is_patio_batel
            FROM store_mix
            {where}
            ORDER BY store_name
            LIMIT ?
        """, params + [top_n])
        return rows_to_dicts(cur)


@mcp.tool()
def mix_por_entidade(
    entidade: str,
    segmento: str = "",
) -> dict:
    """Breakdown do mix de lojas agregado por entidade/operadora.

    entidade: igti11 (Iguatemi) | mult3 (Multiplan) | jhsf3 (JHSF) — entidades com store_mix disponível
    segmento: filtro por segmento — vazio = todos

    ATENÇÃO: abl_pct confiável apenas para Pátio Batel. Para entidades peer, usar n_lojas e n_lojas_pct.

    Agrega automaticamente todos os shoppings da entidade no store_mix.
    Iguatemi (igti11): Iguatemi SP + Iguatemi Brasília + JK Iguatemi + Leblon (4 shoppings)
    Multiplan (mult3): Village Mall + Park Shopping Barigui (2 shoppings)
    JHSF: Cidade Jardim + CJ Shops (2 shoppings)

    Retorna: por segmento → n_lojas, abl_m2, abl_pct, n_lojas_pct + breakdown por shopping.
    """
    entidade = _validate_entity(entidade)
    segmento = _validate_segment(segmento)

    shoppings = ENTITY_SHOPPINGS.get(entidade)
    if not shoppings:
        raise ValueError(
            f"Entidade '{entidade}' sem store_mix disponível. "
            f"Entidades disponíveis: {', '.join(sorted(ENTITY_SHOPPINGS.keys()))}"
        )

    placeholders = ",".join("?" * len(shoppings))
    params: list = list(shoppings)

    seg_filter = ""
    if segmento:
        seg_filter = "AND segment = ?"
        params.append(segmento)

    with get_db() as conn:
        n_total = conn.execute(f"""
            SELECT COUNT(*) FROM store_mix
            WHERE shopping IN ({placeholders}) AND segment IS NOT NULL {seg_filter}
        """, params).fetchone()[0] or 1

        segs = conn.execute(f"""
            SELECT segment,
                   COUNT(*) as n_lojas,
                   GROUP_CONCAT(DISTINCT subsegment) as subsegmentos
            FROM store_mix
            WHERE shopping IN ({placeholders}) AND segment IS NOT NULL {seg_filter}
            GROUP BY segment ORDER BY n_lojas DESC
        """, params).fetchall()

        by_shopping = conn.execute(f"""
            SELECT shopping, COUNT(*) as n_lojas
            FROM store_mix
            WHERE shopping IN ({placeholders}) AND segment IS NOT NULL {seg_filter}
            GROUP BY shopping ORDER BY n_lojas DESC
        """, params).fetchall()

    breakdown = [
        {
            "segmento": s["segment"],
            "n_lojas": s["n_lojas"],
            "n_lojas_pct": round(s["n_lojas"] * 100 / n_total, 1),
            "subsegmentos": sorted(set(
                x.strip() for x in (s["subsegmentos"] or "").split(",") if x.strip()
            )),
        }
        for s in segs
    ]

    return {
        "entidade": entidade,
        "shoppings_incluidos": shoppings,
        "filtro_segmento": segmento or "todos",
        "n_lojas_total": n_total,
        "por_shopping": [dict(s) for s in by_shopping],
        "por_segmento": breakdown,
    }


# ── UTILITY TOOLS ──────────────────────────────────────────────────────────

@mcp.tool()
def query_sql(sql: str) -> list[dict]:
    """Executa uma query SQL read-only no benchmark.db.

    Apenas SELECT e WITH são permitidos. Limite automático de 500 linhas.
    Tabelas: entities, assets, asset_metrics, portfolio_metrics, entity_asset_stakes, ingestion_log
    Views:
      v_asset_ltm — PREFERIR ESTA para séries temporais. Recalcula sales_psqm e rent_psqm
                    como LTM real (soma 4 quarters / ABL). Colunas extras: sales_psqm_stored,
                    rent_psqm_stored, ltm_sales_total, ltm_rent_total, ltm_quarters_count,
                    psqm_methodology ('LTM' ou 'partial_NQ'), quarter_order.
      v_portfolio_clean, v_asset_full, v_premium_benchmark, v_curitiba_benchmark,
      v_yoy_portfolio, v_entities_coverage

    Regra de período: IGTI11/MULT3 = quarter '4Q25'; demais (ALOS3/XPML11/JHSF3/HGBS11/VISC11/BPML11) = quarter '3Q25'
    Convenção stake_pct: % 0.0–100.0. Zeros = não mapeado (tratar como NULL).
    sales_total / rent_total em asset_metrics = R$ mil @100% do ativo (trimestral isolado).
    sales_psqm em asset_metrics = R$/m²/quarter (valor bruto do trimestre). Usar v_asset_ltm para LTM anual.
    """
    sql_clean = sql.strip()
    sql_upper = sql_clean.upper()
    if not (sql_upper.startswith("SELECT") or sql_upper.startswith("WITH")):
        raise ValueError("Apenas queries SELECT/WITH são permitidas.")

    # ABL por loja do Pátio Batel é confidencial — bloquear acesso direto
    if "ABL_M2" in sql_upper and "STORE_MIX" in sql_upper:
        raise ValueError(
            "abl_m2 da tabela store_mix é dado confidencial do Pátio Batel e não está disponível via query_sql. "
            "Use mix_por_segmento, comparar_mix ou mix_por_entidade para análise de mix."
        )

    if "LIMIT" not in sql_upper:
        sql_clean = f"{sql_clean}\nLIMIT 500"

    with get_db() as conn:
        conn.execute("PRAGMA busy_timeout = 5000")
        cur = conn.execute(sql_clean)
        return rows_to_dicts(cur)


@mcp.tool()
def schema_banco() -> dict:
    """Retorna o schema completo do benchmark.db: tabelas, colunas e views.

    View principal para análise histórica: v_asset_ltm
    - sales_psqm e rent_psqm são LTM real (soma 4 quarters / ABL), em R$/m²/ano
    - sales_psqm_stored / rent_psqm_stored = valores originais da ingestão (Q×4 até 4Q24)
    - ltm_quarters_count: quantos quarters compõem o LTM (4 = completo)
    - psqm_methodology: 'LTM' quando 4 quarters disponíveis
    """
    with get_db() as conn:
        tables = conn.execute("""
            SELECT name, type FROM sqlite_master
            WHERE type IN ('table','view') AND name NOT LIKE 'sqlite_%'
            ORDER BY type DESC, name
        """).fetchall()
        result = {}
        for t in tables:
            cols = conn.execute(f"PRAGMA table_info('{t['name']}')").fetchall()
            result[t["name"]] = {
                "type": t["type"],
                "columns": [{"col": c["name"], "type": c["type"]} for c in cols]
            }
    return result


# ── OAUTH LOGIN ROUTES ────────────────────────────────────────────────────

if oauth_provider:
    from starlette.requests import Request
    from starlette.responses import HTMLResponse, RedirectResponse
    from urllib.parse import urlencode
    from mcp.server.auth.provider import construct_redirect_uri

    from string import Template

    LOGIN_TEMPLATE = Template("""<!DOCTYPE html>
<html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Benchmark Shoppings BR</title>
<style>
body{font-family:system-ui;display:flex;justify-content:center;align-items:center;min-height:100vh;margin:0;background:#f5f5f5}
.card{background:#fff;border-radius:12px;padding:2rem;box-shadow:0 2px 12px rgba(0,0,0,.1);max-width:360px;width:100%}
h2{margin:0 0 .5rem;font-size:1.2rem}
p{color:#666;font-size:.85rem;margin:0 0 1.5rem}
input[type=password]{width:100%;padding:.7rem;border:1px solid #ddd;border-radius:8px;font-size:1rem;box-sizing:border-box}
button{width:100%;padding:.7rem;background:#1a73e8;color:#fff;border:none;border-radius:8px;font-size:1rem;cursor:pointer;margin-top:.8rem}
button:hover{background:#1557b0}
.err{color:#d32f2f;font-size:.85rem;margin-top:.5rem}
</style></head><body>
<div class="card">
<h2>Benchmark Shoppings BR</h2>
<p>Dados de IR de shoppings brasileiros via MCP</p>
<form method="POST" action="/oauth/login">
<input type="hidden" name="client_id" value="$client_id">
<input type="hidden" name="redirect_uri" value="$redirect_uri">
<input type="hidden" name="code_challenge" value="$code_challenge">
<input type="hidden" name="state" value="$state">
<input type="hidden" name="scope" value="$scope">
<input type="hidden" name="redirect_uri_explicit" value="$redirect_uri_explicit">
<input type="password" name="password" placeholder="Senha" autofocus required>
<button type="submit">Entrar</button>
$error
</form></div></body></html>""")

    @mcp.custom_route("/oauth/login", methods=["GET", "POST"])
    async def oauth_login(request: Request):
        if request.method == "GET":
            params = request.query_params
            return HTMLResponse(LOGIN_TEMPLATE.safe_substitute(
                client_id=params.get("client_id", ""),
                redirect_uri=params.get("redirect_uri", ""),
                code_challenge=params.get("code_challenge", ""),
                state=params.get("state", ""),
                scope=params.get("scope", ""),
                redirect_uri_explicit=params.get("redirect_uri_explicit", "0"),
                error="",
            ))

        # POST — validate password
        form = await request.form()
        password = form.get("password", "")
        client_id = form.get("client_id", "")
        redirect_uri = form.get("redirect_uri", "")
        code_challenge = form.get("code_challenge", "")
        state = form.get("state", "")
        scope = form.get("scope", "")
        redirect_uri_explicit = form.get("redirect_uri_explicit", "0")

        if not verify_password(password):
            return HTMLResponse(LOGIN_TEMPLATE.safe_substitute(
                client_id=client_id,
                redirect_uri=redirect_uri,
                code_challenge=code_challenge,
                state=state,
                scope=scope,
                redirect_uri_explicit=redirect_uri_explicit,
                error='<p class="err">Senha incorreta</p>',
            ), status_code=401)

        # Password correct — create auth code and redirect
        scopes = scope.split() if scope else []
        code = oauth_provider.create_authorization_code(
            client_id=client_id,
            code_challenge=code_challenge,
            redirect_uri=redirect_uri,
            redirect_uri_provided_explicitly=redirect_uri_explicit == "1",
            scopes=scopes,
        )

        # Redirect back to client with code
        redirect_params = {"code": code}
        if state:
            redirect_params["state"] = state

        target = f"{redirect_uri}?{urlencode(redirect_params)}"
        return RedirectResponse(url=target, status_code=302)


if __name__ == "__main__":
    n_tools = len(mcp._tool_manager._tools)
    auth_status = "ON" if MCP_OAUTH_PASSWORD else "OFF"
    logger.info("benchmark-mcp iniciando na porta %d com %d tools | oauth: %s", PORT, n_tools, auth_status)
    mcp.run(transport="streamable-http")
