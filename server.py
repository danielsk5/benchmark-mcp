"""Benchmark Shoppings BR — MCP Server

Expõe dados públicos de IR de 5 grupos de shoppings brasileiros:
Iguatemi (IGTI11), Multiplan (MULT3), Allos (ALOS3), XP Malls (XPML11), JHSF (JHSF3)

Períodos:
- Iguatemi/Multiplan: FY2025 vs FY2024 (ano encerrado)
- Allos/XP Malls/JHSF: LTM 3Q25 vs LTM 3Q24 (soma últimos 4 trimestres)

Transporte: streamable-http (sem autenticação — dados públicos de IR)

Convenção stake_pct (entity_asset_stakes):
- Todos os grupos: stake_pct armazenado como decimal (0.0–1.0)
- Zeros (stake_pct = 0.0) indicam ativos sem participação mapeada — tratar como NULL
"""

import os
import sqlite3
import logging
from pathlib import Path
from contextlib import contextmanager

from mcp.server.fastmcp import FastMCP

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("benchmark-mcp")

DB_PATH = os.getenv("DB_PATH", str(Path(__file__).parent / "data" / "benchmark.db"))
PORT = int(os.getenv("PORT", "8000"))

mcp = FastMCP(
    name="benchmark-shoppings-br",
    instructions="""Banco de dados de benchmark de shoppings brasileiros com dados públicos de IR.
5 grupos: Iguatemi (IGTI11), Multiplan (MULT3), Allos (ALOS3), XP Malls (XPML11), JHSF (JHSF3).
Métricas por ativo: vendas/m², aluguel/m², ocupação, yield receita/venda.
Período ref: Iguatemi/Multiplan = FY2025; Allos/XP Malls/JHSF = LTM 3Q25.
Todos os valores monetários em R$ mil. Métricas /m² em R$/m²/ano.

Convenção stake_pct: decimal 0.0–1.0 para todos os grupos.
Colunas _100pct = 100% do shopping (fonte IR). Colunas _empresa = proporcional à participação.

Tools disponíveis (14):
- listar_entidades: lista as 5 empresas
- ranking_ativos: ranking por métrica
- detalhe_ativo: detalhe completo de um ativo
- portfolio_comparativo: período atual vs anterior
- serie_historica: evolução trimestral de um ativo
- comparar_ativos: side-by-side de 2–3 ativos
- top_movers: maiores altas/quedas YoY
- resumo_mercado: médias por estado ou categoria
- peer_group: encontra peers de um ativo referência
- concentracao_portfolio: HHI e concentração de receita
- gap_analysis: gaps de um ativo vs média da categoria
- scatter_data: pares (x,y) para gráficos
- query_sql: SQL livre read-only (max 500 linhas)
- schema_banco: schema completo""",
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
}

VALID_ENTITIES = set(ENTITY_PERIODS.keys())
VALID_STATES = {
    "AL", "AM", "BA", "CE", "DF", "ES", "GO", "MA", "MG", "MS",
    "MT", "PA", "PE", "PR", "RJ", "RN", "RS", "SC", "SP", "TO",
}
VALID_CATEGORIES = {"premium", "regional", "outlet", "power-center", "other"}

CURRENT_QUARTER_FILTER = """(
    (am.entity_id IN ('igti11','mult3') AND am.quarter = '4Q25')
    OR (am.entity_id IN ('alos3','xpml11','jhsf3') AND am.quarter = '3Q25')
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
    """Lista as 5 empresas no banco com cobertura e período de referência."""
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
    empresa: filtro por entity_id (igti11, mult3, alos3, xpml11, jhsf3) — vazio = todas
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
                eas.abl_total_sqm as abl_m2,
                ROUND(am.sales_total / 1000.0, 1) as vendas_100pct_r_mi,
                CASE WHEN eas.stake_pct > 0.0
                     THEN ROUND(am.sales_total * eas.stake_pct / 1000.0, 1)
                     ELSE NULL END as vendas_empresa_r_mi,
                CASE WHEN eas.stake_pct > 0.0
                     THEN ROUND(eas.stake_pct * 100.0, 2)
                     ELSE NULL END as participacao_pct,
                CASE am.entity_id
                    WHEN 'igti11' THEN 'FY2025' WHEN 'mult3' THEN 'FY2025'
                    ELSE 'LTM 3Q25' END as periodo_ref
            FROM asset_metrics am
            JOIN assets a ON a.id = am.asset_id
            JOIN entities e ON e.id = am.entity_id
            JOIN entity_asset_stakes eas
                ON eas.asset_id = am.asset_id AND eas.entity_id = am.entity_id
                AND eas.quarter = am.quarter
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
                CASE WHEN eas.stake_pct > 0.0
                     THEN ROUND(am.sales_total * eas.stake_pct, 0)
                     ELSE NULL END as vendas_empresa_r_mil,
                am.noi as noi_100pct_r_mil,
                CASE WHEN eas.stake_pct > 0.0
                     THEN ROUND(am.noi * eas.stake_pct, 0)
                     ELSE NULL END as noi_empresa_r_mil,
                am.rent_total as aluguel_100pct_r_mil,
                CASE WHEN eas.stake_pct > 0.0
                     THEN ROUND(am.rent_total * eas.stake_pct, 0)
                     ELSE NULL END as aluguel_empresa_r_mil,
                eas.abl_total_sqm as abl_total_m2, eas.abl_own_sqm as abl_propria_m2,
                CASE WHEN eas.stake_pct > 0.0
                     THEN ROUND(eas.stake_pct * 100.0, 4)
                     ELSE NULL END as participacao_pct
            FROM asset_metrics am
            JOIN assets a ON a.id = am.asset_id
            JOIN entities e ON e.id = am.entity_id
            JOIN entity_asset_stakes eas
                ON eas.asset_id = am.asset_id AND eas.entity_id = am.entity_id
                AND eas.quarter = am.quarter
            WHERE {CURRENT_QUARTER_FILTER}
            AND am.period_type = 'quarter'
            AND LOWER(a.name) LIKE LOWER(?)
            ORDER BY am.sales_psqm DESC NULLS LAST
        """, [f"%{nome_ativo}%"])
        return rows_to_dicts(cur)


@mcp.tool()
def portfolio_comparativo(empresa: str = "") -> list[dict]:
    """Comparativo de portfólio: período atual vs anterior por empresa.

    empresa: igti11 | mult3 | alos3 | xpml11 | jhsf3 — vazio = todas
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
    """Série histórica trimestral de um ativo.

    nome_ativo: busca parcial (ex: 'Village', 'Iguatemi SP')
    metrica: vendas_m2 | aluguel_m2 | ocupacao | yield_pct | sss | noi | noi_margin
    n_quarters: quantos trimestres retornar (max 44, default 12)

    Retorna em ordem cronológica (mais antigo primeiro).
    """
    n_quarters = min(int(n_quarters), 44)
    metric_sql = _validate_metric(metrica)

    with get_db() as conn:
        cur = conn.execute(f"""
            SELECT
                a.name as ativo, e.name as empresa,
                am.quarter as trimestre,
                {metric_sql} as valor,
                am.sales_psqm as vendas_m2,
                am.rent_psqm as aluguel_m2,
                am.occupancy_rate as ocupacao_pct,
                am.sss as sss_pct
            FROM asset_metrics am
            JOIN assets a ON a.id = am.asset_id
            JOIN entities e ON e.id = am.entity_id
            WHERE am.period_type = 'quarter'
            AND LOWER(a.name) LIKE LOWER(?)
            AND {metric_sql} IS NOT NULL
            ORDER BY {QUARTER_ORDER} DESC
            LIMIT ?
        """, [f"%{nome_ativo}%", n_quarters])
        rows = rows_to_dicts(cur)

    return list(reversed(rows))


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
                    eas.abl_total_sqm as abl_total_m2,
                    ROUND(am.sales_total / 1000.0, 1) as vendas_100pct_r_mi,
                    CASE WHEN eas.stake_pct > 0.0
                         THEN ROUND(eas.stake_pct * 100.0, 2)
                         ELSE NULL END as participacao_pct
                FROM asset_metrics am
                JOIN assets a ON a.id = am.asset_id
                JOIN entities e ON e.id = am.entity_id
                JOIN entity_asset_stakes eas
                    ON eas.asset_id = am.asset_id AND eas.entity_id = am.entity_id
                    AND eas.quarter = am.quarter
                WHERE {CURRENT_QUARTER_FILTER}
                AND am.period_type = 'quarter'
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

    metric_cur = metric_sql.replace("am.", "cur.")
    metric_prev = metric_sql.replace("am.", "prev.")

    current_prior = """(
        (cur.entity_id IN ('igti11','mult3') AND cur.quarter = '4Q25' AND prev.quarter = '4Q24')
        OR (cur.entity_id IN ('alos3','xpml11','jhsf3') AND cur.quarter = '3Q25' AND prev.quarter = '3Q24')
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
            FROM asset_metrics cur
            JOIN asset_metrics prev
                ON prev.asset_id = cur.asset_id AND prev.entity_id = cur.entity_id
                AND prev.period_type = 'quarter'
            JOIN assets a ON a.id = cur.asset_id
            JOIN entities e ON e.id = cur.entity_id
            WHERE cur.period_type = 'quarter'
            AND {current_prior}
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
            FROM asset_metrics am
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
            FROM asset_metrics am
            JOIN assets a ON a.id = am.asset_id
            WHERE {CURRENT_QUARTER_FILTER}
            AND am.period_type = 'quarter'
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
            FROM asset_metrics am
            JOIN assets a ON a.id = am.asset_id
            JOIN entities e ON e.id = am.entity_id
            WHERE {CURRENT_QUARTER_FILTER}
            AND am.period_type = 'quarter'
            AND {metric_sql} BETWEEN ? AND ?
            ORDER BY is_ref DESC, {metric_sql} DESC
        """, [ref_asset_id, ref_value, ref_value, lower, upper])
        return rows_to_dicts(cur)


@mcp.tool()
def concentracao_portfolio(empresa: str) -> dict:
    """Análise de concentração do portfólio de uma empresa.

    empresa: igti11 | mult3 | alos3 | xpml11 | jhsf3 (obrigatório)

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
            FROM asset_metrics am
            JOIN assets a ON a.id = am.asset_id
            WHERE {CURRENT_QUARTER_FILTER}
            AND am.period_type = 'quarter'
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
            FROM asset_metrics am
            JOIN assets a ON a.id = am.asset_id
            WHERE {CURRENT_QUARTER_FILTER}
            AND am.period_type = 'quarter'
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
            FROM asset_metrics am
            JOIN assets a ON a.id = am.asset_id
            JOIN entities e ON e.id = am.entity_id
            WHERE {where}
            ORDER BY {x_sql} DESC
        """, params)
        return rows_to_dicts(cur)


# ── UTILITY TOOLS ──────────────────────────────────────────────────────────

@mcp.tool()
def query_sql(sql: str) -> list[dict]:
    """Executa uma query SQL read-only no benchmark.db.

    Apenas SELECT e WITH são permitidos. Limite automático de 500 linhas.
    Tabelas: entities, assets, asset_metrics, portfolio_metrics, entity_asset_stakes, ingestion_log
    Views: v_portfolio_clean, v_asset_full, v_premium_benchmark, v_curitiba_benchmark, v_yoy_portfolio, v_entities_coverage

    Regra de período: Iguatemi/Multiplan = quarter '4Q25'; Allos/XP Malls/JHSF = quarter '3Q25'
    Convenção stake_pct: decimal 0.0–1.0. Zeros = não mapeado (tratar como NULL).
    """
    sql_clean = sql.strip()
    sql_upper = sql_clean.upper()
    if not (sql_upper.startswith("SELECT") or sql_upper.startswith("WITH")):
        raise ValueError("Apenas queries SELECT/WITH são permitidas.")

    if "LIMIT" not in sql_upper:
        sql_clean = f"{sql_clean}\nLIMIT 500"

    with get_db() as conn:
        conn.execute("PRAGMA busy_timeout = 5000")
        cur = conn.execute(sql_clean)
        return rows_to_dicts(cur)


@mcp.tool()
def schema_banco() -> dict:
    """Retorna o schema completo do benchmark.db: tabelas, colunas e views."""
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


if __name__ == "__main__":
    logger.info("benchmark-mcp iniciando na porta %d com %d tools", PORT, len(mcp._tool_manager._tools))
    mcp.run(transport="streamable-http")
