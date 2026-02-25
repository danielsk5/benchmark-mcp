"""Benchmark Shoppings BR — MCP Server

Expõe dados públicos de IR de 5 grupos de shoppings brasileiros:
Iguatemi (IGTI11), Multiplan (MULT3), Allos (ALOS3), XP Malls (XPML11), JHSF (JHSF3)

Períodos:
- Iguatemi/Multiplan: FY2025 vs FY2024 (ano encerrado)
- Allos/XP Malls/JHSF: LTM 3Q25 vs LTM 3Q24 (soma últimos 4 trimestres)

Transporte: streamable-http (sem autenticação — dados públicos de IR)
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
Todos os valores monetários em R$ mil. Métricas /m² em R$/m²/ano.""",
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


# ── ENTITIES REFERENCE ──────────────────────────────────────────────────────

ENTITY_PERIODS = {
    "igti11": ("4Q25", "FY2025"),
    "mult3":  ("4Q25", "FY2025"),
    "alos3":  ("3Q25", "LTM 3Q25"),
    "xpml11": ("3Q25", "LTM 3Q25"),
    "jhsf3":  ("3Q25", "LTM 3Q25"),
}

CURRENT_QUARTER_FILTER = """(
    (am.entity_id IN ('igti11','mult3') AND am.quarter = '4Q25')
    OR (am.entity_id IN ('alos3','xpml11','jhsf3') AND am.quarter = '3Q25')
)"""


# ── TOOLS ───────────────────────────────────────────────────────────────────

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
        r["periodo_ref"] = ENTITY_PERIODS.get(r["id"], ("?","?"))[1]
    return result


@mcp.tool()
def ranking_ativos(
    metrica: str = "vendas_m2",
    top_n: int = 30,
    empresa: str = "",
    estado: str = "",
) -> list[dict]:
    """Ranking de ativos por métrica.

    metrica: vendas_m2 | aluguel_m2 | ocupacao | yield_pct
    empresa: filtro por entity_id (igti11, mult3, alos3, xpml11, jhsf3) — vazio = todas
    estado: filtro por UF (ex: PR, SP, RJ) — vazio = todos
    top_n: quantos retornar (max 100)

    Período: FY2025 para Iguatemi/Multiplan; LTM 3Q25 para Allos/XP Malls/JHSF
    """
    top_n = min(int(top_n), 100)
    metric_sql = {
        "vendas_m2":  "am.sales_psqm",
        "aluguel_m2": "am.rent_psqm",
        "ocupacao":   "am.occupancy_rate",
        "yield_pct":  "ROUND(am.rent_psqm * 100.0 / NULLIF(am.sales_psqm,0), 2)",
    }.get(metrica, "am.sales_psqm")

    filters = [CURRENT_QUARTER_FILTER, f"{metric_sql} IS NOT NULL"]
    if empresa:
        filters.append(f"am.entity_id = '{empresa}'")
    if estado:
        filters.append(f"a.state = '{estado.upper()}'")

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
            LIMIT {top_n}
        """)
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
                am.sales_total as vendas_total_r_mil,
                am.noi as noi_r_mil,
                eas.abl_total_sqm as abl_total_m2, eas.abl_own_sqm as abl_propria_m2,
                eas.stake_pct as participacao_pct
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
    fy_filter = "pm.entity_id IN ('igti11','mult3') AND pm.quarter IN ('FY25','FY24') AND pm.period_type='annual'"
    ltm_filter = """pm.entity_id IN ('alos3','xpml11','jhsf3')
        AND pm.quarter IN ('4Q23','1Q24','2Q24','3Q24','4Q24','1Q25','2Q25','3Q25')
        AND pm.period_type='quarter'"""

    if empresa:
        ent = f"pm.entity_id = '{empresa}' AND"
        fy_filter = f"{ent} pm.quarter IN ('FY25','FY24') AND pm.period_type='annual'"
        ltm_filter = f"{ent} pm.quarter IN ('4Q23','1Q24','2Q24','3Q24','4Q24','1Q25','2Q25','3Q25') AND pm.period_type='quarter'"

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
            WHERE {fy_filter}
            ORDER BY e.name, pm.quarter
        """)
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
            WHERE {ltm_filter}
            GROUP BY pm.entity_id, periodo
            ORDER BY e.name, periodo
        """)
        ltm_rows = rows_to_dicts(cur_ltm)

    return fy_rows + ltm_rows


@mcp.tool()
def query_sql(sql: str) -> list[dict]:
    """Executa uma query SQL read-only no benchmark.db.

    Apenas SELECT e WITH são permitidos.
    Tabelas: entities, assets, asset_metrics, portfolio_metrics, entity_asset_stakes, ingestion_log
    Views: v_portfolio_clean, v_asset_full, v_premium_benchmark, v_curitiba_benchmark, v_yoy_portfolio, v_entities_coverage

    Regra de período: Iguatemi/Multiplan = quarter '4Q25'; Allos/XP Malls/JHSF = quarter '3Q25'
    """
    sql_upper = sql.strip().upper()
    if not (sql_upper.startswith("SELECT") or sql_upper.startswith("WITH")):
        raise ValueError("Apenas queries SELECT/WITH são permitidas.")
    with get_db() as conn:
        cur = conn.execute(sql)
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
    logger.info("benchmark-mcp iniciando na porta %d", PORT)
    mcp.run(transport="streamable-http")
