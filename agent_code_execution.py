"""Benchmark Shoppings BR — Code Execution Alternative (sem MCP)

Substitui as 14 tools MCP por execução direta de código Python.
O agente recebe um system prompt com schema + convenções do banco,
e escreve queries SQL sob demanda usando um único executor.

Vantagens sobre MCP (conforme artigo):
- Custo de contexto O(1): nenhuma tool definition injetada
- Cobertura total da API: o agente acessa qualquer tabela/view/coluna
- Composabilidade: pode encadear queries, transformar dados, gerar gráficos
- Auto-evolução: salva scripts úteis em disco para reuso

Uso:
    python agent_code_execution.py                   # modo interativo
    python agent_code_execution.py --query "..."     # query única
    python agent_code_execution.py --script file.py  # executa script salvo
"""

import os
import sys
import json
import sqlite3
import traceback
from pathlib import Path
from contextlib import contextmanager

DB_PATH = os.getenv("DB_PATH", str(Path(__file__).parent / "data" / "benchmark.db"))
SCRIPTS_DIR = Path(__file__).parent / "saved_scripts"
SCRIPTS_DIR.mkdir(exist_ok=True)


# ── DATABASE ACCESS ──────────────────────────────────────────────────────────

@contextmanager
def get_db():
    """Abre conexão read-only com benchmark.db."""
    if not Path(DB_PATH).exists():
        raise FileNotFoundError(f"benchmark.db não encontrado: {DB_PATH}")
    conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro&immutable=1", uri=True)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()


def query(sql: str, params: list | tuple = ()) -> list[dict]:
    """Executa SELECT/WITH e retorna lista de dicts. Max 500 linhas."""
    sql_clean = sql.strip()
    if not (sql_clean.upper().startswith("SELECT") or sql_clean.upper().startswith("WITH")):
        raise ValueError("Apenas SELECT/WITH permitidos.")
    if "LIMIT" not in sql_clean.upper():
        sql_clean += "\nLIMIT 500"
    with get_db() as conn:
        cur = conn.execute(sql_clean, params)
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


def schema() -> dict:
    """Retorna schema completo do banco."""
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
                "columns": [{"col": c["name"], "type": c["type"]} for c in cols],
            }
    return result


# ── CODE EXECUTION SANDBOX ───────────────────────────────────────────────────

def execute_code(code: str) -> dict:
    """Executa código Python com acesso ao banco.

    O código tem acesso a:
    - query(sql, params): executa SQL read-only
    - schema(): retorna schema do banco
    - json, sqlite3, math, statistics, collections, itertools
    - Variável `result` para retornar dados ao chamador

    Returns:
        {"success": True, "result": ..., "stdout": "..."}
        ou {"success": False, "error": "...", "stdout": "..."}
    """
    import io
    import math
    import statistics
    import collections
    import itertools

    # Captura stdout
    stdout_capture = io.StringIO()

    # Namespace de execução
    exec_globals = {
        "__builtins__": __builtins__,
        "query": query,
        "schema": schema,
        "json": json,
        "math": math,
        "statistics": statistics,
        "collections": collections,
        "itertools": itertools,
        "sqlite3": sqlite3,
        "print": lambda *args, **kwargs: print(*args, **kwargs, file=stdout_capture),
        "result": None,
    }

    try:
        exec(code, exec_globals)
        return {
            "success": True,
            "result": exec_globals.get("result"),
            "stdout": stdout_capture.getvalue(),
        }
    except Exception:
        return {
            "success": False,
            "error": traceback.format_exc(),
            "stdout": stdout_capture.getvalue(),
        }


# ── SCRIPT PERSISTENCE ──────────────────────────────────────────────────────

def save_script(name: str, code: str, description: str = "") -> Path:
    """Salva um script para reuso futuro."""
    path = SCRIPTS_DIR / f"{name}.py"
    header = f'"""{description}"""\n\n' if description else ""
    path.write_text(header + code, encoding="utf-8")
    return path


def load_script(name: str) -> str:
    """Carrega um script salvo."""
    path = SCRIPTS_DIR / f"{name}.py"
    if not path.exists():
        raise FileNotFoundError(f"Script '{name}' não encontrado em {SCRIPTS_DIR}")
    return path.read_text(encoding="utf-8")


def list_scripts() -> list[str]:
    """Lista scripts salvos."""
    return [p.stem for p in SCRIPTS_DIR.glob("*.py")]


# ── SYSTEM PROMPT PARA O AGENTE ──────────────────────────────────────────────

SYSTEM_PROMPT = """Você é um analista de dados de shoppings brasileiros.
Você tem acesso a um banco SQLite read-only com dados públicos de IR de 5 grupos:

Empresas (entity_id → ticker):
- igti11 → IGTI11 (Iguatemi) — período ref: FY2025 (quarter='4Q25')
- mult3  → MULT3  (Multiplan) — período ref: FY2025 (quarter='4Q25')
- alos3  → ALOS3  (Allos)     — período ref: LTM 3Q25 (quarter='3Q25')
- xpml11 → XPML11 (XP Malls)  — período ref: LTM 3Q25 (quarter='3Q25')
- jhsf3  → JHSF3  (JHSF)      — período ref: LTM 3Q25 (quarter='3Q25')

TABELAS PRINCIPAIS:
- entities (id, name, ticker, type)
- assets (id, name, city, state, category, entity_id)
  - category: premium, regional, outlet, power-center, other
- asset_metrics (entity_id, asset_id, quarter, period_type, sales_psqm, rent_psqm,
  occupancy_rate, sss, noi, noi_margin, sales_total, rent_total, occ_cost_pct, default_rate, ebitda)
- portfolio_metrics (entity_id, quarter, period_type, sales_total, noi, ebitda,
  ebitda_margin, ffo_margin, sss, ssr, occupancy_rate, abl_own_sqm, total_assets, rent_psqm)
- entity_asset_stakes (entity_id, asset_id, quarter, stake_pct, abl_total_sqm, abl_own_sqm)

VIEWS: v_portfolio_clean, v_asset_full, v_premium_benchmark, v_curitiba_benchmark,
       v_yoy_portfolio, v_entities_coverage

CONVENÇÕES:
- stake_pct: decimal 0.0–1.0. Valor 0.0 = não mapeado (tratar como NULL)
- Colunas _100pct = 100% do shopping. Colunas _empresa = proporcional à participação
- Valores monetários em R$ mil. Métricas /m² em R$/m²/ano
- yield = rent_psqm / sales_psqm * 100
- Filtro período atual: (entity_id IN ('igti11','mult3') AND quarter='4Q25')
  OR (entity_id IN ('alos3','xpml11','jhsf3') AND quarter='3Q25')

FUNÇÕES DISPONÍVEIS:
- query(sql, params=()): executa SQL read-only, retorna list[dict]
- schema(): retorna schema completo
- save_script(name, code, description): salva script para reuso
- load_script(name): carrega script salvo
- list_scripts(): lista scripts disponíveis

Para responder perguntas, escreva código Python que usa query() e atribua o
resultado final à variável `result`. Use print() para saída intermediária.

Exemplo — ranking top 10 vendas/m²:
```python
result = query('''
    SELECT a.name, e.name as empresa, am.sales_psqm
    FROM asset_metrics am
    JOIN assets a ON a.id = am.asset_id
    JOIN entities e ON e.id = am.entity_id
    WHERE (
        (am.entity_id IN ('igti11','mult3') AND am.quarter = '4Q25')
        OR (am.entity_id IN ('alos3','xpml11','jhsf3') AND am.quarter = '3Q25')
    )
    AND am.period_type = 'quarter'
    AND am.sales_psqm IS NOT NULL
    ORDER BY am.sales_psqm DESC
    LIMIT 10
''')
```
"""


# ── CLI ──────────────────────────────────────────────────────────────────────

def print_result(result: dict):
    """Formata e imprime resultado da execução."""
    if result["stdout"]:
        print(result["stdout"], end="")
    if result["success"]:
        if result["result"] is not None:
            if isinstance(result["result"], (list, dict)):
                print(json.dumps(result["result"], ensure_ascii=False, indent=2, default=str))
            else:
                print(result["result"])
    else:
        print(f"ERRO:\n{result['error']}", file=sys.stderr)


def interactive_mode():
    """Modo interativo — REPL para execução de código."""
    print("Benchmark Shoppings BR — Code Execution Mode")
    print(f"Banco: {DB_PATH}")
    print(f"Scripts salvos: {SCRIPTS_DIR}")
    print()
    print("Comandos especiais:")
    print("  .schema          — mostra schema do banco")
    print("  .scripts         — lista scripts salvos")
    print("  .run <nome>      — executa script salvo")
    print("  .save <nome>     — salva último código executado")
    print("  .prompt          — mostra system prompt para LLM")
    print("  .quit            — sai")
    print()
    print("Digite código Python (linha vazia para executar, ou cole bloco com Ctrl+D):")
    print()

    last_code = ""

    while True:
        try:
            lines = []
            while True:
                prompt = ">>> " if not lines else "... "
                line = input(prompt)
                if line == "" and lines:
                    break
                lines.append(line)

            code = "\n".join(lines).strip()
            if not code:
                continue

            # Comandos especiais
            if code == ".schema":
                print(json.dumps(schema(), ensure_ascii=False, indent=2))
                continue
            if code == ".scripts":
                scripts = list_scripts()
                print(f"Scripts salvos ({len(scripts)}):", ", ".join(scripts) if scripts else "(nenhum)")
                continue
            if code.startswith(".run "):
                name = code[5:].strip()
                try:
                    saved = load_script(name)
                    print(f"--- executando {name}.py ---")
                    print_result(execute_code(saved))
                except FileNotFoundError as e:
                    print(str(e))
                continue
            if code.startswith(".save "):
                name = code[6:].strip()
                if not last_code:
                    print("Nenhum código executado ainda.")
                    continue
                desc = input("Descrição (opcional): ").strip()
                path = save_script(name, last_code, desc)
                print(f"Salvo: {path}")
                continue
            if code == ".prompt":
                print(SYSTEM_PROMPT)
                continue
            if code in (".quit", ".exit"):
                break

            # Executa código
            last_code = code
            print_result(execute_code(code))

        except (EOFError, KeyboardInterrupt):
            print()
            break


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Benchmark Shoppings BR — Code Execution (sem MCP)")
    parser.add_argument("--query", "-q", help="Executa SQL direto e imprime resultado")
    parser.add_argument("--code", "-c", help="Executa código Python")
    parser.add_argument("--script", "-s", help="Executa script salvo por nome")
    parser.add_argument("--schema", action="store_true", help="Imprime schema do banco")
    parser.add_argument("--prompt", action="store_true", help="Imprime system prompt para LLM")
    parser.add_argument("--list-scripts", action="store_true", help="Lista scripts salvos")
    args = parser.parse_args()

    if args.schema:
        print(json.dumps(schema(), ensure_ascii=False, indent=2))
    elif args.prompt:
        print(SYSTEM_PROMPT)
    elif args.list_scripts:
        for s in list_scripts():
            print(s)
    elif args.query:
        try:
            rows = query(args.query)
            print(json.dumps(rows, ensure_ascii=False, indent=2, default=str))
        except Exception as e:
            print(f"Erro: {e}", file=sys.stderr)
            sys.exit(1)
    elif args.code:
        print_result(execute_code(args.code))
    elif args.script:
        try:
            code = load_script(args.script)
            print_result(execute_code(code))
        except FileNotFoundError as e:
            print(str(e), file=sys.stderr)
            sys.exit(1)
    else:
        interactive_mode()


if __name__ == "__main__":
    main()
