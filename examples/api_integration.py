"""Exemplo: integração com Claude API usando code execution em vez de MCP.

Em vez de 14 tools MCP, passa 1 tool (execute_code) + system prompt com schema.
O modelo escreve Python/SQL sob demanda.

Requisitos: pip install anthropic
"""

import anthropic
import json
import sys
sys.path.insert(0, "..")
from agent_code_execution import execute_code, SYSTEM_PROMPT

client = anthropic.Anthropic()  # usa ANTHROPIC_API_KEY do env

# Uma única tool definition — substitui as 14 do MCP
TOOLS = [
    {
        "name": "execute_code",
        "description": (
            "Executa código Python com acesso ao banco SQLite de shoppings. "
            "Use query(sql) para consultas e atribua resultado a `result`. "
            "Bibliotecas disponíveis: json, math, statistics, collections, itertools."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "code": {
                    "type": "string",
                    "description": "Código Python a executar. Use query(sql) para acessar o banco.",
                }
            },
            "required": ["code"],
        },
    }
]


def chat(user_message: str) -> str:
    """Faz uma pergunta ao agente com agentic loop (tool use)."""

    messages = [{"role": "user", "content": user_message}]

    while True:
        response = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=4096,
            system=SYSTEM_PROMPT,
            tools=TOOLS,
            messages=messages,
        )

        # Se não tem tool use, retorna texto final
        if response.stop_reason == "end_of_turn":
            return "".join(
                block.text for block in response.content if block.type == "text"
            )

        # Processa tool calls
        messages.append({"role": "assistant", "content": response.content})

        tool_results = []
        for block in response.content:
            if block.type == "tool_use":
                result = execute_code(block.input["code"])
                tool_results.append(
                    {
                        "type": "tool_result",
                        "tool_use_id": block.id,
                        "content": json.dumps(result, ensure_ascii=False, default=str),
                    }
                )

        messages.append({"role": "user", "content": tool_results})


if __name__ == "__main__":
    pergunta = sys.argv[1] if len(sys.argv) > 1 else "Quais os 5 shoppings com maior vendas/m²?"
    print(f"Pergunta: {pergunta}\n")
    resposta = chat(pergunta)
    print(resposta)
