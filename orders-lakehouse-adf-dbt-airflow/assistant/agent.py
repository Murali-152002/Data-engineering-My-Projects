"""
Pipeline Run Assistant: a small tool-calling loop over the orders lakehouse.

The model only chooses which tool to call. The tools (assistant/tools.py) are
read-only and bounded, every tool call and its result is printed so the answer
can be checked, and the loop stops after MAX_STEPS.
"""
import json
import os
import sys

import requests

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from assistant.tools import TOOLS, TOOL_SCHEMAS, UnsafeQuery  # noqa: E402

MAX_STEPS = 6
SYSTEM_PROMPT = (
    "You are a data pipeline assistant for an orders lakehouse (bronze -> dbt staging -> "
    "star-schema marts). Answer ONLY from tool results. Call describe_tables before writing "
    "SQL. If a tool returns an error or no data, say so; never guess numbers. "
    "Mention the SQL you ran.")


class OllamaClient:
    """Local LLM through Ollama's /api/chat (no API key, nothing leaves the machine)."""

    def __init__(self, model=None, host=None):
        self.model = model or os.environ.get("OLLAMA_MODEL", "llama3.1")
        self.host = host or os.environ.get("OLLAMA_HOST", "http://127.0.0.1:11434")

    def chat(self, messages):
        resp = requests.post(
            f"{self.host}/api/chat",
            json={"model": self.model, "messages": messages, "tools": TOOL_SCHEMAS,
                  "stream": False},
            timeout=300)
        resp.raise_for_status()
        return resp.json()["message"]


def call_tool(name, args):
    """Run one tool; errors go back to the model as data instead of crashing the loop."""
    fn = TOOLS.get(name)
    if fn is None:
        return {"error": f"unknown tool {name}"}
    try:
        return fn(**(args or {}))
    except UnsafeQuery as e:
        return {"error": f"query refused: {e}"}
    except Exception as e:  # noqa: BLE001 - report to the model, keep the loop alive
        return {"error": f"{type(e).__name__}: {e}"}


def ask(question, client, verbose=True):
    messages = [{"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": question}]
    for _ in range(MAX_STEPS):
        msg = client.chat(messages)
        messages.append(msg)
        calls = msg.get("tool_calls") or []
        if not calls:
            return msg.get("content", "")
        for c in calls:
            fn = c["function"]
            args = fn.get("arguments") or {}
            if isinstance(args, str):
                args = json.loads(args)
            result = call_tool(fn["name"], args)
            if verbose:
                print(f"[tool] {fn['name']}({json.dumps(args)})")
            messages.append({"role": "tool", "name": fn["name"],
                             "content": json.dumps(result, default=str)})
    return "Stopped: step limit reached before a final answer."


if __name__ == "__main__":
    q = " ".join(sys.argv[1:]) or "How many orders do we have per status?"
    print(ask(q, OllamaClient()))
