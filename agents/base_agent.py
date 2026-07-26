"""
Base Agent: shared LLM client, cost tracking, structured output parsing.

All agents inherit from this class and implement the `run()` method.
"""

import json
import time
from abc import ABC, abstractmethod

from openai import OpenAI

from config.settings import settings
from pipeline.utils import execute_sql, get_logger

log = get_logger("agent")

# Approximate pricing per 1M tokens (GPT-4o-mini as of 2024)
PRICING = {
    "gpt-4o-mini": {"input": 0.15, "output": 0.60},
    "gpt-4o": {"input": 2.50, "output": 10.00},
    "gpt-4.1-mini": {"input": 0.40, "output": 1.60},
    "gpt-4.1-nano": {"input": 0.10, "output": 0.40},
}


class BaseAgent(ABC):
    """Base class for all AI agents in the pipeline."""

    def __init__(self, name: str):
        self.name = name
        self.model = settings.OPENAI_MODEL
        self.client = OpenAI(api_key=settings.OPENAI_API_KEY) if settings.OPENAI_API_KEY else None
        self.total_tokens_in = 0
        self.total_tokens_out = 0
        self.total_cost = 0.0
        self.log = get_logger(f"agent.{name}")

    def call_llm(self, system_prompt: str, user_prompt: str,
                 temperature: float = 0.2, max_tokens: int = 4096) -> dict:
        """
        Call the LLM with structured JSON output.
        Returns parsed JSON response.
        Tracks token usage and cost.
        """
        if not self.client:
            self.log.warning("No OpenAI API key configured. Returning empty response.")
            return {}

        start = time.time()

        response = self.client.chat.completions.create(
            model=self.model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            temperature=temperature,
            max_tokens=max_tokens,
            response_format={"type": "json_object"},
        )

        latency_ms = int((time.time() - start) * 1000)

        # Extract usage
        usage = response.usage
        tokens_in = usage.prompt_tokens
        tokens_out = usage.completion_tokens

        # Calculate cost
        pricing = PRICING.get(self.model, PRICING["gpt-4o-mini"])
        cost = (tokens_in * pricing["input"] + tokens_out * pricing["output"]) / 1_000_000

        # Accumulate
        self.total_tokens_in += tokens_in
        self.total_tokens_out += tokens_out
        self.total_cost += cost

        # Log to DB
        self._log_metrics(tokens_in, tokens_out, cost, latency_ms,
                          action=f"{self.name}.call_llm")

        self.log.info(
            f"LLM call: {tokens_in} in + {tokens_out} out = "
            f"${cost:.6f} | {latency_ms}ms"
        )

        # Parse JSON response
        content = response.choices[0].message.content
        try:
            return json.loads(content)
        except json.JSONDecodeError:
            self.log.error(f"Failed to parse LLM response as JSON: {content[:200]}")
            return {"raw_response": content}

    def _log_metrics(self, tokens_in: int, tokens_out: int,
                     cost: float, latency_ms: int, action: str):
        """Log agent metrics to the database."""
        try:
            execute_sql(
                """
                INSERT INTO lineage.agent_metrics
                    (agent_name, action, tokens_in, tokens_out,
                     cost_usd, latency_ms, model)
                VALUES (:name, :action, :tin, :tout, :cost, :latency, :model)
                """,
                {
                    "name": self.name,
                    "action": action,
                    "tin": tokens_in,
                    "tout": tokens_out,
                    "cost": cost,
                    "latency": latency_ms,
                    "model": self.model,
                },
            )
        except Exception as e:
            self.log.warning(f"Failed to log metrics: {e}")

    def save_proposal(self, proposal_type: str, proposal: dict):
        """Save an agent proposal for human-in-the-loop review."""
        try:
            execute_sql(
                """
                INSERT INTO lineage.agent_proposals
                    (agent_name, proposal_type, proposal, status)
                VALUES (:name, :ptype, :proposal, 'PENDING')
                """,
                {
                    "name": self.name,
                    "ptype": proposal_type,
                    "proposal": json.dumps(proposal),
                },
            )
            self.log.info(f"Saved proposal ({proposal_type}) for review")
        except Exception as e:
            self.log.warning(f"Failed to save proposal: {e}")

    def print_cost_summary(self):
        """Print total cost summary for this agent."""
        self.log.info(
            f"=== {self.name} Cost Summary ===\n"
            f"  Tokens in:  {self.total_tokens_in:,}\n"
            f"  Tokens out: {self.total_tokens_out:,}\n"
            f"  Total cost: ${self.total_cost:.6f}\n"
            f"  Model:      {self.model}"
        )

    @abstractmethod
    def run(self, **kwargs) -> dict:
        """Execute the agent's main task. Returns results as a dict."""
        ...
