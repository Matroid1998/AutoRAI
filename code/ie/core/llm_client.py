"""
Thin wrapper around OpenAI-compatible LLM APIs.

Handles prompt sending, structured JSON response parsing,
retries, and error handling. Used by the unstructured pipeline
for schema-driven extraction.
"""

from __future__ import annotations

import json
import logging
import time
from typing import Any

from openai import OpenAI

logger = logging.getLogger(__name__)

# Default retry configuration
DEFAULT_MAX_RETRIES = 3
DEFAULT_RETRY_DELAY = 2.0  # seconds


class LLMClient:
    """
    Client for OpenAI-compatible LLM APIs.

    Supports OpenAI, OpenRouter, and any API that follows the
    OpenAI chat completions format.
    """

    def __init__(
        self,
        model: str,
        api_key: str,
        base_url: str = "https://openrouter.ai/api/v1",
        temperature: float = 0.0,
        max_tokens: int = 4096,
        max_retries: int = DEFAULT_MAX_RETRIES,
        retry_delay: float = DEFAULT_RETRY_DELAY,
    ):
        self.model = model
        self.temperature = temperature
        self.max_tokens = max_tokens
        self.max_retries = max_retries
        self.retry_delay = retry_delay

        self.client = OpenAI(
            api_key=api_key,
            base_url=base_url,
        )

        logger.info(f"LLMClient initialized: model={model}, base_url={base_url}")

    def generate(
        self,
        prompt: str,
        system_prompt: str = "",
        response_format: str = "json",
        temperature: float | None = None,
    ) -> str:
        """
        Send a prompt to the LLM and return the response text.

        Args:
            prompt: The user message content.
            system_prompt: Optional system message.
            response_format: "json" to request JSON output, "text" for plain text.
            temperature: Override instance temperature for this call.

        Returns:
            The LLM's response as a string.

        Raises:
            RuntimeError: If all retry attempts fail.
        """
        messages = []
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})
        messages.append({"role": "user", "content": prompt})

        kwargs: dict[str, Any] = {
            "model": self.model,
            "messages": messages,
            "temperature": temperature if temperature is not None else self.temperature,
            "max_tokens": self.max_tokens,
        }

        # Request JSON format if supported
        if response_format == "json":
            kwargs["response_format"] = {"type": "json_object"}

        last_error = None
        for attempt in range(1, self.max_retries + 1):
            try:
                response = self.client.chat.completions.create(**kwargs)
                content = response.choices[0].message.content
                if content is None:
                    raise ValueError("LLM returned empty content")

                logger.debug(
                    f"LLM response received: {len(content)} chars, "
                    f"model={response.model}, "
                    f"usage={response.usage}"
                )
                return content

            except Exception as e:
                last_error = e
                logger.warning(
                    f"LLM call failed (attempt {attempt}/{self.max_retries}): {e}"
                )
                if attempt < self.max_retries:
                    time.sleep(self.retry_delay * attempt)  # exponential backoff

        raise RuntimeError(
            f"LLM call failed after {self.max_retries} attempts. Last error: {last_error}"
        )

    def generate_json(
        self,
        prompt: str,
        system_prompt: str = "",
        temperature: float | None = None,
    ) -> dict | list:
        """
        Send a prompt and parse the response as JSON.

        Returns:
            Parsed JSON as a dict or list.

        Raises:
            json.JSONDecodeError: If the response is not valid JSON.
            RuntimeError: If the LLM call fails.
        """
        response_text = self.generate(
            prompt=prompt,
            system_prompt=system_prompt,
            response_format="json",
            temperature=temperature,
        )

        try:
            return json.loads(response_text)
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse LLM response as JSON: {e}")
            logger.debug(f"Raw response: {response_text[:500]}")
            raise
