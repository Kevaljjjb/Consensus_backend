"""
Consensus — Embedding helper.

Uses DeepInfra's OpenAI-compatible API with Qwen/Qwen3-Embedding-8B
to generate 1024-dimensional embeddings for listing descriptions.
"""

import os
from functools import lru_cache
from typing import List

import requests
from openai import OpenAI


_CLIENT: OpenAI | None = None


def _get_client() -> OpenAI:
    """Return a shared OpenAI client pointed at the DeepInfra endpoint."""
    global _CLIENT
    if _CLIENT is None:
        _CLIENT = OpenAI(
            api_key=os.environ.get("EMBEDDING_API_KEY") or os.environ.get("OPENAI_API_KEY"),
            base_url=os.environ.get("EMBEDDING_BASE_URL", "https://api.deepinfra.com/v1/openai"),
        )
    return _CLIENT


_MODEL = None


def _get_model() -> str:
    global _MODEL
    if _MODEL is None:
        _MODEL = os.environ.get("EMBEDDING_MODEL", "Qwen/Qwen3-Embedding-8B")
    return _MODEL


_DIMENSIONS = None


def _get_dimensions() -> int:
    global _DIMENSIONS
    if _DIMENSIONS is None:
        _DIMENSIONS = int(os.environ.get("EMBEDDING_DIMENSIONS", "1024"))
    return _DIMENSIONS


_EMBEDDING_CACHE_SIZE = int(os.environ.get("EMBEDDING_CACHE_SIZE", "256"))
_EMBEDDING_CACHE_MAX_TEXT_LEN = int(os.environ.get("EMBEDDING_CACHE_MAX_TEXT_LEN", "1024"))


@lru_cache(maxsize=_EMBEDDING_CACHE_SIZE)
def _get_embedding_cached(text: str, model: str, dimensions: int) -> tuple:
    """Cache exact query embeddings to avoid repeated remote calls."""
    response = _get_client().embeddings.create(
        model=model,
        input=text,
        dimensions=dimensions,
    )
    return tuple(response.data[0].embedding)


def get_embedding(text: str) -> List[float]:
    """
    Generate a single embedding vector for the given text.
    Returns a list of floats with length = EMBEDDING_DIMENSIONS (default 1024).
    """
    text = text.strip()
    if not text:
        return []

    if len(text) > _EMBEDDING_CACHE_MAX_TEXT_LEN:
        response = _get_client().embeddings.create(
            model=_get_model(),
            input=text,
            dimensions=_get_dimensions(),
        )
        return response.data[0].embedding

    return list(
        _get_embedding_cached(
            text=text,
            model=_get_model(),
            dimensions=_get_dimensions(),
        )
    )


def get_embeddings_batch(texts: List[str]) -> List[List[float]]:
    """
    Generate embeddings for a batch of texts in one API call.
    Returns a list of embedding vectors, one per input text.
    """
    if not texts:
        return []
    response = _get_client().embeddings.create(
        model=_get_model(),
        input=texts,
        dimensions=_get_dimensions(),
    )
    # Sort by index to preserve order
    sorted_data = sorted(response.data, key=lambda d: d.index)
    return [d.embedding for d in sorted_data]


def rerank_documents(query: str, documents: List[str]) -> List[float]:
    """
    Rerank documents against a query using Qwen3-Reranker-8B via DeepInfra.
    Returns a list of scores corresponding to each document.
    """
    if not documents:
        return []

    api_key = os.environ.get("EMBEDDING_API_KEY") or os.environ.get("OPENAI_API_KEY")
    rerank_model = os.environ.get("RERANK_MODEL", "Qwen/Qwen3-Reranker-8B")
    url = f"https://api.deepinfra.com/v1/inference/{rerank_model}"
    headers = {
        "Authorization": f"bearer {api_key}",
        "Content-Type": "application/json"
    }
    payload = {
        "queries": [query],
        "documents": documents
    }

    if not hasattr(rerank_documents, "_session"):
        rerank_documents._session = requests.Session()

    timeout = float(os.environ.get("RERANK_TIMEOUT_SECONDS", "10"))
    response = rerank_documents._session.post(
        url,
        headers=headers,
        json=payload,
        timeout=timeout,
    )
    response.raise_for_status()
    data = response.json()
    return data.get("scores", [])
