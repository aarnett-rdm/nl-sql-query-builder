"""
schema_retriever.py

BM25-based schema chunk retriever for LLM context window management.

Adapted from archive/hybrid_retriever.py for the active tools package.

Given a user question, retrieves the most relevant schema chunks
(table descriptions, column metadata, usage patterns) to inject
into the LLM system prompt. This keeps the context window focused
on relevant tables rather than dumping all 200+ tables.

Dependencies:
  - rank_bm25 (pip install rank-bm25)
  - Embedding support is optional (BM25-only mode works without it)
"""

from __future__ import annotations

import json
import math
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

try:
    from rank_bm25 import BM25Okapi
    _HAS_BM25 = True
except ImportError:
    _HAS_BM25 = False


# -------------------------
# Metric synonym vocabulary
# -------------------------

METRIC_SYNONYMS: Dict[str, List[str]] = {
    "cpc": ["cpc", "cost per click", "avg cpc", "average cpc"],
    "cpa": ["cpa", "cost per action", "cost per acquisition", "cost per conversion"],
    "cpm": ["cpm", "cost per thousand", "cost per 1000 impressions"],
    "ctr": ["ctr", "clickthrough rate", "click-through rate"],
    "conversion_rate": ["conversion rate", "conv rate", "conversion %", "cr"],
    "roas": ["roas", "return on ad spend"],
    "roi": ["roi", "return on investment"],
    "impressions": ["impressions", "impr"],
    "clicks": ["clicks", "click volume"],
    "spend": ["spend", "cost", "ad spend"],
    "revenue": ["revenue", "sales"],
    "profit": ["profit", "margin"],
}


# -------------------------
# Data structures
# -------------------------

@dataclass
class Chunk:
    """Single chunk from semantic_chunks JSONL corpora."""
    id: str
    text: str
    corpus: str
    metadata: Dict[str, Any] = field(default_factory=dict)

    def tables(self) -> List[str]:
        tables = self.metadata.get("tables") or self.metadata.get("table_names") or []
        if isinstance(tables, str):
            tables = [tables]
        return [t.lower() for t in tables]

    def metrics(self) -> List[str]:
        metrics = self.metadata.get("metrics") or self.metadata.get("measures") or []
        if isinstance(metrics, str):
            metrics = [metrics]
        return [m.lower() for m in metrics]

    def platforms(self) -> List[str]:
        platforms = self.metadata.get("platforms") or self.metadata.get("platform") or []
        if isinstance(platforms, str):
            platforms = [platforms]
        return [p.lower() for p in platforms]


@dataclass
class RetrievalConfig:
    corpus_weights: Dict[str, float] = field(default_factory=lambda: {
        "core": 1.0,
        "usage": 0.7,
        "qa": 0.4,
        "core_from_doc": 0.3,
    })
    rrf_k: int = 10
    bm25_k: int = 40
    embed_k: int = 40
    final_k: int = 12
    table_match_boost: float = 0.15
    metric_match_boost: float = 0.12
    metric_priority_boosts: Dict[str, float] = field(default_factory=lambda: {
        "cpa": 0.06, "cpc": 0.04, "cpm": 0.03, "ctr": 0.03,
        "roas": 0.06, "roi": 0.05, "conversion_rate": 0.04,
    })
    platform_match_boost: float = 0.10
    platform_mismatch_penalty: float = -0.08
    high_conf_min_rrf: float = 0.02
    medium_conf_min_rrf: float = 0.01


# -------------------------
# Embedding client (optional)
# -------------------------

class EmbeddingClient:
    """Abstract interface for plugging in a real embedding model."""
    def embed(self, texts: List[str]) -> List[List[float]]:
        raise NotImplementedError


def cosine_similarity(a: List[float], b: List[float]) -> float:
    if not a or not b:
        return 0.0
    n = min(len(a), len(b))
    a, b = a[:n], b[:n]
    dot = sum(x * y for x, y in zip(a, b))
    norm_a = math.sqrt(sum(x * x for x in a)) or 1e-9
    norm_b = math.sqrt(sum(y * y for y in b)) or 1e-9
    return dot / (norm_a * norm_b)


# -------------------------
# Corpus loading
# -------------------------

def simple_tokenize(text: str) -> List[str]:
    return [t.lower() for t in text.split() if t.strip()]


def load_jsonl(path: Path, corpus_name: str) -> List[Chunk]:
    chunks: List[Chunk] = []
    with path.open("r", encoding="utf-8") as f:
        for i, line in enumerate(f):
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            cid = obj.get("id") or f"{corpus_name}-{i}"
            text = obj.get("text") or obj.get("body") or ""
            meta = {k: v for k, v in obj.items() if k not in ("id", "text", "body")}
            chunks.append(Chunk(id=cid, text=text, corpus=corpus_name, metadata=meta))
    return chunks


def load_all_corpora(base_dir: Path) -> List[Chunk]:
    paths = {
        "core": base_dir / "semantic_chunks.core.jsonl",
        "usage": base_dir / "semantic_chunks.usage.jsonl",
        "qa": base_dir / "semantic_chunks.qa.jsonl",
        "core_from_doc": base_dir / "semantic_chunks.core.from_doc.jsonl",
    }
    all_chunks: List[Chunk] = []
    for corpus_name, path in paths.items():
        if path.exists():
            all_chunks.extend(load_jsonl(path, corpus_name))
    return all_chunks


# -------------------------
# BM25 index
# -------------------------

class BM25Index:
    def __init__(self, chunks: List[Chunk]):
        if not _HAS_BM25:
            raise ImportError("rank_bm25 is required: pip install rank-bm25")
        self.chunks = chunks
        self.tokenized_docs = [simple_tokenize(c.text) for c in chunks]
        self.bm25 = BM25Okapi(self.tokenized_docs)

    def search(self, query: str, k: int) -> List[Tuple[int, float]]:
        q_tokens = simple_tokenize(query)
        scores = self.bm25.get_scores(q_tokens)
        ranked = sorted(enumerate(scores), key=lambda x: x[1], reverse=True)
        return ranked[:k]


# -------------------------
# Embedding index (optional)
# -------------------------

class EmbeddingIndex:
    def __init__(self, chunks: List[Chunk], embed_client: EmbeddingClient):
        self.chunks = chunks
        self.embed_client = embed_client
        self.embeddings = self.embed_client.embed([c.text for c in chunks])

    def search(self, query: str, k: int) -> List[Tuple[int, float]]:
        [q_vec] = self.embed_client.embed([query])
        sims = [cosine_similarity(q_vec, doc_vec) for doc_vec in self.embeddings]
        ranked = sorted(enumerate(sims), key=lambda x: x[1], reverse=True)
        return ranked[:k]


# -------------------------
# Platform helpers
# -------------------------

def infer_platform_for_chunk(chunk: Chunk) -> Optional[str]:
    plats = chunk.platforms()
    if plats:
        return plats[0].lower()
    name = chunk.id.lower()
    if "googleads" in name:
        return "google_ads"
    if "microsoftads" in name or "bingads" in name:
        return "microsoft_ads"
    return None


def normalize_platform(p: Optional[str]) -> Optional[str]:
    if p is None:
        return None
    p = p.lower().replace("_ads", "")
    if "google" in p:
        return "google"
    if "microsoft" in p or "bing" in p:
        return "microsoft"
    if "meta" in p or "facebook" in p:
        return "meta"
    return p


# -------------------------
# Hybrid retriever
# -------------------------

class HybridRetriever:
    """
    BM25 + optional embeddings with RRF fusion and metadata boosts.
    If embed_client is None, runs BM25-only.
    """

    def __init__(
        self,
        chunks: List[Chunk],
        embed_client: Optional[EmbeddingClient] = None,
        config: Optional[RetrievalConfig] = None,
    ):
        self.config = config or RetrievalConfig()
        self.chunks = chunks
        self.bm25_index = BM25Index(chunks)

        self.embed_index: Optional[EmbeddingIndex] = None
        if embed_client is not None:
            try:
                self.embed_index = EmbeddingIndex(chunks, embed_client)
            except Exception:
                self.embed_index = None

    def retrieve(self, question: str) -> Dict[str, Any]:
        cfg = self.config
        bm25_results = self.bm25_index.search(question, cfg.bm25_k)
        embed_results = self.embed_index.search(question, cfg.embed_k) if self.embed_index else []

        fused_scores = self._rrf_fusion_with_metadata(question, bm25_results, embed_results)
        ranked = sorted(fused_scores.items(), key=lambda x: x[1]["score"], reverse=True)
        top = ranked[:cfg.final_k]

        retrieved_chunks: List[Dict[str, Any]] = []
        core_ids: List[str] = []

        for idx, info in top:
            chunk = self.chunks[idx]
            if chunk.corpus == "core":
                core_ids.append(chunk.id)
            retrieved_chunks.append({
                "id": chunk.id,
                "text": chunk.text,
                "corpus": chunk.corpus,
                "metadata": chunk.metadata,
                "scores": {
                    "rrf_score": info["score"],
                    "bm25_rank": info.get("bm25_rank"),
                    "bm25_score": info.get("bm25_score"),
                },
            })

        confidence = self._estimate_confidence(top)
        if self.embed_index is None and confidence == "high":
            confidence = "medium"

        return {
            "retrieved_chunks": retrieved_chunks,
            "chosen_core_ids": core_ids,
            "retrieval_confidence": confidence,
        }

    def _parse_intent_metadata(self, question: str) -> Dict[str, List[str]]:
        q = question.lower()
        detected_metrics = [
            canonical for canonical, phrases in METRIC_SYNONYMS.items()
            if any(p in q for p in phrases)
        ]
        platform_keywords = {
            "google": ["google", "google ads", "gads"],
            "microsoft": ["microsoft", "bing", "bing ads", "msft"],
        }
        detected_platforms = [p for p, variants in platform_keywords.items() if any(v in q for v in variants)]
        table_keywords = ["campaign", "ad group", "adgroup", "keyword", "search term", "device", "geo"]
        return {
            "metrics": detected_metrics,
            "platforms": detected_platforms,
            "tables": [k for k in table_keywords if k in q],
        }

    def _rrf_fusion_with_metadata(
        self, question: str, bm25_results: List[Tuple[int, float]], embed_results: List[Tuple[int, float]],
    ) -> Dict[int, Dict[str, Any]]:
        cfg = self.config
        rrf_scores: Dict[int, Dict[str, Any]] = defaultdict(lambda: {
            "score": 0.0, "bm25_rank": None, "bm25_score": None,
            "embed_rank": None, "embed_score": None, "metadata_boost": 0.0,
        })

        for rank, (idx, score) in enumerate(bm25_results, start=1):
            entry = rrf_scores[idx]
            entry["bm25_rank"] = rank
            entry["bm25_score"] = score
            entry["score"] += 1.0 / (cfg.rrf_k + rank)

        for rank, (idx, score) in enumerate(embed_results, start=1):
            entry = rrf_scores[idx]
            entry["embed_rank"] = rank
            entry["embed_score"] = score
            entry["score"] += 1.0 / (cfg.rrf_k + rank)

        for idx, entry in rrf_scores.items():
            chunk = self.chunks[idx]
            weight = cfg.corpus_weights.get(chunk.corpus, 1.0)
            entry["score"] *= weight

        intent = self._parse_intent_metadata(question)
        for idx, entry in rrf_scores.items():
            chunk = self.chunks[idx]
            boost = self._compute_metadata_boost(intent, chunk)
            entry["metadata_boost"] = boost
            entry["score"] += boost

        return rrf_scores

    def _compute_metadata_boost(self, intent: Dict[str, List[str]], chunk: Chunk) -> float:
        cfg = self.config
        boost = 0.0
        if intent["metrics"]:
            chunk_metrics = chunk.metrics()
            matched = [c for c in intent["metrics"] if any(c in m for m in chunk_metrics)]
            if matched:
                boost += cfg.metric_match_boost
                for m in matched:
                    boost += cfg.metric_priority_boosts.get(m, 0.0)
        if intent["tables"]:
            chunk_tables = chunk.tables()
            if any(h.replace(" ", "") in t.replace("_", "") for h in intent["tables"] for t in chunk_tables):
                boost += cfg.table_match_boost
        if intent["platforms"]:
            chunk_platform = normalize_platform(infer_platform_for_chunk(chunk))
            if chunk_platform:
                if chunk_platform in intent["platforms"]:
                    boost += cfg.platform_match_boost
                else:
                    boost += cfg.platform_mismatch_penalty
        return boost

    def _estimate_confidence(self, ranked: List[Tuple[int, Dict[str, Any]]]) -> str:
        if not ranked:
            return "low"
        top_score = ranked[0][1]["score"]
        if top_score >= self.config.high_conf_min_rrf:
            return "high"
        if top_score >= self.config.medium_conf_min_rrf:
            return "medium"
        return "low"


# -------------------------
# Factory
# -------------------------

def build_default_hybrid_retriever(
    base_dir: str,
    embed_client: Optional[EmbeddingClient] = None,
) -> HybridRetriever:
    """
    base_dir: directory containing semantic_chunks.*.jsonl files.
    If embed_client is None, runs BM25-only mode.
    """
    chunks = load_all_corpora(Path(base_dir))
    if not chunks:
        raise ValueError(f"No semantic chunks found in {base_dir}")
    return HybridRetriever(chunks=chunks, embed_client=embed_client)
