import json
import math
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from collections import defaultdict

from rank_bm25 import BM25Okapi


# -------------------------
# Data structures & config
# -------------------------

# Canonical metric ids -> phrases we look for in NL questions

METRIC_SYNONYMS: Dict[str, List[str]] = {
    "cpc": [
        "cpc",
        "cost per click",
        "avg cpc",
        "average cpc",
    ],
    "cpa": [
        "cpa",
        "cost per action",
        "cost per acquisition",
        "cost per conversion",
    ],
    "cpm": [
        "cpm",
        "cost per thousand",
        "cost per 1000 impressions",
    ],
    "ctr": [
        "ctr",
        "clickthrough rate",
        "click-through rate",
    ],
    "conversion_rate": [
        "conversion rate",
        "conv rate",
        "conversion %",
        "cr",
    ],
    "roas": [
        "roas",
        "return on ad spend",
    ],
    "roi": [
        "roi",
        "return on investment",
    ],
    "impressions": [
        "impressions",
        "impr",
    ],
    "clicks": [
        "clicks",
        "click volume",
    ],
    "spend": [
        "spend",
        "cost",
        "ad spend",
    ],
    "revenue": [
        "revenue",
        "sales",
    ],
    "profit": [
        "profit",
        "margin",
    ],
}


@dataclass
class Chunk:
    """Single canonical chunk from any of the semantic_chunks JSONL corpora."""
    id: str
    text: str
    corpus: str  # "core" | "usage" | "qa" | "core_from_doc"
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
    # corpus weights (core highest, usage medium, qa/from_doc lowest)
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

    # metadata boosts
    table_match_boost: float = 0.15
    metric_match_boost: float = 0.12   # base boost for ANY metric match

    # extra bumps for key business metrics (per canonical metric name)
    metric_priority_boosts: Dict[str, float] = field(default_factory=lambda: {
        "cpa": 0.06,
        "cpc": 0.04,
        "cpm": 0.03,
        "ctr": 0.03,
        "roas": 0.06,
        "roi": 0.05,
        "conversion_rate": 0.04,
    })

    platform_match_boost: float = 0.10
    platform_mismatch_penalty: float = -0.08

    high_conf_min_rrf: float = 0.02
    medium_conf_min_rrf: float = 0.01


# -------------------------
# Embedding client abstraction
# -------------------------

class EmbeddingClient:
    """Abstract interface so you can plug a real embedding model later."""
    def embed(self, texts: List[str]) -> List[List[float]]:
        raise NotImplementedError


def cosine_similarity(a: List[float], b: List[float]) -> float:
    if not a or not b:
        return 0.0
    if len(a) != len(b):
        n = min(len(a), len(b))
        a = a[:n]
        b = b[:n]
    dot = sum(x * y for x, y in zip(a, b))
    norm_a = math.sqrt(sum(x * x for x in a)) or 1e-9
    norm_b = math.sqrt(sum(y * y for y in b)) or 1e-9
    return dot / (norm_a * norm_b)


# -------------------------
# Text utils & corpus loading
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
    """
    base_dir: directory containing semantic_chunks.*.jsonl
    """
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
# Platform inference helpers
# -------------------------

def infer_platform_for_chunk(chunk: Chunk) -> Optional[str]:
    """
    Infer platform from explicit metadata first, then from table name.
    Returns a raw platform hint like "google_ads", "microsoft_ads", "google", etc.
    """
    plats = chunk.platforms()
    if plats:
        return plats[0].lower()

    name = chunk.id.lower()
    if "googleads" in name:
        return "google_ads"
    if "microsoftads" in name or "bingads" in name:
        return "microsoft_ads"
    if "metaads" in name or "facebookads" in name or "instagramads" in name:
        return "meta_ads"
    return None


def normalize_platform(p: Optional[str]) -> Optional[str]:
    """
    Normalize raw platform hints to canonical ids:
    "google_ads", "google"      -> "google"
    "microsoft_ads", "bing"     -> "microsoft"
    "meta_ads", "facebook"      -> "meta"
    """
    if p is None:
        return None
    p = p.lower()

    if p.endswith("_ads"):
        p = p.replace("_ads", "")

    if "google" in p:
        return "google"
    if "microsoft" in p or "bing" in p:
        return "microsoft"
    if "meta" in p or "facebook" in p or "instagram" in p:
        return "meta"

    return p


# -------------------------
# Hybrid retriever
# -------------------------

class HybridRetriever:
    """
    Hybrid retriever with:
    - BM25 (always on)
    - Embeddings (optional, can be None)
    - RRF fusion + metadata boosts

    If embed_client is None or embedding setup fails,
    it runs BM25-only but keeps the same API.
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
            except Exception as e:
                print(
                    "WARNING: Failed to build embedding index; "
                    "falling back to BM25-only mode.\nError:", e
                )
                self.embed_index = None

    # ---- main public API ----

    def retrieve(self, question: str) -> Dict[str, Any]:
        """
        Main entry point used by the planner.

        Returns:
        {
          "retrieved_chunks": [ {chunk info + scores + justification}, ... ],
          "chosen_core_ids": [...],
          "retrieval_confidence": "high" | "medium" | "low"
        }
        """
        cfg = self.config

        bm25_results = self.bm25_index.search(question, cfg.bm25_k)

        if self.embed_index is not None:
            embed_results = self.embed_index.search(question, cfg.embed_k)
        else:
            embed_results = []

        fused_scores = self._rrf_fusion_with_metadata(
            question, bm25_results, embed_results
        )

        ranked = sorted(fused_scores.items(), key=lambda x: x[1]["score"], reverse=True)
        top = ranked[: cfg.final_k]

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
                    "embed_rank": info.get("embed_rank"),
                    "embed_score": info.get("embed_score"),
                    "corpus_weight": info.get("corpus_weight"),
                    "metadata_boost": info.get("metadata_boost"),
                },
                "justification": info.get("justification"),
            })

        retrieval_confidence = self._estimate_confidence(top)

        if self.embed_index is None and retrieval_confidence == "high":
            retrieval_confidence = "medium"

        return {
            "retrieved_chunks": retrieved_chunks,
            "chosen_core_ids": core_ids,
            "retrieval_confidence": retrieval_confidence,
        }

    # ---- internal helpers ----

    def _parse_intent_metadata(self, question: str) -> Dict[str, List[str]]:
        """
        Extract metric / table / platform hints from NL question.
        Metrics are returned as canonical ids (keys of METRIC_SYNONYMS),
        so they can match chunk.metadata metrics cleanly.
        """
        q = question.lower()

        table_keywords = [
            "campaign", "ad group", "adgroup", "keyword", "search term",
            "match type", "device", "geo", "placement"
        ]

        platform_keywords = {
            "google": ["google", "google ads", "google search", "gads"],
            "microsoft": ["microsoft", "bing", "bing ads", "msft"],
            "meta": ["facebook", "meta", "instagram", "fb"],
        }

        # ---- metric detection via synonyms ----
        detected_metrics: List[str] = []
        for canonical, phrases in METRIC_SYNONYMS.items():
            if any(p in q for p in phrases):
                detected_metrics.append(canonical)

        # ---- platform detection ----
        detected_platforms: List[str] = []
        for p, variants in platform_keywords.items():
            if any(v in q for v in variants):
                detected_platforms.append(p)

        # ---- table concept hints (simple keywords) ----
        def found(keys: List[str]) -> List[str]:
            return [k for k in keys if k in q]

        return {
            "metrics": detected_metrics,
            "platforms": detected_platforms,
            "tables": found(table_keywords),
        }

    def _rrf_fusion_with_metadata(
        self,
        question: str,
        bm25_results: List[Tuple[int, float]],
        embed_results: List[Tuple[int, float]],
    ) -> Dict[int, Dict[str, Any]]:
        """
        RRF fusion:

        base_score(idx) = sum_systems( 1 / (k + rank_system(idx)) )
        then apply corpus weights and metadata boosts.
        """
        cfg = self.config
        rrf_scores: Dict[int, Dict[str, Any]] = defaultdict(lambda: {
            "score": 0.0,
            "bm25_rank": None,
            "bm25_score": None,
            "embed_rank": None,
            "embed_score": None,
            "corpus_weight": None,
            "metadata_boost": 0.0,
            "justification": [],
        })

        # BM25 ranks
        for rank, (idx, score) in enumerate(bm25_results, start=1):
            entry = rrf_scores[idx]
            entry["bm25_rank"] = rank
            entry["bm25_score"] = score
            entry["score"] += 1.0 / (cfg.rrf_k + rank)

        # Embedding ranks (optional)
        for rank, (idx, score) in enumerate(embed_results, start=1):
            entry = rrf_scores[idx]
            entry["embed_rank"] = rank
            entry["embed_score"] = score
            entry["score"] += 1.0 / (cfg.rrf_k + rank)

        # Corpus weights
        for idx, entry in rrf_scores.items():
            chunk = self.chunks[idx]
            weight = cfg.corpus_weights.get(chunk.corpus, 1.0)
            entry["corpus_weight"] = weight
            entry["score"] *= weight

        # Metadata boosts
        intent = self._parse_intent_metadata(question)
        for idx, entry in rrf_scores.items():
            chunk = self.chunks[idx]
            boost = self._compute_metadata_boost(intent, chunk)
            entry["metadata_boost"] = boost
            entry["score"] += boost
            entry["justification"] = self._build_justification(intent, chunk, entry)

        return rrf_scores

    def _compute_metadata_boost(self, intent: Dict[str, List[str]], chunk: Chunk) -> float:
        cfg = self.config
        boost = 0.0

        # ---- Metric match (with priority boosts) ----
        if intent["metrics"]:
            chunk_metrics = chunk.metrics()  # already lowercased list
            matched_canonicals: List[str] = []

            for canonical in intent["metrics"]:
                # Match if canonical metric name is contained in any chunk metric name
                if any(canonical in m for m in chunk_metrics):
                    matched_canonicals.append(canonical)

            if matched_canonicals:
                # base boost for having any relevant metric
                boost += cfg.metric_match_boost
                # extra per-metric priorities (CPA, CPC, ROAS, etc.)
                for m in matched_canonicals:
                    boost += cfg.metric_priority_boosts.get(m, 0.0)

        # ---- Table-ish match (campaign / adgroup / keyword etc.) ----
        if intent["tables"]:
            chunk_tables = chunk.tables()
            for hint in intent["tables"]:
                if any(hint.replace(" ", "") in t.replace("_", "") for t in chunk_tables):
                    boost += cfg.table_match_boost
                    break

        # ---- Platform-aware boost/penalty ----
        if intent["platforms"]:
            raw_platform = infer_platform_for_chunk(chunk)
            chunk_platform = normalize_platform(raw_platform)
            if chunk_platform:
                if chunk_platform in intent["platforms"]:
                    boost += cfg.platform_match_boost
                else:
                    boost += cfg.platform_mismatch_penalty

        return boost


    def _build_justification(
        self,
        intent: Dict[str, List[str]],
        chunk: Chunk,
        entry: Dict[str, Any],
    ) -> List[str]:
        just: List[str] = []

        if entry.get("bm25_rank") is not None:
            just.append(f"Strong lexical match (BM25 rank {entry['bm25_rank']}).")
        if entry.get("embed_rank") is not None:
            just.append(f"Semantic similarity (embedding rank {entry['embed_rank']}).")

        chunk_tables = chunk.tables()
        chunk_metrics = chunk.metrics()

        if intent["metrics"] and any(m in chunk_metrics for m in intent["metrics"]):
            just.append(f"Matches requested metric(s): {', '.join(intent['metrics'])}.")
        if intent["tables"]:
            matched_hints = []
            for hint in intent["tables"]:
                if any(hint.replace(' ', '') in t.replace('_', '') for t in chunk_tables):
                    matched_hints.append(hint)
            if matched_hints:
                just.append(f"Relevant to table concept(s): {', '.join(matched_hints)}.")

        # Platform explanation
        raw_platform = infer_platform_for_chunk(chunk)
        norm_platform = normalize_platform(raw_platform)
        if intent["platforms"] and norm_platform:
            if norm_platform in intent["platforms"]:
                just.append(f"Platform inferred from table name matches: {norm_platform}.")
            else:
                just.append(
                    f"Platform inferred from table name differs from requested: "
                    f"{norm_platform} (downranked)."
                )

        just.append(f"Chunk sourced from '{chunk.corpus}' corpus.")
        return just

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
# Convenience factory
# -------------------------

def build_default_hybrid_retriever(
    base_dir: str,
    embed_client: Optional[EmbeddingClient] = None,
) -> HybridRetriever:
    """
    base_dir: directory containing semantic_chunks.*.jsonl

    - If embed_client is None => BM25-only mode (no LLM needed).
    - When you're ready, pass a real EmbeddingClient implementation.
    """
    chunks = load_all_corpora(Path(base_dir))
    return HybridRetriever(chunks=chunks, embed_client=embed_client)
