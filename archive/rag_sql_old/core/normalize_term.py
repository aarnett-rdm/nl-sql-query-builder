from dataclasses import dataclass, field
from collections import defaultdict
import re

def normalize_term(term: str) -> str:
    """
    Normalize a free-text term for dictionary keys:
    - lowercase
    - collapse non-alphanumerics to spaces
    - strip extra spaces
    """
    if term is None:
        return ""
    term = term.lower()
    term = re.sub(r"[^a-z0-9]+", " ", term)
    return term.strip()