"""Deterministic text chunking — a pure domain function (no I/O, no clock).

Used by the ``embed_document`` pipeline. Kept pure so it is trivially
unit-testable in isolation and produces identical chunks on every run,
which is what makes the embedding pipeline's artifacts deterministic.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class Chunk:
    """One contiguous slice of the source document.

    ``index`` is the chunk's ordinal position (0-based); ``start``/``end``
    are character offsets into the original text (``end`` exclusive).
    """

    index: int
    start: int
    end: int
    text: str


def chunk_text(text: str, *, size: int = 512, overlap: int = 64) -> list[Chunk]:
    """Split ``text`` into overlapping windows of ``size`` characters.

    Each window starts ``size - overlap`` characters after the previous one,
    so adjacent chunks share ``overlap`` characters of context. The final
    window is whatever remains and may be shorter than ``size``.

    Determinism: this function performs no I/O, uses no clock, and contains
    no randomness — the same arguments always yield the same chunk list.

    Args:
        text: The source document.
        size: Window length in characters. Must be > 0.
        overlap: Characters shared between adjacent windows. Must satisfy
            ``0 <= overlap < size`` (otherwise the stride would not advance).

    Returns:
        Chunks in document order. An empty/whitespace-only string yields ``[]``.

    Raises:
        ValueError: If ``size <= 0`` or ``overlap`` is out of range.
    """
    if size <= 0:
        raise ValueError(f"size must be positive, got {size}")
    if not (0 <= overlap < size):
        raise ValueError(f"overlap must satisfy 0 <= overlap < size, got {overlap} (size={size})")

    stripped = text.strip()
    if not stripped:
        return []

    stride = size - overlap  # guaranteed >= 1 by the validation above
    chunks: list[Chunk] = []
    start = 0
    index = 0
    length = len(stripped)

    while start < length:
        end = min(start + size, length)
        chunks.append(Chunk(index=index, start=start, end=end, text=stripped[start:end]))
        if end == length:  # we just emitted the tail; stop (no redundant trailing window)
            break
        start += stride
        index += 1

    return chunks
