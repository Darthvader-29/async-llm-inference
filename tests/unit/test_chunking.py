"""Unit tests for the pure chunker. No fixtures, no I/O — just math."""

from __future__ import annotations

import pytest

from app.domain.chunking import Chunk, chunk_text


def test_empty_and_whitespace_yield_no_chunks() -> None:
    assert chunk_text("") == []
    assert chunk_text("   \n\t ") == []


def test_short_text_is_single_chunk() -> None:
    chunks = chunk_text("hello world", size=512, overlap=64)
    assert len(chunks) == 1
    assert chunks[0] == Chunk(index=0, start=0, end=11, text="hello world")


def test_windows_overlap_by_exact_amount() -> None:
    text = "abcdefghij"  # length 10
    chunks = chunk_text(text, size=4, overlap=1)  # stride = 3
    # starts 0,3,6 -> abcd, defg, ghij. The window at start=6 already reaches the
    # end (offset 10), so no redundant trailing window is emitted.
    assert [c.text for c in chunks] == ["abcd", "defg", "ghij"]
    assert [(c.start, c.end) for c in chunks] == [(0, 4), (3, 7), (6, 10)]


def test_no_trailing_empty_chunk_on_exact_multiple() -> None:
    text = "abcdef"  # length 6
    chunks = chunk_text(text, size=3, overlap=0)  # stride = 3 -> abc, def, (no empty)
    assert [c.text for c in chunks] == ["abc", "def"]


def test_indices_are_sequential_and_cover_text() -> None:
    text = "x" * 100
    chunks = chunk_text(text, size=30, overlap=10)
    assert [c.index for c in chunks] == list(range(len(chunks)))
    assert chunks[0].start == 0
    assert chunks[-1].end == 100  # last chunk reaches the end


@pytest.mark.parametrize("size,overlap", [(0, 0), (-1, 0), (10, 10), (10, 11), (5, -1)])
def test_invalid_params_raise(size: int, overlap: int) -> None:
    with pytest.raises(ValueError):
        chunk_text("some text", size=size, overlap=overlap)


def test_determinism_same_input_same_output() -> None:
    text = "the quick brown fox jumps over the lazy dog " * 5
    assert chunk_text(text, size=40, overlap=8) == chunk_text(text, size=40, overlap=8)
