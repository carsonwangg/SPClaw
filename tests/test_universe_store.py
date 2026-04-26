from __future__ import annotations

from pathlib import Path

import spclaw.universe_store as universe_store


def test_parse_tickers_filters_noise():
    tickers = universe_store.parse_tickers("create universe defense with PLTR, LMT, RTX and NOC")
    assert tickers == ["PLTR", "LMT", "RTX", "NOC"]


def test_universe_roundtrip(tmp_path: Path):
    universe_store.UNIVERSE_DIR = tmp_path
    path = universe_store.save_universe("defense basket", ["PLTR", "LMT", "RTX"], source="test")
    assert path.exists()
    assert universe_store.list_universes() == ["defense-basket"]
    assert universe_store.load_universe("defense basket") == ["PLTR", "LMT", "RTX"]

    _, added = universe_store.add_to_universe("defense basket", ["NOC", "LMT"], source="test")
    assert added == ["NOC"]
    assert universe_store.load_universe("defense basket") == ["PLTR", "LMT", "RTX", "NOC"]

    _, removed = universe_store.remove_from_universe("defense basket", ["RTX"], source="test")
    assert removed == ["RTX"]
    assert universe_store.load_universe("defense basket") == ["PLTR", "LMT", "NOC"]

