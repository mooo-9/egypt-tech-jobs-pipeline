from pathlib import Path

import yaml

from pipeline.extract import load_all
from pipeline.extract.common import EXTRACTORS

COMPANIES = yaml.safe_load((Path(__file__).parent.parent / "companies.yml").read_text(encoding="utf-8"))
META = {"key", "name", "industry"}


def test_every_entry_has_key_name_and_industry():
    for c in COMPANIES:
        assert all(c.get(field) for field in META), c


def test_keys_are_unique():
    keys = [c["key"] for c in COMPANIES]
    assert len(keys) == len(set(keys))


def test_each_entry_has_exactly_one_known_system():
    load_all()
    for c in COMPANIES:
        systems = set(c) - META
        assert len(systems) == 1 and systems <= set(EXTRACTORS), c
