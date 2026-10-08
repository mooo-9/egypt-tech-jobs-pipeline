"""One live call to a job board; prints how many of its postings are in Egypt.
Usage: check_company.py <greenhouse|lever|ashby|workable> <id>   (board / site / board / account)"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pipeline.extract import load_all  # noqa: E402
from pipeline.extract.common import EXTRACTORS  # noqa: E402

ID_FIELD = {"greenhouse": "board", "lever": "site", "ashby": "board", "workable": "account"}

if __name__ == "__main__":
    system, ident = sys.argv[1:3]
    load_all()
    postings = EXTRACTORS[system]({"key": ident, system: {ID_FIELD[system]: ident}}, "check")
    print(f"{system} {ident}: {len(postings)} Egypt postings")
    for p in postings[:5]:
        print(f"  {p.title} | {p.location}")
