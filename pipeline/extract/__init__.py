def load_all() -> None:
    """Import every extractor module so the registries in common.py are filled."""
    from pipeline.extract import (  # noqa: F401
        amazon, eightfold, jibe, oracle_cloud, phenom, smartrecruiters, workday,
    )
