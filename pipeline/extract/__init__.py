def load_all() -> None:
    """Import every extractor module so the registries in common.py are filled."""
    from pipeline.extract import (  # noqa: F401
        amazon, ashby, eightfold, greenhouse, jibe, lever, oracle_cloud, phenom, smartrecruiters, workable, workday,
    )
