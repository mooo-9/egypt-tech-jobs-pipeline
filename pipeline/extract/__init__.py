def load_all() -> None:
    """Import every extractor module so the registries in common.py are filled."""
    from pipeline.extract import smartrecruiters, workday  # noqa: F401
