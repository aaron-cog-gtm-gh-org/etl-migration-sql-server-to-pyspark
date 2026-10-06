from mfg_lake.common import paths


def test_abfss_uri_contract():
    assert paths.abfss_uri("daily_production") == (
        "abfss://curated@nwhmfglake.dfs.core.windows.net/"
        "manufacturing/daily_production")


def test_curated_dir_resolves_abfss(tmp_path, monkeypatch):
    monkeypatch.setenv("LAKE_ROOT", str(tmp_path))
    # LAKE_ROOT is read at import time; verify path shape via module constant
    import importlib
    importlib.reload(paths)
    p = paths.curated_dir("daily_production", "n1")
    assert str(p).endswith("n1/curated/manufacturing/daily_production")


def test_raw_csv():
    assert str(paths.raw_csv("mes.downtime_event")).endswith(
        "data/raw/mes.downtime_event.csv")
