from pgloadgen.config import LoadGenConfig
import json


def test_load_yaml(tmp_path):
    p = tmp_path / "cfg.yaml"
    p.write_text("dsn: d\nworkload: w\n", encoding="utf-8")
    cfg = LoadGenConfig.load(p)
    assert cfg.dsn == "d" and cfg.workload == "w"


def test_load_json(tmp_path):
    p = tmp_path / "cfg.json"
    p.write_text(json.dumps({"dsn": "d", "workload": "w"}), encoding="utf-8")
    cfg = LoadGenConfig.load(p)
    assert cfg.workload == "w"
