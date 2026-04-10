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


def test_new_config_defaults():
    cfg = LoadGenConfig(dsn="d", workload="w")
    assert cfg.think_time == 0.0
    assert cfg.ramp_up_seconds == 0.0
    assert cfg.output_file == ""
    assert cfg.read_pct == 50.0
    assert cfg.update_pct == 10.0
    assert cfg.delete_pct == 5.0


def test_config_with_new_fields(tmp_path):
    p = tmp_path / "cfg.yaml"
    p.write_text(
        "dsn: d\nworkload: mixed\nthink_time: 0.1\nramp_up_seconds: 5\n"
        "output_file: out.json\nread_pct: 70\nupdate_pct: 20\ndelete_pct: 5\n",
        encoding="utf-8",
    )
    cfg = LoadGenConfig.load(p)
    assert cfg.think_time == 0.1
    assert cfg.ramp_up_seconds == 5
    assert cfg.read_pct == 70
