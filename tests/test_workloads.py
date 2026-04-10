from pgloadgen.workloads import get_workload


def test_fk_chain_workload_registered():
    cls = get_workload("fk_chain_insert")
    assert cls.__name__ == "FkChainInsertWorkload"


def test_fk_chain_workload_accepts_topology():
    cls = get_workload("fk_chain_insert")
    cls(table="loadgen_fk", payload_size=8, fk_topology="chain")
    cls(table="loadgen_fk", payload_size=8, fk_topology="star")


def test_fk_chain_workload_accepts_reset_flag():
    cls = get_workload("fk_chain_insert")
    cls(table="loadgen_fk", payload_size=8, fk_topology="chain", fk_reset=False)


def test_read_query_workload_registered():
    cls = get_workload("read_query")
    assert cls.__name__ == "ReadQueryWorkload"


def test_mixed_workload_registered():
    cls = get_workload("mixed")
    assert cls.__name__ == "MixedWorkload"


def test_mixed_workload_accepts_pct():
    cls = get_workload("mixed")
    w = cls(table="loadgen_mix", payload_size=8, read_pct=60, update_pct=20, delete_pct=10)
    assert w.read_pct == 60
    assert w.update_pct == 20
    assert w.delete_pct == 10


def test_mixed_workload_validates_pct():
    cls = get_workload("mixed")
    import pytest
    with pytest.raises(ValueError, match="must be <= 100"):
        cls(table="t", payload_size=8, read_pct=80, update_pct=30, delete_pct=10)
