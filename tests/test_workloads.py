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
