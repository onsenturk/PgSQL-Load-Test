from pgloadgen.metrics import MetricsRecorder


def test_metrics_record_and_snapshot():
    m = MetricsRecorder()
    m.record(0.001)
    m.record(0.002)
    m.record_error()
    snap = m.snapshot()
    assert snap.operations == 2
    assert snap.errors == 1
    assert snap.percentiles["p50"] >= 0
