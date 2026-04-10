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


def test_error_categories():
    m = MetricsRecorder()
    m.record_error("connection")
    m.record_error("connection")
    m.record_error("timeout")
    m.record_error()  # default "unknown"
    snap = m.snapshot()
    assert snap.errors == 4
    assert snap.error_categories["connection"] == 2
    assert snap.error_categories["timeout"] == 1
    assert snap.error_categories["unknown"] == 1
