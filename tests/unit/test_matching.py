from dml.contracts.matching import (
    ResourceRequirements,
    WorkerSnapshot,
    is_eligible,
    preference_sort_key,
    publish_resource_class,
    worker_streams,
)


def _cpu(**kwargs) -> WorkerSnapshot:
    return WorkerSnapshot(
        worker_id="cpu-1",
        capabilities=frozenset({"dummy", "etl"}),
        cpu_count=8,
        memory_mb=16_384,
        gpu_count=0,
        gpu_memory_mb=0,
        **kwargs,
    )


def _gpu(**kwargs) -> WorkerSnapshot:
    return WorkerSnapshot(
        worker_id="gpu-1",
        capabilities=frozenset({"dummy", "training"}),
        cpu_count=12,
        memory_mb=32_768,
        gpu_count=1,
        gpu_memory_mb=12_288,
        **kwargs,
    )


def test_cpu_worker_cannot_run_gpu_required_task() -> None:
    req = ResourceRequirements(cpu=4, memory_mb=8192, gpu=1, gpu_memory_mb=8000)
    assert is_eligible(_gpu(), "training", req)
    assert not is_eligible(_cpu(), "training", req)
    assert not is_eligible(_gpu(), "etl", req)  # missing capability


def test_gpu_preferred_can_use_cpu_when_gpu_absent() -> None:
    req = ResourceRequirements(cpu=1, gpu=0, gpu_preferred=True)
    assert is_eligible(_cpu(), "dummy", req)
    assert is_eligible(_gpu(), "dummy", req)
    assert publish_resource_class(req, healthy_gpu_worker_exists=False) == "cpu"
    assert publish_resource_class(req, healthy_gpu_worker_exists=True) == "gpu"


def test_cpu_only_work_prefers_cpu_workers() -> None:
    req = ResourceRequirements(cpu=1, gpu=0, gpu_preferred=False)
    cpu = _cpu()
    gpu = _gpu()
    assert preference_sort_key(cpu, req) < preference_sort_key(gpu, req)


def test_gpu_preferred_prefers_gpu_workers() -> None:
    req = ResourceRequirements(cpu=1, gpu=0, gpu_preferred=True)
    assert preference_sort_key(_gpu(), req) < preference_sort_key(_cpu(), req)


def test_unhealthy_worker_ineligible() -> None:
    req = ResourceRequirements()
    assert not is_eligible(_cpu(healthy=False), "dummy", req)


def test_gpu_workers_subscribe_gpu_streams_first() -> None:
    streams = worker_streams(["dummy"], gpu_count=1)
    assert streams[0] == "dml.tasks.dummy.gpu"
    assert streams[1] == "dml.tasks.dummy.cpu"
    assert worker_streams(["dummy"], gpu_count=0) == ["dml.tasks.dummy.cpu"]
