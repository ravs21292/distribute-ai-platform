from dml.db.pipelines import DATA_GRAPH, TRAIN_GRAPH


def test_data_graph_order_and_dependencies() -> None:
    names = [name for name, _deps in DATA_GRAPH]
    assert names == ["ingest", "etl", "dedupe", "embed", "index"]
    deps = dict(DATA_GRAPH)
    assert deps["ingest"] == []
    assert deps["etl"] == ["ingest"]
    assert deps["dedupe"] == ["etl"]
    assert deps["embed"] == ["dedupe"]
    assert deps["index"] == ["embed"]


def test_train_graph_depends_on_dedupe_not_index() -> None:
    deps = dict(TRAIN_GRAPH)
    assert deps["train"] == ["dedupe"]
    assert deps["evaluate"] == ["train"]
    assert deps["promote"] == ["evaluate"]
