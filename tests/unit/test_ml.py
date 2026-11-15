from dml.ml.embeddings import LocalEmbeddingProvider
from dml.ml.tinygpt import TinyGPT


def test_embeddings_are_deterministic_unit_vectors() -> None:
    provider = LocalEmbeddingProvider(dim=64)
    first = provider.embed_batch(["hello workers"])[0]
    second = provider.embed_batch(["hello workers"])[0]
    other = provider.embed_batch(["completely different text"])[0]
    assert first == second
    assert first != other
    norm = sum(x * x for x in first) ** 0.5
    assert abs(norm - 1.0) < 1e-6


def test_tinygpt_checkpoint_resume_continues_step() -> None:
    model = TinyGPT()
    text = "the worker died and another worker resumed training from the checkpoint. "
    loss1 = model.train_step(text, batch_size=8, lr=0.05)
    assert model.step == 1
    blob = model.dumps()
    restored = TinyGPT.loads(blob)
    assert restored.step == 1
    assert restored.cursor == model.cursor
    loss2 = restored.train_step(text, batch_size=8, lr=0.05)
    assert restored.step == 2
    assert isinstance(loss1, float)
    assert isinstance(loss2, float)
