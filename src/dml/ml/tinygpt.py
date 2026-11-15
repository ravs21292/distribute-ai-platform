"""Tiny character-level language model trained from scratch on CPU.

Checkpoints include weights, optimizer momentum, step, and data cursor so a
replacement worker can resume after a crash.
"""

from __future__ import annotations

import io
from dataclasses import dataclass
from typing import Any

import numpy as np

VOCAB = 128
CTX = 12
DIM = 32
HIDDEN = 64


def _softmax(logits: np.ndarray) -> np.ndarray:
    shifted = logits - logits.max(axis=-1, keepdims=True)
    exp = np.exp(shifted)
    return exp / exp.sum(axis=-1, keepdims=True)


@dataclass
class TrainMetrics:
    step: int
    loss: float


class TinyGPT:
    def __init__(self, rng: np.random.Generator | None = None) -> None:
        self.rng = rng or np.random.default_rng(7)
        self.E = self.rng.normal(0, 0.08, (VOCAB, DIM)).astype(np.float64)
        self.W1 = self.rng.normal(0, 0.08, (CTX * DIM, HIDDEN)).astype(np.float64)
        self.b1 = np.zeros(HIDDEN, dtype=np.float64)
        self.W2 = self.rng.normal(0, 0.08, (HIDDEN, VOCAB)).astype(np.float64)
        self.b2 = np.zeros(VOCAB, dtype=np.float64)
        self.vE = np.zeros_like(self.E)
        self.vW1 = np.zeros_like(self.W1)
        self.vb1 = np.zeros_like(self.b1)
        self.vW2 = np.zeros_like(self.W2)
        self.vb2 = np.zeros_like(self.b2)
        self.step = 0
        self.cursor = 0

    def state_dict(self) -> dict[str, Any]:
        return {
            "E": self.E,
            "W1": self.W1,
            "b1": self.b1,
            "W2": self.W2,
            "b2": self.b2,
            "vE": self.vE,
            "vW1": self.vW1,
            "vb1": self.vb1,
            "vW2": self.vW2,
            "vb2": self.vb2,
            "step": np.array([self.step]),
            "cursor": np.array([self.cursor]),
        }

    def load_state_dict(self, payload: dict[str, Any]) -> None:
        for key in ("E", "W1", "b1", "W2", "b2", "vE", "vW1", "vb1", "vW2", "vb2"):
            setattr(self, key, payload[key])
        self.step = int(payload["step"][0])
        self.cursor = int(payload["cursor"][0])

    def dumps(self) -> bytes:
        buffer = io.BytesIO()
        np.savez_compressed(buffer, **self.state_dict())
        return buffer.getvalue()

    @classmethod
    def loads(cls, data: bytes) -> TinyGPT:
        loaded = np.load(io.BytesIO(data), allow_pickle=False)
        model = cls()
        model.load_state_dict({k: loaded[k] for k in loaded.files})
        return model

    def _encode(self, text: str) -> np.ndarray:
        return np.array([min(ord(ch), VOCAB - 1) for ch in text], dtype=np.int64)

    def _windows(self, ids: np.ndarray, batch: int) -> tuple[np.ndarray, np.ndarray]:
        if len(ids) <= CTX + 1:
            ids = np.pad(ids, (0, CTX + 2 - len(ids)))
        xs = []
        ys = []
        for _ in range(batch):
            if self.cursor + CTX + 1 >= len(ids):
                self.cursor = 0
            start = self.cursor
            xs.append(ids[start : start + CTX])
            ys.append(ids[start + CTX])
            self.cursor += 1
        return np.stack(xs), np.array(ys, dtype=np.int64)

    def _forward(self, x: np.ndarray) -> tuple[np.ndarray, dict[str, np.ndarray]]:
        emb = self.E[x]
        flat = emb.reshape(x.shape[0], -1)
        h_pre = flat @ self.W1 + self.b1
        hidden = np.tanh(h_pre)
        logits = hidden @ self.W2 + self.b2
        return logits, {"emb": emb, "flat": flat, "h_pre": h_pre, "hidden": hidden, "x": x}

    def train_step(self, text: str, *, batch_size: int, lr: float) -> float:
        ids = self._encode(text)
        x, y = self._windows(ids, batch_size)
        logits, cache = self._forward(x)
        probs = _softmax(logits)
        loss = float(-np.mean(np.log(probs[np.arange(len(y)), y] + 1e-9)))
        grad = probs
        grad[np.arange(len(y)), y] -= 1
        grad /= len(y)
        gW2 = cache["hidden"].T @ grad
        gb2 = grad.sum(axis=0)
        dhidden = (grad @ self.W2.T) * (1 - cache["hidden"] ** 2)
        gW1 = cache["flat"].T @ dhidden
        gb1 = dhidden.sum(axis=0)
        dflat = dhidden @ self.W1.T
        demb = dflat.reshape(cache["emb"].shape)
        gE = np.zeros_like(self.E)
        np.add.at(gE, cache["x"], demb)
        mu = 0.9
        for param, vel, grad_p in (
            (self.E, self.vE, gE),
            (self.W1, self.vW1, gW1),
            (self.b1, self.vb1, gb1),
            (self.W2, self.vW2, gW2),
            (self.b2, self.vb2, gb2),
        ):
            vel *= mu
            vel += grad_p
            param -= lr * vel
        self.step += 1
        return loss

    def evaluate(self, text: str, batches: int = 8) -> tuple[float, float]:
        ids = self._encode(text)
        losses = []
        saved_cursor = self.cursor
        for _ in range(batches):
            x, y = self._windows(ids, 8)
            logits, _ = self._forward(x)
            probs = _softmax(logits)
            losses.append(float(-np.mean(np.log(probs[np.arange(len(y)), y] + 1e-9))))
        self.cursor = saved_cursor
        loss = float(np.mean(losses)) if losses else 0.0
        return loss, float(np.exp(min(loss, 20)))

    def generate(self, prompt: str, n_chars: int = 40) -> str:
        ids = list(self._encode(prompt or "The "))
        while len(ids) < CTX:
            ids.insert(0, 32)
        out = []
        for _ in range(n_chars):
            ctx = np.array(ids[-CTX:], dtype=np.int64)[None, :]
            logits, _ = self._forward(ctx)
            nxt = int(np.argmax(logits[0]))
            ids.append(nxt)
            out.append(chr(nxt) if 32 <= nxt < 127 else " ")
        return prompt + "".join(out)
