#!/usr/bin/env python3
"""Laedt das Cross-Encoder-Produktionsmodell aus dem privaten HF-Repo in den
Ordner, den match_jsonl_cache2.py per Default erwartet
(matching/cross_encoder_model6_lauf18-final-produktionsmodell).

Aufruf im Workflow VOR dem Matching:

    HF_TOKEN=*** python matching/download_model.py

Revision: standardmaessig der NEUESTE Tag des Repos (nicht hart auf v1-lauf18
gepinnt). So zieht CI automatisch das jeweils zuletzt getaggte Modell, sobald ein
neuer Lauf hochgeladen + getaggt wurde. "Neuester" Tag wird primaer ueber die
Commit-Historie bestimmt (Tag auf dem juengsten Commit), Fallback ist eine
natuerliche Versions-Sortierung. Ueberschreibbar per CE_MODEL_REVISION.

WICHTIG (CE-Cache): Wechselt der neueste Tag auf ein Modell mit ANDEREN Gewichten,
aendert sich der Inhalts-Hash -> der committete ce_pair_cache.json wird (korrekt)
verworfen und einmal neu gerechnet. Solange derselbe Modellstand getaggt bleibt,
greift der Cache.

model_fingerprint() in match_jsonl_cache2.py nimmt den Inhalts-Hash der Gewichte,
SOLANGE weder die Env CE_MODEL_VERSION noch eine version.txt im Modellordner
gesetzt ist. Der geseedete Cache hat genau diesen Inhalts-Hash. -> Im Workflow
CE_MODEL_VERSION NICHT setzen und hier keine version.txt schreiben, sonst wird der
Cache beim ersten CI-Lauf faelschlich verworfen (volle ~90-Min-Neuberechnung).
"""
from __future__ import annotations

import os
import re
import sys
from pathlib import Path

from huggingface_hub import HfApi, snapshot_download

# Defaults ueber Env ueberschreibbar (z.B. neuer Lauf -> neuer Tag).
REPO_ID = os.environ.get("CE_MODEL_REPO", "BigDatalex/gbert-base-cross-encoder-ingredient-product-de")

# Zielordner = Default-Pfad von match_jsonl_cache2.py (--model). Relativ zu dieser
# Datei, damit es unabhaengig vom Arbeitsverzeichnis stimmt (lokal wie auf dem Runner).
TARGET = Path(__file__).resolve().parent / "cross_encoder_model6_lauf18-final-produktionsmodell"


def _natural_key(name: str):
    """Sortierschluessel, der eingebettete Zahlen numerisch vergleicht, damit
    'v2-lauf19' > 'v1-lauf9' korrekt ist (rein lexikalisch waere lauf19 < lauf9)."""
    return [int(p) if p.isdigit() else p for p in re.split(r"(\d+)", name)]


def latest_tag(repo_id: str, token: str | None) -> str:
    """Neuester Tag des Repos. Primaer ueber die Commit-Historie (der Tag, dessen
    Ziel-Commit am weitesten oben in `git log` steht), Fallback natuerliche
    Versions-Sortierung der Tagnamen."""
    api = HfApi(token=token)
    refs = api.list_repo_refs(repo_id)
    tags = list(refs.tags)
    if not tags:
        raise RuntimeError(f"Repo {repo_id} hat keine Tags — nichts zu pinnen.")

    try:
        # list_repo_commits liefert neueste zuerst -> kleinster Index = juengster.
        order = {c.commit_id: i for i, c in enumerate(api.list_repo_commits(repo_id))}
        known = [t for t in tags if t.target_commit in order]
        if known:
            return min(known, key=lambda t: order[t.target_commit]).name
    except Exception:
        pass

    return sorted((t.name for t in tags), key=_natural_key)[-1]


def main() -> int:
    token = os.environ.get("HF_TOKEN") or os.environ.get("HUGGING_FACE_HUB_TOKEN")
    if not token:
        print("FEHLER: HF_TOKEN nicht gesetzt (privates Repo). Abbruch.", file=sys.stderr)
        return 1

    # Explizite Revision gewinnt; sonst automatisch der neueste Tag.
    revision = os.environ.get("CE_MODEL_REVISION") or latest_tag(REPO_ID, token)

    # --print-revision: nur die aufgeloeste Revision ausgeben (fuer den
    # actions/cache-Key im Workflow), nichts herunterladen.
    if "--print-revision" in sys.argv:
        print(revision)
        return 0
    print(f"Lade Modell {REPO_ID}@{revision} -> {TARGET} ...")
    snapshot_download(
        repo_id=REPO_ID,
        revision=revision,
        local_dir=str(TARGET),
        token=token,
    )

    # Mindest-Sanity: ohne Gewichte + config kann das Matching nicht laden.
    missing = [f for f in ("config.json", "model.safetensors") if not (TARGET / f).exists()]
    if missing:
        print(f"FEHLER: nach Download fehlen {missing} in {TARGET}", file=sys.stderr)
        return 1

    print("Modell vollstaendig geladen.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
