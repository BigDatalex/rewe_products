"""
Ingredient → Produkt Matching für JSONL-Rezepte (schema.org Format)
====================================================================

Liest german_recipes_full.jsonl + REWE CSV, matcht jede Zutat per
Cross Encoder gegen den Produktkatalog und schreibt matched_recipes.json.

Deduplizierung: identische (normalisierte) Zutaten werden nur EINMAL
gematcht – bei diesem Datensatz spart das ~75% der CE-Aufrufe.

Per-Paar-CE-Cache: das Retrieval läuft jeden Lauf frisch gegen den aktuellen
Katalog, aber der Cross Encoder rechnet nur die (Zutat, Produkt)-Paare neu, die
noch nicht im Cache (data/ce_pair_cache.json) stehen. Bei stabilem Katalog +
Modell ist das Ergebnis identisch zu einem vollen Rerun, kostet aber nur die
CE-Inferenz auf den wirklich neuen Paaren. Invalidierung: Modell-Retraining
(model_hash), Delisting (product_id weg) und Umbenennung (product_fp).

Usage:
    pip install sentence-transformers rapidfuzz scikit-learn tqdm pandas
    python match_jsonl.py
    python match_jsonl.py --jsonl /pfad/recipes.jsonl --csv /pfad/products.csv
"""

from __future__ import annotations

import argparse
import json
import math
import os
import re
from pathlib import Path

import numpy as np
import pandas as pd
from rapidfuzz import fuzz
from sentence_transformers import SentenceTransformer
from sentence_transformers.cross_encoder import CrossEncoder
from tqdm import tqdm

from state_classifier import is_state_mismatch  # Frisch/TK/Konserve-Vorprüfung,
                                                   # siehe state_classifier.py
from color_classifier import is_color_mismatch  # Farb-Vorprüfung (post-CE Veto),
                                                   # siehe color_classifier.py
from ingredient_preprocessing import dedup_key  # form-bewusster Dedup-Schlüssel
                                                   # (normalize + Form-Tag), trennt
                                                   # frisch/Dose/getrocknet

ROOT     = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"

DENSE_MODEL = "sentence-transformers/all-MiniLM-L6-v2"  # Benchmark-Sieger für
                                                          # die RRF-Fusion auf
                                                          # diesem Datensatz
RRF_K = 60  # Reciprocal-Rank-Fusion-Konstante, wie im Candidate-Benchmark


# ---------------------------------------------------------------------------
# Normalisierung  (identisch zum Training-Script)
# ---------------------------------------------------------------------------

_UNITS = (
    r"\b(g|kg|mg|ml|l|el|tl|msp\.?|stück|stk\.?|prise[n]?|becher|dose[n]?|"
    r"pck\.?|packung(en)?|päckchen|zehe\/n|zehe[n]?|bund|scheibe[n]?|"
    r"m\.-große[snr]?|gr\.|kl\.|große[snr]?|kleine[snr]?|etwas|evtl\.?|"
    r"ca\.?|nach belieben)\b"
)

def normalize(text: str) -> str:
    s = str(text).lower()
    s = re.sub(r"^[\d\s.,/½¼¾-]+", "", s)
    s = re.sub(_UNITS, " ", s)
    s = re.sub(r"\(.*?\)", " ", s)
    s = re.sub(r"[^a-zäöüß\s-]", " ", s)
    return re.sub(r"\s+", " ", s).strip()


# ---------------------------------------------------------------------------
# Lexical blocking
# ---------------------------------------------------------------------------

def lexical_score(a: str, b: str) -> float:
    return max(fuzz.token_set_ratio(a, b), fuzz.partial_ratio(a, b))


def lexical_scores_all(ing_norm: str, product_norms: list[str]) -> np.ndarray:
    return np.array([lexical_score(ing_norm, pn) for pn in product_norms],
                     dtype=np.float64)


def _rrf_rank(scores: np.ndarray) -> np.ndarray:
    """Rang (0 = bester) je Produkt, descending nach score sortiert.

    Bei Gleichstand (sehr häufig bei kurzen Zutaten wie 'Zwiebel', wo
    rapidfuzz dutzende Produkte mit score=100 zurückgibt) wird sekundär nach
    Produktname-Länge sortiert: kürzerer Name landet vorne, weil ein kurzer
    Name eher die Grundzutat selbst ist als ein langes Markenprodukt
    ('Speisezwiebeln' vor 'Maggi Fix für Zwiebel-Sahne-Hähnchen 26g').
    Das alleine löst das Problem nicht vollständig, ist aber eine kostenlose
    Verbesserung VOR der eigentlichen Hybrid-Fusion unten.
    """
    n = len(scores)
    order = np.lexsort((np.arange(n), -scores))  # primär: -score, sekundär: Index als Tiebreak-Fallback
    ranks = np.empty(n, dtype=np.float64)
    ranks[order] = np.arange(n)
    return ranks


def get_candidates_hybrid(
    ing_norm: str,
    ing_raw_for_dense: str,
    product_norms: list[str],
    product_texts: list[str],
    product_dense_emb: np.ndarray,
    dense_model: SentenceTransformer,
    top_k: int,
) -> list[int]:
    """RRF-Fusion aus lexikalischem Ranking (rapidfuzz) und dichtem Ranking
    (Bi-Encoder-Cosine). Ersetzt das rein lexikalische get_candidates().

    Begründung: bei kurzen, generischen Zutaten ('Zwiebel', 'Sahne', 'Ei')
    liefert rapidfuzz für sehr viele Katalogprodukte den exakt gleichen
    Score 100 (das gesuchte Wort taucht irgendwo im Produktnamen auf), und
    die Top-k-Auswahl wird dann effektiv zufällig. Der Bi-Encoder bringt ein
    zweites, davon unabhängiges Signal (semantische Nähe statt reiner
    Teilstring-Treffer), und RRF kombiniert beide robust gegen die
    unterschiedliche Skalierung der Scores.
    """
    lex_scores = lexical_scores_all(ing_norm, product_norms)
    lex_rank = _rrf_rank(lex_scores)

    # length-based secondary sort innerhalb der lexikalischen Top-Kandidaten
    # (siehe _rrf_rank-Docstring) -- wird implizit durch den Ties-Fallback
    # auf den Produkt-Index abgedeckt; für eine echte Längen-Priorisierung
    # bei Ties sortieren wir die Top-Kandidatengruppe explizit nach Länge:
    top_lex_idx = np.argsort(lex_scores)[::-1][: top_k * 3]
    name_lens = np.array([len(product_texts[i]) for i in top_lex_idx])
    tie_break_order = top_lex_idx[np.lexsort((name_lens, -lex_scores[top_lex_idx]))]
    for new_rank, idx in enumerate(tie_break_order):
        lex_rank[idx] = min(lex_rank[idx], new_rank)

    query_emb = dense_model.encode([ing_raw_for_dense], normalize_embeddings=True)[0]
    dense_scores = product_dense_emb @ query_emb
    dense_rank = _rrf_rank(dense_scores)

    fused = 1.0 / (RRF_K + 1 + lex_rank) + 1.0 / (RRF_K + 1 + dense_rank)
    return np.argsort(fused)[::-1][:top_k].tolist()


# ---------------------------------------------------------------------------
# Pantry-Filter
# ---------------------------------------------------------------------------

#PANTRY = {
#    "salz", "pfeffer", "öl", "wasser", "zucker", "mehl", "essig",
#    "brühe", "fond", "lorbeerblätter", "lorbeer", "kümmel", "olivenöl",
#    "pflanzenöl", "sonnenblumenöl", "rapsöl", "backpulver", "natron",
#    "vanillezucker", "speisestärke", "senf", "worcestersauce", "muskat",
#    "paprikapulver", "fett", "margarine",
#}

PANTRY = set()

def is_pantry(norm: str) -> bool:
    tokens = set(norm.split())
    return bool(tokens & PANTRY) and len(tokens) <= 3


# ---------------------------------------------------------------------------
# JSONL laden (schema.org Format)
# ---------------------------------------------------------------------------

def parse_total_time(iso: str) -> str:
    """'0:55:00' → '55 Min', '1:25:00' → '1 Std 25 Min'."""
    if not iso:
        return ""
    parts = iso.split(":")
    if len(parts) != 3:
        return ""
    try:
        h, m, _ = int(parts[0]), int(parts[1]), int(parts[2])
    except ValueError:
        return ""
    if h and m:
        return f"{h} Std {m} Min"
    if h:
        return f"{h} Std"
    return f"{m} Min"


def load_jsonl_recipes(path: Path) -> list[dict]:
    """Liest schema.org Recipe JSONL → einheitliches Format mit Metadaten."""
    recipes = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            r = json.loads(line)
            ingredients = r.get("recipeIngredient", [])
            ingredients = [i.strip() for i in ingredients if i and i.strip()]
            if not ingredients:
                continue

            rating = r.get("aggregateRating") or {}
            kw = r.get("keywords", "")
            keywords = [k.strip() for k in kw.split(",")] if isinstance(kw, str) else []

            recipes.append({
                "N": r.get("name", "").strip(),
                "U": r.get("mainEntityOfPage", ""),
                "yield":        r.get("recipeYield", ""),
                "rating":       rating.get("ratingValue"),
                "rating_count": rating.get("ratingCount"),
                "time":         parse_total_time(r.get("totalTime", "")),
                "image":        r.get("image", ""),
                "category":     r.get("recipeCategory", ""),
                "keywords":     keywords,
                "I_raw": ingredients,
            })
    return recipes


# ---------------------------------------------------------------------------
# Produkte aus CSV
# ---------------------------------------------------------------------------

def build_product_text(p: dict) -> str:
    cat = p.get("cat2") or p.get("cat1") or ""
    return f"{p['name']} | {cat}"


def load_products(csv_path: Path) -> list[dict]:
    df = pd.read_csv(csv_path)
    products = []
    for _, row in df.iterrows():
        disc = row.get("listing_discount_rate")
        try:
            on_offer = disc is not None and not math.isnan(float(disc))
        except (TypeError, ValueError):
            on_offer = False
        products.append({
            "id":   str(row["product_id"]),
            "name": row["product_name"],
            "cat1": row["category_level_1"] if pd.notna(row.get("category_level_1")) else "",
            "cat2": row["category_level_2"] if pd.notna(row.get("category_level_2")) else "",
        })
    return products


# ---------------------------------------------------------------------------
# Eingabe aus committeten Artefakten (--from-matched): laeuft komplett auf den
# bereits im Repo liegenden Dateien, ohne Rohdaten (voller jsonl / BigQuery-CSV).
# Gedacht fuer die CI/Action -- geschlossener Kreis: re-matcht nur BEKANNTE
# Rezepte gegen den aktuellen Katalog, nimmt KEINE neuen Rezepte auf. Beide
# Loader liefern exakt dieselbe interne Struktur wie ihre Rohdaten-Pendants.
# ---------------------------------------------------------------------------

def load_products_from_angebote(path: Path) -> list[dict]:
    """Produktquelle = data/angebote.json. Selbe Struktur wie load_products():
    id/name/cat1/cat2 (angebote.json hat genau diese Felder bereits)."""
    data = json.loads(path.read_text(encoding="utf-8"))
    products = []
    for p in data:
        products.append({
            "id":   str(p["id"]),
            "name": p.get("name", "") or "",
            "cat1": p.get("cat1") or "",
            "cat2": p.get("cat2") or "",
        })
    return products


def load_recipes_from_matched(path: Path) -> list[dict]:
    """Rezeptquelle = data/matched_recipes.json (vorheriger Output). I_raw wird
    aus I[].raw re-derived; die alten product_ids werden ignoriert (sie werden
    neu berechnet). Liefert das gleiche Format wie load_jsonl_recipes()."""
    data = json.loads(path.read_text(encoding="utf-8"))
    recipes = []
    for r in data:
        ingredients = [ing["raw"].strip() for ing in r.get("I", [])
                       if ing.get("raw") and ing["raw"].strip()]
        if not ingredients:
            continue
        recipes.append({
            "N": r.get("N", ""),
            "U": r.get("U", ""),
            "yield":        r.get("yield", ""),
            "rating":       r.get("rating"),
            "rating_count": r.get("rating_count"),
            "time":         r.get("time", ""),
            "image":        r.get("image", ""),
            "category":     r.get("category", ""),
            "keywords":     r.get("keywords", []),
            "I_raw": ingredients,
        })
    return recipes


# ---------------------------------------------------------------------------
# Per-Paar-CE-Cache: gecachte Cross-Encoder-Scores wiederverwenden
# ---------------------------------------------------------------------------
# Der CE-Score eines Paars (zutat_rohtext, produkt_text) ist bei festem Modell
# deterministisch. Wir cachen ihn pro (dedup_key, product_id) und rechnen pro
# Lauf nur die Paare neu, die noch nicht im Cache sind (= neue Produkte im
# frischen Top-k-Retrieval). Gezielt invalidiert wird:
#   - Modell neu trainiert -> model_hash aendert sich  -> ganzer Cache verworfen.
#   - Produkt delisted      -> product_id nicht im Katalog -> Eintrag entfaellt.
#   - Produkt umbenannt      -> product_fp (Hash des Produkttexts) aendert sich
#                               -> betroffene Eintraege verworfen (sonst stale).
# Ergebnis ist damit bit-identisch zu einem vollen Rerun, spart aber die
# CE-Inferenz auf allen unveraenderten Paaren.

import hashlib


def product_catalog_hash(products: list[dict]) -> str:
    """Hash über alle Produkt-IDs + Namen. Ändert sich der Katalog, ändert sich
    der Hash → das Hybrid-Retrieval wird frisch neu berechnet."""
    h = hashlib.sha256()
    for p in sorted(products, key=lambda x: x["id"]):
        h.update(f"{p['id']}|{p['name']}".encode("utf-8"))
    return h.hexdigest()[:16]


def model_fingerprint(model_dir: Path) -> str:
    """Identitäts-Hash des CE-Modells, **download-fest**: hängt am INHALT der
    Gewichte, nicht an mtime/Pfad. Ein frischer Download (CI: Modell aus GCS/HF
    ziehen) setzt die mtime neu — ein mtime-basierter Hash würde den Per-Paar-
    Cache dann jede Woche fälschlich verwerfen. Gleiche Gewichte → gleicher Hash,
    egal wo/wann sie liegen.

    Schnellpfad für CI: ist CE_MODEL_VERSION (Env) oder version.txt im Ordner
    gesetzt, wird nur diese Versionsangabe gehasht — dann muss die 419-MB-Datei
    gar nicht erst durchgelesen werden."""
    explicit = os.environ.get("CE_MODEL_VERSION")
    if not explicit:
        vfile = model_dir / "version.txt"
        if vfile.exists():
            explicit = vfile.read_text(encoding="utf-8").strip()
    if explicit:
        return hashlib.sha256(explicit.encode("utf-8")).hexdigest()[:16]

    h = hashlib.sha256()
    cfg = model_dir / "config.json"
    if cfg.exists():
        h.update(cfg.read_bytes())  # num_labels etc. -> bei gleichem Gewicht relevant
    weights = next((model_dir / f for f in ("model.safetensors", "pytorch_model.bin")
                    if (model_dir / f).exists()), None)
    if weights is not None:
        h.update(f"{weights.name}:{weights.stat().st_size}".encode("utf-8"))
        with open(weights, "rb") as f:
            for chunk in iter(lambda: f.read(1 << 20), b""):
                h.update(chunk)
    return h.hexdigest()[:16]


def _product_fp(text: str) -> str:
    """Kurzer Fingerprint des Produkttexts → erkennt Umbenennungen."""
    return hashlib.sha1(text.encode("utf-8")).hexdigest()[:12]


def load_ce_cache(
    path: Path, model_hash: str,
    product_ids: list[str], product_texts: list[str],
) -> dict[str, dict[str, float]]:
    """Lädt {dedup_key: {product_id: score}} aus dem Per-Paar-Cache.
    Nur gültig bei passendem model_hash; pro Eintrag nur, wenn das Produkt noch
    existiert UND sein Text unverändert ist (sonst wäre der Score stale)."""
    if not path.exists():
        return {}
    try:
        blob = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    if blob.get("model_hash") != model_hash:
        print("CE-Cache: Modell geändert → Cache verworfen (volle Neuberechnung).")
        return {}

    cur_fp = {pid: _product_fp(t) for pid, t in zip(product_ids, product_texts)}
    stored_fp = blob.get("product_fp", {})
    out: dict[str, dict[str, float]] = {}
    kept = dropped = 0
    for key, by_pid in blob.get("scores", {}).items():
        keep: dict[str, float] = {}
        for pid, sc in by_pid.items():
            if pid in cur_fp and stored_fp.get(pid) == cur_fp[pid]:
                keep[pid] = sc
                kept += 1
            else:
                dropped += 1
        if keep:
            out[key] = keep
    print(f"CE-Cache geladen: {kept} Paar-Scores ({dropped} verworfen: "
          f"delisted/umbenannt)")
    return out


def save_ce_cache(
    path: Path, model_hash: str, pair_scores: dict[str, dict[str, float]],
    product_ids: list[str], product_texts: list[str], active_keys: set[str],
) -> None:
    """Schreibt den Per-Paar-Cache zurück, beschränkt auf aktuell existierende
    Produkte und aktuell vorkommende Zutaten → der Cache bleibt beschränkt."""
    cur_fp = {pid: _product_fp(t) for pid, t in zip(product_ids, product_texts)}
    catalog = set(product_ids)
    scores_out: dict[str, dict[str, float]] = {}
    for key in active_keys:
        keep = {pid: sc for pid, sc in pair_scores.get(key, {}).items()
                if pid in catalog}
        if keep:
            scores_out[key] = keep
    used = {pid for d in scores_out.values() for pid in d}
    fp_out = {pid: cur_fp[pid] for pid in used if pid in cur_fp}
    path.write_text(json.dumps({
        "model_hash": model_hash,
        "product_fp": fp_out,
        "scores": scores_out,
    }), encoding="utf-8")


# ---------------------------------------------------------------------------
# Matching mit Deduplizierung
# ---------------------------------------------------------------------------

def match(
    recipes: list[dict],
    products: list[dict],
    model: CrossEncoder,
    model_dir: Path,
    threshold: float,
    top_k: int,
    batch_size: int,
    use_cache: bool = True,
) -> list[dict]:
    product_texts = [build_product_text(p) for p in products]
    product_norms = [normalize(pt) for pt in product_texts]
    product_ids   = [p["id"] for p in products]

    # ── Deduplizierung: jede (form-bewusst) normalisierte Zutat nur EINMAL ──
    # WICHTIG: norm_to_raw merkt sich zu jeder normalisierten Form den ERSTEN
    # rohen Originaltext. Das ist noetig, weil normalize() Woerter wie "Bund"/
    # "Scheibe[n]"/"Dose[n]" als Mengeneinheiten entfernt (siehe _UNITS) --
    # genau die Woerter, die is_state_mismatch() als Frisch/Konserve-Signal
    # braucht. Schlüssel = dedup_key (form-bewusst). norm_to_query haelt die
    # PLAIN-Normalisierung fuer Lexik/Dense/Pantry -- das "|Form-Tag" darf NIE
    # in die Produktsuche, sonst vergiftet es das Blocking.
    norm_to_match: dict[str, list[str] | None] = {}
    norm_to_raw: dict[str, str] = {}
    norm_to_query: dict[str, str] = {}
    for r in recipes:
        for raw in r["I_raw"]:
            key = dedup_key(raw)
            if key.startswith("|"):  # leerer Basis-Name -> ueberspringen
                continue
            if key not in norm_to_raw:
                norm_to_raw[key] = raw
                norm_to_query[key] = normalize(raw)

    # Pantry / leere Queries direkt als "kein Match" abhaken, der Rest ist aktiv.
    active_keys: list[str] = []
    for key in norm_to_raw:
        if is_pantry(norm_to_query[key]) or not norm_to_query[key].strip():
            norm_to_match[key] = None
        else:
            active_keys.append(key)

    total_raw = sum(len(r["I_raw"]) for r in recipes)
    n_unique = len(norm_to_raw)
    print(f"\n{total_raw} Zutaten gesamt → {n_unique} einzigartig "
          f"(Dedup spart {1 - n_unique/total_raw:.0%}) → {len(active_keys)} aktiv")

    if not active_keys:
        print("Keine aktiven Zutaten — nichts zu matchen.")
    else:
        # ── Bi-Encoder laden + Katalog-Embeddings (gecached) ──
        print(f"Lade Bi-Encoder ({DENSE_MODEL}) für Hybrid-Retrieval...")
        dense_model = SentenceTransformer(DENSE_MODEL)
        emb_cache_path = DATA_DIR / "dense_catalog_emb.npy"
        emb_meta_path = DATA_DIR / "dense_catalog_emb.meta.json"
        catalog_sig = f"{DENSE_MODEL}|{len(product_texts)}|{hash(tuple(product_texts[:5]))}"
        product_dense_emb = None
        if emb_cache_path.exists() and emb_meta_path.exists():
            meta = json.loads(emb_meta_path.read_text())
            if meta.get("sig") == catalog_sig:
                product_dense_emb = np.load(emb_cache_path)
                print(f"Katalog-Embeddings aus Cache geladen ({product_dense_emb.shape})")
        if product_dense_emb is None:
            print("Berechne Katalog-Embeddings (einmalig, wird gecached)...")
            product_dense_emb = dense_model.encode(
                product_texts, batch_size=128, normalize_embeddings=True,
                show_progress_bar=True,
            ).astype(np.float32)
            np.save(emb_cache_path, product_dense_emb)
            emb_meta_path.write_text(json.dumps({"sig": catalog_sig}))

        # ── Frisches Hybrid-Retrieval fuer ALLE aktiven Zutaten ──
        # Der Kandidaten-Cache haengt am vollen Katalog-Hash: jede Katalog-
        # aenderung (Zugang/Abgang/Umbenennung) verwirft ihn -> die Top-k sind
        # immer konsistent mit dem aktuellen Katalog ("frisches Retrieval").
        # Bei UNveraendertem Katalog ist Retrieval deterministisch -> Reuse ok.
        # Query an die Produktsuche ist IMMER die Plain-Normalisierung
        # (norm_to_query), nie der dedup_key. Indiziert wird per dedup_key.
        catalog_hash = product_catalog_hash(products)
        cache_path = DATA_DIR / "hybrid_cache_jsonl.json"
        KEY_SCHEME = "dedup_v1"
        # Kandidaten werden auf der Platte als Produkt-IDs abgelegt, NICHT als
        # Positions-Indizes. Grund: catalog_hash sortiert nach id, ist also
        # reihenfolge-UNabhaengig -> angebote.json und CSV gelten als "gleicher
        # Katalog", haben aber andere Array-Reihenfolge. Index-basierte Kandidaten
        # zeigten beim Reuse dann still auf falsche Produkte (Korruption + nahezu
        # 0 CE-Cache-Hits). IDs sind reihenfolge-stabil; beim Laden werden sie auf
        # die aktuellen Indizes aufgeloest. CAND_SCHEME invalidiert alte
        # index-basierte Cache-Files (ohne den Marker) einmalig.
        CAND_SCHEME = "ids_v1"
        idx_by_id = {pid: i for i, pid in enumerate(product_ids)}
        candidates_by_key: dict[str, list[int]] = {}
        if cache_path.exists():
            with open(cache_path) as f:
                c = json.load(f)
            if (c.get("top_k") == top_k and c.get("catalog_hash") == catalog_hash
                    and c.get("dense_model") == DENSE_MODEL
                    and c.get("key_scheme") == KEY_SCHEME
                    and c.get("cand_scheme") == CAND_SCHEME):
                # gespeicherte ID-Listen -> aktuelle Indizes (fehlende IDs raus)
                candidates_by_key = {
                    k: [idx_by_id[pid] for pid in ids if pid in idx_by_id]
                    for k, ids in c["candidates"].items()
                }
                print(f"Hybrid-Cache gültig ({len(candidates_by_key)} Zutaten, "
                      f"Katalog unverändert)")
            else:
                print("Katalog/Config geändert → Retrieval wird frisch berechnet.")

        missing = [k for k in active_keys if k not in candidates_by_key]
        for key in tqdm(missing, desc="Hybrid Blocking (frisch)") if missing else []:
            q = norm_to_query[key]
            candidates_by_key[key] = get_candidates_hybrid(
                q, q, product_norms, product_texts,
                product_dense_emb, dense_model, top_k)
        if missing:
            # nur aktive Keys persistieren -> Cache bleibt schlank; als IDs ablegen
            keep = {k: [product_ids[i] for i in candidates_by_key[k]]
                    for k in active_keys if k in candidates_by_key}
            with open(cache_path, "w") as f:
                json.dump({"top_k": top_k, "catalog_hash": catalog_hash,
                           "dense_model": DENSE_MODEL, "key_scheme": KEY_SCHEME,
                           "cand_scheme": CAND_SCHEME, "candidates": keep}, f)

        # ── Per-Paar-CE-Cache: nur neue Paare durch den Cross Encoder ──
        model_hash = model_fingerprint(model_dir)
        ce_cache_path = DATA_DIR / "ce_pair_cache.json"
        pair_scores = (load_ce_cache(ce_cache_path, model_hash,
                                     product_ids, product_texts)
                       if use_cache else {})

        # WICHTIG: an den CE geht der ROHTEXT (norm_to_raw), nicht "norm" --
        # so wurde das Modell trainiert (Mengen-/Formangaben inklusive), sonst
        # Train/Serve-Mismatch. candidates_by_key bleibt auf "norm" indiziert,
        # weil Stage-1-Retrieval von der Normalisierung profitiert.
        miss_pairs: list[list[str]] = []
        miss_map: list[tuple[str, str]] = []
        for key in active_keys:
            cached = pair_scores.setdefault(key, {})
            for prod_idx in candidates_by_key[key]:
                pid = product_ids[prod_idx]
                if pid not in cached:
                    miss_pairs.append([norm_to_raw[key], product_texts[prod_idx]])
                    miss_map.append((key, pid))

        n_total_pairs = sum(len(candidates_by_key[k]) for k in active_keys)
        print(f"\nCE-Paare gesamt: {n_total_pairs:,} | Cache-Hits: "
              f"{n_total_pairs - len(miss_pairs):,} | neu zu rechnen: "
              f"{len(miss_pairs):,}")

        if miss_pairs:
            # ── Cross Encoder auf allen CPU-Kernen, nur fuer die neuen Paare ──
            n_cores = os.cpu_count() or 1
            os.environ["OMP_NUM_THREADS"] = str(n_cores)
            os.environ["MKL_NUM_THREADS"] = str(n_cores)
            os.environ["TOKENIZERS_PARALLELISM"] = "false"
            import torch
            torch.set_num_threads(n_cores)

            print(f"Cross Encoder scored {len(miss_pairs):,} neue Paare auf "
                  f"{n_cores} Kernen (batch={batch_size})...")
            scores = model.predict(miss_pairs, batch_size=batch_size,
                                   show_progress_bar=True)
            for (key, pid), sc in zip(miss_map, scores):
                pair_scores[key][pid] = float(sc)
        else:
            print("Keine neuen Paare — Cross Encoder wird übersprungen.")

        # ── Ergebnisse je Zutat aus (gecachten + neuen) Scores bilden ──
        for key in active_keys:
            raw_ingredient = norm_to_raw[key]
            cached = pair_scores[key]
            scored = [(cached[product_ids[pi]], pi)
                      for pi in candidates_by_key[key]
                      if product_ids[pi] in cached]
            matches = []
            for sc, prod_idx in sorted(scored, reverse=True):
                if sc < threshold:
                    continue
                p = products[prod_idx]
                cat = p.get("cat2") or p.get("cat1") or ""
                if is_state_mismatch(raw_ingredient, p["name"], cat):
                    continue  # Frisch/TK/Konserve passt nicht -- trotz
                              # CE-Score über Threshold ablehnen (siehe
                              # state_classifier.py; validiert: faengt ca.
                              # 30% der bekannten False Positives, bei
                              # 2.5% Risiko auf bereits ueber-Threshold-
                              # liegende echte Treffer)
                if is_color_mismatch(raw_ingredient, p["name"]):
                    continue  # expliziter Farbwiderspruch (z.B. Zutat weiß,
                              # Produkt rot) -- trotz hohem CE-Score ablehnen
                              # (siehe color_classifier.py; konservativ: blockt
                              # nur wenn beide Seiten widersprechende Farben
                              # tragen)
                matches.append(p["id"])
            norm_to_match[key] = matches if matches else None

        # ── Per-Paar-Cache zurueckschreiben (auf aktuellen Katalog beschraenkt) ──
        if use_cache:
            save_ce_cache(ce_cache_path, model_hash, pair_scores,
                          product_ids, product_texts, set(active_keys))

    # ── Rezepte zusammenbauen ──
    out = []
    total_ings = matched_ings = 0
    for r in recipes:
        ing_list = []
        for raw in r["I_raw"]:
            key = dedup_key(raw)
            pids = norm_to_match.get(key)
            ing_list.append({"raw": raw, "product_ids": pids or []})
            total_ings += 1
            if pids:
                matched_ings += 1
        out.append({
            "N": r["N"],
            "U": r["U"],
            "yield":        r["yield"],
            "rating":       r["rating"],
            "rating_count": r["rating_count"],
            "time":         r["time"],
            "image":        r["image"],
            "category":     r["category"],
            "keywords":     r["keywords"],
            "I": ing_list,
        })

    print(f"\nMatch-Rate: {matched_ings}/{total_ings} Zutaten ({matched_ings/total_ings:.1%})")
    counts = [len(v) for v in norm_to_match.values() if isinstance(v, list) and v]
    if counts:
        print(f"Ø Matches pro Zutat: {sum(counts)/len(counts):.1f} (max: {max(counts)})")
    return out


# ---------------------------------------------------------------------------
def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    # Vorverarbeitete Zutaten (split/dedup/aggregat aus preprocess_recipes.py).
    # Für die Rohdaten: --jsonl german_recipes_full.jsonl
    ap.add_argument("--jsonl", type=Path,
                    default=ROOT / "german_recipes_full_preprocessed.jsonl")
    ap.add_argument("--csv", type=Path, default=ROOT / "data_local/bigquery_export_dlt_rewe_products3.csv")
    ap.add_argument("--model",      type=Path, default=ROOT / "matching" / "cross_encoder_model6_lauf18-final-produktionsmodell")
    ap.add_argument("--out",        type=Path, default=DATA_DIR / "matched_recipes.json")
    ap.add_argument("--top-k",      type=int,  default=25,
                    help="Kandidaten pro Zutat vor dem Cross-Encoder "
                         "(Benchmark-Konfiguration: 25, war vorher 20)")
    ap.add_argument("--batch-size", type=int,  default=128)
    ap.add_argument("--threshold",  type=float, default=None)
    ap.add_argument("--no-reuse",   action="store_true",
                    help="Per-Paar-CE-Cache ignorieren, alle Paare neu durch "
                         "den Cross Encoder")
    ap.add_argument("--from-matched", action="store_true",
                    help="Eingabe aus committeten Artefakten statt Rohdaten: "
                         "Rezepte aus matched_recipes.json (nur BEKANNTE Rezepte, "
                         "keine neuen!), Produkte aus angebote.json. Für CI/Action.")
    ap.add_argument("--matched-in", type=Path, default=DATA_DIR / "matched_recipes.json",
                    help="Rezeptquelle für --from-matched")
    ap.add_argument("--angebote",   type=Path, default=DATA_DIR / "angebote.json",
                    help="Produktquelle für --from-matched")
    args = ap.parse_args()

    if args.from_matched:
        print(f"[--from-matched] Lade Rezepte aus {args.matched_in}...")
        recipes = load_recipes_from_matched(args.matched_in)
        print(f"  {len(recipes)} Rezepte (geschlossener Kreis, keine neuen)")
        print(f"[--from-matched] Lade Produkte aus {args.angebote}...")
        products = load_products_from_angebote(args.angebote)
        print(f"  {len(products)} Produkte")
    else:
        print(f"Lade Rezepte von {args.jsonl}...")
        recipes = load_jsonl_recipes(args.jsonl)
        print(f"  {len(recipes)} Rezepte")

        print(f"Lade Produkte von {args.csv}...")
        products = load_products(args.csv)
        print(f"  {len(products)} Produkte")

    print(f"\nLade Cross Encoder von {args.model}...")
    if not args.model.exists():
        raise FileNotFoundError(f"Modell nicht gefunden: {args.model}")
    model = CrossEncoder(str(args.model), num_labels=1)

    thr_file = args.model / "threshold.json"
    if args.threshold is not None:
        threshold = args.threshold
    elif thr_file.exists():
        threshold = json.loads(thr_file.read_text())["threshold"]
        print(f"  Threshold: {threshold:.3f}")
    else:
        threshold = 0.5

    if args.no_reuse:
        print("--no-reuse: Per-Paar-CE-Cache wird ignoriert (volle Neuberechnung).")
    matched = match(recipes, products, model, args.model, threshold,
                    args.top_k, args.batch_size, use_cache=not args.no_reuse)

    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(matched, f, ensure_ascii=False, separators=(",", ":"), allow_nan=False)

    print(f"\nGespeichert: {args.out} ({args.out.stat().st_size/1024:.0f} KB)")


if __name__ == "__main__":
    main()
