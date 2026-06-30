#!/usr/bin/env python3
"""Baut die Mapping-Dateien für die Browser-Erweiterung.

Pro Rezept (das einen REWE-Mapping-Link hat) wird vorberechnet, welches REWE-
Produkt pro Zutat genommen werden soll und wie viele Packungen nötig sind. Zwei
Modi:
    cheapest -> data/cheapest_mappings.json : absolut günstigstes Produkt
    offers   -> data/offer_mappings.json    : günstigstes Produkt, das AKTUELL
                im Angebot ist (on_offer); gibt es für eine Zutat kein Angebot,
                Fallback auf das absolut günstigste Produkt (wie cheapest).

Das Ergebnis ist nach dem Mapping-Hash aus der rewe_mapping_url indexiert, damit
die Erweiterung auf der Mapping-Seite per URL-Hash genau ihren Eintrag
nachschlägt. Beide Dateien haben identische Struktur -- die Extension lädt je
nach Modus-Schalter die eine oder andere.

Portierung von pickProduct()/resolvePackage() aus index.html.

Eingaben:
    data/matched_recipes.json   – Rezepte mit Zutaten (raw, product_ids, package_info)
    data/angebote.json          – Produkte (id, price, on_offer, ...)
    data/rewe_mappings.jsonl    – chefkoch_url ↔ rewe_mapping_url

Aufruf:
    python scripts/build_cheapest_mappings.py                 # nur cheapest (default)
    python scripts/build_cheapest_mappings.py --mode offers   # nur Angebote
    python scripts/build_cheapest_mappings.py --mode both      # beide Dateien
"""
import argparse
import json
import re
from collections import OrderedDict
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "data"

MATCHED   = DATA / "matched_recipes.json"
ANGEBOTE  = DATA / "angebote.json"
MAPPINGS  = DATA / "rewe_mappings.jsonl"

OUT_BY_MODE = {
    "cheapest": DATA / "cheapest_mappings.json",
    "offers":   DATA / "offer_mappings.json",
}


# ─── Zutatenname normalisieren ────────────────────────────────────────────────
# Muss identisch zur Normalisierung in der Erweiterung (product_changer.js,
# ingredientName()) sein, damit der searchTerm der REWE-Seite auf denselben
# Schlüssel abbildet. Zweck: REWE dedupliziert Zutaten (3× "2 Eier" → "6 Eier"),
# darum matchen wir über den reinen Namen statt über den vollen Mengentext.
_UNITS = {
    "g", "kg", "mg", "ml", "l", "cl", "el", "tl", "msp", "stk", "stück", "stueck",
    "prise", "prisen", "dose", "dosen", "packung", "pck", "pkg", "bund", "tasse",
    "tassen", "becher", "glas", "scheibe", "scheiben", "zehe", "zehen", "kopf",
    "liter", "gramm", "blatt", "blätter",
}
_LEAD = re.compile(r"^[\d.,/\s½¼¾⅓⅔⅛–-]+")


def ingredient_name(raw: str) -> str:
    s = (raw or "").strip().lower()
    s = _LEAD.sub("", s)                 # führende Zahlen/Brüche entfernen
    parts = s.split()
    if parts and parts[0] in _UNITS:     # führende Einheit entfernen
        parts = parts[1:]
    s = " ".join(parts)
    s = s.split(",")[0]                  # Zusätze nach Komma weg
    return re.sub(r"\s+", " ", s).strip()


# ─── Produktwahl (Port von pickProduct/resolvePackage, cheapest) ──────────────
def resolve_packages(ing: dict, pid: str) -> int:
    """Benötigte Packungszahl für Produkt pid; default 1 (wie resolvePackage)."""
    info = (ing.get("package_info") or {}).get(pid)
    if info:
        n = info.get("packages_needed")
        if isinstance(n, (int, float)) and n >= 1:
            return int(n)
    return 1


def pick_cheapest(ing: dict, product_by_id: dict):
    """Absolut günstigstes Produkt nach paket-bereinigten Kosten (price × packs).

    Gibt (product_id, packages_needed) zurück oder None, wenn kein Produkt matcht.
    """
    cands = [pid for pid in (ing.get("product_ids") or []) if pid in product_by_id]
    if not cands:
        return None

    def eff_cost(pid):
        return product_by_id[pid]["price"] * resolve_packages(ing, pid)

    best = min(cands, key=eff_cost)      # bei Gleichstand gewinnt der erste (wie reduce <=)
    return best, resolve_packages(ing, best)


def pick_offer(ing: dict, product_by_id: dict):
    """Günstigstes Produkt unter den Kandidaten, die AKTUELL im Angebot sind
    (on_offer=True). Gibt es für die Zutat KEIN Angebot, Fallback auf das
    absolut günstigste Produkt (pick_cheapest) -- so wird jede matchbare Zutat
    gesetzt, Angebote nur bevorzugt. None nur, wenn gar kein Produkt matcht.
    """
    cands = [pid for pid in (ing.get("product_ids") or [])
             if pid in product_by_id and product_by_id[pid].get("on_offer")]
    if not cands:
        return pick_cheapest(ing, product_by_id)  # kein Angebot -> günstigstes überhaupt

    def eff_cost(pid):
        return product_by_id[pid]["price"] * resolve_packages(ing, pid)

    best = min(cands, key=eff_cost)
    return best, resolve_packages(ing, best)


PICKERS = {"cheapest": pick_cheapest, "offers": pick_offer}


def mapping_hash(url: str) -> str:
    """Letztes Pfadsegment der rewe_mapping_url = eindeutiger Rezept-Hash."""
    return url.rstrip("/").rsplit("/", 1)[-1]


# ─── Dedup nach Zutatenname (spiegelt REWEs zusammengefasste Sicht) ───────────
def dedupe_ingredients(entries: list) -> list:
    """Zutaten mit gleichem Namen zusammenfassen.

    REWE zeigt pro Zutat nur eine Zeile. Treten in der Chefkoch-Liste mehrere
    Zeilen mit gleichem Namen auf, wählen wir das günstigste der gewählten
    Produkte und nehmen als Packungszahl das Maximum der Einzelzeilen. Das
    Mengen-Handling bei Dups ist eine bewusste v1-Vereinfachung (siehe README/
    Chat): exakt wäre eine Neuberechnung über die Summe der Mengen.
    """
    by_name = OrderedDict()
    for e in entries:
        key = e["name"]
        if key not in by_name:
            by_name[key] = e
        else:
            prev = by_name[key]
            # günstigeres Produkt behalten, größere Packungszahl gewinnt
            if e["_eff_cost"] < prev["_eff_cost"]:
                prev["product_id"] = e["product_id"]
                prev["_eff_cost"] = e["_eff_cost"]
            prev["amount"] = max(prev["amount"], e["amount"])
    # interne Felder entfernen
    out = []
    for e in by_name.values():
        out.append({"name": e["name"], "raw": e["raw"],
                    "product_id": e["product_id"], "amount": e["amount"]})
    return out


def build_mappings(recipes, product_by_id, map_by_chefkoch, pick) -> tuple[dict, dict]:
    """Baut das Mapping-Dict für eine Produktwahl-Funktion `pick`."""
    result = {}
    stats = {"recipes": 0, "no_mapping": 0, "ingredients": 0, "unmatched_ing": 0}

    for rec in recipes:
        chefkoch_url = rec.get("U")
        mp = map_by_chefkoch.get(chefkoch_url)
        if not mp:
            stats["no_mapping"] += 1
            continue

        entries = []
        for ing in rec.get("I", []):
            stats["ingredients"] += 1
            picked = pick(ing, product_by_id)
            if not picked:
                stats["unmatched_ing"] += 1
                continue
            pid, amount = picked
            entries.append({
                "name": ingredient_name(ing["raw"]),
                "raw": ing["raw"],
                "product_id": pid,
                "amount": amount,
                "_eff_cost": product_by_id[pid]["price"] * amount,
            })

        if not entries:
            continue

        h = mapping_hash(mp["rewe_mapping_url"])
        result[h] = {
            "recipe_name": mp.get("recipe_name") or rec.get("N"),
            "chefkoch_url": chefkoch_url,
            "ingredients": dedupe_ingredients(entries),
        }
        stats["recipes"] += 1

    return result, stats


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--mode", choices=["cheapest", "offers", "both"],
                    default="cheapest",
                    help="Welche Datei(en) bauen (default: cheapest).")
    args = ap.parse_args()

    modes = ["cheapest", "offers"] if args.mode == "both" else [args.mode]

    # Eingaben EINMAL laden -- beide Modi teilen denselben Katalog/Rezeptstand.
    recipes = json.loads(MATCHED.read_text(encoding="utf-8"))
    products = json.loads(ANGEBOTE.read_text(encoding="utf-8"))
    product_by_id = {p["id"]: p for p in products}

    map_by_chefkoch = {}
    for line in MAPPINGS.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        m = json.loads(line)
        if m.get("chefkoch_url") and m.get("rewe_mapping_url"):
            map_by_chefkoch[m["chefkoch_url"]] = m

    n_on_offer = sum(1 for p in products if p.get("on_offer"))
    print(f"Katalog: {len(products)} Produkte, davon {n_on_offer} im Angebot.\n")

    for mode in modes:
        result, stats = build_mappings(
            recipes, product_by_id, map_by_chefkoch, PICKERS[mode])
        out = OUT_BY_MODE[mode]
        out.write_text(json.dumps(result, ensure_ascii=False), encoding="utf-8")
        size_kb = out.stat().st_size / 1024
        print(f"✔ [{mode}] {out.relative_to(ROOT)} geschrieben")
        print(f"  Rezepte mit Mapping:      {stats['recipes']}")
        print(f"  Rezepte ohne Mapping:     {stats['no_mapping']}")
        print(f"  Zutaten gesamt:           {stats['ingredients']}")
        print(f"  Zutaten ohne Produkt:     {stats['unmatched_ing']}")
        print(f"  Dateigröße:               {size_kb:.0f} KB\n")


if __name__ == "__main__":
    main()
