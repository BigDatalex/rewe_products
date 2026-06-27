#!/usr/bin/env python3
"""Wöchentlicher Quality-Gate für die Rezept-Produkt-Mappings.

Rechnet Katalog-Drift-Metriken aus den Artefakten, die der Weekly-Workflow
ohnehin produziert (kein Neu-Scrape), schreibt eine Historie und einen
Snapshot der aktuellen Katalog-IDs, und meldet über GitHub-Annotations +
Step-Summary. Das eigentliche Fail-on-stale macht der Workflow anhand des
`stale`-Outputs -- dieses Skript beendet sich selbst immer mit 0, damit
Snapshot/History auch bei einer Regression noch committet werden.

Metriken
--------
  stale         : matched - catalog
                  -> gematchte Produkte, die NICHT mehr im Katalog sind.
                     Gesunder Wert = 0. Jeder Wert > 0 = Rezept zeigt ins
                     Leere (delisted). Das ist die Invariante, auf die der
                     Workflow hart failt.
  missed_deals  : on_offer - matched
                  -> aktuell rabattierte Produkte, die das Matching (noch)
                     nicht kennt = verpasste Deals. Trend-Metrik, nur Warnung.
  new_ids       : catalog_jetzt - catalog_letzter_lauf   (braucht Snapshot)
                  -> wie stark sich der Katalog seit dem letzten Lauf bewegt
                     hat. Signal "lokal neu matchen".
  gone_matched  : (catalog_letzter_lauf - catalog_jetzt) & matched
                  -> diese Woche neu verschwundene gematchte Produkte.
"""
from __future__ import annotations

import csv
import json
import os
from datetime import date
from pathlib import Path

DATA = Path("data")
ANGEBOTE = DATA / "angebote.json"
MATCHED = DATA / "matched_recipes.json"
SNAPSHOT = DATA / "catalog_ids_snapshot.json"
HISTORY = DATA / "metrics_history.csv"

# missed_deals schwankt -> nur warnen, nicht failen. Schwelle für die Warnung.
MISSED_DEALS_WARN = 500


def load_catalog() -> tuple[set[str], set[str]]:
    """(alle Katalog-IDs, on_offer-IDs)."""
    products = json.loads(ANGEBOTE.read_text(encoding="utf-8"))
    catalog = {str(p["id"]) for p in products}
    on_offer = {str(p["id"]) for p in products if p.get("on_offer")}
    return catalog, on_offer


def load_matched() -> set[str]:
    """Alle Produkt-IDs, die in irgendeiner Rezept-Zutat gematcht wurden."""
    recipes = json.loads(MATCHED.read_text(encoding="utf-8"))
    matched: set[str] = set()
    for r in recipes:
        for ing in r.get("I", []):
            for pid in ing.get("product_ids", []):
                matched.add(str(pid))
    return matched


def gh_out(key: str, value) -> None:
    """Step-Output für nachfolgende Workflow-Steps setzen."""
    path = os.environ.get("GITHUB_OUTPUT")
    if path:
        with open(path, "a", encoding="utf-8") as f:
            f.write(f"{key}={value}\n")


def gh_summary(md: str) -> None:
    path = os.environ.get("GITHUB_STEP_SUMMARY")
    if path:
        with open(path, "a", encoding="utf-8") as f:
            f.write(md + "\n")


def main() -> int:
    catalog, on_offer = load_catalog()
    matched = load_matched()

    stale = matched - catalog
    missed_deals = on_offer - matched

    # Snapshot-Diff (erst ab dem zweiten Lauf aussagekräftig)
    if SNAPSHOT.exists():
        prev = set(json.loads(SNAPSHOT.read_text(encoding="utf-8")))
        new_ids = catalog - prev
        gone = prev - catalog
        gone_matched = gone & matched
        has_baseline = True
    else:
        new_ids = gone_matched = set()
        has_baseline = False

    # --- Konsolen-Log ---
    print(f"Katalog: {len(catalog)} (on_offer {len(on_offer)}) | "
          f"matched: {len(matched)}")
    print(f"stale (matched - catalog)      : {len(stale)}")
    print(f"missed_deals (on_offer - matched): {len(missed_deals)}")
    if has_baseline:
        print(f"new_ids seit letztem Lauf      : {len(new_ids)}")
        print(f"gone_matched (neu verschwunden): {len(gone_matched)}")
    else:
        print("kein Snapshot -> Baseline wird angelegt (Diff ab nächstem Lauf)")

    # --- Historie fortschreiben ---
    new_file = not HISTORY.exists()
    with HISTORY.open("a", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        if new_file:
            w.writerow(["date", "catalog", "on_offer", "matched", "stale",
                        "missed_deals", "new_ids", "gone_matched"])
        w.writerow([date.today().isoformat(), len(catalog), len(on_offer),
                    len(matched), len(stale), len(missed_deals),
                    len(new_ids) if has_baseline else "",
                    len(gone_matched) if has_baseline else ""])

    # --- Snapshot der aktuellen Katalog-IDs für den nächsten Lauf ---
    SNAPSHOT.write_text(json.dumps(sorted(catalog)), encoding="utf-8")

    # --- GitHub-Annotations + Step-Summary ---
    if stale:
        sample = ", ".join(sorted(stale)[:10])
        print(f"::error::{len(stale)} gematchte Produkte nicht mehr im "
              f"Katalog (delisted), z.B.: {sample}")
    if len(missed_deals) > MISSED_DEALS_WARN:
        print(f"::warning::{len(missed_deals)} rabattierte Produkte nicht "
              f"gematcht (> {MISSED_DEALS_WARN}) -- Matching lokal neu laufen "
              f"lassen, um Deals einzufangen.")

    gh_summary("### Quality-Gate Mappings\n")
    gh_summary("| Metrik | Wert |")
    gh_summary("|---|---|")
    gh_summary(f"| Katalog (on_offer) | {len(catalog)} ({len(on_offer)}) |")
    gh_summary(f"| gematchte Produkte | {len(matched)} |")
    gh_summary(f"| **stale** (delisted, sollte 0 sein) | **{len(stale)}** |")
    gh_summary(f"| missed_deals (on_offer, nicht gematcht) | {len(missed_deals)} |")
    if has_baseline:
        gh_summary(f"| new_ids seit letztem Lauf | {len(new_ids)} |")
        gh_summary(f"| gone_matched (neu verschwunden) | {len(gone_matched)} |")

    gh_out("stale", len(stale))
    gh_out("missed_deals", len(missed_deals))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
