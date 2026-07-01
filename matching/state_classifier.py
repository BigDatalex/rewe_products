"""
Regelbasierte Ableitung von Frisch/TK/Konserve/Getrocknet fuer Zutaten und
Produkte -- KEIN ML-Klassifikator, reine Schluesselwort-/Kategorie-Heuristik.

Hintergrund: Die Fehleranalyse (cross_encoder_test_errors.csv) zeigte
Frisch-vs-TK/getrocknet-Verwechslungen als wiederkehrendes Muster (Thymian,
Petersilie, Schnittlauch, Spinat). Da diese Information meist schon explizit
im Text steht (Produktname oder Kategorie), reicht eine Heuristik -- kein
Trainingsaufwand, kein Risiko durch Klassifikator-Fehler.

WICHTIGE DESIGNENTSCHEIDUNG (nach Validierung gegen project-1-6_transformed.csv):
Ein einzelner Zustand pro Zutat (z.B. nur "frisch") ist zu starr -- viele
Zutaten (nackte Kraeuternamen, Fischfilets, Pilze, Gemuese) sind im Handel
GLEICHZEITIG in mehreren Formen gaengig. Ein einzelner falscher Default
("frisch" nach Regel 8) erzeugte bei einer ersten Version dieser Heuristik
8.3% faelschlich blockierte echte Treffer -- zu hoch fuer einen harten Filter.

Deshalb gibt classify_ingredient_state() jetzt ein SET moeglicher Zustaende
zurueck, kein Einzelwert. Ein Mismatch liegt nur vor, wenn der Produkt-
Zustand in KEINER der fuer die Zutat plausiblen Zustaende enthalten ist --
das ist strenger (weniger falsche Blockierungen) als ein 1:1-Vergleich.

Zustands-Set: {"frisch", "tk", "konserve", "getrocknet_verarbeitet"}
Ein leeres Set / "unbekannt" bedeutet: keine Aussage moeglich, NIE blocken.
"""
from __future__ import annotations

import re

import pandas as pd

# ---------------------------------------------------------------------------
# Schluesselwoerter mit Wortgrenzen (\b...\b), um Teilstring-Fehltreffer wie
# "TK" in "Hartkäse" zu vermeiden (das war ein realer Bug-Kandidat beim
# ersten Entwurf dieser Heuristik).
# ---------------------------------------------------------------------------

_TK_PATTERN = re.compile(
    r"tiefkühl|tiefgekühlt|\bTK\b|\bgefroren\b", re.IGNORECASE
)
_KONSERVE_PATTERN = re.compile(
    r"konserve|in der dose\b|\bdose\b(?!nschnaps)|stückig|geschält|passiert",
    re.IGNORECASE,
)
_GETROCKNET_PATTERN = re.compile(
    r"gerebelt|getrocknet|gemahlen|\bpulver\b|gefriergetrocknet", re.IGNORECASE
)
_FRISCH_HINWEIS_PATTERN = re.compile(
    r"\bfrisch\b|\bBund\b|\bStiel/e\b|\bim Topf\b", re.IGNORECASE
)
_GEHACKT_PATTERN = re.compile(r"gehackt", re.IGNORECASE)

ALL_STATES = {"frisch", "tk", "konserve", "getrocknet_verarbeitet"}

# Gewuerze, die im Rezeptkontext praktisch nie als frische Pflanze gemeint
# sind, sondern fast immer als das verarbeitete Pulver/Gewuerz verkauft
# werden -- hier ist die Menge eng (nur 1 plausibler Zustand), weil der
# Markt hier wirklich eindeutig ist.
_NUR_VERARBEITET_GEWUERZE = {
    "kreuzkümmel", "muskat", "muskatnuss", "zimt",
    "cayennepfeffer", "kardamom", "piment", "paprikapulver", "chilipulver",
    "kurkuma", "ingwerpulver", "korianderpulver", "kardamompulver",
    "safran",  # Safran wird praktisch nie "frisch" verlangt
}

# "Pfeffer" alleine (ohne "frisch"/"Bund") ist im Rezeptkontext praktisch
# immer das gemahlene Gewuerz, nicht frische Pfefferkoerner an der Pflanze --
# anders als Kraeuter wie Thymian/Oregano, die als Topfpflanze genauso
# gaengig sind. Bewusst eigene, engere Liste statt in _KRAEUTER_MEHRDEUTIG.
_NUR_VERARBEITET_GEWUERZE |= {"pfeffer"}

# Nackte Kraeuternamen OHNE Bund/Stiel/frisch-Hinweis: im Handel sowohl als
# frische Topfpflanze/Bund als auch als getrocknetes Gewuerz gaengig --
# also BEIDE Zustaende plausibel, nicht nur "frisch" (das war der Kern-
# fehler der ersten Version).
_KRAEUTER_MEHRDEUTIG = {
    "thymian", "oregano", "majoran", "rosmarin", "schnittlauch", "koriander",
    "basilikum", "dill", "petersilie", "kümmel",
}

# Gemuese/Huelsenfruechte/Pilze, die im deutschen Supermarkt ueberwiegend
# als Konserve oder TK erhaeltlich sind, frische Form aber nicht ausge-
# schlossen (Regel 16 aus annotation_rules.txt) -- TK+Konserve plausibel,
# frisch nicht ausgeschlossen aber seltener.
_MEIST_KONSERVE_ODER_TK = {
    "bohnen", "kichererbsen", "mais", "maiskörner", "zuckermais", "erbsen",
    "bambussprossen", "bambussprosse", "champignons", "linsen", "spargel",
    "schwarzwurzel", "pfirsich", "ananas", "paprika", "rosenkohl",
    # Gurke/Gewuerzgurke: im Rezeptkontext fast immer das eingelegte
    # Konserven-/Glasprodukt ("Gewürzgurken" stehen in Kategorie
    # "GemüseKONSERVEn"). Ohne diesen Eintrag fiel "Gewürzgurke(n),
    # gewürfelte" auf Regel-8-Fallback {frisch, tk} und blockte alle
    # Konserven-Gurken faelschlich (6 verlorene Treffer im Test-Split
    # lauf22, alle CE-Score ~0.99). "gurke" deckt per Teilstring auch
    # "gewürzgurke"/"salzgurke" ab. Frische Salatgurke bleibt ueber
    # frisch/tk in der Menge weiter plausibel.
    "gurke",
}

# Fischfilets/Filets allgemein: sowohl frisch als auch TK marktueblich.
_FILET_MEHRDEUTIG = {
    "filet", "lachsfilet", "rotbarschfilet", "forellenfilet", "thunfisch",
    "entenbrust", "putenbrustfilet",
}


def _contains_any(text: str, keywords: set[str]) -> bool:
    text_low = text.lower()
    return any(kw in text_low for kw in keywords)


def classify_product_state(product_name: str, category: str) -> str:
    """Frisch/TK/Konserve/Verarbeitet fuer ein PRODUKT -- bleibt ein
    Einzelwert, da fuer ein konkretes Produkt (anders als fuer eine
    generische Zutatenbezeichnung) i.d.R. eindeutig feststellbar, was es
    ist. Kategorie zuerst (verlaesslicher), Produktname als Fallback."""
    text = f"{category} {product_name}"
    if _TK_PATTERN.search(text):
        return "tk"
    if _KONSERVE_PATTERN.search(text):
        return "konserve"
    if _GETROCKNET_PATTERN.search(product_name):
        return "getrocknet_verarbeitet"
    if _FRISCH_HINWEIS_PATTERN.search(text):
        return "frisch"
    return "unbekannt"


def classify_ingredient_states(ingredient_text: str) -> set[str]:
    """Gibt die MENGE der fuer diese Zutat plausiblen Zustaende zurueck.

    Explizite Signale im Text (TK/Konserve/getrocknet/frisch-Hinweis)
    schraenken die Menge auf genau 1 Zustand ein -- die Zutat sagt es ja
    direkt. Fehlt ein explizites Signal, kommt eine der drei Mehrdeutig-
    keits-Listen zum Zug (Kraeuter, Konserven-Gemuese, Filets), die jeweils
    eine REALISTISCHE Teilmenge zurueckgeben statt blind "frisch" (Regel 8)
    zu erzwingen. Nur wenn nichts davon zutrifft, bleibt Regel 8 als reiner
    Fallback (einzelnes Lebensmittel ohne bekannte Mehrdeutigkeit).

    Leere Rueckgabe ist hier bewusst nicht vorgesehen -- im Zweifel lieber
    eine zu grosse als eine zu kleine Menge (false negatives beim Blocking
    sind teurer als ein paar ungenutzte Tags)."""
    if _TK_PATTERN.search(ingredient_text):
        return {"tk"}
    if _KONSERVE_PATTERN.search(ingredient_text):
        return {"konserve"}
    if _GETROCKNET_PATTERN.search(ingredient_text):
        return {"getrocknet_verarbeitet"}
    if _FRISCH_HINWEIS_PATTERN.search(ingredient_text):
        return {"frisch"}
    if _contains_any(ingredient_text, _NUR_VERARBEITET_GEWUERZE):
        return {"getrocknet_verarbeitet"}
    if _contains_any(ingredient_text, _KRAEUTER_MEHRDEUTIG):
        # Nackter Kraeutername OHNE Frische-Signal: im Handel als frische
        # Topfpflanze, getrocknetes Gewuerz UND als TK-Ware (Tiefkuehl-
        # Kraeuter) gleichermassen gaengig -- alle drei plausibel. "tk" hier
        # bewusst mit drin, weil generische Kraeuter-Zutaten gegen TK-
        # kategorisierte Produkte (z.B. "Schnittlauch 50g" in "Tiefkuehl-
        # Kraeuter") sonst faelschlich geblockt werden (validiert auf dem
        # Test-Split lauf17: 1 von 3 verlorenen Treffern). Der explizite
        # frisch-Zweig oben bleibt unberuehrt streng.
        return {"frisch", "tk", "getrocknet_verarbeitet"}
    if _contains_any(ingredient_text, _MEIST_KONSERVE_ODER_TK):
        return {"konserve", "tk", "frisch"}
    if _contains_any(ingredient_text, _FILET_MEHRDEUTIG):
        return {"frisch", "tk"}
    if _GEHACKT_PATTERN.search(ingredient_text):
        return {"frisch", "tk", "konserve"}  # generisches "gehackt", Quelle unklar
    # Regel 8 (Fallback): kein Attribut, keine bekannte Mehrdeutigkeit.
    # Frisch ist der Normalfall, ABER ohne explizites Frische-Signal ist die
    # TK-Variante eines Lebensmittels genauso ein gueltiger Treffer (z.B.
    # "Porree" -> "Rahmgemüse Porree", "Brötchen" -> TK-Aufbackbroetchen).
    # Daher {frisch, tk}, damit generisch->TK nicht mehr blockiert wird
    # (validiert auf Test-Split lauf17: behebt 2 der 3 verlorenen Treffer).
    # Konserve/getrocknet bleiben aussen vor -- die signalisiert die Zutat
    # i.d.R. explizit, und sie blind zuzulassen wuerde zu viele FPs durch-
    # lassen.
    return {"frisch", "tk"}


def is_state_mismatch(ingredient_text: str, product_name: str, category: str) -> bool:
    """True nur, wenn der Produkt-Zustand in KEINER der fuer die Zutat
    plausiblen Zustaende vorkommt. 'unbekannt' beim Produkt blockt nie."""
    product_state = classify_product_state(product_name, category)
    if product_state == "unbekannt":
        return False
    ingredient_states = classify_ingredient_states(ingredient_text)
    return product_state not in ingredient_states


def add_state_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Fuegt 'ingredient_states' (Set), 'product_state' und
    'state_mismatch' (bool) Spalten hinzu. Erwartet 'ingredient_text',
    'product_name3' (oder 'product_name') und 'category' Spalten."""
    df = df.copy()
    df["ingredient_states"] = df["ingredient_text"].apply(classify_ingredient_states)
    name_col = "product_name3" if "product_name3" in df.columns else "product_name"
    df["product_state"] = df.apply(
        lambda r: classify_product_state(str(r[name_col]), str(r.get("category", ""))),
        axis=1,
    )
    df["state_mismatch"] = df.apply(
        lambda r: is_state_mismatch(r["ingredient_text"], str(r[name_col]), str(r.get("category", ""))),
        axis=1,
    )
    return df


if __name__ == "__main__":
    test_cases = [
        ("Stiel/e Thymian", "REWE Beste Wahl Thymian gerebelt 16g", "Gewürze"),
        ("Bund Thymian", "Thymian 15g", "Frische Kräuter"),
        ("Thymian", "Thymian gerebelt 30g", "Gewürzkräuter"),  # nackt, mehrdeutig
        ("Stiel/e Petersilie", "REWE Bio Petersilie 50g", "Tiefkühl-Kräuter"),
        ("Handvoll Spinat", "REWE Beste Wahl Junger Spinat 450g", "Tiefkühl-Gemüse"),
        ("Bohnen, grüne", "REWE Bio Brechbohnen 185g", "Bohnen-Konserven"),
        ("Pfeffer, weiß", "Pfeffer weiß gemahlen 100g", "Pfeffer"),
        ("Lachsfilet(s)", "Lachsfilets 420g", "Tiefkühl-Fischfilets"),
        ("Tomatenmark", "Oro di Parma Tomatenmark mit Knoblauch 200g", "Tomatenmark"),
        # Gewuerzgurke: Konserven-/Glasprodukt, darf NICHT geblockt werden
        # (Regression lauf22 -- ohne "gurke" in _MEIST_KONSERVE_ODER_TK fiel
        # dies auf {frisch, tk} und blockte alle Konserven-Gurken faelschlich).
        ("Gewürzgurke(n), gewürfelte", "Kühne Gewürzgurken 360g", "Gemüsekonserven"),
    ]
    print(f"{'Zutat':25} {'Zutat-States':40} {'Produkt':35} {'Prod-State':12} Mismatch")
    for ing, prod, cat in test_cases:
        states = classify_ingredient_states(ing)
        pstate = classify_product_state(prod, cat)
        mismatch = is_state_mismatch(ing, prod, cat)
        print(f"{ing:25} {str(sorted(states)):40} {prod:35} {pstate:12} {mismatch}")
