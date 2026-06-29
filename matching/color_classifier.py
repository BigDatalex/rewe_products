"""
Regelbasierte Farb-Vorpruefung fuer Zutat-Produkt-Paare -- KEIN ML, reine
Schluesselwort-Heuristik, analog zu state_classifier.py und post-CE als
harter Veto ausgefuehrt.

Hintergrund: Die Fehleranalyse (cross_encoder_test_errors.csv von lauf18/
lauf19) zeigte Farb-Verwechslungen als wiederkehrendes Muster mit SEHR HOHEN
CE-Scores (0.85-0.99) -- d.h. der Cross-Encoder ist sich sicher und liegt
trotzdem falsch. Threshold-Tuning hilft dagegen nicht; ein nachgelagerter
Regelfilter schon. Beispiele (is_match=0, pred=1):
    Paprikaschote(n), grün   -> Paprika rot           (0.99)
    Zwiebel(n), weiße        -> Zwiebeln rot          (0.98)
    Pfeffer, weißer          -> Pfeffer schwarz       (0.99)
    Saucenbinder, dunkel     -> Saucenbinder hell     (0.98)

WICHTIGE DESIGNENTSCHEIDUNG (uebernommen aus state_classifier.py):
Wie beim Zustand ist ein 1:1-Farbvergleich zu starr. Viele Zutaten nennen
GAR KEINE Farbe, das passende Produkt aber schon -- diese duerfen NICHT
geblockt werden, sonst entstehen false negatives. Die Fehlerdaten belegen
das direkt (is_match=1, faelschlich pred=0 ohne Farbe in der Zutat):
    Linsen                   -> Braune/Gelbe Linsen    (match!)
    Pfeffer                  -> Pfeffer schwarz        (match!)

Deshalb gilt KONSERVATIV: Es wird nur geblockt, wenn BEIDE Seiten eine Farbe
tragen und sich diese Farben widersprechen (leere Schnittmenge). Nennt eine
Seite keine Farbe, wird nie geblockt.

Robustheit gegen Sortennamen: Farbtokens werden mit Wortgrenzen (\b...\b)
gesucht. Dadurch matcht "rot" NICHT in "Rotkohl", "Karotte" oder "Brot" --
zusammengesetzte Sortennamen (Rotkohl != Weißkohl) bleiben unberuehrt und
werden weiter vom Cross-Encoder entschieden, nicht von dieser Heuristik.
"""
from __future__ import annotations

import re

import pandas as pd

# ---------------------------------------------------------------------------
# Farbvokabular. Schluessel = kanonische Farbe, Werte = Regex-Alternativen
# (inkl. deutscher Flexionsendungen). Alles in \b...\b eingebettet, damit nur
# alleinstehende Farbwoerter matchen, keine Sortennamen-Bestandteile.
# "hell"/"dunkel" sind eine eigene Helligkeitsachse (Saucenbinder, Soße,
# Sesam) und werden wie Farben behandelt -- hell vs. dunkel ist ein ebenso
# klarer Widerspruch wie rot vs. grün.
# ---------------------------------------------------------------------------
_COLOR_ALTERNATIVES: dict[str, str] = {
    "rot":     r"rot|rote[snrm]?",
    "grün":    r"grün|grüne[snrm]?",
    "gelb":    r"gelb|gelbe[snrm]?",
    "weiß":    r"weiß|weiss|weiße[snrm]?|weisse[snrm]?",
    "schwarz": r"schwarz|schwarze[snrm]?",
    "blau":    r"blau|blaue[snrm]?",
    "braun":   r"braun|braune[snrm]?",
    "orange":  r"orange[nr]?",
    "hell":    r"hell|helle[snrm]?",
    "dunkel":  r"dunkel|dunkle[snrm]?",
}

_COLOR_PATTERNS: dict[str, re.Pattern] = {
    color: re.compile(rf"\b(?:{alts})\b", re.IGNORECASE)
    for color, alts in _COLOR_ALTERNATIVES.items()
}

# Wildcard-Hinweise: die Zutat akzeptiert ausdruecklich mehrere/beliebige
# Farben -> niemals wegen Farbe blocken (z.B. "Paprikaschote(n), bunt").
_WILDCARD_PATTERN = re.compile(
    r"\bbunt|\bgemischt|\bmehrfarbig|\bversch", re.IGNORECASE
)


def extract_colors(text: str) -> set[str]:
    """Menge der im Text alleinstehend genannten Farben (kanonische Namen).

    'schwarz-weiß' o.ae. liefert korrekt BEIDE Farben, weil jede Farbe
    einzeln per Wortgrenze gefunden wird."""
    s = str(text)
    return {c for c, pat in _COLOR_PATTERNS.items() if pat.search(s)}


def is_wildcard(text: str) -> bool:
    """True, wenn der Text Farb-Beliebigkeit signalisiert (bunt/gemischt)."""
    return bool(_WILDCARD_PATTERN.search(str(text)))


def is_color_mismatch(ingredient_text: str, product_name: str) -> bool:
    """True nur bei explizitem Farbwiderspruch auf BEIDEN Seiten.

    Konservativ: blockt nicht, wenn eine Seite keine Farbe nennt oder die
    Zutat eine Wildcard ('bunt') ist. Ein Mismatch liegt nur vor, wenn
    Zutat- und Produktfarben jeweils nicht leer sind und sich nicht
    ueberschneiden."""
    if is_wildcard(ingredient_text):
        return False
    ing_colors = extract_colors(ingredient_text)
    if not ing_colors:
        return False
    prod_colors = extract_colors(product_name)
    if not prod_colors:
        return False
    return ing_colors.isdisjoint(prod_colors)


def add_color_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Fuegt 'ingredient_colors', 'product_colors' und 'color_mismatch'
    Spalten hinzu. Erwartet 'ingredient_text' und 'product_name3'
    (oder 'product_name')."""
    df = df.copy()
    name_col = "product_name3" if "product_name3" in df.columns else "product_name"
    df["ingredient_colors"] = df["ingredient_text"].apply(extract_colors)
    df["product_colors"] = df[name_col].apply(lambda x: extract_colors(str(x)))
    df["color_mismatch"] = df.apply(
        lambda r: is_color_mismatch(r["ingredient_text"], str(r[name_col])),
        axis=1,
    )
    return df


if __name__ == "__main__":
    # Faelle aus cross_encoder_test_errors.csv (lauf18/lauf19).
    # erwartet_block = soll geblockt werden (echter FP)?
    test_cases = [
        # (zutat, produkt, erwartet_block)
        ("Paprikaschote(n), grün", "Paprika rot ca. 250g", True),
        ("Zwiebel(n), weiße", "Zwiebeln rot 300g", True),
        ("Pfeffer, weißer", "Pfeffer schwarz gemahlen 40g", True),
        ("Saucenbinder, dunkel", "Saucenbinder hell 250g", True),
        ("Paprikaschote(n), bunt", "Paprika rot ca. 250g", False),   # Wildcard
        ("Linsen", "Braune Linsen", False),                          # Zutat ohne Farbe
        ("Linsen", "Gelbe Linsen", False),                           # Zutat ohne Farbe
        ("Pfeffer", "Pfeffer schwarz ganz", False),                  # Zutat ohne Farbe
        ("Pfeffer, weißer", "Pfeffer schwarz-weiß geschrotet", False),  # weiß in beidem
        ("Rotkohl", "Weißkohl 1 Stück", False),                      # Sortennamen, Wortgrenze
    ]
    print(f"{'Zutat':28} {'Produkt':38} {'Z-Farben':18} {'P-Farben':18} {'block':6} {'ok'}")
    for ing, prod, expect in test_cases:
        block = is_color_mismatch(ing, prod)
        ok = "✓" if block == expect else "✗ FEHLER"
        print(f"{ing:28} {prod:38} {str(sorted(extract_colors(ing))):18} "
              f"{str(sorted(extract_colors(prod))):18} {str(block):6} {ok}")