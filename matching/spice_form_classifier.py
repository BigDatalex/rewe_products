"""
Regelbasierte Form-Vorpruefung (GANZ vs. GEMAHLEN) fuer Zutat-Produkt-Paare --
KEIN ML, reine Schluesselwort-Heuristik, analog zu state_classifier.py und
color_classifier.py und post-CE als harter Veto ausgefuehrt.

Hintergrund: Die Fehleranalyse (cross_encoder_test_errors.csv, lauf22) zeigte
Form-Verwechslungen bei Gewuerzen als wiederkehrendes Muster mit SEHR HOHEN
CE-Scores (0.84-0.99) -- der Cross-Encoder ist sich sicher und liegt trotzdem
falsch, weil "Pfeffer", "Pfefferkoerner" und "Pfeffer gemahlen" sich textlich
sehr aehneln. Threshold-Tuning hilft nicht; ein nachgelagerter Regelfilter
schon. Beispiele (is_match=0, faelschlich pred=1):
    Prise(n) Pfeffer  -> Pfefferkörner weiß       (0.92)
    Prise(n) Pfeffer  -> Pfeffer schwarz ganz     (0.84)
    Piment, ganz      -> Piment gemahlen 35g      (0.99)

Diese Heuristik ist die FORM-Achse; die FARB-Achse (rot/weiß/schwarz/...) deckt
bereits color_classifier.py ab, der ZUSTAND (frisch/TK/Konserve/getrocknet)
state_classifier.py. Die drei sind komplementaer und werden post-CE gemeinsam
als Veto angewandt -- hier wird Farbe daher BEWUSST NICHT noch einmal behandelt.

WICHTIGE DESIGNENTSCHEIDUNG (uebernommen aus color_classifier.py):
Ein 1:1-Formvergleich ist zu starr. Viele Zutaten nennen GAR KEINE Form, das
passende Produkt aber schon -- diese duerfen NICHT geblockt werden. Die
Fehlerdaten belegen das direkt (is_match=1, korrekt trotz fehlender Form):
    Zimt              -> Zimtstangen 10 Stück     (match! Zutat nennt keine Form)
    Zimt              -> Zimt gemahlen 80g        (match!)
    Muskat, gerieben  -> Muskatnuss gemahlen      (match! gerieben ~ gemahlen)

Deshalb gilt KONSERVATIV: Es wird nur geblockt, wenn BEIDE Seiten eine Form
tragen und sich diese Formen widersprechen (leere Schnittmenge). Nennt eine
Seite keine Form, wird nie geblockt. "gerieben" und "gemahlen" liegen bewusst
in DERSELBEN Klasse (beides fein zerkleinert) -> Muskat gerieben != Muskatnuss
gemahlen wird NICHT geblockt.

Robustheit gegen Komposita -- ANDERS als bei den Farben:
Deutsche Komposita verkleben die Form-Tokens ("PfefferKÖRNER", "ZimtSTANGE",
"VanilleSCHOTE"). Eine strikte Wortgrenze vorn (\b) wuerde "körner" in
"Pfefferkörner" NICHT finden -> die GANZ-Tokens werden daher als Teilstring
gesucht. Das Risiko (z.B. "stange" in "Stangensellerie") ist gering, weil die
Gegenseite eine widersprechende Form tragen MUSS, damit ueberhaupt geblockt
wird. "ganz" dagegen steht frei und bekommt \b, um "ganzjährig" o.ae. nicht zu
treffen.
"""
from __future__ import annotations

import re

import pandas as pd

# ---------------------------------------------------------------------------
# GEMAHLEN / fein zerkleinert: Pulver, gemahlen, gerieben, geschrotet,
# gemoersert, gestossen. "Prise" zaehlt mit: eine Prise nimmt man von feinem
# Pulver, nie von ganzen Koernern -> impliziter Mahl-Hinweis auf Zutatenseite
# (deckt "Prise(n) Pfeffer" -> "Pfefferkörner/ganz" ab).
# "(?<!un)" schuetzt "ungemahlen": das ist GANZ, nicht gemahlen.
# ---------------------------------------------------------------------------
_GROUND_PATTERN = re.compile(
    r"(?<!un)gemahlen|gemörsert|gemoersert|geschrotet|geschroten|gerieben|"
    r"gestoßen|gestossen|pulver|\bprise",
    re.IGNORECASE,
)

# ---------------------------------------------------------------------------
# GANZ / unzerkleinert: ganze Koerner, Stangen, Schoten, Blaetter. Teilstring-
# Suche (kein vorderes \b) wegen der Komposita; "ganz" als freistehendes Wort
# mit \b. "ungemahlen" hier mit drin (== ganz).
# ---------------------------------------------------------------------------
_WHOLE_PATTERN = re.compile(
    r"\bganz(?:e[snr]?)?\b|körner|körnern|ungemahlen|stange|schote|"
    r"blätter|blatt",
    re.IGNORECASE,
)

GROUND = "gemahlen"
WHOLE = "ganz"

# ---------------------------------------------------------------------------
# AUSNAHMEN: ganz <-> gemahlen ist hier KEIN Widerspruch, weil man das Gewuerz
# typischerweise selbst aus der ganzen Ware zerkleinert. Validiert gegen
# project-1-11_cleaned.csv -- ohne diese Ausnahmen wuerden 4 echte Treffer
# (is_match=1) faelschlich geblockt:
#   "Pfeffer, frisch gemahlen" -> "Pfeffer ganz"      (Mühle: ganzes Korn mahlen)
#   "Prise(n) Muskat"          -> "Muskatnuss ganz"   (Muskat reibt man frisch)
#   "Nelke(n), gemahlen"       -> "Nelken ganz"
#
# 1) Muehlen-/Frisch-gemahlen-Hinweis auf ZUTATENSEITE: der Koch mahlt selbst
#    -> ein GANZES Produkt ist der richtige Treffer.
_MILL_PATTERN = re.compile(
    r"frisch gemahlen|aus der mühle|a\.?\s?d\.?\s?mühle|\bmühle\b|"
    r"frisch gerieben|frisch gemörsert",
    re.IGNORECASE,
)
# 2) Gewuerze, die im Handel fast nur als GANZE Ware verkauft und zuhause
#    gerieben/gemahlen werden -> ganz und gemahlen sind austauschbar. Bewusst
#    ENG (nicht z.B. Pfeffer, der oft schon vorgemahlen verkauft wird --
#    "Prise Pfeffer" -> "Pfefferkörner" ist ein echter Mismatch, is_match=0).
_GRIND_AT_HOME = re.compile(
    r"muskat|muskatnuss|\bnelke", re.IGNORECASE
)


def _form_is_interchangeable(ingredient_text: str) -> bool:
    """True, wenn ganz<->gemahlen fuer diese Zutat KEIN Widerspruch ist
    (Mühlen-Hinweis oder selbst-zu-mahlendes Gewuerz)."""
    return bool(_MILL_PATTERN.search(ingredient_text)
                or _GRIND_AT_HOME.search(ingredient_text))


def extract_forms(text: str) -> set[str]:
    """Menge der im Text genannten Formen: Teilmenge von {'gemahlen','ganz'}.

    Leere Menge = keine Formangabe -> diese Seite blockt nie. Beide Tags
    gleichzeitig (z.B. "Pfeffer ganz oder gemahlen") ist moeglich und fuehrt
    durch die Schnittmengen-Logik korrekt zu KEINEM Mismatch."""
    s = str(text)
    out: set[str] = set()
    if _GROUND_PATTERN.search(s):
        out.add(GROUND)
    if _WHOLE_PATTERN.search(s):
        out.add(WHOLE)
    return out


def is_form_mismatch(ingredient_text: str, product_name: str) -> bool:
    """True nur bei explizitem Formwiderspruch auf BEIDEN Seiten.

    Konservativ: blockt nicht, wenn eine Seite keine Form nennt. Ein Mismatch
    liegt nur vor, wenn Zutat- und Produktformen jeweils nicht leer sind und
    sich nicht ueberschneiden (z.B. Zutat={ganz}, Produkt={gemahlen}).

    Ausnahme: selbst-zu-mahlende Gewuerze (Mühle/Muskat/Nelke) -> ganz und
    gemahlen sind austauschbar, nie blocken."""
    if _form_is_interchangeable(ingredient_text):
        return False
    ing_forms = extract_forms(ingredient_text)
    if not ing_forms:
        return False
    prod_forms = extract_forms(product_name)
    if not prod_forms:
        return False
    return ing_forms.isdisjoint(prod_forms)


def add_form_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Fuegt 'ingredient_forms', 'product_forms' und 'form_mismatch' Spalten
    hinzu. Erwartet 'ingredient_text' und 'product_name3' (oder 'product_name').
    """
    df = df.copy()
    name_col = "product_name3" if "product_name3" in df.columns else "product_name"
    df["ingredient_forms"] = df["ingredient_text"].apply(extract_forms)
    df["product_forms"] = df[name_col].apply(lambda x: extract_forms(str(x)))
    df["form_mismatch"] = df.apply(
        lambda r: is_form_mismatch(r["ingredient_text"], str(r[name_col])),
        axis=1,
    )
    return df


if __name__ == "__main__":
    # Faelle aus cross_encoder_test_errors.csv (lauf22).
    # erwartet_block = soll geblockt werden (echter FP)?
    test_cases = [
        # (zutat, produkt, erwartet_block)
        ("Prise(n) Pfeffer", "Pfefferkörner weiß", True),
        ("Prise(n) Pfeffer", "Pfeffer schwarz ganz", True),
        ("Prise(n) Pfeffer", "Pfefferkörner schwarz", True),
        ("Piment, ganz", "Piment gemahlen 35g", True),
        # --- darf NICHT blocken ---
        ("Prise(n) Pfeffer", "Pfeffer weiß gemahlen", False),   # beide gemahlen
        ("Muskat, gerieben", "Muskatnuss gemahlen 50g", False), # gerieben ~ gemahlen
        ("Zimt", "Zimtstangen 10 Stück", False),                # Zutat ohne Form
        ("Zimt", "Zimt gemahlen 80g", False),                   # Zutat ohne Form
        ("Käse, gerieben", "Pizzakäse-Ersatz gerieben 180g", False),  # beide gemahlen
        ("Pfeffer, weißer", "BioWagner Pfeffer Schwarz Ganz 50g", False),  # Zutat ohne Form (Farbe blockt)
        ("Pfeffer", "Pfeffer schwarz ganz", False),             # generisch, keine Form
        ("Lorbeer, gemahlen", "Lorbeerblätter", True),          # gemahlen vs Blatt
        # --- Ausnahmen: ganz<->gemahlen austauschbar (selbst gemahlen) ---
        ("Pfeffer, schwarz, frisch gemahlen", "Pfeffer schwarz ganz", False),
        ("Prise(n) Muskat", "Muskatnuss ganz", False),
        ("Nelke(n), gemahlen", "REWE Bio Nelken ganz 30g", False),
        ("Muskat, gerieben", "Muskatnuss ganz", False),
    ]
    print(f"{'Zutat':22} {'Produkt':38} {'Z-Form':14} {'P-Form':14} {'block':6} {'ok'}")
    ok_all = True
    for ing, prod, expect in test_cases:
        block = is_form_mismatch(ing, prod)
        ok = "✓" if block == expect else "✗ FEHLER"
        if block != expect:
            ok_all = False
        print(f"{ing:22} {prod:38} {str(sorted(extract_forms(ing))):14} "
              f"{str(sorted(extract_forms(prod))):14} {str(block):6} {ok}")
    print("\nAlle Testfaelle korrekt." if ok_all else "\n!! Es gibt FEHLER.")
