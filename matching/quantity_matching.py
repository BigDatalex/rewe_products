"""
Extrahiert Mengenangaben aus Rezeptzutaten und Produkttexten und berechnet
daraus, wie viele Packungen eines gematchten Produkts tatsaechlich gekauft
werden muessen (z.B. "1kg Hackfleisch" gegen ein "500g"-Produkt -> 2 Pakete;
"4 Eier" gegen "Bio Eier 10 Stück" -> 1 Packung).

Bewusste Einschraenkung: nur eindeutig numerische Mengenangaben werden
umgerechnet (Gewicht: g/kg, Volumen: ml/l/Liter, Stueckzahl: Zahl + Zaehl-
nomen wie "2 Zwiebel(n)" auf Zutatenseite, "X Stück" im Produktnamen auf
Produktseite). Vage Mengen ("etwas", "1 Prise", "1 Bund", "n. B.") werden
NICHT geraten -- fuer diese Zutaten wird keine Paketanzahl berechnet, das
Feld bleibt None. Lieber keine Aussage als eine falsche.

Die drei Mengen-DIMENSIONEN (Gewicht/Volumen/Stueck) werden nicht
gegeneinander umgerechnet (1g != 1ml != 1 Stueck) -- eine Paketanzahl wird
nur berechnet, wenn Zutat UND Produkt in der GLEICHEN Dimension eine Menge
haben. Stueckzahl auf Produktseite ist seltener als Gewicht/Volumen (~43
Lebensmittel-relevante Produkte im Katalog, v.a. Obst/Gemuese/Eier wie
"Zitrone 1 Stück", "Bio Eier 10 Stück") -- fuer alle anderen Produkte ohne
erkennbare Menge bleibt das Ergebnis None, statt eine Stueckzahl von 1 zu
unterstellen.
"""
from __future__ import annotations

import math
import re

# ---------------------------------------------------------------------------
# Zutaten-Seite: Menge + Einheit am Anfang des Rohtexts extrahieren.
# Format ist nicht einheitlich ("400 g X", "1.5 TL, gehäuft Y", "6 m.-große
# Kartoffel(n)") -- wir matchen nur die ZAHL + bekannte EINHEIT direkt danach,
# alles andere (Adjektive wie "m.-große", Zusaetze wie ", gehäuft") wird
# ignoriert, nicht fehlinterpretiert.
# ---------------------------------------------------------------------------

_NUM = r"(\d+(?:[.,]\d+)?)"  # "400", "1.5", "1,5"

_WEIGHT_PATTERN = re.compile(rf"^{_NUM}\s*(kg|g)\b", re.IGNORECASE)
_VOLUME_PATTERN = re.compile(rf"^{_NUM}\s*(l|liter|ml)\b", re.IGNORECASE)

# Zaehlnomen, bei denen "Zahl + Wort" eine Stueckzahl ist, KEINE Einheit
# (z.B. "2 Zwiebel(n)" = 2 Stueck Zwiebel, nicht "Einheit Zwiebel").
# Erkannt ueber: Zahl direkt gefolgt von einem Wort, das NICHT eine der
# bekannten vagen Mengeneinheiten (EL/TL/Pck/Dose/Bund/Prise/...) ist.
_VAGUE_UNITS = {
    "el", "tl", "msp", "prise", "prisen", "becher", "dose", "dosen",
    "pck", "pck.", "packung", "packungen", "päckchen", "bund", "scheibe",
    "scheiben", "tasse", "tassen", "handvoll", "kopf", "köpfe", "zehe",
    "zehen", "stange", "stangen", "glas", "gläser", "msl", "tropfen",
    "blatt", "blätter",
}


def _parse_number(s: str) -> float:
    return float(s.replace(",", "."))


# Endungen, die auf einen TEIL eines groesseren Lebensmittels hinweisen,
# nicht auf das ganze, zaehlbare Stueck -- "Knoblauchzehe" zaehlt Zehen,
# aber Produkte wie "Knoblauch 1 Stück" zaehlen ganze Knollen. Ohne diesen
# Schutz wuerde "4 Knoblauchzehe(n)" gegen "Knoblauch 1 Stück" eine falsche
# Paketanzahl von 4 statt der korrekten 1 berechnen.
_PART_WORD_SUFFIXES = ("zehe", "scheibe", "blatt", "blätter", "stange",
                       "rippe", "viertel", "hälfte")


def extract_ingredient_quantity(raw: str) -> dict | None:
    """Gibt {'dimension': 'weight'|'volume'|'count', 'amount': float,
    'unit_base': 'g'|'ml'|'stk'} zurueck, oder None wenn keine eindeutige
    numerische Menge erkannt wurde."""
    text = raw.strip()

    m = _WEIGHT_PATTERN.match(text)
    if m:
        amount = _parse_number(m.group(1))
        unit = m.group(2).lower()
        grams = amount * 1000 if unit == "kg" else amount
        return {"dimension": "weight", "amount": grams, "unit_base": "g"}

    m = _VOLUME_PATTERN.match(text)
    if m:
        amount = _parse_number(m.group(1))
        unit = m.group(2).lower()
        ml = amount * 1000 if unit in ("l", "liter") else amount
        return {"dimension": "volume", "amount": ml, "unit_base": "ml"}

    # Stueckzahl: Zahl am Anfang, gefolgt von einem Wort, das NICHT in den
    # bekannten vagen Mengeneinheiten steht (sonst waere es z.B. "2 EL").
    m = re.match(rf"^{_NUM}\s*([a-zäöüß.]+)", text, re.IGNORECASE)
    if m:
        word = m.group(2).lower().rstrip(".")
        if word in _VAGUE_UNITS or word in ("kg", "g", "l", "liter", "ml"):
            return None
        if word.endswith(_PART_WORD_SUFFIXES):
            # "Knoblauchzehe", "Zitronenscheibe" etc. -- zaehlt einen TEIL,
            # nicht das ganze Lebensmittel. Eine Produkt-Stueckzahl ("1
            # Stück") zaehlt typischerweise das Ganze -- kein sicherer
            # Vergleich moeglich, also lieber kein Ergebnis als ein
            # falsches.
            return None
        amount = _parse_number(m.group(1))
        return {"dimension": "count", "amount": amount, "unit_base": "stk"}

    return None


# ---------------------------------------------------------------------------
# Produkt-Seite: extracted_grammage ist bereits ein einheitliches Format
# wie "500g", "1,5kg", "0,33l", "300ml". Mehrfachpackungen wie "2x180g"
# werden auf die GESAMTMENGE umgerechnet (2*180=360g), nicht auf eine
# einzelne Teileinheit -- das ist die Menge, die man beim Kauf EINER
# Packung tatsaechlich bekommt.
# ---------------------------------------------------------------------------

_GRAMMAGE_MULTI_PATTERN = re.compile(
    rf"^{_NUM}\s*x\s*{_NUM}\s*(kg|g|l|ml)\b", re.IGNORECASE
)
_GRAMMAGE_SINGLE_PATTERN = re.compile(rf"^{_NUM}\s*(kg|g|l|ml)\b", re.IGNORECASE)

# "X Stück" steht NICHT in extracted_grammage (das deckt nur Gewicht/Volumen
# ab), sondern im PRODUKTNAMEN selbst -- z.B. "Haehnlein Bio-Eier 10 Stück",
# "Zitrone 1 Stück", "Knoblauch 1 Stück". Betrifft v.a. Obst/Gemuese/Eier,
# wo Rezeptzutaten typischerweise in Stueckzahl statt Gewicht angegeben
# werden ("4 Eier", "1 Knoblauchzehe") -- ca. 43 Produkte im Katalog haben
# NUR eine Stueckzahl und kein Gewicht (Gemuese/Obst/Eier), der Rest der
# "X Stück"-Produkte hat zusaetzlich ein Gewicht, das schon ueber
# extracted_grammage abgedeckt ist.
_PIECE_COUNT_PATTERN = re.compile(rf"{_NUM}\s*Stück\b", re.IGNORECASE)


def extract_product_quantity(
    grammage: str | None, product_name: str | None = None
) -> dict | None:
    """Gibt {'dimension': 'weight'|'volume'|'count', 'amount': float
    (GESAMTMENGE der Packung, in g/ml/Stueck)} zurueck, oder None.

    Prueft zuerst Gewicht/Volumen aus `grammage` (extracted_grammage-Spalte),
    dann Stueckzahl aus `product_name` (z.B. "... 10 Stück"). Ein Produkt mit
    BEIDEM (z.B. "Hot Dog Rolls 6 Stück", extracted_grammage="270g") gibt
    bewusst die Gewichtsdimension zurueck -- Gewicht ist die praezisere,
    weniger ambige Angabe, wenn beide vorhanden sind.
    """
    if grammage and isinstance(grammage, str):
        text = grammage.strip()

        m = _GRAMMAGE_MULTI_PATTERN.match(text)
        if m:
            n_units = _parse_number(m.group(1))
            per_unit = _parse_number(m.group(2))
            unit = m.group(3).lower()
            total = n_units * per_unit
            if unit == "kg":
                return {"dimension": "weight", "amount": total * 1000}
            if unit == "g":
                return {"dimension": "weight", "amount": total}
            if unit == "l":
                return {"dimension": "volume", "amount": total * 1000}
            if unit == "ml":
                return {"dimension": "volume", "amount": total}

        m = _GRAMMAGE_SINGLE_PATTERN.match(text)
        if m:
            amount = _parse_number(m.group(1))
            unit = m.group(2).lower()
            if unit == "kg":
                return {"dimension": "weight", "amount": amount * 1000}
            if unit == "g":
                return {"dimension": "weight", "amount": amount}
            if unit == "l":
                return {"dimension": "volume", "amount": amount * 1000}
            if unit == "ml":
                return {"dimension": "volume", "amount": amount}

    # "N Stück" kann im Produktnamen ODER in der Grammage-Spalte stehen: bei
    # Obst/Gemuese ohne Gewicht liefert REWE die Stueckzahl teils nur als
    # grammage "1 Stück" (z.B. aus listing_grammage). Beide Quellen pruefen.
    # Reihenfolge unkritisch -- eine Stueckzahl ist eine Stueckzahl.
    for source in (grammage, product_name):
        if source and isinstance(source, str):
            m = _PIECE_COUNT_PATTERN.search(source)
            if m:
                return {"dimension": "count", "amount": _parse_number(m.group(1))}

    return None


def compute_package_count(
    ingredient_raw: str,
    product_grammage: str | None,
    product_name: str | None = None,
) -> dict | None:
    """Berechnet, wie viele Packungen des Produkts gekauft werden muessen,
    um die im Rezept verlangte Menge zu decken.

    Gibt None zurueck, wenn:
      - die Zutat keine eindeutige numerische Menge hat, ODER
      - das Produkt keine extrahierbare Menge hat (weder Grammage noch eine
        Stueckzahl im Namen), ODER
      - beide Mengen in unterschiedlichen Dimensionen sind (z.B. Zutat in
        Gramm, Produkt nur als Stueckzahl bekannt -- kein Vergleich moeglich)

    Sonst: {'packages_needed': int, 'ingredient_amount': float,
            'product_amount': float, 'dimension': str}
    """
    ing_qty = extract_ingredient_quantity(ingredient_raw)
    if ing_qty is None:
        return None

    prod_qty = extract_product_quantity(product_grammage, product_name)
    if prod_qty is None:
        return None

    if ing_qty["dimension"] != prod_qty["dimension"]:
        return None  # z.B. Zutat in Gramm, Produkt nur als Stueck bekannt

    if prod_qty["amount"] <= 0:
        return None

    packages_needed = math.ceil(ing_qty["amount"] / prod_qty["amount"])
    return {
        "packages_needed": packages_needed,
        "ingredient_amount": ing_qty["amount"],
        "product_amount": prod_qty["amount"],
        "dimension": ing_qty["dimension"],
    }


if __name__ == "__main__":
    test_cases = [
        ("1kg Hackfleisch", "500g", None),
        ("1 kg Hackfleisch", "500g", None),
        ("250 g Sahne", "200ml", None),  # unterschiedliche Dimension -> kein Ergebnis
        ("250 ml Sahne", "200ml", None),
        ("2 EL Tomatenmark", "200g", None),  # vage Menge -> kein Ergebnis
        ("4  Ei(er)", None, "Bio Eier frisch 10 Stück"),
        ("1  Zitrone(n)", None, "Zitrone 1 Stück"),
        ("4  Knoblauchzehe(n)", None, "Knoblauch 1 Stück"),  # Zehe != ganzer
                                                              # Knoblauch, siehe Hinweis unten
        ("1 Liter Wasser", "0,75l", None),
        ("500 g Tomaten, passierte", "2x400g", None),
        ("6 m.-große Kartoffel(n)", None, None),
    ]
    print(f"{'Zutat':25} {'Grammage':10} {'Produktname':30} -> Paketanzahl")
    for raw, grammage, name in test_cases:
        result = compute_package_count(raw, grammage, name)
        print(f"{raw:25} {str(grammage):10} {str(name):30} -> {result}")
