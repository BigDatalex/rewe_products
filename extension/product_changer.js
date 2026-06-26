// =============================================================================
//  REWE Mapping – Produkte automatisch auf die eigenen Angebots-Matches tauschen
// =============================================================================
//  Ablauf pro Rezept:
//    1. Alle Zutaten-Zeilen der Mapping-Seite einsammeln.
//    2. Pro Zeile das aktuell von REWE gewählte Produkt auslesen.
//    3. Mit der gewünschten Produkt-ID (eigenes Angebots-Match) vergleichen.
//    4. Nur bei Abweichung das Produkt über den Modify-Dialog austauschen.
//
//  Die Zeilen werden NACHEINANDER (await) abgearbeitet, weil der Modify-Dialog
//  global ist und immer nur für eine Zutat gleichzeitig offen sein kann.
// =============================================================================


// In eine IIFE gekapselt, damit wiederholtes Ausführen/Injizieren auf derselben
// Seite nicht an bereits deklarierten Konstanten scheitert.
(() => {

// ─── DOM-Helfer ───────────────────────────────────────────────────────────────

// Klick simulieren (React reagiert auf echte MouseEvents)
function simulateClick(element) {
    const event = new MouseEvent('click', { view: window, bubbles: true, cancelable: true });
    element.dispatchEvent(event);
}

// React-spezifische Eingabe-Erzwingung ins Suchfeld
function setValueForReweInput(inputElement, text) {
    inputElement.focus();
    const nativeInputValueSetter = Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype, "value").set;
    nativeInputValueSetter.call(inputElement, text);

    inputElement.dispatchEvent(new Event('input', { bubbles: true }));
    inputElement.dispatchEvent(new Event('change', { bubbles: true }));

    inputElement.blur();
    inputElement.focus();

    const enterEvent = new KeyboardEvent('keydown', {
        key: 'Enter', code: 'Enter', keyCode: 13, which: 13, bubbles: true, cancelable: true
    });
    inputElement.dispatchEvent(enterEvent);
}

// Wartet, bis ein Element im DOM auftaucht
function waitForElement(selector, timeout = 5000) {
    return new Promise((resolve, reject) => {
        const startTime = Date.now();
        (function check() {
            const el = document.querySelector(selector);
            if (el) return resolve(el);
            if (Date.now() - startTime > timeout) return reject(new Error("Timeout für Selektor: " + selector));
            setTimeout(check, 100);
        })();
    });
}

// Wartet, bis ein Element wieder verschwindet (z. B. Modify-Dialog nach dem Übernehmen)
function waitForElementGone(selector, timeout = 5000) {
    return new Promise((resolve, reject) => {
        const startTime = Date.now();
        (function check() {
            if (!document.querySelector(selector)) return resolve();
            if (Date.now() - startTime > timeout) return reject(new Error("Timeout: Element bleibt sichtbar: " + selector));
            setTimeout(check, 100);
        })();
    });
}

const sleep = (ms) => new Promise(r => setTimeout(r, ms));

// Schließt einen offenen Modify-Dialog wieder. WICHTIG: Der Dialog ist global –
// bleibt er nach einem fehlgeschlagenen Tausch offen, sieht der nächste Tausch
// das alte Suchfeld und tippt ins Leere bzw. in die falsche Zutat. Darum nach
// JEDEM Tausch (auch bei Fehlern) sicherstellen, dass er geschlossen ist.
//
// REWE reagiert weder auf Escape noch auf einen Klick außerhalb – die Maske
// lässt sich NUR über den Zurück-Pfeil schließen
// (button.imr-SearchTile__productSearchInputBackButton).
async function ensureDialogClosed() {
    const SEARCH = 'input.imr-SearchTile__productSearchInput';
    const BACK   = 'button.imr-SearchTile__productSearchInputBackButton';
    if (!document.querySelector(SEARCH)) return;

    for (let i = 0; i < 4 && document.querySelector(SEARCH); i++) {
        const backBtn = document.querySelector(BACK);
        if (backBtn) {
            simulateClick(backBtn);
        } else {
            console.warn('Zurück-Pfeil nicht gefunden – Dialog kann nicht geschlossen werden.');
            break;
        }
        await waitForElementGone(SEARCH, 1500).catch(() => {});
    }
}


// ─── Zutaten-Zeilen der Mapping-Seite auslesen ────────────────────────────────

// Liefert für jede Zutat: das stabile ID-Klassen-Kennzeichen, den Zutatentext
// und die aktuell gewählte Produkt-ID. Zeilen ohne Produkt/Modify-Button (z. B.
// "Salz & Pfeffer") werden übersprungen.
function collectIngredientRows() {
    const rows = [];
    document.querySelectorAll('.imr-IngredientsListItem').forEach(item => {
        const modifyBtn = item.querySelector('.imr-ModifyButton');
        if (!modifyBtn) return; // keine austauschbare Zutat

        // Stabiles Kennzeichen wie "imr-IngredientsListItem__id-2-1" – damit wir
        // die Zeile nach einem Re-Render der Seite wiederfinden.
        const idClass = [...item.classList].find(c => c.startsWith('imr-IngredientsListItem__id-'));

        const termEl = item.querySelector('.imr-IngredientsListItem__searchTerm__Content');
        const searchTerm = termEl ? termEl.textContent.trim() : null;

        const tile = item.querySelector('.imr-IngredientsListItem__tile .imr-Tile');
        const currentProductId = tile ? tile.id : null;

        // Aktuelle Packungszahl (REWE startet bei 1)
        const amountInput = item.querySelector('.imr-AmountHandler__Amount');
        const currentAmount = amountInput ? (parseInt(amountInput.value, 10) || 1) : 1;

        rows.push({ idClass, searchTerm, currentProductId, currentAmount });
    });
    return rows;
}


// ─── Ein einzelnes Produkt austauschen (dein erprobter Workflow) ──────────────

// Wartet, bis die Produktsuche ein Ergebnis liefert, und gibt den passenden
// "Artikel übernehmen"-Button zurück. Bevorzugt die Ergebnis-Kachel mit der
// gewünschten Produkt-ID; nur wenn die nicht auffindbar ist, das erste Ergebnis.
// Wirft, wenn nach `timeout` ms gar kein Ergebnis erscheint (z. B. Produkt nicht
// (mehr) im Sortiment) – dann KEIN Button blind klicken.
async function waitForSubmitButton(productId, timeout = 6000) {
    const BTN = 'button.imr-AlternativeProductListItemCallToAction__Button';
    const start = Date.now();
    while (Date.now() - start < timeout) {
        const tile = document.getElementById(String(productId));
        if (tile) {
            const container = tile.closest('.imr-AlternativeProductListItem') || tile.parentElement;
            const btn = container && container.querySelector(BTN);
            if (btn) return btn;
        }
        const fallback = document.querySelector(BTN);
        if (fallback) return fallback;
        await sleep(100);
    }
    throw new Error(`Kein Suchergebnis/Übernehmen-Button für Produkt ${productId} (Sortiment?).`);
}

async function swapProductForRow(idClass, desired) {
    // Niemals einen zweiten Dialog über einem hängengebliebenen öffnen.
    await ensureDialogClosed();

    // Zeile frisch aus dem DOM holen (kann nach vorherigem Tausch neu gerendert sein)
    const item = document.querySelector('.' + idClass);
    if (!item) throw new Error(`Zutaten-Zeile ${idClass} nicht gefunden.`);

    const modifyBtn = item.querySelector('.imr-ModifyButton');
    if (!modifyBtn) throw new Error(`Kein Modify-Button in ${idClass}.`);

    // 1. Modify-Dialog öffnen
    simulateClick(modifyBtn);

    try {
        // 2. Auf das Suchfeld warten
        const searchInput = await waitForElement('input.imr-SearchTile__productSearchInput');

        // 3. Gewünschte Produkt-ID eintragen und Suche auslösen (zuverlässigste Auswahl)
        setValueForReweInput(searchInput, String(desired.productId));

        // 4. Auf das passende Suchergebnis warten und "Artikel übernehmen" klicken
        const submitBtn = await waitForSubmitButton(desired.productId);
        simulateClick(submitBtn);

        // 5. Warten, bis der Dialog geschlossen ist, bevor die nächste Zutat drankommt
        await waitForElementGone('input.imr-SearchTile__productSearchInput').catch(() => {});
        await sleep(300); // kleine Puffer-Pause für das Re-Render der Kachel
    } finally {
        // Egal ob erfolgreich oder Fehler: Dialog garantiert schließen, sonst
        // vergiftet er alle folgenden Zutaten.
        await ensureDialogClosed();
    }
}

// Setzt den angezeigten Zutaten-Titel zurück auf den ursprünglichen Zutatentext.
// Nach der ID-Suche steht dort sonst die ID. Achtung: rein kosmetisch – React
// kann den Text bei einem erneuten Re-Render der Zeile wieder überschreiben.
function restoreTitle(idClass, originalText) {
    const item = document.querySelector('.' + idClass);
    if (!item) return;
    const el = item.querySelector('.imr-IngredientsListItem__searchTerm__Content');
    if (el) el.textContent = originalText;
}


// ─── Anzahl der Packungen eines Produkts setzen ───────────────────────────────
//
//  Ist das gewählte Produkt kleiner als die benötigte Menge, braucht das Rezept
//  mehrere Packungen (package_info.packages_needed > 1). REWE startet bei 1; wir
//  klicken den Plus-/Minus-Button so oft, bis der Zähler den Zielwert erreicht.
//  Wir lesen nach jedem Klick den echten Input-Wert neu, statt blind zu zählen –
//  so kann das Re-Render uns nicht aus dem Tritt bringen.
async function setAmountForRow(idClass, targetAmount) {
    const target = Math.max(1, parseInt(targetAmount, 10) || 1);

    for (let guard = 0; guard < 30; guard++) {
        const item = document.querySelector('.' + idClass);
        if (!item) throw new Error(`Zutaten-Zeile ${idClass} nicht gefunden.`);

        const input = item.querySelector('.imr-AmountHandler__Amount');
        const current = input ? (parseInt(input.value, 10) || 1) : 1;
        if (current === target) return;

        const btn = current < target
            ? item.querySelector('.imr-AmountHandler__ButtonIncrease')
            : item.querySelector('.imr-AmountHandler__ButtonDecrease');
        if (!btn) throw new Error(`Mengen-Button (${current}→${target}) in ${idClass} nicht gefunden.`);

        simulateClick(btn);
        await sleep(250); // auf das Re-Render des Zählers warten
    }
    console.warn(`Mengenanpassung in ${idClass} nach 30 Schritten abgebrochen.`);
}


// ─── Gewünschte Produkte von GitHub Pages laden ───────────────────────────────
//
//  data/cheapest_mappings.json ist nach dem Mapping-Hash indexiert (letztes
//  Pfadsegment der URL). Pro Rezept liegt dort eine Zutatenliste mit dem
//  günstigsten Produkt + benötigter Packungszahl. Wird einmal geladen und
//  nach Zutatennamen indexiert.
const DATA_URL = "https://bigdatalex.github.io/rewe_products/data/cheapest_mappings.json";

// Führende Menge + Einheit aus dem Zutatentext entfernen.
const INGREDIENT_UNITS = new Set([
    "g", "kg", "mg", "ml", "l", "cl", "el", "tl", "msp", "stk", "stück", "stueck",
    "prise", "prisen", "dose", "dosen", "packung", "pck", "pkg", "bund", "tasse",
    "tassen", "becher", "glas", "scheibe", "scheiben", "zehe", "zehen", "kopf",
    "liter", "gramm", "blatt", "blätter",
]);

function ingredientName(raw) {
    let s = (raw || "").trim().toLowerCase();
    s = s.replace(/^[\d.,/\s½¼¾⅓⅔⅛–-]+/, "");   // führende Zahlen/Brüche weg
    let parts = s.split(/\s+/);
    if (parts.length && INGREDIENT_UNITS.has(parts[0])) parts = parts.slice(1); // Einheit weg
    s = parts.join(" ").split(",")[0];           // Zusätze nach Komma weg
    return s.replace(/\s+/g, " ").trim();
}

// ── Fuzzy-Matching ────────────────────────────────────────────────────────────
// Chefkoch und REWE formatieren Zutaten unterschiedlich (z.B. "Aubergine(n)" vs
// "Auberginen", "m.-große" vs "mittelgroße"). Darum vergleichen wir nicht den
// vollen Text, sondern Mengen von gestemmten Inhalts-Tokens und nehmen pro
// REWE-Zeile den ähnlichsten Eintrag aus unseren Daten.
const MATCH_STOP = new Set([
    "und", "oder", "mit", "im", "in", "aus", "dem", "den", "der", "die", "das",
    "zum", "zur", "von", "je", "ca", "etwa", "etwas", "evtl", "nach", "ggf",
    "ein", "eine", "einer", "einem", "sowie", "bzw", "a",
]);
// gestemmte Füll-Adjektive (Größe/Zubereitung/Farbe) – fürs Matching irrelevant
const MATCH_STOP_STEM = new Set([
    "groß", "klein", "mittelgroß", "mittel", "frisch", "getrocknet", "gehackt",
    "gerieben", "gemahl", "gewürfelt", "gekocht", "roh", "reif", "bio", "jung",
    "gelb", "rot", "grün", "weiß", "braun", "schwarz",
]);

// grobes deutsches Plural-/Flexions-Stemming (symmetrisch für Singular/Plural)
function stemToken(t) {
    return t.replace(/(en|er|n|e|s)$/, "");
}

function tokenize(name) {
    const set = new Set();
    for (let w of (name || "").toLowerCase().split(/[^a-zäöüß]+/)) {
        if (!w || w.length < 2 || MATCH_STOP.has(w)) continue;
        w = stemToken(w);
        if (w.length >= 2 && !MATCH_STOP_STEM.has(w)) set.add(w);
    }
    return set;
}

function matchScore(a, b) {
    if (!a.size || !b.size) return { overlap: 0, jaccard: 0 };
    let inter = 0;
    for (const t of a) if (b.has(t)) inter++;
    return { overlap: inter / Math.min(a.size, b.size), jaccard: inter / (a.size + b.size - inter) };
}

const MATCH_THRESHOLD = 0.6;

// Liste der gewünschten Produkte des Rezepts (mit vorberechneten Tokens).
let DESIRED_LIST = [];

async function loadDesiredProducts() {
    const hash = location.pathname.split("/").filter(Boolean).pop();
    const res = await fetch(DATA_URL, { cache: "no-cache" });
    if (!res.ok) throw new Error(`cheapest_mappings.json nicht ladbar (HTTP ${res.status})`);

    const all = await res.json();
    const entry = all[hash];
    if (!entry) throw new Error(`Kein Mapping-Eintrag für Hash ${hash}`);

    const list = entry.ingredients.map(i => ({
        name: i.name,
        productId: String(i.product_id),
        amount: i.amount || 1,
        tokens: tokenize(i.name),
    }));
    console.log(`Geladen: "${entry.recipe_name}" – ${list.length} Zutaten.`);
    return list;
}

// Beste Übereinstimmung für einen REWE-searchTerm: höchster Overlap, bei
// Gleichstand höchster Jaccard (bestraft überflüssige Tokens → exakter Treffer).
function getDesiredFor(searchTerm) {
    const q = tokenize(ingredientName(searchTerm));
    if (!q.size) return null;

    let best = null, bestOverlap = 0, bestJaccard = 0;
    for (const cand of DESIRED_LIST) {
        const { overlap, jaccard } = matchScore(q, cand.tokens);
        if (overlap > bestOverlap || (overlap === bestOverlap && jaccard > bestJaccard)) {
            best = cand; bestOverlap = overlap; bestJaccard = jaccard;
        }
    }
    if (bestOverlap < MATCH_THRESHOLD) return null;
    if (bestOverlap < 1) {
        console.debug(`fuzzy: "${searchTerm}" → "${best.name}" (overlap ${bestOverlap.toFixed(2)})`);
    }
    return best;
}


// ─── Hauptablauf ──────────────────────────────────────────────────────────────

async function runCompleteModifyWorkflow() {
    try {
        DESIRED_LIST = await loadDesiredProducts();
    } catch (err) {
        console.error("Abbruch – Produktdaten konnten nicht geladen werden:", err.message);
        return;
    }

    const rows = collectIngredientRows();
    console.log(`Gefundene austauschbare Zutaten: ${rows.length}`);

    let changed = 0, skipped = 0, missing = 0;

    for (const row of rows) {
        // Sicherheitsnetz: vor jeder Zutat darf kein Dialog mehr offen sein.
        await ensureDialogClosed();

        const desired = getDesiredFor(row.searchTerm);

        if (!desired) {
            console.log(`– "${row.searchTerm}": kein eigenes Match hinterlegt → übersprungen`);
            missing++;
            continue;
        }

        const targetAmount = desired.amount || 1;
        const needSwap   = String(row.currentProductId) !== String(desired.productId);
        const needAmount = (row.currentAmount || 1) !== targetAmount;

        if (!needSwap && !needAmount) {
            console.log(`✓ "${row.searchTerm}": bereits korrekt (${desired.productId} ×${targetAmount})`);
            skipped++;
            continue;
        }

        try {
            if (needSwap) {
                console.log(`↻ "${row.searchTerm}": ${row.currentProductId} → ${desired.productId} …`);
                await swapProductForRow(row.idClass, desired);
            }
            // Menge IMMER nach dem (möglichen) Tausch setzen: nach einem Tausch
            // steht der Zähler wieder auf 1.
            await setAmountForRow(row.idClass, targetAmount);
            // Titel zurück auf den Zutatentext (nur nach Tausch nötig). Als letzte
            // Aktion, damit ein Amount-Re-Render ihn nicht direkt wieder ersetzt.
            if (needSwap) restoreTitle(row.idClass, row.searchTerm);
            changed++;
            console.log(`  ✔ ${desired.productId} ×${targetAmount} gesetzt.`);
        } catch (err) {
            console.error(`  ✘ Fehler bei "${row.searchTerm}":`, err.message);
        }
    }

    console.log(`Fertig. Getauscht: ${changed}, bereits korrekt: ${skipped}, ohne Match: ${missing}.`);
}

// Gesamten Workflow starten
runCompleteModifyWorkflow();

})();
