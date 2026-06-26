// Klick auf das Symbol in der Toolbar → product_changer.js in die aktive
// REWE-Mapping-Seite injizieren. world: "MAIN" sorgt dafür, dass das Skript
// im selben Kontext wie ein Konsolen-Paste läuft (identisches Verhalten zum Test).
chrome.action.onClicked.addListener(async (tab) => {
  const url = tab.url || "";
  const isMappingPage = /^https:\/\/(www|shop)\.rewe\.de\/.*recipes\/mapping\//.test(url);

  if (!tab.id || !isMappingPage) {
    console.log("Kein REWE-Mapping-Tab – nichts zu tun.", url);
    return;
  }

  try {
    await chrome.scripting.executeScript({
      target: { tabId: tab.id },
      world: "MAIN",
      files: ["product_changer.js"],
    });
  } catch (err) {
    console.error("Injektion fehlgeschlagen:", err);
  }
});
