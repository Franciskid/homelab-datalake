"""The console page. One file, no build step, no network requests.

Kept as a Python string rather than a template directory because there is one page
and no templating: the schema and the results arrive as JSON and are rendered by the
browser. A file that has to be found at runtime is one more thing that can be missing
from the image.
"""

PAGE = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Datalake SQL</title>
<style>
  :root {
    --bg: #14161a; --panel: #1b1e24; --line: #2a2f38; --text: #e6e9ef;
    --muted: #8b94a3; --accent: #6ea8fe; --error: #ff8080; --ok: #7ee2b8;
    --mono: ui-monospace, SFMono-Regular, "SF Mono", Menlo, Consolas, monospace;
  }
  * { box-sizing: border-box; }
  html, body { height: 100%; margin: 0; }
  body {
    background: var(--bg); color: var(--text); font: 14px/1.5 system-ui, sans-serif;
    display: grid; grid-template-columns: 260px 1fr; grid-template-rows: 100vh;
  }
  aside {
    background: var(--panel); border-right: 1px solid var(--line);
    overflow-y: auto; padding: 12px;
  }
  aside h1 { font-size: 13px; letter-spacing: .08em; text-transform: uppercase;
             color: var(--muted); margin: 0 0 12px; }
  .layer { margin-bottom: 14px; }
  .layer > b { display: block; color: var(--accent); font-size: 12px;
               text-transform: uppercase; letter-spacing: .06em; margin-bottom: 4px; }
  .tbl { cursor: pointer; padding: 2px 6px; border-radius: 4px; font-family: var(--mono);
         font-size: 12px; color: var(--text); }
  .tbl:hover { background: #262b34; }
  .cols { display: none; padding: 2px 0 6px 12px; color: var(--muted);
          font-family: var(--mono); font-size: 11px; word-break: break-all; }
  .cols.open { display: block; }
  main { display: grid; grid-template-rows: auto auto 1fr; min-width: 0; }
  #editor {
    width: 100%; height: 160px; resize: vertical; border: none; outline: none;
    background: #0f1114; color: var(--text); font-family: var(--mono); font-size: 13px;
    padding: 12px; border-bottom: 1px solid var(--line);
  }
  .bar { display: flex; gap: 10px; align-items: center; padding: 8px 12px;
         border-bottom: 1px solid var(--line); }
  button { background: var(--accent); color: #10131a; border: 0; border-radius: 5px;
           padding: 6px 14px; font-weight: 600; cursor: pointer; }
  button.ghost { background: transparent; color: var(--muted); border: 1px solid var(--line); }
  button:disabled { opacity: .5; cursor: default; }
  .status { color: var(--muted); font-size: 12px; }
  .status.err { color: var(--error); }
  .status.ok { color: var(--ok); }
  .out { overflow: auto; }
  table { border-collapse: collapse; font-family: var(--mono); font-size: 12px; }
  th, td { border-bottom: 1px solid var(--line); padding: 4px 10px; text-align: left;
           white-space: nowrap; }
  th { position: sticky; top: 0; background: var(--panel); color: var(--muted); }
  td.n { text-align: right; }
  td.null { color: #5c6572; font-style: italic; }
  .hint { padding: 16px; color: var(--muted); }
  .hint code { font-family: var(--mono); color: var(--accent); }
</style>
</head>
<body>
<aside>
  <h1>Lake</h1>
  <div id="schema" class="hint">loading…</div>
</aside>
<main>
  <textarea id="editor" spellcheck="false"
    placeholder="SELECT ... &#10;&#10;Ctrl/Cmd + Enter to run"></textarea>
  <div class="bar">
    <button id="run">Run</button>
    <button id="csv" class="ghost">CSV</button>
    <span id="status" class="status"></span>
  </div>
  <div class="out" id="out">
    <div class="hint">
      Three layers are in scope.
      <code>raw.*</code> is what landed, <code>curated.*</code> is cleaned and
      enriched, <code>marts.*</code> holds the facts and daily aggregates.
      Click a table on the left to insert it.
      <br><br>
      Read-only: SELECT and EXPLAIN, one statement, 1000 rows, 30 seconds.
    </div>
  </div>
</main>
<script>
const $ = (id) => document.getElementById(id);
const editor = $("editor"), status = $("status"), out = $("out");

const START = "SELECT partition_date, requests, visitors\nFROM marts.agg_traffic_daily\nORDER BY partition_date DESC\nLIMIT 20;";
editor.value = sessionStorage.getItem("sql") || START;

fetch("api/schema").then(r => r.json()).then(data => {
  const box = $("schema");
  box.className = "";
  box.innerHTML = "";
  const layers = data.layers || {};
  if (!Object.keys(layers).length) { box.className = "hint"; box.textContent = "nothing landed yet"; return; }
  for (const [layer, tables] of Object.entries(layers)) {
    const wrap = document.createElement("div");
    wrap.className = "layer";
    const head = document.createElement("b");
    head.textContent = layer;
    wrap.appendChild(head);
    for (const t of tables) {
      const row = document.createElement("div");
      row.className = "tbl";
      row.textContent = t.table;
      const cols = document.createElement("div");
      cols.className = "cols";
      cols.textContent = t.columns.join(", ");
      row.onclick = (e) => {
        if (e.altKey) { cols.classList.toggle("open"); return; }
        insert(layer + "." + t.table);
        cols.classList.toggle("open");
      };
      wrap.appendChild(row); wrap.appendChild(cols);
    }
    box.appendChild(wrap);
  }
}).catch(() => { $("schema").textContent = "could not read the schema"; });

function insert(name) {
  const at = editor.selectionStart;
  editor.value = editor.value.slice(0, at) + name + editor.value.slice(editor.selectionEnd);
  editor.focus();
  editor.selectionStart = editor.selectionEnd = at + name.length;
}

function say(text, kind) { status.textContent = text; status.className = "status " + (kind || ""); }

function render(data) {
  if (!data.columns.length) { out.innerHTML = '<div class="hint">no columns</div>'; return; }
  const table = document.createElement("table");
  const head = table.createTHead().insertRow();
  for (const c of data.columns) { const th = document.createElement("th"); th.textContent = c; head.appendChild(th); }
  const body = table.createTBody();
  for (const row of data.rows) {
    const tr = body.insertRow();
    for (const v of row) {
      const td = tr.insertCell();
      if (v === null) { td.textContent = "null"; td.className = "null"; }
      else if (typeof v === "number") { td.textContent = v; td.className = "n"; }
      else { td.textContent = v; }
    }
  }
  out.innerHTML = ""; out.appendChild(table);
}

async function run() {
  const sql = editor.value;
  sessionStorage.setItem("sql", sql);
  $("run").disabled = true; say("running…");
  try {
    const res = await fetch("api/query", {
      method: "POST", headers: {"content-type": "application/json"},
      body: JSON.stringify({sql})
    });
    const data = await res.json();
    if (!res.ok) { say(data.error, "err"); out.innerHTML = ""; return; }
    render(data);
    say(`${data.rows.length} row${data.rows.length === 1 ? "" : "s"}`
        + (data.truncated ? " (capped)" : "") + ` in ${data.elapsed_ms} ms`, "ok");
  } catch (e) { say(String(e), "err"); }
  finally { $("run").disabled = false; }
}

$("run").onclick = run;
$("csv").onclick = () => {
  const form = document.createElement("form");
  form.method = "POST"; form.action = "api/download";
  const field = document.createElement("input");
  field.type = "hidden"; field.name = "sql"; field.value = editor.value;
  form.appendChild(field); document.body.appendChild(form); form.submit(); form.remove();
};
editor.addEventListener("keydown", (e) => {
  if ((e.metaKey || e.ctrlKey) && e.key === "Enter") { e.preventDefault(); run(); }
});
</script>
</body>
</html>
"""
