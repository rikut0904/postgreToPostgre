const $ = (id) => document.getElementById(id);

function getConn(prefix) {
  return {
    url: $(`${prefix}-url`).value.trim(),
    host: $(`${prefix}-host`).value.trim(),
    port: Number($(`${prefix}-port`).value || 5432),
    database: $(`${prefix}-db`).value.trim(),
    user: $(`${prefix}-user`).value.trim(),
    password: $(`${prefix}-pass`).value,
    sslMode: $(`${prefix}-ssl`).value.trim(),
  };
}

function setMessage(msg) {
  $("conn-message").textContent = msg;
}

async function testConnection(prefix) {
  setMessage("接続テスト中...");
  const res = await fetch("/api/test-connection", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ conn: getConn(prefix) }),
  });
  if (res.ok) {
    setMessage("接続成功");
  } else {
    const text = await res.text();
    setMessage(`接続失敗: ${text}`);
  }
}

function renderSchemaTree(data) {
  const root = $("schema-tree");
  root.innerHTML = "";
  data.schemas.forEach((schema) => {
    const block = document.createElement("div");
    block.className = "schema";
    block.innerHTML = `
      <h4>${schema.schema}</h4>
      <div class="objects">
        ${renderObjectList(schema.schema, "tables", "テーブル", schema.tables)}
        ${renderObjectList(schema.schema, "views", "ビュー", schema.views)}
        ${renderObjectList(schema.schema, "functions", "関数", schema.functions)}
        ${renderObjectList(schema.schema, "sequences", "シーケンス", schema.sequences)}
      </div>
    `;
    root.appendChild(block);
  });
}

function renderObjectList(schema, kind, label, items) {
  if (!items || items.length === 0) {
    return `<div class="object-list"><strong>${label}</strong><div>なし</div></div>`;
  }
  const checks = items
    .map(
      (name) => `
      <label class="object-item">
        <input type="checkbox" data-schema="${schema}" data-kind="${kind}" data-name="${name}" checked />
        ${name}
      </label>
    `
    )
    .join("");
  return `<div class="object-list"><strong>${label}</strong>${checks}</div>`;
}

function collectSelection() {
  const entries = Array.from(
    document.querySelectorAll("#schema-tree input[type=checkbox]:checked")
  );
  const map = new Map();
  for (const el of entries) {
    const schema = el.dataset.schema;
    if (!map.has(schema)) {
      map.set(schema, {
        name: schema,
        tables: [],
        views: [],
        functions: [],
        sequences: [],
      });
    }
    const obj = map.get(schema);
    const kind = el.dataset.kind;
    obj[kind].push(el.dataset.name);
  }
  return { schemas: Array.from(map.values()) };
}

async function browseSchemas() {
  setMessage("スキーマ取得中...");
  const res = await fetch("/api/browse", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ conn: getConn("src") }),
  });
  if (!res.ok) {
    setMessage(`取得失敗: ${await res.text()}`);
    return;
  }
  const data = await res.json();
  renderSchemaTree(data);
  setMessage("スキーマ取得完了");
}

function getOptions() {
  return {
    existingTableMode: $("opt-table").value,
    existingDataMode: $("opt-data").value,
    batchSize: Number($("opt-batch").value || 1000),
    workers: Number($("opt-workers").value || 0),
    disableFk: $("opt-fk").checked,
    dryRun: $("opt-dry").checked,
  };
}

async function startMigration() {
  clearLog();
  const payload = {
    source: getConn("src"),
    destination: getConn("dst"),
    selection: collectSelection(),
    options: getOptions(),
  };

  const res = await fetch("/api/migrate", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  if (!res.ok) {
    alert(`開始失敗: ${await res.text()}`);
    return;
  }
}

async function cancelMigration() {
  await fetch("/api/migrate/cancel", { method: "POST" });
}

function updateProgress(msg) {
  $("overall").textContent = `${msg.overallPercent}%`;
  setElapsedBase(msg.elapsedSeconds, msg.running);
  $("phase").textContent = msg.currentPhase;
  if (msg.logMessage) {
    appendLog(msg.logMessage);
  }
  const rows = msg.tables || [];
  const container = $("table-progress");
  container.innerHTML = "";
  rows.forEach((t) => {
    const row = document.createElement("div");
    row.className = "table-row" + (t.status === "failed" ? " failed" : "");
    row.innerHTML = `
      <span>${t.schema}.${t.name}</span>
      <span>${t.percent}% (${t.status})</span>
    `;
    container.appendChild(row);
  });

  const failed = msg.failedTables || [];
  const failedContainer = $("failed-tables");
  if (failedContainer) {
    if (failed.length > 0) {
      failedContainer.style.display = "block";
      failedContainer.innerHTML = "<h4>失敗したテーブル</h4>" +
        failed.map((f) => `<div class="failed-entry">${f.schema}.${f.name}: ${f.error}</div>`).join("");
    } else {
      failedContainer.style.display = "none";
      failedContainer.innerHTML = "";
    }
  }
}

let elapsedBase = 0;
let elapsedBaseAt = Date.now();
let elapsedTimer = null;

function setElapsedBase(seconds, running) {
  elapsedBase = Number(seconds || 0);
  elapsedBaseAt = Date.now();
  updateElapsed();
  if (!running) {
    stopElapsedTimer();
    return;
  }
  if (!elapsedTimer) {
    elapsedTimer = setInterval(updateElapsed, 1000);
  }
}

function updateElapsed() {
  const delta = Math.floor((Date.now() - elapsedBaseAt) / 1000);
  $("elapsed").textContent = `${elapsedBase + Math.max(0, delta)}s`;
}

function stopElapsedTimer() {
  if (elapsedTimer) {
    clearInterval(elapsedTimer);
    elapsedTimer = null;
  }
}

function appendLog(message) {
  const log = $("log");
  const lines = log.textContent ? log.textContent.split("\n") : [];
  if (lines.length > 0 && lines[lines.length - 1] === message) {
    return;
  }
  lines.push(message);
  if (lines.length > 300) {
    lines.splice(0, lines.length - 300);
  }
  log.textContent = lines.join("\n");
}

function clearLog() {
  $("log").textContent = "";
}

function initWS() {
  const proto = location.protocol === "https:" ? "wss" : "ws";
  const ws = new WebSocket(`${proto}://${location.host}/ws/progress`);
  ws.onmessage = (event) => {
    const msg = JSON.parse(event.data);
    if (msg.type === "progress") {
      updateProgress(msg.data);
    }
  };
}

document.addEventListener("DOMContentLoaded", () => {
  $("src-test").addEventListener("click", () => testConnection("src"));
  $("dst-test").addEventListener("click", () => testConnection("dst"));
  $("browse").addEventListener("click", browseSchemas);
  $("start").addEventListener("click", startMigration);
  $("cancel").addEventListener("click", cancelMigration);
  initWS();
});
