"use strict";

const byId = (id) => document.getElementById(id);
const countFormat = new Intl.NumberFormat();
const percentFormat = new Intl.NumberFormat(undefined, {
  minimumFractionDigits: 1,
  maximumFractionDigits: 1,
});
const names = new Intl.Collator(undefined, { numeric: true, sensitivity: "base" });
let inspections = [];
let largestValid = 0;

function text(tag, value, className) {
  const element = document.createElement(tag);
  element.textContent = value;
  if (className) element.className = className;
  return element;
}

function requiredCount(value) {
  if (!Number.isSafeInteger(value) || value < 0) {
    throw new Error("The export contains an invalid count or timestamp.");
  }
  return value;
}

function checkedString(seconds) {
  const date = new Date(seconds * 1000);
  return Number.isNaN(date.getTime()) ? "Unknown date" : date.toLocaleString();
}

function validateExport(exported) {
  if (!exported || !Array.isArray(exported.datasets)) {
    throw new Error("The export is missing its datasets array.");
  }
  requiredCount(exported.generated_at_unix);
  const estimated = Object.hasOwn(exported, "estimated_unique_valid_smiles");
  const legacyExact = Object.hasOwn(exported, "unique_valid_smiles");
  if (estimated === legacyExact) {
    throw new Error("The export must contain one unique-count representation.");
  }
  requiredCount(estimated ? exported.estimated_unique_valid_smiles : exported.unique_valid_smiles);
  for (const row of exported.datasets) {
    if (!row || !["complete", "partial"].includes(row.status) ||
        ["dataset", "version", "dataset_url", "file", "file_url", "format", "axis", "found_column"]
          .some((field) => typeof row[field] !== "string")) {
      throw new Error("The export contains an invalid inspection row.");
    }
    for (const field of ["rows_scanned", "valid_smiles", "invalid_smiles", "empty_cells",
      "non_string_cells", "oversized_cells", "checked_at_unix"]) {
      requiredCount(row[field]);
    }
    requiredCount(row.valid_smiles + row.invalid_smiles);
  }
  return exported;
}

// The resolve URL carries the pinned revision. Do not use an unpinned dataset URL
// as a fallback, and keep encoded path segments intact when deriving browse links.
function pinnedLinks(row) {
  try {
    const download = new URL(row.file_url);
    if (download.origin !== "https://huggingface.co" || download.username ||
        download.password || download.hash) return null;
    const match = download.pathname.match(/^(\/datasets\/(?:[^/]+\/)?[^/]+)\/resolve\/([^/]+)\/(.+)$/);
    if (!match) return null;
    const [, datasetPath, revision, filePath] = match;
    if (decodeURIComponent(datasetPath.slice("/datasets/".length)) !== row.dataset ||
        decodeURIComponent(revision) !== row.version ||
        decodeURIComponent(filePath) !== row.file) return null;
    const browseBase = `${download.origin}${datasetPath}`;
    return {
      dataset: `${browseBase}/tree/${revision}`,
      file: `${browseBase}/blob/${revision}/${filePath}`,
      download: download.href,
    };
  } catch {
    return null;
  }
}

function link(label, href) {
  const element = text("a", label);
  element.href = href;
  element.rel = "noopener noreferrer";
  return element;
}

function renderRow(row) {
  const tr = document.createElement("tr");
  const datasetCell = document.createElement("td");
  const links = pinnedLinks(row);
  const name = text("span", row.dataset, "name");
  if (links) {
    const datasetLink = link(row.dataset, links.dataset);
    datasetLink.className = "name";
    datasetCell.append(datasetLink);
  } else {
    datasetCell.append(name);
  }
  datasetCell.append(text("span", row.file, "file"));
  datasetCell.append(text("span", `Revision ${row.version} · ${row.format}`, "meta"));
  if (links) {
    const actions = document.createElement("div");
    actions.className = "links";
    actions.append(link("Browse file", links.file), link("Download file", links.download));
    datasetCell.append(actions);
  } else {
    datasetCell.append(text("span", "Pinned Hub links unavailable", "meta"));
  }

  const scopeCell = document.createElement("td");
  const badge = text("span", row.status === "partial" ? "Partial" : "Complete", `badge ${row.status}`);
  scopeCell.append(badge);
  const column = text("span", `${row.axis} axis · found column `, "file");
  column.append(text("code", row.found_column));
  scopeCell.append(column);
  scopeCell.append(text("span", `${countFormat.format(row.rows_scanned)} rows scanned in this file`, "meta"));

  const validityCell = document.createElement("td");
  const parsed = row.valid_smiles + row.invalid_smiles;
  if (parsed) {
    const percent = row.valid_smiles / parsed * 100;
    validityCell.append(text("strong", `${percentFormat.format(percent)}% valid`));
    const bar = document.createElement("div");
    bar.className = "chart";
    bar.setAttribute("aria-hidden", "true");
    const valid = text("span", "", "valid");
    const invalid = text("span", "", "invalid");
    valid.style.width = `${percent}%`;
    invalid.style.width = `${100 - percent}%`;
    bar.append(valid, invalid);
    validityCell.append(bar);
  } else {
    validityCell.append(text("strong", "No parsed cells"));
  }
  validityCell.append(text("span", `${countFormat.format(row.valid_smiles)} valid · ${countFormat.format(row.invalid_smiles)} invalid`, "meta"));
  const volume = document.createElement("div");
  volume.className = "volume";
  volume.setAttribute("aria-hidden", "true");
  const volumeBar = text("span", "");
  volumeBar.style.width = `${largestValid ? row.valid_smiles / largestValid * 100 : 0}%`;
  volume.append(volumeBar);
  validityCell.append(volume);

  const otherCell = document.createElement("td");
  otherCell.append(text("span", `${countFormat.format(row.empty_cells)} empty`, "file"));
  otherCell.append(text("span", `${countFormat.format(row.non_string_cells)} non-string`, "file"));
  otherCell.append(text("span", `${countFormat.format(row.oversized_cells)} oversized`, "file"));

  const checkedCell = text("td", checkedString(row.checked_at_unix), "number");
  tr.append(datasetCell, scopeCell, validityCell, otherCell, checkedCell);
  return tr;
}

function renderResults() {
  const query = byId("search").value.trim().toLocaleLowerCase();
  const status = byId("status-filter").value;
  const sort = byId("sort").value;
  const rows = inspections.filter((row) =>
    (status === "all" || row.status === status) &&
    (!query || [row.dataset, row.file, row.version, row.format, row.axis, row.found_column]
      .some((part) => part.toLocaleLowerCase().includes(query))));
  const byName = (a, b) => names.compare(a.dataset, b.dataset) || names.compare(a.file, b.file);
  const ratio = (row) => row.valid_smiles / (row.valid_smiles + row.invalid_smiles);
  rows.sort((a, b) => {
    if (sort === "validity-desc" || sort === "validity-asc") {
      const aRatio = ratio(a);
      const bRatio = ratio(b);
      // Files without parsed cells have no validity percentage and sort last.
      if (Number.isNaN(aRatio)) return Number.isNaN(bRatio) ? byName(a, b) : 1;
      if (Number.isNaN(bRatio)) return -1;
      return (sort === "validity-desc" ? bRatio - aRatio : aRatio - bRatio) || byName(a, b);
    }
    if (sort === "valid-desc") return b.valid_smiles - a.valid_smiles || byName(a, b);
    if (sort === "rows-desc") return b.rows_scanned - a.rows_scanned || byName(a, b);
    if (sort === "checked-desc") return b.checked_at_unix - a.checked_at_unix || byName(a, b);
    return byName(a, b);
  });
  const fragment = document.createDocumentFragment();
  for (const row of rows) fragment.append(renderRow(row));
  byId("results").replaceChildren(fragment);
  byId("result-count").textContent = `Showing ${countFormat.format(rows.length)} of ${countFormat.format(inspections.length)} inspected files`;
  const empty = byId("empty");
  empty.hidden = rows.length !== 0;
  if (!rows.length) {
    empty.textContent = inspections.length ? "No inspected files match these filters." :
      "No inspected datasets are in this export yet.";
  }
}

async function loadDashboard() {
  try {
    const response = await fetch("./data.json");
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    const exported = validateExport(await response.json());
    inspections = exported.datasets;
    largestValid = inspections.reduce((max, row) => Math.max(max, row.valid_smiles), 0);
    byId("snapshot").textContent = `Snapshot generated ${checkedString(exported.generated_at_unix)}. Results cover only explicitly inspected files.`;
    byId("dataset-count").textContent = countFormat.format(new Set(inspections.map((row) => row.dataset)).size);
    byId("valid-count").textContent = countFormat.format(inspections.reduce((sum, row) => sum + row.valid_smiles, 0));
    if (Object.hasOwn(exported, "estimated_unique_valid_smiles")) {
      byId("unique-label").textContent = "Estimated unique valid strings";
      byId("unique-count").textContent = `≈${countFormat.format(exported.estimated_unique_valid_smiles)}`;
      byId("unique-method-note").textContent = "The across-file unique count is a HyperLogLog estimate (nominal relative standard error ≈0.8%).";
    } else {
      byId("unique-label").textContent = "Exact unique valid strings (legacy snapshot)";
      byId("unique-count").textContent = countFormat.format(exported.unique_valid_smiles);
      byId("unique-method-note").textContent = "This snapshot predates HyperLogLog. Migrate the cache and re-export to publish an estimated count.";
    }
    const complete = inspections.filter((row) => row.status === "complete").length;
    byId("status-count").textContent = `${countFormat.format(complete)} / ${countFormat.format(inspections.length - complete)}`;
    renderResults();
    byId("state").hidden = true;
    byId("dashboard").hidden = false;
  } catch (error) {
    const state = byId("state");
    state.classList.add("error");
    state.setAttribute("role", "alert");
    state.textContent = `Could not load ./data.json: ${error.message}. Generate the inspection export with the CLI and publish it alongside this page.`;
  }
}

for (const id of ["search", "status-filter", "sort"]) {
  byId(id).addEventListener(id === "search" ? "input" : "change", renderResults);
}
loadDashboard();
