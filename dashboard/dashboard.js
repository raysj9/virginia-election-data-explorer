const state = { response: null, selectedDistrict: null, messages: [] };
const numberFormatter = new Intl.NumberFormat("en-US");

function districtNumber(districtId) {
  return String(Number(districtId)).padStart(2, "0");
}

function formatNumber(value) {
  return value == null ? "—" : numberFormatter.format(value);
}

function formatShare(value) {
  return value == null ? "—" : `${(value * 100).toFixed(2)}%`;
}

function formatPoints(value) {
  if (value == null) return "—";
  const sign = value > 0 ? "+" : value < 0 ? "−" : "";
  return `${sign}${Math.abs(value).toFixed(2)} pp`;
}

function titleCase(value) {
  return value.toLowerCase().replace(/\b\w/g, (letter) => letter.toUpperCase());
}

function getResults() {
  return [...state.response.results].sort((a, b) => a.district_id.localeCompare(b.district_id));
}

function getSelectedResult() {
  return state.response.results.find((result) => result.district_id === state.selectedDistrict);
}

function partyClass(party) {
  const value = (party || "").toLowerCase();
  if (value.includes("dem")) return "democratic";
  if (value.includes("rep")) return "republican";
  return "other";
}

function make(tag, className, text) {
  const element = document.createElement(tag);
  if (className) element.className = className;
  if (text != null) element.textContent = text;
  return element;
}

function renderDistrictSelect() {
  const select = document.querySelector("#district-select");
  select.replaceChildren();
  for (const result of getResults()) {
    const option = document.createElement("option");
    option.value = result.district_id;
    option.textContent = `District ${districtNumber(result.district_id)}`;
    select.append(option);
  }
  select.value = state.selectedDistrict;
}

function renderShareComparison(result) {
  const target = document.querySelector("#share-comparison");
  target.replaceChildren();
  const axis = make("div", "share-axis");
  axis.setAttribute("aria-hidden", "true");
  const ticks = make("span", "share-ticks");
  for (const value of ["0%", "50%", "100%"] ) ticks.append(make("span", "", value));
  axis.append(make("span", ""), ticks, make("span", ""));
  target.append(axis);

  for (const year of [2026, 2022]) {
    const demShare = result[`democratic_share_${year}`];
    const repShare = result[`republican_share_${year}`];
    const row = make("div", `share-row${year === 2026 ? " latest" : ""}`);
    const yearLabel = make("div", "share-year");
    yearLabel.append(make("strong", "", String(year)), make("span", "", year === 2026 ? "Nov 3" : "Nov 8"));

    const barGroup = make("div", "share-bar-group");
    const bar = make("div", "share-bar");
    bar.setAttribute("role", "img");
    bar.setAttribute("aria-label", `${year}: Democratic ${formatShare(demShare)}, Republican ${formatShare(repShare)}`);
    const democratic = make("span", "share-segment democratic");
    democratic.style.width = `${Math.max(0, Math.min(1, demShare || 0)) * 100}%`;
    const republican = make("span", "share-segment republican");
    republican.style.width = `${Math.max(0, Math.min(1, repShare || 0)) * 100}%`;
    bar.append(democratic, republican);
    const exact = make("div", "share-values");
    exact.append(
      make("span", "democratic", `D ${formatShare(demShare)}`),
      make("span", "republican", `R ${formatShare(repShare)}`),
    );
    barGroup.append(bar, exact);
    const sourceStatus = titleCase(state.response.sources[String(year)].source_status);
    row.append(yearLabel, barGroup, make("span", `share-status${year === 2026 ? " unofficial" : ""}`, sourceStatus));
    target.append(row);
  }
}

function renderShareSummary(result) {
  const change = result.democratic_share_change_pp;
  const deltaClass = change > 0 ? "democratic" : change < 0 ? "republican" : "";
  const summary = document.querySelector("#share-summary");
  summary.replaceChildren();
  for (const [label, value, className] of [
    ["Democratic share · 2022", formatShare(result.democratic_share_2022), "democratic"],
    ["Democratic share · 2026", formatShare(result.democratic_share_2026), "democratic"],
    ["Change vs 2022", formatPoints(change), deltaClass],
  ]) {
    const item = make("div", "summary-item");
    item.append(make("span", "summary-label", label), make("strong", `summary-value ${className}`.trim(), value));
    summary.append(item);
  }
}

function renderCandidates(targetId, candidates) {
  const target = document.querySelector(targetId);
  target.replaceChildren();
  if (!candidates?.length) {
    target.append(make("p", "empty-cell", "Candidate detail is not available for this district."));
    return;
  }

  const table = make("table", "candidate-table");
  const head = document.createElement("thead");
  const header = document.createElement("tr");
  for (const [label, className] of [["Candidate", ""], ["Party", ""], ["Votes", "numeric"]]) {
    const cell = make("th", className, label);
    cell.scope = "col";
    header.append(cell);
  }
  head.append(header);
  const body = document.createElement("tbody");

  for (const candidate of [...candidates].sort((a, b) => (b.votes ?? -1) - (a.votes ?? -1))) {
    const kind = partyClass(candidate.party);
    const row = document.createElement("tr");
    row.append(
      make("td", `candidate-name ${kind}`, candidate.candidate_name || "Unnamed choice"),
      make("td", `candidate-party ${kind}`, candidate.party || "Not reported"),
      make("td", "candidate-votes", formatNumber(candidate.votes)),
    );
    body.append(row);
  }
  table.append(head, body);
  target.append(table);
}

function setSource(year, source) {
  const titleCaseStatus = titleCase(source.source_status);
  const statusClass = `source-status ${source.source_status.toLowerCase().includes("unofficial") ? "unofficial" : "official"}`;
  document.querySelector(`#source-status-${year}`).textContent = `${titleCaseStatus} results`;
  document.querySelector(`#source-status-${year}`).className = statusClass;
  document.querySelector(`#detail-status-${year}`).textContent = titleCaseStatus;
  document.querySelector(`#detail-status-${year}`).className = statusClass;
  document.querySelector(`#snapshot-${year}`).textContent = source.snapshot_id;
  document.querySelector(`#detail-snapshot-${year}`).textContent = source.snapshot_id;
  for (const id of [`#source-${year}`, `#detail-source-${year}`]) {
    const link = document.querySelector(id);
    link.href = source.source_url;
    link.setAttribute("aria-label", `Open Virginia Department of Elections ${year} source`);
  }
}

function renderDistrictDetail() {
  const result = getSelectedResult();
  if (!result) return;
  document.querySelector("#district-select").value = result.district_id;
  document.querySelector("#race-status").textContent = `${titleCase(state.response.sources["2026"].source_status)} · saved results`;
  renderShareComparison(result);
  renderShareSummary(result);
  renderCandidates("#candidates-2026", result.candidates_2026 || []);
  renderCandidates("#candidates-2022", result.candidates_2022 || []);
}

function selectDistrict(districtId) {
  state.selectedDistrict = districtId;
  renderDistrictDetail();
}

function selectResultsTab(tab) {
  const buttons = [...document.querySelectorAll(".results-tab")];
  for (const button of buttons) {
    const selected = button === tab;
    button.setAttribute("aria-selected", String(selected));
    button.tabIndex = selected ? 0 : -1;
    document.querySelector(`#${button.getAttribute("aria-controls")}`).hidden = !selected;
  }
}

async function loadResults() {
  const message = document.querySelector("#load-message");
  try {
    const response = await fetch("/api/v1/house-comparisons", { headers: { Accept: "application/json" } });
    if (!response.ok) throw new Error(`API returned HTTP ${response.status}`);
    state.response = await response.json();
    if (!state.response.results?.length) throw new Error("The API returned no district results.");

    for (const year of ["2022", "2026"]) setSource(year, state.response.sources[year]);
    state.selectedDistrict = getResults()[0].district_id;
    renderDistrictSelect();
    renderDistrictDetail();
    message.textContent = `Data validation: ${state.response.overall_status}`;
  } catch (error) {
    message.classList.add("error");
    message.textContent = `Could not load the election comparison: ${error.message}. Start the FastAPI server, then refresh.`;
    document.querySelector("#district-select").replaceChildren();
    document.querySelector("#district-select").disabled = true;
    document.querySelector("#share-comparison").replaceChildren();
    document.querySelector("#share-summary").replaceChildren();
    document.querySelector("#candidates-2026").textContent = "Results unavailable.";
    document.querySelector("#candidates-2022").textContent = "Results unavailable.";
  }
}

async function askAssistant(event) {
  event.preventDefault();
  const submit = document.querySelector("#assistant-submit");
  const questionField = document.querySelector("#assistant-question");
  const history = state.messages.slice(-12).map(({ role, content }) => ({ role, content }));
  const question = questionField.value.trim();
  if (!question) return;

  setChatOpen(true);
  const userMessage = { role: "user", content: question };
  state.messages.push(userMessage);
  appendChatMessage(userMessage);
  questionField.value = "";

  const assistantMessage = { role: "assistant", content: "Checking the saved results…", pending: true };
  state.messages.push(assistantMessage);
  const assistantNode = appendChatMessage(assistantMessage);
  submit.disabled = true;
  submit.textContent = "Sending";
  document.querySelector("#assistant-reset").disabled = true;

  try {
    const response = await fetch("/api/v1/assistant", {
      method: "POST",
      headers: { "Content-Type": "application/json", Accept: "application/json" },
      body: JSON.stringify({ question, selected_district: state.selectedDistrict, history }),
    });
    const result = await response.json();
    if (!response.ok) {
      const detail = typeof result.detail === "string" ? result.detail : `API returned HTTP ${response.status}`;
      throw new Error(detail);
    }

    assistantMessage.content = result.answer;
    assistantMessage.engine = result.engine;
    assistantMessage.note = result.note;
    assistantMessage.sources = result.sources || [];
    assistantMessage.pending = false;
  } catch (error) {
    assistantMessage.content = `Could not answer from the saved results: ${error.message}`;
    assistantMessage.pending = false;
  } finally {
    submit.disabled = false;
    submit.textContent = "Send";
    document.querySelector("#assistant-reset").disabled = false;
  }

  assistantNode.replaceWith(buildChatMessage(assistantMessage));
  scrollChatToLatest();
}

function buildChatMessage(message) {
  const entry = make("article", `assistant-message ${message.role}${message.pending ? " pending" : ""}`);
  const meta = make("div", "assistant-message-meta");
  meta.append(make("span", "assistant-speaker", message.role === "user" ? "You" : "Election assistant"));
  if (message.engine) {
    meta.append(make("span", "assistant-engine", message.engine === "openai" ? "OpenAI" : "Local lookup"));
  }
  entry.append(meta, make("p", "assistant-bubble", message.content));

  if (message.note) entry.append(make("p", "assistant-note", message.note));
  if (message.sources?.length) {
    const details = make("details", "assistant-citations");
    details.append(make("summary", "", "Sources"));
    const list = document.createElement("ul");
    for (const source of message.sources) {
      const item = document.createElement("li");
      const link = document.createElement("a");
      link.href = source.source_url;
      link.target = "_blank";
      link.rel = "noreferrer noopener";
      link.textContent = `${source.label} · ${source.source_status} · ${source.snapshot_id}`;
      item.append(link);
      list.append(item);
    }
    details.append(list);
    entry.append(details);
  }
  return entry;
}

function appendChatMessage(message) {
  const transcript = document.querySelector("#assistant-messages");
  document.querySelector("#ask-results").classList.add("has-messages");
  document.querySelector("#assistant-empty").hidden = true;
  const node = buildChatMessage(message);
  transcript.append(node);
  scrollChatToLatest();
  return node;
}

function scrollChatToLatest() {
  const transcript = document.querySelector("#assistant-messages");
  transcript.scrollTop = transcript.scrollHeight;
}

function setChatOpen(open) {
  const panel = document.querySelector("#ask-results");
  const toggle = document.querySelector("#assistant-toggle");
  panel.classList.toggle("open", open);
  toggle.setAttribute("aria-expanded", String(open));
  toggle.setAttribute("aria-label", open ? "Collapse conversation" : "Expand conversation");
}

document.querySelector("#assistant-form").addEventListener("submit", askAssistant);
document.querySelector("#assistant-toggle").addEventListener("click", () => {
  const expanded = document.querySelector("#assistant-toggle").getAttribute("aria-expanded") === "true";
  setChatOpen(!expanded);
});
document.querySelector("#assistant-reset").addEventListener("click", () => {
  state.messages = [];
  const transcript = document.querySelector("#assistant-messages");
  transcript.replaceChildren();
  document.querySelector("#ask-results").classList.remove("has-messages");
  const empty = make("p", "assistant-empty", "Ask about a district, candidate votes, or the share change.");
  empty.id = "assistant-empty";
  transcript.append(empty);
  setChatOpen(false);
  document.querySelector("#assistant-question").focus();
});

document.querySelector("#district-select").addEventListener("change", (event) => {
  selectDistrict(event.currentTarget.value);
});

const resultsTabList = document.querySelector(".results-tabs");
resultsTabList.addEventListener("click", (event) => {
  const tab = event.target.closest(".results-tab");
  if (tab) selectResultsTab(tab);
});
resultsTabList.addEventListener("keydown", (event) => {
  const tabs = [...resultsTabList.querySelectorAll(".results-tab")];
  const current = tabs.indexOf(document.activeElement);
  let next = current;
  if (event.key === "ArrowRight") next = (current + 1) % tabs.length;
  if (event.key === "ArrowLeft") next = (current - 1 + tabs.length) % tabs.length;
  if (event.key === "Home") next = 0;
  if (event.key === "End") next = tabs.length - 1;
  if (next === current) return;
  event.preventDefault();
  tabs[next].focus();
  selectResultsTab(tabs[next]);
});

loadResults();
