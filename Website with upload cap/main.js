// Initialize AOS (scroll animations)
AOS.init({ once:true, duration:600, easing:'ease-out' });

// Back-to-top button visibility
const toTop = document.getElementById('toTop');
window.addEventListener('scroll', () => {
  toTop.style.display = window.scrollY > 600 ? 'block' : 'none';
});
toTop.addEventListener('click', () => window.scrollTo({ top:0, behavior:'smooth' }));

// Theme toggle
const themeToggle = document.getElementById('themeToggle');
let dark = false;
themeToggle.addEventListener('click', () => {
  dark = !dark;
  document.documentElement.classList.toggle('dark', dark);
  themeToggle.textContent = dark ? 'Light' : 'Dark';
});

// Global filters (stub wiring for later API calls)
const dateRange = document.getElementById('dateRange');
const dateVal = document.getElementById('dateVal');
const genotypeSelect = document.getElementById('genotypeSelect');
const lateralitySelect = document.getElementById('lateralitySelect');
const mouseSelect = document.getElementById('mouseSelect');

dateRange?.addEventListener('input', () => { dateVal.textContent = dateRange.value; syncGlobalFilters(); });
genotypeSelect?.addEventListener('change', syncGlobalFilters);
lateralitySelect?.addEventListener('change', syncGlobalFilters);
mouseSelect?.addEventListener('change', syncGlobalFilters);

function syncGlobalFilters(){
  const params = {
    maxYear: +dateRange.value,
    genotype: genotypeSelect.value,
    laterality: lateralitySelect.value,
    mouse: mouseSelect.value
  };
  // TODO: forward to per-tab updaters once charts are attached
  // e.g., updateRabiesCharts(params); updateDoubleCharts(params); updateRegionalCharts(params);
}

// Tabs logic
const tabs = [
  { btn: 'rabiesTabBtn', panel: 'rabiesTab' },
  { btn: 'doubleTabBtn', panel: 'doubleTab' },
  { btn: 'regionalTabBtn', panel: 'regionalTab' }
];

tabs.forEach(({btn, panel}) => {
  const b = document.getElementById(btn);
  const p = document.getElementById(panel);
  b?.addEventListener('click', () => activateTab(btn, panel));
});

function activateTab(activeBtnId, activePanelId){
  // buttons
  document.querySelectorAll('.tab').forEach(t => {
    const isActive = t.id === activeBtnId;
    t.classList.toggle('is-active', isActive);
    t.setAttribute('aria-selected', isActive ? 'true' : 'false');
  });
  // panels
  document.querySelectorAll('.tabpanel').forEach(p => {
    const isActive = p.id === activePanelId;
    p.classList.toggle('is-active', isActive);
    if(isActive){ p.removeAttribute('hidden'); } else { p.setAttribute('hidden', ''); }
  });
}

// Per-tab stub controls
const rabiesWindow = document.getElementById('rabiesWindow');
const rabiesWindowVal = document.getElementById('rabiesWindowVal');
rabiesWindow?.addEventListener('input', () => {
  rabiesWindowVal.textContent = rabiesWindow.value;
  // TODO: update rabies charts
});

// Vega-Lite theme (kept for future charts)
const accent1 = getComputedStyle(document.documentElement).getPropertyValue('--accent1').trim();
const accent2 = getComputedStyle(document.documentElement).getPropertyValue('--accent2').trim();
const accent3 = getComputedStyle(document.documentElement).getPropertyValue('--accent3').trim();

function vlTheme(){
  return {
    background: 'transparent',
    title: { color: '#111827', font: 'Inter', fontSize: 16, fontWeight: 600 },
    axis: { labelColor: '#374151', titleColor: '#374151', gridColor: '#e5e7eb' },
    legend: { labelColor: '#374151', titleColor: '#374151' },
    range: { category: [accent2, accent1, accent3, '#7c8aa6', '#aab6cf'] }
  };
}

async function embedVL(targetId, spec){
  const specFinal = { $schema:'https://vega.github.io/schema/vega-lite/v5.json', ...spec, config: vlTheme() };
  return vegaEmbed('#' + targetId, specFinal, { actions:false, renderer:'canvas' });
}

// Placeholder updaters (no data yet)
async function updateRabiesCharts(params){ /* TODO */ }
async function updateDoubleCharts(params){ /* TODO */ }
async function updateRegionalCharts(params){ /* TODO */ }

// Init
(function init(){
  // open first tab by default
  activateTab('rabiesTabBtn', 'rabiesTab');
})();

/* === Upload Center Logic === */
const dropzone = document.getElementById('dropzone');
const fileInput = document.getElementById('fileInput');
const browseLink = document.getElementById('browseLink');
const fileList = document.getElementById('fileList');
const registerBtn = document.getElementById('registerBtn');
const clearBtn = document.getElementById('clearBtn');
const uploadStatus = document.getElementById('uploadStatus');
const uploadWarning = document.getElementById('uploadWarning');
const uploadModality = document.getElementById('uploadModality');
const uploadMouse = document.getElementById('uploadMouse');
const uploadDate = document.getElementById('uploadDate');
const uploadProtocol = document.getElementById('uploadProtocol');
const queue = [];

browseLink?.addEventListener('click', () => fileInput?.click());
fileInput?.addEventListener('change', (e) => addFiles([...e.target.files]));

['dragenter','dragover'].forEach(evt => {
  dropzone?.addEventListener(evt, (e) => { e.preventDefault(); dropzone.classList.add('is-hover'); });
});
['dragleave','drop'].forEach(evt => {
  dropzone?.addEventListener(evt, (e) => { e.preventDefault(); dropzone.classList.remove('is-hover'); });
});
dropzone?.addEventListener('drop', (e) => {
  const files = [...e.dataTransfer.files];
  addFiles(files);
});

function addFiles(files){
  files.forEach(f => {
    if(!queue.find(q => q.name === f.name && q.size === f.size)){
      queue.push(f);
    }
  });
  renderFileList();
}

function renderFileList(){
  if(!fileList) return;
  if(queue.length === 0){ fileList.innerHTML = ''; return; }
  fileList.innerHTML = '<div class="file-list__title">Files to register</div>' +
    queue.map((f,idx) => `<div class="file-item" data-idx="${idx}"><span>${f.name}</span><span class="muted">${prettyBytes(f.size)}</span><button class="btn btn--mini remove-file" aria-label="Remove ${f.name}">×</button></div>`).join('');
}

fileList?.addEventListener('click', (e) => {
  const t = e.target;
  if(t && t.classList && t.classList.contains('remove-file')){
    const item = t.closest('.file-item');
    const idx = +item.getAttribute('data-idx');
    if(!isNaN(idx)){
      queue.splice(idx, 1);
      renderFileList();
    }
  }
});

clearBtn?.addEventListener('click', () => {
  queue.splice(0, queue.length);
  renderFileList();
  setWarning('');
});

registerBtn?.addEventListener('click', () => {
  const modality = uploadModality?.value;
  const mouse = (uploadMouse?.value || '').trim();
  if(!modality || !mouse){
    setWarning('Please choose a modality and provide a Mouse ID.');
    return;
  }
  if(queue.length === 0){
    setWarning('Add at least one file for this mouse experiment.');
    return;
  }
  registerExperiment(modality, mouse, uploadDate?.value || null, uploadProtocol?.value || null, queue).catch(err => {
    console.error(err);
    setWarning('Could not reach backend API at http://localhost:8000. Is it running?');
  });
});

function setStatus(msg){
  if(uploadStatus){ uploadStatus.textContent = msg; uploadStatus.hidden = false; }
  if(uploadWarning){ uploadWarning.hidden = true; }
}
function setWarning(msg){
  if(!msg){ uploadWarning.hidden = true; return; }
  if(uploadWarning){ uploadWarning.textContent = msg; uploadWarning.hidden = false; }
  if(uploadStatus){ uploadStatus.hidden = true; }
}

function prettyBytes(bytes){
  if(bytes < 1024) return bytes + ' B';
  const units = ['KB','MB','GB','TB'];
  let u = -1;
  do { bytes /= 1024; ++u; } while(bytes >= 1024 && u < units.length - 1);
  return bytes.toFixed(1) + ' ' + units[u];
}

// Calls your FastAPI backend (same shape as earlier)
async function registerExperiment(modality, mouse, date_run, protocol, files){
  const API = 'http://localhost:8000';
  const payload = { modality, mouse_id: mouse, date_run: date_run || null, protocol: protocol || null };
  const expRes = await fetch(API + '/experiments', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload)
  });
  if(!expRes.ok){
    const t = await expRes.text();
    throw new Error('Experiment create failed: ' + t);
  }
  const exp = await expRes.json();
  if(files && files.length){
    const form = new FormData();
    files.forEach(f => form.append('files', f, f.name));
    const uploadRes = await fetch(API + `/experiments/${exp.exp_id}/files`, {
      method: 'POST',
      body: form
    });
    if(!uploadRes.ok){
      const t = await uploadRes.text();
      throw new Error('File upload failed: ' + t);
    }
    const saved = await uploadRes.json();
    setStatus(`Registered ${saved.length} file(s) for ${mouse} (${modality}).`);
  }else{
    setStatus(`Registered 0 files for ${mouse} (${modality}).`);
  }
  queue.splice(0, queue.length);
  renderFileList();
}
