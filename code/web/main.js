// Initialize AOS (scroll animations)
AOS.init({ once:true, duration:600, easing:'ease-out' });

// Back-to-top button visibility
const toTop = document.getElementById('toTop');
window.addEventListener('scroll', () => {
  toTop.style.display = window.scrollY > 600 ? 'block' : 'none';
});
toTop.addEventListener('click', () => window.scrollTo({ top:0, behavior:'smooth' }));

// Global filters (stub wiring for later API calls)
const dateRange = document.getElementById('dateRange');
const dateVal = document.getElementById('dateVal');
const genotypeSelect = document.getElementById('genotypeSelect');
const lateralitySelect = document.getElementById('lateralitySelect');
const mouseSelect = document.getElementById('mouseSelect');
const scrnaSampleSelect = document.getElementById('scrnaSampleSelect');
const scrnaClusterSelect = document.getElementById('scrnaClusterSelect');
const fileSelect = document.getElementById('fileSelect');
const fileDetails = document.getElementById('fileDetails');

dateRange?.addEventListener('input', () => { dateVal.textContent = dateRange.value; syncGlobalFilters(); });
genotypeSelect?.addEventListener('change', syncGlobalFilters);
lateralitySelect?.addEventListener('change', syncGlobalFilters);
mouseSelect?.addEventListener('change', syncGlobalFilters);

function syncGlobalFilters(){
  // charts removed for now; placeholder hook for future visuals
}

// Tabs logic
const tabs = [
  { btn: 'rabiesTabBtn', panel: 'rabiesTab' },
  { btn: 'doubleTabBtn', panel: 'doubleTab' },
  { btn: 'scrnaTabBtn', panel: 'scrnaTab' }
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

const API = 'http://localhost:8000';

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
function normalizeHemisphere(val){
  if(!val || val === 'all') return null;
  if(['left','right','bilateral'].includes(val)) return val;
  return null;
}

// Data fetchers for future charts (kept for reuse)
async function fetchFluorSummary(experimentType, hemisphere, subjectId, regionId){
  const qs = new URLSearchParams();
  if(experimentType) qs.append('experiment_type', experimentType);
  if(hemisphere) qs.append('hemisphere', hemisphere);
  if(subjectId && subjectId !== 'all') qs.append('subject_id', subjectId);
  if(regionId) qs.append('region_id', regionId);
  qs.append('limit', 200);
  return fetchJson(`${API}/fluor/summary?${qs.toString()}`);
}

async function updateRabiesCharts(params){
  const hemi = normalizeHemisphere(params?.laterality);
  try{
    const data = await fetchFluorSummary('rabies', hemi, params?.mouse);
    const values = data.map(d => ({
      region: d.region_name,
      load: d.load_avg ?? 0,
      pixels: d.region_pixels_avg ?? 0
    })).sort((a,b) => b.load - a.load).slice(0, 20);
    const spec = {
      data: { values },
      mark: { type:'bar', cornerRadiusEnd:3 },
      encoding: {
        x: { field:'load', type:'quantitative', title:'Avg load', axis:{grid:false} },
        y: { field:'region', type:'nominal', sort:'-x', title:'Region', axis:{labelLimit:180} },
        color: { field:'load', type:'quantitative', legend:null, scale:{scheme:'blues'} },
        tooltip: [
          {field:'region', type:'nominal'},
          {field:'load', type:'quantitative', title:'Avg load', format:'.4f'},
          {field:'pixels', type:'quantitative', title:'Avg pixels', format:'.0f'}
        ]
      }
    };
    embedVL('rabies_load_chart', spec);
  }catch(err){
    console.warn('Rabies chart failed', err);
    embedVL('rabies_load_chart', { data:{values:[]}, mark:'bar', encoding:{} });
  }
}

async function updateDoubleCharts(params){
  const hemi = normalizeHemisphere(params?.laterality);
  try{
    const data = await fetchFluorSummary('double_injection', hemi, params?.mouse);
    const values = data.map(d => ({
      region: d.region_name,
      pixels: d.region_pixels_avg ?? 0,
      load: d.load_avg ?? 0
    })).sort((a,b) => b.pixels - a.pixels).slice(0, 20);
    const spec = {
      data: { values },
      mark: 'bar',
      encoding: {
        x: { field:'pixels', type:'quantitative', title:'Avg pixels' },
        y: { field:'region', type:'nominal', sort:'-x', title:'Region' },
        color: { field:'pixels', type:'quantitative', legend:null }
      }
    };
    embedVL('double_compare', spec);
  }catch(err){
    console.warn('Double chart failed', err);
    embedVL('double_compare', { data:{values:[]}, mark:'bar', encoding:{} });
  }
}

async function updateRegionalCharts(params){
  // Placeholder for other datasets; keep empty for now
}

// Init
(function init(){
  // open first tab by default
  activateTab('rabiesTabBtn', 'rabiesTab');
  loadSubjects();
  loadSamples();
  loadFiles();
  syncGlobalFilters();
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
const IMAGE_EXT = ['.png','.jpg','.jpeg','.tif','.tiff','.ome.tif','.ome.tiff','.zarr','.ome.zarr'];
const CSV_EXT = ['.csv'];

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
  const rejected = [];
  files.forEach(f => {
    const name = (f.name || '').toLowerCase();
    const isCsv = CSV_EXT.some(ext => name.endsWith(ext));
    const isImage = IMAGE_EXT.some(ext => name.endsWith(ext));
    if(!(isCsv || isImage)){
      rejected.push(f.name || 'unknown');
      return;
    }
    if(!queue.find(q => q.name === f.name && q.size === f.size)){
      queue.push(f);
    }
  });
  renderFileList();
  if(rejected.length){
    setWarning(`Rejected unsupported file types: ${rejected.join(', ')}`);
  }else{
    setWarning('');
  }
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
  resetUploadForm();
});

function updateValueState(el){
  if(!el) return;
  if(el.value && el.value.trim() !== ''){
    el.classList.add('has-value');
  }else{
    el.classList.remove('has-value');
  }
}

[uploadModality, uploadDate].forEach(el => {
  el?.addEventListener('change', () => updateValueState(el));
  updateValueState(el);
});

registerBtn?.addEventListener('click', async () => {
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
  const hemiVal = lateralitySelect?.value;
  const hemisphere = (!hemiVal || hemiVal === 'all') ? 'bilateral' : hemiVal;
  const experimentType = modality === 'rabies' ? 'rabies' : 'double_injection';
  const sessionId = (uploadProtocol?.value && uploadProtocol.value.trim())
    ? uploadProtocol.value.trim().replace(/\s+/g, '-').toLowerCase()
    : `ses-${modality}`;

  const csvFiles = queue.filter(f => f.name.toLowerCase().endsWith('.csv'));
  const imageFiles = queue.filter(f => !f.name.toLowerCase().endsWith('.csv'));

  try{
    if(imageFiles.length){
      await uploadMicroscopy(modality, mouse, sessionId, hemisphere, imageFiles);
    }
    if(csvFiles.length){
      await uploadRegionCounts(experimentType, mouse, sessionId, hemisphere, csvFiles);
    }
    setStatus(`Uploaded ${imageFiles.length} image(s) and ${csvFiles.length} CSV(s) for ${mouse} -> ${sessionId}`);
    queue.splice(0, queue.length);
    renderFileList();
    loadSubjects();
    loadSamples();
    loadFiles();
  }catch(err){
    console.error(err);
    setWarning('Upload failed: ' + err.message);
  }
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

async function uploadMicroscopy(modality, mouse, sessionId, hemisphere, files){
  const form = new FormData();
  const subj = mouse.startsWith('sub-') ? mouse : `sub-${mouse}`;
  form.append('subject_id', subj);
  form.append('session_id', sessionId || `ses-${modality}`);
  form.append('hemisphere', hemisphere || 'bilateral');
  form.append('pixel_size_um', 0.5);
  form.append('experiment_type', modality === 'rabies' ? 'rabies' : 'double_injection');
  files.forEach(f => form.append('files', f, f.name));

  const res = await fetch(`${API}/upload/microscopy`, {
    method: 'POST',
    body: form
  });
  if(!res.ok){
    const t = await res.text();
    throw new Error(t || 'Upload failed');
  }
  const data = await res.json();
  setStatus(`Uploaded ${files.length} microscopy file(s) for ${subj} -> ${sessionId}`);
  return data;
}

function resetUploadForm(){
  queue.splice(0, queue.length);
  renderFileList();
  [uploadModality, uploadMouse, uploadDate, uploadProtocol].forEach(el => {
    if(!el) return;
    if(el.tagName === 'SELECT'){ el.selectedIndex = 0; }
    else { el.value = ''; }
    el.classList.remove('has-value');
  });
  if(fileInput){ fileInput.value = ''; }
  setWarning('');
  if(uploadStatus){ uploadStatus.hidden = true; uploadStatus.textContent = ''; }
}

async function uploadRegionCounts(experimentType, mouse, sessionId, hemisphere, files){
  const form = new FormData();
  const subj = mouse.startsWith('sub-') ? mouse : `sub-${mouse}`;
  form.append('subject_id', subj);
  if(sessionId){ form.append('session_id', sessionId); }
  form.append('hemisphere', hemisphere || 'bilateral');
  form.append('experiment_type', experimentType);
  files.forEach(f => form.append('files', f, f.name));

  const res = await fetch(`${API}/upload/region-counts`, {
    method: 'POST',
    body: form
  });
  if(!res.ok){
    const t = await res.text();
    throw new Error(t || 'Upload failed');
  }
  const data = await res.json();
  setStatus(`Ingested ${data.rows_ingested || 0} row(s) from ${files.length} CSV(s) for ${subj}`);
  return data;
}

async function fetchJson(url){
  const res = await fetch(url);
  if(!res.ok) throw new Error(await res.text());
  return res.json();
}

async function loadSubjects(){
  try{
    const subjects = await fetchJson(`${API}/subjects`);
    if(mouseSelect){
      mouseSelect.innerHTML = '<option value="all" selected>All</option>' +
        subjects.map(s => `<option value="${s.subject_id}">${s.subject_id}</option>`).join('');
    }
    if(uploadMouse && uploadMouse.tagName === 'SELECT'){
      uploadMouse.innerHTML = '<option value="" disabled selected>Select mouse</option>' +
        subjects.map(s => `<option value="${s.subject_id}">${s.subject_id}</option>`).join('');
    }
  }catch(err){
    console.warn('Failed to load subjects', err);
  }
}

async function loadSamples(){
  try{
    const samples = await fetchJson(`${API}/scrna/samples`);
    if(scrnaSampleSelect){
      scrnaSampleSelect.innerHTML = '<option value="" disabled selected>Select sample</option>' +
        samples.map(s => `<option value="${s.sample_id}">${s.sample_id}</option>`).join('');
    }
  }catch(err){
    console.warn('Failed to load scRNA samples', err);
  }
}

async function loadFiles(){
  try{
    const files = await fetchJson(`${API}/files`);
    if(fileSelect){
      fileSelect.innerHTML = '<option value="" disabled selected>Select file</option>' +
        files.map(f => {
          const labelParts = [f.subject_id || '', f.session_id || '', f.hemisphere || '', f.run ? `run-${f.run}` : ''].filter(Boolean);
          const label = labelParts.join(' • ') || f.path;
          return `<option value="${encodeURIComponent(f.path)}" data-path="${encodeURIComponent(f.path)}" data-session="${f.session_id || ''}" data-subject="${f.subject_id || ''}" data-hemisphere="${f.hemisphere || ''}">${label}</option>`;
        }).join('');
      if(files.length && fileSelect.options.length > 1){
        fileSelect.selectedIndex = 1;
        renderFileDetails(fileSelect.options[fileSelect.selectedIndex]);
      }
    }
  }catch(err){
    console.warn('Failed to load files', err);
  }
}

function renderFileDetails(option){
  if(!option || !fileDetails) return;
  const path = decodeURIComponent(option.getAttribute('data-path') || '');
  const sess = option.getAttribute('data-session') || '';
  const subj = option.getAttribute('data-subject') || '';
  const hemi = option.getAttribute('data-hemisphere') || '';
  fileDetails.innerHTML = `<strong>Path:</strong> ${path}<br><strong>Subject:</strong> ${subj}<br><strong>Session:</strong> ${sess}<br><strong>Hemisphere:</strong> ${hemi || 'bilateral'}`;
  fileDetails.hidden = false;
}

fileSelect?.addEventListener('change', (e) => {
  renderFileDetails(e.target.options[e.target.selectedIndex]);
});

scrnaSampleSelect?.addEventListener('change', async () => {
  const sample = scrnaSampleSelect.value;
  try{
    const clusters = await fetchJson(`${API}/scrna/clusters?sample_id=${encodeURIComponent(sample)}`);
    if(scrnaClusterSelect){
      scrnaClusterSelect.innerHTML = '<option value="" disabled selected>Select cluster</option>' +
        clusters.map(c => `<option value="${c.cluster_id}">${c.cluster_id} (${c.n_cells || 0} cells)</option>`).join('');
    }
    updateScrnaBar(clusters);
    updateScrnaHeatmap(sample, scrnaClusterSelect.value || null);
  }catch(err){
    console.warn('Failed to load clusters', err);
  }
});

scrnaClusterSelect?.addEventListener('change', () => {
  const sample = scrnaSampleSelect.value;
  const cluster = scrnaClusterSelect.value;
  updateScrnaHeatmap(sample, cluster);
});

function updateScrnaBar(clusters){
  if(!clusters || !clusters.length){
    embedVL('scrna_bar', { data:{values:[]}, mark:'bar', encoding:{} });
    return;
  }
  const values = clusters.map(c => ({ cluster: c.cluster_id, cells: c.n_cells || 0 }));
  const spec = {
    data: { values },
    mark: 'bar',
    encoding: {
      x: { field: 'cluster', type: 'nominal', sort: null, title: 'Cluster' },
      y: { field: 'cells', type: 'quantitative', title: 'Cells' },
      color: { field: 'cluster', type: 'nominal', legend: null }
    }
  };
  embedVL('scrna_bar', spec);
}

async function updateScrnaHeatmap(sample, cluster){
  if(!sample || !cluster){
    embedVL('scrna_heatmap', { data:{values:[]}, mark:'rect', encoding:{} });
    return;
  }
  try{
    const markers = await fetchJson(`${API}/scrna/markers?sample_id=${encodeURIComponent(sample)}&cluster_id=${encodeURIComponent(cluster)}`);
    const spec = {
      data: { values: markers },
      mark: 'rect',
      encoding: {
        x: { field: 'gene', type: 'nominal', sort: null, title: 'Gene' },
        y: { field: 'cluster_id', type: 'nominal', title: 'Cluster' },
        color: { field: 'logfc', type: 'quantitative', title: 'logFC' }
      }
    };
    embedVL('scrna_heatmap', spec);
  }catch(err){
    console.warn('Failed to load markers', err);
    embedVL('scrna_heatmap', { data:{values:[]}, mark:'rect', encoding:{} });
  }
}
