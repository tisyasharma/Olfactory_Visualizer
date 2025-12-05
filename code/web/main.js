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
  if(val === 'ipsi') return 'left';
  if(val === 'contra') return 'right';
  if(['left','right','bilateral'].includes(val)) return val;
  return null;
}
function mapLateralityToApi(val){
  return normalizeHemisphere(val) || 'bilateral';
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
const imageList = document.getElementById('imageList');
const registerBtn = document.getElementById('registerBtn');
const clearBtn = document.getElementById('clearBtn');
const uploadStatus = document.getElementById('uploadStatus');
const uploadWarning = document.getElementById('uploadWarning');
const uploadModality = document.getElementById('uploadModality');
const uploadMouse = document.getElementById('uploadMouse');
const uploadDate = document.getElementById('uploadDate');
const uploadProtocol = document.getElementById('uploadProtocol');
const imageQueue = [];
const pendingCsv = [];
const countsQueues = { bilateral: [], left: [], right: [] };
const IMAGE_EXT = ['.png','.jpg','.jpeg','.tif','.tiff','.ome.tif','.ome.tiff','.zarr','.ome.zarr'];
const CSV_EXT = ['.csv'];
const addImagesBtn = document.getElementById('addImagesBtn');
const addCsvBilateral = document.getElementById('addCsvBilateral');
const addCsvLeft = document.getElementById('addCsvLeft');
const addCsvRight = document.getElementById('addCsvRight');
const csvInputBilateral = document.getElementById('csvInputBilateral');
const csvInputLeft = document.getElementById('csvInputLeft');
const csvInputRight = document.getElementById('csvInputRight');
const csvList = document.getElementById('csvList');
const uploadSpinner = document.getElementById('uploadSpinner');

addImagesBtn?.addEventListener('click', () => fileInput?.click());
fileInput?.addEventListener('change', (e) => addFiles([...e.target.files], 'image'));
addCsvBilateral?.addEventListener('click', () => csvInputBilateral?.click());
addCsvLeft?.addEventListener('click', () => csvInputLeft?.click());
addCsvRight?.addEventListener('click', () => csvInputRight?.click());
csvInputBilateral?.addEventListener('change', (e) => addFiles([...e.target.files], 'bilateral'));
csvInputLeft?.addEventListener('change', (e) => addFiles([...e.target.files], 'left'));
csvInputRight?.addEventListener('change', (e) => addFiles([...e.target.files], 'right'));
updateReadyStates();

['dragenter','dragover'].forEach(evt => {
  dropzone?.addEventListener(evt, (e) => { e.preventDefault(); dropzone.classList.add('is-hover'); });
});
['dragleave','drop'].forEach(evt => {
  dropzone?.addEventListener(evt, (e) => { e.preventDefault(); dropzone.classList.remove('is-hover'); });
});
dropzone?.addEventListener('drop', (e) => {
  const files = [...e.dataTransfer.files];
  const images = files.filter(f => IMAGE_EXT.some(ext => (f.name || '').toLowerCase().endsWith(ext)));
  const csvs = files.filter(f => CSV_EXT.some(ext => (f.name || '').toLowerCase().endsWith(ext)));
  if(images.length){ addFiles(images, 'image'); }
  if(csvs.length){ addFiles(csvs, 'pending'); }
});

function addFiles(files, target){
const rejected = [];
files.forEach(f => {
  const name = (f.name || '').toLowerCase();
  const isCsv = CSV_EXT.some(ext => name.endsWith(ext));
  const isImage = IMAGE_EXT.some(ext => name.endsWith(ext));
  if(target === 'image' && !isImage){
    rejected.push(f.name || 'unknown');
    return;
  }
    if(target !== 'image' && !isCsv){
      rejected.push(f.name || 'unknown');
      return;
    }
  if(target === 'image'){
    if(!imageQueue.find(q => q.name === f.name && q.size === f.size)){
      imageQueue.push(f);
    }
    }else if(target && ['bilateral','left','right'].includes(target)){
      const bucket = countsQueues[target];
      if(!bucket.find(q => q.name === f.name && q.size === f.size)){
        bucket.push(f);
      }
    }else if(target === 'pending'){
      if(!pendingCsv.find(q => q.name === f.name && q.size === f.size)){
        pendingCsv.push(f);
      }
    }
  });
  renderFileLists();
  updateReadyStates();
  if(rejected.length){
    setWarning(`Rejected unsupported file types: ${rejected.join(', ')}`);
  }else{
    setWarning('');
  }
}

function renderFileLists(){
  if(imageList){
    if(imageQueue.length === 0){ imageList.innerHTML = ''; }
    else {
      imageList.innerHTML =
        imageQueue.map((f,idx) => `<div class="file-item" data-type="image" data-idx="${idx}"><span>${f.name}</span><span class="muted">${prettyBytes(f.size)}</span><button class="btn btn--mini remove-file" aria-label="Remove ${f.name}">×</button></div>`).join('');
    }
  }
  if(csvList){
    const rows = [];
    // pending with assign dropdown
    pendingCsv.forEach((f, idx) => {
      rows.push(`<div class="file-item" data-type="pending" data-idx="${idx}">
        <span>UNASSIGNED: ${f.name}</span>
        <span class="muted">${prettyBytes(f.size)}</span>
        <select class="csv-assign" data-idx="${idx}">
          <option value="">Assign hemisphere</option>
          <option value="bilateral">Bilateral</option>
          <option value="left">Ipsilateral</option>
          <option value="right">Contralateral</option>
        </select>
        <button class="btn btn--mini remove-file" aria-label="Remove ${f.name}">×</button>
      </div>`);
    });
    const pushRow = (label, bucket) => {
      bucket.forEach((f, idx) => {
        rows.push(`<div class="file-item" data-type="${label}" data-idx="${idx}"><span>${label.toUpperCase()}: ${f.name}</span><span class="muted">${prettyBytes(f.size)}</span><button class="btn btn--mini remove-file" aria-label="Remove ${f.name}">×</button></div>`);
      });
    };
    pushRow('bilateral', countsQueues.bilateral);
    pushRow('left', countsQueues.left);
    pushRow('right', countsQueues.right);
    if(rows.length === 0){ csvList.innerHTML = ''; }
    else {
      csvList.innerHTML = '<div class="file-list__title">Quantification CSVs</div>' + rows.join('');
    }
  }
}

document.addEventListener('click', (e) => {
  const t = e.target;
  if(t && t.classList && t.classList.contains('csv-assign')){
    const idx = +t.getAttribute('data-idx');
    const val = t.value;
    if(!isNaN(idx) && ['bilateral','left','right'].includes(val)){
      const f = pendingCsv[idx];
      if(f){
        pendingCsv.splice(idx, 1);
        countsQueues[val].push(f);
        renderFileLists();
        updateReadyStates();
      }
    }
    return;
  }
  if(!(t && t.classList && t.classList.contains('remove-file'))) return;
  const item = t.closest('.file-item');
  const idx = +item.getAttribute('data-idx');
  const type = item.getAttribute('data-type');
  if(isNaN(idx) || !type) return;
  if(type === 'image'){
    imageQueue.splice(idx, 1);
  }else if(['bilateral','left','right'].includes(type)){
    countsQueues[type].splice(idx, 1);
  }else if(type === 'pending'){
    pendingCsv.splice(idx, 1);
  }
  renderFileLists();
  updateReadyStates();
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
  hideSpinner();
  const modality = uploadModality?.value;
  const mouse = (uploadMouse?.value || '').trim();
  const dateVal = (uploadDate?.value || '').trim();
  if(!modality || !mouse || !dateVal){
    setWarning('Please choose a modality, provide a Mouse ID, and select a date.');
    return;
  }
  const hasImages = imageQueue.length > 0;
  const totalCsv = countsQueues.bilateral.length + countsQueues.left.length + countsQueues.right.length;
  if(pendingCsv.length > 0){
    setWarning('Assign all quantification CSVs to bilateral/ipsilateral/contralateral before submitting.');
    return;
  }
  if(!hasImages){
    setWarning('Please add microscopy images before submitting.');
    return;
  }
  if(countsQueues.bilateral.length === 0 || countsQueues.left.length === 0 || countsQueues.right.length === 0){
    setWarning('Add all three quantification files: bilateral, ipsilateral, and contralateral.');
    return;
  }
  const hemiVal = lateralitySelect?.value;
  const hemisphere = mapLateralityToApi(!hemiVal ? 'all' : hemiVal);
  const experimentType = modality === 'rabies' ? 'rabies' : 'double_injection';
  const sessionId = (uploadProtocol?.value && uploadProtocol.value.trim())
    ? uploadProtocol.value.trim().replace(/\s+/g, '-').toLowerCase()
    : 'auto';

  const csvFiles = {
    bilateral: countsQueues.bilateral.slice(),
    left: countsQueues.left.slice(),
    right: countsQueues.right.slice()
  };
  const imageFiles = imageQueue.slice();

  const confirmMsg = `You are about to ingest ${imageFiles.length} image(s) and ${totalCsv} CSV file(s) for ${mouse} (${sessionId}). This will write to the database. Continue?`;
  if(!window.confirm(confirmMsg)){
    return;
  }

  try{
    showSpinner('Uploading…');
    if(imageFiles.length){
      await uploadMicroscopy(modality, mouse, sessionId, hemisphere, imageFiles);
    }
    if(csvFiles.bilateral.length){
      await uploadRegionCounts(experimentType, mouse, sessionId, 'bilateral', csvFiles.bilateral);
    }
    if(csvFiles.left.length){
      await uploadRegionCounts(experimentType, mouse, sessionId, 'left', csvFiles.left);
    }
    if(csvFiles.right.length){
      await uploadRegionCounts(experimentType, mouse, sessionId, 'right', csvFiles.right);
    }
    setStatus(`Uploaded ${imageFiles.length} image(s) and ${totalCsv} CSV(s) for ${mouse} -> ${sessionId}`);
    resetUploadForm();
    loadSubjects();
    loadSamples();
    loadFiles();
    // Show a brief success indicator
    if(uploadSpinner){
      uploadSpinner.classList.add('spinner--success');
      const textEl = uploadSpinner.querySelector('.spinner__label');
      if(textEl){ textEl.textContent = 'Uploaded'; }
      const dot = uploadSpinner.querySelector('.spinner__dot');
      if(dot){ dot.style.animation = 'none'; }
    }
  }catch(err){
    console.error(err);
    setWarning('Upload failed: ' + err.message);
  }finally{
    hideSpinner();
  }
});

function setStatus(msg){
  if(uploadStatus){ uploadStatus.textContent = msg; uploadStatus.hidden = false; }
  if(uploadWarning){
    uploadWarning.hidden = true;
    uploadWarning.textContent = '';
    uploadWarning.classList.remove('note--error');
  }
}
function setWarning(msg){
  if(!msg){
    if(uploadWarning){
      uploadWarning.hidden = true;
      uploadWarning.textContent = '';
      uploadWarning.classList.remove('note--error');
    }
    return;
  }
  if(uploadWarning){
    uploadWarning.textContent = msg;
    uploadWarning.hidden = false;
    uploadWarning.classList.remove('note--ok');
    uploadWarning.classList.add('note--error');
    uploadWarning.style.display = 'block';
  }
  if(uploadStatus){ uploadStatus.hidden = true; }
}

function showSpinner(label){
  if(!uploadSpinner) return;
  uploadSpinner.classList.remove('spinner--success');
  const textEl = uploadSpinner.querySelector('.spinner__label');
  if(textEl){ textEl.textContent = label || 'Uploading…'; }
  uploadSpinner.hidden = false;
}
function hideSpinner(){
  if(uploadSpinner){
    uploadSpinner.hidden = true;
    const textEl = uploadSpinner.querySelector('.spinner__label');
    if(textEl){ textEl.textContent = 'Uploading…'; }
    uploadSpinner.classList.remove('spinner--success');
  }
}

function updateReadyStates(){
  const toggle = (el, ready) => {
    if(!el) return;
    el.classList.toggle('btn--ready', !!ready);
  };
  toggle(addImagesBtn, imageQueue.length > 0);
  toggle(addCsvBilateral, countsQueues.bilateral.length > 0);
  toggle(addCsvLeft, countsQueues.left.length > 0);
  toggle(addCsvRight, countsQueues.right.length > 0);
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
    const text = await res.text();
    let msg = text;
    try{ const parsed = JSON.parse(text); if(parsed?.detail){ msg = parsed.detail; } }catch(_){}
    if(res.status === 409){
      setWarning(`Upload failed: files already exist in the system. (${msg || 'Microscopy duplicate detected'})`);
      return { status:'skipped', message: msg };
    }
    throw new Error(msg || 'Upload failed');
  }
  const data = await res.json();
  setStatus(`Uploaded ${files.length} microscopy file(s) for ${subj} -> ${sessionId}`);
  return data;
}

function resetUploadForm(){
  imageQueue.splice(0, imageQueue.length);
  pendingCsv.splice(0, pendingCsv.length);
  countsQueues.bilateral.splice(0, countsQueues.bilateral.length);
  countsQueues.left.splice(0, countsQueues.left.length);
  countsQueues.right.splice(0, countsQueues.right.length);
  renderFileLists();
  updateReadyStates();
  [uploadModality, uploadMouse, uploadDate, uploadProtocol].forEach(el => {
    if(!el) return;
    if(el.tagName === 'SELECT'){ el.selectedIndex = 0; }
    else { el.value = ''; }
    el.classList.remove('has-value');
  });
  if(fileInput){ fileInput.value = ''; }
  if(csvInputBilateral){ csvInputBilateral.value = ''; }
  if(csvInputLeft){ csvInputLeft.value = ''; }
  if(csvInputRight){ csvInputRight.value = ''; }
  setWarning('');
  if(uploadStatus){ uploadStatus.hidden = true; uploadStatus.textContent = ''; }
  hideSpinner();
}

async function uploadRegionCounts(experimentType, mouse, sessionId, hemisphere, files){
  const form = new FormData();
  const subj = mouse.startsWith('sub-') ? mouse : `sub-${mouse}`;
  form.append('subject_id', subj);
  if(sessionId){ form.append('session_id', sessionId); }
  form.append('hemisphere', hemisphere || 'bilateral');
  form.append('experiment_type', experimentType);
  files.forEach(f => {
    let fname = f.name || 'counts.csv';
    if(hemisphere === 'left' && !fname.toLowerCase().startsWith('left_')){
      fname = `Left_${fname}`;
    }else if(hemisphere === 'right' && !fname.toLowerCase().startsWith('right_')){
      fname = `Right_${fname}`;
    }else if(hemisphere === 'bilateral' && !fname.toLowerCase().startsWith('bilateral_')){
      fname = `Bilateral_${fname}`;
    }
    form.append('files', f, fname);
  });

  const res = await fetch(`${API}/upload/region-counts`, {
    method: 'POST',
    body: form
  });
  if(!res.ok){
    const text = await res.text();
    let msg = text;
    try{ const parsed = JSON.parse(text); if(parsed?.detail){ msg = parsed.detail; } }catch(_){}
    if(res.status === 409){
      setWarning(`Upload failed: files already exist in the system. (${msg || 'Quantification duplicate detected'})`);
      return { status:'skipped', message: msg };
    }
    throw new Error(msg || 'Upload failed');
  }
  const data = await res.json();
  if((data.rows_ingested || 0) === 0){
    setWarning('No rows ingested (possible duplicate content).');
  }else{
    setStatus(`Ingested ${data.rows_ingested || 0} row(s) from ${files.length} CSV(s) for ${subj}`);
  }
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
