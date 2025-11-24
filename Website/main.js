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