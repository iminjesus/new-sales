// ===== Constants & initial state =====
const COLORS = ["#1f77b4","#ff7f0e","#2ca02c","#d62728","#9467bd","#8c564b",
                "#e377c2","#7f7f7f","#bcbd22","#17becf","#aec7e8","#ffbb78"];
const REGION_SALESMEN = {
  NSW:["Hamid Jallis","LUTTRELL STEVE","Hulley Gary","Lee Don"],
  QLD:["Lopez Randall","Spires Steven","Sampson Kieren","Marsh Aaron"],
  VIC:["Bellotto Nicola","Bilston Kelley","Gultjaeff Jason","Hobkirk Calvin"],
  WA:["Fruci Davide","Gilbert Michael"]
};

const filters = {
  group_by: "product_group",
  region: "ALL",
  salesman: "ALL",
  sold_to_group: "ALL",
  sold_to: "ALL",
  product_group: "ALL"
};

// Chart instances (so we can destroy before redraw)
let dailyInst, cumInst, sdInst, sdpInst, scInst, scpInst;

// ===== Utilities =====
const $ = (sel) => document.querySelector(sel);
const fetchJSON = (url) => fetch(url).then(r => r.json());

function setActiveButtons(container, value) {
  container.querySelectorAll('.btn').forEach(btn => {
    btn.classList.toggle('active', btn.dataset.val === value);
  });
}

function makeButton(label, value) {
  const b = document.createElement('button');
  b.className = 'btn';
  b.textContent = label; b.dataset.val = value;
  return b;
}

function populateSelect(el, items, includeAll=true) {
  el.innerHTML = '';
  if (includeAll) {
    const all = document.createElement('option'); all.value='ALL'; all.textContent='ALL'; el.appendChild(all);
  }
  items.forEach(v=>{ const o=document.createElement('option'); o.value=v; o.textContent=v; el.appendChild(o); });
}

function makeStackedBar(ctx, labels, datasets, yTitle="", yMax=null) {
  return new Chart(ctx, {
    type: 'bar',
    data: { labels, datasets },
    options: {
      responsive: true,
      plugins: { legend: { position: 'top' } },
      scales: {
        x: { stacked: true },
        y: { stacked: true, beginAtZero: true, max: yMax ?? undefined,
             title: { display: !!yTitle, text: yTitle } }
      }
    }
  });
}

// ===== UI setup =====
function buildButtons() {
  // GroupBy
  const gb = $('#groupByButtons');
  [
    ['Product Group','product_group'],
    ['Region','region'],
    ['Salesman','salesman'],
    ['Sold-to Group','sold_to_group'],
    ['Sold-to','sold_to']
  ].forEach(([label,val])=> gb.appendChild(makeButton(label,val)));
  gb.addEventListener('click', e=>{
    if(!e.target.classList.contains('btn')) return;
    filters.group_by = e.target.dataset.val;
    setActiveButtons(gb, filters.group_by);
  });
  setActiveButtons(gb, filters.group_by);

  // Region
  const rb = $('#regionButtons');
  ['ALL','NSW','QLD','VIC','WA'].forEach(r=> rb.appendChild(makeButton(r,r)));
  rb.addEventListener('click', e=>{
    if(!e.target.classList.contains('btn')) return;
    filters.region = e.target.dataset.val;
    setActiveButtons(rb, filters.region);
    // refresh salesman list
    const all = Object.values(REGION_SALESMEN).flat();
    const list = filters.region==='ALL'? all : (REGION_SALESMEN[filters.region]||[]);
    populateSelect($('#salesman_name'), [...new Set(list)].sort());
  });
  setActiveButtons(rb, filters.region);
}

async function initDropdowns() {
  // Salesman (all initially)
  const allSales = Object.values(REGION_SALESMEN).flat();
  populateSelect($('#salesman_name'), [...new Set(allSales)].sort());

  // Product groups
  const groups = await fetchJSON('/api/product_group');
  populateSelect($('#product_group'), groups);

  // Sold-to names (start with ALL group)
  populateSelect($('#sold_to_group'), ['ALL'], false); // keep simple; you can fill with real groups if needed
  $('#sold_to_group').value = 'ALL';
  const names = await fetchJSON('/api/sold_to_names?sold_to_group=ALL');
  populateSelect($('#sold_to'), names);

  // Cascade sold_to when sold_to_group changes
  $('#sold_to_group').addEventListener('change', async ()=>{
    const group = $('#sold_to_group').value;
    const names2 = await fetchJSON(`/api/sold_to_names?sold_to_group=${group}`);
    populateSelect($('#sold_to'), names2);
  });
}

// ===== Data → Charts =====
async function drawAllCharts(){
  // capture current dropdown values
  filters.salesman = $('#salesman_name').value || 'ALL';
  filters.sold_to_group = $('#sold_to_group').value || 'ALL';
  filters.sold_to = $('#sold_to').value || 'ALL';
  filters.product_group = $('#product_group').value || 'ALL';

  const qs = new URLSearchParams(filters).toString();
  const data = await fetchJSON(`/api/sku_trend?${qs}`);

  const dates = [...new Set(data.map(d=>d.billing_date))].sort();
  const keys  = [...new Set(data.map(d=>d.group_label))];

  const daily={}, pct={}, cum={}, cumpct={};
  keys.forEach(k=>{ daily[k]=Array(dates.length).fill(0); pct[k]=Array(dates.length).fill(0); cum[k]=Array(dates.length).fill(0); cumpct[k]=Array(dates.length).fill(0); });

  data.forEach(d=>{
    const i = dates.indexOf(d.billing_date), k=d.group_label;
    daily[k][i] = Number(d.daily_qty)||0;
    pct[k][i]   = Number(d.percentage)||0;
  });

  keys.forEach(k=>{ let r=0; for(let i=0;i<dates.length;i++){ r+=daily[k][i]; cum[k][i]=r; }});
  for(let i=0;i<dates.length;i++){
    const total = keys.reduce((a,k)=>a+cum[k][i],0)||1;
    keys.forEach(k=>{ cumpct[k][i] = +(cum[k][i]/total*100).toFixed(2); });
  }

  const makeDS = (map)=> keys.map((k,i)=>({ label:k, data: map[k], backgroundColor: COLORS[i%COLORS.length], stack:'S' }));

  const dailyTotal = dates.map((_,i)=> keys.reduce((a,k)=>a+daily[k][i],0));
  const cumTotal = dailyTotal.reduce((acc,v)=>{ acc.push((acc.at(-1)||0)+v); return acc; }, []);

  [dailyInst, cumInst, sdInst, sdpInst, scInst, scpInst].forEach(c=>c&&c.destroy());

  dailyInst = new Chart($('#dailyChart'), {
    type:'bar',
    data:{ labels:dates, datasets:[{ label:'Daily SKU (ALL)', data: dailyTotal, backgroundColor:'#a78bfa' }]},
    options:{ responsive:true, plugins:{ legend:{ position:'top'}}, scales:{ y:{ beginAtZero:true }}}
  });

  cumInst = new Chart($('#cumulativeChart'), {
    type:'bar',
    data:{ labels:dates, datasets:[{ label:'Cumulative SKU (ALL)', data: cumTotal, backgroundColor:'#0ea5a3' }]},
    options:{ responsive:true, plugins:{ legend:{ position:'top'}}, scales:{ y:{ beginAtZero:true }}}
  });

  sdInst  = makeStackedBar($('#stackedDaily'),    dates, makeDS(daily),  'Daily QTY');
  sdpInst = makeStackedBar($('#stackedDailyPct'), dates, makeDS(pct),    'Daily %', 100);
  scInst  = makeStackedBar($('#stackedCum'),      dates, makeDS(cum),    'Cumulative QTY');
  scpInst = makeStackedBar($('#stackedCumPct'),   dates, makeDS(cumpct), 'Cumulative %', 100);
}

// ===== Boot =====
document.getElementById('apply-btn').addEventListener('click', drawAllCharts);

(async function start(){
  buildButtons();
  await initDropdowns();
  await drawAllCharts();
})();
