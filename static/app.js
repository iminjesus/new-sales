// --- fast defaults ---

Chart.defaults.animation = false;               // no animations
Chart.defaults.responsiveAnimationDuration = 0;
Chart.defaults.normalized = true;               // faster parsing
Chart.defaults.elements.bar.borderWidth = 0;

// turn value labels OFF (they’re expensive to draw)
const SHOW_BAR_VALUES = false;
if (SHOW_BAR_VALUES && !Chart.registry.plugins.get("showDataValues")) {
  Chart.register(showDataValuesPlugin);
}

/* -------------------------- state & helpers -------------------------- */
const COLORS=["#1f77b4","#ff7f0e","#2ca02c","#d62728","#9467bd","#8c564b","#e377c2","#7f7f7f","#bcbd22","#17becf","#aec7e8","#ffbb78"];
const REGION_SALESMEN={
  NSW:["Hamid Jallis","LUTTRELL STEVE","Hulley Gary","Lee Don"],
  QLD:["Lopez Randall","Spires Steven","Sampson Kieren","Marsh Aaron"],
  VIC:["Bellotto Nicola","Bilston Kelley","Gultjaeff Jason","Hobkirk Calvin"],
  WA:["Fruci Davide","Gilbert Michael"]
};
const fmt = (n) => (+n || 0).toLocaleString();

// Normalise names so monthly & daily labels match exactly
const norm = (s) => (s ?? "")
  .toString()
  .replace(/\s+/g, " ")
  .trim()
  .toUpperCase();

const filters={
  metric:"qty",
  group_by:"region",
  region:"ALL",
  salesman:"ALL",
  sold_to_group:"ALL",
  sold_to:"ALL",
  ship_to:"ALL",          
  product_group:"ALL",
  pattern:"ALL",          
  category:"ALL"
};

let dailyInst,dailyCumInst,monthlyInst,monthlyCumInst,yearlyInst,
    stackedDailyInst,stackedDailyCumInst, stackedDailyPctInst, stackedDailyCumPctInst, stackedYearlyInst, stackedYearlyPctInst,
    stackedMonthlyInst, stackedMonthlyCumInst, stackedMonthlyPctInst, stackedMonthlyCumPctInst;

const $=s=>document.querySelector(s);
function showError(msg){
  const el = document.getElementById('errbar');
  if (!el) return;
  el.textContent = msg;
  el.hidden = false;
}
const fetchJSON = async (u) => {
  try {
    const r = await fetch(u, {credentials:'same-origin'});
    if(!r.ok) throw new Error(`${r.status} ${r.statusText}`);
    return await r.json();
  } catch (e) {
    console.error('Fetch fail:', u, e);
    showError(`Failed: ${u} — ${e.message}`);
    return [];
  }
};
const setActive=(wrap,attr,val)=>[...wrap.querySelectorAll(".btn")].forEach(b=>b.classList.toggle("active",b.dataset[attr]===val));
function populateSelect(el,arr,includeAll=true){ el.innerHTML=""; if(includeAll){const o=document.createElement("option");o.value="ALL";o.textContent="ALL";el.appendChild(o);} arr.forEach(v=>{const o=document.createElement("option");o.value=v;o.textContent=v;el.appendChild(o);}); }
function makeStacked(id,labels,datasets,title,max){ return new Chart(document.getElementById(id),{type:"bar",data:{labels,datasets},options:getCommonOptions(true, max, title)}); }
const monthsLabels=()=>["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"];
const daysLabels=()=>[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31];
const yearsLabels=()=>[2021,2022,2023,2024];
function toCumulative(arr){const out=[];let run=0;for(const v of arr){run+=(+v||0);out.push(run);}return out;}
function cumPerGroup(map){ const out={}; for(const k in map){out[k]=toCumulative(map[k]);} return out;}

// The API already returns r.group_label for any grouping.
// Keep labels exactly as the API gives them.
const labelForRow = (r) => r.group_label || 'UNKNOWN';

/* -------- Top 10 Sold-to state & helpers -------- */
let TOP_MODE = 'all';     // 'all' | 'top10'
let TOP_SET = null;       // Set of *normalized* Sold-to names
let TOP_SET_KEY = '';

function keyForTopSet(){
  return JSON.stringify({
    metric:        filters.metric,
    category:      filters.category,
    region:        filters.region,
    salesman:      filters.salesman,
    sold_to_group: filters.sold_to_group,
    sold_to:       filters.sold_to,   
    ship_to:       filters.ship_to,   
    product_group: filters.product_group,
    pattern:       filters.pattern    
  });
}

function populateDatalist(listId, items){
  const list = document.getElementById(listId);
  list.innerHTML = '';
  (items||[]).forEach(v=>{
    const o = document.createElement('option');
    o.value = v; list.appendChild(o);
  });
}

// Try backend endpoint first; if missing, fall back to computing from monthly breakdown
async function fetchTopSet2025(){
  const qs = new URLSearchParams({
    metric:        filters.metric,
    category:      filters.category,
    region:        filters.region,
    salesman:      filters.salesman,
    sold_to_group: filters.sold_to_group,
    product_group: filters.product_group,
    n: 10
  }).toString();

  // 1) Backend route (if implemented)
  const rows = await fetchJSON(`/api/top_customers_2025?${qs}`);
  if (Array.isArray(rows) && rows.length) {
    return new Set(rows.map(r => norm(r.sold_to_name)));
  }

  // 2) Fallback: compute from monthly breakdown grouped by sold_to
  const backupQs = new URLSearchParams({
    metric:        filters.metric,
    category:      filters.category,
    region:        filters.region,
    salesman:      filters.salesman,
    sold_to_group: filters.sold_to_group,
    sold_to:       'ALL',
    product_group: filters.product_group,
    group_by:      'sold_to'
  }).toString();

  const monthlyRows = await fetchJSON(`/api/monthly_reakdown?${backupQs}`);
  const sumBySoldTo = new Map(); // normalized name -> total
  monthlyRows.forEach(r => {
    const k = norm(r.group_label || 'UNKNOWN');
    sumBySoldTo.set(k, (sumBySoldTo.get(k) || 0) + (+r.value || 0));
  });

  const topNorm = [...sumBySoldTo.entries()]
    .sort((a,b)=> b[1]-a[1])
    .slice(0,10)
    .map(([k]) => k);

  return new Set(topNorm);
}

async function ensureTopSet(){
  const k = keyForTopSet();
  if (TOP_SET && TOP_SET_KEY === k) return TOP_SET;
  TOP_SET = await fetchTopSet2025();
  TOP_SET_KEY = k;
  return TOP_SET;
}

/*
 * Reduce to Top-10 + “Other” smartly:
 * 1) Try TOP_SET intersection with current groups (normalized).
 * 2) If intersection is empty, fall back to top 10 present in the current map (by totals).
 */
function reduceToTopSmart(groups, map, topSet){
  if (!groups || !groups.length) return { groups, map };

  const firstKey = groups.find(g => Array.isArray(map[g]));
  if (!firstKey) return { groups, map };
  const len = map[firstKey].length;

  // Totals per group for fallback
  const totals = new Map(groups.map(g => [g, (map[g]||[]).reduce((a,b)=>a + (+b||0), 0)]));

  // Preferred keep set: intersection with TOP_SET (by normalized name)
  let keep = [];
  if (topSet && topSet.size) keep = groups.filter(g => topSet.has(norm(g)));

  // If no intersection, use top 10 by totals present in this dataset
  if (keep.length === 0) {
    keep = [...totals.entries()]
      .sort((a,b)=> b[1]-a[1])
      .slice(0,10)
      .map(([g])=>g);
  }

  const newMap = {};
  const other = Array(len).fill(0);

  groups.forEach(g=>{
    if (keep.includes(g)) {
      newMap[g] = map[g] || Array(len).fill(0);
    } else {
      const arr = map[g] || [];
      for (let i=0;i<len;i++) other[i] += (+arr[i]||0);
    }
  });

  const outGroups = [...keep];
  if (other.some(v=>v!==0)) {
    newMap['Other'] = other;
    outGroups.push('Other');
  }

  return { groups: outGroups, map: newMap };
}

/* ---------- Common options with dd-mm x labels & tooltip title ---------- */
function xAxisDdMm(stacked=false){
  return {
    stacked,
    grid: { color: "rgba(0,0,0,0.05)" },
    ticks: {
      maxRotation: 0,
      autoSkip: true,
      callback: function(value){
        const full = this.getLabelForValue(value);
        return typeof full === 'string' ? full.slice(0,2) : full; // dd
      }
    }
  };
}
// Plugin to display actual data values centered inside bars
const showDataValuesPlugin = {
  id: "showDataValues",
  afterDatasetsDraw(chart, _args, opts) {
    const { ctx } = chart;
    ctx.save();
    const color = opts?.color || "#111";
    const font  = opts?.font  || "10px Arial";

    chart.data.datasets.forEach((ds, i) => {
      const meta = chart.getDatasetMeta(i);
      if (!meta || meta.hidden) return;
      if (meta.type !== "bar") return;

      meta.data.forEach((elem, idx) => {
        if (!elem) return;
        const raw = ds.data[idx];
        const val = Number(raw);
        if (raw == null || !Number.isFinite(val) || val === 0) return; // ← skip 0s

        const props = elem.getProps(["x", "y", "base"], true);
        const centerY = props.y + (props.base - props.y) / 2;
        ctx.fillStyle = color;
        ctx.font = font;
        ctx.textAlign = "center";
        ctx.textBaseline = "middle";
        const text = typeof val === "number" ? val.toLocaleString() : String(val);
        ctx.fillText(text, props.x, centerY);
      });
    });
    ctx.restore();
  }
};

function getCommonOptions(stacked=false, yMax, yTitle){
  return {
    responsive:true,
    plugins:{
      legend:{position:"right"},
      tooltip:{
        callbacks:{
          title: function(items){
            const lbl = items && items[0] ? items[0].label : '';
            return typeof lbl === 'string' ? lbl.slice(0,5) : lbl; // dd-mm in tooltip title
          }
        }
      }
    },
    scales:{
      x: xAxisDdMm(stacked),
      y:{ beginAtZero:true, max: yMax ?? undefined, title:{ display: !!yTitle, text: yTitle } }
    }
  };
}
// Collect the exact params your APIs expect
function getFilterParams() {
  // If you already have a global getFilterParams(), keep using it.
  // This fallback matches your APIs: metric, category, region, salesman, sold_to_group, sold_to, product_group, ship_to, pattern
  const $ = (sel) => document.querySelector(sel);

  // Adjust selectors only if yours differ.
  const metric        = (document.querySelector('[name="metric"]:checked')?.value || 'qty').toLowerCase();
  const category      = (document.querySelector('.cat-btn.active')?.dataset?.cat || 'ALL').toUpperCase();
  const region        = ($('#regionTabs .active')?.dataset?.region || 'ALL').toUpperCase();
  const salesman      = ($('#salesmanSelect')?.value || 'ALL');
  const sold_to_group = ($('#soldToGroup')?.value || 'ALL');
  const sold_to       = ($('#soldTo')?.value || 'ALL');
  const product_group = ($('#productGroup')?.value || 'ALL');
  const ship_to       = ($('#shipTo')?.value || 'ALL');
  const pattern       = ($('#patternInput')?.value || 'ALL');

  return { metric, category, region, salesman, sold_to_group, sold_to, product_group, ship_to, pattern };
}






/* -------------------------- UI wiring -------------------------- */
document.getElementById('catBtns').addEventListener("click",e=>{
  if(!e.target.classList.contains("btn"))return;
  filters.category=e.target.dataset.val;
  [...document.querySelectorAll("#catBtns .btn")].forEach(b=>b.classList.toggle("active",b.dataset.val===filters.category));
  refreshAllWithKpi();
});
document.getElementById('metricBtns').addEventListener("click",e=>{
  if(!e.target.classList.contains("btn"))return;
  filters.metric=e.target.dataset.metric;
  setActive(document.getElementById('metricBtns'),"metric",filters.metric);
  document.getElementById('dailyTitle').textContent = filters.metric==="amount"?"Daily Amount":"Daily Sales";
  document.getElementById('cumTitle').textContent   = filters.metric==="amount"?"Cumulative Amount":"Cumulative Sales";
  refreshAllWithKpi();
});
document.getElementById('group_by').addEventListener("change",()=>{
  filters.group_by=document.getElementById('group_by').value;
  refreshAllWithKpi();
});
document.getElementById('regionBtns').addEventListener("click",e=>{
  if(!e.target.classList.contains("btn"))return;
  filters.region=e.target.dataset.val; setActive(document.getElementById('regionBtns'),"val",filters.region);
  const all=Object.values(REGION_SALESMEN).flat();
  const list=filters.region==="ALL"?all:(REGION_SALESMEN[filters.region]||[]);
  populateSelect(document.getElementById('salesman_name'),[...new Set(list)].sort());
  filters.salesman = 'ALL';
  document.getElementById('salesman_name').value = 'ALL';
  refreshAllWithKpi();
});

document.getElementById('salesman_name').addEventListener('change', (e)=>{
  filters.salesman = e.target.value || 'ALL';
  refreshAllWithKpi();
});
document.getElementById('sold_to_group').addEventListener('change', async ()=>{  filters.sold_to_group = document.getElementById('sold_to_group').value || 'ALL';

  const names = await fetchJSON(`/api/sold_to_names?sold_to_group=${document.getElementById('sold_to_group').value}`);
  populateDatalist('sold_to_list', names);
  await refreshShipTo(); // NEW: keep ship-to list in sync with group
  refreshAllWithKpi();
});

// When SOLD_TO_GROUP changes you already reload sold_to options — keep as is.

// SOLD-TO -> fetch Ship-to names under that Sold-to, enable input
document.getElementById('sold_to').addEventListener('input', async (e) => {
  filters.sold_to = e.target.value || 'ALL';
   await refreshShipTo();
  const shipInput = document.getElementById('ship_to');
  const listId = 'ship_to_list';

  if (filters.sold_to && filters.sold_to !== 'ALL') {
    const qs = new URLSearchParams({ sold_to: filters.sold_to }).toString();
    const names = await fetchJSON(`/api/ship_to_names?${qs}`);
    populateDatalist(listId, names);
    shipInput.disabled = false;
  } else {
    populateDatalist(listId, []);
    shipInput.value = '';
    shipInput.disabled = true;
    filters.ship_to = 'ALL';
  }
  refreshAllWithKpi();
});

// Ship-to input -> mirror into filters
document.getElementById('ship_to').addEventListener('input', (e)=>{
  filters.ship_to = document.getElementById('ship_to').value || 'ALL';
  refreshAllWithKpi();
});

// PRODUCT GROUP -> existing code… plus refresh patterns
document.getElementById('product_group').addEventListener('change', async ()=>{
  filters.product_group = document.getElementById('product_group').value || 'ALL';
  await refreshPatterns();     // NEW
  refreshAllWithKpi();
});

// PATTERN input -> mirror into filters
document.getElementById('pattern').addEventListener('input', (e)=>{
  filters.pattern = e.target.value || 'ALL';
  refreshAllWithKpi();
});

async function refreshShipTo(){
  const stg3 = document.getElementById('sold_to_group').value || 'ALL';
  const sold = document.getElementById('sold_to').value || 'ALL';
  const qs = new URLSearchParams({ sold_to_group: stg3, sold_to: sold }).toString();
  const names = await fetchJSON(`/api/ship_to_names?${qs}`);
  populateDatalist('ship_to_list', names);
  refreshAllWithKpi();
}

// Load patterns for current product group
async function refreshPatterns(){
  const pg = document.getElementById('product_group').value || 'ALL';
  const names = await fetchJSON(`/api/patterns?product_group=${encodeURIComponent(pg)}`);
  populateDatalist('pattern_list', names);
  refreshAllWithKpi();
}


// optional debounce helper
function debounce(fn,ms){let t;return(...a)=>{clearTimeout(t);t=setTimeout(()=>fn(...a),ms)}}





/* -------------------------- daily (Oct) – same structure as monthly -------------------------- */

async function fetchDailySales(){
  const qs = new URLSearchParams({
    metric:filters.metric, category:filters.category, region:filters.region, salesman:filters.salesman,
    sold_to_group:filters.sold_to_group, sold_to:filters.sold_to, ship_to:filters.ship_to,
    product_group:filters.product_group, pattern:filters.pattern
  }).toString();
  return fetchJSON(`/api/daily_sales?${qs}`);
}

async function fetchDailyBreakdownWithGroup(groupBy){
  const qs = new URLSearchParams({
    metric:filters.metric, category:filters.category, region:filters.region, salesman:filters.salesman,
    sold_to_group:filters.sold_to_group, sold_to:filters.sold_to, ship_to:filters.ship_to,
    product_group:filters.product_group, pattern:filters.pattern, group_by: groupBy
  }).toString();
  return fetchJSON(`/api/daily_breakdown?${qs}`);
}



// totals (bar + cumulative), same shape as drawMonthlyTotals (no target for daily)
async function drawDailyTotals(){
  const [salesRows] = await Promise.all([ fetchDailySales() ]);
  const labels   = daysLabels();
  const sales    = labels.map((_,i)=> +((salesRows[i]?.value) || 0));
  const salesCum = toCumulative(sales);

  [dailyInst,dailyCumInst].forEach(c=>c&&c.destroy());


  dailyInst = new Chart(document.getElementById("dailyChart"), {
    type:"bar",
    data:{ labels, datasets:[
      { label: filters.metric==="amount"?"Daily Amount":"Daily Qty",
        data:sales, backgroundColor:"#93c5fd", categoryPercentage:0.9, barPercentage:0.9 }
    ]},
    options:getCommonOptions(false, undefined, "Daily")
  });

  dailyCumInst = new Chart(document.getElementById("dailyCumChart"), {
    type:"bar",
    data:{ labels, datasets:[
      { label: filters.metric==="amount"?"Cumulative Amount":"Cumulative Qty",
        data:salesCum, backgroundColor:"#34d399", categoryPercentage:0.9, barPercentage:0.9 }
    ]},
    options:getCommonOptions(false, undefined, "Cumulative")
  });
}

function buildDailyStacks(rows){
  const labels = daysLabels();;
  const groups = [...new Set(rows.map(r => r.group_label))];
  const byGroup = {};
  groups.forEach(g => byGroup[g] = Array(31).fill(0));
  rows.forEach(r => {
    const d = parseInt(r.day, 10);
    if (d>=1 && d<=31) byGroup[r.group_label][d-1] += (+r.value || 0);
  });
  const datasets = groups.map((g,i)=>({
    label:g,
    data:byGroup[g],
    backgroundColor:COLORS[i%COLORS.length],
    stack:"S", categoryPercentage:0.9, barPercentage:0.9
  }));
  return { labels, groups, byGroup, datasets };
}

function toPercentStacksN(byKey, N){
  const keys = Object.keys(byKey);
  const pct = {}; keys.forEach(k => pct[k] = Array(N).fill(0));
  for (let i=0; i<N; i++){
    const tot = keys.reduce((a,k)=> a + (+byKey[k][i]||0), 0) || 1;
    keys.forEach(k => pct[k][i] = +((byKey[k][i] / tot) * 100).toFixed(2));
  }
  return pct;
}


// stacked (value / cumulative / % / cumulative %) — identical to monthly version
async function drawDailyStacked(){
  const effectiveGroup = filters.group_by;
  const rows = await fetchDailyBreakdownWithGroup(effectiveGroup);
  
  if (!rows || !rows.length){
    
    const totals = await fetchDailySales();
    const labels = daysLabels();;
    const data=totals.map(r=>+r.value||0);
    const cum = toCumulative(data);
    stackedDailyInst = makeStacked("stackedDailyChart", labels, [
      { label:"Total", data, backgroundColor:"#a78bfa", stack:"S", categoryPercentage:0.9, barPercentage:0.9 }
    ], "Daily");

    stackedDailyPctInst = makeStacked("stackedDailyPercentChart", labels, [
      { label:"Total %", data: labels.map(()=>100), backgroundColor:"#a78bfa", stack:"S", categoryPercentage:0.9, barPercentage:0.9 }
    ], "Daily %", 100);

    
    // IMPORTANT: write to the same IDs you use in HTML
    stackedDailyCumInst = makeStacked("stackedDailyCumChart", labels, [
      { label:"Total", data:cum, backgroundColor:"#10b981", stack:"S", categoryPercentage:0.9, barPercentage:0.9 }
    ], "Cumulative by Day");

    stackedDailyCumPctInst = makeStacked("stackedDailyCumPercentChart", labels, [
      { label:"Total %", data: labels.map(()=>100), backgroundColor:"#10b981", stack:"S", categoryPercentage:0.9, barPercentage:0.9 }
    ], "Cumulative %", 100);

    return;
  }

  // Build stacks
  let { labels, groups, byGroup, datasets } = buildDailyStacks(rows);

  // Optional Top10 reduction (sold_to only), same as monthly
  if (TOP_MODE === 'top10' && effectiveGroup === 'sold_to'){
    const topSet  = await ensureTopSet();
    const reduced = reduceToTopSmart(groups, byGroup, topSet);
    groups  = reduced.groups;
    byGroup = reduced.map;
    datasets = groups.map((g,i)=>({
      label:g, data:byGroup[g], backgroundColor:COLORS[i%COLORS.length],
      stack:"S", categoryPercentage:0.9, barPercentage:0.9
    }));
  }

  const byGroupCum   = cumPerGroup(byGroup);
  const datasetsCum  = groups.map((g,i)=>({ label:g, data:byGroupCum[g], backgroundColor:COLORS[i%COLORS.length], stack:"S", categoryPercentage:0.9, barPercentage:0.9 }));
  const pct          = toPercentStacksN(byGroup, 31);
  const pctCum       = toPercentStacksN(byGroupCum, 31);
  const datasetsPct  = groups.map((g,i)=>({ label:g, data:pct[g],    backgroundColor:COLORS[i%COLORS.length], stack:"S", categoryPercentage:0.9, barPercentage:0.9 }));
  const datasetsPctC = groups.map((g,i)=>({ label:g, data:pctCum[g], backgroundColor:COLORS[i%COLORS.length], stack:"S", categoryPercentage:0.9, barPercentage:0.9 }));

  [stackedDailyInst, stackedDailyCumInst, stackedDailyPctInst, stackedDailyCumPctInst]
    .forEach(c=>c&&c.destroy());

  stackedDailyInst = new Chart(document.getElementById("stackedDailyChart"), {
    type:"bar", data:{ labels, datasets }, options:getCommonOptions(true, undefined, "Daily")
  });
  // IMPORTANT: match the IDs in your HTML (second box in row 1)
  stackedDailyCumInst = new Chart(document.getElementById("stackedDailyCumChart"), {
    type:"bar", data:{ labels, datasets:datasetsCum }, options:getCommonOptions(true, undefined, "Cumulative by Day")
  });
  stackedDailyPctInst = new Chart(document.getElementById("stackedDailyPercentChart"), {
    type:"bar", data:{ labels, datasets:datasetsPct }, options:getCommonOptions(true, 100, "Daily %")
  });
  stackedDailyCumPctInst = new Chart(document.getElementById("stackedDailyCumPercentChart"), {
    type:"bar", data:{ labels, datasets:datasetsPctC }, options:getCommonOptions(true, 100, "Cumulative %")
  });
}

/* -------------------------- monthly charts -------------------------- */

async function fetchMonthlySales(){
  const qs=new URLSearchParams({
    metric:filters.metric, category:filters.category, region:filters.region, salesman:filters.salesman,
    sold_to_group:filters.sold_to_group, sold_to:filters.sold_to, ship_to:filters.ship_to,
    product_group:filters.product_group, pattern:filters.pattern
  }).toString();
  return fetchJSON(`/api/monthly_sales?${qs}`);
}
async function fetchMonthlyBreakdownWithGroup(groupBy){
  const params = {
    metric:filters.metric, category:filters.category, region:filters.region, salesman:filters.salesman,
    sold_to_group:filters.sold_to_group, sold_to:filters.sold_to, ship_to:filters.ship_to,
    product_group:filters.product_group, pattern:filters.pattern, group_by: groupBy
  };
  const qs=new URLSearchParams(params).toString();
  return fetchJSON(`/api/monthly_breakdown?${qs}`);
}

async function drawMonthlyTotals(){
  const [salesRows,targetRows]=await Promise.all([
    fetchMonthlySales(),
    fetchJSON(`/api/monthly_target?${new URLSearchParams({
      metric:filters.metric, category:filters.category, region:filters.region, salesman:filters.salesman,
      sold_to_group:filters.sold_to_group, sold_to:filters.sold_to, ship_to:filters.ship_to,
      product_group:filters.product_group, pattern:filters.pattern
    }).toString()}`)
  ]);
  const labels=monthsLabels();
  const sales = salesRows.map(r=>+r.value||0);
  const targets= targetRows.map(r=>+r.value||0);
  const salesCum=toCumulative(sales), targetCum=toCumulative(targets);

  [monthlyInst,monthlyCumInst].forEach(c=>c&&c.destroy());
  if (!Chart.registry.plugins.get("showDataValues")) {
    Chart.register(showDataValuesPlugin);
  }
  monthlyInst=new Chart(document.getElementById("monthlyChart"),{
    type:"bar",
    data:{labels,datasets:[
      {label:filters.metric==="amount"?"Monthly Amount":"Monthly Qty",data:sales,backgroundColor:"#93c5fd", categoryPercentage:0.9, barPercentage:0.9},
      {label:"Monthly Target",type:"line",data:targets,borderWidth:2,pointRadius:0,borderDash:[6,4],borderColor:"#ef4444"}
    ]},
    options:getCommonOptions(false)
  });
  monthlyCumInst=new Chart(document.getElementById("monthlyCumChart"),{
    type:"bar",
    data:{labels,datasets:[
      {label:filters.metric==="amount"?"Cumulative Amount":"Cumulative Qty",data:salesCum,backgroundColor:"#34d399", categoryPercentage:0.9, barPercentage:0.9},
      {label:"Cumulative Target",type:"line",data:targetCum,borderWidth:2,pointRadius:0,borderDash:[6,4],borderColor:"#ef4444"}
    ]},
    options:getCommonOptions(false)
  });
}

function buildMonthlyStacks(rows){
  const labels=monthsLabels();
  const groups=[...new Set(rows.map(r=>r.group_label))];
  const byGroup={}; groups.forEach(g=>byGroup[g]=Array(12).fill(0));
  rows.forEach(r=>{ const m=parseInt(r.month,10); if(m>=1 && m<=12){ byGroup[r.group_label][m-1]+= (+r.value||0); } });
  const datasets=groups.map((g,i)=>({label:g,data:byGroup[g],backgroundColor:COLORS[i%COLORS.length],stack:"S", categoryPercentage:0.9, barPercentage:0.9}));
  return {labels,groups,byGroup,datasets};
}

function toPercentStacks(byKey){
  const n=12, keys=Object.keys(byKey);
  const pct={}; keys.forEach(k=>pct[k]=Array(n).fill(0));
  for(let i=0;i<n;i++){
    const tot=keys.reduce((a,k)=>a+(+byKey[k][i]||0),0)||1;
    keys.forEach(k=>{ pct[k][i]= +(((byKey[k][i]/tot)*100)).toFixed(2); });
  }
  return pct;
}

async function drawMonthlyStacked(){
  // Respect user’s Group By; only reduce when Group By = sold_to and Top10 active
  const effectiveGroup = filters.group_by;
  const rows = await fetchMonthlyBreakdownWithGroup(effectiveGroup);

  if(!rows || !rows.length){
    [stackedMonthlyInst, stackedMonthlyCumInst, stackedMonthlyPctInst, stackedMonthlyCumPctInst]
      .forEach(c=>c&&c.destroy());
    const totals=await fetchMonthlySales();
    const labels=monthsLabels();
    const data=totals.map(r=>+r.value||0);
    stackedMonthlyInst=makeStacked("stackedMonthlyChart",labels,[{label:"Total",data,backgroundColor:"#a78bfa",stack:"S", categoryPercentage:0.9, barPercentage:0.9}],"Monthly");
    stackedMonthlyPctInst=makeStacked("stackedMonthlyPercentChart",labels,[{label:"Total %",data:labels.map(()=>100),backgroundColor:"#a78bfa",stack:"S", categoryPercentage:0.9, barPercentage:0.9}],"Monthly %",100);
    const cum=toCumulative(data);
    stackedMonthlyCumInst=makeStacked("stackedMonthlyCumChart",labels,[{label:"Total",data:cum,backgroundColor:"#10b981",stack:"S", categoryPercentage:0.9, barPercentage:0.9}],"Cumulative by Month");
    stackedMonthlyCumPctInst=makeStacked("stackedMonthlyCumPercentChart",labels,[{label:"Total %",data:labels.map(()=>100),backgroundColor:"#10b981",stack:"S", categoryPercentage:0.9, barPercentage:0.9}],"Cumulative %",100);
    return;
  }

  let {labels,groups,byGroup,datasets}=buildMonthlyStacks(rows);

  // Reduce to Top10 + Other when appropriate (only when we're on sold_to)
  if (TOP_MODE === 'top10' && effectiveGroup === 'sold_to') {
    const topSet = await ensureTopSet();
    const reduced = reduceToTopSmart(groups, byGroup, topSet);
    groups = reduced.groups;
    byGroup = reduced.map;
    // Find the last month index that has *any* data across groups (0–11)
    const lastMonth = (()=>{ const arrs = Object.values(byGroup); let li=-1; for(let i=0;i<12;i++){ let s=0; for(const a of arrs) s += (+a[i]||0); if (s!==0) li=i; } return li; })();
    // Helper to blank future months
    const cut = a => a.map((v,i)=> i>lastMonth ? null : v);

    datasets = groups.map((g,i)=>({
      label:g, data: byGroup[g], backgroundColor: COLORS[i%COLORS.length],
      stack:"S", categoryPercentage:0.9, barPercentage:0.9
    }));
  }

  const byGroupCum=cumPerGroup(byGroup);
  const datasetsCum=groups.map((g,i)=>({label:g,data:byGroupCum[g],backgroundColor:COLORS[i%COLORS.length],stack:"S", categoryPercentage:0.9, barPercentage:0.9}));
  const pct=toPercentStacks(byGroup);
  const datasetsPct=groups.map((g,i)=>({label:g,data:pct[g],backgroundColor:COLORS[i%COLORS.length],stack:"S", categoryPercentage:0.9, barPercentage:0.9}));
  const pctCum=toPercentStacks(byGroupCum);
  const datasetsPctCum=groups.map((g,i)=>({label:g,data:pctCum[g],backgroundColor:COLORS[i%COLORS.length],stack:"S", categoryPercentage:0.9, barPercentage:0.9}));

  [stackedMonthlyInst, stackedMonthlyCumInst, stackedMonthlyPctInst, stackedMonthlyCumPctInst]
    .forEach(c=>c&&c.destroy());

  stackedMonthlyInst       = new Chart(document.getElementById("stackedMonthlyChart"), { type:"bar", data:{ labels, datasets }, options:getCommonOptions(true, undefined, "Monthly") });
  stackedMonthlyCumInst    = new Chart(document.getElementById("stackedMonthlyCumChart"), { type:"bar", data:{ labels, datasets: datasetsCum }, options:getCommonOptions(true, undefined, "Cumulative by Month") });
  stackedMonthlyPctInst    = new Chart(document.getElementById("stackedMonthlyPercentChart"), { type:"bar", data:{ labels, datasets: datasetsPct }, options:getCommonOptions(true, 100, "Monthly %") });
  stackedMonthlyCumPctInst = new Chart(document.getElementById("stackedMonthlyCumPercentChart"), { type:"bar", data:{ labels, datasets: datasetsPctCum }, options:getCommonOptions(true, 100, "Cumulative %") });
}

/* -------------------------- yearly charts -------------------------- */

// we already have: let yearlyInst, stackedYearlyInst, stackedYearlyPctInst;
// DON’T redeclare them again.

async function fetchYearlySales() {
  const qs = new URLSearchParams({
    metric:        filters.metric,
    category:      filters.category,
    region:        filters.region,
    salesman:      filters.salesman,
    sold_to_group: filters.sold_to_group,
    sold_to:       filters.sold_to,
    ship_to:       filters.ship_to,
    product_group: filters.product_group,
    pattern:       filters.pattern
  }).toString();
  return fetchJSON(`/api/yearly_sales?${qs}`);
}

async function fetchYearlyBreakdownWithGroup(groupBy) {
  const qs = new URLSearchParams({
    metric:        filters.metric,
    category:      filters.category,
    region:        filters.region,
    salesman:      filters.salesman,
    sold_to_group: filters.sold_to_group,
    sold_to:       filters.sold_to,
    ship_to:       filters.ship_to,
    product_group: filters.product_group,
    pattern:       filters.pattern,
    group_by:      groupBy
  }).toString();
  return fetchJSON(`/api/yearly_breakdown?${qs}`);
}


// simple yearly bar (no cumulative)
async function drawYearlyTotals() {
  const rows = await fetchYearlySales();   // [{year:2024, value:123}, ...]
  const labels = yearsLabels();
  // map 4 years -> values
  const data = labels.map(y => {
    const r = rows.find(row => +row.year === y);
    return r ? +r.value || 0 : 0;
  });

  if (yearlyInst) {
    yearlyInst.destroy();
  }

  yearlyInst = new Chart(document.getElementById("yearlyChart"), {
    type: "bar",
    data: {
      labels,
      datasets: [
        {
          label: filters.metric === "amount" ? "Yearly Amount" : "Yearly Qty",
          data,
          backgroundColor: "#93c5fd",
          categoryPercentage: 0.9,
          barPercentage: 0.9
        }
      ]
    },
    options: getCommonOptions(false, undefined, "Yearly")
  });
}

// build stacks for yearly
function buildYearlyStacks(rows) {
  const labels = yearsLabels();
  const groups = [...new Set(rows.map(r => r.group_label))];
  const byGroup = {};
  groups.forEach(g => (byGroup[g] = Array(labels.length).fill(0)));

  rows.forEach(r => {
    const y = parseInt(r.year, 10);
    const idx = labels.indexOf(y);
    if (idx !== -1) {
      byGroup[r.group_label][idx] += (+r.value || 0);
    }
  });

  const datasets = groups.map((g, i) => ({
    label: g,
    data: byGroup[g],
    backgroundColor: COLORS[i % COLORS.length],
    stack: "S",
    categoryPercentage: 0.9,
    barPercentage: 0.9
  }));

  return { labels, groups, byGroup, datasets };
}

// % helper for N=number of years
function toPercentStacksNYears(byKey, labelsLen) {
  const keys = Object.keys(byKey);
  const pct = {};
  keys.forEach(k => (pct[k] = Array(labelsLen).fill(0)));
  for (let i = 0; i < labelsLen; i++) {
    const tot = keys.reduce((a, k) => a + (+byKey[k][i] || 0), 0) || 1;
    keys.forEach(k => {
      pct[k][i] = +(((byKey[k][i] || 0) / tot) * 100).toFixed(2);
    });
  }
  return pct;
}

async function drawYearlyStacked() {
  const effectiveGroup = filters.group_by;
  const rows = await fetchYearlyBreakdownWithGroup(effectiveGroup);

  // no data -> show single total per year
  if (!rows || !rows.length) {
    const totals = await fetchYearlySales();
    const labels = yearsLabels();
    const data = labels.map(y => {
      const r = totals.find(t => +t.year === y);
      return r ? +r.value || 0 : 0;
    });

    if (stackedYearlyInst) stackedYearlyInst.destroy();
    if (stackedYearlyPctInst) stackedYearlyPctInst.destroy();

    stackedYearlyInst = makeStacked(
      "stackedYearlyChart",
      labels,
      [
        {
          label: "Total",
          data,
          backgroundColor: "#a78bfa",
          stack: "S",
          categoryPercentage: 0.9,
          barPercentage: 0.9
        }
      ],
      "Yearly"
    );

    stackedYearlyPctInst = makeStacked(
      "stackedYearlyPercentChart",
      labels,
      [
        {
          label: "Total %",
          data: labels.map(() => 100),
          backgroundColor: "#a78bfa",
          stack: "S",
          categoryPercentage: 0.9,
          barPercentage: 0.9
        }
      ],
      "Yearly %",
      100
    );
    return;
  }

  // build stacks from real rows
  let { labels, groups, byGroup, datasets } = buildYearlyStacks(rows);

  // same top10 logic as monthly/daily
  if (TOP_MODE === "top10" && effectiveGroup === "sold_to") {
    const topSet = await ensureTopSet();
    const reduced = reduceToTopSmart(groups, byGroup, topSet);
    groups = reduced.groups;
    byGroup = reduced.map;
    datasets = groups.map((g, i) => ({
      label: g,
      data: byGroup[g],
      backgroundColor: COLORS[i % COLORS.length],
      stack: "S",
      categoryPercentage: 0.9,
      barPercentage: 0.9
    }));
  }

  const pct = toPercentStacksNYears(byGroup, labels.length);
  const datasetsPct = groups.map((g, i) => ({
    label: g,
    data: pct[g],
    backgroundColor: COLORS[i % COLORS.length],
    stack: "S",
    categoryPercentage: 0.9,
    barPercentage: 0.9
  }));

  if (stackedYearlyInst) stackedYearlyInst.destroy();
  if (stackedYearlyPctInst) stackedYearlyPctInst.destroy();

  stackedYearlyInst = new Chart(document.getElementById("stackedYearlyChart"), {
    type: "bar",
    data: { labels, datasets },
    options: getCommonOptions(true, undefined, "Yearly")
  });

  stackedYearlyPctInst = new Chart(
    document.getElementById("stackedYearlyPercentChart"),
    {
      type: "bar",
      data: { labels, datasets: datasetsPct },
      options: getCommonOptions(true, 100, "Yearly %")
    }
  );
}

/* -------------------------- init & orchestrator -------------------------- */
async function initControls(){
  setActive(document.getElementById('metricBtns'),"metric",filters.metric);
  setActive(document.getElementById('regionBtns'),"val",filters.region);
  populateSelect(document.getElementById('salesman_name'),[...new Set(Object.values(REGION_SALESMEN).flat())].sort());

  const groups=await fetchJSON("/api/product_group"); populateSelect(document.getElementById('product_group'),groups);

  const stg3 = await fetchJSON("/api/sold_to_groups");
  populateSelect(document.getElementById('sold_to_group'), stg3, true);
  document.getElementById('sold_to_group').value = "ALL";

  const soldTo=await fetchJSON("/api/sold_to_names?sold_to_group=ALL");
  const list = document.getElementById('sold_to_list');
  list.innerHTML = '';
  soldTo.forEach(n => { const o=document.createElement('option'); o.value=n; list.appendChild(o); });
  await refreshShipTo(); // show ALL ship-to names initially

}
/* ---------------------- FAST KPI TABLE (robust) ---------------------- */

/* helpers */
const __fmtInt = n => (Math.round(+n || 0)).toLocaleString();
const __pctClass = v => v>=80 ? 'kpi-good' : (v>=60 ? 'kpi-ok' : 'kpi-bad');

function kpiQS() {
  const base = {
    metric:        filters.metric,
    category:      filters.category,
    region:        filters.region,
    sold_to_group: filters.sold_to_group,
    sold_to:       filters.sold_to,
    ship_to:       filters.ship_to,
    product_group: filters.product_group,
    pattern:       filters.pattern
 ,
    salesman:      filters.salesman
  };
  return new URLSearchParams(base).toString();
}

const __kpiCache = new Map();
async function fetchKpiSnapshot() {
  const qs = kpiQS();
  if (__kpiCache.has(qs)) return __kpiCache.get(qs);
  const data = await fetchJSON(`/api/kpi_snapshot?${qs}`);
  __kpiCache.set(qs, data);
  return data;
}

/* Ensure/return the container we render into */
function ensureKpiContainer() {
  let wrap = document.getElementById('kpiRegionGrid');
  if (!wrap) {
    // Try a parent holder first
    const holder =
      document.getElementById('kpiRegionGridWrap') ||
      document.querySelector('#kpiRegionGridWrap') ||
      document.body;

    wrap = document.createElement('div');
    wrap.id = 'kpiRegionGrid';
    holder.appendChild(wrap);
  }
  return wrap;
}

function rowHtml(name, pack, isHeader=false) {
  const cell = (obj) =>
    `<td class="num">
       <div class="pct ${__pctClass(+obj.p||0)}">${__fmtInt(obj.p)}%</div>
       <div class="mini">${__fmtInt(obj.a)} / ${__fmtInt(obj.t)}</div>
     </td>`;
  return `
    <tr class="${isHeader?'is-overall':''}">
      <td class="name">${name ?? ''}</td>
      ${cell(pack.jul)}${cell(pack.q1)}${cell(pack.q2)}${cell(pack.q3)}
    </tr>`;
}

function smallTableHtml(caption, rowsHtml) {
  return `
    <div class="region-table-box">
      <div class="region-caption">${caption ?? ''}</div>
      <table class="kpi-table">
        <thead>
          <tr>
            <th class="name-col">Name</th>
            <th>Jul</th>
            <th>Q1</th>
            <th>Q2</th>
            <th>Q3-to-date</th>
          </tr>
        </thead>
        <tbody>${rowsHtml}</tbody>
      </table>
    </div>`;
}

async function renderFastKpiTable() {
  const wrap = ensureKpiContainer();
  wrap.innerHTML = '<div class="muted">Loading KPI…</div>';

  try {
    const snap = await fetchKpiSnapshot();
    if (!snap || snap.error) {
      console.warn('kpi_snapshot returned no data or error:', snap?.error);
      wrap.innerHTML = '<div class="muted">No KPI data.</div>';
      return;
    }

    // Overall
  let html = smallTableHtml('Overall', rowHtml('All', snap.overall, true));

  // Regions (respect filters.region)
  const regions = Array.isArray(snap.regions) ? snap.regions : [];
  const filteredRegions = (filters.region && filters.region !== 'ALL') ? regions.filter(r => String(r.region||'').toUpperCase() === String(filters.region||'').toUpperCase()) : regions;
  html += filteredRegions.map(r => {
      const caption = r.region ?? 'Region';
      let rows = rowHtml(r.region ?? 'Region', r.kpi || { jul:{a:0,t:0,p:0}, q1:{a:0,t:0,p:0}, q2:{a:0,t:0,p:0}, q3:{a:0,t:0,p:0} }, true);
      let salesmen = Array.isArray(r.salesmen) ? r.salesmen : [];
      if (filters.salesman && filters.salesman !== 'ALL') {
        salesmen = salesmen.filter(s => (s.name||'') === filters.salesman);
      }
      salesmen.forEach(s => { rows += rowHtml(s.name ?? 'UNK', s); });
      return smallTableHtml(caption, rows);
    }).join('');

    wrap.innerHTML = html;
  } catch (e) {
    console.error('renderFastKpiTable failed:', e);
    wrap.innerHTML = '<div class="muted">Failed to load KPIs.</div>';
    showError(`KPI table error: ${e.message || e}`);
  }
}
/* ===================== PROFIT (COMBINED) ===================== */

let profitComboInst = null;

const PROFIT_MONTH_LABELS = [
  "Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"
];

function renderProfitCombined(rows) {
  const el = document.getElementById("profitComboChart");
  if (!el) return;

  if (profitComboInst) {
    profitComboInst.destroy();
    profitComboInst = null;
  }

  // Ensure we always have 12 months
  const byMonth = Array.from({ length: 12 }, (_, i) =>
    rows.find(r => +r.month === i + 1) || { month: i + 1, gross: 0, sd: 0, cogs: 0, op_cost: 0 }
  );

  const gross = byMonth.map(r => +r.gross || 0);
  const sd    = byMonth.map(r => +r.sd || 0);
  const cogs  = byMonth.map(r => +r.cogs || 0);
  const op    = byMonth.map(r => +r.op_cost || 0);

  const totalCost = sd.map((v, i) => v + cogs[i] + op[i]);
  const profitPct = gross.map((g, i) => (g > 0 ? ((g - totalCost[i]) / g) * 100 : 0));

  profitComboInst = new Chart(el, {
    type: "bar",
    data: {
      labels: PROFIT_MONTH_LABELS,
      datasets: [
        // Bar group 1: Gross
        {
          type: "bar",
          label: "Gross",
          data: gross,
          yAxisID: "y",
          stack: "G",
          backgroundColor: "#93c5fd",
          borderColor: "#60a5fa",
          borderWidth: 1,
          categoryPercentage: 0.9,
          barPercentage: 0.9
        },
        // Bar group 2: stacked Costs (beside Gross)
       
        {
          type: "bar",
          label: "COGS",
          data: cogs,
          yAxisID: "y",
          stack: "C",
          backgroundColor: "#f87171",
          categoryPercentage: 0.9,
          barPercentage: 0.9
        },
        {
          type: "bar",
          label: "Op Cost",
          data: op,
          yAxisID: "y",
          stack: "C",
          backgroundColor: "#fbbf24",
          categoryPercentage: 0.9,
          barPercentage: 0.9
        },
        {
          type: "bar",
          label: "Sales Deduction",
          data: sd,
          yAxisID: "y",
          stack: "C",
          backgroundColor: "#d55fc3ff",
          categoryPercentage: 0.9,
          barPercentage: 0.9
        },
        // Line: Profit %
        {
          type: "line",
          label: "Profit %",
          data: profitPct,
          yAxisID: "y1",   // right axis
          tension: 0.25,
          borderWidth: 2,
          pointRadius: 2,
          fill: false,
          borderColor: "#10b981",
          pointBackgroundColor: "#10b981"
        }
      ]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      interaction: { mode: "index", intersect: false },
      scales: {
        x: { stacked: false },
        y: {
          beginAtZero: true,
          title: { display: true, text: "Amount" },
          ticks: { callback: v => Number(v).toLocaleString() }
        },
        y1: {
          position: "right",
          beginAtZero: true,
          suggestedMax: 100,
          title: { display: true, text: "Profit %" },
          grid: { drawOnChartArea: false },
          ticks: { callback: v => `${Math.round(v)}%` }
        }
      },
      plugins: {
        datalabels: false,
        legend: { display: true },
        tooltip: {
          callbacks: {
            label: (ctx) => {
              if (ctx.dataset.yAxisID === "y1") {
                return `${ctx.dataset.label}: ${ctx.parsed.y.toFixed(1)}%`;
              }
              return `${ctx.dataset.label}: ${Number(ctx.parsed.y || 0).toLocaleString()}`;
            }
          }
        }
      }
    }
  });
}

async function loadProfit() {
  const qs = new URLSearchParams({
    metric:        filters.metric,
    category:      filters.category,
    region:        filters.region,
    salesman:      filters.salesman,
    sold_to_group: filters.sold_to_group,
    sold_to:       filters.sold_to,
    ship_to:       filters.ship_to,
    product_group: filters.product_group,
    pattern:       filters.pattern
  }).toString();

  const rows = await fetchJSON(`/api/profit_monthly?${qs}`);
  renderProfitCombined(Array.isArray(rows) ? rows : []);
}

/* =================== END PROFIT (COMBINED) =================== */




async function refreshAllWithKpi(){
  await drawDailyTotals(),          // now uses October data internally
  await drawDailyStacked(),
  await drawMonthlyTotals();
  await drawMonthlyStacked();
  await loadProfit();
  await drawYearlyTotals();
  await drawYearlyStacked();
  await renderFastKpiTable();
  await loadProfit();
}

(async function start(){
  await initControls();
  [...document.querySelectorAll("#catBtns .btn")].forEach(b=>b.classList.toggle("active",b.dataset.val===filters.category));
  await refreshAllWithKpi();
  
  await refreshPatterns();                 // make pattern list available on load
  document.getElementById('ship_to').disabled = false; // locked until Sold-to is chosen
})();
