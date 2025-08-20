/* -------------------------- state & helpers -------------------------- */
const COLORS=["#1f77b4","#ff7f0e","#2ca02c","#d62728","#9467bd","#8c564b","#e377c2","#7f7f7f","#bcbd22","#17becf","#aec7e8","#ffbb78"];
const REGION_SALESMEN={
  NSW:["Hamid Jallis","LUTTRELL STEVE","Hulley Gary","Lee Don"],
  QLD:["Lopez Randall","Spires Steven","Sampson Kieren","Marsh Aaron"],
  VIC:["Bellotto Nicola","Bilston Kelley","Gultjaeff Jason","Hobkirk Calvin"],
  WA:["Fruci Davide","Gilbert Michael"]
};

// Normalise names so monthly & daily labels match exactly
const norm = (s) => (s ?? "")
  .toString()
  .replace(/\s+/g, " ")
  .trim()
  .toUpperCase();

const filters={metric:"qty",group_by:"region",region:"ALL",salesman:"ALL",sold_to_group:"ALL",sold_to:"ALL",product_group:"ALL",category:"ALL"};

let dailyInst,cumulativeInst,monthlyInst,monthlyCumInst,
    stackedDailyInst,stackedDailyPctInst,stackedCumInst,stackedCumPctInst,
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
    product_group: filters.product_group
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

  const monthlyRows = await fetchJSON(`/api/monthly_sales_breakdown?${backupQs}`);
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
        const val = ds.data[idx];
        if (val == null) return; // skip gaps
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
      legend:{position:"top"},
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

// ---------- JULY LABELS ----------
function julyLabels(year = 2025) {
  const labels = [];
  for (let d=1; d<=31; d++) {
    const dd = String(d).padStart(2,'0');
    const mm = '07';
    const yy = String(year).slice(-2);
    labels.push(`${dd}-${mm}-${yy}`);
  }
  return labels;
}

/* -------------------------- UI wiring -------------------------- */
document.getElementById('catBtns').addEventListener("click",e=>{
  if(!e.target.classList.contains("btn"))return;
  filters.category=e.target.dataset.val;
  [...document.querySelectorAll("#catBtns .btn")].forEach(b=>b.classList.toggle("active",b.dataset.val===filters.category));
  drawAll();
});
document.getElementById('metricBtns').addEventListener("click",e=>{
  if(!e.target.classList.contains("btn"))return;
  filters.metric=e.target.dataset.metric;
  setActive(document.getElementById('metricBtns'),"metric",filters.metric);
  document.getElementById('dailyTitle').textContent = filters.metric==="amount"?"Daily Amount":"Daily Sales";
  document.getElementById('cumTitle').textContent   = filters.metric==="amount"?"Cumulative Amount":"Cumulative Sales";
  drawAll();
});
document.getElementById('group_by').addEventListener("change",()=>{
  filters.group_by=document.getElementById('group_by').value;
  drawAll();
});
document.getElementById('regionBtns').addEventListener("click",e=>{
  if(!e.target.classList.contains("btn"))return;
  filters.region=e.target.dataset.val; setActive(document.getElementById('regionBtns'),"val",filters.region);
  const all=Object.values(REGION_SALESMEN).flat();
  const list=filters.region==="ALL"?all:(REGION_SALESMEN[filters.region]||[]);
  populateSelect(document.getElementById('salesman_name'),[...new Set(list)].sort());
});
document.getElementById('sold_to_group').addEventListener('change', async ()=>{
  const names = await fetchJSON(`/api/sold_to_names?sold_to_group=${document.getElementById('sold_to_group').value}`);
  const list = document.getElementById('sold_to_list');
  list.innerHTML='';
  names.forEach(n=>{ const o=document.createElement('option'); o.value=n; list.appendChild(o);});
});
document.getElementById('sold_to').addEventListener('input', (e) => { filters.sold_to = e.target.value || 'ALL'; });
document.getElementById('topModeBtns').addEventListener('click', async (e)=>{
  if (!e.target.classList.contains('btn')) return;
  TOP_MODE = e.target.dataset.mode; // 'all' | 'top10'
  setActive(document.getElementById('topModeBtns'), 'mode', TOP_MODE);
  await drawAll();
});
document.getElementById('applyBtn').addEventListener('click',()=>drawAll());

/* -------------------------- data fetchers -------------------------- */
async function fetchDailyTarget(){
  const query=new URLSearchParams({
    metric:filters.metric, region:filters.region, salesman:filters.salesman,
    sold_to_group:filters.sold_to_group, sold_to:filters.sold_to, product_group:filters.product_group,
    category:filters.category
  }).toString();
  const data = await fetchJSON(`/api/july_target?${query}`);
  return (data && typeof data.daily_target==="number")? data.daily_target : null;
}
async function fetchMonthlySales(){
  const qs=new URLSearchParams({
    metric:filters.metric, category:filters.category, region:filters.region, salesman:filters.salesman,
    sold_to_group:filters.sold_to_group, sold_to:filters.sold_to, product_group:filters.product_group
  }).toString();
  return fetchJSON(`/api/monthly_sales?${qs}`);
}
async function fetchMonthlyBreakdownWithGroup(groupBy){
  const params = {
    metric:filters.metric, category:filters.category, region:filters.region, salesman:filters.salesman,
    sold_to_group:filters.sold_to_group, sold_to:filters.sold_to, product_group:filters.product_group,
    group_by:groupBy
  };
  const qs=new URLSearchParams(params).toString();
  return fetchJSON(`/api/monthly_sales_breakdown?${qs}`);
}

/* -------------------------- monthly charts -------------------------- */
async function drawMonthlyTotals(){
  const [salesRows,targetRows]=await Promise.all([
    fetchMonthlySales(),
    fetchJSON(`/api/monthly_target?${new URLSearchParams({
      metric:filters.metric, category:filters.category, region:filters.region, salesman:filters.salesman, sold_to_group:filters.sold_to_group
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

/* -------------------------- daily & cumulative (July + KPI) -------------------------- */
function updateAchievementKPI(actual, target){
  const card = document.getElementById('achvCard');
  const vEl  = document.getElementById('achvValue');
  const sEl  = document.getElementById('achvSub');
  if (!card || !vEl || !sEl) return;

  if (!target || target <= 0){
    vEl.textContent = '--%';
    sEl.textContent = 'Actual — / Target —';
    card.classList.remove('good','ok','bad');
    return;
  }

  const pct = (actual / target) * 100;
  vEl.textContent = pct.toFixed(1) + '%';
  sEl.textContent = `${(actual||0).toLocaleString()} / ${target.toLocaleString()}`;

  card.classList.remove('good','ok','bad');
  if (pct >= 100) card.classList.add('good');
  else if (pct >= 90) card.classList.add('ok');
  else card.classList.add('bad');
}

async function drawDailyAndCumulative(){
  filters.salesman=document.getElementById('salesman_name').value||"ALL";
  filters.sold_to_group=document.getElementById('sold_to_group').value||"ALL";
  filters.sold_to=document.getElementById('sold_to').value||"ALL";
  filters.product_group=document.getElementById('product_group').value||"ALL";

  // Use user's Group By. We’ll reduce to Top-10 only when grouping by Sold-to and there isn’t a single Sold-to filter.
  const skuParams = { ...filters };
  const isSoldToGrouping = skuParams.group_by === 'sold_to';
  const isSingleSoldTo = skuParams.sold_to && skuParams.sold_to !== 'ALL';

  const [rows,dailyTarget]=await Promise.all([
    fetchJSON(`/api/sku_trend?${new URLSearchParams(skuParams).toString()}`),
    fetchDailyTarget()
  ]);

  const labels=julyLabels(2025);
  const idxMap=Object.fromEntries(labels.map((d,i)=>[d,i]));

  const groupBy = filters.group_by;
  let cats=[...new Set(rows.map(d=> labelForRow(d)))];
  const dailyByCat={}; cats.forEach(k=> dailyByCat[k]=Array(labels.length).fill(0));

  rows.forEach(r=>{
    const i=idxMap[r.billing_date];
    if(i==null) return;
    const k = labelForRow(r);
    const val=Number(r.daily_value ?? r.daily_qty ?? 0) || 0;
    if (!dailyByCat[k]) dailyByCat[k] = Array(labels.length).fill(0);
    dailyByCat[k][i] += val;
  });

  // If Top-10 is active and grouping by Sold-to, reduce to Top-10 + Other (smart fallback)
  if (TOP_MODE === 'top10' && isSoldToGrouping && !isSingleSoldTo) {
    const topSet = await ensureTopSet();
    const reduced = reduceToTopSmart(cats, dailyByCat, topSet);
    cats = reduced.groups;

    // replace dailyByCat with reduced.map
    for (const k of Object.keys(dailyByCat)) delete dailyByCat[k];
    Object.assign(dailyByCat, reduced.map);
  }

  const dailyTotal = labels.map((_,i)=> cats.reduce((a,k)=> a + (dailyByCat[k][i]||0), 0));
  const cumulativeTotalRaw = (()=>{ let run=0; return labels.map((_,i)=> (run += (dailyTotal[i]||0))); })();

  const hadAnyData = new Set(rows.map(r=>r.billing_date));
  const dailyTotalForChart = labels.map((d,i)=> hadAnyData.has(d) ? dailyTotal[i] : null);
  const cumulativeForChart = labels.map((d,i)=> hadAnyData.has(d) ? cumulativeTotalRaw[i] : null);

  const pctByCat={}, cumByCat={}, cumpctByCat={};
  cats.forEach(k=>{
    pctByCat[k]=labels.map((d,i)=>{
      const tot=dailyTotal[i]||0;
      if(!hadAnyData.has(d) || tot===0) return null;
      return +(((dailyByCat[k][i]||0)/tot)*100).toFixed(2);
    });
    let run=0;
    cumByCat[k]=labels.map((d,i)=>{ run += (dailyByCat[k][i]||0); return hadAnyData.has(d) ? run : null; });
  });
  for(let i=0;i<labels.length;i++){
    const d=labels[i];
    const cumTot=cats.reduce((a,k)=> a + (cumByCat[k][i]||0), 0);
    cats.forEach(k=>{
      if(!cumpctByCat[k]) cumpctByCat[k]=Array(labels.length).fill(null);
      cumpctByCat[k][i] = (!hadAnyData.has(d) || cumTot===0) ? null : +(((cumByCat[k][i]||0)/cumTot)*100).toFixed(2);
    });
  }

  const ds = (map)=> cats.map((k,i)=>({label:k,data:map[k],backgroundColor:COLORS[i%COLORS.length],stack:"S", categoryPercentage:0.9, barPercentage:0.9}));

  const showTarget = dailyTarget!=null;
  const dailyTargetLine = showTarget? {label:"Daily Target",type:"line",data:labels.map(()=>dailyTarget),borderColor:"red",borderWidth:2,pointRadius:0,fill:false}:null;
  const cumulativeTargetLine= showTarget? {label:"Cumulative Target",type:"line",data:labels.map((_,i)=>dailyTarget*(i+1)),borderColor:"red",borderWidth:2,pointRadius:0,fill:false}:null;

  [dailyInst,cumulativeInst,stackedDailyInst,stackedDailyPctInst,stackedCumInst,stackedCumPctInst].forEach(c=>c&&c.destroy());

  const metricLabel = filters.metric==="amount"?"Amount":"SKU";

  dailyInst=new Chart(document.getElementById("dailyChart"),{
    type:"bar",
    data:{labels,datasets:[
      {label:`Daily ${metricLabel}`,data:dailyTotalForChart,backgroundColor:"#a78bfa", categoryPercentage:0.9, barPercentage:0.9},
      ...(dailyTargetLine?[dailyTargetLine]:[])
    ]},
    options:getCommonOptions(false)
  });

  cumulativeInst=new Chart(document.getElementById("cumulativeChart"),{
    type:"bar",
    data:{labels,datasets:[
      {label:`Cumulative ${metricLabel}`,data:cumulativeForChart,backgroundColor:"#0ea5a3", categoryPercentage:0.9, barPercentage:0.9},
      ...(cumulativeTargetLine?[cumulativeTargetLine]:[])
    ]},
    options:getCommonOptions(false)
  });

  // Achievement % (overall) up to last non-null day
  let lastIdx = -1;
  for (let i = cumulativeForChart.length - 1; i >= 0; i--) {
    if (cumulativeForChart[i] != null) { lastIdx = i; break; }
  }
  const cumActual = lastIdx >= 0 ? cumulativeTotalRaw[lastIdx] : 0;
  const cumTargetVal = (dailyTarget != null && lastIdx >= 0) ? dailyTarget * (lastIdx + 1) : 0;
  updateAchievementKPI(cumActual, cumTargetVal);

  // Stacked daily / % / cumulative / cumulative %
  stackedDailyInst   = new Chart(document.getElementById("stackedDailyChart"),         { type:"bar", data:{ labels, datasets: ds(dailyByCat) },     options:getCommonOptions(true) });
  stackedDailyPctInst= new Chart(document.getElementById("stackedDailyPercentChart"), { type:"bar", data:{ labels, datasets: ds(pctByCat) },       options:getCommonOptions(true, 100, "Daily %") });
  stackedCumInst     = new Chart(document.getElementById("stackedCumulativeChart"),   { type:"bar", data:{ labels, datasets: ds(cumByCat) },       options:getCommonOptions(true) });
  stackedCumPctInst  = new Chart(document.getElementById("stackedCumulativePercentChart"), { type:"bar", data:{ labels, datasets: ds(cumpctByCat) }, options:getCommonOptions(true, 100, "Cumulative %") });
}

/* -------------------------- KPI by Salesman -------------------------- */
function salesmanNamesByRegion(region="ALL"){
  const all = Object.values(REGION_SALESMEN).flat();
  return region && region !== "ALL" ? (REGION_SALESMEN[region] || []) : all;
}
function classForPct(p){ return p>=100 ? 'good' : (p>=90 ? 'ok' : 'bad'); }

async function renderSalesmanKPIs(currentFilters){
  const grid = document.getElementById('salesmanKpiGrid');
  if(!grid) return;
  grid.innerHTML = '<div style="color:#666;font-size:12px;">Loading…</div>';

  const qs = new URLSearchParams({
    metric: currentFilters.metric,
    region: currentFilters.region,
    sold_to_group: currentFilters.sold_to_group,
    sold_to: currentFilters.sold_to,
    product_group: currentFilters.product_group,
    category: currentFilters.category,
    salesman: document.getElementById('salesman_name').value || 'ALL'
  }).toString();

  const kpis = await fetchJSON(`/api/salesman_kpis?${qs}`);

  grid.innerHTML = '';
  (kpis || []).forEach(k=>{
    const card = document.createElement('div');
    card.className = `kpi-card tight ${classForPct(k.pct)}`;
    card.innerHTML = `
      <div class="kpi-name">${k.name}</div>
      <div class="kpi-value" style="font-size:32px;">${(+k.pct||0).toFixed(1)}%</div>
      <div class="kpi-mini">${(+k.actual||0).toLocaleString()} / ${(+k.target||0).toLocaleString()}</div>
    `;
    grid.appendChild(card);
  });
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
}

async function drawAll(){
  await drawDailyAndCumulative();
  await drawMonthlyTotals();
  await drawMonthlyStacked();
  await renderSalesmanKPIs({
    metric: filters.metric,
    region: filters.region,
    sold_to_group: filters.sold_to_group,
    sold_to: filters.sold_to,
    product_group: filters.product_group,
    category: filters.category
  });
}

(async function start(){
  await initControls();
  [...document.querySelectorAll("#catBtns .btn")].forEach(b=>b.classList.toggle("active",b.dataset.val===filters.category));
  await drawAll();
})();
