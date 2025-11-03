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
