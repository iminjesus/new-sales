from flask import Flask, request, jsonify, send_from_directory
import mysql.connector
from time import time  # cache timestamps
import os
# --------------------------- tiny cache (unchanged) ---------------------------
_KPI_CACHE = {}
def cache_get(namespace: str, key: str, ttl: int = 60):
    entry = _KPI_CACHE.get((namespace, key))
    if not entry:
        return None
    payload, ts = entry
    if time() - ts > ttl:
        try:
            del _KPI_CACHE[(namespace, key)]
        finally:
            return None
    return payload

def cache_set(namespace: str, key: str, payload):
    _KPI_CACHE[(namespace, key)] = (payload, time())
    
def parse_filters(req):
    """Uniform filter extraction."""
    return {
        "category":      (req.args.get("category") or "ALL").upper().strip(),
        "metric":        (req.args.get("metric") or "qty").lower().strip(),
        "region":        (req.args.get("region") or "ALL").strip(),
        "salesman":      (req.args.get("salesman") or "ALL").strip(),
        "sold_to_group": (req.args.get("sold_to_group") or "ALL").strip(),
        "sold_to":       (req.args.get("sold_to") or "ALL").strip(),
        "ship_to":       (req.args.get("ship_to") or "ALL").strip(),
        "product_group": (req.args.get("product_group") or "ALL").strip(),
        "pattern":       (req.args.get("pattern") or "ALL").strip(),
    }

def build_customer_filters(alias_fact: str, f, *, use_sold_to_name: bool=False):
    """
    Returns (joins, wheres, params) to apply Region/Salesman/Group/Sold_to on a fact table
    by joining customer once on equality:
        JOIN customer cus ON cus.Ship_to = <fact>.Ship_To
    If use_sold_to_name=True, 'sold_to' will match customer.Sold_to_Name instead of id.
    """
    joins = [f"JOIN customer cus ON cus.Ship_to = {alias_fact}.Ship_To"]
    wh, p = [], []

    if f["region"] != "ALL":
        wh.append("cus.Ship_to_State = %s"); p.append(f["region"])
    if f["salesman"] != "ALL":
        wh.append("UPPER(TRIM(cus.Salesman_Name)) = UPPER(TRIM(%s))"); p.append(f["salesman"])
    if f["sold_to_group"] != "ALL":
        wh.append("cus.sold_to_group = %s"); p.append(f["sold_to_group"])

    # sold_to: id (A.. / digits) or match by name via customer
    if f["sold_to"] != "ALL":
        sv = f["sold_to"]
        if not use_sold_to_name and (sv.isdigit() or sv.upper().startswith("A")):
            wh.append(f"{alias_fact}.Ship_To = %s"); p.append(sv)
        else:
            wh.append("cus.Sold_to_Name = %s"); p.append(sv)

    # explicit ship_to id filter if given
    if f["ship_to"] != "ALL":
        wh.append(f"{alias_fact}.Ship_To = %s"); p.append(f["ship_to"])

    return joins, wh, p

def build_product_filters(alias_fact: str, f):
    """
    Adds optional EXISTS against carrying_july only when needed.
    Uses equality on M_CODE + Product_Group/Pattern for index usage.
    """
    wh, p = [], []
    exists_pg = exists_pt = ""
    if f["product_group"] != "ALL":
        exists_pg = f"""
          AND EXISTS (
                SELECT 1
                  FROM carrying_july cj_pg
                 WHERE cj_pg.M_CODE = {alias_fact}.Material
                   AND cj_pg.Product_Group = %s
          )
        """
        p.append(f["product_group"])
    if f["pattern"] != "ALL":
        exists_pt = f"""
          AND EXISTS (
                SELECT 1
                  FROM carrying_july cj_pt
                 WHERE cj_pt.M_CODE = {alias_fact}.Material
                   AND cj_pt.Pattern = %s
          )
        """
        p.append(f["pattern"])
    return exists_pg + exists_pt, p

def category_filters_monthly(alias: str, category: str):
    """
    Return (joins, wheres) for monthly-schema facts (sales2025, profit).
    - alias: table alias for the fact (e.g., "s" for sales2025, "p" for profit)
    - All predicates are index-friendly (equality / LIKE prefix).
    - Add optional JOINs only when that category needs them.
    """
    joins, wh = [], []
    cat = (category or "ALL").upper()

    if cat == "ALL":
        return joins, wh

    if cat == "PCLT":
        # material codes starting with 1 or 2
        wh.append(f"({alias}.Material LIKE '1%' OR {alias}.Material LIKE '2%')")

    elif cat == "TBR":
        # example logic: material codes starting with 3 (adjust to your real rule)
        wh.append(f"{alias}.Material LIKE '3%'")

    elif cat == "ISEG":
        # ISEG mapping by Material
        # Ensure an index on iseg(Material)
        joins.append(f"JOIN iseg i ON cast(trim(i.Material) as unsigned) = {alias}.Material")

    elif cat == "SUV":
        # SUV by Pattern
        # Ensure an index on suv(Pattern)
        joins.append(f"JOIN suv suv ON suv.Pattern = {alias}.Pattern")

    elif cat == "LOWPROFILE":
        # Low profile / strategic by Material
        joins.append(f"JOIN lowprofile lp ON cast(trim(lp.Material) as unsigned) = {alias}.Material")

    elif cat == "HM":
        # HM by Sold-To (use your customer join for Ship_To ⇒ Sold_To; keep simple)
        # If your HM rule is customer-list based, prefer EXISTS against a keyed table.
        joins.append(f"JOIN HM hm ON cast(trim(hm.Sold_To) as unsigned) = {alias}.Sold_To")

    return joins, wh

    

# ------------------------------ app/bootstrap --------------------------------
app = Flask(__name__, static_folder="static")

def get_connection():
    return mysql.connector.connect(
        host="localhost",
        user="root",
        password="",
        database="my_new_database",
    )

# ---------------------------- category rules ---------------------------------


REGION_SALESMEN_PY = {
    "NSW": ["Hamid Jallis","LUTTRELL STEVE","Hulley Gary","Lee Don"],
    "QLD": ["Lopez Randall","Spires Steven","Sampson Kieren","Marsh Aaron"],
    "VIC": ["Bellotto Nicola","Bilston Kelley","Gultjaeff Jason","Hobkirk Calvin"],
    "WA" : ["Fruci Davide","Gilbert Michael"],
}

def allowed_salesmen_for_region(region: str, salesman_param: str = "ALL"):
    if region and region in REGION_SALESMEN_PY:
        base = REGION_SALESMEN_PY[region][:]
    else:
        base = sorted({n for names in REGION_SALESMEN_PY.values() for n in names})
    if salesman_param and salesman_param != "ALL":
        sel = salesman_param.strip().upper()
        baseU = {n.strip().upper() for n in base}
        return [salesman_param] if sel in baseU else []
    return base

# ------------------------------------------------------------------------------
@app.get("/")
def index():
    return send_from_directory(app.static_folder, "index.html")

def add_cond(where_list, params_list, cond, val):
    if val and val != "ALL":
        where_list.append(cond)
        params_list.append(val)

# ------------------------------- KPI SNAPSHOT --------------------------------
@app.get("/api/kpi_snapshot")
def kpi_snapshot():

    # -------- inputs
    metric   = request.args.get("metric", "qty") or "qty"
    value_yr = "Qty" if metric == "qty" else "Amt"                   # sales2025
    value_js = "billqty_in_SKU" if metric == "qty" else "net_value"  # julysales
    qa       = "Q" if metric == "qty" else "A"

    category      = request.args.get("category", "ALL") or "ALL"
    region        = request.args.get("region", "ALL") or "ALL"
    salesman      = request.args.get("salesman", "ALL") or "ALL"
    sold_to_group = request.args.get("sold_to_group", "ALL") or "ALL"
    sold_to       = request.args.get("sold_to", "ALL") or "ALL"
    product_group = request.args.get("product_group", "ALL") or "ALL"
    ship_to       = request.args.get("ship_to", "ALL") or "ALL"
    pattern       = request.args.get("pattern", "ALL") or "ALL"

    # category rules
    cat_rule_m = CATEGORY_RULES_MONTHLY.get(category, CATEGORY_RULES_MONTHLY["ALL"])
    cat_join_m  = cat_rule_m.get("join", "")
    cat_where_m = cat_rule_m.get("where", [])[:]

    cat_rule_d = CATEGORY_RULES.get(category, CATEGORY_RULES["ALL"])
    cat_join_d  = cat_rule_d.get("sales_join", "")
    cat_where_d = cat_rule_d.get("sales_where", [])[:]

    # -------- normalized mapping joins (customer)
    bm_join_yr = """
        JOIN customer cus
          ON cus.ship_to = s.ship_to
    """
    bm_join_js = """
        JOIN customer cus
          ON cus.ship_to = j.ship_to
    """

    # -------- helper filters (against customer)
    def add_common_mapping_filters(where, params, alias_cus="cus"):
        # Region (state of ship-to)
        if region and region != "ALL":
            where.append(f"{alias_cus}.bde_state = %s"); params.append(region)
        # Salesman name
        if salesman and salesman != "ALL":
            where.append(f"UPPER(TRIM({alias_cus}.Salesman_Name)) = UPPER(TRIM(%s))"); params.append(salesman)
        # Sold-to Group (sold_to_group)
        if sold_to_group and sold_to_group != "ALL":
            where.append(f"{alias_cus}.sold_to_group = %s"); params.append(sold_to_group)

    def add_sold_to_filters(where, params, alias_s, is_num_table=True):
        # sold_to can be id or name
        if sold_to and sold_to != "ALL":
            sv = sold_to.strip()
            if sv.isdigit() or sv.upper().startswith("A"):
                where.append(
                    f"REGEXP_REPLACE(UPPER(TRIM(CAST({alias_s}.Sold_To AS CHAR))), '[^A-Z0-9]', '') = "
                    f"REGEXP_REPLACE(UPPER(TRIM(%s)), '[^A-Z0-9]', '')"
                )
                params.append(sv)
            else:
                col = "Sold_To_Name" if alias_s == "s" else "sold_to_name"
                where.append(f"{alias_s}.{col} = %s"); params.append(sv)

    def add_other_filters(where, params, alias_s, has_product=True, has_pattern=True, ship_col="Ship_To_Name"):
        if product_group and product_group != "ALL" and has_product:
            col = "Product_Group" if alias_s == "s" else "product_group"   # note: 'j' has no product_group column
            where.append(f"{alias_s}.{col} = %s"); params.append(product_group)
        if ship_to and ship_to != "ALL":
            col = ship_col if alias_s == "s" else "ship_to_name"
            where.append(f"{alias_s}.{col} = %s"); params.append(ship_to)
        if pattern and pattern != "ALL" and has_pattern:
            col = "Pattern" if alias_s == "s" else "pattern"
            where.append(f"{alias_s}.{col} = %s"); params.append(pattern)

    wanted_regions = ["NSW","QLD","VIC","WA"] if (not region or region == "ALL") else [region]

    # helpers
    def months12(): return [0.0]*13  # 1..12

    actual = {}  # (region, salesman) -> month array
    target = {}

    # -------- Q1/Q2 actuals from sales2025
    wh_m, prm_m = [], []
    wh_m.append("s.Month BETWEEN 1 AND 6")
    add_common_mapping_filters(wh_m, prm_m, "cus")
    add_sold_to_filters(wh_m, prm_m, "s", is_num_table=True)
    add_other_filters(wh_m, prm_m, "s", has_product=True, has_pattern=True, ship_col="Ship_To_Name")
    wh_m.extend(cat_where_m)

    sql_m = f"""
        SELECT cus.bde_state                      AS region,
               UPPER(TRIM(cus.Salesman_Name))         AS salesman,
               s.Month                                AS mth,
               SUM(s.{value_yr})                      AS v
          FROM sales2025 s
          {bm_join_yr}
          {cat_join_m}
         WHERE {" AND ".join(wh_m)}
         GROUP BY cus.bde_State, UPPER(TRIM(cus.Salesman_Name)), s.Month
    """

    # -------- July actuals from julysales
    wh_j, prm_j = [], []
    add_common_mapping_filters(wh_j, prm_j, "cus")
    add_sold_to_filters(wh_j, prm_j, "j", is_num_table=False)
    add_other_filters(wh_j, prm_j, "j", has_product=True, has_pattern=True, ship_col="ship_to_name")
    wh_j.extend(cat_where_d)

    sql_j = f"""
        SELECT cus.bde_State              AS region,
               UPPER(TRIM(cus.Salesman_Name)) AS salesman,
               SUM(j.{value_js})              AS v
          FROM julysales j
          {bm_join_js}
          {cat_join_d}
         {"WHERE " + " AND ".join(wh_j) if wh_j else ""}
         GROUP BY cus.bde_State, UPPER(TRIM(cus.Salesman_Name))
    """

    # -------- Targets helper (by region) using customer table
    def fetch_targets_for_region(reg_filter):
        where, params = ["t.QA = %s", "t.Month BETWEEN 1 AND 7"], [qa]
        item = CATEGORY_RULES.get(category, CATEGORY_RULES["ALL"])["item"]
        if item and item != "ALL":
            where.append("t.Item = %s"); params.append(item)
        if salesman and salesman != "ALL":
            where.append("UPPER(TRIM(t.Salesman_name)) = UPPER(TRIM(%s))"); params.append(salesman)
        # Scope to region via customer
        if reg_filter and reg_filter != "ALL":
            where.append("""
              EXISTS (
                SELECT 1 FROM customer cus
                 WHERE UPPER(TRIM(cus.Salesman_Name)) = UPPER(TRIM(t.Salesman_name))
                   AND cus.bde_State = %s
              )""")
            params.append(reg_filter)
        # Scope to group via customer
        if sold_to_group and sold_to_group != "ALL":
            where.append("""
              EXISTS (
                SELECT 1 FROM customer cus2
                 WHERE UPPER(TRIM(cus2.Salesman_Name)) = UPPER(TRIM(t.Salesman_name))
                   AND cus2.sold_to_group = %s
              )""")
            params.append(sold_to_group)

        sql = f"""
          SELECT UPPER(TRIM(t.Salesman_name)) AS salesman, t.Month, SUM(t.Value) AS v
            FROM target t
           WHERE {" AND ".join(where)}
           GROUP BY UPPER(TRIM(t.Salesman_name)), t.Month
        """
        return sql, params

    # -------- run queries
    try:
        conn = get_connection()
        cur  = conn.cursor(dictionary=True)

        # Q1/Q2 actuals
        cur.execute(sql_m, tuple(prm_m))
        for r in cur.fetchall():
            reg = (r["region"] or "").strip().upper()
            bde = r["salesman"]
            if reg not in wanted_regions:
                continue
            key = (reg, bde)
            arr = actual.setdefault(key, months12())
            mth = int(r["mth"])
            if 1 <= mth <= 12:
                arr[mth] += float(r["v"] or 0)

        # July actuals
        cur.execute(sql_j, tuple(prm_j))
        for r in cur.fetchall():
            reg = (r["region"] or "").strip().upper()
            bde = r["salesman"]
            if reg not in wanted_regions:
                continue
            key = (reg, bde)
            arr = actual.setdefault(key, months12())
            arr[7] += float(r["v"] or 0)

        # Targets per region
        targets_per_region = {}
        for reg in wanted_regions:
            sql_t, prm_t = fetch_targets_for_region(reg)
            cur.execute(sql_t, tuple(prm_t))
            reg_map = {}
            for r in cur.fetchall():
                bde = r["salesman"]
                key = (reg, bde)
                arr = reg_map.setdefault(key, months12())
                mth = int(r["Month"])
                if 1 <= mth <= 12:
                    arr[mth] += float(r["v"] or 0)
            targets_per_region[reg] = reg_map
            # merge into global target
            for key, arr in reg_map.items():
                tot = target.setdefault(key, months12())
                for i in range(1, 8):
                    tot[i] += arr[i]

        cur.close(); conn.close()
    except Exception as e:
        try:
            cur.close(); conn.close()
        except Exception:
            pass
        return jsonify({"error": str(e)}), 500

    # -------- packers
    def pack(arr_a, arr_t):
        q1a = sum(arr_a[1:4]);  q1t = sum(arr_t[1:4])
        q2a = sum(arr_a[4:7]);  q2t = sum(arr_t[4:7])
        jla = arr_a[7];         jlt = arr_t[7]
        def p(a,t): return (a/t*100.0) if t>0 else 0.0
        return {
          "jul": {"a": jla, "t": jlt, "p": p(jla,jlt)},
          "q1":  {"a": q1a, "t": q1t, "p": p(q1a,q1t)},
          "q2":  {"a": q2a, "t": q2t, "p": p(q2a,q2t)},
          "q3":  {"a": jla, "t": jlt, "p": p(jla,jlt)}  # July-to-date
        }

    keys = set(actual.keys()) | set(target.keys())
    out_regions = []
    overall_a = months12()
    overall_t = months12()

    for reg in (["NSW","QLD","VIC","WA"] if (not region or region == "ALL") else [region]):
        salesmen_rows = []
        region_a = months12()
        region_t = months12()
        region_keys = [k for k in keys if k[0] == reg]

        for key in region_keys:
            a = actual.get(key, months12())
            t = target.get(key, months12())
            if sum(a[1:8]) == 0 and sum(t[1:8]) == 0:
                continue
            salesmen_rows.append({"name": key[1], **pack(a, t)})
            for i in range(1, 8):
                region_a[i] += a[i];  region_t[i] += t[i]
                overall_a[i] += a[i]; overall_t[i] += t[i]

        if sum(region_a[1:8]) == 0 and sum(region_t[1:8]) == 0 and (region not in (None, "", "ALL")):
            continue

        out_regions.append({"region": reg, "kpi": pack(region_a, region_t), "salesmen": salesmen_rows})

    return jsonify({"overall": pack(overall_a, overall_t), "regions": out_regions})

# ----------------------------- Daily Sales ---------------------------------
@app.get("/api/daily_sales")
def daily_sales():
    f = parse_filters(request)
    value = "Qty" if f["metric"] == "qty" else "Amt"

    joins, wh, params = build_customer_filters("s", f, use_sold_to_name=False)

    # category
    cat_joins, cat_where = category_filters_monthly("s", f["category"])
    joins += cat_joins
    wh    += cat_where

    # direct fields (indexable)
    if f["product_group"] != "ALL":
        wh.append("s.Product_Group = %s"); params.append(f["product_group"])
    if f["pattern"] != "ALL":
        wh.append("s.Pattern = %s"); params.append(f["pattern"])

    where_sql = ("WHERE " + " AND ".join(wh)) if wh else ""
    sql = f"""
      SELECT s.Day AS day_num, SUM(s.{value}) AS daily_total
        FROM sales2510 s
        {' '.join(joins)}
        {where_sql}
       GROUP BY s.Day
       ORDER BY s.Day
    """

    conn = get_connection(); cur = conn.cursor(dictionary=True)
    try:
        cur.execute(sql, tuple(params))
        rows = cur.fetchall()
    finally:
        cur.close(); conn.close()

    day_map = {int(r["day_num"]): float(r["daily_total"] or 0) for r in rows}
    return jsonify([{"day": m, "value": day_map.get(m, 0)} for m in range(1, 31)])    


# -------------------- Daily breakdown (stacked by group) -------------------
@app.get("/api/daily_breakdown")
def daily_breakdown():

    f = parse_filters(request)
    value = "Qty" if f["metric"] == "qty" else "Amt"

    # Which dimension to group by?
    group_by = (request.args.get("group_by") or "region").strip()
    group_cols = {
        "product_group": "s.Product_Group",
        "region":        "cus.Ship_to_State",
        "salesman":      "cus.Salesman_Name",
        "sold_to_group": "cus.sold_to_group",
        "sold_to":       "cus.Sold_to_Name",
        "pattern":       "s.Pattern",
    }
    if group_by not in group_cols:
        return jsonify({"error": "invalid group_by"}), 400
    group_col = group_cols[group_by]

    # Optional Top-N on sold_to (only if not already filtering a single sold_to)
    top_only = str(request.args.get("top_only", "0")).lower() in ("1", "true", "yes")
    try:
        top_n = int(request.args.get("top_n", 10))
    except Exception:
        top_n = 10
    apply_top = top_only and group_by == "sold_to" and (f["sold_to"] in ("", "ALL"))

    # ---- Build base JOINs / WHEREs (consistent helpers) ----
    joins, wh, params = build_customer_filters("s", f, use_sold_to_name=False)
    cat_joins, cat_where = category_filters_monthly("s", f["category"])
    joins += cat_joins
    wh    += cat_where

    # Direct, index-friendly filters that live on sales2025
    if f["product_group"] != "ALL":
        wh.append("s.Product_Group = %s"); params.append(f["product_group"])
    if f["pattern"] != "ALL":
        wh.append("s.Pattern = %s");       params.append(f["pattern"])

    where_sql = ("WHERE " + " AND ".join(wh)) if wh else ""

    # ---- Optional Top-N CTE for sold_to ----
    top_cte = ""
    top_join = ""
    top_params: list = []
    if apply_top:
        # Reuse the same joins/where for a ranking by total 2025 value
        top_cte = f"""
          WITH top_sold AS (
            SELECT {group_col} AS sold_nm, SUM(s.{value}) AS tot
              FROM sales2510 s
              {' '.join(joins)}
              {where_sql}
             GROUP BY {group_col}
             ORDER BY tot DESC
             LIMIT %s
          )
        """
        top_join = "JOIN top_sold ts ON ts.sold_nm = " + group_col
        top_params = [top_n]

    # ---- Final query (monthly breakdown) ----
    sql = f"""
      {top_cte}
      SELECT s.Day AS day,
             {group_col} AS group_label,
             SUM(s.{value}) AS value
        FROM sales2510 s
        {' '.join(joins)}
        {top_join}
        {where_sql}
       GROUP BY s.Day, {group_col}
       ORDER BY s.Day
    """

    try:
        conn = get_connection(); cur = conn.cursor(dictionary=True)
        cur.execute(sql, tuple(top_params + params))
        rows = cur.fetchall()
    finally:
        try: cur.close(); conn.close()
        except: pass

    return jsonify(rows)
# ----------------------------- Monthly Sales ---------------------------------
@app.get("/api/monthly_sales")
def monthly_sales():
    f = parse_filters(request)
    value = "Qty" if f["metric"] == "qty" else "Amt"

    joins, wh, params = build_customer_filters("s", f, use_sold_to_name=False)

    # category
    cat_joins, cat_where = category_filters_monthly("s", f["category"])
    joins += cat_joins
    wh    += cat_where

    # direct fields (indexable)
    if f["product_group"] != "ALL":
        wh.append("s.Product_Group = %s"); params.append(f["product_group"])
    if f["pattern"] != "ALL":
        wh.append("s.Pattern = %s"); params.append(f["pattern"])

    where_sql = ("WHERE " + " AND ".join(wh)) if wh else ""
    sql = f"""
      SELECT s.Month AS month_num, SUM(s.{value}) AS monthly_total
        FROM sales2025 s
        {' '.join(joins)}
        {where_sql}
       GROUP BY s.Month
       ORDER BY s.Month
    """

    conn = get_connection(); cur = conn.cursor(dictionary=True)
    try:
        cur.execute(sql, tuple(params))
        rows = cur.fetchall()
    finally:
        cur.close(); conn.close()

    month_map = {int(r["month_num"]): float(r["monthly_total"] or 0) for r in rows}
    return jsonify([{"month": m, "value": month_map.get(m, 0)} for m in range(1, 13)])

# ----------------------------- Monthly Target --------------------------------
@app.get("/api/monthly_target")
def monthly_target():

    metric   = (request.args.get("metric", "qty") or "qty").lower().strip()
    qa       = "Q" if metric == "qty" else "A"

    category = (request.args.get("category", "ALL") or "ALL").upper()
    item     = CATEGORY_RULES.get(category, CATEGORY_RULES["ALL"])["item"]

    region        = (request.args.get("region", "ALL") or "ALL").strip()
    sold_to_group = (request.args.get("sold_to_group", "ALL") or "ALL").strip()
    sold_to       = (request.args.get("sold_to", "ALL") or "ALL").strip()
    salesman      = (request.args.get("salesman", "ALL") or "ALL").strip()

    where, params = ["t.QA = %s"], [qa]

    if item != "ALL":
        where.append("t.Item = %s")
        params.append(item)

    if salesman and salesman != "ALL":
        where.append("UPPER(TRIM(t.Salesman_name)) = UPPER(TRIM(%s))")
        params.append(salesman)

    # Region filter via bde_mapping
    if region and region != "ALL":
        where.append("""
          EXISTS (
            SELECT 1
              FROM customer cus
             WHERE UPPER(TRIM(cus.salesman_name)) = UPPER(TRIM(t.Salesman_name))
               AND cus.bde_state = %s
          )
        """)
        params.append(region)


    sql = f"""
      SELECT t.Month AS month_num, SUM(t.Value) AS monthly_target
        FROM target t
       WHERE {' AND '.join(where)}
       GROUP BY t.Month
       ORDER BY t.Month
    """

    try:
        conn = get_connection()
        cur  = conn.cursor(dictionary=True)
        cur.execute(sql, tuple(params))
        rows = cur.fetchall()
    except Exception as e:
        try:
            cur.close(); conn.close()
        except Exception:
            pass
        return jsonify({"error": str(e)}), 500
    finally:
        try:
            cur.close(); conn.close()
        except:
            pass

    month_map = {int(r["month_num"]): float(r["monthly_target"] or 0) for r in rows}
    return jsonify([{"month": m, "value": month_map.get(m, 0)} for m in range(1, 13)])


# -------------------- Monthly breakdown (stacked by group) -------------------
@app.get("/api/monthly_breakdown")
def monthly_breakdown():

    f = parse_filters(request)
    value = "Qty" if f["metric"] == "qty" else "Amt"

    # Which dimension to group by?
    group_by = (request.args.get("group_by") or "region").strip()
    group_cols = {
        "product_group": "s.Product_Group",
        "region":        "cus.Ship_to_State",
        "salesman":      "cus.Salesman_Name",
        "sold_to_group": "cus.sold_to_group",
        "sold_to":       "cus.Sold_to_Name",
        "pattern":       "s.Pattern",
    }
    if group_by not in group_cols:
        return jsonify({"error": "invalid group_by"}), 400
    group_col = group_cols[group_by]

    # Optional Top-N on sold_to (only if not already filtering a single sold_to)
    top_only = str(request.args.get("top_only", "0")).lower() in ("1", "true", "yes")
    try:
        top_n = int(request.args.get("top_n", 10))
    except Exception:
        top_n = 10
    apply_top = top_only and group_by == "sold_to" and (f["sold_to"] in ("", "ALL"))

    # ---- Build base JOINs / WHEREs (consistent helpers) ----
    joins, wh, params = build_customer_filters("s", f, use_sold_to_name=False)
    cat_joins, cat_where = category_filters_monthly("s", f["category"])
    joins += cat_joins
    wh    += cat_where

    # Direct, index-friendly filters that live on sales2025
    if f["product_group"] != "ALL":
        wh.append("s.Product_Group = %s"); params.append(f["product_group"])
    if f["pattern"] != "ALL":
        wh.append("s.Pattern = %s");       params.append(f["pattern"])

    where_sql = ("WHERE " + " AND ".join(wh)) if wh else ""

    # ---- Optional Top-N CTE for sold_to ----
    top_cte = ""
    top_join = ""
    top_params: list = []
    if apply_top:
        # Reuse the same joins/where for a ranking by total 2025 value
        top_cte = f"""
          WITH top_sold AS (
            SELECT {group_col} AS sold_nm, SUM(s.{value}) AS tot
              FROM sales2025 s
              {' '.join(joins)}
              {where_sql}
             GROUP BY {group_col}
             ORDER BY tot DESC
             LIMIT %s
          )
        """
        top_join = "JOIN top_sold ts ON ts.sold_nm = " + group_col
        top_params = [top_n]

    # ---- Final query (monthly breakdown) ----
    sql = f"""
      {top_cte}
      SELECT s.Month AS month,
             {group_col} AS group_label,
             SUM(s.{value}) AS value
        FROM sales2025 s
        {' '.join(joins)}
        {top_join}
        {where_sql}
       GROUP BY s.Month, {group_col}
       ORDER BY s.Month
    """

    try:
        conn = get_connection(); cur = conn.cursor(dictionary=True)
        cur.execute(sql, tuple(top_params + params))
        rows = cur.fetchall()
    finally:
        try: cur.close(); conn.close()
        except: pass

    return jsonify(rows)

# ----------------------------- Yearly Sales ---------------------------------
@app.get("/api/yearly_sales")
def yearly_sales():
    f = parse_filters(request)
    value = "Qty" if f["metric"] == "qty" else "Amt"

    joins, wh, params = build_customer_filters("s", f, use_sold_to_name=False)

    # category
    cat_joins, cat_where = category_filters_monthly("s", f["category"])
    joins += cat_joins
    wh    += cat_where

    # direct fields (indexable)
    if f["product_group"] != "ALL":
        wh.append("s.Product_Group = %s"); params.append(f["product_group"])
    if f["pattern"] != "ALL":
        wh.append("s.Pattern = %s"); params.append(f["pattern"])

    where_sql = ("WHERE " + " AND ".join(wh)) if wh else ""
    sql = f"""
      SELECT s.Year AS year_num, SUM(s.{value}) AS yearly_total
        FROM sales2124 s
        {' '.join(joins)}
        {where_sql}
       GROUP BY s.Year
       ORDER BY s.Year
    """

    conn = get_connection(); cur = conn.cursor(dictionary=True)
    try:
        cur.execute(sql, tuple(params))
        rows = cur.fetchall()
    finally:
        cur.close(); conn.close()

    year_map = {int(r["year_num"]): float(r["yearly_total"] or 0) for r in rows}
    return jsonify([{"year": m, "value": year_map.get(m, 0)} for m in range(2021, 2025)])    


# -------------------- yearly breakdown (stacked by group) -------------------
@app.get("/api/yearly_breakdown")
def yearly_breakdown():

    f = parse_filters(request)
    value = "Qty" if f["metric"] == "qty" else "Amt"

    # Which dimension to group by?
    group_by = (request.args.get("group_by") or "region").strip()
    group_cols = {
        "product_group": "s.Product_Group",
        "region":        "cus.Ship_to_State",
        "salesman":      "cus.Salesman_Name",
        "sold_to_group": "cus.sold_to_group",
        "sold_to":       "cus.Sold_to_Name",
        "pattern":       "s.Pattern",
    }
    if group_by not in group_cols:
        return jsonify({"error": "invalid group_by"}), 400
    group_col = group_cols[group_by]

    # Optional Top-N on sold_to (only if not already filtering a single sold_to)
    top_only = str(request.args.get("top_only", "0")).lower() in ("1", "true", "yes")
    try:
        top_n = int(request.args.get("top_n", 10))
    except Exception:
        top_n = 10
    apply_top = top_only and group_by == "sold_to" and (f["sold_to"] in ("", "ALL"))

    # ---- Build base JOINs / WHEREs (consistent helpers) ----
    joins, wh, params = build_customer_filters("s", f, use_sold_to_name=False)
    cat_joins, cat_where = category_filters_monthly("s", f["category"])
    joins += cat_joins
    wh    += cat_where

    # Direct, index-friendly filters that live on sales2025
    if f["product_group"] != "ALL":
        wh.append("s.Product_Group = %s"); params.append(f["product_group"])
    if f["pattern"] != "ALL":
        wh.append("s.Pattern = %s");       params.append(f["pattern"])

    where_sql = ("WHERE " + " AND ".join(wh)) if wh else ""

    # ---- Optional Top-N CTE for sold_to ----
    top_cte = ""
    top_join = ""
    top_params: list = []
    if apply_top:
        # Reuse the same joins/where for a ranking by total 2025 value
        top_cte = f"""
          WITH top_sold AS (
            SELECT {group_col} AS sold_nm, SUM(s.{value}) AS tot
              FROM sales2124 s
              {' '.join(joins)}
              {where_sql}
             GROUP BY {group_col}
             ORDER BY tot DESC
             LIMIT %s
          )
        """
        top_join = "JOIN top_sold ts ON ts.sold_nm = " + group_col
        top_params = [top_n]

    # ---- Final query (monthly breakdown) ----
    sql = f"""
      {top_cte}
      SELECT s.Year AS year,
             {group_col} AS group_label,
             SUM(s.{value}) AS value
        FROM sales2124 s
        {' '.join(joins)}
        {top_join}
        {where_sql}
       GROUP BY s.Year, {group_col}
       ORDER BY s.Year
    """

    try:
        conn = get_connection(); cur = conn.cursor(dictionary=True)
        cur.execute(sql, tuple(top_params + params))
        rows = cur.fetchall()
    finally:
        try: cur.close(); conn.close()
        except: pass

    return jsonify(rows)

# ---------------------- lookups used by the UI (optional) --------------------
@app.get("/api/sold_to_groups")
def sold_to_groups():
    try:
        conn = get_connection(); cur = conn.cursor()
        cur.execute("""
            SELECT DISTINCT TRIM(sold_to_group)
            FROM customer
            WHERE sold_to_group IS NOT NULL AND TRIM(sold_to_group) <> ''
            ORDER BY TRIM(sold_to_group)
        """)
        groups = [r[0] for r in cur.fetchall()]
        cur.close(); conn.close()
        return jsonify(groups)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.get("/api/sold_to_names")
def sold_to_names():
    # expect ?sold_to_group=ACM (same name as above)
    parent = request.args.get("sold_to_group", "ALL")
    try:
        conn = get_connection(); cur = conn.cursor()
        if parent != "ALL":
            cur.execute("""
                SELECT DISTINCT TRIM(sold_to_name)
                FROM customer
                WHERE sold_to_group = %s
                  AND sold_to_name IS NOT NULL
                  AND TRIM(sold_to_name) <> ''
                ORDER BY TRIM(sold_to_name)
            """, (parent,))
        else:
            cur.execute("""
                SELECT DISTINCT TRIM(sold_to_name)
                FROM customer
                WHERE sold_to_name IS NOT NULL
                  AND TRIM(sold_to_name) <> ''
                ORDER BY TRIM(sold_to_name)
            """)
        names = [r[0] for r in cur.fetchall()]
        cur.close(); conn.close()
        return jsonify(names)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.get("/api/ship_to_names")
def ship_to_names():
    # parent (big group)
    stg3    = (request.args.get("sold_to_group") or "ALL").strip()
    # child (sold-to name that user picked)
    sold_to = (request.args.get("sold_to") or "ALL").strip()

    try:
        conn = get_connection(); cur = conn.cursor()

        where = ["ship_to_name IS NOT NULL", "TRIM(ship_to_name) <> ''"]
        params = []

        # 1) if user picked a specific sold_to_name → use that
        if sold_to.upper() != "ALL":
            where.append("TRIM(sold_to_name) = %s")
            params.append(sold_to)
        # 2) otherwise, if user picked a group → use that
        elif stg3.upper() != "ALL":
            where.append("TRIM(sold_to_group) = %s")
            params.append(stg3)

        where_sql = "WHERE " + " AND ".join(where)

        cur.execute(f"""
            SELECT DISTINCT TRIM(ship_to_name)
            FROM customer
            {where_sql}
            ORDER BY TRIM(ship_to_name)
        """, tuple(params))

        names = [r[0] for r in cur.fetchall()]
        cur.close(); conn.close()
        return jsonify(names)

    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.get("/api/product_group")
def product_group():
    try:
        conn = get_connection(); cur = conn.cursor()
        cur.execute("SELECT DISTINCT product_group FROM carrying_july")
        groups = sorted(r[0] for r in cur.fetchall())
        cur.close(); conn.close()
        return jsonify(groups)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.get("/api/top_customers_2025")
def top_customers_2025():
    metric = (request.args.get("metric", "qty") or "qty").lower().strip()
    value_field = "Qty" if metric == "qty" else "Amt"
    region        = request.args.get("region", "ALL")
    salesman      = request.args.get("salesman", "ALL")
    sold_to_group = request.args.get("sold_to_group", "ALL")
    product_group = request.args.get("product_group", "ALL")
    category      = (request.args.get("category", "ALL") or "ALL").upper()
    top_n         = int(request.args.get("n", "10") or 10)
    ship_to       = request.args.get("ship_to", "ALL")
    pattern       = request.args.get("pattern", "ALL")

    cat_rule  = CATEGORY_RULES_MONTHLY.get(category, CATEGORY_RULES_MONTHLY["ALL"])
    cat_join  = cat_rule.get("join", "")
    cat_where = cat_rule.get("where", [])[:]

    where, params = [], []
    bm_join = """
        JOIN bde_mapping bm
          ON REGEXP_REPLACE(TRIM(CAST(bm.Ship_To AS CHAR)), '[^0-9A-Za-z]', '') =
             REGEXP_REPLACE(TRIM(CAST(s.Ship_To AS CHAR)),  '[^0-9A-Za-z]', '')
    """
    if region and region != "ALL":        where.append("bm.State = %s");        params.append(region)
    if salesman and salesman != "ALL":    where.append("UPPER(TRIM(bm.BDE)) = UPPER(TRIM(%s))"); params.append(salesman)
    if sold_to_group and sold_to_group != "ALL": where.append("bm.STG3 = %s"); params.append(sold_to_group)
    if product_group and product_group != "ALL": where.append("s.Product_Group = %s"); params.append(product_group)
    if ship_to and ship_to != "ALL":      where.append("s.ship_to_name = %s");   params.append(ship_to)
    if pattern and pattern != "ALL":      where.append("s.Pattern = %s");        params.append(pattern)
    where.extend(cat_where)
    where_sql = ("WHERE " + " AND ".join(where)) if where else ""

    sql = f"""
        SELECT s.Sold_To_Name AS sold_to_name, SUM(s.{value_field}) AS total
        FROM sales2025 s
        {bm_join}
        {cat_join}
        {where_sql}
        GROUP BY s.Sold_To_Name
        ORDER BY total DESC
        LIMIT %s
    """
    try:
        conn = get_connection(); cur = conn.cursor(dictionary=True)
        cur.execute(sql, tuple(params + [top_n]))
        rows = cur.fetchall()
        cur.close(); conn.close()
        return jsonify(rows)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    
@app.get("/api/patterns")
def patterns():
    product_group = request.args.get("product_group", "ALL")
    try:
        conn = get_connection(); cur = conn.cursor()
        if product_group and product_group != "ALL":
            cur.execute("""
                SELECT DISTINCT TRIM(Pattern)
                FROM sales2025
                WHERE Product_Group = %s
                ORDER BY TRIM(Pattern)
            """, (product_group,))
        else:
            cur.execute("""
                SELECT DISTINCT TRIM(Pattern)
                FROM sales2025
                ORDER BY TRIM(Pattern)
            """)
        names = [r[0] for r in cur.fetchall()]
        cur.close(); conn.close()
        return jsonify(names)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# -------------------------- Region KPIs (summary) ----------------------------
@app.get("/api/region_kpis")
def region_kpis():
    metric        = (request.args.get("metric", "qty") or "qty").lower().strip()
    qa            = "Q" if metric == "qty" else "A"
    value_js      = "billqty_in_SKU" if metric == "qty" else "net_value"

    category      = (request.args.get("category", "ALL") or "ALL").upper()
    region_arg    = request.args.get("region", "ALL")
    salesman      = request.args.get("salesman", "ALL")
    sold_to_group = request.args.get("sold_to_group", "ALL")
    sold_to       = request.args.get("sold_to", "ALL")
    product_group = request.args.get("product_group", "ALL")

    regions = ["NSW","QLD","VIC","WA"]
    if region_arg and region_arg != "ALL":
        regions = [region_arg]

    rule         = CATEGORY_RULES.get(category, CATEGORY_RULES["ALL"])
    cat_join_js  = rule.get("sales_join", "")
    cat_where_js = rule.get("sales_where", [])[:]
    target_item  = rule.get("item", "Overall Sales")

    bm_join_js = """
        JOIN bde_mapping bm
          ON REGEXP_REPLACE(TRIM(CAST(bm.Ship_To AS CHAR)), '[^0-9A-Za-z]', '') =
             REGEXP_REPLACE(TRIM(CAST(j.ship_to  AS CHAR)), '[^0-9A-Za-z]', '')
    """

    act_where, act_params = [], []
    act_where.append("UPPER(TRIM(bm.State)) IN (" + ",".join(["%s"]*len(regions)) + ")")
    act_params.extend(regions)
    if salesman and salesman != "ALL":    act_where.append("UPPER(TRIM(bm.BDE)) = UPPER(TRIM(%s))"); act_params.append(salesman)
    if sold_to_group and sold_to_group != "ALL": act_where.append("bm.STG3 = %s"); act_params.append(sold_to_group)
    if sold_to and sold_to != "ALL":      act_where.append("j.sold_to_name = %s"); act_params.append(sold_to)
    if product_group and product_group != "ALL": act_where.append("c.product_group = %s"); act_params.append(product_group)
    act_where.extend(cat_where_js)
    act_where_sql = ("WHERE " + " AND ".join(act_where)) if act_where else ""

    bde_state_where, bde_state_params = [], []
    bde_state_where.append("UPPER(TRIM(State)) IN (" + ",".join(["%s"]*len(regions)) + ")")
    bde_state_params.extend(regions)
    if sold_to_group and sold_to_group != "ALL": bde_state_where.append("STG3 = %s"); bde_state_params.append(sold_to_group)
    if sold_to and sold_to != "ALL":             bde_state_where.append("TRIM(Sold_To) = TRIM(%s)"); bde_state_params.append(sold_to)

    tgt_where, tgt_params = ["t.QA = %s", "t.Month = 7"], [qa]
    if target_item and target_item != "ALL": tgt_where.append("t.Item = %s"); tgt_params.append(target_item)
    if salesman and salesman != "ALL":       tgt_where.append("UPPER(TRIM(t.Salesman)) = UPPER(TRIM(%s))"); tgt_params.append(salesman)
    tgt_where_sql = " AND ".join(tgt_where)

    sql_actuals = f"""
        SELECT UPPER(TRIM(bm.State)) AS region,
               MAX(DAY(j.billing_date)) AS last_day,
               SUM(j.{value_js}) AS actual
        FROM julysales j
        JOIN carrying_july c ON j.material = c.M_CODE
        {bm_join_js}
        {cat_join_js}
        {act_where_sql}
        GROUP BY UPPER(TRIM(bm.State))
    """
    sql_targets = f"""
        WITH bde_state AS (
          SELECT DISTINCT UPPER(TRIM(BDE)) AS BDE, UPPER(TRIM(State)) AS State
          FROM bde_mapping
          WHERE {" AND ".join(bde_state_where)}
        )
        SELECT bs.State AS region, SUM(t.Value) AS month_target
        FROM target t
        JOIN bde_state bs ON bs.BDE = UPPER(TRIM(t.Salesman))
        WHERE {tgt_where_sql}
        GROUP BY bs.State
    """

    try:
        conn = get_connection()
        cur  = conn.cursor(dictionary=True)

        cur.execute(sql_actuals, tuple(act_params))
        act_rows = cur.fetchall()
        act_map = { (r["region"] or "").upper(): {
            "actual": float(r["actual"] or 0),
            "last_day": int(r["last_day"] or 0),
        } for r in act_rows }

        cur.execute(sql_targets, tuple(bde_state_params + tgt_params))
        tgt_rows = cur.fetchall()
        tgt_map = { (r["region"] or "").upper(): float(r["month_target"] or 0) for r in tgt_rows }

        cur.close(); conn.close()

        out = []
        for reg in regions:
            a = act_map.get(reg, {"actual": 0, "last_day": 0})
            month_target = tgt_map.get(reg, 0)
            mtd_target = (month_target / 31) * (a["last_day"] or 0)
            pct = round((a["actual"] / mtd_target) * 100.0, 0) if mtd_target > 0 else 0
            out.append({
                "region": reg,
                "actual": round(a["actual"], 0),
                "target": round(mtd_target, 0),
                "pct": pct
            })
        return jsonify(out)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.get("/api/profit_monthly")
def profit_monthly():
    f = parse_filters(request)

    joins, wh, params = build_customer_filters("p", f, use_sold_to_name=False)
    cat_joins, cat_where = category_filters_monthly("p", f["category"])
    joins += cat_joins
    wh    += cat_where

    # optional Product_Group / Pattern via EXISTS on carrying only if set
    exists_sql, exists_params = build_product_filters("p", f)

    where_sql = ("WHERE " + " AND ".join(wh)) if wh else ""
    sql = f"""
      SELECT CAST(p.Month AS UNSIGNED) AS month,
             SUM(p.Gross)           AS gross,
             SUM(p.Sales_Deduction) AS sd,
             SUM(p.COGS)            AS cogs,
             SUM(p.Op_Cost)         AS op_cost
        FROM profit p
        {' '.join(joins)}
        {where_sql}
        {exists_sql}
       GROUP BY CAST(p.Month AS UNSIGNED)
       ORDER BY CAST(p.Month AS UNSIGNED)
    """

    conn = get_connection(); cur = conn.cursor(dictionary=True)
    try:
        cur.execute(sql, tuple(params + exists_params))
        rows = cur.fetchall()
    finally:
        cur.close(); conn.close()

    out = [dict(month=m, gross=0, sd=0, cogs=0, op_cost=0) for m in range(1,13)]
    for r in rows:
        i = max(1, min(12, int(r["month"]))) - 1
        out[i].update({
          "gross": float(r["gross"] or 0),
          "sd":    float(r["sd"] or 0),
          "cogs":  float(r["cogs"] or 0),
          "op_cost": float(r["op_cost"] or 0)
        })
    return jsonify(out)

# ------------------------------ Quarterly KPI --------------------------------
@app.get("/api/quarterly_achievement")
def quarterly_achievement():
    metric   = (request.args.get("metric", "qty") or "qty").lower().strip()
    value_yr = "Qty" if metric == "qty" else "Amt"
    value_js = "billqty_in_SKU" if metric == "qty" else "net_value"
    qa       = "Q" if metric == "qty" else "A"

    category      = (request.args.get("category", "ALL") or "ALL").upper()
    region        = request.args.get("region", "ALL")
    salesman      = request.args.get("salesman", "ALL")
    sold_to_group = request.args.get("sold_to_group", "ALL")
    sold_to       = request.args.get("sold_to", "ALL")
    product_group = request.args.get("product_group", "ALL")
    ship_to       = request.args.get("ship_to", "ALL")
    pattern       = request.args.get("pattern", "ALL")
    as_of         = (request.args.get("as_of", "today") or "today").lower()

    cat_rule_m = CATEGORY_RULES_MONTHLY.get(category, CATEGORY_RULES_MONTHLY["ALL"])
    cat_join_m  = cat_rule_m.get("join", "")
    cat_where_m = cat_rule_m.get("where", [])[:]

    cat_rule_d = CATEGORY_RULES.get(category, CATEGORY_RULES["ALL"])
    cat_join_d  = cat_rule_d.get("sales_join", "")
    cat_where_d = cat_rule_d.get("sales_where", [])[:]

    bm_join_yr = """
        JOIN bde_mapping bm
          ON REGEXP_REPLACE(TRIM(CAST(bm.Ship_To AS CHAR)), '[^0-9A-Za-z]', '') =
             REGEXP_REPLACE(TRIM(CAST(s.Ship_To  AS CHAR)), '[^0-9A-Za-z]', '')
    """
    bm_join_js = """
        JOIN bde_mapping bm
          ON REGEXP_REPLACE(TRIM(CAST(bm.Ship_To AS CHAR)), '[^0-9A-Za-z]', '') =
             REGEXP_REPLACE(TRIM(CAST(j.ship_to  AS CHAR)), '[^0-9A-Za-z]', '')
    """

    wh_yr, params_yr = [], []
    if region and region != "ALL":        wh_yr.append("bm.State = %s"),        params_yr.append(region)
    if salesman and salesman != "ALL":    wh_yr.append("UPPER(TRIM(bm.BDE)) = UPPER(TRIM(%s))"), params_yr.append(salesman)
    if sold_to_group and sold_to_group != "ALL": wh_yr.append("bm.STG3 = %s"), params_yr.append(sold_to_group)
    if sold_to and sold_to != "ALL":
        if sold_to.isdigit() or sold_to.upper().startswith("A"):
            wh_yr.append(
                "REGEXP_REPLACE(UPPER(TRIM(CAST(s.Sold_To AS CHAR))), '[^A-Z0-9]', '') = "
                "REGEXP_REPLACE(UPPER(TRIM(%s)), '[^A-Z0-9]', '')"
            ); params_yr.append(sold_to)
        else:
            wh_yr.append("s.Sold_To_Name = %s"), params_yr.append(sold_to)
    if product_group and product_group != "ALL": wh_yr.append("s.Product_Group = %s"), params_yr.append(product_group)
    if ship_to and ship_to != "ALL":             wh_yr.append("s.Sold_To_Name = %s"), params_yr.append(ship_to)
    if pattern and pattern != "ALL":             wh_yr.append("s.Pattern = %s"),      params_yr.append(pattern)
    wh_yr.extend(cat_where_m)
    where_sql_yr = ("WHERE " + " AND ".join(wh_yr)) if wh_yr else ""

    wh_js, params_js = [], []
    if region and region != "ALL":        wh_js.append("bm.State = %s"),        params_js.append(region)
    if salesman and salesman != "ALL":    wh_js.append("UPPER(TRIM(bm.BDE)) = UPPER(TRIM(%s))"), params_js.append(salesman)
    if sold_to_group and sold_to_group != "ALL": wh_js.append("bm.STG3 = %s"), params_js.append(sold_to_group)
    if sold_to and sold_to != "ALL":      wh_js.append("j.sold_to_name = %s"),  params_js.append(sold_to)
    if product_group and product_group != "ALL": wh_js.append("c.product_group = %s"), params_js.append(product_group)
    if ship_to and ship_to != "ALL":      wh_js.append("j.ship_to_name = %s"),  params_js.append(ship_to)
    if pattern and pattern != "ALL":      wh_js.append("j.Pattern = %s"),       params_js.append(pattern)
    wh_js.extend(cat_where_d)
    where_sql_js = ("WHERE " + " AND ".join(wh_js)) if wh_js else ""

    def build_target_where(months):
        tw, tp = ["t.QA = %s", f"t.Month IN ({','.join(['%s']*len(months))})"], [qa, *months]
        item = CATEGORY_RULES.get(category, CATEGORY_RULES["ALL"])["item"]
        if item != "ALL": tw.append("t.Item = %s"); tp.append(item)
        if salesman and salesman != "ALL":
            tw.append("UPPER(TRIM(t.Salesman)) = UPPER(TRIM(%s))"); tp.append(salesman)
        if region and region != "ALL":
            tw.append("""
                EXISTS (SELECT 1 FROM bde_mapping bm_r
                        WHERE UPPER(TRIM(bm_r.BDE)) = UPPER(TRIM(t.Salesman))
                          AND bm_r.State = %s)"""); tp.append(region)
        if sold_to_group and sold_to_group != "ALL":
            tw.append("""
                EXISTS (SELECT 1 FROM bde_mapping bm_g
                        WHERE UPPER(TRIM(bm_g.BDE)) = UPPER(TRIM(t.Salesman))
                          AND bm_g.STG3 = %s)"""); tp.append(sold_to_group)
        return " AND ".join(tw), tp

    sql_q1_actual = f"""
        SELECT COALESCE(SUM(s.{value_yr}),0) AS v
        FROM sales2025 s
        {bm_join_yr}
        {cat_join_m}
        {where_sql_yr}
          AND s.Month IN (1,2,3)
    """
    sql_q2_actual = f"""
        SELECT COALESCE(SUM(s.{value_yr}),0) AS v
        FROM sales2025 s
        {bm_join_yr}
        {cat_join_m}
        {where_sql_yr}
          AND s.Month IN (4,5,6)
    """
    sql_july_actual = f"""
        SELECT COALESCE(SUM(j.{value_js}),0) AS v,
               COALESCE(MAX(DAY(j.billing_date)),0) AS last_day
        FROM julysales j
        JOIN carrying_july c ON j.material = c.M_CODE
        {bm_join_js}
        {cat_join_d}
        {where_sql_js}
    """

    w_q1, p_q1 = build_target_where([1,2,3])
    sql_q1_target = f"SELECT COALESCE(SUM(t.Value),0) AS v FROM target t WHERE {w_q1}"
    w_q2, p_q2 = build_target_where([4,5,6])
    sql_q2_target = f"SELECT COALESCE(SUM(t.Value),0) AS v FROM target t WHERE {w_q2}"
    w_july, p_july = build_target_where([7])
    sql_july_target = f"SELECT COALESCE(SUM(t.Value),0) AS v FROM target t WHERE {w_july}"

    try:
        conn = get_connection()
        cur  = conn.cursor(dictionary=True)
        cur.execute(sql_q1_actual, tuple(params_yr)); q1_actual = float(cur.fetchone()["v"] or 0)
        cur.execute(sql_q2_actual, tuple(params_yr)); q2_actual = float(cur.fetchone()["v"] or 0)
        cur.execute(sql_july_actual, tuple(params_js)); row_j = cur.fetchone() or {"v": 0, "last_day": 0}
        july_actual = float(row_j["v"] or 0); last_day = int(row_j["last_day"] or 0)
        cur.execute(sql_q1_target, tuple(p_q1)); q1_target = float((cur.fetchone() or {"v":0})["v"] or 0)
        cur.execute(sql_q2_target, tuple(p_q2)); q2_target = float((cur.fetchone() or {"v":0})["v"] or 0)
        cur.execute(sql_july_target, tuple(p_july)); july_target_full = float((cur.fetchone() or {"v":0})["v"] or 0)
        cur.close(); conn.close()
    except Exception as e:
        try:
            cur.close(); conn.close()
        except Exception:
            pass
        return jsonify({"error": str(e)}), 500

    if as_of in ("today","now","current"):
        from datetime import date
        as_of_days = min(date.today().day, 31)
    else:
        as_of_days = max(min(last_day, 31), 0)

    july_target_scaled = (july_target_full / 31) * as_of_days if july_target_full else 0
    def pct(a, t): return round((a / t * 100.0), 0) if t else 0

    q1 = {"actual": round(q1_actual, 0), "target": round(q1_target, 0), "pct": pct(q1_actual, q1_target)}
    q2 = {"actual": round(q2_actual, 0), "target": round(q2_target, 0), "pct": pct(q2_actual, q2_target)}
    q3_to_date = {
        "actual": round(july_actual, 0),
        "target": round(july_target_scaled, 0),
        "pct": pct(july_actual, july_target_scaled),
        "july_target_full": round(july_target_full, 0),
        "as_of_days": as_of_days
    }
    return jsonify({"as_of": as_of, "q1": q1, "q2": q2, "q3_to_date": q3_to_date})


    metric = (request.args.get("metric", "qty") or "qty").lower().strip()
    value_field = "Qty" if metric == "qty" else "Amt"
    category      = (request.args.get("category", "ALL") or "ALL").upper()
    region        = request.args.get("region", "ALL")
    salesman      = request.args.get("salesman", "ALL")
    sold_to_group = request.args.get("sold_to_group", "ALL")
    sold_to       = request.args.get("sold_to", "ALL")
    product_group = request.args.get("product_group", "ALL")
    group_by      = request.args.get("group_by", "region")
    ship_to       = request.args.get("ship_to", "ALL")
    pattern       = request.args.get("pattern", "ALL")
    top_only = request.args.get("top_only", "0") in ("1", "true", "True")
    try:
        top_n = int(request.args.get("top_n", "10") or 10)
    except Exception:
        top_n = 10

    group_cols = {
        "product_group": "s.Product_Group",
        "region":        "bm.State",
        "salesman":      "bm.BDE",
        "sold_to_group": "bm.STG3",
        "sold_to":       "s.Sold_To_Name",
    }
    if group_by not in group_cols:
        return jsonify([])

    group_col = group_cols[group_by]
    cat_rule  = CATEGORY_RULES_MONTHLY.get(category, CATEGORY_RULES_MONTHLY["ALL"])
    cat_join  = cat_rule.get("join", "")
    cat_where = cat_rule.get("where", [])[:]

    join_bde = """
        LEFT JOIN bde_mapping bm
               ON REGEXP_REPLACE(UPPER(TRIM(CAST(bm.Ship_To AS CHAR))), '[^A-Z0-9]', '') =
                  REGEXP_REPLACE(UPPER(TRIM(CAST(s.Sold_To  AS CHAR))), '[^A-Z0-9]', '')
    """

    where, params = [], []
    where.extend(cat_where)
    if region and region != "ALL":        where.append("bm.State = %s");        params.append(region)
    if salesman and salesman != "ALL":    where.append("UPPER(TRIM(bm.BDE)) = UPPER(TRIM(%s))"); params.append(salesman)
    if sold_to_group and sold_to_group != "ALL": where.append("bm.STG3 = %s");  params.append(sold_to_group)
    if sold_to and sold_to != "ALL":
        if sold_to.isdigit() or sold_to.upper().startswith("A"):
            where.append(
                "REGEXP_REPLACE(UPPER(TRIM(CAST(s.Sold_To AS CHAR))), '[^A-Z0-9]', '') = "
                "REGEXP_REPLACE(UPPER(TRIM(%s)), '[^A-Z0-9]', '')"
            ); params.append(sold_to)
        else:
            where.append("s.Sold_To_Name = %s"); params.append(sold_to)
    if product_group and product_group != "ALL": where.append("s.Product_Group = %s");  params.append(product_group)
    if ship_to and ship_to != "ALL":      where.append("j.ship_to_name = %s");   params.append(ship_to)
    if pattern and pattern != "ALL":      where.append("j.Pattern = %s");        params.append(pattern)
    where_sql = ("WHERE " + " AND ".join(where)) if where else ""

    top_cte = ""
    top_join = ""
    top_params: list = []
    if top_only and group_by == "sold_to" and (sold_to in (None, "", "ALL")):
        top_cte = f"""
          WITH top_sold AS (
            SELECT s.Sold_To_Name
            FROM sales21to24 s
            {join_bde}
            {cat_join}
            {where_sql}
            GROUP BY s.Sold_To_Name
            ORDER BY SUM(s.{value_field}) DESC
            LIMIT %s
          )
        """
        top_join = "JOIN top_sold ts ON ts.Sold_To_Name = s.Sold_To_Name"
        top_params = [top_n]

    sql = f"""
        {top_cte}
        SELECT
            s.`Year` AS year,
            COALESCE({group_col}, 'COMMON') AS group_label,
            SUM(s.{value_field}) AS value
        FROM sales21to24 s
        {join_bde}
        {cat_join}
        {top_join}
        {where_sql}
        GROUP BY s.`Year`, {group_col}
        ORDER BY s.`Year`
    """
    try:
        conn = get_connection()
        cur = conn.cursor(dictionary=True)
        cur.execute(sql, tuple(top_params + params))
        rows = cur.fetchall()
        cur.close(); conn.close()
        return jsonify(rows)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ------------------------------------------------------------------------------
if __name__ == "__main__":
    port = int(os.getenv("PORT", 5000))   # Cloudtype probes 5000
    app.run(host="0.0.0.0", port=port, debug=False)