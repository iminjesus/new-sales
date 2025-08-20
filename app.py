from flask import Flask, request, jsonify, send_from_directory
import mysql.connector

app = Flask(__name__, static_folder="static")


def get_connection():
    return mysql.connector.connect(
        host="localhost",
        user="root",
        password="",
        database="my_new_database",
    )

CATEGORY_RULES = {
    # Sales filter                    # Target filter (t.item)
    "ALL":        {"sales_join": "",                                  "sales_where": [],                                  "item": "Overall Sales"},
    "PCLT":       {"sales_join": "",                                  "sales_where": ["j.Line NOT LIKE '%TBR%'"],         "item": "1-1 PCLT(Amt)"},
    "TBR":        {"sales_join": "",                                  "sales_where": ["j.Line LIKE '%TBR%'"],             "item": "1-2 TBR (Amount)"},
    "18PLUS":     {"sales_join": "",                                  "sales_where": ["j.Line NOT LIKE '%TBR%'", "j.Inch >= 18.0"], "item": "2-1 18+ PCLT Sales (Quantity)"},
    "ISEG":       {"sales_join": "JOIN iseg i ON CAST(TRIM(i.Material) AS UNSIGNED) = j.material",
                   "sales_where": [],                                  "item": "2-2 EV.ISEG(Quantity)"},
    "SUV":        {"sales_join": "JOIN suv suv ON suv.Pattern = j.Pattern",
                   "sales_where": [],                                  "item": "2-3 SUV (Quantity)"},
    "LOWPROFILE": {"sales_join": "JOIN lowprofile lp ON CAST(TRIM(lp.Material) AS UNSIGNED) = j.material",
                   "sales_where": [],                                  "item": "2-4 Low Profile & Strategic TBR Sales"},
    "HM": {
            "sales_join": (
                "JOIN HM hm ON "
                "REGEXP_REPLACE(UPPER(TRIM(CAST(hm.Sold_To AS CHAR))), '[^A-Z0-9]', '') = "
                "REGEXP_REPLACE(UPPER(TRIM(CAST(j.Sold_To AS CHAR))), '[^A-Z0-9]', '')"
            ),
            "sales_where": [],
            "item": "3-1 HM Sales (Amount)"
            },
}
# Reuse your existing CATEGORY_RULES for daily/july (j,c)
# and add monthly versions that reference s,c,cust for sales2025
CATEGORY_RULES_MONTHLY = {
    "ALL":        {"join": "", "where": [], "item": "Overall Sales"},
    "PCLT":       {"join": "", "where": ["s.Line NOT LIKE '%TBR%'"],                        "item": "1-1 PCLT(Amt)"},
    "TBR":        {"join": "", "where": ["s.Line LIKE '%TBR%'"],                            "item": "1-2 TBR (Amount)"},
    "18PLUS":     {"join": "", "where": ["s.Line NOT LIKE '%TBR%'", "s.Inch >= 18.0"],      "item": "2-1 18+ PCLT Sales (Quantity)"},
    "ISEG":       {"join": "JOIN iseg i ON CAST(TRIM(i.Material) AS UNSIGNED) = s.Material",
                   "where": [],                                                              "item": "2-2 EV.ISEG(Quantity)"},
    "SUV":        {"join": "JOIN suv suv ON suv.Pattern = s.Pattern",
                   "where": [],                                                              "item": "2-3 SUV (Quantity)"},
    "LOWPROFILE": {"join": "JOIN lowprofile lp ON CAST(TRIM(lp.Material) AS UNSIGNED) = s.Material",
                   "where": [],                                                              "item": "2-4 Low Profile & Strategic TBR Sales"},
    "HM":         {"join": (
                      "JOIN HM hm ON "
                      "REGEXP_REPLACE(UPPER(TRIM(CAST(hm.Sold_To AS CHAR))), '[^A-Z0-9]', '') = "
                      "REGEXP_REPLACE(UPPER(TRIM(CAST(s.Sold_To   AS CHAR))), '[^A-Z0-9]', '')"
                   ), "where": [],                                                           "item": "3-1 HM Sales (Amount)"},
}

# --- Frontend REGION_SALESMEN mirrored on the backend ---
REGION_SALESMEN_PY = {
    "NSW": ["Hamid Jallis","LUTTRELL STEVE","Hulley Gary","Lee Don"],
    "QLD": ["Lopez Randall","Spires Steven","Sampson Kieren","Marsh Aaron"],
    "VIC": ["Bellotto Nicola","Bilston Kelley","Gultjaeff Jason","Hobkirk Calvin"],
    "WA" : ["Fruci Davide","Gilbert Michael"],
}

def allowed_salesmen_for_region(region: str, salesman_param: str = "ALL"):
    # Union of all if region == ALL/blank
    if region and region in REGION_SALESMEN_PY:
        base = REGION_SALESMEN_PY[region][:]
    else:
        base = sorted({n for names in REGION_SALESMEN_PY.values() for n in names})

    # If UI selected a specific salesman, intersect with whitelist
    if salesman_param and salesman_param != "ALL":
        sel = salesman_param.strip().upper()
        baseU = {n.strip().upper() for n in base}
        return [salesman_param] if sel in baseU else []

    return base

@app.get("/")
def index():
    return send_from_directory(app.static_folder, "index.html")
# --- Helper: add filter if not ALL ---
def add_cond(where_list, params_list, cond, val):
    if val and val != "ALL":
        where_list.append(cond)
        params_list.append(val)

@app.get("/api/monthly_sales")
def monthly_sales():
    metric   = (request.args.get("metric", "qty") or "qty").lower().strip()
    value    = "Qty" if metric == "qty" else "Amt"

    # UI filters
    region        = request.args.get("region", "ALL")
    salesman      = request.args.get("salesman", "ALL")          # bde_mapping.BDE
    sold_to_group = request.args.get("sold_to_group", "ALL")     # bde_mapping.STG3
    sold_to       = request.args.get("sold_to", "ALL")           # can be ID or Name
    product_group = request.args.get("product_group", "ALL")     # sales2025.Product_Group
    category      = (request.args.get("category", "ALL") or "ALL").upper()

    # --- Category rules (use the monthly rules so we can JOIN other tables) ---
    cat_rule = CATEGORY_RULES_MONTHLY.get(category, CATEGORY_RULES_MONTHLY["ALL"])
    cat_join  = cat_rule.get("join", "")
    cat_where = cat_rule.get("where", [])[:]  # copy

    # Build WHERE from UI filters
    wh, params = [], []

    # bde_mapping-driven filters (join via normalized Ship_To)
    if region and region != "ALL":
        wh.append("bm.State = %s"); params.append(region)

    if salesman and salesman != "ALL":
        wh.append("UPPER(TRIM(bm.BDE)) = UPPER(TRIM(%s))"); params.append(salesman)

    if sold_to_group and sold_to_group != "ALL":
        wh.append("bm.STG3 = %s"); params.append(sold_to_group)

    # Sold-to can be numeric ID or name
    if sold_to and sold_to != "ALL":
        # numeric ID (or IDs like A000123) via normalized match
        if sold_to.isdigit() or sold_to.upper().startswith("A"):
            wh.append(
                "REGEXP_REPLACE(UPPER(TRIM(CAST(s.Sold_To AS CHAR))), '[^A-Z0-9]', '') = "
                "REGEXP_REPLACE(UPPER(TRIM(%s)), '[^A-Z0-9]', '')"
            )
            params.append(sold_to)
        else:
            wh.append("s.Sold_To_Name = %s"); params.append(sold_to)

    if product_group and product_group != "ALL":
        wh.append("s.Product_Group = %s"); params.append(product_group)

    # Add category WHERE parts
    wh.extend(cat_where)

    where_sql = ("WHERE " + " AND ".join(wh)) if wh else ""

    # Normalized join to bde_mapping
    bm_join = """
        JOIN bde_mapping bm
          ON REGEXP_REPLACE(TRIM(CAST(bm.Ship_To AS CHAR)), '[^0-9A-Za-z]', '') =
             REGEXP_REPLACE(TRIM(CAST(s.Ship_To AS CHAR)),  '[^0-9A-Za-z]', '')
    """

    sql = f"""
        SELECT s.Month AS month_num, SUM(s.{value}) AS monthly_total
        FROM sales2025 s
        {bm_join}
        {cat_join}
        {where_sql}
        GROUP BY s.Month
        ORDER BY s.Month
    """

    try:
        conn = get_connection()
        cur = conn.cursor(dictionary=True)
        cur.execute(sql, tuple(params))
        rows = cur.fetchall()
        cur.close(); conn.close()

        month_map = {int(r["month_num"]): float(r["monthly_total"] or 0) for r in rows}
        return jsonify([{"month": m, "value": month_map.get(m, 0.0)} for m in range(1, 12+1)])
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.get("/api/monthly_target")
def monthly_target():
    metric   = (request.args.get("metric", "qty") or "qty").lower().strip()
    qa       = "Q" if metric == "qty" else "A"

    category      = (request.args.get("category", "ALL") or "ALL").upper()
    salesman      = request.args.get("salesman", "ALL")
    # new filters from the UI that should map via bde_mapping
    region        = request.args.get("region", "ALL")           # NSW / QLD / VIC / WA
    sold_to_group = request.args.get("sold_to_group", "ALL")    # maps to bde_mapping.STG3

    # Category -> Item mapping used in `target`
    item = CATEGORY_RULES.get(category, CATEGORY_RULES["ALL"])["item"]

    where  = ["t.QA = %s", "t.Month BETWEEN 1 AND 12"]
    params = [qa]

    if item != "ALL":
        where.append("t.Item = %s")
        params.append(item)

    if salesman and salesman != "ALL":
        where.append("UPPER(TRIM(t.Salesman)) = UPPER(TRIM(%s))")
        params.append(salesman)

    # Filter target rows by BDE mapping without multiplying rows:
    # Count a target row only if its salesman exists in mapping for the chosen State / STG3.
    if region and region != "ALL":
        where.append("""
            EXISTS (
              SELECT 1
              FROM bde_mapping bm
              WHERE UPPER(TRIM(bm.BDE)) = UPPER(TRIM(t.Salesman))
                AND bm.State = %s
            )
        """)
        params.append(region)

    if sold_to_group and sold_to_group != "ALL":
        where.append("""
            EXISTS (
              SELECT 1
              FROM bde_mapping bm2
              WHERE UPPER(TRIM(bm2.BDE)) = UPPER(TRIM(t.Salesman))
                AND bm2.STG3 = %s
            )
        """)
        params.append(sold_to_group)

    sql = f"""
        SELECT t.Month AS month_num, SUM(t.Value) AS monthly_target
        FROM target t
        WHERE {' AND '.join(where)}
        GROUP BY t.Month
        ORDER BY t.Month
    """

    try:
        conn = get_connection(); cur = conn.cursor(dictionary=True)
        cur.execute(sql, tuple(params))
        rows = cur.fetchall()
        cur.close(); conn.close()

        month_map = {int(r["month_num"]): float(r["monthly_target"] or 0) for r in rows}
        return jsonify([{"month": m, "value": month_map.get(m, 0.0)} for m in range(1, 13)])
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.get("/api/monthly_sales_breakdown")
def monthly_sales_breakdown():
    metric = (request.args.get("metric", "qty") or "qty").lower().strip()
    value_field = "Qty" if metric == "qty" else "Amt"

    category      = (request.args.get("category", "ALL") or "ALL").upper()
    region        = request.args.get("region", "ALL")
    salesman      = request.args.get("salesman", "ALL")
    sold_to_group = request.args.get("sold_to_group", "ALL")
    sold_to       = request.args.get("sold_to", "ALL")
    product_group = request.args.get("product_group", "ALL")
    group_by      = request.args.get("group_by", "region")

    top_only = request.args.get("top_only", "0") == "1"
    top_n    = int(request.args.get("top_n", "10") or 10)

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

    cat_wheres = []
    if category == "PCLT":
        cat_wheres.append("s.Line NOT LIKE '%TBR%'")
    elif category == "TBR":
        cat_wheres.append("s.Line LIKE '%TBR%'")
    elif category == "18PLUS":
        cat_wheres.append("s.Line NOT LIKE '%TBR%'")
        cat_wheres.append("s.Inch >= 18.0")

    join_bde = """
        LEFT JOIN bde_mapping bm
               ON REGEXP_REPLACE(UPPER(TRIM(CAST(bm.Ship_To AS CHAR))), '[^A-Z0-9]', '') =
                  REGEXP_REPLACE(UPPER(TRIM(CAST(s.Ship_To  AS CHAR))), '[^A-Z0-9]', '')
    """

    where, params = [], []
    where.extend(cat_wheres)

    if region and region != "ALL":
        where.append("bm.State = %s");        params.append(region)
    if salesman and salesman != "ALL":
        where.append("bm.BDE = %s");          params.append(salesman)
    if sold_to_group and sold_to_group != "ALL":
        where.append("bm.STG3 = %s");         params.append(sold_to_group)
    if sold_to and sold_to != "ALL":
        where.append("s.Sold_To_Name = %s");  params.append(sold_to)
    if product_group and product_group != "ALL":
        where.append("s.Product_Group = %s"); params.append(product_group)

    where_sql = ("WHERE " + " AND ".join(where)) if where else ""

    # If we’re grouping by sold_to and asked for Top-N only, restrict rows using a CTE
    top_cte = ""
    top_join = ""
    top_params: list = []
    if top_only and group_by == "sold_to":
        top_cte = f"""
          WITH top_sold AS (
            SELECT s.Sold_To_Name
            FROM sales2025 s
            {join_bde}
            {where_sql}
            GROUP BY s.Sold_To_Name
            ORDER BY SUM(s.{value_field}) DESC
            LIMIT %s
          )
        """
        top_join = "JOIN top_sold ts ON UPPER(TRIM(ts.Sold_To_Name)) = UPPER(TRIM(s.Sold_To_Name))"
        top_params = [top_n]

    sql = f"""
        {top_cte}
        SELECT
            s.Month AS month,
            COALESCE({group_col}, 'COMMON') AS group_label,
            SUM(s.{value_field}) AS value
        FROM sales2025 s
        {join_bde}
        {top_join}
        {where_sql}
        GROUP BY s.Month, group_label
        ORDER BY s.Month
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


# --- Targets (daily) ---
@app.get("/api/july_target")
def july_target():
    metric   = (request.args.get("metric", "qty") or "qty").lower().strip()
    qa       = "Q" if metric == "qty" else "A"

    category      = (request.args.get("category", "ALL") or "ALL").upper()
    item          = CATEGORY_RULES.get(category, CATEGORY_RULES["ALL"])["item"]

    # UI filters
    region        = request.args.get("region", "ALL")           # e.g. NSW/QLD/VIC/WA (maps to bde_mapping.State or Region—pick one)
    sold_to_group = request.args.get("sold_to_group", "ALL")    # maps to bde_mapping.STG3
    sold_to       = request.args.get("sold_to", "ALL")          # maps to bde_mapping.Sold_To (ID)
    salesman      = request.args.get("salesman", "ALL")         # matches t.Salesman in target

    where  = ["t.QA = %s", "t.Month = 7"]
    params = [qa]

    if item != "ALL":
        where.append("t.Item = %s")
        params.append(item)

    if salesman and salesman != "ALL":
        # normalize to avoid minor spacing/case differences
        where.append("UPPER(TRIM(t.Salesman)) = UPPER(TRIM(%s))")
        params.append(salesman)

    # --- constrain by BDE mapping without multiplying rows ---
    # Region/State
    if region and region != "ALL":
        where.append("""
            EXISTS (
              SELECT 1
              FROM bde_mapping bm_r
              WHERE UPPER(TRIM(bm_r.BDE)) = UPPER(TRIM(t.Salesman))
                AND bm_r.State = %s
            )
        """)
        params.append(region)

    # Sold-to Group (STG3)
    if sold_to_group and sold_to_group != "ALL":
        where.append("""
            EXISTS (
              SELECT 1
              FROM bde_mapping bm_g
              WHERE UPPER(TRIM(bm_g.BDE)) = UPPER(TRIM(t.Salesman))
                AND bm_g.STG3 = %s
            )
        """)
        params.append(sold_to_group)

    # Specific Sold_To (ID). If your UI passes a name instead, switch to bm_xxx.Sold_To_Name.
    if sold_to and sold_to != "ALL":
        where.append("""
            EXISTS (
              SELECT 1
              FROM bde_mapping bm_s
              WHERE UPPER(TRIM(bm_s.BDE)) = UPPER(TRIM(t.Salesman))
                AND TRIM(bm_s.Sold_To) = TRIM(%s)
            )
        """)
        params.append(sold_to)

    sql = f"""
        SELECT SUM(t.Value) AS mv
        FROM target t
        WHERE {' AND '.join(where)}
    """

    try:
        conn = get_connection(); cur = conn.cursor()
        cur.execute(sql, tuple(params))
        mv = cur.fetchone()[0] or 0
        cur.close(); conn.close()

        # July has 31 days
        daily_target = round(float(mv)/31, 2) if mv else 0.0
        return jsonify({"daily_target": daily_target, "metric": metric, "qa": qa, "item": item})
    except Exception as e:
        return jsonify({"error": str(e)}), 500



# --- Sales trend (daily + shares) ---
@app.get("/api/sku_trend")
def sku_trend():
    metric = (request.args.get("metric", "qty") or "qty").lower().strip()
    value_field_js = "billqty_in_SKU" if metric == "qty" else "net_value"
    value_field_yr = "Qty" if metric == "qty" else "Amt"  # for sales2025 top-N calc

    # UI filters
    region        = request.args.get("region", "ALL")
    salesman      = request.args.get("salesman", "ALL")
    sold_to_group = request.args.get("sold_to_group", "ALL")
    sold_to       = request.args.get("sold_to", "ALL")
    product_group = request.args.get("product_group", "ALL")
    group_by      = request.args.get("group_by", "product_group")
    category      = (request.args.get("category", "ALL") or "ALL").upper()

    # Top-N flags (frontend sends these when you click “Top 10 (2025)”)
    top_only = str(request.args.get("top_only", "0")).lower() in ("1", "true", "yes")
    try:
        top_n = int(request.args.get("top_n", 10))
    except Exception:
        top_n = 10
    # Only meaningful when grouping by sold_to and NOT already filtering a single sold_to
    apply_top = top_only and group_by == "sold_to" and (not sold_to or sold_to == "ALL")

    # bde_mapping-based grouping
    group_cols = {
        "product_group": "c.product_group",
        "region":        "bm.State",
        "salesman":      "bm.BDE",
        "sold_to_group": "bm.STG3",
        "sold_to":       "j.sold_to_name",
    }
    if group_by not in group_cols:
        return jsonify({"error": "invalid group_by"}), 400
    group_col = group_cols[group_by]

    # Normalized mapping join for julysales
    bm_join_js = """
        JOIN bde_mapping bm
          ON REGEXP_REPLACE(TRIM(CAST(bm.Ship_To AS CHAR)), '[^0-9A-Za-z]', '') =
             REGEXP_REPLACE(TRIM(CAST(j.ship_to  AS CHAR)), '[^0-9A-Za-z]', '')
    """

    # Category rules (daily)
    rule = CATEGORY_RULES.get(category, CATEGORY_RULES["ALL"])
    extra_join_js  = rule["sales_join"]                 # joins like iseg/lowprofile/HM
    extra_where_js = rule["sales_where"][:]             # copy

    # WHERE for julysales (daily)
    where_js, params_js = [], []
    if region and region != "ALL":
        where_js.append("bm.State = %s");          params_js.append(region)
    if salesman and salesman != "ALL":
        where_js.append("UPPER(TRIM(bm.BDE)) = UPPER(TRIM(%s))"); params_js.append(salesman)
    if sold_to_group and sold_to_group != "ALL":
        where_js.append("bm.STG3 = %s");           params_js.append(sold_to_group)
    if sold_to and sold_to != "ALL":
        where_js.append("j.sold_to_name = %s");    params_js.append(sold_to)
    if product_group and product_group != "ALL":
        where_js.append("c.product_group = %s");   params_js.append(product_group)
    where_js.extend(extra_where_js)
    where_sql_js = ("WHERE " + " AND ".join(where_js)) if where_js else ""

    # ---------- Optional Top-N (2025) CTE over sales2025 ----------
    # We reuse the monthly category rules so we can join to helper tables with 's.*'
    top_cte_sql = ""
    top_cte_params: list = []
    top_join_js = ""  # join into the main SELECT when top-N is active

    if apply_top:
        cat_rule_monthly = CATEGORY_RULES_MONTHLY.get(category, CATEGORY_RULES_MONTHLY["ALL"])
        top_join_monthly = cat_rule_monthly.get("join", "")
        top_where_m, top_params_m = [], []

        # Mapping join for sales2025
        bm_join_m = """
            JOIN bde_mapping bm2
              ON REGEXP_REPLACE(TRIM(CAST(bm2.Ship_To AS CHAR)), '[^0-9A-Za-z]', '') =
                 REGEXP_REPLACE(TRIM(CAST(s.Ship_To  AS CHAR)),  '[^0-9A-Za-z]', '')
        """

        if region and region != "ALL":
            top_where_m.append("bm2.State = %s");        top_params_m.append(region)
        if salesman and salesman != "ALL":
            top_where_m.append("UPPER(TRIM(bm2.BDE)) = UPPER(TRIM(%s))"); top_params_m.append(salesman)
        if sold_to_group and sold_to_group != "ALL":
            top_where_m.append("bm2.STG3 = %s");         top_params_m.append(sold_to_group)
        if product_group and product_group != "ALL":
            top_where_m.append("s.Product_Group = %s");  top_params_m.append(product_group)
        top_where_m.extend(cat_rule_monthly.get("where", []))
        top_where_sql_m = ("WHERE " + " AND ".join(top_where_m)) if top_where_m else ""

        # Build CTE that finds top-N Sold_to_Name based on 2025 totals
        top_cte_sql = f"""
            top_sold AS (
              SELECT s.Sold_To_Name AS sold_to_name
              FROM sales2025 s
              {bm_join_m}
              {top_join_monthly}
              {top_where_sql_m}
              GROUP BY s.Sold_To_Name
              ORDER BY SUM(s.{value_field_yr}) DESC
              LIMIT %s
            ),
        """
        top_cte_params = top_params_m + [top_n]
        top_join_js = "JOIN top_sold ts ON UPPER(TRIM(ts.sold_to_name)) = UPPER(TRIM(j.sold_to_name))"

    # ---------- Final query ----------
    sql = f"""
        WITH
        {top_cte_sql}
        daily_totals AS (
            SELECT j.billing_date, SUM(j.{value_field_js}) AS total_value
            FROM julysales j
            JOIN carrying_july c ON j.material = c.M_CODE
            {bm_join_js}
            {extra_join_js}
            {where_sql_js}
            GROUP BY j.billing_date
        )
        SELECT
            DATE_FORMAT(j.billing_date, '%d-%m-%y') AS billing_date,
            {group_col} AS group_label,
            SUM(j.{value_field_js}) AS daily_value,
            t.total_value,
            ROUND(SUM(j.{value_field_js}) / NULLIF(t.total_value, 0) * 100, 2) AS percentage
        FROM julysales j
        JOIN carrying_july c ON j.material = c.M_CODE
        {bm_join_js}
        {extra_join_js}
        {top_join_js}
        JOIN daily_totals t ON j.billing_date = t.billing_date
        {where_sql_js}
        GROUP BY j.billing_date, {group_col}
        ORDER BY STR_TO_DATE(DATE_FORMAT(j.billing_date, '%d-%m-%y'), '%d-%m-%y')
    """

    try:
        conn = get_connection()
        cur = conn.cursor(dictionary=True)

        # Parameter order must match placeholder order in SQL:
        # [top_cte_params] + [params_js for daily_totals] + [params_js for outer SELECT]
        all_params = tuple(top_cte_params + params_js + params_js)
        cur.execute(sql, all_params)
        rows = cur.fetchall()
        cur.close(); conn.close()
        return jsonify(rows)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.get("/api/sold_to_groups")
def sold_to_groups():
    """
    Return distinct Sold-to Group (STG3) values for the dropdown.
    Prefer bde_mapping.STG3 (cleaner), fall back to julysales if you want.
    """
    try:
        conn = get_connection()
        cur = conn.cursor()

        # Use bde_mapping; comment this and uncomment the julysales block if you prefer that source.
        cur.execute("""
            SELECT DISTINCT TRIM(STG3)
            FROM bde_mapping
            WHERE STG3 IS NOT NULL AND TRIM(STG3) <> ''
            ORDER BY TRIM(STG3)
        """)

        # If you’d rather use julysales:
        # cur.execute("""
        #     SELECT DISTINCT TRIM(sold_to_grp3)
        #     FROM julysales
        #     WHERE sold_to_grp3 IS NOT NULL AND TRIM(sold_to_grp3) <> ''
        #     ORDER BY TRIM(sold_to_grp3)
        # """)

        groups = [r[0] for r in cur.fetchall()]
        cur.close(); conn.close()

        return jsonify(groups)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ---------- Lookups used by the UI ----------
@app.get("/api/sold_to_names")
def sold_to_names():
    group = request.args.get("sold_to_group", "ALL")
    try:
        conn = get_connection(); cur = conn.cursor()
        if group != "ALL":
            cur.execute("SELECT DISTINCT sold_to_name FROM julysales WHERE sold_to_grp3 = %s", (group,))
        else:
            cur.execute("SELECT DISTINCT sold_to_name FROM julysales")
        names = sorted(r[0] for r in cur.fetchall())
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

    # UI filters
    region        = request.args.get("region", "ALL")
    salesman      = request.args.get("salesman", "ALL")
    sold_to_group = request.args.get("sold_to_group", "ALL")
    product_group = request.args.get("product_group", "ALL")
    category      = (request.args.get("category", "ALL") or "ALL").upper()
    top_n         = int(request.args.get("n", "10") or 10)

    # Monthly category rules (same schema as sales2025)
    cat_rule  = CATEGORY_RULES_MONTHLY.get(category, CATEGORY_RULES_MONTHLY["ALL"])
    cat_join  = cat_rule.get("join", "")
    cat_where = cat_rule.get("where", [])[:]

    where, params = [], []

    # bde_mapping normalized join
    bm_join = """
        JOIN bde_mapping bm
          ON REGEXP_REPLACE(TRIM(CAST(bm.Ship_To AS CHAR)), '[^0-9A-Za-z]', '') =
             REGEXP_REPLACE(TRIM(CAST(s.Ship_To AS CHAR)),  '[^0-9A-Za-z]', '')
    """

    if region and region != "ALL":
        where.append("bm.State = %s");        params.append(region)
    if salesman and salesman != "ALL":
        where.append("UPPER(TRIM(bm.BDE)) = UPPER(TRIM(%s))"); params.append(salesman)
    if sold_to_group and sold_to_group != "ALL":
        where.append("bm.STG3 = %s");         params.append(sold_to_group)
    if product_group and product_group != "ALL":
        where.append("s.Product_Group = %s"); params.append(product_group)

    where.extend(cat_where)
    where_sql = ("WHERE " + " AND ".join(where)) if where else ""

    sql = f"""
        SELECT
          s.Sold_To_Name AS sold_to_name,
          SUM(s.{value_field}) AS total
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

@app.get("/api/salesman_kpis")
def salesman_kpis():
    """
    KPI for ONLY the salesmen in REGION_SALESMEN_PY (by region).
    Still respects other filters; computed in one fast query.
    """
    metric   = (request.args.get("metric", "qty") or "qty").lower().strip()
    qa       = "Q" if metric == "qty" else "A"
    value_js = "billqty_in_SKU" if metric == "qty" else "net_value"

    category      = (request.args.get("category", "ALL") or "ALL").upper()
    region        = request.args.get("region", "ALL")
    salesman_f    = request.args.get("salesman", "ALL")
    sold_to_group = request.args.get("sold_to_group", "ALL")
    sold_to       = request.args.get("sold_to", "ALL")
    product_group = request.args.get("product_group", "ALL")

    # Mapping for targets + joins/where for sales
    item_rule   = CATEGORY_RULES.get(category, CATEGORY_RULES["ALL"])
    target_item = item_rule["item"]
    cat_join_js  = item_rule.get("sales_join", "")
    cat_where_js = item_rule.get("sales_where", [])[:]  # copy

    # --- Whitelist the salesmen by region (and optional single salesman filter) ---
    allowed = allowed_salesmen_for_region(region, salesman_f)
    # nothing allowed? return empty list quickly
    if not allowed:
        return jsonify([])

    # Build a VALUES-equivalent list (as UNION ALL) for a "salesmen" CTE
    salesmen_cte = " UNION ALL ".join(["SELECT %s AS salesman"] + ["SELECT %s"] * (len(allowed) - 1))
    salesmen_params = [n.strip().upper() for n in allowed]  # normalize to match UPPER(TRIM(...)) use

    # --- ACTUALS (julysales) WHERE ---
    act_where, act_params = [], []
    if region and region != "ALL":
        act_where.append("bm.State = %s");          act_params.append(region)
    if sold_to_group and sold_to_group != "ALL":
        act_where.append("bm.STG3 = %s");           act_params.append(sold_to_group)
    if sold_to and sold_to != "ALL":
        act_where.append("j.sold_to_name = %s");    act_params.append(sold_to)
    if product_group and product_group != "ALL":
        act_where.append("c.product_group = %s");   act_params.append(product_group)
    # category filters
    act_where.extend(cat_where_js)
    # Only for allowed salesmen
    act_where.append("UPPER(TRIM(bm.BDE)) IN (" + ",".join(["%s"] * len(salesmen_params)) + ")")
    act_params.extend(salesmen_params)
    act_where_sql = ("WHERE " + " AND ".join(act_where)) if act_where else ""

    # --- TARGETS WHERE (month=7; constrain by mapping filters via EXISTS as before) ---
    tgt_where, tgt_params = ["t.QA = %s", "t.Month = 7"], [qa]
    if target_item != "ALL":
        tgt_where.append("t.Item = %s"); tgt_params.append(target_item)
    # Only for allowed salesmen
    tgt_where.append("UPPER(TRIM(t.Salesman)) IN (" + ",".join(["%s"] * len(salesmen_params)) + ")")
    tgt_params.extend(salesmen_params)

    if region and region != "ALL":
        tgt_where.append("""
            EXISTS (
              SELECT 1 FROM bde_mapping bm_r
              WHERE UPPER(TRIM(bm_r.BDE)) = UPPER(TRIM(t.Salesman))
                AND bm_r.State = %s
            )
        """); tgt_params.append(region)
    if sold_to_group and sold_to_group != "ALL":
        tgt_where.append("""
            EXISTS (
              SELECT 1 FROM bde_mapping bm_g
              WHERE UPPER(TRIM(bm_g.BDE)) = UPPER(TRIM(t.Salesman))
                AND bm_g.STG3 = %s
            )
        """); tgt_params.append(sold_to_group)
    if sold_to and sold_to != "ALL":
        tgt_where.append("""
            EXISTS (
              SELECT 1 FROM bde_mapping bm_s
              WHERE UPPER(TRIM(bm_s.BDE)) = UPPER(TRIM(t.Salesman))
                AND TRIM(bm_s.Sold_To) = TRIM(%s)
            )
        """); tgt_params.append(sold_to)
    tgt_where_sql = " AND ".join(tgt_where)

    # Normalized mapping join for actuals
    bm_join_js = """
        JOIN bde_mapping bm
          ON REGEXP_REPLACE(TRIM(CAST(bm.Ship_To AS CHAR)), '[^0-9A-Za-z]', '') =
             REGEXP_REPLACE(TRIM(CAST(j.ship_to  AS CHAR)), '[^0-9A-Za-z]', '')
    """

    # Single SQL: whitelist -> actuals per salesman -> targets per salesman -> final KPI
    sql = f"""
        WITH salesmen AS (
          {salesmen_cte}
        ),
        actuals AS (
          SELECT
            UPPER(TRIM(bm.BDE)) AS salesman,
            MAX(DAY(j.billing_date)) AS last_day,
            SUM(j.{value_js}) AS actual
          FROM julysales j
          JOIN carrying_july c ON j.material = c.M_CODE
          {bm_join_js}
          {cat_join_js}
          {act_where_sql}
          GROUP BY UPPER(TRIM(bm.BDE))
        ),
        targets AS (
          SELECT
            UPPER(TRIM(t.Salesman)) AS salesman,
            SUM(t.Value) AS month_target
          FROM target t
          WHERE {tgt_where_sql}
          GROUP BY UPPER(TRIM(t.Salesman))
        )
        SELECT
          s.salesman AS name,
          COALESCE(a.actual,0) AS actual,
          COALESCE(ROUND((COALESCE(t.month_target,0)/31.0) * COALESCE(a.last_day,0), 2), 0) AS target,
          CASE
            WHEN COALESCE(t.month_target,0)=0 OR COALESCE(a.last_day,0)=0 THEN 0
            ELSE ROUND( (COALESCE(a.actual,0) / ((t.month_target/31.0) * a.last_day)) * 100, 1)
          END AS pct
        FROM salesmen s
        LEFT JOIN actuals a ON a.salesman = s.salesman
        LEFT JOIN targets t ON t.salesman = s.salesman
        ORDER BY pct DESC, name
    """

    try:
        conn = get_connection()
        cur = conn.cursor(dictionary=True)
        cur.execute(sql, tuple(salesmen_params + act_params + tgt_params))
        rows = cur.fetchall()
        cur.close(); conn.close()
        return jsonify(rows)
    except Exception as e:
        return jsonify({"error": str(e)}), 500




if __name__ == "__main__":
    app.run(debug=True)
