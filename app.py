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

@app.get("/")
def index():
    return send_from_directory(app.static_folder, "index.html")


# ---------- Target endpoint (supports metric + QA + filters) ----------
@app.get("/api/july_target")
def july_target():
    metric = request.args.get("metric", "qty").lower().strip()
    qa = 'Q' if metric == 'qty' else 'A'
    region = request.args.get("region", "ALL")
    salesman = request.args.get("salesman", "ALL")
    sold_to_group = request.args.get("sold_to_group", "ALL")
    sold_to = request.args.get("sold_to", "ALL")
    product_group = request.args.get("product_group", "ALL")

    where = ["(t.Year = 25 OR t.Year = 2025)", "t.Month = 7", "t.QA = %s"]
    params = [qa]

    def add(cond, val):
        if val and val != "ALL":
            where.append(cond)
            params.append(val)

    add("t.BDE_State = %s", region)
    add("t.Salesman_name = %s", salesman)
    add("t.CGR = %s", sold_to_group)
    add("t.Sold_To = %s", sold_to)
    add("t.Prod = %s", product_group)

    sql = f"""
        SELECT SUM(t.Month_Value_2025) AS mv
        FROM target t
        WHERE {" AND ".join(where)}
    """

    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(sql, tuple(params))
        row = cur.fetchone()
        cur.close(); conn.close()

        mv = row[0] if row and row[0] is not None else 0
        daily_target = round(float(mv) / 31, 2) if mv else 0.0
        return jsonify({"daily_target": daily_target, "metric": metric, "qa": qa})
    except Exception as e:
        return jsonify({"error": str(e)}), 500



# ---------- SKU/Amount trend endpoint (supports metric + group_by + filters) ----------
@app.get("/api/sku_trend")
def sku_trend():
    """
    Returns rows with (all for selected metric):
      billing_date (dd-mm-yy),
      group_label (depends on group_by),
      daily_value (sum for selected metric),
      total_value (same day overall),
      percentage (daily share % of day's total),
      daily_qty (NULL if metric=amount),
      daily_amount (NULL if metric=qty)

    Accepts:
      ?metric=qty|amount (default: qty)
      ?group_by=product_group|region|salesman|sold_to_group|sold_to
      + usual filters region/salesman/sold_to_group/sold_to/product_group
    """
    metric         = request.args.get("metric", "qty").lower().strip()
    region         = request.args.get("region", "ALL")
    salesman       = request.args.get("salesman", "ALL")
    sold_to_group  = request.args.get("sold_to_group", "ALL")
    sold_to        = request.args.get("sold_to", "ALL")
    product_group  = request.args.get("product_group", "ALL")
    group_by       = request.args.get("group_by", "product_group")

    # Choose value expression by metric
    if metric == "amount":
        value_sum_expr = f"SUM(j.net_value)"
        daily_qty_expr = "NULL"
        daily_amt_expr = value_sum_expr
    else:
        metric = "qty"
        value_sum_expr = "SUM(j.billqty_in_SKU)"
        daily_qty_expr = value_sum_expr
        daily_amt_expr = "NULL"

    group_cols = {
        "product_group": "c.product_group",
        "region":        "j.ship_to_region",
        "salesman":      "j.salesman_name",
        "sold_to_group": "j.sold_to_grp3",
        "sold_to":       "j.sold_to_name",
    }
    if group_by not in group_cols:
        return jsonify({"error": "invalid group_by"}), 400
    group_col = group_cols[group_by]

    filters, params = [], []
    def add(cond, val):
        if val and val != "ALL":
            filters.append(cond); params.append(val)

    add("j.ship_to_region = %s", region)
    add("j.salesman_name = %s", salesman)
    add("j.sold_to_grp3 = %s", sold_to_group)
    add("j.sold_to_name = %s", sold_to)
    add("c.product_group = %s", product_group)

    where = ("WHERE " + " AND ".join(filters)) if filters else ""

    sql = f"""
        WITH daily_totals AS (
            SELECT j.billing_date, {value_sum_expr} AS total_value
            FROM julysales j
            JOIN carrying_july c ON j.material = c.M_CODE
            {where}
            GROUP BY j.billing_date
        )
        SELECT
            DATE_FORMAT(j.billing_date, '%d-%m-%y') AS billing_date,
            {group_col} AS group_label,
            {value_sum_expr} AS daily_value,
            t.total_value,
            ROUND({value_sum_expr} / NULLIF(t.total_value, 0) * 100, 2) AS percentage,
            {daily_qty_expr}   AS daily_qty,
            {daily_amt_expr}   AS daily_amount
        FROM julysales j
        JOIN carrying_july c ON j.material = c.M_CODE
        JOIN daily_totals t ON j.billing_date = t.billing_date
        {where}
        GROUP BY j.billing_date, {group_col}
        ORDER BY j.billing_date
    """
    try:
        conn = get_connection()
        cur = conn.cursor(dictionary=True)
        cur.execute(sql, tuple(params*2 if filters else ()))
        rows = cur.fetchall()
        cur.close(); conn.close()
        return jsonify(rows)
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


if __name__ == "__main__":
    app.run(debug=True)
