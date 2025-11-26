@app.get("/api/daily_sales")
def daily_sales():
    f = parse_filters(request)
    value = "qty" if f["metric"] == "qty" else "amt"

    joins, wh, params = build_customer_filters("s", f, use_sold_to_name=False)

    # category
    cat_joins, cat_where = category_filters("s", f["category"])
    joins += cat_joins
    wh    += cat_where

    # direct fields (indexable)
    if f["product_group"] != "ALL":
        wh.append("s.product_group = %s"); params.append(f["product_group"])
    if f["pattern"] != "ALL":
        wh.append("s.pattern = %s"); params.append(f["pattern"])

    where_sql = ("WHERE " + " AND ".join(wh)) if wh else ""
    sql = f"""
      SELECT s.day AS day_num, SUM(s.{value}) AS daily_total
        FROM sales2510 s
        {' '.join(joins)}
        {where_sql}
       GROUP BY s.day
       ORDER BY s.day
    """
    
    conn = get_connection(); cur = conn.cursor(dictionary=True)
    try:
        cur.execute(sql, tuple(params))
        rows = cur.fetchall()
    finally:
        cur.close(); conn.close()

    day_map = {int(r["day_num"]): float(r["daily_total"] or 0) for r in rows}
    return jsonify([{"day": m, "value": day_map.get(m, 0)} for m in range(1, 31)])    