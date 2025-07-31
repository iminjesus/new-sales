from flask import Flask, jsonify, request, send_from_directory
import mysql.connector
from flask_cors import CORS
import os

app = Flask(__name__, static_folder='static', static_url_path='')
CORS(app)

def get_connection():
    return mysql.connector.connect(
        host="localhost",
        user="root",
        password="",
        database="my_new_database"
    )

@app.route('/')
def serve_index():
    return send_from_directory(app.static_folder, 'index.html')

@app.route("/api/sku_trend")
def get_sku_trend():
    region = request.args.get("region")
    salesman = request.args.get("salesman")

    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)

        base_query = """
            SELECT 
                DATE_FORMAT(billing_date, '%d-%m-%y') AS billing_date,
                SUM(billqty_in_SKU) AS daily_qty,
                SUM(SUM(billqty_in_SKU)) OVER (ORDER BY billing_date) AS cumulative_qty
            FROM julysales
            WHERE 1=1
        """
        conditions = []
        params = []

        if region and region != "ALL":
            conditions.append("ship_to_region = %s")
            params.append(region)
        if salesman and salesman != "ALL":
            conditions.append("salesman_name = %s")
            params.append(salesman)

        if conditions:
            base_query += " AND " + " AND ".join(conditions)

        base_query += " GROUP BY billing_date ORDER BY billing_date"

        cursor.execute(base_query, tuple(params))
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        return jsonify(rows)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/product_group_percentage")
def get_product_group_percentage():
    region = request.args.get("region")
    salesman = request.args.get("salesman")

    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)

        query = """
            SELECT 
                DATE_FORMAT(j.billing_date, '%d-%m-%y') AS billing_date,
                c.Product_Group,
                ROUND(SUM(j.billqty_in_SKU) / SUM(SUM(j.billqty_in_SKU)) OVER (PARTITION BY j.billing_date) * 100, 2) AS percentage
            FROM julysales j
            JOIN carrying_july c ON j.material = c.M_CODE
            WHERE 1=1
        """
        conditions = []
        params = []

        if region and region != "ALL":
            conditions.append("j.ship_to_region = %s")
            params.append(region)
        if salesman and salesman != "ALL":
            conditions.append("j.salesman_name = %s")
            params.append(salesman)

        if conditions:
            query += " AND " + " AND ".join(conditions)

        query += """
            GROUP BY j.billing_date, c.Product_Group
            ORDER BY j.billing_date
        """

        cursor.execute(query, tuple(params))
        result = cursor.fetchall()
        cursor.close()
        conn.close()
        return jsonify(result)

    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/product_group_cumulative")
def get_product_group_cumulative():
    region = request.args.get("region")
    salesman = request.args.get("salesman")

    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)

        query = """
            WITH daily_sums AS (
                SELECT
                    j.billing_date,
                    c.Product_Group,
                    SUM(j.billqty_in_SKU) AS daily_qty
                FROM julysales j
                JOIN carrying_july c ON j.material = c.M_CODE
                WHERE 1=1
        """
        conditions = []
        params = []

        if region and region != "ALL":
            conditions.append("j.ship_to_region = %s")
            params.append(region)
        if salesman and salesman != "ALL":
            conditions.append("j.salesman_name = %s")
            params.append(salesman)

        if conditions:
            query += " AND " + " AND ".join(conditions)

        query += """
                GROUP BY j.billing_date, c.Product_Group
            ),
            cumulative AS (
                SELECT
                    billing_date,
                    Product_Group,
                    SUM(daily_qty) OVER (PARTITION BY Product_Group ORDER BY billing_date) AS cumulative_qty
                FROM daily_sums
            ),
            total_per_day AS (
                SELECT
                    billing_date,
                    SUM(cumulative_qty) AS total_qty
                FROM cumulative
                GROUP BY billing_date
            )
            SELECT 
                DATE_FORMAT(c.billing_date, '%d-%m-%y') AS billing_date,
                c.Product_Group,
                ROUND(c.cumulative_qty / t.total_qty * 100, 2) AS percentage
            FROM cumulative c
            JOIN total_per_day t ON c.billing_date = t.billing_date
            ORDER BY c.billing_date
        """

        cursor.execute(query, tuple(params))
        result = cursor.fetchall()
        cursor.close()
        conn.close()
        return jsonify(result)

    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(debug=True)
