from flask import Flask, render_template, request, jsonify, send_file
import psycopg2
from psycopg2 import pool
import pandas as pd
import io, os
from dotenv import load_dotenv
from functools import wraps

app = Flask(__name__)
load_dotenv()

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": int(os.getenv("DB_PORT")),
    "database": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD")
}

db_pool = pool.SimpleConnectionPool(1, 10, **DB_CONFIG)
PAGE_SIZE = 50

FIELDS = [
    "doc_counterparty_inn",
    "doc_counterparty_full_name",
    "doc_number",
    "inside_doc_item_code",
    "inside_doc_item_name",
    "Номенклатура.ГАУ",
    "Номенклатура.ГАУ.Группа"
]

COLUMN_ORDER=[
    "doc_counterparty_inn","doc_counterparty_full_name","Дата","doc_number","doc_department",
    "doc_assigned_manager","inside_doc_author","inside_doc_item_code","inside_doc_item_name",
    "inside_doc_item_quantity","inside_doc_item_full_item_price","Номенклатура.ГАУ",
    "Номенклатура.ГАУ.Группа","Sale_type"
]

def get_connection():
    return db_pool.getconn()

def release_connection(conn):
    db_pool.putconn(conn)

def safe_db_call(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        conn = None
        try:
            conn = get_connection()
            return func(conn, *args, **kwargs)
        except Exception as e:
            return jsonify({"error": str(e)}), 500
        finally:
            if conn:
                release_connection(conn)
    return wrapper

# ---------------- Фильтрация ----------------
def build_filter_query(params):
    filters, values = [], []
    for f in FIELDS:
        vals = params.getlist(f+'[]')
        if vals:
            filters.append(f'"{f}" = ANY(%s)')
            values.append(vals)
    if params.get("date_from"):
        filters.append('"Дата">=%s')
        values.append(params["date_from"])
    if params.get("date_to"):
        filters.append('"Дата"<=%s')
        values.append(params["date_to"])
    where_clause = " WHERE " + " AND ".join(filters) if filters else ""
    return where_clause, values

# ---------------- Маршруты ----------------
@app.route("/")
@safe_db_call
def index(conn):
    cur = conn.cursor()
    options = {}
    for f in FIELDS:
        cur.execute(f'SELECT DISTINCT "{f}" FROM intermediate_scheme.sbis_coll_sell_upd')
        options[f] = sorted([r[0] for r in cur.fetchall() if r[0]])
    cur.close()
    return render_template("index_final.html", options=options)

@app.route("/data")
@safe_db_call
def data(conn):
    page = int(request.args.get("page",1))
    sort_col = request.args.get("sort_col","Дата")
    sort_dir = request.args.get("sort_dir","desc")
    offset = (page-1)*PAGE_SIZE

    if sort_col not in COLUMN_ORDER: sort_col="Дата"
    sort_dir = "ASC" if sort_dir=="asc" else "DESC"

    where_clause, values = build_filter_query(request.args)
    cols = ", ".join([f'"{c}"' for c in COLUMN_ORDER])
    query = f'SELECT {cols} FROM intermediate_scheme.sbis_coll_sell_upd {where_clause} ORDER BY "{sort_col}" {sort_dir} LIMIT {PAGE_SIZE} OFFSET {offset}'
    count_query = f'SELECT COUNT(*) FROM intermediate_scheme.sbis_coll_sell_upd {where_clause}'

    df = pd.read_sql(query, conn, params=values)
    cur = conn.cursor()
    cur.execute(count_query, values)
    total_rows = cur.fetchone()[0]
    cur.close()

    if "Дата" in df.columns:
        df["Дата"] = pd.to_datetime(df["Дата"], errors="coerce").dt.strftime("%Y-%m-%d")

    total_pages = (total_rows // PAGE_SIZE) + (1 if total_rows % PAGE_SIZE else 0)
    return jsonify({"data": df.to_dict(orient="records"), "total_pages": total_pages})

@app.route("/export")
@safe_db_call
def export_excel(conn):
    # собираем фильтры как обычно
    where_clause, values = build_filter_query(request.args)
    cols = ", ".join([f'"{c}"' for c in COLUMN_ORDER])
    query = f'SELECT {cols} FROM intermediate_scheme.sbis_coll_sell_upd {where_clause} ORDER BY "Дата" DESC'

    df = pd.read_sql(query, conn, params=values)
    if "Дата" in df.columns:
        df["Дата"] = pd.to_datetime(df["Дата"], errors="coerce")

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, sheet_name="Продажи")
        workbook = writer.book
        worksheet = writer.sheets["Продажи"]

        # Форматирование заголовков
        header_format = workbook.add_format({'bold': True,'align': 'center','valign': 'vcenter','fg_color': '#DDEBF7','border': 1})
        for col_num, value in enumerate(df.columns.values):
            worksheet.write(0, col_num, value, header_format)

        # Чередование строк и форматы данных
        row_formats = [workbook.add_format({'bg_color':'#FFFFFF','border':1}), workbook.add_format({'bg_color':'#F3F3F3','border':1})]
        number_format = workbook.add_format({'num_format':'#,##0.00','border':1})
        date_format = workbook.add_format({'num_format':'yyyy-mm-dd','border':1})

        for row_num, row in enumerate(df.itertuples(index=False), start=1):
            fmt = row_formats[row_num % 2]
            for col_num, cell in enumerate(row):
                if pd.isna(cell):
                    worksheet.write(row_num, col_num, "", fmt)
                elif isinstance(cell, (int,float)):
                    worksheet.write(row_num, col_num, cell, number_format)
                elif isinstance(cell, pd.Timestamp):
                    worksheet.write(row_num, col_num, cell, date_format)
                else:
                    worksheet.write(row_num, col_num, str(cell), fmt)

        # Автоширина и высота
        for i, col in enumerate(df.columns):
            max_len = max(df[col].astype(str).map(len).max(), len(str(col)))
            worksheet.set_column(i, i, max_len + 2)
        for row_num in range(len(df)+1):
            worksheet.set_row(row_num, 20)

        worksheet.freeze_panes(1, 0)

    output.seek(0)
    return send_file(
        output,
        as_attachment=True,
        download_name="Продажи.xlsx",
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
@app.route("/autocomplete/<field>")
@safe_db_call
def autocomplete(conn, field):
    if field not in FIELDS:
        return jsonify([])

    q = request.args.get("q","")
    filters, values = [], []
    for f in FIELDS:
        vals = request.args.getlist(f+'[]')
        if vals and f != field:
            filters.append(f'"{f}" = ANY(%s)')
            values.append(vals)
    if q:
        filters.append(f'"{field}" ILIKE %s')
        values.append(f"%{q}%")
    where_clause = " WHERE " + " AND ".join(filters) if filters else ""
    cur = conn.cursor()
    cur.execute(f'SELECT DISTINCT "{field}" FROM intermediate_scheme.sbis_coll_sell_upd {where_clause} ORDER BY "{field}" LIMIT 50', values)
    res = [r[0] for r in cur.fetchall() if r[0]]
    cur.close()
    return jsonify(res)

if __name__=="__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)