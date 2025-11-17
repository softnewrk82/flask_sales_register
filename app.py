from flask import Flask, render_template, request, jsonify, send_file
import psycopg2
from psycopg2 import pool
import pandas as pd
import io, os, time
from dotenv import load_dotenv
from functools import wraps

# -------------------------
# Настройка приложения
# -------------------------
app = Flask(__name__, template_folder="templates")
load_dotenv()

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": int(os.getenv("DB_PORT", 5432)),
    "database": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD")
}

# пул соединений
db_pool = pool.SimpleConnectionPool(1, 10, **DB_CONFIG)
PAGE_SIZE = 50

# -------------------------
# Справочник регионов (код -> наименование)
# -------------------------
REGION_MAP = {
    1: "Республика Адыгея", 
    2: "Республика Башкортостан", 
    3: "Республика Бурятия", 
    4: "Республика Алтай",
    5: "Республика Дагестан", 
    6: "Республика Ингушетия", 
    7: "Кабардино-Балкарская Республика",
    8: "Республика Калмыкия", 
    9: "Карачаево-Черкесская Республика", 
    10: "Республика Карелия",
    11: "Республика Коми", 
    12: "Республика Марий Эл", 
    13: "Республика Мордовия",
    14: "Республика Саха (Якутия)", 
    15: "Республика Северная Осетия — Алания", 
    16: "Республика Татарстан",
    17: "Республика Тыва", 
    18: "Удмуртская Республика", 
    19: "Республика Хакасия",
    20: "Чеченская Республика", 
    21: "Чувашская Республика", 
    22: "Алтайский край",
    23: "Краснодарский край", 
    24: "Красноярский край", 
    25: "Приморский край", 
    26: "Ставропольский край",
    27: "Хабаровский край", 
    28: "Амурская область", 
    29: "Архангельская область", 
    30: "Астраханская область",
    31: "Белгородская область",
    32: "Брянская область",
    33: "Владимирская область",
    34: "Волгоградская область",
    35: "Вологодская область",
    36: "Воронежская область",
    37: "Ивановская область",
    38: "Иркутская область",
    39: "Калининградская область", 
    40: "Калужская область", 
    41: "Камчатский край", 
    42: "Кемеровская область",
    43: "Кировская область", 
    44: "Костромская область", 
    45: "Курганская область", 
    46: "Курская область",
    47: "Ленинградская область", 
    48: "Липецкая область",
    49: "Магаданская область",
    50: "Московская область",
    51: "Мурманская область", 
    52: "Нижегородская область", 
    53: "Новгородская область", 
    54: "Новосибирская область",
    55: "Омская область",
    56: "Оренбургская область",
    57: "Орловская область",
    58: "Пензенская область",
    59: "Пермский край",
    60: "Псковская область",
    61: "Ростовская область",
    62: "Рязанская область",
    63: "Самарская область",
    64: "Саратовская область",
    65: "Сахалинская область",
    66: "Свердловская область",
    67: "Смоленская область",
    68: "Тамбовская область",
    69: "Тверская область",
    70: "Томская область",
    71: "Тульская область",
    72: "Тюменская область",
    73: "Ульяновская область",
    74: "Челябинская область",
    75: "Забайкальский край",
    76: "Ярославская область",
    77: "Москва", 
    78: "Санкт-Петербург",
    79: "Еврейская автономная область", 
    80: "",
    81: "",
    82: "",
    83: "Ненецкий автономный округ",
    84: "",
    85: "",
    86: "Ханты-Мансийский автономный округ — Югра", 
    87: "Чукотский автономный округ",
    88: "",
    89: "Ямало-Ненецкий автономный округ", 
    90: "Запорожская область", 
    91: "Республика Крым", 
    92: "Севастополь", 
    93: "Донецкая Народная Республика", 
    94: "Луганская Народная Республика", 
    95: "Херсонская область", 
    96: "", 
    97: "", 
    98: "",     
    99: "Иные территории"
}

# -------------------------
# Поля и порядок колонок
# -------------------------
FIELDS = [
    "doc_counterparty_inn",
    "doc_counterparty_full_name",
    "doc_number",
    "inside_doc_item_code",
    "inside_doc_item_name",
    "Номенклатура.ГАУ",
    "Номенклатура.ГАУ.Группа"
]

COLUMN_ORDER = [
    "doc_counterparty_inn", "Регион", "doc_counterparty_full_name", "Дата", "doc_number", "doc_department",
    "doc_assigned_manager", "inside_doc_author", "inside_doc_item_code", "inside_doc_item_name",
    "inside_doc_item_quantity", "inside_doc_item_full_item_price", "Номенклатура.ГАУ",
    "Номенклатура.ГАУ.Группа", "Sale_type"
]

# -------------------------
# Кэш автоподсказок
# -------------------------
autocomplete_cache = {}
CACHE_TTL = 600  # seconds

def get_cache_key(field, params):
    parts = [field]
    for k in sorted(params.keys()):
        v = params.getlist(k)
        parts.append(f"{k}={'|'.join(v)}")
    return "|".join(parts)

def get_from_cache(key):
    item = autocomplete_cache.get(key)
    if item and (time.time() - item["time"] < CACHE_TTL):
        return item["data"]
    if item:
        del autocomplete_cache[key]
    return None

def set_to_cache(key, data):
    autocomplete_cache[key] = {"data": data, "time": time.time()}

# -------------------------
# Утилиты работы с БД и фильтрами
# -------------------------
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
            # отладочная информация в JSON
            return jsonify({"error": str(e)}), 500
        finally:
            if conn:
                release_connection(conn)
    return wrapper

def extract_region_from_inn(inn: str):
    try:
        s = str(inn)
        if not s:
            return "Неизвестный регион"
        code = int(s[:2])
        return REGION_MAP.get(code, "Неизвестный регион")
    except Exception:
        return "Неизвестный регион"

def parse_region_codes_from_params(params):
    """Берёт region_code[] значения и пытается извлечь коды (int).
       Поддерживает формат '77 — Москва' и просто '77'."""
    raw = [r for r in params.getlist("region_code[]") if r.strip()]
    codes = []
    for val in raw:
        try:
            # если формат "77 — Москва" или "77-...", берём первую часть до нецифр
            part = str(val).split("—")[0].split("-")[0].strip()
            # также возможна запятая, пробелы и т.п.
            code = int("".join([c for c in part if c.isdigit()]) or 0)
            if code:
                codes.append(code)
        except Exception:
            continue
    return codes

def build_filter_query(params):
    filters, values = [], []
    # стандартные поля
    for f in FIELDS:
        vals = params.getlist(f + '[]')
        if vals:
            filters.append(f'"{f}" = ANY(%s)')
            values.append(vals)

    # region_code[]
    region_codes = parse_region_codes_from_params(params)
    if region_codes:
        filters.append(
            'COALESCE(NULLIF(regexp_replace(SUBSTRING("doc_counterparty_inn" FROM 1 FOR 2), \'[^0-9]\', \'\', \'g\'), \'\')::int, 0) = ANY(%s)'
        )
        values.append(region_codes)

    # date_from / date_to
    if params.get("date_from"):
        filters.append('"Дата">=%s')
        values.append(params["date_from"])
    if params.get("date_to"):
        filters.append('"Дата"<=%s')
        values.append(params["date_to"])

    where_clause = " WHERE " + " AND ".join(filters) if filters else ""
    return where_clause, values

# -------------------------
# Маршруты
# -------------------------
@app.route("/")
@safe_db_call
def index(conn):
    # собрать варианты для initial dropdowns (необязательно, но удобно)
    options = {}
    cur = conn.cursor()
    for f in FIELDS:
        cur.execute(f'SELECT DISTINCT "{f}" FROM intermediate_scheme.sbis_coll_sell_upd_for_flask')
        options[f] = sorted([r[0] for r in cur.fetchall() if r[0]])
    # region codes present in table
    cur.execute('''
        SELECT DISTINCT COALESCE(NULLIF(regexp_replace(SUBSTRING("doc_counterparty_inn" FROM 1 FOR 2), '[^0-9]', '', 'g'), '')::int, 0)
        FROM intermediate_scheme.sbis_coll_sell_upd_for_flask
    ''')
    region_codes = sorted([r[0] for r in cur.fetchall() if r[0]])
    options["region_code"] = [f"{code} — {REGION_MAP.get(code, 'Неизвестный регион')}" for code in region_codes]
    cur.close()
    return render_template("index_final.html", options=options, fields=FIELDS, column_order=COLUMN_ORDER)

@app.route("/data")
@safe_db_call
def data(conn):
    page = int(request.args.get("page", 1))
    sort_col = request.args.get("sort_col", "Дата")
    sort_dir = request.args.get("sort_dir", "desc")
    offset = (page - 1) * PAGE_SIZE

    if sort_col not in COLUMN_ORDER:
        sort_col = "Дата"
    sort_dir = "ASC" if sort_dir == "asc" else "DESC"

    where_clause, values = build_filter_query(request.args)
    # выбираем все колонки, кроме виртуальной "Регион" (его добавим в pandas)
    cols = ", ".join([f'"{c}"' for c in COLUMN_ORDER if c != "Регион"])
    query = f'SELECT {cols} FROM intermediate_scheme.sbis_coll_sell_upd_for_flask {where_clause} ORDER BY "{sort_col}" {sort_dir} LIMIT {PAGE_SIZE} OFFSET {offset}'
    count_query = f'SELECT COUNT(*) FROM intermediate_scheme.sbis_coll_sell_upd_for_flask {where_clause}'
    df = pd.read_sql(query, conn, params=values)
    cur = conn.cursor()
    cur.execute(count_query, values)
    total_rows = cur.fetchone()[0]
    cur.close()

    # добавляем колонку "Регион"
    if "doc_counterparty_inn" in df.columns:
        df["Регион"] = df["doc_counterparty_inn"].apply(extract_region_from_inn)
    if "Дата" in df.columns:
        df["Дата"] = pd.to_datetime(df["Дата"], errors="coerce").dt.strftime("%Y-%m-%d")

    # приводим порядок столбцов к COLUMN_ORDER (если они есть)
    df = df[[c for c in COLUMN_ORDER if c in df.columns]]
    total_pages = (total_rows // PAGE_SIZE) + (1 if total_rows % PAGE_SIZE else 0)
    return jsonify({"data": df.to_dict(orient="records"), "total_pages": total_pages})

@app.route("/export")
@safe_db_call
def export_excel(conn):
    where_clause, values = build_filter_query(request.args)
    cols = ", ".join([f'"{c}"' for c in COLUMN_ORDER if c != "Регион"])
    query = f'SELECT {cols} FROM intermediate_scheme.sbis_coll_sell_upd_for_flask {where_clause} ORDER BY "Дата" DESC'
    df = pd.read_sql(query, conn, params=values)

    # Добавляем Регион
    if "doc_counterparty_inn" in df.columns:
        df["Регион"] = df["doc_counterparty_inn"].apply(extract_region_from_inn)
    if "Дата" in df.columns:
        df["Дата"] = pd.to_datetime(df["Дата"], errors="coerce")

    df = df[[c for c in COLUMN_ORDER if c in df.columns]]

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, sheet_name="Продажи")
        workbook = writer.book
        worksheet = writer.sheets["Продажи"]

        header_format = workbook.add_format({'bold': True, 'align': 'center', 'valign': 'vcenter', 'fg_color': '#DDEBF7', 'border': 1})
        for col_num, value in enumerate(df.columns.values):
            worksheet.write(0, col_num, value, header_format)

        number_format = workbook.add_format({'num_format': '#,##0.00', 'border': 1})
        date_format = workbook.add_format({'num_format': 'yyyy-mm-dd', 'border': 1})
        row_formats = [workbook.add_format({'bg_color': '#FFFFFF', 'border': 1}),
                       workbook.add_format({'bg_color': '#F3F3F3', 'border': 1})]

        for row_num, row in enumerate(df.itertuples(index=False), start=1):
            fmt = row_formats[row_num % 2]
            for col_num, cell in enumerate(row):
                if pd.isna(cell):
                    worksheet.write(row_num, col_num, "", fmt)
                elif isinstance(cell, (int, float)):
                    worksheet.write(row_num, col_num, cell, number_format)
                elif isinstance(cell, pd.Timestamp):
                    worksheet.write(row_num, col_num, cell, date_format)
                else:
                    worksheet.write(row_num, col_num, str(cell), fmt)

        for i, col in enumerate(df.columns):
            max_len = max(df[col].astype(str).map(len).max(), len(str(col)))
            worksheet.set_column(i, i, max_len + 2)
        worksheet.freeze_panes(1, 0)

    output.seek(0)
    return send_file(output, as_attachment=True,
                     download_name="Продажи.xlsx",
                     mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

@app.route("/autocomplete/<field>")
@safe_db_call
def autocomplete(conn, field):
    # разрешённые поля: FIELDS и region_code, Дата
    if field not in FIELDS and field not in ["region_code", "Дата"]:
        return jsonify([])

    q = request.args.get("q", "").strip()
    # кэш ключ собираем из всех параметров (чтобы учитывать зависимость от других выбранных фильтров)
    cache_key = get_cache_key(field, request.args)
    cached = get_from_cache(cache_key)
    if cached is not None:
        return jsonify(cached)

    # Если поле — region_code, возвращаем формат "77 — Москва" и учитываем q
    if field == "region_code":
        filtered = []
        for code, name in REGION_MAP.items():
            text = f"{code} — {name}"
            if not q or q.lower() in text.lower():
                filtered.append(text)
        set_to_cache(cache_key, filtered[:50])
        return jsonify(filtered[:50])

    # Для остальных полей учитываем уже выбранные фильтры (в т.ч. region_code[] в формате "77 — Москва")
    where_clause, values = build_filter_query(request.args)

    cur = conn.cursor()
    extra = ""
    if q:
        extra = f'AND "{field}" ILIKE %s'
        values.append(f"%{q}%")
    sql = f'SELECT DISTINCT "{field}" FROM intermediate_scheme.sbis_coll_sell_upd_for_flask {where_clause} {extra} ORDER BY "{field}" LIMIT 50'
    cur.execute(sql, values)
    res = [r[0] for r in cur.fetchall() if r[0]]
    cur.close()
    set_to_cache(cache_key, res)
    return jsonify(res)

@app.route("/last_update")
@safe_db_call
def last_update(conn):
    cur = conn.cursor()
    cur.execute('SELECT MAX("datetime") FROM service_toolkit.upd_t')
    last_update = cur.fetchone()[0]
    cur.close()
    last_update_str = last_update.strftime("%Y-%m-%d %H:%M:%S") if last_update else "нет данных"
    return jsonify({"last_update": last_update_str})

# -------------------------
# Запуск
# -------------------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
