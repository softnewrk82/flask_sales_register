# @app.route("/autocomplete/<field>")
# @safe_db_call
# def autocomplete(conn, field):
#     # разрешённые поля: FIELDS и region_code, Дата
#     if field not in FIELDS and field not in ["region_code", "Дата"]:
#         return jsonify([])

#     q = request.args.get("q", "").strip()
#     # кэш ключ собираем из всех параметров (чтобы учитывать зависимость от других выбранных фильтров)
#     cache_key = get_cache_key(field, request.args)
#     cached = get_from_cache(cache_key)
#     if cached is not None:
#         return jsonify(cached)

#     # Если поле — region_code, возвращаем формат "77 — Москва" и учитываем q
#     if field == "region_code":
#         filtered = []
#         for code, name in REGION_MAP.items():
#             text = f"{code} — {name}"
#             if not q or q.lower() in text.lower():
#                 filtered.append(text)
#         set_to_cache(cache_key, filtered[:50])
#         return jsonify(filtered[:50])

#     # Для остальных полей учитываем уже выбранные фильтры (в т.ч. region_code[] в формате "77 — Москва")
#     where_clause, values = build_filter_query(request.args)

#     cur = conn.cursor()
#     extra = ""
#     if q:
#         extra = f'AND "{field}" ILIKE %s'
#         values.append(f"%{q}%")
#     sql = f'SELECT DISTINCT "{field}" FROM intermediate_scheme.sbis_coll_sell_upd_for_flask {where_clause} {extra} ORDER BY "{field}" LIMIT 50'
#     cur.execute(sql, values)
#     res = [r[0] for r in cur.fetchall() if r[0]]
#     cur.close()
#     set_to_cache(cache_key, res)
#     return jsonify(res)