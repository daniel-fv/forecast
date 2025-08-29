# -*- coding: utf-8 -*-
"""
Forecast mensual por ítem (Ago-2025 a Dic-2025) con Prophet + Clasificaciones ABC/XYZ.
- Parser de meses robusto (ES/EN, fechas Excel)
- Winsorización SELECTIVA (solo ítems con negativos o CV alto), por mes del año
- Prophet (tendencia + estacionalidad anual) para Ago–Dic 2025
- ABC 2026 desde hoja 'Forecast' (Jan-26..Dec-26) * Precio de 'Precios-Costos'
- XYZ desde hoja 'Historico' (CV por ítem; pctiles 33/67)
- Exporta en Excel (sin gráficos):
  * Datos históricos
  * Forecast por ítem
  * Resumen por categoría
  * ABC 2026
  * XYZ Historico
  * ABC_XYZ
  * Historico_Clean
  * Items
  * CV_Stats
  * Precios_Costos
"""

import sys, subprocess, importlib, re
from typing import Optional

def install_and_import(package, import_name=None):
    try:
        return importlib.import_module(import_name or package)
    except ImportError:
        subprocess.check_call([sys.executable, "-m", "pip", "install", package])
        return importlib.import_module(import_name or package)

# Librerías
pd = install_and_import("pandas")
np = install_and_import("numpy")
install_and_import("openpyxl")
install_and_import("cmdstanpy")
Prophet = install_and_import("prophet", "prophet").Prophet

# Parámetros
URL_ITEMS = "https://github.com/daniel-fv/forecast/raw/refs/heads/main/supermercado_660_items.csv"
URL_HIST = "https://github.com/daniel-fv/forecast/raw/refs/heads/main/Tarea-Forecast%20Hasta%20Fin%20A%C3%B1o.xlsx"

FORECAST_MONTHS = 5
FORECAST_START = pd.Timestamp("2025-08-01")
FORECAST_END   = pd.Timestamp("2025-12-31")

# Criterio winsorización selectiva
CV_SELECTIVE_THRESHOLD = 0.50   # si CV > 0.5, candidato a winsor
CV_AGGRESSIVE_THRESHOLD = 0.70  # si CV > 0.7, usar p95 en vez de p99
UPPER_Q_MILD  = 0.99            # winsor suave
UPPER_Q_STRONG = 0.95           # winsor agresivo
LOWER_CLIP = 0.0                # nunca negativos

OUTPUT_XLSX = "Forecast_y_Clasificaciones.xlsx"

# ===== Maestro de ítems =====
items_df = pd.read_csv(URL_ITEMS)
items_df.columns = [c.strip() for c in items_df.columns]
colmap = {}
for c in items_df.columns:
    l = c.lower()
    if l.startswith("item"):
        colmap[c] = "ITEM"
    elif "nombre" in l:
        colmap[c] = "Nombre"
    elif "categor" in l:
        colmap[c] = "Categoría"
items_df = items_df.rename(columns=colmap)
for req in ["ITEM", "Nombre", "Categoría"]:
    if req not in items_df.columns:
        raise ValueError(f"Columna requerida no encontrada en maestro: {req}")

# ===== Carga archivo XLS =====
xls = pd.ExcelFile(URL_HIST)
# Historico
sheet_hist = None
for s in xls.sheet_names:
    if s.strip().lower() == "historico":
        sheet_hist = s; break
if sheet_hist is None:
    sheet_hist = xls.sheet_names[0]

# Forecast 2026 (para ABC)
sheet_forecast = None
for s in xls.sheet_names:
    if s.strip().lower() == "forecast":
        sheet_forecast = s; break

# Precios-Costos (para ABC)
sheet_precios = None
for s in xls.sheet_names:
    if s.strip().lower() in ("precios-costos", "precios_costos", "precios costos"):
        sheet_precios = s; break

# ===== Cargar Historico =====
hist_raw = pd.read_excel(URL_HIST, sheet_name=sheet_hist)
hist_raw.columns = [str(c).strip() for c in hist_raw.columns]
hist_raw = hist_raw[[c for c in hist_raw.columns if not str(c).lower().startswith("unnamed")]]

# Asegura primera col ITEM
first_col = hist_raw.columns[0]
if first_col.lower() != "item":
    hist_raw = hist_raw.rename(columns={first_col: "ITEM"})

# Numerizar
for c in hist_raw.columns:
    if c == "ITEM": continue
    if not pd.api.types.is_numeric_dtype(hist_raw[c]):
        hist_raw[c] = (
            hist_raw[c].astype(str)
            .str.replace(r"[,\s$]", "", regex=True)
            .str.replace("\u2212", "-", regex=False)
        )
    hist_raw[c] = pd.to_numeric(hist_raw[c], errors="coerce")

hist_original = hist_raw.copy()

# ===== Parser de meses =====
ES_TO_EN = {"Ene":"Jan","Feb":"Feb","Mar":"Mar","Abr":"Apr","May":"May","Jun":"Jun",
            "Jul":"Jul","Ago":"Aug","Sep":"Sep","Oct":"Oct","Nov":"Nov","Dic":"Dec"}

def normalize_month_header(h) -> str:
    s = str(h).strip()
    s = s.replace("–","-").replace("—","-").replace("_","-").replace(".","-")
    s = re.sub(r"\s+"," ", s)
    return s

def to_month_ts(col) -> Optional[pd.Timestamp]:
    import datetime as _dt
    if isinstance(col, pd.Timestamp):
        return col.to_period("M").to_timestamp(how="start")
    if isinstance(col, _dt.date):
        return pd.Timestamp(col).to_period("M").to_timestamp(how="start")
    s = normalize_month_header(col)
    for es,en in ES_TO_EN.items():
        s = re.sub(rf"\b{es}\b", en, s, flags=re.IGNORECASE)
    fmts = ["%b-%y","%b-%Y","%b %y","%b %Y"]
    for fmt in fmts:
        dt = pd.to_datetime(s, format=fmt, errors="coerce")
        if pd.notna(dt):
            return dt.to_period("M").to_timestamp(how="start")
    dt = pd.to_datetime(s, errors="coerce")
    if pd.notna(dt):
        return dt.to_period("M").to_timestamp(how="start")
    return None

value_cols = [c for c in hist_raw.columns if c != "ITEM"]
month_cols, month_map = [], {}
for c in value_cols:
    ts = to_month_ts(c)
    if ts is not None:
        month_cols.append(c)
        month_map[c] = ts
if not month_cols:
    raise ValueError("No se detectaron columnas de mes válidas en 'Historico'.")

# ===== Ancho→Largo (Historico) =====
long_rows = []
for _, row in hist_raw.iterrows():
    item = row["ITEM"]
    for c in month_cols:
        ds = month_map[c]
        y = row[c]
        if pd.notna(y):
            long_rows.append((item, ds, float(y)))
hist_long = pd.DataFrame(long_rows, columns=["ITEM","ds","y"])
if hist_long.empty:
    raise ValueError("No se construyó histórico largo.")

# ===== Winsorización selectiva =====
def coeff_variation(arr: np.ndarray) -> float:
    x = np.asarray(arr, dtype=float)
    x = x[~np.isnan(x)]
    if x.size == 0: return np.nan
    mu = np.mean(x)
    if mu == 0: return np.nan
    sigma = np.std(x, ddof=1) if x.size>1 else 0.0
    return sigma/mu

negatives_flag = (
    hist_long.groupby("ITEM")["y"].apply(lambda s: (s<0).any())
    .rename("had_negative").reset_index()
)
hist_nonneg = hist_long.copy()
hist_nonneg["y"] = hist_nonneg["y"].clip(lower=LOWER_CLIP)

cv_after_neg = (
    hist_nonneg.groupby("ITEM")["y"]
    .apply(lambda s: coeff_variation(s.values))
    .rename("cv_after_neg").reset_index()
)
crit = negatives_flag.merge(cv_after_neg, on="ITEM", how="left")
crit["should_winsor"] = crit["had_negative"] | (crit["cv_after_neg"] > CV_SELECTIVE_THRESHOLD)

hist_nonneg["month"] = hist_nonneg["ds"].dt.month
def winsorize_by_moy(df_item: pd.DataFrame, cv_val: float) -> pd.DataFrame:
    upper_q = UPPER_Q_STRONG if (pd.notna(cv_val) and cv_val > CV_AGGRESSIVE_THRESHOLD) else UPPER_Q_MILD
    g = df_item.copy()
    caps = g.groupby("month")["y"].quantile(upper_q).rename("cap").reset_index()
    g = g.merge(caps, on="month", how="left")
    g["y"] = g.apply(lambda r: min(max(r["y"], LOWER_CLIP), r["cap"]) if pd.notna(r["cap"]) else max(r["y"], LOWER_CLIP), axis=1)
    return g.drop(columns=["cap"])

clean_parts, audit_rows = [], []
for _, row in crit.iterrows():
    item_id = row["ITEM"]; had_neg = bool(row["had_negative"]); cv_val = row["cv_after_neg"]; doit = bool(row["should_winsor"])
    df_item = hist_nonneg[hist_nonneg["ITEM"]==item_id].copy()
    before_cv = coeff_variation(df_item["y"].values)
    if doit:
        df_item = winsorize_by_moy(df_item, cv_val)
        action = f"winsor_moy_{'p95' if (pd.notna(cv_val) and cv_val>CV_AGGRESSIVE_THRESHOLD) else 'p99'}"
    else:
        action = "no_winsor"
    after_cv = coeff_variation(df_item["y"].values)
    df_item = df_item.drop(columns=["month"])
    clean_parts.append(df_item)
    audit_rows.append({"ITEM":item_id,"had_negative":had_neg,"cv_after_neg":cv_val,"winsor_action":action,"CV_before":before_cv,"CV_after":after_cv})

hist_clean = pd.concat(clean_parts, ignore_index=True)
cv_stats = pd.DataFrame(audit_rows)

# ===== Prophet (Ago–Dic 2025) =====
def fit_and_forecast_prophet(df_item: pd.DataFrame, periods=FORECAST_MONTHS):
    df_item = df_item.sort_values("ds")
    if df_item["y"].dropna().shape[0] < 2:
        return pd.DataFrame(columns=["ds","yhat"])
    m = Prophet(yearly_seasonality=True, weekly_seasonality=False, daily_seasonality=False, seasonality_mode="additive")
    m.fit(df_item[["ds","y"]])
    future = m.make_future_dataframe(periods=periods, freq="MS", include_history=False)
    fcst = m.predict(future)
    out = fcst[["ds","yhat"]].copy()
    return out[(out["ds"]>=FORECAST_START) & (out["ds"]<=FORECAST_END)]

all_forecasts = []
for item_id, grp in hist_clean.groupby("ITEM", sort=False):
    f = fit_and_forecast_prophet(grp[["ds","y"]].copy())
    if not f.empty:
        f["ITEM"] = item_id
        all_forecasts.append(f)
if not all_forecasts:
    raise RuntimeError("No se generaron pronósticos (Ago–Dic 2025).")
fcst_long = pd.concat(all_forecasts, ignore_index=True)

def month_label(ts: pd.Timestamp) -> str:
    return ts.strftime("%b-%y")

fcst_long["Mes"] = fcst_long["ds"].apply(month_label)
fcst_long["Forecast"] = np.round(fcst_long["yhat"], 0)
ordered_months = pd.date_range(start="2025-08-01", end="2025-12-01", freq="MS")
ordered_cols = [month_label(d) for d in ordered_months]
fcst_pivot = fcst_long.pivot_table(index="ITEM", columns="Mes", values="Forecast", aggfunc="first")
for col in ordered_cols:
    if col not in fcst_pivot.columns: fcst_pivot[col] = np.nan
fcst_pivot = fcst_pivot[ordered_cols].reset_index()
out_final = fcst_pivot.merge(items_df, on="ITEM", how="left")
out_final = out_final.reindex(columns=["ITEM","Nombre","Categoría"]+ordered_cols)

# ===== Reconstruir históricos (ancho) =====
def wide_from_long(df_long: pd.DataFrame, value_col: str, label_fmt="%b-%y"):
    df_w = df_long.copy()
    df_w["Mes"] = df_w["ds"].dt.strftime(label_fmt)
    wide = df_w.pivot_table(index="ITEM", columns="Mes", values=value_col, aggfunc="first")
    return wide.reset_index()

orig_long_rows = []
for _, row in hist_original.iterrows():
    item = row["ITEM"]
    for col in month_cols:
        ds = month_map.get(col)
        if ds is None: continue
        y = row[col]
        if pd.notna(y): orig_long_rows.append((item, ds, float(y)))
hist_original_long = pd.DataFrame(orig_long_rows, columns=["ITEM","ds","y"])
historico_original_w = wide_from_long(hist_original_long, "y")
historico_clean_w    = wide_from_long(hist_clean, "y")

def sort_month_cols(df):
    cols = df.columns.tolist()
    pairs=[]
    for c in cols:
        if c=="ITEM": continue
        try: pairs.append((c, pd.to_datetime(c, format="%b-%y")))
        except: pass
    ordered = ["ITEM"] + [c for c,_ in sorted(pairs, key=lambda kv: kv[1])]
    return df.reindex(columns=ordered)

historico_original_w = sort_month_cols(historico_original_w)
historico_clean_w    = sort_month_cols(historico_clean_w)

# ===== Resumen por categoría (hist total + forecast Ago–Dic 2025) =====
hist_clean_cat = hist_clean.merge(items_df[["ITEM","Categoría"]], on="ITEM", how="left")
resumen_hist_cat = (hist_clean_cat.groupby("Categoría", as_index=False)["y"].sum()
                    .rename(columns={"y":"Historico_Total"}))
fcst_cat = fcst_long.merge(items_df[["ITEM","Categoría"]], on="ITEM", how="left")
resumen_fcst_cat = (fcst_cat.groupby("Categoría", as_index=False)["Forecast"].sum()
                    .rename(columns={"Forecast":"Forecast_AgoDic_2025"}))
resumen_categoria = (resumen_hist_cat.merge(resumen_fcst_cat, on="Categoría", how="outer")
                     .fillna(0).sort_values("Historico_Total", ascending=False))

# =================== ABC (desde hoja Forecast 2026) ===================
abc_table = pd.DataFrame()
abc_join  = pd.DataFrame()
precios_clean = pd.DataFrame()

if sheet_forecast is not None and sheet_precios is not None:
    # Leer Forecast 2026
    fcst26_raw = pd.read_excel(URL_HIST, sheet_name=sheet_forecast)
    fcst26_raw.columns = [str(c).strip() for c in fcst26_raw.columns]
    fcst26_raw = fcst26_raw[[c for c in fcst26_raw.columns if not str(c).lower().startswith("unnamed")]]
    # asegurar ITEM
    if fcst26_raw.columns[0].lower() != "item":
        fcst26_raw = fcst26_raw.rename(columns={fcst26_raw.columns[0]:"ITEM"})
    # parse num
    for c in fcst26_raw.columns:
        if c=="ITEM": continue
        if not pd.api.types.is_numeric_dtype(fcst26_raw[c]):
            fcst26_raw[c] = (fcst26_raw[c].astype(str)
                             .str.replace(r"[,\s$]", "", regex=True)
                             .str.replace("\u2212","-", regex=False))
        fcst26_raw[c] = pd.to_numeric(fcst26_raw[c], errors="coerce")

    # Detectar columnas 2026
    fcst26_cols = []
    for c in fcst26_raw.columns:
        if c=="ITEM": continue
        ts = to_month_ts(c)
        if ts is not None and ts.year==2026:
            fcst26_cols.append(c)
    if not fcst26_cols:
        raise ValueError("No se detectaron columnas Jan-26..Dec-26 en la hoja 'Forecast'.")

    # Ventas 2026 por ítem (unidades)
    ventas2026 = fcst26_raw[["ITEM"]+fcst26_cols].copy()
    ventas2026["Unidades_2026"] = ventas2026[fcst26_cols].sum(axis=1)

    # Leer Precios-Costos
    precios_raw = pd.read_excel(URL_HIST, sheet_name=sheet_precios)
    precios_raw.columns = [str(c).strip() for c in precios_raw.columns]
    precios_raw = precios_raw[[c for c in precios_raw.columns if not str(c).lower().startswith("unnamed")]]
    if precios_raw.columns[0].lower() != "item":
        precios_raw = precios_raw.rename(columns={precios_raw.columns[0]:"ITEM"})
    # limpiar $ y comas
    for col in ["Precio","Costo"]:
        if col in precios_raw.columns:
            precios_raw[col] = (precios_raw[col].astype(str)
                                .str.replace(r"[,\s$]", "", regex=True))
            precios_raw[col] = pd.to_numeric(precios_raw[col], errors="coerce")
    precios_clean = precios_raw.copy()

    # Merge y Revenue
    abc = ventas2026[["ITEM","Unidades_2026"]].merge(precios_raw[["ITEM","Precio"]], on="ITEM", how="left")
    abc["Revenue_2026"] = abc["Unidades_2026"] * abc["Precio"]
    # añadir metadata
    abc = abc.merge(items_df[["ITEM","Nombre","Categoría"]], on="ITEM", how="left")

    # Orden y clasificación acumulada
    abc = abc.sort_values("Revenue_2026", ascending=False).reset_index(drop=True)
    total_rev = abc["Revenue_2026"].sum()
    abc["%_Revenue"] = abc["Revenue_2026"] / (total_rev if total_rev!=0 else 1.0)
    abc["%_Acum"] = abc["%_Revenue"].cumsum()

    def bucket_abc(p):
        if p <= 0.60: return "A"
        if p <= 0.80: return "B"
        return "C"
    abc["ABC"] = abc["%_Acum"].apply(bucket_abc)

    abc_table = abc[["ITEM","Nombre","Categoría","Unidades_2026","Precio","Revenue_2026","%_Revenue","%_Acum","ABC"]]
else:
    print("[Aviso] No se encontraron hojas 'Forecast' y/o 'Precios-Costos'; se omite ABC 2026.")

# =================== XYZ (desde Historico) ===================
# CV por ítem usando el histórico original numerizado (sin winsor, negativos como vengan)
cv_vals = (hist_original_long.groupby("ITEM")["y"]
           .apply(lambda s: coeff_variation(s.values))
           .reset_index().rename(columns={"y":"CV"}))
# percentiles (ignorar NaN)
valid_cv = cv_vals["CV"].replace([np.inf,-np.inf], np.nan).dropna()
p33 = np.nanpercentile(valid_cv, 33)
p67 = np.nanpercentile(valid_cv, 67)

def band_xyz(cv):
    if pd.isna(cv): return "Z"  # por defecto
    if cv <= p33: return "X"
    if cv <= p67: return "Y"
    return "Z"

cv_vals["XYZ"] = cv_vals["CV"].apply(band_xyz)
xyz_table = cv_vals.merge(items_df[["ITEM","Nombre","Categoría"]], on="ITEM", how="left")
xyz_table = xyz_table[["ITEM","Nombre","Categoría","CV","XYZ"]].sort_values("CV", na_position="last")

# =================== ABC ∩ XYZ ===================
abc_xyz = pd.DataFrame()
if not abc_table.empty and not xyz_table.empty:
    abc_xyz = (abc_table[["ITEM","ABC"]].merge(xyz_table[["ITEM","XYZ"]], on="ITEM", how="inner")
               .merge(items_df[["ITEM","Nombre","Categoría"]], on="ITEM", how="left"))
    abc_xyz = abc_xyz[["ITEM","Nombre","Categoría","ABC","XYZ"]]

# =================== Exportar ===================
with pd.ExcelWriter(OUTPUT_XLSX, engine="openpyxl") as writer:
    historico_original_w.to_excel(writer, sheet_name="Datos históricos", index=False)
    out_final.to_excel(writer, sheet_name="Forecast por ítem", index=False)
    resumen_categoria.to_excel(writer, sheet_name="Resumen por categoría", index=False)
    # Clasificaciones
    if not abc_table.empty:
        abc_table.to_excel(writer, sheet_name="ABC 2026", index=False)
    xyz_table.to_excel(writer, sheet_name="XYZ Historico", index=False)
    if not abc_xyz.empty:
        abc_xyz.to_excel(writer, sheet_name="ABC_XYZ", index=False)
    # Auditoría
    historico_clean_w.to_excel(writer, sheet_name="Historico_Clean", index=False)
    items_df.to_excel(writer, sheet_name="Items", index=False)
    cv_stats.to_excel(writer, sheet_name="CV_Stats", index=False)
    if not precios_clean.empty:
        precios_clean.to_excel(writer, sheet_name="Precios_Costos", index=False)

print(f"Archivo generado: {OUTPUT_XLSX}")
if sheet_forecast is None:
    print("Nota: No se encontró la hoja 'Forecast' → no se calculó ABC 2026.")
if sheet_precios is None:
    print("Nota: No se encontró la hoja 'Precios-Costos' → no se calculó ABC 2026.")
print(f"Cortes XYZ (sobre CV): P33={p33:.4f}, P67={p67:.4f}")
