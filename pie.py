"""
Pipeline: Impacto del derbi (Wanda) en movilidad MITMA + GeoJSON/HTML por hora
============================================================================
- Descarga MITMA (csv.gz) por día
- Lee en chunks, filtra origen=Wanda, excluye intradistrito, filtra IDs válidos (según GeoJSON)
- Calcula esperado por (destino,hora) con controles (media + IC95)
- Calcula impacto derbi vs esperado
- Exporta GeoJSON con "impacto_por_hora" para visor Leaflet (slider 0-23)
"""

import warnings
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Set
import json

import numpy as np
import pandas as pd
import requests
import geopandas as gpd

warnings.filterwarnings("ignore")

# =========================
# CONFIG
# =========================
OUTPUT_DIR = Path(r"C:\Users\khora\Downloads\analisis_derbi")
DATA_DIR = OUTPUT_DIR / "datos_movilidad"
ANALYSIS_DIR = OUTPUT_DIR / "analisis"
MAP_DIR = OUTPUT_DIR / "mapa_impacto"

for d in [DATA_DIR, ANALYSIS_DIR, MAP_DIR]:
    d.mkdir(parents=True, exist_ok=True)

BASE_URL = (
    "https://movilidad-opendata.mitma.es/"
    "estudios_basicos/por-distritos/viajes/ficheros-diarios"
)

# Wanda (según tu código)
DISTRITO_WANDA = "2807920"  # mantenemos como string

# ⚠️ Pon aquí la fecha REAL del derbi (me dijiste marzo)
FECHA_DERBI = "2025-03-12"   # <-- CAMBIA ESTO

# Controles: MISMO día de semana que FECHA_DERBI (mete varios de marzo/abril sin evento)
FECHAS_CONTROL = [
    "2025-03-19","2025-03-26","2025-03-05"
    # "2025-03-..", "2025-03-..", ...
]

# GeoJSON base distritos (tu fichero)
BASE_GEOJSON = r"D:\Datos\GeojsonZonas\zonificacionDistritosMITMA\zonificacion_distritos.geojson"
GEO_ID_COL = "ID"

# Parámetros estadísticos
INTERVALO_CONFIANZA_Z = 1.96  # 95%
MIN_N = 3  # mínimo nº días control para considerar (destino,hora)

# Salida visor
OUT_GEOJSON = str(MAP_DIR / "distritos_impacto_por_hora.geojson")
OUT_HTML = str(MAP_DIR / "visor_impacto_por_hora.html")


# =========================
# Helpers (formato como tu ejemplo)
# =========================
def to_zone_str(s: pd.Series) -> pd.Series:
    """Convierte zona a string, estilo tu ejemplo."""
    return (
        pd.to_numeric(s, errors="coerce")
        .astype("Int64")
        .astype(str)
        .str.zfill(5)
    )

def parse_miles_float(series: pd.Series) -> pd.Series:
    """'8.013' -> 8013 ; '8,3' -> 8.3 ; robusto a basura."""
    s = series.astype(str).str.strip()
    s = s.str.replace(".", "", regex=False).str.replace(",", ".", regex=False)
    return pd.to_numeric(s, errors="coerce").fillna(0.0).astype(float)

def download_file(url: str, dest: Path) -> bool:
    dest.parent.mkdir(parents=True, exist_ok=True)
    try:
        print(f"  Descargando: {url}")
        with requests.get(url, stream=True, timeout=120) as r:
            r.raise_for_status()
            with open(dest, "wb") as f:
                for chunk in r.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        f.write(chunk)
        return True
    except Exception as e:
        print(f"  ⚠️ Error descarga: {e}")
        return False

def build_url(fecha_str: str) -> str:
    d = datetime.strptime(fecha_str, "%Y-%m-%d").date()
    yyyymm = d.strftime("%Y-%m")
    yyyymmdd = d.strftime("%Y%m%d")
    return f"{BASE_URL}/{yyyymm}/{yyyymmdd}_Viajes_distritos.csv.gz"

def yyyymmdd(fecha_str: str) -> str:
    return datetime.strptime(fecha_str, "%Y-%m-%d").strftime("%Y%m%d")

def load_geo_ids(geojson_path: str, id_col: str) -> Set[str]:
    gdf = gpd.read_file(geojson_path)
    gdf[id_col] = gdf[id_col].astype(str).str.zfill(5)
    return set(gdf[id_col].astype(str))


# =========================
# Lectura MITMA en chunks -> agregado Wanda (destino,hora)
# =========================
def read_mitma_wanda_agg(
    gz_path: Path,
    distrito_wanda: str,
    valid_ids: Optional[Set[str]] = None,
    chunksize: int = 500_000,
) -> pd.DataFrame:
    """
    Lee el MITMA csv.gz por chunks, filtra origen=Wanda, excluye intradistrito,
    y (opcional) filtra destinos a los IDs del geojson.
    Devuelve DF: destino(str), periodo(int), viajes(float)
    """

    # MITMA puede venir como origin/destination/period/trips o como origen/destino/periodo/viajes.
    # Leemos header primero para decidir.
    head = pd.read_csv(gz_path, compression="gzip", sep="|", nrows=5, dtype="string")
    cols = [c.strip() for c in head.columns]

    # Detectar nombres
    if set(["origin","destination","period","trips"]).issubset(cols):
        usecols = ["origin","destination","period","trips"]
        ren = {"origin":"origen","destination":"destino","period":"periodo","trips":"viajes"}
    elif set(["origen","destino","periodo","viajes"]).issubset(cols):
        usecols = ["origen","destino","periodo","viajes"]
        ren = {}  # ya ok
    else:
        raise ValueError(f"Columnas inesperadas en {gz_path.name}: {cols[:20]}")

    chunks = pd.read_csv(
        gz_path,
        compression="gzip",
        sep="|",
        usecols=usecols,
        dtype="string",
        chunksize=chunksize,
        low_memory=True,
        engine="c",
        on_bad_lines="skip",
    )

    out = []
    wanda = str(distrito_wanda)

    for ch in chunks:
        if ren:
            ch = ch.rename(columns=ren)

        # Formato como tu ejemplo
        ch["origen"]  = to_zone_str(ch["origen"])
        ch["destino"] = to_zone_str(ch["destino"])
        ch["periodo"] = pd.to_numeric(ch["periodo"], errors="coerce").fillna(0).astype(int)
        ch["viajes"]  = parse_miles_float(ch["viajes"])

        # Filtrar origen wanda
        ch = ch[ch["origen"] == wanda]
        if ch.empty:
            continue

        # Excluir intradistrito (muy importante)
        ch = ch[ch["destino"] != ch["origen"]]
        if ch.empty:
            continue

        # Filtrar a IDs válidos del geojson (si quieres mapa de distritos Madrid)
        if valid_ids is not None:
            ch = ch[ch["destino"].isin(valid_ids)]
            if ch.empty:
                continue

        agg = ch.groupby(["destino","periodo"], as_index=False)["viajes"].sum()
        out.append(agg)

    if not out:
        return pd.DataFrame(columns=["destino","periodo","viajes"])

    res = pd.concat(out, ignore_index=True)
    res = res.groupby(["destino","periodo"], as_index=False)["viajes"].sum()
    return res


def descargar_y_agregar(fecha_str: str, valid_ids: Set[str]) -> Path:
    """Descarga el gz (si no existe) y guarda parquet agregado Wanda."""
    fnum = yyyymmdd(fecha_str)
    gz_path = DATA_DIR / f"{fnum}_Viajes_distritos.csv.gz"
    pq_path = DATA_DIR / f"{fnum}_wanda_agg.parquet"

    if pq_path.exists():
        print(f"✓ Ya existe: {pq_path.name}")
        return pq_path

    if not gz_path.exists():
        url = build_url(fecha_str)
        ok = download_file(url, gz_path)
        if not ok:
            return None

    print("  Procesando (chunks) -> agregado Wanda...")
    agg = read_mitma_wanda_agg(gz_path, DISTRITO_WANDA, valid_ids=valid_ids)
    agg["fecha"] = fnum
    agg.to_parquet(pq_path, index=False)

    # opcional: borrar gz
    gz_path.unlink(missing_ok=True)

    print(f"  ✓ Guardado: {pq_path.name} ({len(agg):,} filas)")
    return pq_path


# =========================
# Estadística esperado + impacto
# =========================
def expected_stats(df_control: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula media y IC95 por (destino,periodo). Devuelve DF:
    destino,periodo,n,media,std,ic_low,ic_high
    """
    g = df_control.groupby(["destino","periodo"])["viajes"]
    stats = g.agg(n="count", media="mean", std="std").reset_index()

    # std puede salir NaN si n=1
    stats["std"] = stats["std"].fillna(0.0)

    # Solo donde hay suficiente n
    stats = stats[stats["n"] >= MIN_N].copy()

    # IC95 sobre la media
    se = stats["std"] / np.sqrt(stats["n"])
    margen = INTERVALO_CONFIANZA_Z * se
    stats["ic_low"] = (stats["media"] - margen).clip(lower=0)
    stats["ic_high"] = stats["media"] + margen

    return stats


def impacto_derbi(df_derbi: pd.DataFrame, stats: pd.DataFrame) -> pd.DataFrame:
    """
    Join derbi con stats y calcula:
    diff_abs, diff_pct, z, significativo (derbi > ic_high)
    """
    m = df_derbi.merge(stats, on=["destino","periodo"], how="inner")
    m["diff_abs"] = m["viajes"] - m["media"]
    m["diff_pct"] = np.where(m["media"] > 0, (m["diff_abs"] / m["media"]) * 100, 0.0)

    # z-score (evitar división por 0)
    m["z"] = np.where(m["std"] > 0, (m["viajes"] - m["media"]) / m["std"], 0.0)
    m["significativo"] = m["viajes"] > m["ic_high"]

    return m


# =========================
# GeoJSON + HTML (como tu visor)
# =========================
def build_geojson_with_hour_dict(
    base_geojson: str,
    df: pd.DataFrame,
    id_col: str,
    zone_col: str,
    value_col: str,
    hour_col: str,
    out_geojson: str,
):
    """
    df: columnas [zone_col, hour_col, value_col] con una fila por (zona,hora)
    crea propiedades: impacto_por_hora { "0":..., "1":..., ...}
    """
    # 1) Agregar por zona y hora (por si acaso)
    agg = df.groupby([zone_col, hour_col], dropna=False)[value_col].sum().reset_index()

    # 2) dict por zona
    by_zone = {}
    for zid, sub in agg.groupby(zone_col):
        d = {str(int(h)): float(v) for h, v in zip(sub[hour_col], sub[value_col])}
        for h in range(24):
            d.setdefault(str(h), 0.0)
        by_zone[str(zid)] = d

    # 3) leer base geojson
    gdf = gpd.read_file(base_geojson)
    if gdf.crs is None:
        # ajusta si tu fichero lo necesita
        gdf = gdf.set_crs(epsg=3042)

    gdf[id_col] = gdf[id_col].astype(str).str.zfill(5)
    gdf = gdf.to_crs(epsg=4326)

    geo = json.loads(gdf.to_json())

    max_v = 0.0
    for feat in geo["features"]:
        zid = str(feat["properties"].get(id_col, "")).zfill(5)
        horas = by_zone.get(zid, {str(h): 0.0 for h in range(24)})
        feat["properties"]["impacto_por_hora"] = horas
        max_v = max(max_v, max(horas.values()))

    geo["properties"] = geo.get("properties", {})
    geo["properties"]["max_value"] = float(max_v)
    geo["properties"]["value_col"] = value_col
    geo["properties"]["zona_col"] = zone_col
    geo["properties"]["hour_col"] = hour_col

    Path(out_geojson).parent.mkdir(parents=True, exist_ok=True)
    with open(out_geojson, "w", encoding="utf-8") as f:
        json.dump(geo, f, ensure_ascii=False)

    print("✅ GeoJSON impacto creado:", out_geojson, "max_value=", max_v)


def write_leaflet_html(out_html: str, out_geojson: str, value_label: str = "impacto"):
    """
    HTML simple con slider (0-23). Carga el geojson local (mismo directorio).
    """
    out_dir = Path(out_html).parent
    out_dir.mkdir(parents=True, exist_ok=True)

    geojson_name = Path(out_geojson).name
    geojson_target = out_dir / geojson_name

    # copiar geojson al mismo dir del html para fetch
    if str(geojson_target).lower() != str(Path(out_geojson)).lower():
        with open(out_geojson, "r", encoding="utf-8") as fi, open(geojson_target, "w", encoding="utf-8") as fo:
            fo.write(fi.read())

    html = f"""<!doctype html>
<html lang="es"><head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Visor impacto por hora</title>
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"/>
<style>
html,body,#map{{height:100%;margin:0}}
.control{{position:absolute;top:10px;left:10px;z-index:1000;background:#fff;padding:10px 12px;border-radius:8px;box-shadow:0 2px 10px rgba(0,0,0,.15);font-family:system-ui;min-width:260px}}
.legend{{position:absolute;bottom:20px;left:10px;z-index:1000;background:#fff;padding:10px 12px;border-radius:8px;box-shadow:0 2px 10px rgba(0,0,0,.15);font-family:system-ui;font-size:12px}}
.swatch{{width:14px;height:14px;display:inline-block;margin-right:6px;vertical-align:middle;border:1px solid rgba(0,0,0,.15)}}
</style></head><body>
<div id="map"></div>
<div class="control">
  <div><strong>Hora:</strong> <span id="hourLabel">0</span></div>
  <input id="hour" type="range" min="0" max="23" step="1" value="0" style="width:100%"/>
  <div style="color:#666;font-size:12px">Colorea por <strong>{value_label}</strong> (0 = transparente)</div>
</div>
<div class="legend" id="legend"></div>

<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<script>
const GEOJSON_URL = "{geojson_name}";
const hourInput = document.getElementById("hour");
const hourLabel = document.getElementById("hourLabel");
const legendDiv = document.getElementById("legend");

function colorRamp(t){{
  const stops=[[255,255,204],[255,237,160],[254,217,118],[254,178,76],[253,141,60],[252,78,42],[227,26,28],[177,0,38],[128,0,38]];
  t=Math.max(0,Math.min(1,t));
  const idx=t*(stops.length-1), i0=Math.floor(idx), i1=Math.min(stops.length-1,i0+1), a=idx-i0;
  const c0=stops[i0], c1=stops[i1];
  const r=Math.round(c0[0]+a*(c1[0]-c0[0]));
  const g=Math.round(c0[1]+a*(c1[1]-c0[1]));
  const b=Math.round(c0[2]+a*(c1[2]-c0[2]));
  return `rgb(${{r}},${{g}},${{b}})`;
}}
function styleFeature(feature, hour, maxValue){{
  const d = feature.properties.impacto_por_hora || {{}};
  const v = (d[String(hour)] ?? 0);
  const t = maxValue>0 ? (v/maxValue) : 0;
  return {{color:"#666",weight:0.6,fillColor:(v>0?colorRamp(t):"transparent"),fillOpacity:(v>0?0.85:0)}};
}}
function makeLegend(maxValue){{
  const steps=5;
  let h=`<div><strong>Leyenda ({value_label})</strong></div>`;
  for(let i=0;i<steps;i++){{ 
    const v0=maxValue*i/steps, v1=maxValue*(i+1)/steps, col=colorRamp((i+1)/steps);
    h += `<div><span class="swatch" style="background:${{col}}"></span>${{Math.round(v0)}} – ${{Math.round(v1)}}</div>`;
  }}
  h += `<div style="color:#666">0 = transparente</div>`;
  legendDiv.innerHTML=h;
}}

const map=L.map("map");
L.tileLayer("https://{{s}}.basemaps.cartocdn.com/light_all/{{z}}/{{x}}/{{y}}{{r}}.png",{{attribution:"&copy; OSM &copy; CARTO"}}).addTo(map);

let layer=null, geo=null, maxValue=1;

fetch(GEOJSON_URL).then(r=>r.json()).then(data=>{{
  geo=data;
  maxValue=(data.properties && data.properties.max_value) ? data.properties.max_value : 1;
  layer=L.geoJSON(geo,{{
    style:(f)=>styleFeature(f, Number(hourInput.value), maxValue),
    onEachFeature:(f, lyr)=>{{
      const id=f.properties.ID || "";
      lyr.on("mouseover", ()=>{{
        const v=(f.properties.impacto_por_hora||{{}})[String(hourInput.value)] ?? 0;
        lyr.bindTooltip(`ID: ${{id}}<br>{value_label}: ${{v}}`,{{sticky:true}}).openTooltip();
      }});
    }}
  }}).addTo(map);
  map.fitBounds(layer.getBounds());
  makeLegend(maxValue);
}});

function update(){{
  const h=Number(hourInput.value);
  hourLabel.textContent=h;
  if(!layer) return;
  layer.setStyle((f)=>styleFeature(f,h,maxValue));
}}
hourInput.addEventListener("input", update);
</script>
</body></html>
"""
    with open(out_html, "w", encoding="utf-8") as f:
        f.write(html)
    print("✅ HTML creado:", out_html)
    print("ℹ️ Ábrelo con servidor local: python -m http.server 8000 (en la carpeta del HTML)")


# =========================
# RUN
# =========================
print("="*70)
print("Cargando IDs válidos del GeoJSON (para filtrar destinos)")
print("="*70)
VALID_IDS = load_geo_ids(BASE_GEOJSON, GEO_ID_COL)
print("IDs válidos:", len(VALID_IDS))

print("="*70)
print("PASO 1: DESCARGA + AGREGADO (WANDA)")
print("="*70)

print(f"\n📅 Derbi: {FECHA_DERBI}")
derbi_pq = descargar_y_agregar(FECHA_DERBI, VALID_IDS)
if derbi_pq is None:
    raise RuntimeError("No se pudo descargar/procesar el derbi.")

control_pqs = []
print(f"\n📅 Controles: {len(FECHAS_CONTROL)}")
for f in FECHAS_CONTROL:
    print(f"\n  {f}")
    p = descargar_y_agregar(f, VALID_IDS)
    if p is not None:
        control_pqs.append(p)

if len(control_pqs) < MIN_N:
    print(f"⚠️ Ojo: solo tienes {len(control_pqs)} controles. MIN_N={MIN_N} para estadística estable.")

print("="*70)
print("PASO 2: CARGA PARQUETS -> DF CONTROL/DERBI")
print("="*70)

dfs = []
for p in control_pqs:
    df = pd.read_parquet(p)
    dfs.append(df[["fecha","destino","periodo","viajes"]])
df_control = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame(columns=["fecha","destino","periodo","viajes"])

df_derbi = pd.read_parquet(derbi_pq)[["fecha","destino","periodo","viajes"]]

# Normaliza tipos
df_control["destino"] = df_control["destino"].astype(str).str.zfill(5)
df_derbi["destino"]   = df_derbi["destino"].astype(str).str.zfill(5)
df_control["periodo"] = pd.to_numeric(df_control["periodo"], errors="coerce").fillna(0).astype(int)
df_derbi["periodo"]   = pd.to_numeric(df_derbi["periodo"], errors="coerce").fillna(0).astype(int)
df_control["viajes"]  = pd.to_numeric(df_control["viajes"], errors="coerce").fillna(0.0).astype(float)
df_derbi["viajes"]    = pd.to_numeric(df_derbi["viajes"], errors="coerce").fillna(0.0).astype(float)

print("Control filas:", len(df_control), "Derbi filas:", len(df_derbi))

print("="*70)
print("PASO 3: ESPERADO (MEDIA + IC95) y IMPACTO")
print("="*70)

stats = expected_stats(df_control)
stats.to_parquet(ANALYSIS_DIR / "estadisticas_esperadas.parquet", index=False)

imp = impacto_derbi(df_derbi, stats)
imp.to_parquet(ANALYSIS_DIR / "impacto_derbi.parquet", index=False)

# Impacto positivo y significativo
imp_sig = imp[(imp["significativo"]) & (imp["diff_abs"] > 0)].copy()
imp_sig.to_parquet(ANALYSIS_DIR / "impacto_significativo.parquet", index=False)

print("\n📊 Resumen:")
print("  total destino-hora analizados:", len(imp))
print("  impactos positivos significativos:", len(imp_sig))
print("  destinos afectados:", imp_sig["destino"].nunique())

print("\n🔥 Top 10 por impacto absoluto:")
top_abs = imp_sig.nlargest(10, "diff_abs")[["destino","periodo","viajes","media","diff_abs","diff_pct","z"]]
print(top_abs.to_string(index=False))

print("\n📈 Top 10 por impacto porcentual:")
top_pct = imp_sig.nlargest(10, "diff_pct")[["destino","periodo","viajes","media","diff_abs","diff_pct","z"]]
print(top_pct.to_string(index=False))

# =========================
# PASO 4: GeoJSON/HTML del impacto por hora
# =========================
print("="*70)
print("PASO 4: EXPORT GEOJSON + HTML (IMPACTO POR HORA)")
print("="*70)

# Para el visor queremos, por distrito destino y hora, el impacto absoluto (diff_abs) (solo positivo)
impact_map = imp.copy()
impact_map["impacto"] = impact_map["diff_abs"].clip(lower=0)
impact_map = impact_map[["destino","periodo","impacto"]].copy()

build_geojson_with_hour_dict(
    base_geojson=BASE_GEOJSON,
    df=impact_map,
    id_col=GEO_ID_COL,
    zone_col="destino",
    value_col="impacto",
    hour_col="periodo",
    out_geojson=OUT_GEOJSON
)

write_leaflet_html(OUT_HTML, OUT_GEOJSON, value_label="impacto (viajes extra)")
print("✅ Listo. Abre el HTML con servidor local.")
