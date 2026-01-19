import os
from pathlib import Path
from datetime import datetime, timedelta

import pandas as pd
import requests


OUTPUT_DIR = Path(r"C:\Users\khora\Downloads")

BASE_URL = (
    "https://movilidad-opendata.mitma.es/"
    "estudios_basicos/por-distritos/viajes/ficheros-diarios"
)

START_DATE = "2025-02-14"
END_DATE   = "2025-02-14"


def daterange(start: str, end: str):
    d0 = datetime.strptime(start, "%Y-%m-%d").date()
    d1 = datetime.strptime(end, "%Y-%m-%d").date()
    d = d0
    while d <= d1:
        yield d
        d += timedelta(days=1)


def download_file(url: str, dest: Path) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    with requests.get(url, stream=True, timeout=120) as r:
        r.raise_for_status()
        with open(dest, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)


def read_mitma_csv_gz(path: Path) -> pd.DataFrame:
    """
    Lee el CSV.gz probando separadores típicos del MITMA: '|', ';', ','.
    Se queda con el separador que produzca MÁS columnas.
    """
    best_df = None
    best_cols = 0
    best_sep = None

    for sep in ["|"]:
        try:
            df = pd.read_csv(path, compression="gzip", sep=sep, dtype=str, low_memory=False)
            ncols = df.shape[1]
            if ncols > best_cols:
                best_df = df
                best_cols = ncols
                best_sep = sep
        except Exception:
            continue

    if best_df is None or best_cols < 6:
        raise ValueError(
            f"No se pudo leer {path.name} con separadores esperados. "
            f"Columnas obtenidas: {best_cols}. Último sep probado: {best_sep}"
        )

    return best_df


def parse_miles_int(series: pd.Series) -> pd.Series:
    """
    Para conteos tipo '2.788' => 2788
    Si viene con coma decimal (raro en viajes), lo redondea.
    """
    s = series.astype("string").fillna("0").str.strip()
    has_comma = s.str.contains(",", na=False)

    s_comma = (
        s.where(has_comma, "0")
        .str.replace(".", "", regex=False)
        .str.replace(",", ".", regex=False)
    )

    s_nocomma = (
        s.where(~has_comma, "0")
        .str.replace(r"[^\d\-]", "", regex=True)
    )

    merged = s_nocomma.where(~has_comma, s_comma)
    num = pd.to_numeric(merged, errors="coerce").fillna(0)
    return num.round().astype("int64")


def parse_miles_float(series: pd.Series) -> pd.Series:
    """
    Para km tipo '4.678' => 4678 (si fueran km enteros) o '1.234,56' => 1234.56
    Lo devuelve float.
    """
    s = series.astype("string").fillna("0").str.strip()
    has_comma = s.str.contains(",", na=False)

    s_comma = (
        s.where(has_comma, "0")
        .str.replace(".", "", regex=False)
        .str.replace(",", ".", regex=False)
    )
    s_nocomma = (
        s.where(~has_comma, "0")
        .str.replace(".", "", regex=False)   # en km suele ser miles con punto
        .str.replace(r"[^\d\-]", "", regex=True)
    )
    merged = s_nocomma.where(~has_comma, s_comma)
    return pd.to_numeric(merged, errors="coerce").fillna(0.0).astype(float)


def normalize_columns(df: pd.DataFrame, yyyymmdd: str) -> pd.DataFrame:
    """
    Deja el dataframe con columnas estándar:
      fecha, origen, destino, periodo, residencia, renta, edad, sexo, viajes, viajes_km
    Todas como string salvo viajes(int) y viajes_km(float).
    """
    colmap = {
        # fecha
        "date": "fecha",
        "fecha": "fecha",

        # zonas
        "origin": "origen",
        "destination": "destino",
        "origen": "origen",
        "destino": "destino",

        # hora/periodo
        "period": "periodo",
        "periodo": "periodo",

        # desagregaciones
        "residence": "residencia",
        "residencia": "residencia",
        "income": "renta",
        "renta": "renta",
        "age": "edad",
        "edad": "edad",
        "sex": "sexo",
        "sexo": "sexo",

        # valores
        "trips": "viajes",
        "viajes": "viajes",
        "trips_km": "viajes_km",
        "viajes_km": "viajes_km",
    }

    df = df.rename(columns={c: colmap[c] for c in df.columns if c in colmap}).copy()

    if "fecha" not in df.columns:
        df["fecha"] = yyyymmdd
    df["fecha"] = df["fecha"].astype(str).str.replace("-", "", regex=False)

    required = ["fecha", "origen", "destino", "periodo", "viajes"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise KeyError(f"Faltan columnas esperadas: {missing}. Columnas presentes: {list(df.columns)}")

    # valores numéricos con miles
    df["viajes"] = parse_miles_int(df["viajes"])
    if "viajes_km" in df.columns:
        df["viajes_km"] = parse_miles_float(df["viajes_km"])
    else:
        df["viajes_km"] = 0.0

    # columnas opcionales (si no existen, las creamos vacías)
    for opt in ["residencia", "renta", "edad", "sexo"]:
        if opt not in df.columns:
            df[opt] = ""

    # tipado de zonas/periodo
    df["origen"] = df["origen"].astype(str)
    df["destino"] = df["destino"].astype(str)
    df["periodo"] = pd.to_numeric(df["periodo"], errors="coerce").fillna(0).astype(int)

    cols = ["fecha", "origen", "destino", "periodo", "residencia", "renta", "edad", "sexo", "viajes", "viajes_km"]
    return df[cols]


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    for d in daterange(START_DATE, END_DATE):
        yyyymm = d.strftime("%Y-%m")
        yyyymmdd = d.strftime("%Y%m%d")

        # Nombre típico (ajusta si el portal usa otro)
        url = f"{BASE_URL}/{yyyymm}/{yyyymmdd}_Viajes_distritos.csv.gz"

        gz_path = OUTPUT_DIR / f"{yyyymmdd}_Viajes_distritos.csv.gz"
        parquet_path = OUTPUT_DIR / f"{yyyymmdd}_Viajes_distritos.parquet"

        if not gz_path.exists() and not parquet_path.exists():
            print(f"Descargando {url}")
            try:
                download_file(url, gz_path)
            except requests.HTTPError as e:
                print(f"No se pudo descargar ({e}). Puede que no exista ese día o el nombre cambie.")
                continue
            print(f"Guardado: {gz_path}")
        else:
            print(f"Ya existe (gz o parquet) para {yyyymmdd}")

        if not parquet_path.exists():
            print(f"Convirtiendo a parquet: {parquet_path.name}")
            df = read_mitma_csv_gz(gz_path)
            df = normalize_columns(df, yyyymmdd)
            df.to_parquet(parquet_path, index=False)
            print(f"OK: {parquet_path}")

            # borra gz para ahorrar espacio
            if gz_path.exists():
                gz_path.unlink()
                print(f"Eliminado: {gz_path.name}")
        else:
            print(f"Ya existe: {parquet_path}")

    print("Terminado.")


if __name__ == "__main__":
    main()
