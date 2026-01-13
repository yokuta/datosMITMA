import os
from pathlib import Path
from datetime import datetime, timedelta

import pandas as pd
import requests


OUTPUT_DIR = Path(r"C:\Users\khora\Downloads")

BASE_URL = (
    "https://movilidad-opendata.mitma.es/"
    "estudios_basicos/por-distritos/pernoctaciones/ficheros-diarios"
)

START_DATE = "2025-02-17"
END_DATE   = "2025-02-17"


def daterange(start: str, end: str):
    d0 = datetime.strptime(start, "%Y-%m-%d").date()
    d1 = datetime.strptime(end, "%Y-%m-%d").date()
    d = d0
    while d <= d1:
        yield d
        d += timedelta(days=1)


def download_file(url: str, dest: Path) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    with requests.get(url, stream=True, timeout=60) as r:
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

    for sep in ["|", ";", ","]:
        try:
            df = pd.read_csv(path, compression="gzip", sep=sep, dtype=str)
            ncols = df.shape[1]
            if ncols > best_cols:
                best_df = df
                best_cols = ncols
                best_sep = sep
        except Exception:
            continue

    if best_df is None or best_cols < 4:
        raise ValueError(
            f"No se pudo leer {path.name} con separadores esperados. "
            f"Columnas obtenidas: {best_cols}. Último sep probado: {best_sep}"
        )

    return best_df


def normalize_columns(df: pd.DataFrame, yyyymmdd: str) -> pd.DataFrame:
    """
    Deja el dataframe con columnas:
      fecha, zona_residencia, zona_pernoctacion, personas
    (todas como string, como tus parquets existentes).
    """
    colmap = {
        "date": "fecha",
        "residence_area": "zona_residencia",
        "overnight_stay_area": "zona_pernoctacion",
        "people": "personas",
        "zona_residencia": "zona_residencia",
        "zona_pernoctacion": "zona_pernoctacion",
        "personas": "personas",
        "fecha": "fecha",
    }

    df = df.rename(columns={c: colmap[c] for c in df.columns if c in colmap}).copy()

    # si no viene fecha, la imponemos
    if "fecha" not in df.columns:
        df["fecha"] = yyyymmdd

    # normalizar fecha a YYYYMMDD
    df["fecha"] = df["fecha"].astype(str).str.replace("-", "", regex=False)

    required = ["fecha", "zona_residencia", "zona_pernoctacion", "personas"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise KeyError(f"Faltan columnas esperadas: {missing}. Columnas presentes: {list(df.columns)}")

    return df[required].astype(str)


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    for d in daterange(START_DATE, END_DATE):
        yyyymm = d.strftime("%Y-%m")
        yyyymmdd = d.strftime("%Y%m%d")

        url = f"{BASE_URL}/{yyyymm}/{yyyymmdd}_Pernoctaciones_distritos.csv.gz"
        gz_path = OUTPUT_DIR / f"{yyyymmdd}_Pernoctaciones_distritos.csv.gz"
        parquet_path = OUTPUT_DIR / f"{yyyymmdd}_Pernoctaciones_distritos.parquet"

        if not gz_path.exists():
            print(f"Descargando {url}")
            download_file(url, gz_path)
            print(f"Guardado: {gz_path}")
        else:
            print(f"Ya existe: {gz_path}")

        if not parquet_path.exists():
            print(f"Convirtiendo a parquet: {parquet_path.name}")
            df = read_mitma_csv_gz(gz_path)
            df = normalize_columns(df, yyyymmdd)
            df.to_parquet(parquet_path, index=False)
            print(f"OK: {parquet_path}")
            if gz_path.exists():
                gz_path.unlink()
                print(f"Eliminado: {gz_path.name}")
        else:
            print(f"Ya existe: {parquet_path}")

    print("Terminado.")


if __name__ == "__main__":
    main()
