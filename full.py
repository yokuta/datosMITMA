import os
import pandas as pd
import requests
from pathlib import Path
from datetime import datetime, timedelta

# --- CONFIGURACIÓN ---
OUTPUT_DIR = Path(r"C:\Users\khora\Downloads\EstudioExodo2025")
BASE_URL = "https://movilidad-opendata.mitma.es/estudios_basicos/por-distritos/pernoctaciones/ficheros-diarios"
START_DATE = "2025-03-01"
END_DATE = "2025-05-01"

# Diccionario de capitales (Prefijo INE de 5 dígitos)
CAPITALES = {
    "Madrid": "28079", "Barcelona": "08019", "Valencia": "46250", 
    "Sevilla": "41091", "Zaragoza": "50297", "Malaga": "29067",
    "Murcia": "30030", "Palma": "07040", "Bilbao": "48020",
    "Alicante": "03014", "Cordoba": "14021", "Valladolid": "47186",
    "Vigo": "36057", "Gijon": "33024", "Pamplona": "31201"
    # Puedes añadir todas las que necesites siguiendo el mismo formato
}

# --- FUNCIONES DE APOYO ---

def daterange(start: str, end: str):
    d0 = datetime.strptime(start, "%Y-%m-%d").date()
    d1 = datetime.strptime(end, "%Y-%m-%d").date()
    curr = d0
    while curr <= d1:
        yield curr
        curr += timedelta(days=1)

def download_and_convert(d: datetime.date):
    yyyymm = d.strftime("%Y-%m")
    yyyymmdd = d.strftime("%Y%m%d")
    url = f"{BASE_URL}/{yyyymm}/{yyyymmdd}_Pernoctaciones_distritos.csv.gz"
    gz_path = OUTPUT_DIR / f"{yyyymmdd}.csv.gz"
    parquet_path = OUTPUT_DIR / f"{yyyymmdd}.parquet"

    if parquet_path.exists():
        return parquet_path

    try:
        response = requests.get(url, stream=True, timeout=60)
        response.raise_for_status()
        with open(gz_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=1024*1024):
                f.write(chunk)
        
        # Lectura flexible (basada en tu código)
        df = None
        for sep in ["|", ";", ","]:
            try:
                df = pd.read_csv(gz_path, compression="gzip", sep=sep, dtype=str)
                if df.shape[1] >= 4: break
            except: continue
        
        if df is not None:
            # Normalizar nombres de columnas
            col_map = {"date":"fecha", "residence_area":"zona_residencia", 
                       "overnight_stay_area":"zona_pernoctacion", "people":"personas"}
            df.rename(columns=lambda x: col_map.get(x, x), inplace=True)
            
            # Guardar y limpiar
            df.to_parquet(parquet_path, index=False)
            gz_path.unlink()
            return parquet_path
    except Exception as e:
        print(f"Error procesando {yyyymmdd}: {e}")
        return None

# --- PROCESO PRINCIPAL ---

def ejecutar_estudio():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    resultados = []

    print(f"Iniciando estudio desde {START_DATE} hasta {END_DATE}...")

    for dia in daterange(START_DATE, END_DATE):
        path_pq = download_and_convert(dia)
        if path_pq:
            print(f"Analizando día: {dia}")
            df = pd.read_parquet(path_pq)
            df['personas'] = pd.to_numeric(df['personas'], errors='coerce').fillna(0)
            
            for ciudad, prefijo in CAPITALES.items():
                # 1. Residentes de la ciudad
                residentes = df[df['zona_residencia'].str.startswith(prefijo)]
                # 2. Pernoctan fuera (no empieza por el mismo prefijo)
                exodo = residentes[~residentes['zona_pernoctacion'].str.startswith(prefijo)]
                
                total_exodo = int(exodo['personas'].sum())
                
                resultados.append({
                    "fecha": dia,
                    "ciudad": ciudad,
                    "exodo_personas": total_exodo
                })
            
            # Opcional: Borrar parquets tras analizar para no llenar el disco
            # path_pq.unlink() 

    # --- GENERAR INFORME FINAL ---
    df_final = pd.DataFrame(resultados)
    
    # Encontrar el día de máximo éxodo por ciudad
    maximos = df_final.loc[df_final.groupby('ciudad')['exodo_personas'].idxmax()]
    
    print("\n--- RESULTADOS: DÍAS DE MÁXIMO ÉXODO POR CIUDAD ---")
    print(maximos.sort_values(by="exodo_personas", ascending=False))
    
    # Guardar a CSV para tu noticia
    maximos.to_csv(OUTPUT_DIR / "maximos_exodos_2025.csv", index=False)
    print(f"\nInforme guardado en: {OUTPUT_DIR / 'maximos_exodos_2025.csv'}")

if __name__ == "__main__":
    ejecutar_estudio()