import gzip
import shutil
from pathlib import Path

# Cambia esta ruta por tu directorio
directorio = Path(r"C:\Users\khora\Downloads\viajes")

for gz_file in directorio.glob("*.gz"):
    output_file = gz_file.with_suffix("")  # quita el .gz (ej: .csv.gz -> .csv)

    print(f"Descomprimiendo: {gz_file.name} -> {output_file.name}")

    with gzip.open(gz_file, "rb") as f_in:
        with open(output_file, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)

print(" Listo: todos los .gz han sido descomprimidos.")
