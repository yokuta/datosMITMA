import pandas as pd

# Cargar los datos de los días clave del Orgullo por año
df2022 = pd.read_parquet("D://Datos//Movilidad//MinisteriodeTransportes//EstudiosBasicos//Pernoctaciones//20220604_Pernoctaciones_distritos.parquet")
df2023 = pd.read_parquet("D://Datos//Movilidad//MinisteriodeTransportes//EstudiosBasicos//Pernoctaciones//20230218_Pernoctaciones_distritos.parquet")
df2024 = pd.read_parquet("D://Datos//Movilidad//MinisteriodeTransportes//EstudiosBasicos//Pernoctaciones//20240210_Pernoctaciones_distritos.parquet")
df2025 = pd.read_parquet("D://Datos//Movilidad//MinisteriodeTransportes//EstudiosBasicos//Pernoctaciones//20250301_Pernoctaciones_distritos.parquet")

# Añadir columna de año a cada DataFrame
df2022['año'] = 2022
df2023['año'] = 2023
df2024['año'] = 2024
df2025['año'] = 2025


# Unirlos en uno solo
df = pd.concat([df2022,df2023,df2024,df2025], ignore_index=True)

# Definir los distritos objetivo 
zonas_distritos = ['1101201','1101202','1101203','1101204','1101205','1101206','1101207','1101208','1101209','1101210']

# Filtrar solo pernoctaciones en esos distritos
dffiltrado = df[df['zona_pernoctacion'].isin(zonas_distritos)].copy()

# Asegurar que 'personas' es numérico
dffiltrado['personas'] = pd.to_numeric(dffiltrado['personas'], errors='coerce')

# Agrupar por zona_residencia y año
df_origen_anual = dffiltrado.groupby(['zona_residencia', 'año'])['personas'].sum().reset_index()

# Pivotar para tener una columna por año
df_pivot = df_origen_anual.pivot(index='zona_residencia', columns='año', values='personas').fillna(0)

# Reordenar columnas por año
df_pivot = df_pivot[[2022,2023,2024,2025]]


# Mostrar top 20 zonas con más pernoctaciones en total
df_pivot['total'] = df_pivot.sum(axis=1)
df_top20 = df_pivot.sort_values(by='total', ascending=False).drop(columns='total')
print(df_top20.head(30))
