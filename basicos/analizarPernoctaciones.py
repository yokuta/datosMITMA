import pandas as pd

# Cargar los datos de los días clave del Orgullo por año
df2022 = pd.read_parquet("D://Datos//Movilidad//MinisteriodeTransportes//EstudiosBasicos//Pernoctaciones//20220709_Pernoctaciones_distritos.parquet")

# Añadir columna de año a cada DataFrame
df2022['año'] = 2022

# Unirlos en uno solo
df = pd.concat([df2022], ignore_index=True)

# Definir los distritos objetivo 
zonas_distritos = ['2807901', '2807902', '2807903', '2807904', '2807907']

# Filtrar solo pernoctaciones en esos distritos
dfMadrid = df[df['zona_pernoctacion'].isin(zonas_distritos)].copy()

# Asegurar que 'personas' es numérico
dfMadrid['personas'] = pd.to_numeric(dfMadrid['personas'], errors='coerce')

# Agrupar por zona_residencia y año
df_origen_anual = dfMadrid.groupby(['zona_residencia', 'año'])['personas'].sum().reset_index()

# Pivotar para tener una columna por año
df_pivot = df_origen_anual.pivot(index='zona_residencia', columns='año', values='personas').fillna(0)

# Reordenar columnas por año
df_pivot = df_pivot[[2022]]

# Guardar el resultado a CSV
df_pivot.to_csv("D://Datos//juan.csv")

# Mostrar top 20 zonas con más pernoctaciones en total
df_pivot['total'] = df_pivot.sum(axis=1)
df_top20 = df_pivot.sort_values(by='total', ascending=False).drop(columns='total')
print(df_top20.head(20))
