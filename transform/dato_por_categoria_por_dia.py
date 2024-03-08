from extract.db import fetch_data
from transform.ponderadores import ponderadores_inflacion_actualizado, ponderadores_alquileres
import ast
import pandas as pd

query_precios_supermercado = """
SELECT p.date, cp.categoria_indice, sum(p.precio)/count(p.producto) as precio_promedio
FROM `slowpoke-v1`.precios p
JOIN `slowpoke-v1`.categorias_productos cp ON p.producto = cp.productos
WHERE p.date >= '2024-03-01'
GROUP BY p.date, cp.categoria_indice
"""

query_alquileres = """
SELECT * FROM `slowpoke-v1`.alquileres
"""

query_dolar = """
SELECT date, dolar_blue FROM `slowpoke-v1`.dolar;
"""
precios_supermercados = fetch_data(query_precios_supermercado)


ponderadores_aplanados = {}
for key, value in ponderadores_inflacion_actualizado.items():
    if isinstance(value, dict):
        for sub_key, sub_value in value.items():
            ponderadores_aplanados[sub_key] = sub_value
    else:
        ponderadores_aplanados[key] = value

# Multiplicamos sum(p.precio) por el ponderador correspondiente
precios_supermercados['sum_ponderado'] = precios_supermercados.apply(lambda row: row['precio_promedio'] * ponderadores_aplanados.get(row['categoria_indice'], 1), axis=1)

alquileres = fetch_data(query_alquileres)
dolar = fetch_data(query_dolar)
def corregir_valores(x):
    if x['compra'] > 10000:  # Si compra es evidentemente un error
        x['compra'] /= 100
    if x['venta'] > 10000:  # Si venta es evidentemente un error
        x['venta'] /= 100
    return (x['venta'] + x['compra']) / 2
dolar['dolar_blue'] = dolar['dolar_blue'].apply(ast.literal_eval)

dolar['promedio'] = dolar['dolar_blue'].apply(lambda x: (x['venta'] + x['compra']) / 2)
dolar['promedio'] = dolar['dolar_blue'].apply(corregir_valores)



# Re-definir la función de conversión
def convert_currency(row):
    if row['moneda_alquiler'] == 'USD':
        row['alquiler'] = row['alquiler'] * row['promedio']
        row['moneda_alquiler'] = '$'  # Cambiar la moneda a pesos argentinos después de la conversión
    if row['moneda_expensas'] == 'USD' and pd.notnull(row['expensas']):
        row['expensas'] = row['expensas'] * row['promedio']
        row['moneda_expensas'] = '$'
        pass
    return row

# Corregir el tipo de dato de 'date' en df2 para que coincida con df1
dolar['date'] = dolar['date'].dt.date
alquileres['date'] = alquileres['date'].dt.date
# Intentar el merge nuevamente
merged_df = alquileres.merge(dolar, on='date', how='left')

df_alquileres = merged_df.apply(convert_currency, axis=1)


def aplicar_ponderador(row):
    # Obtener el ponderador para la localidad de la fila
    ponderador = ponderadores_alquileres.get(row['localidad'], 1)  # Usar 1 como valor predeterminado si no se encuentra la localidad
    # Aplicar el ponderador al valor del alquiler
    row['alquiler'] *= ponderador
    return row

# Aplicar la función a cada fila del DataFrame
df_alquileres_ponderados = df_alquileres.apply(aplicar_ponderador, axis=1)

df_alquileres_ponderados[['id', 'date', 'alquiler', 'expensas', 'localidad']]

#group by date and sum alquiler
df_alquileres_ponderados = df_alquileres_ponderados.groupby('date').agg({'alquiler': 'mean', 'expensas': 'mean'}).reset_index()


# Paso 1: Crear DataFrame completo con rango de fechas
rango_fechas = pd.date_range(start=df_alquileres_ponderados['date'].min(), end=df_alquileres_ponderados['date'].max())
df_fechas_completo = pd.DataFrame({'date': rango_fechas})

# Convertir la columna 'date' en df_fechas_completo y df_alquileres_ponderados a datetime64[ns] si aún no lo son
df_fechas_completo['date'] = pd.to_datetime(df_fechas_completo['date'])
df_alquileres_ponderados['date'] = pd.to_datetime(df_alquileres_ponderados['date'])

# Ahora que ambos tienen el mismo tipo de dato en 'date', intentar hacer el merge nuevamente
df_merge = df_fechas_completo.merge(df_alquileres_ponderados, on='date', how='left')

# Paso 3: Llenar valores faltantes manualmente para cada columna numérica
for col in ['alquiler', 'expensas']:
    # Identificar índices de filas con valores NaN
    indices_nan = df_merge[df_merge[col].isna()].index
    
    for i in indices_nan:
        # Encontrar el valor anterior y posterior no-NaN más cercanos
        prev_val = df_merge[col][:i].dropna().last_valid_index()
        next_val = df_merge[col][i:].dropna().first_valid_index()
        
        # Calcular el promedio si ambos valores existen, sino usar el valor disponible
        if pd.notna(prev_val) and pd.notna(next_val):
            df_merge.loc[i, col] = (df_merge.loc[prev_val, col] + df_merge.loc[next_val, col]) / 2
        elif pd.notna(prev_val):
            df_merge.loc[i, col] = df_merge.loc[prev_val, col]
        elif pd.notna(next_val):
            df_merge.loc[i, col] = df_merge.loc[next_val, col]

# El DataFrame df_merge ahora tiene las fechas completadas y los valores faltantes rellenados
df_merge['date'] = pd.to_datetime(df_merge['date'])

# Encontrar la fecha máxima en df_merge y la fecha de hoy
ultima_fecha = df_merge['date'].max()
fecha_hoy = pd.Timestamp('today').normalize()  # Normalizar para obtener solo la fecha sin componente de tiempo

# Si la última fecha en df_merge es antes de hoy, necesitamos agregar filas hasta hoy
if ultima_fecha < fecha_hoy:
    # Crear un DataFrame con el rango de fechas faltantes hasta hoy
    fechas_adicionales = pd.date_range(start=ultima_fecha + pd.Timedelta(days=1), end=fecha_hoy)
    df_adicionales = pd.DataFrame({'date': fechas_adicionales})
    
    # Replicar los últimos valores conocidos de 'alquiler' y 'expensas' para las nuevas fechas
    ultimos_valores = df_merge.iloc[-1][['alquiler', 'expensas']].to_dict()
    df_adicionales = df_adicionales.assign(**ultimos_valores)
    
    # Concatenar el DataFrame original con el de fechas adicionales
    df_merge_completo = pd.concat([df_merge, df_adicionales], ignore_index=True)
else:
    df_merge_completo = df_merge

df_merge_completo  # Mostrar las últimas filas para verificar el resultado
