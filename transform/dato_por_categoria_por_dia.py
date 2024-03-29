from extract.db import fetch_data
from transform.ponderadores import ponderadores_inflacion_actualizado, ponderadores_alquileres
import ast
import pandas as pd


categorias_interes = ['farmacia', 'frutas', 'Bebidas no alcoholicas', 'limpieza', 'Azucar/ dulces/ chocolate/golosinas/ etc.', 'cuidado oral']


query_precios_supermercado = """
SELECT p.date, cp.categoria_indice, p.precio, p.producto
FROM `slowpoke-v1`.precios p
JOIN `slowpoke-v1`.categorias_productos cp ON p.producto = cp.productos
WHERE p.date >= '2024-02-25'
"""

query_alquileres = """
SELECT * FROM `slowpoke-v1`.alquileres
WHERE date >= '2024-02-23';
"""

query_dolar = """
SELECT date, dolar_blue FROM `slowpoke-v1`.dolar;
WHERE date >= '2024-02-25';
"""

query_electricidad = """
SELECT * FROM `slowpoke-v1`.tarifas_electricidad
WHERE date >= '2024-02-01';
"""


def corregir_valores(dolar):
    if dolar['compra'] > 10000:  # Si compra es evidentemente un error
        dolar['compra'] /= 100
    if dolar['venta'] > 10000:  # Si venta es evidentemente un error
        dolar['venta'] /= 100
    return (dolar['venta'] + dolar['compra']) / 2

def procesar_dolar(dolar):
    dolar['dolar_blue'] = dolar['dolar_blue'].apply(ast.literal_eval)

    dolar['promedio'] = dolar['dolar_blue'].apply(corregir_valores)
    dolar['promedio'] = dolar['dolar_blue'].apply(lambda x: (x['venta'] + x['compra']) / 2)
    dolar['date'] = dolar['date'].dt.date

    return dolar


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

def unir_y_convertir(alquileres, dolar):
    # Asegurarse de que 'date' en ambos DFs sea datetime64[ns]
    alquileres['date'] = pd.to_datetime(alquileres['date']).dt.date
    dolar['date'] = pd.to_datetime(dolar['date']).dt.date

    # Realizar el merge
    merged_df = alquileres.merge(dolar, on='date', how='left')

    # Aplicar la conversión de moneda
    df_alquileres = merged_df.apply(convert_currency, axis=1)

    return df_alquileres



def aplicar_ponderador_por_ciudad_alquiler(df_alquileres, ponderadores_alquileres):
    # transformar los valores "localidad" none por "CABA"
    df_alquileres['localidad'] = df_alquileres['localidad'].fillna('CABA')
    df_alquileres['alquiler_ponderado'] = df_alquileres.apply(lambda row: row['alquiler'] * ponderadores_alquileres.get(row['localidad'], 1), axis=1)
    return df_alquileres

def agrupar_y_promediar(df):
    df_agrupado = df.groupby('date').agg({'alquiler': 'mean', 'expensas': 'mean'}).reset_index()
    return df_agrupado

def completar_datos(df):
    # Crear DataFrame completo con rango de fechas
    rango_fechas = pd.date_range(start=df['date'].min(), end=df['date'].max())
    df_fechas_completo = pd.DataFrame({'date': rango_fechas})
    df_fechas_completo['date'] = pd.to_datetime(df_fechas_completo['date'])

    df['date'] = pd.to_datetime(df['date'])

    # Merge con DataFrame de fechas para encontrar y rellenar fechas faltantes
    df_merge = df_fechas_completo.merge(df, on='date', how='left')

    # Llenar valores faltantes
    for col in ['alquiler', 'expensas']:
        df_merge[col] = df_merge[col].fillna(method='ffill').fillna(method='bfill')
    
    # Extender hasta la fecha actual si es necesario
    ultima_fecha = df_merge['date'].max()
    fecha_hoy = pd.Timestamp('today').normalize()
    if ultima_fecha < fecha_hoy:
        fechas_adicionales = pd.date_range(start=ultima_fecha + pd.Timedelta(days=1), end=fecha_hoy)
        df_adicionales = pd.DataFrame({'date': fechas_adicionales})
        ultimos_valores = df_merge.iloc[-1][['alquiler', 'expensas']].to_dict()
        df_adicionales = df_adicionales.assign(**ultimos_valores)
        df_merge = pd.concat([df_merge, df_adicionales], ignore_index=True)
    
    df_merge['date'] = pd.to_datetime(df_merge['date'])

    return df_merge
def transformar_alquileres(df):
    df['date'] = pd.to_datetime(df['date']).dt.date  # Convertir a solo fecha
    df['categoria_indice'] = 'Alquileres'  # Columna nueva con valor 'Alquileres'
    df.rename(columns={'alquiler': 'precio_promedio'}, inplace=True)  # Renombrar 'alquiler' a 'precio_promedio'
    df.drop('expensas', axis=1, inplace=True)  # Eliminar columna 'expensas'
    return df[['date', 'categoria_indice', 'precio_promedio']]

def procesar_alquileres_no_ponderados(query_alquileres, query_dolar):
    alquileres = fetch_data(query_alquileres)
    dolar = fetch_data(query_dolar)
    dolar_procesado = procesar_dolar(dolar)
    df_alquileres = unir_y_convertir(alquileres, dolar_procesado)
    df_alquileres_ponderados = aplicar_ponderador_por_ciudad_alquiler(df_alquileres, ponderadores_alquileres)
    # Nuevo paso: Agrupar por fecha y calcular promedios antes de completar datos
    df_alquileres_agrupados = agrupar_y_promediar(df_alquileres_ponderados)
    # Ahora completamos los datos, incluyendo la lógica de fechas faltantes y aplicación final de ponderadores
    df_alquileres_completos = completar_datos(df_alquileres_agrupados)
    df_alquileres_completos = transformar_alquileres(df_alquileres_completos)
    df_alquileres_completos['date'] = pd.to_datetime(df_alquileres_completos['date']).dt.date

    return df_alquileres_completos


def obtener_y_promediar_datos_electricidad(query_electricidad):
    precios_electricidad = fetch_data(query_electricidad)
    precios_electricidad_promedio = precios_electricidad.groupby('Date').agg({'Costo_fijo': 'mean', 'costo_variable': 'mean'}).reset_index()
    return precios_electricidad_promedio

def extender_datos_hasta_actual_con_dias(df):
    ultimo_dia_datos = df['Date'].max()
    dia_actual = pd.Timestamp('today').normalize()

    rango_dias = pd.date_range(start=ultimo_dia_datos + pd.Timedelta(days=1), end=dia_actual, freq='D')

    if not rango_dias.empty:
        ultimo_costo_fijo = df.iloc[-1]['Costo_fijo']
        ultimo_costo_variable = df.iloc[-1]['costo_variable']

        df_nuevos_dias = pd.DataFrame({
            'Date': rango_dias,
            'Costo_fijo': ultimo_costo_fijo,
            'costo_variable': ultimo_costo_variable,
            'precio_total': 0  # Se inicializa aquí y se calcula después
        })

        df = pd.concat([df, df_nuevos_dias], ignore_index=True)

    return df

def calcular_precio_total(df, consumo_variable=250):  # Asumiendo 250 como valor por defecto
    df['costo_variable'] = df['costo_variable'].astype(float)
    df['Costo_fijo'] = df['Costo_fijo'].astype(float)
    df['precio_total'] = df['Costo_fijo'] + df['costo_variable'] * consumo_variable
    return df

def transformar_tarifas_electricidad(df):
    df['Date'] = pd.to_datetime(df['Date']).dt.date  # Asegurarse de que está en formato de fecha
    df.rename(columns={'precio_total': 'precio_promedio'}, inplace=True)  # Cambiar nombre de columna
    df['categoria_indice'] = 'Electricidad'  # Columna nueva con valor 'Electricidad'
    return df[['Date', 'categoria_indice', 'precio_promedio']]

def procesar_datos_electricidad(query_electricidad):
    df = obtener_y_promediar_datos_electricidad(query_electricidad)
    df = extender_datos_hasta_actual_con_dias(df)
    df = calcular_precio_total(df)
    df = transformar_tarifas_electricidad(df)
    df['Date'] = pd.to_datetime(df['Date']).dt.date  # Asegúrate de cambiar 'Date' a 'date' después
    df.rename(columns={'Date': 'date'}, inplace=True)

    return df
def completar_precios_faltantes(df, categorias):
    df_original = df.copy()
    df['date'] = pd.to_datetime(df['date'])
    
    # Procesar solo las filas de las categorías especificadas
    df_filtrado = df[df['categoria_indice'].isin(categorias)]

    # Ordenar y promediar precios para entradas duplicadas
    df_filtrado.sort_values(by=['producto', 'date'], inplace=True)
    df_filtrado = df_filtrado.groupby(['producto', 'date', 'categoria_indice']).agg({'precio': 'mean'}).reset_index()

    df_completado = pd.DataFrame()

    for categoria in categorias:
        df_cat = df_filtrado[df_filtrado['categoria_indice'] == categoria]
        primer_dia = df_cat['date'].min()
        productos_dia_1 = df_cat[df_cat['date'] == primer_dia]['producto'].unique()
        
        for producto in productos_dia_1:
            df_producto = df_cat[df_cat['producto'] == producto].copy()
            df_producto.set_index('date', inplace=True)
            df_producto = df_producto.resample('D').asfreq()

            if df_producto['precio'].bfill().notna().any():
                df_producto['precio'] = df_producto['precio'].fillna(df_producto['precio'].bfill().mean())
            
            df_producto['precio'] = df_producto['precio'].fillna(method='ffill')
            df_producto.reset_index(inplace=True)
            df_producto['producto'] = producto
            df_producto['categoria_indice'] = categoria  # Asegurarse de asignar la categoría
            df_completado = pd.concat([df_completado, df_producto])

    # Asegurarse de que las categorías no procesadas se incluyen en el resultado final
    df_final = pd.concat([df_completado, df_original[~df_original['categoria_indice'].isin(categorias)]])

    # Asegurar que los tipos de datos son correctos y ordenar
    df_final['precio'] = df_final['precio'].astype(float)
    df_final['date'] = pd.to_datetime(df_final['date'])
    df_final.sort_values(by=['categoria_indice', 'date', 'producto'], inplace=True)
    
    return df_final


def calcular_precio_promedio(df):
    """
    Calcula el precio promedio por categoría y fecha.
    
    Args:
    df (pd.DataFrame): DataFrame con los precios completados y las columnas 'date', 'categoria_indice', 'precio' y 'producto'.
    
    Returns:
    pd.DataFrame: DataFrame con el precio promedio por categoría y fecha.
    """
    
    # Asegurar que 'categoria_indice_y' es la columna correcta para 'categoria_indice' después de la unión
    if 'categoria_indice_y' in df.columns:
        df['categoria_indice'] = df['categoria_indice_y']
    
    # Calcular el precio promedio por categoría y fecha
    df_precio_promedio = df.groupby(['date', 'categoria_indice']).agg(precio_promedio=('precio', 'mean')).reset_index()
    
    return df_precio_promedio

def procesar_datos_supermercado(query_precios_supermercado, categorias_interes):
    precios_supermercados = fetch_data(query_precios_supermercado)
    precios_supermercados['date'] = pd.to_datetime(precios_supermercados['date']).dt.date
    precios_supermercados = completar_precios_faltantes(precios_supermercados, categorias_interes)
    precios_supermercados = calcular_precio_promedio(precios_supermercados)
    precios_supermercados['date'] = pd.to_datetime(precios_supermercados['date']).dt.date
    return precios_supermercados

def generar_datos_por_categoria(query_precios_supermercado, query_alquileres, query_dolar, query_electricidad):
    tarifas_electricidad = procesar_datos_electricidad(query_electricidad)
    alquileres_completo  = procesar_alquileres_no_ponderados(query_alquileres, query_dolar)
    precios_supermercados = procesar_datos_supermercado(query_precios_supermercado, categorias_interes)
    # Asumiendo que df1 es tu primer DataFrame, df2 el segundo y df3 el tercero
    # Primero, asegúrate de que las columnas de fecha tengan el mismo nombre y formato
    alquileres_completo['date'] = pd.to_datetime(alquileres_completo['date']).dt.date
    tarifas_electricidad.rename(columns={'Date': 'date'}, inplace=True)
    tarifas_electricidad['date'] = pd.to_datetime(tarifas_electricidad['date']).dt.date

    # Ahora, concatena los DataFrames
    df_final = pd.concat([precios_supermercados, alquileres_completo, tarifas_electricidad])
    df_final.sort_values(by='date', inplace=True)
    df_final.reset_index(drop=True, inplace=True)
    # eliminar todos datos inferiores a marzo
    df_final = df_final[df_final['date'] >= pd.to_datetime('2024-02-25').date()]
    return df_final


if __name__ == "__main__":
    df_final = generar_datos_por_categoria(query_precios_supermercado, query_alquileres, query_dolar, query_electricidad)
    df_final.to_csv('datos_por_categoria_por_dia.csv', index=False)


