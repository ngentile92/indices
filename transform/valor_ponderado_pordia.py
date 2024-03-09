"""
Este script toma los datos por categoria por dia y los pondera segun la ponderacion de la inflacion actualizada.
"""
from ponderadores import ponderadores_inflacion_actualizado
from dato_por_categoria_por_dia import generar_datos_por_categoria, query_precios_supermercado, query_alquileres, query_dolar, query_electricidad
import pandas as pd

def aplanar_ponderadores(ponderadores):
    ponderadores_aplanados = {}
    for categoria, subcategorias in ponderadores.items():
        if isinstance(subcategorias, dict):  # Si la categoría tiene subcategorías
            for subcategoria, valor in subcategorias.items():
                ponderadores_aplanados[subcategoria] = valor
        else:
            ponderadores_aplanados[categoria] = subcategorias
    return ponderadores_aplanados

# Función para aplicar la ponderación
def aplicar_ponderacion(df, ponderadores):
    # Aplanar los ponderadores
    ponderadores_aplanados = aplanar_ponderadores(ponderadores)
    # Mapear las categorías del DataFrame a los ponderadores
    # Nota: puede ser necesario ajustar las categorías del DataFrame para que coincidan con las claves del diccionario de ponderadores
    df['ponderador'] = df['categoria_indice'].map(ponderadores_aplanados)
    
    # Verificar y manejar posibles valores NaN en la columna 'ponderador'
    df['ponderador'].fillna(0, inplace=True)
    
    # Multiplicar el precio_promedio por el ponderador
    df['precio_ponderado'] = df['precio_promedio'] * df['ponderador']
    
    return df
def calcular_indice_vida_adulta(df):
    fecha_base = '2024-03-01'
    df['date'] = pd.to_datetime(df['date'])

    # print (df) para la fecha base
    valor_base = df[df['date'] == fecha_base]['precio_ponderado'].sum()

    # 2. Calcular el valor total ponderado para cada fecha
    valor_total_ponderado_por_fecha = df.groupby('date')['precio_ponderado'].sum()

    # 3. Calcular el índice para cada fecha
    indice_de_vida_adulta = (valor_total_ponderado_por_fecha / valor_base) * 100

    # Transformar la Serie en DataFrame para una mejor visualización y manipulación
    indice_de_vida_adulta_df = indice_de_vida_adulta.reset_index()
    indice_de_vida_adulta_df.rename(columns={'precio_ponderado': 'indice_de_vida_adulta'}, inplace=True)
    return indice_de_vida_adulta_df

def calcular_indice_por_categoria(df, fecha_base):
    # Asegúrate de que 'date' está en formato datetime
    df['date'] = pd.to_datetime(df['date'])
    fecha_base = pd.to_datetime(fecha_base)

    # 1. Calcular el valor base para cada categoría en la fecha base
    valor_base_categoria = df[df['date'] == fecha_base].groupby('categoria_indice')['precio_ponderado'].sum()

    # Identificar las categorías con valor base NaN, null o 0
    categorias_con_valor_base_invalido = valor_base_categoria[valor_base_categoria.fillna(0) == 0].index.tolist()
    
    # Imprimir las categorías con valor base NaN, null o 0
    if categorias_con_valor_base_invalido:
        print("Categorías con valor base NaN, null o 0 en la fecha base:")
        print(categorias_con_valor_base_invalido)
    else:
        print("No hay categorías con valor base NaN, null o 0 en la fecha base.")

    # Continuar con el cálculo solo para las categorías con valor base válido
    valor_base_categoria = valor_base_categoria[valor_base_categoria > 0]

    # 2. Calcular el valor total ponderado para cada categoría por cada fecha
    valor_total_ponderado_por_fecha_categoria = df.groupby(['date', 'categoria_indice'])['precio_ponderado'].sum().reset_index()

    # Preparar DataFrame para almacenar los índices
    indice_por_categoria_producto = valor_total_ponderado_por_fecha_categoria.copy()

    # 3. Calcular el índice para cada categoría por día
    # Se hace un mapeo del valor base por categoría para usarlo en el cálculo
    indice_por_categoria_producto['valor_base'] = indice_por_categoria_producto['categoria_indice'].map(valor_base_categoria)

    # Calcular el índice, asegurándose de manejar valores base faltantes
    indice_por_categoria_producto['indice'] = (indice_por_categoria_producto['precio_ponderado'] / indice_por_categoria_producto['valor_base'].replace({pd.NA: 0})) * 100

    # Limpieza final: eliminar cualquier fila con NA o 0 en 'valor_base' (debido a valores base faltantes o 0)
    indice_por_categoria_producto = indice_por_categoria_producto[indice_por_categoria_producto['valor_base'] > 0]

    return indice_por_categoria_producto[['date', 'categoria_indice', 'indice']]


def main():
    datos_por_categoria = generar_datos_por_categoria(query_precios_supermercado, query_alquileres, query_dolar, query_electricidad)
    valor_ponderado = aplicar_ponderacion(datos_por_categoria,ponderadores_inflacion_actualizado)
    # tranformar a dataframe
    valor_ponderado.to_csv("valor_ponderado_pordia.csv")
    indice_vida_adulta = calcular_indice_vida_adulta(valor_ponderado)
    indice_vida_adulta.to_csv("indice_vida_adulta.csv")
    fecha_base = '2024-03-01'
    indice_por_categoria = calcular_indice_por_categoria(valor_ponderado, fecha_base)
    indice_por_categoria.to_csv("indice_por_categoria.csv")
    # Suponiendo que 'df' es tu DataFrame con los datos
    df_indices_categoria_producto = calcular_indice_por_categoria_producto(valor_ponderado, '2024-03-01')
    print(df_indices_categoria_producto)
    df_indices_categoria_producto.to_csv("indice_por_categoria_producto.csv")


def calcular_indice_por_categoria_producto(df, fecha_base):
    # Convertir las fechas a datetime
    df['date'] = pd.to_datetime(df['date'])
    fecha_base = pd.to_datetime(fecha_base)

    # Calcular el valor base para cada categoría en la fecha base
    df_base = df[df['date'] == fecha_base]
    valor_base_categoria = df_base.groupby('categoria_indice')['precio_ponderado'].sum()

    # Crear un DataFrame para calcular el índice diario por categoría
    df_indice = df.copy()
    df_indice['indice'] = 0  # Inicializar la columna del índice

    for categoria in valor_base_categoria.index:
        # Valor base para la categoría
        valor_base = valor_base_categoria[categoria]
        # Filtrar el df por categoría
        df_cat = df_indice[df_indice['categoria_indice'] == categoria]
        # Calcular el índice para la categoría
        df_indice.loc[df_cat.index, 'indice'] = (df_cat['precio_ponderado'] / valor_base) * 100

    return df_indice[['date', 'categoria_indice', 'indice']]



if __name__ == "__main__":
    main()
