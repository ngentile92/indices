from extract.db import fetch_data
from transform.ponderadores import ponderadores_inflacion_actualizado, ponderadores_alquileres
import ast

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



