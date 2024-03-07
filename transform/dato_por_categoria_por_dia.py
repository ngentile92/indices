from extract.db import fetch_data
from transform.ponderadores import ponderadores_inflacion_actualizado

query = """
SELECT p.date, cp.categoria_indice, sum(p.precio)/count(p.producto) as precio_promedio
FROM `slowpoke-v1`.precios p
JOIN `slowpoke-v1`.categorias_productos cp ON p.producto = cp.productos
WHERE p.date >= '2024-03-01'
GROUP BY p.date, cp.categoria_indice
"""

df = fetch_data(query)


ponderadores_aplanados = {}
for key, value in ponderadores_inflacion_actualizado.items():
    if isinstance(value, dict):
        for sub_key, sub_value in value.items():
            ponderadores_aplanados[sub_key] = sub_value
    else:
        ponderadores_aplanados[key] = value

# Multiplicamos sum(p.precio) por el ponderador correspondiente
df['sum_ponderado'] = df.apply(lambda row: row['precio_promedio'] * ponderadores_aplanados.get(row['categoria_indice'], 1), axis=1)

df
