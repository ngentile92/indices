ponderadores_inflacion_actualizado = {
    "Alimentos": {
        'Pan / pastas y cereales': 0.1519,
        'Carnes y derivados': 0.1779,
        'Pescados y mariscos': 0.015,
        'Leche/ productos lacteos/ huevos y alimentos vegetales': 0.0899,
        'Aceites/ aderezos/ grasas y manteca': 0.015,
        'Frutas': 0.03,
        'Verduras/ tuberculos y legumbres': 0.054,
        'Azucar/ dulces/ chocolate/golosinas/ etc.': 0.028,
        'Otros alimentos': 0.009,
    },
  'Bebidas no alcoholicas': 0.062,
  'Bebidas alcoholicas y tabaco': 0.056,
  'Prendas de vestir': 0.05,
  'Calzado': 0.04,
  'Electricidad': 0.025,
  'Alquileres': 0.0799,
  'celulares y pequenos electrodomesticos': 0.023,
  'farmacia': 0.022,
  'informatica': 0.02,
  'sin TACC': 0.008,
  'cuidado oral': 0.008,
  'libreria': 0.016,
  'limpieza': 0.01939999999999981,
  }
ponderadores_alquileres = {
    "mendoza": 0.15,
    "rosario": 0.2,
    "CABA": 0.45,
    "cordoba": 0.2,
}


# Reajustar los ponderadores para que la suma sea exactamente 1 y que cada ponderador no tenga más de 4 dígitos decimales

# Aplanar el diccionario de ponderadores
ponderadores_aplanados = {}
for categoria, ponderadores in ponderadores_inflacion_actualizado.items():
    if isinstance(ponderadores, dict):
        for subcategoria, valor in ponderadores.items():
            ponderadores_aplanados[subcategoria] = valor
    else:
        ponderadores_aplanados[categoria] = ponderadores

# Sumar los valores aplanados para obtener el total
total_ponderadores = sum(ponderadores_aplanados.values())

# Reajustar los ponderadores para que sumen 1 y redondear a 4 dígitos decimales
ponderadores_reajustados = {k: round(v / total_ponderadores, 4) for k, v in ponderadores_aplanados.items()}

# Verificar la suma después del reajuste y redondeo
suma_reajustada = sum(ponderadores_reajustados.values())

# Si la suma reajustada no es exactamente 1 debido al redondeo, ajustar el último valor
if suma_reajustada != 1:
    # Calcular la diferencia a ajustar
    diferencia = 1 - suma_reajustada
    # Aplicar la diferencia al último valor
    ultimo_ponderador = list(ponderadores_reajustados.keys())[-1]
    ponderadores_reajustados[ultimo_ponderador] += diferencia

# Comprobar la suma final para asegurarse de que es 1
suma_final = sum(ponderadores_reajustados.values())
suma_final, ponderadores_reajustados

