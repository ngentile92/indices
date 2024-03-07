"""
Archivo principal de lógica del programa
"""
import json
import argparse
import pandas as pd

from extract.precios import process_all
from extract.dolar import scrapeo_dolar
from extract.BCRA import process_BCRA
from load.gcs_load import load_data_to_db, load_dolar_to_db, load_bcra_to_db, load_categorias_productos_to_db, load_tarifas_to_db, load_alquileres_to_db
from extract.tarifas import extraer_enlaces_cuadros_tarifarios, procesar_html_para_usuarios_generales, limpiar_titulos
from zonaprop_scraping import procesar_zonaprop

# add filepath C:\Users\nagge\Desktop\Nico\scraper_precio\zona-prop-scraper to sys.path
def pipeline_supermercados():
    """
    Función que ejecuta el pipeline de supermercados
    - Scrapea los datos de las URLs provistas en el .csv
    - Carga los datos en la base de datos
    """
    # Extraer los datos de las URLs
    # Open csv file
    with open('url_productos.csv', 'r') as f:
        datos = datos = pd.read_csv(f, encoding='ISO-8859-1')
        # Convertir a listas
        url_list = datos['URL'].tolist()

    # Extraer los nombres de los productos
    product_names_unified = datos['producto_unificado'].tolist()
    # Extraer los precios de las URLs para cada producto
    data_json = process_all(url_list, product_names_unified)

    print(data_json)

    # Cargar los datos en la base de datos
    load_data_to_db(data_json)

def pipeline_dolar():
    """
    Función que ejecuta el pipeline de dólar
    - Scrapea los datos de dolar de la pagina 'https://www.infodolar.com/'
    - Carga los datos en la base de datos
    """
    dolar_info = scrapeo_dolar()
    dolar_info_json = json.dumps(dolar_info)
    load_dolar_to_db(dolar_info_json)

def pipeline_BCRA():
    """
    Función que ejecuta el pipeline de dólar
    - Scrapea los datos de dolar de la pagina bcra
    - Carga los datos en la base de datos
    """
    BCRA_data = process_BCRA()
    load_bcra_to_db(BCRA_data)

def pipeline_tarifas_electricas():
    URL = "https://www.argentina.gob.ar/enre/cuadros_tarifarios"
    enlace_cuadro_tarifario = extraer_enlaces_cuadros_tarifarios(URL)
    resultados_globales = []
    #enlace_cuadro_tarifario = [
    #    "https://www.enre.gov.ar/web/tarifasd.nsf/todoscuadros/EE287D6D2EC07DE70325894B00640A20?opendocument",
    #    "https://www.enre.gov.ar/web/TARIFASD.nsf/todoscuadros/6AF98FD8169DDCB403258966004CE1ED?opendocument",
    #    "https://www.enre.gov.ar/web/TARIFASD.nsf/todoscuadros/02F87103D2A44EFA03258967004C56EB?opendocument",
    #    "https://www.enre.gov.ar/web/tarifasd.nsf/todoscuadros/2C70F98CD96070C2032589A6003EB4F1?opendocument",
    #    "https://www.enre.gov.ar/web/tarifasd.nsf/todoscuadros/C39BA4874DCA1FEF03258854004FBE7D?opendocument",
    #    "https://www.enre.gov.ar/web/tarifasd.nsf/todoscuadros/498ACB3ED0593512032588780042D316?opendocument",
    #    "https://www.enre.gov.ar/web/tarifasd.nsf/todoscuadros/DA9ACE2067B6D9F70325889100482B86?opendocument",
    #    "https://www.enre.gov.ar/web/tarifasd.nsf/todoscuadros/6831EEB39C0A5E2F032586CB006834CA?opendocument",
    #    "https://www.enre.gov.ar/web/tarifasd.nsf/todoscuadros/EB3643E64C0963C7032588D8004CB1D9?opendocument",
    #    "https://www.enre.gov.ar/web/tarifasd.nsf/todoscuadros/9BDA7860C6E30AFB032588EF00448161?opendocument",
    #    "https://www.enre.gov.ar/web/tarifasd.nsf/todoscuadros/C6449DE85BF30A030325890F006B5510?opendocument",
    #    "https://www.enre.gov.ar/web/tarifasd.nsf/todoscuadros/949C99A58F4C79EA0325892D00662E40?opendocument",
    #    "https://www.enre.gov.ar/web/tarifasd.nsf/todoscuadros/70443DAC0651153D032589B4004218B8?opendocument",
    #    "https://www.enre.gov.ar/web/tarifasd.nsf/todoscuadros/0C4CDDE197FF7409032589E4005EF7CB?opendocument",
    #    "https://www.enre.gov.ar/web/tarifasd.nsf/todoscuadros/D0A25F2999EC0CC1032589F900667B84?opendocument",
    #    "https://www.enre.gov.ar/web/TARIFASD.nsf/todoscuadros/77005EF0D0961ED403258A21004793BF?opendocument",
    #    "https://www.enre.gov.ar/web/tarifasd.nsf/todoscuadros/C120B41EBDDE16CD03258A3C003BD252?opendocument",
    #    "https://www.enre.gov.ar/web/tarifasd.nsf/todoscuadros/7F88055965CC017903258A5F00459959?opendocument",
    #    "https://www.enre.gov.ar/web/tarifasd.nsf/todoscuadros/BAF634BB93647DEA03258A7C00432386?opendocument",
    #    "https://www.enre.gov.ar/web/tarifasd.nsf/todoscuadros/A0CE9C40FB1AF8F203258AA20051EC60?opendocument"
    #]
    print(f"Enlace encontrado: {enlace_cuadro_tarifario}")

    datos_tarifas = procesar_html_para_usuarios_generales(enlace_cuadro_tarifario)
    datos_tarifas_limpio = limpiar_titulos(datos_tarifas)
    resultados_globales.append(datos_tarifas_limpio)

    # Cargar los datos a la base de datos
    load_tarifas_to_db(resultados_globales)

def pipeline_zonaprop(should_save: bool = True) -> None:

    urls = ['https://www.zonaprop.com.ar/inmuebles-alquiler-ciudad-de-mendoza-mz-1-habitacion.html',
            'https://www.zonaprop.com.ar/inmuebles-alquiler-rosario-1-habitacion.html',
            'https://www.zonaprop.com.ar/inmuebles-alquiler-capital-federal-1-habitacion.html',
            'https://www.zonaprop.com.ar/inmuebles-alquiler-cordoba-1-habitacion.html'
            ]
    dfs = []
    for url in urls:
        df = procesar_zonaprop(url)
        dfs.append(df)
    
    # Concatenamos todos los DataFrames en uno solo
    final_df = pd.concat(dfs, ignore_index=True)
    #save un a csv
    if should_save:
        load_alquileres_to_db(final_df)
    else:
        final_df.to_csv('final_df.csv', index=False)
def main() -> None:


    parser = argparse.ArgumentParser(description="Ejecuta el pipeline de supermercados o de dólar")

    parser.add_argument(
        "--supermercados",
        action="store_true",
        help="Ejecuta el pipeline de supermercados"
    )

    parser.add_argument(
        "--dolar",
        action="store_true",
        help="Ejecuta el pipeline de dólar"
    )
    parser.add_argument(
        "--bcra",
        action="store_true",
        help="ejecuta pipeline de BCRA estadisticas diarias"
    )
    parser.add_argument(
        "--categorias-productos",
        action="store_true",
        help="ejecuta pipeline de categorias de productos"
    )
    parser.add_argument(
        "--correr-todo",
        action="store_true",
        help="todos los flows empezando por dolar, bcra y luego supermercados"
    )
    parser.add_argument(
        "--tarifas-electricas",
        action="store_true",
        help="ejecuta pipeline de tarifas electricas"
    )
    parser.add_argument(
        "--zonaprop",
        action="store_true",
        help="ejecuta pipeline de zonaprop"
    )
    args = parser.parse_args()

    if args.supermercados:
        pipeline_supermercados()
    elif args.dolar:
        pipeline_dolar()
    elif args.bcra:
        pipeline_BCRA()
    elif args.categorias_productos:
        load_categorias_productos_to_db('producto_categorias.csv')
    elif args.tarifas_electricas:
        pipeline_tarifas_electricas()
    elif args.zonaprop:
        pipeline_zonaprop()
    elif args.correr_todo:
        pipeline_dolar()
        pipeline_BCRA()
        pipeline_supermercados()
    else:
        print("Debe especificar el pipeline a ejecutar")
        parser.print_help()

#################################################################################################################
#################################################################################################################

if __name__ == "__main__":
    main()
