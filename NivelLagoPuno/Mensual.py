import requests
import pandas as pd
from datetime import datetime, timedelta
import concurrent.futures
import logging
import time
import json

# Configuración de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

url = "https://www.senamhi.gob.pe/include/ajax-informacion-mensual.php"
headers = {
    'Accept': 'application/json, text/javascript, */*; q=0.01',
    'Content-Type': 'application/x-www-form-urlencoded',
    'User-Agent': 'Mozilla/5.0',
}

def filtrar_datos(data, estaciones):
    try:
        return [entry for entry in data['content'][0]['detalle'] if entry['nomEsta'] in estaciones]
    except (KeyError, IndexError, TypeError) as e:
        logging.error(f"Error al filtrar datos: {e}")
        return []

def obtener_datos_mensuales(fecha, estaciones, max_retries=3):
    data = {
        'fecha': fecha.strftime('%Y-%m'),
    }

    for attempt in range(max_retries):
        try:
            response = requests.post(url, headers=headers, data=data, timeout=10)
            response.raise_for_status()
            json_data = response.json()
            datos_filtrados = filtrar_datos(json_data, estaciones)
            return fecha.strftime('%Y-%m'), datos_filtrados
        except requests.exceptions.RequestException as e:
            logging.warning(f"Intento {attempt + 1} fallido para {fecha.strftime('%Y-%m')}: {e}")
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)  # Espera exponencial entre intentos
            else:
                logging.error(f"Error en la solicitud para {fecha.strftime('%Y-%m')} después de {max_retries} intentos: {e}")
        except json.JSONDecodeError as e:
            logging.error(f"Error al decodificar JSON para {fecha.strftime('%Y-%m')}: {e}")
            break  # Si hay un error de JSON, no tiene sentido reintentar

    return fecha.strftime('%Y-%m'), None

def main():
    estaciones = ['MUELLE ENAFER', 'OTRA ESTACION']
    fecha_inicio = datetime(2023, 1, 1)
    fecha_fin = datetime(2024, 9, 1)

    datos_recolectados = []
    fechas = []
    fecha_actual = fecha_inicio

    while fecha_actual <= fecha_fin:
        fechas.append(fecha_actual)
        fecha_actual += timedelta(days=32)
        fecha_actual = fecha_actual.replace(day=1)

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        futuro_a_fecha = {executor.submit(obtener_datos_mensuales, fecha, estaciones): fecha for fecha in fechas}
        for futuro in concurrent.futures.as_completed(futuro_a_fecha):
            fecha = futuro_a_fecha[futuro]
            try:
                fecha_str, resultado = futuro.result()
                if resultado:
                    datos_recolectados.extend([{
                        'fecha': fecha_str,
                        'codZonal': dato.get('codZonal', ''),
                        'codEsta': dato.get('codEsta', ''),
                        'nomEsta': dato.get('nomEsta', ''),
                        'uniHidrografica': dato.get('uniHidrografica', ''),
                        'nomDepa': dato.get('nomDepa', ''),
                        'nomSector': dato.get('nomSector', ''),
                        'dato': dato.get('dato', ''),
                        'datoAnt': dato.get('datoAnt', ''),
                        'unidad': dato.get('unidad', ''),
                        'datAnomalia': dato.get('datAnomalia', ''),
                        'uniAnomalia': dato.get('uniAnomalia', ''),
                        'tendencia': dato.get('tendencia', ''),
                        'umbralRojo': dato.get('umbralRojo', ''),
                        'cuerpoAgua': dato.get('cuerpoAgua', '')
                    } for dato in resultado])

                porcentaje_recolectados = (len(datos_recolectados) / (len(fechas) * len(estaciones))) * 100
                logging.info(f"Datos recolectados: {len(datos_recolectados)} de {len(fechas)} posibles ({porcentaje_recolectados:.2f}%).")
            except Exception as e:
                logging.error(f"Error al procesar los resultados para {fecha}: {e}")

    if datos_recolectados:
        # Crear DataFrame y ordenar por fecha
        df = pd.DataFrame(datos_recolectados)
        df['fecha'] = pd.to_datetime(df['fecha'])  # Convertir 'fecha' a datetime
        df = df.sort_values('fecha')  # Ordenar por fecha

        csv_filename = os.path.join(path_dir, 'datos_mensuales_estaciones_2023-2024.csv')
        df.to_csv(csv_filename, index=False)
        logging.info("Datos guardados en 'datos_mensuales_estaciones_2023-2024.csv'.")
    else:
        logging.warning("No se recolectaron datos. No se creará el archivo CSV.")

if __name__ == "__main__":
    main()