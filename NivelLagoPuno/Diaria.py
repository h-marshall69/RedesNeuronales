import requests
import pandas as pd
from datetime import datetime, timedelta
import concurrent.futures
import time

url = "https://www.senamhi.gob.pe/include/ajax-informacion-diaria.php"
headers = {
    'Accept': 'application/json, text/javascript, */*; q=0.01',
    'Content-Type': 'application/x-www-form-urlencoded',
    'User-Agent': 'Mozilla/5.0',
}

def filtrar_datos(data, estaciones):
    return [entry for entry in data['content'] if entry['nomEsta'] in estaciones]

def obtener_datos(fecha, hora, estaciones):
    fecha_str = fecha.strftime('%Y-%m-%d')
    data = {
        'fecha': fecha_str,
        'hora': hora
    }

    try:
        response = requests.post(url, headers=headers, data=data)
        response.raise_for_status()
        datos_filtrados = filtrar_datos(response.json(), estaciones)
        return fecha_str, datos_filtrados
    except requests.exceptions.RequestException as e:
        print(f"Error en la solicitud para {fecha_str}: {e}")
        return fecha_str, None

def main():
    estaciones = ['MUELLE ENAFER']  # Añade aquí las estaciones que quieras monitorear
    fecha_inicio = datetime(2024, 8, 1)
    fecha_fin = datetime(2024, 10, 13)
    hora = '18:00'

    fechas = [fecha_inicio + timedelta(days=i) for i in range((fecha_fin - fecha_inicio).days + 1)]
    datos_recolectados = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        futuro_a_fecha = {executor.submit(obtener_datos, fecha, hora, estaciones): fecha for fecha in fechas}
        for futuro in concurrent.futures.as_completed(futuro_a_fecha):
            fecha = futuro_a_fecha[futuro]
            fecha_str, resultado = futuro.result()
            if resultado:
                datos_recolectados.extend([{
                    'fecha': fecha_str,
                    'codZonal': dato['codZonal'],
                    'codEsta': dato['codEsta'],
                    'nomEsta': dato['nomEsta'],
                    'uniHidrografica': dato['uniHidrografica'],
                    'nomDepa': dato['nomDepa'],
                    'nomCuenca': dato['nomCuenca'],
                    'nomSector': dato['nomSector'],
                    'dato': dato['dato'],
                    'unidad': dato['unidad'],
                    'datAnomalia': dato['datAnomalia'],
                    'uniAnomalia': dato['uniAnomalia'],
                    'tendencia': dato['tendencia'],
                    'umbralRojo': dato['umbralRojo'],
                    'cuerpoAgua': dato['cuerpoAgua']
                } for dato in resultado])

            porcentaje_recolectados = (len(datos_recolectados) / (len(fechas) * len(estaciones))) * 100
            print(f"Datos recolectados: {len(datos_recolectados)} de {len(fechas) * len(estaciones)} posibles ({porcentaje_recolectados:.2f}%).")

    # Crear DataFrame y ordenar por fecha
    df = pd.DataFrame(datos_recolectados)
    df['fecha'] = pd.to_datetime(df['fecha'])  # Convertir 'fecha' a datetime
    df = df.sort_values('fecha')  # Ordenar por fecha
    csv_filename = os.path.join(path_dir, 'datos_estaciones_2024.csv')
    df.to_csv(csv_filename, index=False)
    print("Datos guardados en 'datos_estaciones_2024.csv'.")

if __name__ == "__main__":
    main()