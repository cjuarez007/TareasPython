import os, json, time, logging, sys

def agregarpath(libreria):
    # Construir la ruta completa del archivo o directorio 'libreria'
    ruta_libreria = os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), libreria)

    # Validación 1: Verificar si la ruta existe
    if not os.path.exists(ruta_libreria):
        print(f"Error: La ruta {ruta_libreria} no existe.")
        return

    # Validación 2: Si es un archivo .zip, añadirlo correctamente
    if ruta_libreria.endswith('.zip'):
        # Asegurarse de que es un archivo .zip
        if os.path.isfile(ruta_libreria):
            sys.path.insert(0, ruta_libreria)
            print(f"El archivo .zip {ruta_libreria} ha sido agregado a sys.path.")
        else:
            print(f"Error: {ruta_libreria} no es un archivo válido.")
        return

    # Validación 3: Si es un directorio, verificar que sea un directorio válido
    if os.path.isdir(ruta_libreria):
        sys.path.insert(0, ruta_libreria)
        print(f"La ruta {ruta_libreria} ha sido agregada a sys.path.")
        return

    # Si no es un archivo .zip ni un directorio, mostrar error
    print(f"Error: La ruta {ruta_libreria} no es un directorio válido ni un archivo .zip.")


agregarpath("Library")

import pandas as pd
import requests


BASE_URL = 'https://api.cloud.varicent.com/api/v1'
HEADERS = {
    "Authorization": "Bearer icm-VQ1qp8Ls6dDwN+6aW2R+7+j4i3BzqMb64Bd6ygZB1Vs=",  # Token ALEX
    "Model": "MODEL",
    "Content-Type": "application/json; charset=utf-8",
}

# Configuración del logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def hacer_solicitud(url, headers, data=None, intentos=3, timeout=600):
    """Realiza solicitudes HTTP con reintentos exponenciales y manejo de errores más robusto."""
    for intento in range(1, intentos + 1):
        try:
            # Realizamos la solicitud POST
            respuesta = requests.post(url, headers=headers, data=data, timeout=timeout)

            # Verificamos que la respuesta no sea un error (código 4xx o 5xx)
            respuesta.raise_for_status()

            # Si la respuesta es exitosa, devolvemos el contenido
            return respuesta.json()

        except requests.exceptions.Timeout:
            # Error en el tiempo de espera (timeout)
            logger.warning(f"Error de timeout en intento {intento}/{intentos}.")
        except requests.exceptions.TooManyRedirects:
            # Error si hay demasiadas redirecciones
            logger.error(f"Demasiadas redirecciones en el intento {intento}/{intentos}.")
            break
        except requests.exceptions.RequestException as e:
            # Capturamos errores generales y los mostramos
            logger.error(f"Error en solicitud (Intento {intento}/{intentos}): {e}")

        # Si no es el último intento, espera exponencialmente antes de intentar de nuevo
        if intento < intentos:
            wait_time = 2 ** intento  # Exponential backoff (espera más tiempo con cada intento)
            logger.info(f"Esperando {wait_time} segundos antes de reintentar...")
            time.sleep(wait_time)

    # Si fallan todos los intentos, lanzamos una excepción
    logger.error(f"Falló la solicitud tras {intentos} intentos.")
    raise Exception(f"Falló la solicitud tras {intentos} intentos.")



def get_data_query(query, limit=99999999, chunk_size=100000, model='femcodev', eff=True):
    logger.info(f"Ejecutando peticion get por query. Obtencion por lotes.")

    # Determinar si ya tiene una cláusula LIMIT o no
    has_limit = "LIMIT" in query.upper()
    if has_limit:
        # Si ya tiene LIMIT, dejamos la consulta como está
        base_query = query
    else:
        # Si no tiene LIMIT, añadimos LIMIT y OFFSET para paginación
        base_query = f"{query} LIMIT {chunk_size} OFFSET "

    offset = 0
    full_data = []
    columnas = None
    HEADERS["Model"] = model

    while True:
        if has_limit:
            # Si ya tiene LIMIT, usamos la consulta original para el primer lote
            paginated_query = base_query
            has_limit = False  # Solo usamos la consulta original para el primer lote
        else:
            # Crear la consulta paginada
            paginated_query = f"{base_query}{offset}"

        datos = {
            "importParams": {
                "query": paginated_query,
                "model": model,
                "filename": None,
                "hasHeader": True,
                "queryTimeout": 400,
                "importType": "DBImport"
            },
            "numLines": min(chunk_size, limit - offset) if limit != 99999999 else chunk_size
        }

        respuesta = hacer_solicitud(f"{BASE_URL}/imports/getdbpreview", HEADERS, json.dumps(datos))

        if not respuesta or len(respuesta) <= 1:  # No hay más datos o error
            break

        if columnas is None:  # Primer lote, almacenar nombres de columnas
            columnas = respuesta[0]

        batch_data = respuesta[1:]

        if not batch_data:  # No hay más datos
            break

        # Si es el primer lote y el tamaño del lote es menor que chunk_size,
        # podemos devolver directamente un DataFrame
        if eff:
            if offset == 0 and len(batch_data) < chunk_size:
                df = pd.DataFrame(batch_data, columns=columnas)
                # Eliminar las columnas EffStart_ y EffEnd_ si existen
                # df = df.drop(columns=[col for col in ["EffStart_", "EffEnd_"] if col in df.columns], errors="ignore")
                logger.info(f"Consulta ejecutada correctamente, recuperadas {len(df)} filas.")
                return df

        # Si estamos recuperando múltiples lotes, vamos acumulando los datos
        full_data.extend(batch_data)
        logger.info(f"Recuperado lote {offset // chunk_size + 1} con {len(batch_data)} filas...")

        # Incrementar offset para el siguiente lote
        offset += len(batch_data)

        # Si hemos alcanzado el límite, detenerse
        if offset >= limit:
            break

    if not full_data:  # No se recuperaron datos
        logger.warning("No se recuperaron datos o error durante la ejecución de la consulta.")
        return pd.DataFrame()

    # Crear DataFrame con todos los datos acumulados
    df = pd.DataFrame(full_data, columns=columnas)

    logger.info(f"Consulta ejecutada correctamente, recuperadas {len(df)} filas en total.")
    return df


def main():
    logger.info("Iniciando Proceso Por Lotes - ReplicaICMVS-VentaCerveza")

    root = os.path.join("root", "Publication", "ReplicaICMVS")

    query = '''
        SELECT "PERNR"
        ,"LGART"
        ,"BEGDA"
        ,"ANZHL"
        ,"PTZEINH"
        ,"BETRG"
        ,"WAERS"
        ,"ESTDT"
        ,"ZUORD"
        ,"DATUM"
        ,"ZUTIMER"
        ,"ZSTAT"
        ,"ZFPROC"
        ,"ZUTIME"
        ,"ZUPROC"
        ,"ZDESCST"
        ,"ZINFOTIPO"
        FROM "ReplicaICMVS"
        WHERE "ZSTAT" = 'N'
        AND "DATUM" = (
        SELECT MAX("DATUM")
        FROM "ReplicaICMVS"
        WHERE "ZSTAT" = 'N'
        AND "LGART" = '118A'
        )
        AND "ZUTIMER" = (
        SELECT MAX("ZUTIMER")
        FROM "ReplicaICMVS"
        WHERE "DATUM" = (
        SELECT MAX("DATUM")
        FROM "ReplicaICMVS"
        WHERE "ZSTAT" = 'N'
          AND "LGART" = '118A'
        )
        AND "LGART" = '118A'
        AND "ZSTAT" = 'N'
        ) 
        AND "LGART" = '118A' 
    '''

    model = 'femcovsqa'
    limit = 10000

    try:

        rows_count = int(get_data_query(f'''
            SELECT COUNT(*) FROM (
            {query}            
            )             
        ''', model=model).values[0][0])

        if rows_count == 0:
            logger.info("No existen registros.")
        elif rows_count > limit:
            df = get_data_query(query=query, model=model)
            count_archivos = 1
            for offset in range(0, rows_count, limit):
                logger.info(f"Generando archivo, lote No.{count_archivos}")
                pd.DataFrame(df.iloc[offset:offset + limit]).to_csv(f"{root}/ReplicaICMVS-VentaCerveza-{count_archivos}.txt", index=False, sep=',')
                count_archivos += 1
        else:
            logger.info(f"Generando archivo, lote No.1")
            df = get_data_query(query=query, model=model)
            df.to_csv(f'{root}/ReplicaICMVS-VentaCerveza-1.txt', index=False, sep=',')

    except Exception as ex:
        logger.error(f"Error: {ex}")


if __name__ == '__main__':
    main()
