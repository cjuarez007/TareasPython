from datetime import datetime
import gc
import sys
import os
# import psutil
import gc
import base64
import logging
from decimal import Decimal

db_path = os.path.join('root', 'Script', 'Z_MT_INC_VARIABLE_PROCESO', 'localbd_z_mt_inc_variable_proceso.duckdb')


# ===================================== ICM CLOUD=====================================

'''
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


agregarpath("Library")'''
# Ruta absoluta al archivo de funciones
ruta_funciones = os.path.join('root', 'Script', 'functions')

# Agregar al path de búsqueda de módulos
sys.path.insert(0, ruta_funciones)

import duckdb
import requests
import pandas as pd
import csv
import configparser
from common import OVR
from common import insert_csv_into_table
from common import export_table_to_csv
from common import fetch_and_convert_to_csv
from common import procesar_tabla
from common import fetch_and_convertresult_to_csv
from common import export_resultjson_string_to_csv
from common import safe_delete
from common import safe_truncate
from common import safe_insert
from common import safe_update
from common import borrar_csv_en_directorio
from dotenv import load_dotenv

dotenv_path = os.path.join('root', 'Script', '.env')
# Cargar el archivo .env
load_dotenv(dotenv_path)
# Crear instancia del parser
config = configparser.ConfigParser()
config.read(os.path.join('root', 'Script', 'Z_MT_INC_VARIABLE_PROCESO', 'config.ini'))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(
            os.path.join('root', 'Script', 'Z_MT_INC_VARIABLE_PROCESO', 'Z_MT_INC_VARIABLE_PROCESO.log'), mode='a'),
        logging.StreamHandler(sys.stdout)  # (opcional) para ver también en consola
    ]
)


# Redefinir print para usar logging
class LoggerWriter:
    def __init__(self, level):
        self.level = level

    def write(self, message):
        message = message.strip()
        if message:
            self.level(message)

    def flush(self):  # Requerido para compatibilidad con sys.stdout
        pass


sys.stdout = LoggerWriter(logging.info)
sys.stderr = LoggerWriter(logging.error)


# ===================================== MANEJO DE ARCHIVOS Y CARGA A DUCK DB =====================================


def get_duckdb_connection(db_path):
    """
    Crea y retorna una conexión a DuckDB con el límite de memoria configurado.
    """
    conn = duckdb.connect(db_path, read_only=False)
    memory_limit = config['DEFAULT']['duckdblimit']

    conn.execute(f"SET memory_limit = '{memory_limit}';")
    return conn


# ===================================== FUNCIONES AUXILIARES SP =====================================

def send_mail(
        subject,
        body,
        email_id=["ralvarez@exsoinf.com"],
        cc=["ralvarez@exsoinf.com"],
        # model=base64.b64decode(os.getenv("model")).decode("utf-8"),
        model='femcodev',
        api_url="https://api.cloud.varicent.com/api/v1/admin/tsapi/sendMail",
        auth_token=base64.b64decode(os.getenv("API_KEY")).decode("utf-8")):
    headers = {
        "Authorization": f"Bearer {auth_token}",
        "Content-Type": "application/json",
        "model": model
    }

    payload = {
        "to": email_id,
        "cc": cc,
        "subject": subject,
        "body": f"""
        <html>
          <body>
            <div class="container">
              {body}
              <p>Gracias por su atención.</p>
              <p>Correo automatico. Favor de no responder este correo.</p>
            </div>
          </body>
        </html>
        """,
        "useHtml": True,  # Important. It tells the API to interpret body as HTML.
    }

    try:
        response = requests.post(api_url, headers=headers, json=payload)
        response.raise_for_status()  # Raises exception for HTTP error codes
        print(f"Correo #{email_id} enviado con éxito.")
        return response.json()
    except requests.exceptions.HTTPError as http_err:
        print(f"Error HTTP en correo #{email_id}: {http_err}")
    except requests.exceptions.RequestException as req_err:
        print(f"Error en la solicitud del correo #{email_id}: {req_err}")


# ===================================== LOGICA SP =====================================


def main(db_path: str, Headers):
    allowed = config['DEFAULT']['allowed_tables']
    allowed_tables = [t.strip() for t in allowed.split(',')]
    try:
        CONN = get_duckdb_connection(db_path)
        FileName = os.path.join('root', 'Script', 'Z_MT_INC_VARIABLE_PROCESO', 'Input', 'HistoryAbsences.csv')
        CONN.execute('''
            BEGIN TRANSACTION
        ''');
        # CONN.execute(f'''
        #     TRUNCATE TABLE HistoryAbsences
        # ''')
        safe_truncate(CONN, 'HistoryAbsences', allowed_tables)
        CONN.execute(f'''
          INSERT INTO HistoryAbsences
          SELECT * FROM read_csv_auto('{FileName}',delim=',')
      ''')

        # CONN.execute(f'''
        #     TRUNCATE TABLE SALIDA_DETALLE
        # ''')
        safe_truncate(CONN, 'SALIDA_DETALLE', allowed_tables)
        FECHAMESANTERIOR = OVR(CONN.execute('''
          SELECT CAST(current_date - INTERVAL '1 month' AS DATE) AS fechamesanterior
      ''').fetchone())

        # TEST
        FECHAMESANTERIOR = '2025-05-01'

        # print(FECHAMESANTERIOR, type(FECHAMESANTERIOR))
        Inflacion = OVR(CONN.execute('''
          SELECT COALESCE(
              (SELECT CAST(INFLACION AS DECIMAL(14,4))
              FROM zMTVINFLACION
              WHERE ? BETWEEN CAST(BEGDA AS DATE) AND CAST(ENDDA AS DATE)),
              NULL
          ) AS INFLACION
      ''', (FECHAMESANTERIOR,)).fetchone())

        if Inflacion is not None:

            count = 0

            CONN.execute('''
              create TEMPORARY Table TAB 
              (
                  TAB1 decimal(14,4),
                  TAB2 decimal(14,4),
                  RANGO INT
              )
          ''')

            CONN.execute('''
              Insert Into TAB 
              select 0.0000,?-0.0001,?
          ''', (Inflacion, count + 1,))

            count += 1

            while (count < 6):
                CONN.execute('''
                  INSERT INTO TAB
                  select ?, ? + 0.0099 ,?
              ''', (Inflacion, Inflacion, count + 1,))

                Inflacion = Inflacion + Decimal("0.0100")
                count += 1

            CONN.execute('''
              INSERT INTO TAB
              select ?,2000.00,?
          ''', (Inflacion, count + 1,))

            CONN.execute('''
              CREATE TEMPORARY TABLE TEMP AS 
              select a.TDA_A,a.TDA_B,a.BEGDA,a.ENDDA,a.LGART,b.VTACONTABLE as VA_TDA_B,c.VTACONTABLE as VV_TDA_B,
              --((IFNULL(b.VTACONTABLE,0.00)-IFNULL(c.VTACONTABLE,0.00))/IFNULL(c.VTACONTABLE,0.0)) as Incremento, otra forma de sacar el incremento 
              IFNULL((b.VTACONTABLE/c.VTACONTABLE),0.0)-1 as Incremento,
              a.CASOTABULADOR,
              --			d.RANGO as RANGO_B,
              CASE WHEN g.ENFOQUE='E' then 1 
              WHEN g.ENFOQUE='I' then 2 
              WHEN g.ENFOQUE='C' then 3 
              WHEN g.ENFOQUE='D' then 4
              ELSE 0
              END as 'Enfoque_A',
              CASE WHEN e.ENFOQUE='E' then 1 
              WHEN e.ENFOQUE='I' then 2 
              WHEN e.ENFOQUE='C' then 3 
              WHEN e.ENFOQUE='D' then 4
              else 0
              END as 'Enfoque_B'
              from VALIDACION_VARIABLE a
              left join VTA_TDA_MULTI  b on a.TDA_B=b.IDSTORE  and  b.IDSTATUS=1
              AND date_trunc('month', CAST(b.FECHA AS DATE)) = date_trunc('month', CAST(a.BEGDA AS DATE))
              --left join VTA_TDA_MULTI  c on a.TDA_B=c.IDSTORE  and  c.IDSTATUS=1
              --and CAST(c.FECHA AS DATE) = date_trunc('month', CAST(a.BEGDA AS DATE) - interval '1 year') + interval '1 month' - interval '1 day'

              -- test
              left join VTA_TDA_MULTI c 
              on a.TDA_B = c.IDSTORE
              and c.IDSTATUS = 1
              and date_trunc('month', c.FECHA) = date_trunc('month', a.BEGDA - interval '1 year')
              --


              LEFT join ENFOQUE_TDA e 
              on e.TIENDA=a.TDA_B
              and date_trunc('month', CAST(e.BEGDA AS DATE)) = date_trunc('month', CAST(a.BEGDA AS DATE) - INTERVAL 1 month)
              left join ENFOQUE_TDA g 
              on g.TIENDA=a.TDA_A    
              and date_trunc('month', CAST(g.BEGDA AS DATE)) = date_trunc('month', CAST(a.BEGDA AS DATE) - INTERVAL 1 month)
              where a.IDS=1
          ''')

            # test --------------------------------
            # Imprimir los resultados de TEMP con headers
            print("Contenido de TEMP:")
            temp_results = CONN.execute("SELECT * FROM TEMP").fetchall()
            print([desc[0] for desc in CONN.execute("SELECT * FROM TEMP").description])  # Imprimir encabezados
            for row in temp_results:
                print(row)
            # ---------------------------------

            CONN.execute('''
              CREATE TEMPORARY TABLE ET AS
              select 
              a.TDA_A,a.TDA_B,a.BEGDA,a.ENDDA,a.LGART,
              IFNULL(a.VA_TDA_B,0.0) as 'VA_TDA_B',
              IFNULL(a.VV_TDA_B,0.0) as 'VV_TDA_B',
              a.Incremento,a.CASOTABULADOR,a.Enfoque_A,a.Enfoque_B,
              case 
              when a.Incremento >0.0 then (select d.RANGO from TAB d where a.Incremento between d.TAB1 and d.TAB2)
              else 
              1
              END as 'RANGO'
              from TEMP a
          ''')

            CONN.execute('''
              CREATE TEMPORARY TABLE ENTRADA_TABULADOR AS 
              select Xd.*,IFNULL(fb.PAGOENCARGADO,0.0) as 'PAGOENCARGADO_B',IFNULL(fb.PAGOLIDER,0.0) as 'PAGOLIDER_B_VTA',
              IFNULL(fb.PAGOLIDERENFOQUE,0.0) as 'PAGOLIDER_B_EFQ'
              ,IFNULL(fa.PAGOENCARGADO,0.0) as 'PAGOENCARGADO_A' 
              from (
              select TDA_A,TDA_B,BEGDA,ENDDA,LGART,VA_TDA_B,VV_TDA_B,Incremento,CASOTABULADOR,Enfoque_A,Enfoque_B,RANGO as 'RANGO_B' 
              from ET
              ) Xd
              left join TABULADORES_DESEMPEÑO fb
              on fb.CASOTABULADOR=Xd.CASOTABULADOR 
              and fb.ENFOQUE=Xd.Enfoque_B
              and fb.RANGO=Xd.RANGO_B
              left join TABULADORES_DESEMPEÑO fa
              on fa.CASOTABULADOR=Xd.CASOTABULADOR 
              and fa.ENFOQUE=Xd.Enfoque_A
              and fa.RANGO=Xd.RANGO_B
          ''')

            # test--------------------------------

            # Imprimir los resultados de ENTRADA_TABULADOR
            print("Contenido de ENTRADA_TABULADOR:")
            et = CONN.execute("SELECT * FROM ENTRADA_TABULADOR").fetchall()
            for row in et:
                print(row)

            # ---------------------------------

            # Guardar ENTRADA_TABULADOR en un archivo CSV para revisión
            export_table_to_csv(CONN, "ENTRADA_TABULADOR",
                                os.path.join('root', 'Script', 'Z_MT_INC_VARIABLE_PROCESO', 'Output',
                                             'ENTRADA_TABULADOR_debug.csv'))

            # results = CONN.execute("SELECT * FROM ENTRADA_TABULADOR").fetchall()
            results = CONN.execute(
                "SELECT DISTINCT TDA_B FROM VALIDACION_VARIABLE UNION SELECT DISTINCT TDA_A FROM VALIDACION_VARIABLE").fetchall()

            # Paso 1: Extraer los IDStore únicos (primera columna)
            id_stores = sorted(set(row[0] for row in results))

            # Paso 2: Armar una cadena entre comillas
            id_store_str = ", ".join(f"'{id}'" for id in id_stores)
            # id_store_str= "'TIE-10CUE50OK5','TIE-10CUE50PXF','TIE-10OBR501I8','TIE-10OBR5024Q','TIE-10OBR50EEQ','TIE-10OBR50RTF','TIE-10PBI50AI1','TIE-10PBI50J4X','TIE-10PBI50K9T','TIE-10PBI50PCL','TIE-10UMI50916','TIE-10UMI50WXT'"
            print(id_store_str)
            queries = [
                {
                    "queryString": f"""
          SELECT "PayeeID","Parent","TitleID","ReportsTo",
                      to_char("DateStart", 'YYYY-MM-DD') AS "DateStart",
                      to_char("DateEnd", 'YYYY-MM-DD') AS "DateEnd","IDOrganizationalUnit","IDSociety","IDPersonalDivision","IDPersonalSubdivision","IDEEGroup","IDPersonalArea","IDPayrollArea","IDPosition","IDJobKey","IDCostCenter","IDAuxiliaryCeco","IDStore","IDRole",
                      "DateInsertion",
                      to_char("EffStart_", 'YYYY-MM-DD') AS "EffStart",
                      to_char("EffEnd_", 'YYYY-MM-DD') AS "EffEnd","IDStatus" FROM "HistoryPayee" WHERE "IDStore" IN (
                              {id_store_str}
                          )          

              """,

                    "output": "HistoryPayee.csv"
                }
            ]

            for query in queries:
                payload = {
                    "queryString": query["queryString"],
                    "offset": 0,
                    "limit": 0,
                    "exportFileFormat": "Text"
                }

                output_csv = os.path.join(base_path, query["output"])
                fetch_and_convert_to_csv(api_url, headers, payload, output_csv, CONN)
            csv_table_map = {
                'HistoryPayee.csv': "HistoryPayee"
            }

            for file_name, table_name in csv_table_map.items():
                csv_file = os.path.join(base_path, file_name)
                insert_csv_into_table(CONN, csv_file, table_name, header=True, truncate=True)

            CONN.execute('''
              CREATE TEMPORARY TABLE LIDER_TAB AS 
              SELECT
              a.*,
              c.PayeeID,
              'L' AS 'Rol',
              (ABS(
              DATEDIFF(
                'day',
                CAST(
                  (CASE
                      WHEN a.ENDDA > c.DateEnd THEN c.DateEnd
                      ELSE a.ENDDA
                    END) AS DATE
                ),
                CAST(
                  (CASE
                      WHEN a.BEGDA > c.DateStart THEN a.BEGDA
                      ELSE c.DateStart
                    END) AS DATE
                )
              )
              ) + 1
              ) AS D,
              CAST(
              DAY(
                LAST_DAY(
                  CAST(a.BEGDA AS DATE)
                )
              ) AS DECIMAL(16,2)
              ) AS M,
              CASE
              WHEN c.DateStart < a.BEGDA THEN CAST(a.BEGDA AS DATE)
              ELSE CAST(c.DateStart AS DATE)
              END AS 'DateStart',
              CASE
              WHEN c.DateEnd > a.ENDDA THEN CAST(a.ENDDA AS DATE)
              ELSE CAST(c.DateEnd AS DATE)
              END AS 'DateEnd',
              (
              (ABS(
                DATEDIFF(
                  'day',
                  CAST(
                    (CASE
                        WHEN a.ENDDA > c.DateEnd THEN c.DateEnd
                        ELSE a.ENDDA
                      END) AS DATE
                  ),
                  CAST(
                    (CASE
                        WHEN a.BEGDA > c.DateStart THEN a.BEGDA
                        ELSE c.DateStart
                      END) AS DATE
                  )
                )
              ) + 1
              )
              / CAST(
                  DAY(
                    LAST_DAY(
                      CAST(a.BEGDA AS DATE)
                    )
                  ) AS DECIMAL(16,2)
                )
              ) AS 'Proporcional',
              (
              (
                (ABS(
                  DATEDIFF(
                    'day',
                    CAST(
                      (CASE
                          WHEN a.ENDDA > c.DateEnd THEN c.DateEnd
                          ELSE a.ENDDA
                        END) AS DATE
                    ),
                    CAST(
                      (CASE
                          WHEN a.BEGDA > c.DateStart THEN a.BEGDA
                          ELSE c.DateStart
                        END) AS DATE
                    )
                  )
                ) + 1
                )
                / CAST(
                    DAY(
                      LAST_DAY(
                        CAST(a.BEGDA AS DATE)
                      )
                    ) AS DECIMAL(16,2)
                  )
              ) * a.PAGOLIDER_B_VTA
              ) AS 'PAGOSAP_VTA',
              (
              (
                (ABS(
                  DATEDIFF(
                    'day',
                    CAST(
                      (CASE
                          WHEN a.ENDDA > c.DateEnd THEN c.DateEnd
                          ELSE a.ENDDA
                        END) AS DATE
                    ),
                    CAST(
                      (CASE
                          WHEN a.BEGDA > c.DateStart THEN a.BEGDA
                          ELSE c.DateStart
                        END) AS DATE
                    )
                  )
                ) + 1
                )
                / CAST(
                    DAY(
                      LAST_DAY(
                        CAST(a.BEGDA AS DATE)
                      )
                    ) AS DECIMAL(16,2)
                  )
              ) * a.PAGOLIDER_B_EFQ
              ) AS 'PAGOSAP_EnFOQUE',
              (
              (
                (ABS(
                  DATEDIFF(
                    'day',
                    CAST(
                      (CASE
                          WHEN a.ENDDA > c.DateEnd THEN c.DateEnd
                          ELSE a.ENDDA
                        END) AS DATE
                    ),
                    CAST(
                      (CASE
                          WHEN a.BEGDA > c.DateStart THEN a.BEGDA
                          ELSE c.DateStart
                        END) AS DATE
                    )
                  )
                ) + 1
                )
                / CAST(
                    DAY(
                      LAST_DAY(
                        CAST(a.BEGDA AS DATE)
                      )
                    ) AS DECIMAL(16,2)
                  )
              ) * a.PAGOLIDER_B_VTA
              )
              +
              (
              (
                (ABS(
                  DATEDIFF(
                    'day',
                    CAST(
                      (CASE
                          WHEN a.ENDDA > c.DateEnd THEN c.DateEnd
                          ELSE a.ENDDA
                        END) AS DATE
                    ),
                    CAST(
                      (CASE
                          WHEN a.BEGDA > c.DateStart THEN a.BEGDA
                          ELSE c.DateStart
                        END) AS DATE
                    )
                  )
                ) + 1
                )
                / CAST(
                    DAY(
                      LAST_DAY(
                        CAST(a.BEGDA AS DATE)
                      )
                    ) AS DECIMAL(16,2)
                  )
              ) * a.PAGOLIDER_B_EFQ
              ) AS 'PAGOSAP',
              IFNULL(c.IDPersonalArea,'NP') AS 'IDPA'
              FROM ENTRADA_TABULADOR a
              inner join HistoryPayee c on 
              c.IDStore =a.TDA_A
              and c.IDRole like 'L%' 
              and c.IDStatus=1 
              and c.DateStart <= a.ENDDA and c.DateEnd >= a.BEGDA
              GROUP BY a.CASOTABULADOR,a.TDA_A,a.TDA_B,a.BEGDA,a.ENDDA,a.LGART,a.VA_TDA_B,a.VV_TDA_B,a.Incremento, a.RANGO_B,a.Enfoque_A,a.Enfoque_B,a.PAGOENCARGADO_B,a.PAGOENCARGADO_A,
              a.PAGOLIDER_B_VTA,
              a.PAGOLIDER_B_EFQ,
              c.PayeeID,c.DateStart,c.DateEnd,c.IDPersonalArea
          ''')

            CONN.execute('''
              CREATE TEMPORARY TABLE LIDER_GROUP1 AS
              select  a.*,IFNULL(b.DIAS,0.0)as 'Ausentismos' ,
              --(IFNULL(b.Days,0.0)/a.M)*a.PAGOLIDER_B
              (IFNULL(b.DIAS,0.0)/a.M)*a.PAGOLIDER_B_VTA
              + 
              (IFNULL(b.DIAS,0.0)/a.M)*a.PAGOLIDER_B_EFQ
              as 'PAusentismos'     
              from LIDER_TAB a 
              --LEFT join  HistoryAbsences b  
              LEFT join  HistoryAbsences b
              --on a.PayeeID=b.PayeeID and  b.DateStart between a.DateStart and a.DateEnd
              on a.PayeeID=b.EMPLEADO   and  b.FECHAINICIO between a.DateStart and a.DateEnd
              --and  b.IDAbsence in (select IDAbsence from CfgAbsences where IDStatus=1) 

          ''')

            CONN.execute('''
              CREATE TEMPORARY TABLE GROUP1 AS
              select TDA_A,TDA_B,BEGDA,ENDDA,LGART,VA_TDA_B,VV_TDA_B,Incremento,CASOTABULADOR,RANGO_B,Enfoque_A,Enfoque_B,PAGOENCARGADO_B,
              PAGOLIDER_B_VTA,
              PAGOLIDER_B_EFQ,
              PAGOENCARGADO_A,
              PayeeID, Rol, D, M, DateStart, DateEnd, Proporcional, PAGOSAP, IDPA,SUM(Ausentismos) as 'Ausentismos',SUM(PAusentismos) as'Pausentismos'    
              from LIDER_GROUP1 
              group by 
              TDA_A,TDA_B,BEGDA,ENDDA,LGART,VA_TDA_B,VV_TDA_B,Incremento,CASOTABULADOR,Enfoque_A,Enfoque_B,	
              RANGO_B,PAGOENCARGADO_B,
              PAGOLIDER_B_VTA,
              PAGOLIDER_B_EFQ,
              PAGOENCARGADO_A,PayeeID,Rol,D,M,DateStart,DateEnd,Proporcional,PAGOSAP,IDPA
          ''')

            safe_insert(
                CONN,
                'SALIDA_DETALLE',
                '''
                INSERT INTO {tabla}
                select *,(PAGOSAP-Pausentismos) as 'PAGOSAPFINAL' from GROUP1
                ''',
                allowed_tables,
                None
            )
            CONN.execute('''
          CREATE TEMPORARY TABLE ENCARGADO_A_TAB AS 
          SELECT
          a.*,
          c.PayeeID,
          'EA' AS "Rol",

          -- Días trabajados
          (
          ABS(
            datediff(
              'day',
              CAST(
                CASE
                  WHEN a.ENDDA > c.DateEnd THEN c.DateEnd
                  ELSE a.ENDDA
                END AS DATE
              ),
              CAST(
                CASE
                  WHEN a.BEGDA > c.DateStart THEN a.BEGDA
                  ELSE c.DateStart
                END AS DATE
              )
            )
          )
          + 1
          ) AS D,

          -- Mes original reemplazado temporalmente (en pruebas)
          CAST(DAY(last_day(CAST(a.BEGDA AS DATE))) AS DECIMAL(16,2)) AS M,
          --1 AS M,

          -- Fecha de inicio efectiva
          CASE
          WHEN c.DateStart < a.BEGDA THEN a.BEGDA
          ELSE c.DateStart
          END AS "DateStart",

          -- Fecha final efectiva
          CASE
          WHEN c.DateEnd > a.ENDDA THEN a.ENDDA
          ELSE c.DateEnd
          END AS "DateEnd",

          -- Proporción del mes trabajado
          (
          ABS(
            datediff(
              'day',
              CAST(
                CASE
                  WHEN a.ENDDA > c.DateEnd THEN c.DateEnd
                  ELSE a.ENDDA
                END AS DATE
              ),
              CAST(
                CASE
                  WHEN a.BEGDA > c.DateStart THEN a.BEGDA
                  ELSE c.DateStart
                END AS DATE
              )
            )
          )
          + 1
          )
          /
          CAST(
          DAY(last_day(CAST(a.BEGDA AS DATE)))
          AS DECIMAL(16,2)
          ) AS "Proporcional",

          -- Pago proporcional
          (
          (
            ABS(
              datediff(
                'day',
                CAST(
                  CASE
                    WHEN a.ENDDA > c.DateEnd THEN c.DateEnd
                    ELSE a.ENDDA
                  END AS DATE
                ),
                CAST(
                  CASE
                    WHEN a.BEGDA > c.DateStart THEN a.BEGDA
                    ELSE c.DateStart
                  END AS DATE
                )
              )
            )
            + 1
          )
          /
          CAST(
            DAY(last_day(CAST(a.BEGDA AS DATE)))
            AS DECIMAL(16,2)
          )
          )
          * a.PAGOENCARGADO_A AS PAGOSAP,

          IFNULL(c.IDPersonalArea, 'NP') AS "IDPA"
          FROM ENTRADA_TABULADOR a
          INNER JOIN HistoryPayee c
          ON c.IDStore = a.TDA_A
          AND c.IDRole LIKE 'E%'
          AND c.IDStatus = 1
          AND c.DateStart <= a.ENDDA
          AND c.DateEnd >= a.BEGDA
          GROUP BY
          a.CASOTABULADOR,
          a.TDA_A,
          a.TDA_B,
          a.BEGDA,
          a.ENDDA,
          a.LGART,
          a.VA_TDA_B,
          a.VV_TDA_B,
          a.Incremento,
          a.RANGO_B,
          a.Enfoque_A,
          a.Enfoque_B,
          a.PAGOENCARGADO_B,
          a.PAGOENCARGADO_A,
          a.PAGOLIDER_B_VTA,
          a.PAGOLIDER_B_EFQ,
          c.PayeeID,
          c.DateStart,
          c.DateEnd,
          c.IDPersonalArea;
          ''')

            CONN.execute('''
          CREATE TEMPORARY TABLE EA_GROUP1 AS
          select  a.*,IFNULL(b.DIAS,0.0)as 'Ausentismos' ,
          (IFNULL(b.DIAS,0.0)/a.M)*a.PAGOENCARGADO_A as 'PAusentismos' 
          from ENCARGADO_A_TAB a 
          --LEFT join  HistoryAbsences b  
          LEFT join  HistoryAbsences b  
          --on a.PayeeID=b.PayeeID and  CAST(b.DateStart AS DATE) between CAST(a.DateStart AS DATE) and CAST(a.DateEnd AS DATE)
          on a.PayeeID=b.EMPLEADO  and  CAST(b.FECHAINICIO AS DATE) between CAST(a.DateStart AS DATE) and CAST(a.DateEnd AS DATE)
          --and  b.IDAbsence in (select IDAbsence from CfgAbsences where IDStatus=1) 
          ''')

            CONN.execute('''    
              CREATE TEMPORARY TABLE GROUP2 AS
              select TDA_A,TDA_B,BEGDA,ENDDA,LGART,VA_TDA_B,VV_TDA_B,Incremento,CASOTABULADOR,RANGO_B,Enfoque_A,Enfoque_B,PAGOENCARGADO_B,PAGOLIDER_B_VTA,PAGOLIDER_B_EFQ,PAGOENCARGADO_A,
              PayeeID, Rol, D, M, DateStart, DateEnd, Proporcional, PAGOSAP, IDPA,SUM(Ausentismos) as 'Ausentismos',SUM(PAusentismos) as'Pausentismos'    
              from EA_GROUP1 
              group by 
              TDA_A,TDA_B,BEGDA,ENDDA,LGART,VA_TDA_B,VV_TDA_B,Incremento,CASOTABULADOR,Enfoque_A,Enfoque_B,	
              RANGO_B,PAGOENCARGADO_B,PAGOLIDER_B_VTA,PAGOLIDER_B_EFQ,PAGOENCARGADO_A,PayeeID,Rol,D,M,DateStart,DateEnd,Proporcional,PAGOSAP,IDPA
          ''')

            safe_insert(
                CONN,
                'SALIDA_DETALLE',
                '''
                INSERT INTO {tabla}
                select *,(PAGOSAP-Pausentismos) as 'PAGOSAPFINAL' from GROUP2
                ''',
                allowed_tables,
                None
            )

            CONN.execute('''
              CREATE TEMPORARY TABLE ENCARGADO_B_TAB AS
              SELECT
              a.*,
              c.PayeeID,
              'EB' AS "Rol",

              -- Cálculo de D
              (
              ABS(
                datediff(
                  'day',
                  CAST(
                    CASE WHEN a.ENDDA > c.DateEnd THEN c.DateEnd ELSE a.ENDDA END
                  AS DATE),
                  CAST(
                    CASE WHEN a.BEGDA > c.DateStart THEN a.BEGDA ELSE c.DateStart END
                  AS DATE)
                )
              ) + 1
              ) AS D,

              -- M con last_day (último día del mes de BEGDA)
              CAST(
              DAY(last_day(CAST(a.BEGDA AS DATE)))
              AS DECIMAL(16,2)
              ) AS M,

              -- DateStart y DateEnd
              CASE WHEN c.DateStart < a.BEGDA THEN a.BEGDA ELSE c.DateStart END AS "DateStart",
              CASE WHEN c.DateEnd   > a.ENDDA THEN a.ENDDA   ELSE c.DateEnd   END AS "DateEnd",

              -- Proporcional
              (
              ABS(
                datediff(
                  'day',
                  CAST(
                    CASE WHEN a.ENDDA > c.DateEnd THEN c.DateEnd ELSE a.ENDDA END
                  AS DATE),
                  CAST(
                    CASE WHEN a.BEGDA > c.DateStart THEN a.BEGDA ELSE c.DateStart END
                  AS DATE)
                )
              ) + 1
              )
              / CAST(
                DAY(last_day(CAST(a.BEGDA AS DATE)))
                AS DECIMAL(16,2)
              )
              AS Proporcional,

              -- PAGOSAP
              (
              (
                ABS(
                  datediff(
                    'day',
                    CAST(
                      CASE WHEN a.ENDDA > c.DateEnd THEN c.DateEnd ELSE a.ENDDA END
                    AS DATE),
                    CAST(
                      CASE WHEN a.BEGDA > c.DateStart THEN a.BEGDA ELSE c.DateStart END
                    AS DATE)
                  )
                ) + 1
              )
              / CAST(
                  DAY(last_day(CAST(a.BEGDA AS DATE)))
                  AS DECIMAL(16,2)
                )
              ) * a.PAGOENCARGADO_B AS PAGOSAP,
              -- IDPA
              IFNULL(c.IDPersonalArea, CAST(3781 AS VARCHAR)) AS "IDPA"
              FROM ENTRADA_TABULADOR a
              INNER JOIN HistoryPayee c
              ON c.IDStore = a.TDA_B
              AND c.IDRole LIKE 'E%'
              AND c.IDStatus = 1
              AND c.DateStart <= a.ENDDA
              AND c.DateEnd >= a.BEGDA        
              GROUP BY
              a.CASOTABULADOR,
              a.TDA_A,
              a.TDA_B,
              a.BEGDA,
              a.ENDDA,
              a.LGART,
              a.VA_TDA_B,
              a.VV_TDA_B,
              a.Incremento,
              a.RANGO_B,
              a.Enfoque_A,
              a.Enfoque_B,
              a.PAGOENCARGADO_B,
              a.PAGOENCARGADO_A,
              a.PAGOLIDER_B_VTA,
              a.PAGOLIDER_B_EFQ,
              c.PayeeID,
              c.DateStart,
              c.DateEnd,
              c.IDPersonalArea;
          ''')

            CONN.execute('''
              CREATE TEMPORARY TABLE EB_GROUP1 AS
              select  a.*,IFNULL(b.DIAS,0.0)as 'Ausentismos' ,
              (IFNULL(b.DIAS,0.0)/a.M)*a.PAGOENCARGADO_B as 'PAusentismos'         
              from ENCARGADO_B_TAB a 
              --LEFT join  HistoryAbsences b  
              LEFT join  HistoryAbsences b  
              --on a.PayeeID=b.PayeeID and  CAST(b.DateStart AS DATE) between CAST(a.DateStart AS DATE) and CAST(a.DateEnd AS DATE)
              on a.PayeeID=b.EMPLEADO   and  CAST(b.FECHAINICIO AS DATE) between CAST(a.DateStart AS DATE) and CAST(a.DateEnd AS DATE)
              --and  b.IDAbsence in (select IDAbsence from CfgAbsences where IDStatus=1) 
          ''')

            CONN.execute('''
              CREATE TEMPORARY TABLE GROUP3 AS
              select TDA_A,TDA_B,BEGDA,ENDDA,LGART,VA_TDA_B,VV_TDA_B,Incremento,CASOTABULADOR,RANGO_B,Enfoque_A,Enfoque_B,PAGOENCARGADO_B,PAGOLIDER_B_VTA,PAGOLIDER_B_EFQ,PAGOENCARGADO_A,
              PayeeID, Rol, D, M, DateStart, DateEnd, Proporcional, PAGOSAP, IDPA,SUM(Ausentismos) as 'Ausentismos',SUM(PAusentismos) as'Pausentismos'
              from EB_GROUP1 
              group by 
              TDA_A,TDA_B,BEGDA,ENDDA,LGART,VA_TDA_B,VV_TDA_B,Incremento,CASOTABULADOR,Enfoque_A,Enfoque_B,	
              RANGO_B,PAGOENCARGADO_B,PAGOLIDER_B_VTA,PAGOLIDER_B_EFQ,PAGOENCARGADO_A,PayeeID,Rol,D,M,DateStart,DateEnd,Proporcional,PAGOSAP,IDPA    
          ''')

            safe_insert(
                CONN,
                'SALIDA_DETALLE',
                '''
                INSERT INTO {tabla}
                select *,(PAGOSAP-Pausentismos) as 'PAGOSAPFINAL' from GROUP3
                ''',
                allowed_tables,
                None
            )

            ReportId = 7

            Subject = 'ICM CV (Correo Informativo sin contenido) MultiTienda Inc.variable Termino de ejecucion'
            Body = '<h1>Se realizo la ejecucion del proceso de Inc.variable Multitienda </h1>'

            Footer = OVR(CONN.execute('''
              SELECT "To" FROM ICMToolsReports WHERE ReportId = ?
            ''', (ReportId,)).fetchone())

            Table = '''
              <table border = "0" style="font-family:Calibri;font-size:11pt">
              <thead style="color: white; background-color:lightblue;">
                <th><strong> Registros</strong></th>
              </thead>
              <tbody>
          '''

            count = CONN.execute('''
              SELECT COALESCE(Count(*), 0)
              FROM SALIDA_DETALLE
          ''').fetchone()[0]
            Table += f'''
                      <tr>
                          <td>{count}</td>
                      </tr>
                  </tbody>
              </table>
          '''

            Body += Table + Footer

            to_emails = [email.strip() for email in config["DEFAULT"]["ToEmail"].split(",")]
            cc = [email.strip() for email in config["DEFAULT"]["CC"].split(",")]
            send_mail(
                subject=Subject,
                body=Body,
                email_id=to_emails,
                cc=cc
            )

        else:
            ReportId = 7
            Inflacion = 0.0

            subject = 'ICM CV (Correo Informativo sin contenido) MultiTienda Inc.variable Termino de ejecucion'
            body = '''
              No se tiene Inflacion para el periodo a calcular favor de revisar la tabla en ICM zMTVINFLACION de Inc.variable Multitienda
          '''
            MyFooterI = OVR(CONN.execute('''
          SELECT Footer FROM ICMToolsReports WHERE ReportId = ?
          ''', (ReportId,)).fetchone())
            to_emails = [email.strip() for email in config["DEFAULT"]["ToEmail"].split(",")]
            cc = [email.strip() for email in config["DEFAULT"]["CC"].split(",")]
            send_mail(
                subject=Subject,
                body=body + MyFooterI,
                email_id=to_emails,
                cc=cc
            )

        CONN.execute('''
        COMMIT
      ''');

    finally:
        CONN.close()


if __name__ == "__main__":

    api_url = 'https://api.cloud.varicent.com/api/v1/rpc/querytool/export'
    token = "icm-VQ1qp8Ls6dDwN+6aW2R+7+j4i3BzqMb64Bd6ygZB1Vs="

    headers = {
        "Authorization": f"Bearer {token}",
        "Model": "femcoepqa"
    }
    base_path = os.path.join('root', 'Script', 'Z_MT_INC_VARIABLE_PROCESO')
    # ===================================== INFORMACION MODELO =====================================

    try:

        queries = [
            {
                "queryString": """
            SELECT "TDAA","TDAB",to_char("BEGDA", 'YYYY-MM-DD')AS "BEGDA",to_char("ENDDA", 'YYYY-MM-DD') AS "ENDDA","LGART","VATDAB","VVTDAB","Incremento","CASOTABULADOR","RANGOB","EnfoqueA","EnfoqueB","PAGOENCARGADOB","PAGOLIDERBVTA","PAGOLIDERBEFQ","PAGOENCARGADOA","PayeeID","Rol","D","M",to_char("DateStart", 'YYYY-MM-DD') AS "DateStart",to_char("DateEnd", 'YYYY-MM-DD') AS "DateEnd","Proportional","PAGOSAP","IDPA","AUSENTISMOS","PTRESTAR","PAGOFINAL" 
            FROM "FemcoTransferSalidaDetalle_"          

            """,
                "output": "SALIDA_DETALLE.csv"
            },
            {
                "queryString": """
                SELECT "INFLACION",to_char("BEGDA", 'YYYY-MM-DD') AS "BEGDA",to_char("ENDDA", 'YYYY-MM-DD') AS "ENDDA",to_char("EffStart_", 'YYYY-MM-DD') AS "EffStart",to_char("EffEnd_", 'YYYY-MM-DD') AS "ENDDA"
                FROM "zMTVINFLACION"            

            """,
                "output": "zMTVINFLACION.csv"
            },
            {
                "queryString": """
            SELECT "IDSTORE",to_char("FECHA"::date, 'YYYY-MM-DD') AS "FECHA","VTACONTABLE","TRAFICO","IDSTATUS"
            FROM "FemcoTransferVTATDAMULTI"             

            """,
                "output": "VTA_TDA_MULTI.csv"
            },
            {
                "queryString": """
              SELECT "TIENDA","ENFOQUE",to_char("BEGDA"::date, 'YYYY-MM-DD') AS "BEGDA",to_char("ENDDA"::date, 'YYYY-MM-DD') AS "ENDDA"
                FROM "FemcoTransferENFOQUETDA"             

            """,
                "output": "ENFOQUE_TDA.csv"
            },
            {
                "queryString": """
              SELECT "CASOTABULADOR","ENFOQUE","RANGO","PAGOLIDER","PAGOENCARGADO","PAGOLIDERENFOQUE"
              FROM "FemcoTransferTABULADORESDESEMPENO"           

            """,
                "output": "TABULADORES_DESEMPEÑO.csv"
            },
            {
                "queryString": """
            SELECT "IDAbsence","Descripcion","IDStatus"
            FROM "CfgAbsences"             

            """,
                "output": "CfgAbsences.csv"
            },
            {
                "queryString": """
            SELECT
                "CASOTABULADOR",
                "CRPLAZAA",
                "CRTIENDAA",
                "TDAA",
                "CRPLAZAB",
                "CRTIENDAB",
                "TDAB",
                to_char("BEGDA"::DATE, 'YYYY-MM-DD') AS "BEGDA",
                to_char("ENDDA"::DATE, 'YYYY-MM-DD') AS "ENDDA",
                "LGART",
                "IDS",
                "DESERROR",
                to_char("DateInsert"::DATE, 'YYYY-MM-DD') AS "DateInsert"
            FROM "FemcoTransferVALIDACIONVARIABLE"            

            """,
                "output": "VALIDACION_VARIABLE.csv"
            },
            {
                "queryString": """
          SELECT     "ReportId",
              "Name" ,
              "Subject",
              "Body" ,
              "Footer" ,
              "To" ,
              "Cc" ,
              "Bcc" ,
              "CreatedDate" FROM "FemcoTransferICMToolsReports"
          WHERE "ReportId"='7'          

            """,
                "output": "ICMToolsReports.csv"
            }
        ]
        for query in queries:
            payload = {
                "queryString": query["queryString"],
                "offset": 0,
                "limit": 0,
                "exportFileFormat": "Text"
            }
            CONN = get_duckdb_connection(db_path)
            output_csv = os.path.join(base_path, query["output"])
            fetch_and_convert_to_csv(api_url, headers, payload, output_csv, CONN)

    finally:
        CONN.close()

    # ===================================== CARGA A DUCK DB =====================================

    csv_table_map = {
        'SALIDA_DETALLE.csv': "SALIDA_DETALLE",
        'zMTVINFLACION.csv': "zMTVINFLACION",
        'VTA_TDA_MULTI.csv': "VTA_TDA_MULTI",
        'ENFOQUE_TDA.csv': "ENFOQUE_TDA",
        'TABULADORES_DESEMPEÑO.csv': "TABULADORES_DESEMPEÑO",
        'CfgAbsences.csv': "CfgAbsences",
        'VALIDACION_VARIABLE.csv': "VALIDACION_VARIABLE",
        'ICMToolsReports.csv': "ICMToolsReports"
    }

    CONN = get_duckdb_connection(db_path)
    try:
        for file_name, table_name in csv_table_map.items():
            csv_file = os.path.join(base_path, file_name)
            insert_csv_into_table(CONN, csv_file, table_name, header=True, truncate=True)
    finally:
        CONN.close()

# ===================================== EJECUCION SP =====================================

main(db_path, headers)

# ===================================== EXPORTAR CSV FINAL =====================================


CONN = get_duckdb_connection(db_path)
try:
    table = 'SALIDA_DETALLE'
    output_file = os.path.join('root', 'Data', 'SALIDA_DETALLEEXPORT.csv')
    export_table_to_csv(CONN, table, output_file)
finally:
    CONN.close()

# ===================================== BORRAR ARCHIVOS CSV DE PASO =====================================
directorio = os.path.join('root', 'Script', 'Z_MT_INC_VARIABLE_PROCESO')
# borrar_csv_en_directorio(directorio)
