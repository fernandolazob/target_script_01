import os
import re
import pandas as pd
import win32com.client
from datetime import datetime, timedelta
from sqlalchemy import create_engine


def descargar_archivos_adjuntos(remitentes_permitidos,extensiones_permitidas,prefijos,fecha_inicio,ruta_adjuntos_outlook,ruta_lista_descarga_archivos_correo):

    def obtener_correo_remitente(mensaje):
        """
        Obtiene el correo SMTP del remitente.
        """
        try:
            if mensaje.SenderEmailType == "EX":

                usuario_exchange = mensaje.Sender.GetExchangeUser()

                if usuario_exchange:
                    return usuario_exchange.PrimarySmtpAddress

            return mensaje.SenderEmailAddress

        except Exception:
            return mensaje.SenderEmailAddress

    def limpiar_nombre_archivo(nombre):
        """
        Reemplaza caracteres no permitidos en Windows.
        """
        return re.sub(r'[<>:"/\\|?*]', "_", nombre)

    def obtener_ruta_unica(carpeta, nombre_archivo):
        """
        Evita reemplazar archivos ya existentes.
        """
        nombre_archivo = limpiar_nombre_archivo(nombre_archivo)

        # nombre, extension = os.path.splitext(nombre_archivo)
        ruta_final = os.path.join(carpeta, nombre_archivo)

        # contador = 1

        # while os.path.exists(ruta_final):

        #     nuevo_nombre = f"{nombre}_{contador}{extension}"

        #     ruta_final = os.path.join(
        #         carpeta,
        #         nuevo_nombre
        #     )

        #     contador += 1

        return ruta_final

    ruta_control = os.path.join(ruta_lista_descarga_archivos_correo,"control_retiro_efe.csv")
    fecha_fin = fecha_inicio + timedelta(days=1)

    columnas_control = [
            "correo_remitente",
            "asunto",
            "fecha_correo",
            "nombre_archivo",
            "ruta_archivo",
            "cuerpo"
        ]

    # validar=False

    if os.path.exists(ruta_control):

        df_control = pd.read_csv(
            ruta_control,
            sep=";",
            encoding="utf-8-sig"
        )

        df_control["fecha_correo"] = pd.to_datetime(
            df_control["fecha_correo"],
            errors="coerce"
        )

        df_control = df_control.dropna(
            subset=["fecha_correo"]
        ).copy()

        df_control["correo_remitente"] = (
            df_control["correo_remitente"]
            .astype(str)
            .str.strip()
            .str.lower()
        )

        df_control["asunto"] = (
            df_control["asunto"]
            .astype(str)
            .str.strip()
        )

        print(
            f"Control encontrado: {len(df_control)} registros"
        )
        hoy = fecha_inicio.date()

        df_control = df_control[
            df_control["fecha_correo"].dt.date == hoy
        ]
    else:

        df_control = pd.DataFrame(
            columns=columnas_control
        )

        print(
            "El archivo de control no existe. "
            "Se creará al finalizar."
        )

    # if not df_control.empty:

    #     df_control["fecha_actualizacion"] = (
    #         df_control["fecha_correo"].dt.date
    #     )

    # else:

    #     df_control["fecha_actualizacion"] = pd.Series(
    #         dtype="object"
    #     )
        
    # CONECTAR CON OUTLOOK
    outlook = win32com.client.Dispatch(
        "Outlook.Application"
    )

    namespace = outlook.GetNamespace("MAPI")
    bandeja_entrada = namespace.GetDefaultFolder(6)
    mensajes = bandeja_entrada.Items
    filtro = (
        f"[ReceivedTime] >= '{fecha_inicio.strftime('%d/%m/%Y %I:%M %p')}' "
        f"AND [ReceivedTime] < '{fecha_fin.strftime('%d/%m/%Y %I:%M %p')}'"
    )
    mensajes = mensajes.Restrict(filtro)

    mensajes.Sort("[ReceivedTime]", True)
    print("Correos encontrados:", mensajes.Count)

    # RECORRER Y DESCARGAR

    registro_descargas = []

    correos_revisados_por_dia = set()

    for mensaje in mensajes:
        try:
            # 43 = MailItem
            if mensaje.Class != 43:
                continue
            fecha_correo = mensaje.ReceivedTime.replace(tzinfo=None)
            correo_remitente = obtener_correo_remitente(mensaje)
                
            if not correo_remitente:
                continue
            correo_remitente = correo_remitente.strip().lower()
                
            if correo_remitente not in remitentes_permitidos:
                continue
            if correo_remitente not in archivos_permitidos:
                continue            

            asunto = (mensaje.Subject or "Sin asunto").strip()

            # Identificador por remitente, asunto
            clave_dia = (
                correo_remitente,
                asunto.lower()
            )

            if clave_dia in correos_revisados_por_dia:
                continue

            correos_revisados_por_dia.add(clave_dia)

            # VALIDAR QUE TENGA ADJUNTOS

            if mensaje.Attachments.Count == 0:
                continue

            # # ====================================================
            # # ====================================================
            # # ====================================================

            for posicion in range(
                1,
                mensaje.Attachments.Count + 1
            ):

                adjunto = mensaje.Attachments.Item(
                    posicion
                )

                nombre_archivo = adjunto.FileName
                cuerpo = mensaje.Body
                if not nombre_archivo.lower().endswith(extensiones_permitidas):
                    continue
                if not nombre_archivo.lower().startswith(prefijos):
                    continue

                existe = (
                    (df_control['fecha_correo'] == fecha_correo) &
                    (df_control['nombre_archivo'] == nombre_archivo)
                ).any()

                if existe:
                    continue

                ruta_final = obtener_ruta_unica(
                    ruta_adjuntos_outlook,
                    nombre_archivo
                )

                adjunto.SaveAsFile(ruta_final)


                registro_descargas.append({
                    "correo_remitente": correo_remitente,
                    "asunto": asunto,
                    "fecha_correo": fecha_correo,
                    "nombre_archivo": nombre_archivo,
                    "ruta_archivo": ruta_final,
                    "cuerpo":cuerpo
                })

                print(
                    f"Descargado: {nombre_archivo}"
                )


        except Exception as error:
            
            print(
                "Error procesando correo:",
                error
            )

    if not registro_descargas:
        print("No se encontró ningún correo")
        return pd.DataFrame()

    if registro_descargas:
        df_nuevos = pd.DataFrame(registro_descargas)
        df_control = pd.concat(
            [df_control, df_nuevos],
            ignore_index=True
        )
        # display(df_nuevos.head())
        # display(df_control.head())
        df_control = df_control.drop_duplicates(
            subset=["asunto", "nombre_archivo"],
            keep="last"
        )
        df_control.to_csv(
            ruta_control,
            index=False,
            encoding="utf-8-sig",
            sep=';'

        )
    return df_control

def nombre_mes_anio(fecha_mes_base):
    from datetime import datetime

    fecha = datetime.strptime(fecha_mes_base, "%Y-%m-%d")

    meses = [
        "enero", "febrero", "marzo", "abril", "mayo", "junio",
        "julio", "agosto", "septiembre", "octubre", "noviembre", "diciembre"
    ]

    return f"{meses[fecha.month - 1]} {fecha.year}"

def carga_desembolso_consumo(ruta_archivo,engine_samantha):
    df_desembolso_cli = pd.read_excel(ruta_archivo,sheet_name='Base')

    df_desembolso_cli["FECHA_HORA_DESEMBOLSO"] = pd.to_datetime(df_desembolso_cli["FECHA_HORA_DESEMBOLSO"], errors="coerce")

    fecha_min = df_desembolso_cli["FECHA_HORA_DESEMBOLSO"].min()
    if pd.isna(fecha_min):
        print('consumo sin info')
        return
    fecha_desembolso = fecha_min.replace(day=1)
    fecha_desembolso = fecha_min.replace(day=1).strftime("%Y-%m-%d")
    df_desembolso_cli['CAMPAÑA']=nombre_mes_anio(fecha_desembolso)


    from sqlalchemy import text

    with engine_samantha.begin() as conn:
        conn.execute(text(f"""
            DELETE FROM SAMANTHA.dbo.efectiva_ventas_desembolso
            WHERE FECHA_HORA_DESEMBOLSO >= '{fecha_desembolso}'
            AND FECHA_HORA_DESEMBOLSO <= EOMONTH('{fecha_desembolso}');
        """))

    df_desembolso_cli.to_sql(
        name="efectiva_ventas_desembolso",
        con=engine_samantha,
        if_exists="append",
        index=False,
        chunksize=1000
    )
    print('consumo')

def carga_desembolso_negocios(ruta_archivo,engine_samantha):
    df_desembolso_cli = pd.read_excel(ruta_archivo,sheet_name='Base')

    df_desembolso_cli["FECHA_DESEMBOLSO"] = pd.to_datetime(df_desembolso_cli["FECHA_DESEMBOLSO"], errors="coerce")

    fecha_min = df_desembolso_cli["FECHA_DESEMBOLSO"].min()

    if pd.isna(fecha_min):
        print('negocios sin info')

        return
        
    fecha_desembolso = fecha_min.replace(day=1)
    fecha_desembolso = fecha_min.replace(day=1).strftime("%Y-%m-%d")
    df_desembolso_cli['CAMPAÑA']=nombre_mes_anio(fecha_desembolso)


    from sqlalchemy import text

    with engine_samantha.begin() as conn:
        conn.execute(text(f"""
            DELETE FROM SAMANTHA.dbo.efectiva_negocios_ventas_desembolso
            WHERE FECHA_DESEMBOLSO >= '{fecha_desembolso}'
            AND FECHA_DESEMBOLSO <= EOMONTH('{fecha_desembolso}');
        """))

    df_desembolso_cli.to_sql(
        name="efectiva_negocios_ventas_desembolso",
        con=engine_samantha,
        if_exists="append",
        index=False,
        chunksize=1000
    )
    print('negocios')


user_sql = "Zeus"
pwd_sql = "target12345"
server_sql = "192.168.2.12"
db_sql = "SAMANTHA"
engine_samantha = create_engine(
    f"mssql+pyodbc://{user_sql}:{pwd_sql}@{server_sql}/{db_sql}"
    "?driver=ODBC+Driver+17+for+SQL+Server"
)

ruta_adjuntos_outlook = r"\\192.168.2.12\SQLServer Compartido\001_gestiones_py_vigente\Adjuntos_Outlook"
os.makedirs(ruta_adjuntos_outlook, exist_ok=True)
ruta_lista_descarga_archivos_correo = r"\\192.168.2.12\SQLServer Compartido\001_gestiones_py_vigente"


remitentes_permitidos = {
        "enviosbi@efectiva.com.pe",
    }

archivos_permitidos = {
        "LISTA ROBINSON.CSV",
    }

fecha_inicio = datetime.now().replace(
        hour=0,
        minute=0,
        second=0,
        microsecond=0
    )

extensiones_permitidas = (
        ".xlsx",
        ".xls",
    )

prefijos = (
        "target_efectinegocio",
        "target_efectivo"
    )

# fecha_inicio=fecha_inicio - timedelta(days=2)
print(fecha_inicio)
df_lista=descargar_archivos_adjuntos(remitentes_permitidos,extensiones_permitidas,prefijos,fecha_inicio,ruta_adjuntos_outlook,ruta_lista_descarga_archivos_correo)

procesos = {
    "target_efectivo": carga_desembolso_consumo,
    "target_efectinegocio": carga_desembolso_negocios,
}

if not df_lista.empty:
    list_archivo = set(df_lista["nombre_archivo"].str.lower())
    for archivo in list_archivo:

        for prefijo, funcion in procesos.items():
            if archivo.startswith(prefijo):

                ruta = os.path.join(
                    ruta_adjuntos_outlook,
                    archivo
                )

                funcion(
                    ruta,
                    engine_samantha
                )

                break
