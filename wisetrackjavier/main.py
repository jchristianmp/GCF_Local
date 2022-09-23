# -*- coding: utf-8 -*-
import json
import requests
import datetime
import pytz
import os
from google.cloud import storage


def _AgregarCeroMenor9(num):  # entero como parametro y devuelve STRING
    if int(num) <= 9:
        num = "0" + str(num)
    else:
        str(num)
    return num


def getjsonfromwisetrack(request):
    try:
        ################################
        # Parametros obtenidos por post:
        request_json = request.get_json()
        # Las patentes son las placas de los moviles: patente='0',
        # trae toda las placas de los moviles.
        # tanto para utlima posicion,alertas telemetria y registros button
        patente = request_json["patente"]
        # para tipos de alertas telemetria: idtipoalerta='0',
        # trae todo los tipos de alerta
        idtipoalerta = request_json["idtipoalerta"]
        # variable creada para traer data de un dia en
        # especifico, valor 0 trae data del mismo
        # valor 1 trae data de ayer, valor 2 data de antes
        # de ayer y así  sucesivamente
        # nrodia_AleTel: Para Alertas telemetria
        # nrodia_RegIbut: Para RegistrosIbuton
        nrodia = request_json["nrodia"]
        # Tipo de tabla: 1:UltimaPosicion
        # 2:AlertasTelemetria
        # 3:RegistrosIbutton
        tipotabla = request_json["tipotabla"]
        ##################################################
        ##################################################
        # Variables de entorno:
        token = "Bearer " + str(os.getenv("token"))
        env = str(os.getenv("env"))
        # volores
        # d:desarrollo
        # q:calidad
        # p:produccion
        ##################################################
        # Variables de fecha,LIMA-PERU
        # fecha referencial para ultima posicion
        fecha_actual_ult_pos = datetime.datetime.now(pytz.timezone("America/Lima"))
        anio_ult_pos = fecha_actual_ult_pos.year
        mes_ult_pos = fecha_actual_ult_pos.month
        dia_ult_pos = fecha_actual_ult_pos.day
        hora_ult_pos = fecha_actual_ult_pos.hour
        minuto_ult_pos = fecha_actual_ult_pos.minute
        # fecha referencial para alertas telemetria
        fecha_actual_ale_tel = datetime.datetime.now(pytz.timezone("America/Lima"))
        fecha_ref_ale_tel = fecha_actual_ale_tel - datetime.timedelta(int(nrodia))
        anio_ale_tel = fecha_ref_ale_tel.year
        mes_ale_tel = fecha_ref_ale_tel.month
        dia_ale_tel = fecha_ref_ale_tel.day
        fecha_inicio_ale_tele = (
            str(anio_ale_tel)
            + str(_AgregarCeroMenor9(mes_ale_tel))
            + str(_AgregarCeroMenor9(dia_ale_tel))
        )
        fecha_fin_ale_tele = (
            str(anio_ale_tel)
            + str(_AgregarCeroMenor9(mes_ale_tel))
            + str(_AgregarCeroMenor9(dia_ale_tel))
        )
        # fecha referencial para Registros Ibutton
        fecha_actual_reg_ibu = datetime.datetime.now(pytz.timezone("America/Lima"))
        fecha_ref_reg_ibu = fecha_actual_reg_ibu - datetime.timedelta(int(nrodia))
        anio_reg_ibu = fecha_ref_reg_ibu.year
        mes_reg_ibu = fecha_ref_reg_ibu.month
        dia_reg_ibu = fecha_ref_reg_ibu.day
        fecha_inicio_reg_ibu = (
            str(anio_reg_ibu)
            + str(_AgregarCeroMenor9(mes_reg_ibu))
            + str(_AgregarCeroMenor9(dia_reg_ibu))
        )
        fecha_fin_reg_ibu = (
            str(anio_reg_ibu)
            + str(_AgregarCeroMenor9(mes_reg_ibu))
            + str(_AgregarCeroMenor9(dia_reg_ibu))
        )

        ##############################################
        # valores: env=d,q,p
        bucket_name_raiz = "pecst_pacasmayo_raw_01_" + str(env)
        ###############
        # PROCESO DE CARGA JSON PARA ULTIMA POSICION:
        try:

            if tipotabla == "1":
                url_ult_pos = (
                    "http://ei.wisetrack.cl/Peru/PacasMayo/UltimaPosicion?Usuario=TMPacasMayo&Patente="
                    + str(patente)
                )
                response_ult_pos = requests.get(
                    url_ult_pos, headers={"Authorization": token}
                )
                dataJsonUltimaPosicion = response_ult_pos.json()
                contents_ult_pos = json.dumps(
                    dataJsonUltimaPosicion, ensure_ascii=False
                )
                # Nomenclaturas de carpetas:
                # NOMENCLATURA DE GUARDADO DE LA DATA
                # WISETRACK/wisetrack/ultima-posicion/2022/09/07/19/wisetrack_ultima_posicion_20220907_1947.json
                ruta_raiz_ult_pos = "WISETRACK/wisetrack/ultima-posicion/"
                # nomenclatura de fechas:
                ruta_anio_ult_pos = str(_AgregarCeroMenor9(anio_ult_pos)) + "/"
                ruta_mes_ult_pos = str(_AgregarCeroMenor9(mes_ult_pos)) + "/"
                ruta_dia_ult_pos = str(_AgregarCeroMenor9(dia_ult_pos)) + "/"
                ruta_hora_ult_pos = str(_AgregarCeroMenor9(hora_ult_pos)) + "/data/"
                # nomenclatura archivo
                nombre_archivo_ult_pos = (
                    "wisetrack_ultima_posicion_"
                    + str(_AgregarCeroMenor9(anio_ult_pos))
                    + str(_AgregarCeroMenor9(mes_ult_pos))
                    + str(_AgregarCeroMenor9(dia_ult_pos))
                    + ""
                    + str(_AgregarCeroMenor9(hora_ult_pos))
                    + str(_AgregarCeroMenor9(minuto_ult_pos))
                    + ".json"
                )
                bucket_gs_ult_pos = (
                    ruta_raiz_ult_pos
                    + ruta_anio_ult_pos
                    + ruta_mes_ult_pos
                    + ruta_dia_ult_pos
                    + ruta_hora_ult_pos
                    + nombre_archivo_ult_pos
                )
                # valores: env=d,q,p

                # llevando data al cloud storage
                destination_blob_name_ult_pos = bucket_gs_ult_pos
                storage_client_ult_pos = storage.Client()
                bucket_ult_pos = storage_client_ult_pos.bucket(bucket_name_raiz)
                blob_ult_pos = bucket_ult_pos.blob(
                    destination_blob_name_ult_pos, chunk_size=None
                )
                blob_ult_pos.upload_from_string(
                    contents_ult_pos, content_type="text/json"
                )

                LOG = "TODO CONFORME ULTIMA POSICION"
        except ValueError:
            print(ValueError)
            LOG = "ERROR ULTIMA POSICION "
        #################################################
        # PROCESO DE CARGA DE ALERTAS TELEMETRIA
        #################################################
        # Saldra todos los dias a las 5:00 am y se cargara data del dia anterior
        try:

            if tipotabla == "2":
                # url telemetria
                url_ale_tel = (
                    "http://ei.wisetrack.cl/Peru/PacasMayo/AlertasTelemetria?Usuario=TMPacasMayo&FechaInicio="
                    + fecha_inicio_ale_tele
                    + "&FechaFin="
                    + fecha_fin_ale_tele
                    + "&Patente="
                    + str(patente)
                    + "&IDTipoAlerta="
                    + str(idtipoalerta)
                )
                # obteniendo el json
                response_ale_tel = requests.get(
                    url_ale_tel, headers={"Authorization": token}
                )
                dataJsonAlertasTelemetria = response_ale_tel.json()
                # NOMENCLATURA DE GUARDADO DE LA DATA
                # WISETRACK/wisetrack/alertas-telemetria/2022/09/wisetrack_alertas_telemetria_20220905.json
                ruta_raiz_ale_tel = "WISETRACK/wisetrack/alertas-telemetria/"
                # nomenclatura fecha
                ruta_anio_ale_tel = str(_AgregarCeroMenor9(anio_ale_tel)) + "/"
                ruta_mes_ale_tel = str(_AgregarCeroMenor9(mes_ale_tel)) + "/data/"
                # nomenclatura archivo
                nombre_archivo_ale_tel = (
                    "wisetrack_alertas_telemetria_"
                    + str(_AgregarCeroMenor9(anio_ale_tel))
                    + str(_AgregarCeroMenor9(mes_ale_tel))
                    + str(_AgregarCeroMenor9(dia_ale_tel))
                    + ".json"
                )
                bucket_gs_ale_tel = (
                    ruta_raiz_ale_tel
                    + ruta_anio_ale_tel
                    + ruta_mes_ale_tel
                    + nombre_archivo_ale_tel
                )

                # llevando data al cloud storage
                contents_ale_tel = json.dumps(
                    dataJsonAlertasTelemetria, ensure_ascii=False
                )
                destination_blob_name_ale_tel = bucket_gs_ale_tel
                storage_client_ale_tel = storage.Client()
                bucket_ale_tel = storage_client_ale_tel.bucket(bucket_name_raiz)
                blob_ale_tel = bucket_ale_tel.blob(
                    destination_blob_name_ale_tel, chunk_size=None
                )
                blob_ale_tel.upload_from_string(
                    contents_ale_tel, content_type="text/json"
                )
                LOG = "TODO CONFORME ALERTA TELEMETRIA"

        except ValueError:
            print(ValueError)
            LOG = "ERROR ALERTA TELEMETRIA"
        ############################################
        # PROCEO DE CARGA PARA Registros Ibutton
        ###################################################
        # Saldra todos los dias a las 5:15 am y se cargara data del dia anterior
        try:

            if tipotabla == "3":
                # url
                url_reg_ibu = (
                    "http://ei.wisetrack.cl/Peru/PacasMayo/RegistrosIbutton?Usuario=TMPacasMayo&FechaInicio="
                    + fecha_inicio_reg_ibu
                    + "&FechaFin="
                    + fecha_fin_reg_ibu
                    + "&Patente="
                    + str(patente)
                )
                # obteniendo el json
                response_reg_ibu = requests.get(
                    url_reg_ibu, headers={"Authorization": token}
                )
                dataJsonRegistrosButton = response_reg_ibu.json()
                # NOMENCLATURA DE GUARDADO DE LA DATA
                # WISETRACK/wisetrack/registros-ibutton/2022/09/wisetrack_registros_ibutton_20220905.json
                ruta_raiz_reg_ibu = "WISETRACK/wisetrack/registros-ibutton/"
                # nomenclatura fecha
                ruta_anio_reg_ibu = str(_AgregarCeroMenor9(anio_reg_ibu)) + "/"
                ruta_mes_reg_ibu = str(_AgregarCeroMenor9(mes_reg_ibu)) + "/data/"
                # nomenclatura archivo
                nombre_archivo_reg_ibu = (
                    "wisetrack_registros_ibutton_"
                    + str(_AgregarCeroMenor9(anio_reg_ibu))
                    + str(_AgregarCeroMenor9(mes_reg_ibu))
                    + str(_AgregarCeroMenor9(dia_reg_ibu))
                    + ".json"
                )
                bucket_gs_reg_ibu = (
                    ruta_raiz_reg_ibu
                    + ruta_anio_reg_ibu
                    + ruta_mes_reg_ibu
                    + nombre_archivo_reg_ibu
                )

                # llevando data al cloud storage
                contents_reg_ibu = json.dumps(
                    dataJsonRegistrosButton, ensure_ascii=False
                )
                destination_blob_name_reg_ibu = bucket_gs_reg_ibu
                storage_client_reg_ibu = storage.Client()
                bucket_reg_ibu = storage_client_reg_ibu.bucket(bucket_name_raiz)
                blob_reg_ibu = bucket_reg_ibu.blob(
                    destination_blob_name_reg_ibu, chunk_size=None
                )
                blob_reg_ibu.upload_from_string(
                    contents_reg_ibu, content_type="text/json"
                )
                LOG = "TODO CONFORME REGISTROS IBUTTON"

        except ValueError:
            print(ValueError)
            LOG = "ERROR REGISTROS IBUTTON"

    except ValueError:
        print(ValueError)

    return "PROCESO TERMINADO: " + str(LOG)
