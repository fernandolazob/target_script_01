import sys 
sys.path.append('C:/Users/DATA/Documents/datos/01_script/inicio/funciones')
from funciones import *
from funciones_spark import *
from variables_inicio import *
from utils_sql import *

spark = SparkSession.builder \
    .appName("SparkExample") \
    .master("local[*]") \
    .config('spark.driver.extraClassPath', 'C:/spark/jars/mssql-jdbc-13.2.1.jre11.jar') \
    .config('spark.executor.extraClassPath', 'C:/spark/jars/mssql-jdbc-13.2.1.jre11.jar') \
    .config('spark.executor.memory', '8g') \
    .config('spark.driver.memory', '8g') \
    .getOrCreate()

def list02_prestamo_cenco(spark):
    fecha_mes_base='2026-05-01'
    tipi_cond1='SAE'
    tipi_cond2='SAR'
    tipi_cond3='AGENDA'
    tb_tipolofia='tTipologia_Cencosud_PPFF'
    servidor_01=76
    tipi_cod='Codigo'
    tipi_resp_cod='R'
    tipi_descrip='DESCRIPCION'
    tipi_estado='tipo'
    tipi_resp_estado='NO GESTIONADO'
    tipi_subdescripcion='SUB_DESCRIPCION'
    tnum_tb='tNumeroCenco_Sae'
    tnum_dni='CODDOC'
    tlista_generada='borrar_prestamo_cencosud'
    get_base=since_base_maestra_cencosud_ppff

    lista_generada(spark,fecha_mes_base,tipi_cond1,tipi_cond2,tipi_cond3,tb_tipolofia,servidor_01,tipi_cod,tipi_resp_cod,tipi_descrip,tipi_estado,tipi_resp_estado,tipi_subdescripcion,tnum_tb,tnum_dni,tlista_generada,get_base)

def list02_tc_cenco(spark):
    fecha_mes_base='2026-05-01'
    tipi_cond1='CENCOSUD_TC'
    tipi_cond2='xx'
    tipi_cond3='xx'
    tb_tipolofia='tTipologia_Cencosud_TC'
    servidor_01=76
    tipi_cod='Codigo'
    tipi_resp_cod='R'
    tipi_descrip='DESCRIPCION'
    tipi_estado='tipo'
    tipi_resp_estado='NO GESTIONADO'
    tipi_subdescripcion='SUB_DESCRIPCION'
    tnum_tb='tNumeroCenco_Tc'
    tnum_dni='CODDOC'
    tlista_generada='borrar_tc_cencosud'
    get_base=since_base_maestra_cencosud_tc

    lista_generada(spark,fecha_mes_base,tipi_cond1,tipi_cond2,tipi_cond3,tb_tipolofia,servidor_01,tipi_cod,tipi_resp_cod,tipi_descrip,tipi_estado,tipi_resp_estado,tipi_subdescripcion,tnum_tb,tnum_dni,tlista_generada,get_base)

def list02_consumo_efe(spark):
    fecha_mes_base='2026-05-01'
    tipi_cond1='EFECTIVA_NC'
    tipi_cond2='xx'
    tipi_cond3='xx'
    tb_tipolofia='tTipologia_Efectiva'
    servidor_01=7
    tipi_cod='Codigo'
    tipi_resp_cod='B'
    tipi_descrip='DESCRIPCION'
    tipi_estado='tipo'
    tipi_resp_estado='NO CONTACTO'
    tipi_subdescripcion='SUB_DESCRIPCION'
    tnum_tb='tNumeroEfectiva'
    tnum_dni='DNI'
    tlista_generada='borrar_consumo_efe'
    get_base=since_base_maestra_efe_consumo
    lista_generada(spark,fecha_mes_base,tipi_cond1,tipi_cond2,tipi_cond3,tb_tipolofia,servidor_01,tipi_cod,tipi_resp_cod,tipi_descrip,tipi_estado,tipi_resp_estado,tipi_subdescripcion,tnum_tb,tnum_dni,tlista_generada,get_base)

def list02_negocios_efe(spark):
    fecha_mes_base='2026-05-01'
    tipi_cond1='EFECTIVA_NEGOCIOS'
    tipi_cond2='xx'
    tipi_cond3='xx'
    tb_tipolofia='tTipologia_Efectiva'
    servidor_01=64
    tipi_cod='Codigo'
    tipi_resp_cod='B'
    tipi_descrip='DESCRIPCION'
    tipi_estado='tipo'
    tipi_resp_estado='NO CONTACTO'
    tipi_subdescripcion='SUB_DESCRIPCION'
    tnum_tb='tNumeroEfectivaNegocios'
    tnum_dni='NUMDOCUMENTO'
    tlista_generada='borrar_negocio_efe'
    get_base=since_base_maestra_efe_negocio

    lista_generada(spark,fecha_mes_base,tipi_cond1,tipi_cond2,tipi_cond3,tb_tipolofia,servidor_01,tipi_cod,tipi_resp_cod,tipi_descrip,tipi_estado,tipi_resp_estado,tipi_subdescripcion,tnum_tb,tnum_dni,tlista_generada,get_base)

def list02_tc_dinners(spark):
    fecha_mes_base='2026-05-01'
    tipi_cond1='DINERS TC'
    tipi_cond2='xx'
    tipi_cond3='xx'
    tb_tipolofia='tTipologia_Diners_TC'
    servidor_01=21
    tipi_cod='cod'
    tipi_resp_cod='H'
    tipi_descrip='[NIVEL 4]'
    tipi_estado='[NIVEL 2]'
    tipi_resp_estado='NO CONTACTO'
    tipi_subdescripcion='[NIVEL 3]'
    tnum_tb='tNumeroDinersTc'
    tnum_dni='NUMERO_DOCUMENTO'
    tlista_generada='borrar_tc_dinner'
    get_base=since_base_maestra_tc_dinners
    lista_generada(spark,fecha_mes_base,tipi_cond1,tipi_cond2,tipi_cond3,tb_tipolofia,servidor_01,tipi_cod,tipi_resp_cod,tipi_descrip,tipi_estado,tipi_resp_estado,tipi_subdescripcion,tnum_tb,tnum_dni,tlista_generada,get_base)

def list02_prestamo_dinners(spark):
    fecha_mes_base='2026-05-01'
    # tipi_cond1='diners'
    tipi_cond1='plus'
    tipi_cond2='xx'
    tipi_cond3='xx'
    tb_tipolofia='tTipologia_Diners_PPD'
    servidor_01=21
    tipi_cod='cod'
    tipi_resp_cod='P'
    tipi_descrip='[NIVEL 4]'
    tipi_estado='[NIVEL 2]'
    tipi_resp_estado='NO CONTACTO'
    tipi_subdescripcion='[NIVEL 3]'
    tnum_tb='tNumeroDiners'
    tnum_dni='NumDoc'
    tlista_generada='borrar_prestamo_dinner'
    get_base=since_base_maestra_pp_dinners
    lista_generada(spark,fecha_mes_base,tipi_cond1,tipi_cond2,tipi_cond3,tb_tipolofia,servidor_01,tipi_cod,tipi_resp_cod,tipi_descrip,tipi_estado,tipi_resp_estado,tipi_subdescripcion,tnum_tb,tnum_dni,tlista_generada,get_base)

def list03_alfin(spark):
    ls_una_vez=[]
    ls_casilla=[]
    ls_ocupado=[]
    tb_gestiones='alfin_gestion'
    tb_cliente='alfin_clientes'
    tb_tipolofia='ALFin_tipificaciones'
    fecha_mes_base='2026-05-01'
    get_base=since_base_maestra_alfin
    tlista_generada='borrar_alfin_01'
    name_campana='alfin'
    app_campana=12
    lista_generada_valentina_actual(spark,fecha_mes_base,ls_una_vez,ls_casilla,ls_ocupado,tlista_generada,tb_tipolofia,tb_gestiones,tb_cliente,get_base,name_campana,app_campana)   

def list03_credicash(spark):
   
   
    ls_una_vez=[]
    ls_casilla=[]
    ls_ocupado=[]
    tb_gestiones='alfcc_gestion'
    tb_cliente='alfcc_clientes'
    tb_tipolofia='ALFcc_tipificaciones'
    fecha_mes_base='2026-05-01'
    get_base=since_base_maestra_alfcc
    tlista_generada='borrar_credicash_01'
    name_campana='alfcc'
    app_campana=17
    lista_generada_valentina_actual(spark,fecha_mes_base,ls_una_vez,ls_casilla,ls_ocupado,tlista_generada,tb_tipolofia,tb_gestiones,tb_cliente,get_base,name_campana,app_campana)   

    # lista_generada_valentina(spark,fecha_mes_base,ls_una_vez,ls_casilla,ls_ocupado,tlista_generada,tb_tipolofia,tb_gestiones,tb_cliente,get_base,tb_tnumero)

# fecha_mes_base='2026-05-01'
# update_tnumeros_valentina(spark,'alfcc_clientes',fecha_mes_base,'tNumero_alfcc_borrar')
# update_tnumeros_valentina(spark,'alfin_clientes',fecha_mes_base,'tNumero_alfin_borrar')

# tb_name_tnum='tNumero_alfcc_borrar'
# tb_name_tnum_resultante='tNumero_alfcc_borrar_01'
# pool_tnumeros_valentina(spark,'dni_cliente',tb_name_tnum,tb_name_tnum_resultante)

# tb_name_tnum='tNumero_alfin_borrar'
# tb_name_tnum_resultante='tNumero_alfin_borrar_01'
# pool_tnumeros_valentina(spark,'dni_cliente',tb_name_tnum,tb_name_tnum_resultante)

# list02_consumo_efe(spark)

# list02_prestamo_cenco(spark)

# list02_negocios_efe(spark)
# list02_tc_cenco(spark)

# list02_prestamo_dinners(spark)
# list02_tc_dinners(spark) 
# list03_alfin(spark) 
list03_credicash(spark)

# biya
# telemarketing@vensud.com
# T@RG3T.2025!$pp_dinner_202604151615q


# tipis 
# ventas

# se procesa el histroico generl 
# la mejor gestion


# tmp_llamadas_dinners_5 la mejor
# tmp_llamada_dinners_ total gestion