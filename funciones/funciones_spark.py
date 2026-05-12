import sys 
sys.path.append('C:/Users/DATA/Documents/datos/01_script/inicio/funciones')
from funciones import *
from variables_inicio import *


from dateutil.relativedelta import relativedelta

from pyspark.sql.window import Window

from pyspark.sql.functions import create_map
from itertools import chain

from pyspark.sql.functions import to_date, hour, col,count,when,dayofmonth,row_number

from pyspark.sql.functions import dense_rank
from pyspark.sql.functions import greatest


def cargar_archivo_csv(spark,filename,sep,bol_header):
    filePath = os.path.join(ruta_csv, filename)
    return spark.read.csv(filePath, sep=sep, header=bol_header)

def obtener_tabla_sql(spark,query,server,username,pwd,db):
    #connTable='TB_outboundMovil'
    connTable=f"({query}) AS tmp"
    jdbc_url = f"jdbc:sqlserver://{server}:{port};database={db}"
    connProperties={
        "user": f"{username}", 
        "password": f"{pwd}", 
        "trustServerCertificate": "true",
    }
    return spark.read.jdbc(url=jdbc_url, table=connTable, properties=connProperties) 

def obtener_tabla_mysql(spark, query, server, username, pwd, db):

    connTable = f"({query}) AS tmp"

    jdbc_url = f"jdbc:mysql://{server}:{port_mysql}/{db}"

    connProperties = {
        "user": username,
        "password": pwd,
        "driver": "com.mysql.cj.jdbc.Driver"
    }

    return spark.read.jdbc(url=jdbc_url,table=connTable,properties=connProperties)

def overwrite_table_SQL(spark,df,new_table,server,username,pwd,db):
    jdbc_url = f"jdbc:sqlserver://{server}:{port};database={db}"
    connProperties={
        "user": f"{username}", 
        "password": f"{pwd}", 
        "trustServerCertificate": "true",
        "truncate": "true",
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    }
    try:
        df.write.jdbc(url=jdbc_url, table=new_table, mode='overwrite', properties=connProperties)
    except Exception as e:  
        print("Error al insertar JDBC:", e)   

def append_table_SQL(spark,df,new_table,server,username,pwd,db):
    jdbc_url = f"jdbc:sqlserver://{server}:{port};database={db}"
    connProperties={
        "user": f"{username}", 
        "password": f"{pwd}", 
        "trustServerCertificate": "true",
        "truncate": "true",
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    }
    try:
        df.write.jdbc(url=jdbc_url, table=new_table, mode='append', properties=connProperties)
    except Exception as e:  
        print("Error al insertar JDBC:", e)   

def append_table_mysql(spark, df, new_table, server, username, pwd, db):

    jdbc_url = f"jdbc:mysql://{server}:{port_mysql}/{db}?useSSL=false&allowPublicKeyRetrieval=true"

    connProperties = {
        "user": username,
        "password": pwd,
        "driver": "com.mysql.cj.jdbc.Driver"
    }

    try:
        df.write.jdbc(
            url=jdbc_url,
            table=new_table,
            mode='append',
            properties=connProperties
        )
        print("Insert OK en MySQL")
    except Exception as e:
        print("Error al insertar JDBC MySQL:", e)


def since_valentina(spark,fecha_mes_base,tb_tipolofia,tb_gestiones):

    query = f"""
        select 
        dni_cliente,
        fecha as fecha_llamada,Celular as contacto,
        Tipificacion as codigo,
        servicio,id_supervisor,Intentos as attempt,tmo as duracion,t_espera,
        dni_ejecutivo
        from VALENTINA.dbo.{tb_gestiones}
        WHERE CAST(fecha AS DATE) >= CAST('{fecha_mes_base}' AS DATE)
        AND CAST(fecha AS DATE) < DATEADD(MONTH, 1, CAST('{fecha_mes_base}' AS DATE))
        """
    df_vicidial=obtener_tabla_sql(spark,query,server_sa,user_sa,pwd_sa,db_sa)
        
    query = f"""
        SELECT 
        nombre as descripcion,
        id_banco as codigo,
        id as peso
        FROM VALENTINA.dbo.{tb_tipolofia}
        where estado='a'
        """
    df_tipi=obtener_tabla_sql(spark,query,server_sa,user_sa,pwd_sa,db_sa)

    df_vicidial=df_vicidial.join(df_tipi,["codigo"],"left")

    window_spec = Window.partitionBy("dni_cliente").orderBy(col("peso").asc_nulls_last())
    df_vicidial = df_vicidial.withColumn("n_mejor_resul", row_number().over(window_spec))

    window_part = Window.partitionBy("dni_cliente")
    df_vicidial = df_vicidial.withColumn(
        "mejor_codigo_cli",
        F.max(
            F.when(F.col("n_mejor_resul") == 1, F.col("codigo"))
        ).over(window_part)
    )

    window_part = Window.partitionBy("dni_cliente","contacto","codigo")

    df_vicidial = df_vicidial.withColumn(
        "cod_attempt",
        F.count("codigo").over(window_part)
    )

    window_spec = Window.partitionBy("fecha_llamada","dni_cliente").orderBy(col("peso").asc_nulls_last())
    df_vicidial = df_vicidial.withColumn("n_mejor_resul_dia", row_number().over(window_spec))

    window_part = Window.partitionBy("dni_cliente")
    df_vicidial = df_vicidial.withColumn(
        "mejor_codigo_cli_dia",
        F.max(
            F.when(F.col("n_mejor_resul_dia") == 1, F.col("codigo"))
        ).over(window_part)
    )

    df_vicidial = df_vicidial.withColumn(
        "q_intentos_telef",
        count("*").over(window_part)
    )

    window_spec = Window.partitionBy("dni_cliente",'contacto').orderBy(col("fecha_llamada").desc())
    df_vicidial = df_vicidial.withColumn("n_ult_resul", row_number().over(window_spec))

    window_part = Window.partitionBy("dni_cliente",'contacto')
    df_vicidial = df_vicidial.withColumn(
        "ult_codigo_result",
        F.max(
            F.when(F.col("n_ult_resul") == 1, F.col("codigo"))
        ).over(window_part)
    )
    df_vicidial = df_vicidial.withColumn(
        "fecha_llamada_1",
        F.max(
            F.when(F.col("n_ult_resul") == 1, F.col("fecha_llamada"))
        ).over(window_part)
    )    

    return df_vicidial.select('dni_cliente', 'duracion', 'fecha_llamada', 'mejor_codigo_cli','peso','contacto','codigo','n_mejor_resul','mejor_codigo_cli_dia','cod_attempt','dni_ejecutivo','q_intentos_telef','descripcion','ult_codigo_result','fecha_llamada_1')




def since_valentina_actual(spark,fecha_mes_base,tb_tipolofia,tb_gestiones,name_campana,app_campana):

    vicidial_hoy_valentina(name_campana,fecha_mes_base,app_campana,user_valentina,pwd_valentina,server_valentina,port_mysql,db_valentina)
    vicidial_hoy=cargar_archivo_csv(spark,'tmp_vici.csv',';',True)

    cl_base1 = fecha_a_nombre(fecha_mes_base)
    query = f"""
        select distinct cl_id as cid,NUMERO_DOCUMENTO as dni_cliente from valentina.dbo.{name_campana}_clientes
        where cl_base='{cl_base1}'
        """
    df_cl_id=obtener_tabla_sql(spark,query,server_sa,user_sa,pwd_sa,db_sa)

    vicidial_hoy=vicidial_hoy.join(df_cl_id,['cid'],'inner')
    vicidial_hoy=vicidial_hoy.select( 'contacto', 'fecha_llamada', 'duracion', 'codigo', 'dni_ejecutivo', 'dni_cliente')
    vicidial_hoy = vicidial_hoy.withColumn(
        "dni_cliente",
        F.right(
            F.concat(F.lit("00000000"), F.col("dni_cliente")),
            F.lit(8)
        )
    )
   
    window_part = Window.partitionBy("dni_cliente","contacto","codigo")

    query = f"""
        select 
        dni_cliente,
        fecha as fecha_llamada,Celular as contacto,
        Tipificacion as codigo,
        tmo as duracion,
        dni_ejecutivo
        from VALENTINA.dbo.{tb_gestiones}
        WHERE CAST(fecha AS DATE) >= CAST('{fecha_mes_base}' AS DATE)
        AND CAST(fecha AS DATE) < DATEADD(MONTH, 1, CAST('{fecha_mes_base}' AS DATE))
        """
    df_vicidial=obtener_tabla_sql(spark,query,server_sa,user_sa,pwd_sa,db_sa)
        
    df_vicidial=df_vicidial.unionByName(vicidial_hoy)

    query = f"""
        SELECT 
        nombre as descripcion,
        id_banco as codigo,
        id as peso
        FROM VALENTINA.dbo.{tb_tipolofia}
        where estado='a'
        """
    df_tipi=obtener_tabla_sql(spark,query,server_sa,user_sa,pwd_sa,db_sa)

    df_vicidial=df_vicidial.join(df_tipi,["codigo"],"left")

    window_spec = Window.partitionBy("dni_cliente").orderBy(col("peso").asc_nulls_last())
    df_vicidial = df_vicidial.withColumn("n_mejor_resul", row_number().over(window_spec))

    window_part = Window.partitionBy("dni_cliente")
    df_vicidial = df_vicidial.withColumn(
        "mejor_codigo_cli",
        F.max(
            F.when(F.col("n_mejor_resul") == 1, F.col("codigo"))
        ).over(window_part)
    )

    window_part = Window.partitionBy("dni_cliente","contacto","codigo")

    df_vicidial = df_vicidial.withColumn(
        "cod_attempt",
        F.count("codigo").over(window_part)
    )

    window_spec = Window.partitionBy("fecha_llamada","dni_cliente").orderBy(col("peso").asc_nulls_last())
    df_vicidial = df_vicidial.withColumn("n_mejor_resul_dia", row_number().over(window_spec))

    window_part = Window.partitionBy("dni_cliente")
    df_vicidial = df_vicidial.withColumn(
        "mejor_codigo_cli_dia",
        F.max(
            F.when(F.col("n_mejor_resul_dia") == 1, F.col("codigo"))
        ).over(window_part)
    )

    df_vicidial = df_vicidial.withColumn(
        "q_intentos_telef",
        count("*").over(window_part)
    )

    window_spec = Window.partitionBy("dni_cliente",'contacto').orderBy(col("fecha_llamada").desc())
    df_vicidial = df_vicidial.withColumn("n_ult_resul", row_number().over(window_spec))

    window_part = Window.partitionBy("dni_cliente",'contacto')
    df_vicidial = df_vicidial.withColumn(
        "ult_codigo_result",
        F.max(
            F.when(F.col("n_ult_resul") == 1, F.col("codigo"))
        ).over(window_part)
    )
    df_vicidial = df_vicidial.withColumn(
        "fecha_llamada_1",
        F.max(
            F.when(F.col("n_ult_resul") == 1, F.col("fecha_llamada"))
        ).over(window_part)
    )    

    return df_vicidial.select('dni_cliente', 'duracion', 'fecha_llamada', 'mejor_codigo_cli','peso','contacto','codigo','n_mejor_resul','mejor_codigo_cli_dia','cod_attempt','dni_ejecutivo','q_intentos_telef','descripcion','ult_codigo_result','fecha_llamada_1')


def since_vicidial(spark,fecha_mes_base,tipi_cond1,tipi_cond2,tipi_cond3,tb_tipolofia,servidor_01,tipi_cod,tipi_resp_cod,tipi_descrip,tipi_estado,tipi_resp_estado,tnum_tb,tnum_dni):
            # a.user AS dni_ejecutivo,        
            # c.full_name AS ejecutivo,        
    query = f"""
        SELECT *
        FROM OPENQUERY([192.168.3.{servidor_01}], '
            SELECT        
            rtrim(ltrim(d.vendor_lead_code)) AS vendor_lead_code,        
            e.dial_method,
            a.campaign_id AS numero_campana,        
            a.user AS dni_ejecutivo,
            c.full_name AS ejecutivo,
            e.campaign_name AS nombre_campana,        
            a.call_date AS fecha_hora_llamada,        
            a.length_in_sec AS duracion,        
            b.status_name AS call_result,        
            f.list_description,        
            f.list_name,        
            a.phone_number as phone_number,        
            d.alt_phone as fecha_agenda,        
            d.comments as comentarios,        
            a.status AS codigo
            FROM asterisk.vicidial_log a         
            LEFT JOIN asterisk.vicidial_list d ON a.lead_id=d.lead_id        
            LEFT JOIN asterisk.vicidial_campaigns e ON a.campaign_id=e.campaign_id        
            LEFT JOIN asterisk.vicidial_lists f ON a.list_id=f.list_id        
            LEFT JOIN asterisk.vicidial_statuses b ON a.status=b.status        
            LEFT JOIN asterisk.vicidial_users c ON a.user=c.user        
            WHERE (e.campaign_name like "%{tipi_cond1}" or e.campaign_name like "%{tipi_cond2}" or e.campaign_name like "%{tipi_cond3}")
            AND a.call_date >= DATE_FORMAT(''{fecha_mes_base}'', ''%Y-%m-01'')
            AND a.call_date < 
            DATE_ADD(DATE_FORMAT(''{fecha_mes_base}'', ''%Y-%m-01''), INTERVAL 1 MONTH)
        ')

        """
    df_vicidial=obtener_tabla_sql(spark,query,server_zeus,user_zeus,pwd_zeus,db_zeus)

    df_vicidial=df_vicidial.withColumn(
            "vendor_lead_code",
            F.right(
                F.concat(F.lit("00000000"), F.col("vendor_lead_code")),
                F.lit(8)
            )
        )

    query = f"""
        select 
        distinct
        Telefonos as phone_number,{tnum_dni} as vendor_lead_code_1
        from DANTALION.dbo.{tnum_tb}
        """
    df_tnumer=obtener_tabla_sql(spark,query,server_kishin,user_kishin,pwd_kishin,db_kishin)
    
    df_tnumer = df_tnumer.withColumn(
            "vendor_lead_code_1",
            F.right(
                F.concat(F.lit("00000000"), F.col("vendor_lead_code_1")),
                F.lit(8)
            )
        )

    df_vicidial=df_vicidial.join(df_tnumer,['phone_number'],'left')

    df_vicidial = df_vicidial.withColumn(
        "vendor_lead_code",
        F.coalesce(F.col("vendor_lead_code"), F.col("vendor_lead_code_1"))
    ).drop("vendor_lead_code_1")
    df_vicidial=df_vicidial.filter(F.col('vendor_lead_code').isNotNull())

    df_vicidial = (
        df_vicidial
        .withColumn("fecha_llamada", F.to_date(F.col("fecha_hora_llamada")))
        .withColumn("tramo", F.hour(F.col("fecha_hora_llamada")))
        .withColumn("hora_a", F.date_format(F.col("fecha_hora_llamada"), "HH:mm:ss"))
    )

    query = f"""
        SELECT {tipi_cod} as codigo
        , case
            when {tipi_cod}='CALLBK' then 'VOLVER A LLAMAR - call'
            else {tipi_descrip} 
        end as descripcion
        ,case 
            when {tipi_cod}='CALLBK' then 1200
            else peso 
        end as peso  FROM [ODIN].[dbo].{tb_tipolofia}
        where LEFT({tipi_cod},1)='{tipi_resp_cod}' or {tipi_estado}='{tipi_resp_estado}' or {tipi_cod}='CALLBK'
        """
    df_tipi=obtener_tabla_sql(spark,query,server_zeus,user_zeus,pwd_zeus,db_zeus)

    df_vicidial=df_vicidial.join(df_tipi,["codigo"],"left")

    df_vicidial = df_vicidial.withColumn(
        'peso',
        F.when(F.col('codigo') == 'INCALL', 100000)
        .when((F.col('codigo') == 'DCMX') & (F.col('duracion') > 15), 100001)
        .when(F.col('codigo') == 'DCMX', 100002)
        .otherwise(F.col('peso'))
    )

    window_spec = (Window.partitionBy("vendor_lead_code")
                    .orderBy(col("peso").asc_nulls_last()
                    ))
    df_vicidial = df_vicidial.withColumn("n_mejor_resul_cli", row_number().over(window_spec))

    window_spec = (Window.partitionBy("vendor_lead_code",'phone_number')
                    .orderBy(
                        col("peso").asc_nulls_last(),
                        col("hora_a").desc_nulls_last()
                    ))

    df_vicidial = df_vicidial.withColumn(
        "n_mejor_resul_telf",
        F.when(
            F.col("duracion") > 15,
            F.row_number().over(window_spec)
        )
    )

    window_spec = (Window.partitionBy("vendor_lead_code")
                    .orderBy(
                        col("fecha_hora_llamada").desc_nulls_last()
                    ))
    df_vicidial = df_vicidial.withColumn("n_ult_resul", row_number().over(window_spec))

    window_part = Window.partitionBy("vendor_lead_code")
    df_vicidial = df_vicidial.withColumn(
        "mejor_codigo_cli",
        F.max(
            F.when(F.col("n_mejor_resul_cli") == 1, F.col("codigo"))
        ).over(window_part)
    )

    df_vicidial = df_vicidial.withColumn(
        "ult_codigo_cli",
        F.max(
            F.when(F.col("n_ult_resul") == 1, F.col("codigo"))
        ).over(window_part)
    )

    df_vicidial = df_vicidial.withColumn(
        "ult_call_result",
        F.max(
            F.when(F.col("n_ult_resul") == 1, F.col("call_result"))
        ).over(window_part)
    )

    df_vicidial = df_vicidial.withColumn(
        "fecha_llamada_1",
        F.max(
            F.when(F.col("n_ult_resul") == 1, F.col("fecha_llamada"))
        ).over(window_part)
    )

    df_vicidial = df_vicidial.withColumn(
        "fecha_hora_llamada_1",
        F.max(
            F.when(F.col("n_ult_resul") == 1, F.col("fecha_hora_llamada"))
        ).over(window_part)
    )  

    df_vicidial = df_vicidial.withColumn("q_intentos",count("*").over(window_part))

    window_part = Window.partitionBy("vendor_lead_code",'phone_number')
    df_vicidial = df_vicidial.withColumn(
        "mejor_codigo_telf",
        F.max(
            F.when(F.col("n_mejor_resul_telf") == 1, F.col("codigo"))
        ).over(window_part)
    )
    
    df_vicidial = df_vicidial.withColumn("q_intentos_telf",count("*").over(window_part))

    window_part = Window.partitionBy('fecha_llamada',"vendor_lead_code")
    df_vicidial = df_vicidial.withColumn("q_intentos_dia",count("*").over(window_part))

    window_part = Window.partitionBy("vendor_lead_code")
    df_vicidial = df_vicidial.withColumn(
        "q_intentos_dia_1",
        F.max(
            F.when(F.col("fecha_llamada") == F.col("fecha_llamada_1"), F.col("q_intentos_dia"))
        ).over(window_part)
    )

    return df_vicidial

def since_ventas(spark,fecha_mes_base,campana):
    query = f"""
        select FECHA as fecha_gestion,DNI as vendor_lead_code,PROMOTOR as promotor,ESTADO as estado_venta,TRAMA_HORA as tramo_venta,MONTO as monto_venta,DNIEjecutivo as dni_ejecutivo_venta,Producto as producto_venta,Producto as title,Celular as cel_venta,1 as venta
        from SAMANTHA.dbo.Ventas_Target
        where campana='{campana}'
        and cast(fecha as date) between '{fecha_mes_base}' and EOMONTH('{fecha_mes_base}')
        """
    return obtener_tabla_sql(spark,query,server_zeus,user_zeus,pwd_zeus,db_zeus)

def since_base_maestra_cencosud_ppff(spark):
    query = """
        SELECT 
            ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS indice
            ,regimen_laboral
            ,'Regimen Lab. '+REGIMEN_LABORAL as first_name  
            ,isnull(DEPARTAMENTO,'Sin_Departamento') as last_name
            ,rtrim(ltrim(isnull(APELLIDO_PATERNO,'')))+' '+rtrim(ltrim(isnull(APELLIDO_MATERNO,'')))+' '+rtrim(ltrim(isnull(NOMBRE_1,'')))/*+' '+rtrim(ltrim(isnull(NOMBRE_2,''))) */as address1  
            ,'LINEA SAE'+' '+CONVERT(VARCHAR,[LINEA_SAE]) as security_phrase  
            ,DIRECCION as address2
            ,FEC_FACTURACION as address3
            ,CODDOC as vendor_lead_code 
            ,PROVINCIA as province
            ,DISTRITO as city
            ,' PCT:'+' '+CONVERT(VARCHAR,isnull([PCT_SAE],0)) +' TEA: '+ISNULL(CONVERT(VARCHAR,[TEA]),'0.0') as email
            ,case 
                when /*FECHA_ULT_ATM*/FEC_PAGO is not null 
                    then 'PCT_ANT '+ISNULL(FECHA_ULT_ATM,'')+
                    ' TEA_ANT '+ISNULL(FEC_PAGO,'')+
                    ' MONTO_ANT '+ISNULL(MONTO_ANT,'') 
                    ELSE 'DEUDA_ACTUAL'+' '+isnull(FECHA_ULT_COMP,'Sin Fecha')+
                    ' '+'MONTO_DESEMBOLSAR'+' '+isnull(MONTO_DESEMBOLSAR,'Sin Monto')+
                    ' SALDO_DISP'+REPLACE(ISNULL(RNG_SALDO_TC_ENTRE_LINEA_TOTAL_TC,'SIN MONTO'),'A.[NO CALCULABLE]','')   
                END + '///DISP_RETIRO_EFEC: '+ISNULL(DISP_RETIRO_EFECT_,'') + '//CONSENTIMIENTO:'+
                ISNULL(MARCA,'') + '//MEJORA_TASA:'+ISNULL(MEJORA_TASA,'')  as comments
            ,ISNULL(PRODUCTO,'NA') AS producto
            ,marca
            ,tea
            ,case
                when len(replace(retiro,' ',''))>3  then lower(replace(retiro,' ','_'))
                else 'no_aplica'
            end as retiro
            ,fec_pago,fecha_ult_atm,monto_ant,fecha_ult_comp,monto_desembolsar,rng_saldo_tc_entre_linea_total_tc,disp_retiro_efect_,mejora_tasa,linea_sae,pct_sae
            ,entidad1,deuda1,entidad2,deuda2,entidad3
            ,deuda3,edad,frescura_target,flg_mejora,propension,tipdoc
            ,fecha_envio
        FROM DANTALION.dbo.Base_Maestra_Cencosud_PPFF_Vigente
    """

    df_base= obtener_tabla_sql(spark,query,server_kishin,user_kishin,pwd_kishin,db_kishin)

    df_base = df_base.withColumn(
        "title",
            F.when(F.col("producto") == "Reenganche EC", F.lit("REC"))
            .when(F.col("producto") == "EC Cliente", F.lit("SAE"))
            .otherwise(F.lit("NA"))
    )

    df_base = df_base.withColumn(
        "seg_monto",
        F.when(F.col("LINEA_SAE") < 1000, "a. [0,1000)")
        .when((F.col("LINEA_SAE") >= 1000) & (F.col("LINEA_SAE") < 2000), "b. [1000,2000)")
        .when((F.col("LINEA_SAE") >= 2000) & (F.col("LINEA_SAE") < 3000), "c. [2000,3000)")
        .when((F.col("LINEA_SAE") >= 3000) & (F.col("LINEA_SAE") < 4000), "d. [3000,4000)")
        .when((F.col("LINEA_SAE") >= 4000) & (F.col("LINEA_SAE") < 5000), "e. [4000,5000)")
        .when((F.col("LINEA_SAE") >= 5000) & (F.col("LINEA_SAE") < 6000), "f. [5000,6000)")
        .when((F.col("LINEA_SAE") >= 6000) & (F.col("LINEA_SAE") < 7000), "g. [6000,7000)")
        .when((F.col("LINEA_SAE") >= 7000) & (F.col("LINEA_SAE") < 8000), "h. [7000,8000)")
        .when((F.col("LINEA_SAE") >= 8000) & (F.col("LINEA_SAE") < 9000), "i. [8000,9000)")
        .when((F.col("LINEA_SAE") >= 9000) & (F.col("LINEA_SAE") < 10000), "j. [9000,10000)")
        .when((F.col("LINEA_SAE") >= 10000) & (F.col("LINEA_SAE") < 12000), "k. [10000,12000)")
        .when((F.col("LINEA_SAE") >= 12000) & (F.col("LINEA_SAE") < 15000), "l. [12000,15000)")
        .when((F.col("LINEA_SAE") >= 15000) & (F.col("LINEA_SAE") < 16000), "m. [15000,16000)")
        .when((F.col("LINEA_SAE") >= 16000) & (F.col("LINEA_SAE") < 20000), "n. [16000,20000)")
        .when((F.col("LINEA_SAE") >= 20000) & (F.col("LINEA_SAE") < 25000), "o. [20000,25000)")
        .when(F.col("LINEA_SAE") >= 25000, "p. [25000,+)")
        .otherwise("z. otros")
    )

    df_base = df_base.withColumn(
        "seg_tea",
        F.when(F.col("TEA") < 0.2, "a. [0,20%)")
        .when((F.col("TEA") >= 0.2) & (F.col("TEA") < 0.3), "b. [20%,30%)")
        .when((F.col("TEA") >= 0.3) & (F.col("TEA") < 0.4), "c. [30%,40%)")
        .when((F.col("TEA") >= 0.4) & (F.col("TEA") < 0.5), "d. [40%,50%)")
        .when((F.col("TEA") >= 0.5) & (F.col("TEA") < 0.6), "e. [50%,60%)")
        .when((F.col("TEA") >= 0.6) & (F.col("TEA") < 0.7), "f. [60%,70%)")
        .when((F.col("TEA") >= 0.7) & (F.col("TEA") < 0.8), "g. [70%,80%)")
        .when(F.col("TEA") >= 0.8, "h. [80%,+)")
        .otherwise("z. otros")
    )
    edad = F.col("edad").cast("int")

    df_base = df_base.withColumn(
        "seg_edad",
        F.when((edad >= 20) & (edad < 30), "a. [20,30)")
        .when((edad >= 30) & (edad < 40), "b. [30,40)")
        .when((edad >= 40) & (edad < 50), "c. [40,50)")
        .when((edad >= 50) & (edad < 60), "d. [50,60)")
        .when((edad >= 60) & (edad < 70), "e. [60,70)")
        .when(edad >= 70, "f. [70,+)")
        .otherwise("z. otros")
    )

    return df_base.withColumn(
        "vendor_lead_code",
        F.right(
            F.concat(F.lit("00000000"), F.col("vendor_lead_code")),
            F.lit(8)
        )
    )

def since_base_maestra_cencosud_tc(spark):
    query = """
        SELECT
        ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS indice,
        ISNULL([DEPARTAMENTO_A],'Sin_Departamento') AS last_name,
        CASE 
            WHEN SEGMENTO='TC1' THEN 'SEGMENTO EXCLUSIVO'
            WHEN SEGMENTO='TC2' THEN 'SEGMENTO BONUS'
            WHEN SEGMENTO='TC3' THEN 'SEGMENTO SIN BONUS'
        END AS first_name,
        rtrim(ltrim(isnull([APATERNO],'')))+' '+rtrim(ltrim(isnull([AMATERNO],'')))+' '+rtrim(ltrim(isnull([NOMBRE],''))) as address1,
        [CODDOC] AS vendor_lead_code,
        'TIP_TARJETA'+' '+[TIPO_TARJETA_CENCO] as address2,
        'REQUERIMIENTO'+' '+[DOCUMENTOS]+' '+'REG_LAB'+' '+ISNULL([REGIMEN_LABORAL],' ') AS address3,
        'OFERTA'+' '+CONVERT(VARCHAR(20),[LINEA_CENCOSUD]) AS security_phrase,
        [DEPARTAMENTO_B] AS province,
        [DISTRITO] AS city,
        ISNULL(FRECUENCIA_COMPRA,'')+'//'+
        CASE 
            WHEN FLG_EX_CLIENTE='0' THEN 'NUEVO' 
            WHEN FLG_EX_CLIENTE='1' 
            THEN 'EX-CLIENTE' 
        END as comments,
        SEGMENTO as segmento,
        tipdoc,
        regimen_laboral,
        QUINTIL_PROPENSION AS quintil,
        CONVERT(INT,LINEA_CENCOSUD) AS linea_cencosud,
        rng_edad,
        propension_efectivo,
        tipo_tarjeta_cenco,
        num_tc,
        FLG_EX_CLIENTE as flg_ex_cliente_n,
        FLG_FRESCURA as flg_frescura_n,
        FLG_CLIENTE_NUEVO as flg_cliente_nuevo_n,
        FLG_MEJORA_OFERTA AS mejora_oferta,
        seg_contact,
        fecha_envio,
        case 
            when REP1=1 then 'STOCK' 
            else 'NUEVO' 
        end as antiguedad,
        case
            when len(replace(retiro,' ',''))>3  then lower(replace(retiro,' ','_'))
            else 'no_aplica'
        end as retiro
        FROM DANTALION.dbo.Base_Maestra_Cencosud_TC_vigente
    """
    df_base_p=obtener_tabla_sql(spark,query,server_kishin,user_kishin,pwd_kishin,db_kishin)

    df_base_p = df_base_p.withColumn(
        "seg_edad",
        F.when(F.col("rng_edad")=="A","a.[18-25]")
        .when(F.col("rng_edad")=="B","b.[25-30]")
        .when(F.col("rng_edad")=="C","c.[30-40]")
        .when(F.col("rng_edad")=="D","d.[40-50]")
        .when(F.col("rng_edad")=="E","e.[50-55]")
        .when(F.col("rng_edad")=="F","f.[55-65]")
        .when(F.col("rng_edad")=="G","g.[65-MÁS]")
        .otherwise("h.otros")
    )
    df_base_p = df_base_p.withColumn(
        "flg_ex_cliente",
        F.when(F.col("flg_ex_cliente_n")=="0","NUEVO")
        .when(F.col("flg_ex_cliente_n")=="1","EX-CLIENTE")
        .otherwise(F.col("flg_ex_cliente_n"))
    ).drop('flg_ex_cliente_n')
    df_base_p=df_base_p.withColumn('email',F.lit('T NORMAL'))

    df_base_p = df_base_p.withColumn(
        "region",
        F.when(F.coalesce(F.col("province"), F.lit("")) == "", "Leads sin región")
        .when(F.col("province").isin("LIMA","CALLAO"), "Leads Lima")
        .otherwise("Leads Provincia")
    )
    df_base_p = df_base_p.withColumn(
        "linea_cencosud",
        F.regexp_replace("linea_cencosud", ",", "").cast("double")
    )

    df_base_p = df_base_p.withColumn(
        "flg_frescura",
        F.when(F.col("flg_frescura_n") == "0", "REPETIDO")
        .when(F.col("flg_frescura_n") == "1", "NUEVO")
        .otherwise(F.col("flg_frescura_n"))
    ).drop('flg_frescura_n')
    df_base_p = df_base_p.withColumn(
        "flg_cliente_nuevo",
        F.when(F.col("flg_cliente_nuevo_n") == "0", "REPETIDO")
        .when(F.col("flg_cliente_nuevo_n") == "1", "NUEVO")
        .otherwise(F.col("flg_cliente_nuevo_n"))
    ).drop('flg_cliente_nuevo_n')

    cond_base = (
        (F.col("segmento").isin("TC2","TC3")) &
        (F.col("province").isin("LIMA","CALLAO")) &
        (F.col("tipo_tarjeta_cenco") == "CLASICA")
    )
    df_base_p = df_base_p.withColumn(
        "mes_prioridad",
        F.when(cond_base & F.col("quintil").isin("1","2") & (F.col("linea_cencosud") <= 5000),"PRIORIDAD_1")
        .when(cond_base & F.col("quintil").isin("1","2") & (F.col("linea_cencosud").between(1001,3000)),"PRIORIDAD_2")
        .when(cond_base & F.col("quintil").isin("3","4") & (F.col("linea_cencosud") > 3000),"PRIORIDAD_3")
        .when(cond_base & (F.col("quintil")=="5"),"PRIORIDAD_4")
    )
    return df_base_p.withColumn(
        "vendor_lead_code",
        F.right(
            F.concat(F.lit("00000000"), F.col("vendor_lead_code")),
            F.lit(8)
        )
    )

def since_base_maestra_efe_consumo(spark):
    query = """
    select        
        ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS indice,
        tipocliente as first_name,
        isnull(departamento,'Sin_Departamento') as last_name,
        cliente as address1,        
        'EMP1='+empresa1+'/ EMP2='+empresa2+'/ EMP3='+ empresa3 as address3,     
        empresa1,
        empresa2,
        empresa3,
        direccion as comments_2,        
        dni as vendor_lead_code,        
        ISNULL(provincia,'') as province,        
        distrito as city,        
        'COD_AGENCIA ='+codigo_agencia as email,   
        agencia,     
        ('LINEA_EXPRESS'+' '+isnull(linea_ft,'')+'//'+'LINEA_ESCRITORIO'+' '+isnull(linea_hs_rs,'')+'//'+'LINEA_SEMIFULL'+' '+isnull(linea_hs_plus,'')+'//'+'LINEA_FULL'+' '+isnull(linea_full,'')) as comments,        
        linea_acotada as oferta,        
        marca2 as tip_prioridad,     
        marca_2025,        
        nomcomercial,        
        perfil_ic,        
        tipoingreso,        
        asignacion,     
        TRY_CAST(
                REPLACE(linea_ft, '''', '')
            AS FLOAT) as linea_ft,    
        TRY_CAST(
                REPLACE(linea_hs_rs, '''', '')
            AS FLOAT) as linea_hs_rs,
        TRY_CAST(
                REPLACE(linea_hs_plus, '''', '')
            AS FLOAT) AS linea_hs_plus, 
        linea_full,   
        case when rep1=1 then 'stock' else 'nuevo' end as marca,        
        perfil,
        segmento,        
        zona,        
        fecha_envio,          
        case
            when len(replace(retiro,' ',''))>3  then lower(replace(retiro,' ','_'))
            else 'no_aplica'
        end as retiro,   
        cast(replace(tasa,'NULL','') as float)/100.0 as tasa,
        cast(replace(score,'NULL','') as int) as score,
        rep1,
        case 
            when isnull(provincia,'') = '' then 'leads sin región'  
            when provincia in ('lima','callao') then 'leads lima'  
            else 'leads provincia' 
        end as region, 
        marca as marcadesbase,
        marca2,
        flgsubproceso_hs,
        situacionlaboral         
    from dantalion.dbo.base_maestra_efectiva_vigente
    """
        
    df_base= obtener_tabla_sql(spark,query,server_kishin,user_kishin,pwd_kishin,db_kishin)

    df_base = df_base.withColumn(
        "score_%",
        F.when((F.col("score") >= 0) & (F.col("score") <= 199), "a. [0,20%)")
        .when(F.col("score") <= 399, "b. [20%,40%)")
        .when(F.col("score") <= 599, "c. [40%,60%)")
        .when(F.col("score") <= 799, "d. [60%,80%)")
        .when(F.col("score") <= 999, "e. [80%,100%)")
        .otherwise("z. otros")
    )

    df_base = df_base.withColumn(
        "rango_tasa",
        F.when((F.col("tasa") >= 14) & (F.col("tasa") <= 30), "a. [14%,30%]")
        .when(F.col("tasa") <= 0.4, "b. (30%,40%]")
        .when(F.col("tasa") <= 0.5, "c. (40%,50%]")
        .when(F.col("tasa") <= 0.6, "d. (50%,60%]")
        .when(F.col("tasa") <= 0.7, "e. (60%,70%]")
        .when(F.col("tasa") <= 0.8, "f. (70%,80%]")
        .when(F.col("tasa") <= 0.9, "g. (80%,90%]")
        .when(F.col("tasa") <= 2.0, "h. (90%,+)")
        .otherwise("z. otros")
    )
    df_base = df_base.withColumn(
            "seg_linea_hs_plus",
            F.when(F.col("linea_hs_plus") < 1000, "a. [0,1000)")
            .when((F.col("linea_hs_plus") >= 1000) & (F.col("linea_hs_plus") < 2000), "b. [1000,2000)")
            .when((F.col("linea_hs_plus") >= 2000) & (F.col("linea_hs_plus") < 3000), "c. [2000,3000)")
            .when((F.col("linea_hs_plus") >= 3000) & (F.col("linea_hs_plus") < 4000), "d. [3000,4000)")
            .when((F.col("linea_hs_plus") >= 4000) & (F.col("linea_hs_plus") < 5000), "e. [4000,5000)")
            .when((F.col("linea_hs_plus") >= 5000) & (F.col("linea_hs_plus") < 6000), "f. [5000,6000)")
            .when((F.col("linea_hs_plus") >= 6000) & (F.col("linea_hs_plus") < 7000), "g. [6000,7000)")
            .when((F.col("linea_hs_plus") >= 7000) & (F.col("linea_hs_plus") < 8000), "h. [7000,8000)")
            .when((F.col("linea_hs_plus") >= 8000) & (F.col("linea_hs_plus") < 9000), "i. [8000,9000)")
            .when((F.col("linea_hs_plus") >= 9000) & (F.col("linea_hs_plus") < 10000), "j. [9000,10000)")
            .when((F.col("linea_hs_plus") >= 10000) & (F.col("linea_hs_plus") < 12000), "k. [10000,12000)")
            .when((F.col("linea_hs_plus") >= 12000) & (F.col("linea_hs_plus") < 15000), "l. [12000,15000)")
            .when((F.col("linea_hs_plus") >= 15000) & (F.col("linea_hs_plus") < 16000), "m. [15000,16000)")
            .when((F.col("linea_hs_plus") >= 16000) & (F.col("linea_hs_plus") < 20000), "n. [16000,20000)")
            .when((F.col("linea_hs_plus") >= 20000) & (F.col("linea_hs_plus") < 25000), "o. [20000,25000)")
            .when(F.col("linea_hs_plus") >= 25000, "p. [25000,+)")
            .otherwise("z. otros")
        )
    df_base = df_base.withColumn(
            "seg_linea_ft",
                F.when(F.col("linea_ft") < 1000, "a. [0,1000)")
                .when((F.col("linea_ft") >= 1000) & (F.col("linea_ft") < 2000), "b. [1000,2000)")
                .when((F.col("linea_ft") >= 2000) & (F.col("linea_ft") < 3000), "c. [2000,3000)")
                .when((F.col("linea_ft") >= 3000) & (F.col("linea_ft") < 4000), "d. [3000,4000)")
                .when((F.col("linea_ft") >= 4000) & (F.col("linea_ft") < 5000), "e. [4000,5000)")
                .when((F.col("linea_ft") >= 5000) & (F.col("linea_ft") < 6000), "f. [5000,6000)")
                .when((F.col("linea_ft") >= 6000) & (F.col("linea_ft") < 7000), "g. [6000,7000)")
                .when((F.col("linea_ft") >= 7000) & (F.col("linea_ft") < 8000), "h. [7000,8000)")
                .when((F.col("linea_ft") >= 8000) & (F.col("linea_ft") < 9000), "i. [8000,9000)")
                .when((F.col("linea_ft") >= 9000) & (F.col("linea_ft") < 10000), "j. [9000,10000)")
                .when((F.col("linea_ft") >= 10000) & (F.col("linea_ft") < 12000), "k. [10000,12000)")
                .when((F.col("linea_ft") >= 12000) & (F.col("linea_ft") < 15000), "l. [12000,15000)")
                .when((F.col("linea_ft") >= 15000) & (F.col("linea_ft") < 16000), "m. [15000,16000)")
                .when((F.col("linea_ft") >= 16000) & (F.col("linea_ft") < 20000), "n. [16000,20000)")
                .when((F.col("linea_ft") >= 20000) & (F.col("linea_ft") < 25000), "o. [20000,25000)")
                .when(F.col("linea_ft") >= 25000, "p. [25000,+)")
                .otherwise("z. otros")
            )
    df_base = df_base.withColumn(
            "seg_linea_hs_rs",
                F.when(F.col("linea_hs_rs") < 1000, "a. [0,1000)")
                .when((F.col("linea_hs_rs") >= 1000) & (F.col("linea_hs_rs") < 2000), "b. [1000,2000)")
                .when((F.col("linea_hs_rs") >= 2000) & (F.col("linea_hs_rs") < 3000), "c. [2000,3000)")
                .when((F.col("linea_hs_rs") >= 3000) & (F.col("linea_hs_rs") < 4000), "d. [3000,4000)")
                .when((F.col("linea_hs_rs") >= 4000) & (F.col("linea_hs_rs") < 5000), "e. [4000,5000)")
                .when((F.col("linea_hs_rs") >= 5000) & (F.col("linea_hs_rs") < 6000), "f. [5000,6000)")
                .when((F.col("linea_hs_rs") >= 6000) & (F.col("linea_hs_rs") < 7000), "g. [6000,7000)")
                .when((F.col("linea_hs_rs") >= 7000) & (F.col("linea_hs_rs") < 8000), "h. [7000,8000)")
                .when((F.col("linea_hs_rs") >= 8000) & (F.col("linea_hs_rs") < 9000), "i. [8000,9000)")
                .when((F.col("linea_hs_rs") >= 9000) & (F.col("linea_hs_rs") < 10000), "j. [9000,10000)")
                .when((F.col("linea_hs_rs") >= 10000) & (F.col("linea_hs_rs") < 12000), "k. [10000,12000)")
                .when((F.col("linea_hs_rs") >= 12000) & (F.col("linea_hs_rs") < 15000), "l. [12000,15000)")
                .when((F.col("linea_hs_rs") >= 15000) & (F.col("linea_hs_rs") < 16000), "m. [15000,16000)")
                .when((F.col("linea_hs_rs") >= 16000) & (F.col("linea_hs_rs") < 20000), "n. [16000,20000)")
                .when((F.col("linea_hs_rs") >= 20000) & (F.col("linea_hs_rs") < 25000), "o. [20000,25000)")
                .when(F.col("linea_hs_rs") >= 25000, "p. [25000,+)")
                .otherwise("z. otros")
            )
    df_base = df_base.withColumn(
        "region",
        F.when(F.col("province").isNull() | (F.col("province") == ""), "a. leads sin región")
        .when(F.col("province").isin("LIMA", "CALLAO"), "b. leads lima")
        .otherwise("c. leads provincia")
    )

    return df_base.withColumn(
        "vendor_lead_code",
        F.right(
            F.concat(F.lit("00000000"), F.col("vendor_lead_code")),
            F.lit(8)
        )
    )

def since_base_maestra_efe_negocio(spark):
    query = """
        Select  
        ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS indice,
        ISNULL([SEGMICROPEQUENACOMERCIAL],'IMPULSA') as first_name,  
        isnull([DEPARTAMENTO],'Sin_Departamento') as last_name,  
        CASE 
            WHEN Materno IS NULL THEN Nombres +' '+Paterno 
            ELSE Nombres + ' ' + Paterno +' '+ Materno 
        END AS address1,  
        [DIRECCION] as address2,  
        'EMP1='+[empresa1]+'/ EMP2='+[empresa2]+'/ EMP3='+ [empresa3] as address3,  
        Proceso as title,  
        Montos_Referenciales as comments, 
        'IMPORTE DEUDA'+' '+CONVERT(VARCHAR,IMPDHM) as security_phrase,  
        NUMDOCUMENTO as vendor_lead_code,  
        PROVINCIA as province,  
        DISTRITO as city,  
        AREAEFECTINEGOCIOS AS email,  
        IMPDHM AS deuda,  
        SEGMICROPEQUENACOMERCIAL as tip_prioridad,  
        Zona,  
        perfil_ic,  
        canal_asignado,
        CASE 
            WHEN REP1=1 then 'STOCK' 
            ELSE 'NUEVO'
        END as marca,  
        fecha_envio,           
        case
            when len(replace(retiro,' ',''))>3  then lower(replace(retiro,' ','_'))
            else 'no_aplica'
        end as retiro 
        from dantalion.dbo.Base_Maestra_Efectiva_Negocios_Vigente
    """
        
    df_base= obtener_tabla_sql(spark,query,server_kishin,user_kishin,pwd_kishin,db_kishin)

    df_base = df_base.withColumn(
        "region",
        F.when(F.coalesce(F.col("province"), F.lit("")) == "", "Leads sin región")
        .when(F.col("province").isin("LIMA", "CALLAO"), "Leads Lima")
        .otherwise("Leads Provincia")
    )

    df_base = df_base.withColumn(
        "prioridada",
        F.when(
            (F.col("province").isin("LIMA", "CALLAO")) &
            (F.col("title") == "humano seguro") &
            (F.col("tip_prioridad") == "prf"),
            "prioridad 2"
        )
        .when(
            (F.col("province") != "LIMA") &
            (F.col("title") == "fasttrack") &
            (F.col("tip_prioridad") == "elt"),
            "prioridad 1"
        )
        .when(
            (F.col("province").isin("LIMA", "CALLAO")) &
            (F.col("title") == "FULL") &
            (F.col("tip_prioridad") == "prf"),
            "prioridad 3"
        )
        .otherwise("otra prioridad")
    )

    return df_base.withColumn(
        "vendor_lead_code",
        F.right(
            F.concat(F.lit("00000000"), F.col("vendor_lead_code")),
            F.lit(8)
        )
    )
    # print(df_base.columns)

def since_base_maestra_tc_dinners(spark):
    query = """
        select 
            numero_documento as vendor_lead_code,
            [tasa(tea)] as tea,tea_df,tcea,tcea_cuotas_df,
            isnull([producto],'') as first_name,
            isnull([limaprovincia],'sin_departamento') as last_name,
            ISNULL([nombres],'')+' '+ISNULL([apellido paterno],'')+' '+isnull([apellido materno],'') as address1,
            datediff(year, try_convert(date, fec_nac, 23), getdate()) as address2,
            'free'+' tea '+convert(varchar,tea_df)+' tcea '+convert(varchar,tcea_cuotas_df) as address3,
            isnull(cashback,'') as city,
            isnull(marca_speech,'') as province,
            'tea'+' '+convert(varchar,[tasa(tea)])+' '+'tcea'+' '+ convert(varchar,tcea) as email,
            'ofert.dolar'+' '+isnull([linea credito dolares],'') as security_phrase,
            CAST(REPLACE([LINEA CREDITO DOLARES], ',', '') AS numeric(18,2)) AS linea_credito,
            cprueba as comments,
            CONVERT(float, REPLACE(l_bcp, ',', '.'))   AS l_bcp,
            CONVERT(float, REPLACE(l_bbva, ',', '.'))  AS l_bbva,
            CONVERT(float, REPLACE(l_ibk, ',', '.'))   AS l_ibk,
            CONVERT(float, REPLACE(l_sco, ',', '.'))   AS l_sco,
            CONVERT(float, REPLACE(l_bif, ',', '.'))   AS l_bif,
            CONVERT(float, REPLACE(l_citi, ',', '.'))  AS l_citi,
            CONVERT(float, REPLACE(l_fin, ',', '.'))   AS l_fin,
            CONVERT(float, REPLACE(l_rip, ',', '.'))   AS l_rip,
            CONVERT(float, REPLACE(l_cmr, ',', '.'))   AS l_cmr,
            CONVERT(float, REPLACE(l_cresco, ',', '.'))AS l_cresco,
            CONVERT(float, REPLACE(l_cen, ',', '.'))   AS l_cen,
            CONVERT(float, REPLACE(l_azt, ',', '.'))   AS l_azt,
            CONVERT(float, REPLACE(l_uno, ',', '.'))   AS l_uno,
            CONVERT(float, REPLACE(l_gnb, ',', '.'))   AS l_gnb,
            CONVERT(float, REPLACE(l_efe, ',', '.'))   AS l_efe,
            CONVERT(float, REPLACE(l_com, ',', '.'))   AS l_com,
            CONVERT(float, REPLACE(l_nac, ',', '.'))   AS l_nac,
            campanasyd as straming,
            fec_nac,
            grupo_ejecucion as prioridad,
            [id proveedor] as priori,servicio,
            perfil as marca,
            new_profile as prfl,
            n_base,
            recurrencia as marca3,
            case when isnull(rep1,0)=1 then 'stock' else 'nuevo' end as recurr1m,
            case when isnull(rep2,0)=1 then 'stock' else 'nuevo' end as recurr2m,
            case when rep3=1 then 'stock' else 'nuevo' end as recurr3m,
            case when (rep1=1 or rep2=1 or rep3=1) then 'stock' else 'nuevo' end as conjunto3meses,
            marca as numtc,
            marca2 as piloto,
            marca3 as segmentacion,
            fecha_envio,
            provincia,
            prob_contacto,           
            case
                when len(replace(retiro,' ',''))>3  then lower(replace(retiro,' ','_'))
                else 'no_aplica'
            end as retiro ,
            case when (rep1=1 or rep2=1 or rep3=1) then 'sotck' else 'nuevo' end as recurrencia3m
        FROM DANTALION.dbo.BASE_MAESTRA_DINERS_TC_VIGENTE
    """
    df_base_p=obtener_tabla_sql(spark,query,server_kishin,user_kishin,pwd_kishin,db_kishin)

    # from pyspark.sql.functions import greatest, col, when

    df_base_p = df_base_p.withColumn(
        "monto_title",
        greatest(
            F.col("l_ibk"), F.col("l_bcp"), F.col("l_bbva"), F.col("l_sco"),
            F.col("l_bif"), F.col("l_citi"), F.col("l_fin"), F.col("l_rip"),
            F.col("l_cmr"), F.col("l_cresco"), F.col("l_cen"), F.col("l_azt"),
            F.col("l_uno"), F.col("l_gnb"), F.col("l_efe"), F.col("l_com"), F.col("l_nac")
        )
    )

    df_base_p = df_base_p.withColumn(
        "title",
        F.when(F.col("monto_title") == F.col("l_ibk"), "l_ibk")
        .when(F.col("monto_title") == F.col("l_bcp"), "l_bcp")
        .when(F.col("monto_title") == F.col("l_bbva"), "l_bbva")
        .when(F.col("monto_title") == F.col("l_sco"), "l_sco")
        .when(F.col("monto_title") == F.col("l_bif"), "l_bif")
        .when(F.col("monto_title") == F.col("l_citi"), "l_citi")
        .when(F.col("monto_title") == F.col("l_fin"), "l_fin")
        .when(F.col("monto_title") == F.col("l_rip"), "l_rip")
        .when(F.col("monto_title") == F.col("l_cmr"), "l_cmr")
        .when(F.col("monto_title") == F.col("l_cresco"), "l_cresco")
        .when(F.col("monto_title") == F.col("l_cen"), "l_cen")
        .when(F.col("monto_title") == F.col("l_azt"), "l_azt")
        .when(F.col("monto_title") == F.col("l_uno"), "l_uno")
        .when(F.col("monto_title") == F.col("l_gnb"), "l_gnb")
        .when(F.col("monto_title") == F.col("l_efe"), "l_efe")
        .when(F.col("monto_title") == F.col("l_com"), "l_com")
        .when(F.col("monto_title") == F.col("l_nac"), "l_nac")
        .otherwise("OTROS")
    )

    df_base_p = df_base_p.withColumn(
        "seg_edad",
        F.when((F.floor(F.months_between(F.current_date(), F.col("fec_nac"))/12) >= 20) &
            (F.floor(F.months_between(F.current_date(), F.col("fec_nac"))/12) < 30), "a. 20 A 30")
        .when((F.floor(F.months_between(F.current_date(), F.col("fec_nac"))/12) >= 30) &
            (F.floor(F.months_between(F.current_date(), F.col("fec_nac"))/12) < 40), "b. 30 A 40")
        .when((F.floor(F.months_between(F.current_date(), F.col("fec_nac"))/12) >= 40) &
            (F.floor(F.months_between(F.current_date(), F.col("fec_nac"))/12) < 50), "c. 40 A 50")
        .when((F.floor(F.months_between(F.current_date(), F.col("fec_nac"))/12) >= 50) &
            (F.floor(F.months_between(F.current_date(), F.col("fec_nac"))/12) < 60), "d. 50 A 60")
        .when((F.floor(F.months_between(F.current_date(), F.col("fec_nac"))/12) >= 60) &
            (F.floor(F.months_between(F.current_date(), F.col("fec_nac"))/12) < 76), "e. 60 A 70")
        .otherwise('f. Otros')
    )
    df_base_p = df_base_p.withColumn(
        'edad',
        F.floor(F.months_between(F.current_date(), F.col("fec_nac")) / 12)
    )
    df_base_p= df_base_p.withColumn(
        "seg_oferta",
        F.when((F.col("linea_credito") >= 1000) & (F.col("linea_credito") <= 2000), "A. 1,000 A 2000")
        .when((F.col("linea_credito") > 2000) & (F.col("linea_credito") <= 4000), "A. 2,000 A 4000")
        .when((F.col("linea_credito") > 4000) & (F.col("linea_credito") <= 6000), "B. 4,000 A 6,000")
        .when((F.col("linea_credito") > 6000) & (F.col("linea_credito") <= 8000), "C. 6,000 A 8,000")
        .when((F.col("linea_credito") > 8000) & (F.col("linea_credito") <= 10000), "D. 8,000 A 10,000")
        .when((F.col("linea_credito") > 10000) & (F.col("linea_credito") <= 12000), "D. 10,000 A 12,000")
        .when(F.col("linea_credito") > 12000, "E. 12,000 A MÁS")
    )
    return df_base_p.withColumn(
        "vendor_lead_code",
        F.right(
            F.concat(F.lit("00000000"), F.col("vendor_lead_code")),
            F.lit(8)
        )
    )


# TRY_CAST(REPLACE(RIGHT(prioridad_inicial, 2), ' ', '') AS INT) AS prioridad_inicial,

def since_base_maestra_pp_dinners(spark):
    query = f"""
        SELECT
            NumDoc AS vendor_lead_code,
            marca AS title,
            ISNULL(nombre_producto, '') AS first_name,
            ISNULL(departamento, 'Sin_Departamento') AS last_name,
            nombre_largo AS address1,

            'FEC.EXP.TC' + ' ' +
            ISNULL(SUBSTRING(fecha_expiracion, 1, 4), ' ') + '-' +
            ISNULL(SUBSTRING(fecha_expiracion, 5, 2), ' ') + '-' +
            ' Numcuots: ' + ISNULL(CONVERT(VARCHAR, nrocuotas_rest), ' ')
            AS address2,

            'CICLO ' + ISNULL(CONVERT(VARCHAR, ciclo), ' ') +
            ' nro_tc:' + ISNULL(nro_tarjeta, '')
            AS address3,

            ISNULL(distrito, '') AS city,
            ISNULL(provincia, '') AS province,
            fecha_expiracion,

            CASE
            WHEN tea_formato IS NOT NULL AND tea_PPD IS NOT NULL THEN 
            'TEA' + ' ' + ISNULL(CONVERT(VARCHAR, tea_formato), ' ') +' '+
            'TEA anterior' + ' ' + ISNULL(CONVERT(VARCHAR, tea_PPD), ' ') +' '+
            ' TEM ppd ' + ISNULL(CONVERT(VARCHAR, tem_formato), ' ') +' '+
            'TASA app'+' '+isnull(CONVERT(varchar,tasa_app),' ')
            WHEN tea_formato IS NOT NULL THEN 
            'TEA' + ' ' + ISNULL(CONVERT(VARCHAR, tea_formato), ' ') +' '+
            ' TEM ' + ISNULL(CONVERT(VARCHAR, tem_formato), ' ') +' '+
            'TASA app'+' '+isnull(CONVERT(varchar,tasa_app),' ')
            WHEN tea_PPD IS NOT NULL THEN 
            'TEA anterior' + ' ' + ISNULL(CONVERT(VARCHAR, tea_PPD), ' ') +' '+
            ' TEM ' + ISNULL(CONVERT(VARCHAR, tem_formato), ' ') +' '+
            'TASA app'+' '+isnull(CONVERT(varchar,tasa_app),' ')
            END
            AS email,

            CASE
            WHEN saldo_ppd IS NOT NULL  AND Linea_EI_60M IS NOT NULL THEN 
            'saldo:' + ISNULL(CAST(saldo_ppd AS VARCHAR), '')+' '+ 
            'Monto:' + ISNULL(CAST(Linea_EI_60M AS VARCHAR), '')+' '+ 
            'Campaña :' + ISNULL(marca4, 'no tiene')
            WHEN saldo_ppd IS NOT NULL  THEN 
            'saldo:' + ISNULL(CAST(saldo_ppd AS VARCHAR), '')+' '+ 
            'Campaña :' + ISNULL(marca4, 'no tiene')
            WHEN Linea_EI_60M IS NOT NULL THEN 
            'Monto:' + ISNULL(CAST(Linea_EI_60M AS VARCHAR), '')+' '+ 
            'Campaña :' + ISNULL(marca4, 'no tiene')
            END 
            AS security_phrase,

            CASE
                WHEN MARCA='CD' THEN ',Deuda_BCP: ' + ISNULL(Deuda_BCP,'') +' '+ CHAR(13) + CHAR(10) +
            ',Deuda_bbva: ' + ISNULL(Deuda_Continental,'') +' '+ CHAR(13) + CHAR(10) +
            ',Deuda_ibk: ' + ISNULL(Deuda_Interbank,'') +' '+ CHAR(13) + CHAR(10) +
            ',Deuda_cco: ' + ISNULL(Deuda_ScotiaBank,'') +' '+ CHAR(13) + CHAR(10) +
            ',Deuda_Falab: ' + ISNULL(Deuda_Otros,'')
            ELSE 
                case
                    when TRY_CAST(REPLACE( LEFT(prioridad_inicial, CHARINDEX('.', prioridad_inicial) - 1) , ' ', '') AS INT) in (4,5) then 'seguro 3%'
                    else ''
                end 
            END
            AS comments,

            TRY_CAST(
                REPLACE(
                    NULLIF(REPLACE(TRIM(Deuda_BCP), '-', ''), ''),
                    ',', '.'
                ) AS FLOAT
            ) AS deuda_bcp,

            TRY_CAST(
                REPLACE(
                    NULLIF(REPLACE(TRIM(Deuda_Continental), '-', ''), ''),
                    ',', '.'
                ) AS FLOAT
            ) AS deuda_continental,

            TRY_CAST(
                REPLACE(
                    NULLIF(REPLACE(TRIM(Deuda_ScotiaBank), '-', ''), ''),
                    ',', '.'
                ) AS FLOAT
            ) AS deuda_scotiaBank,

            TRY_CAST(
                REPLACE(
                    NULLIF(REPLACE(TRIM(Deuda_Interbank), '-', ''), ''),
                    ',', '.'
                ) AS FLOAT
            ) AS deuda_interbank,

            TRY_CAST(
                REPLACE(
                    NULLIF(REPLACE(TRIM(Deuda_Otros), '-', ''), ''),
                    ',', '.'
                ) AS FLOAT
            ) AS deuda_otros,

            TRY_CAST(
                REPLACE(
                    NULLIF(REPLACE(TRIM(saldo_ppd), '-', ''), ''),
                    ',', '.'
                ) AS FLOAT
            ) AS saldo_ppd,

            CONVERT(DATE, CAST(FechaNacimiento AS VARCHAR(8))) AS fec_nac,
            TRIM(estadocivil) AS estadocivil,

            TRY_CAST(
                REPLACE(
                    NULLIF(REPLACE(TRIM(tea_formato), '%', ''), ''),
                    ',', '.'
                ) AS DECIMAL(10,4)
            ) / 100 AS tea_formato,

            TRY_CAST(
                REPLACE(
                    NULLIF(REPLACE(TRIM(tem_formato), '%', ''), ''),
                    ',', '.'
                ) AS DECIMAL(10,4)
            ) / 100 AS tem_formato,

            nacionalidad,
            Flag_entrega,

            CASE
                WHEN mes_entrega IS NOT NULL AND mes_entrega <> 0
                    THEN CONVERT(DATE, CAST(mes_entrega AS VARCHAR(6)) + '01')
                ELSE NULL
            END AS mes_entrega,

            TRY_CAST(REPLACE( LEFT(prioridad_inicial, CHARINDEX('.', prioridad_inicial) - 1) , ' ', '') AS INT) AS prioridad_inicial,

            ciclo,
            flg_segmento_digital,

            TRY_CAST(
                REPLACE(
                    NULLIF(REPLACE(TRIM(tasa_app), '-', ''), ''),
                    ',', '.'
                ) AS FLOAT
            ) / 100 AS tasa_app,

            oferppd,
            oferppdplus,
            oferds,
            oferbt,
            marca2,
            marca3 as recurrencia_digital,
            marca4 as clave_web,
            recencia,
            linea_ei_60m,
            oferta_disef,
            oferta_ppd,
            tasappd,
            nrocuotas_rest,
            cargo,
            profesion,
            rpioridadvocales,
            estado_tarjeta,
            distrito_lab,
            TRY_CAST(
                REPLACE(
                    NULLIF(REPLACE(TRIM(tea_ppd), '%', ''), ''),
                    ',', '.'
                ) AS DECIMAL(10,4)
            ) / 100 AS tea_ppd,
            TRY_CAST(
                REPLACE(
                    NULLIF(REPLACE(TRIM(Linea_EI_60M), '-', ''), ''),
                    ',', '.'
                ) AS FLOAT
            ) AS monto_ppd,
            CASE
                WHEN rep1 = 1 THEN 'stock'
                ELSE 'nuevo'
            END AS rep,

            fecha_envio,
            flag_adicion,

            CASE
                WHEN LEN(REPLACE(retiro, ' ', '')) > 3 THEN LOWER(REPLACE(retiro, ' ', '_'))
                ELSE 'no_aplica'
            END AS retiro,

            nombre_producto

        FROM DANTALION.dbo.Base_Maestra_Diners_Vigente
        """
    df_base = obtener_tabla_sql(spark, query, server_kishin, user_kishin, pwd_kishin, db_kishin)

    df_base = df_base.withColumn(
        "monto_deuda_max",
        greatest(
            F.col("deuda_bcp"), F.col("deuda_continental"), F.col("deuda_scotiaBank"), F.col("deuda_interbank"),
            F.col("deuda_otros")
        )
    )

    df_base = df_base.withColumn(
        "deuda_bench",
        F.when(F.col("monto_deuda_max") == F.col("deuda_bcp"), "deuda_bcp")
        .when(F.col("monto_deuda_max") == F.col("deuda_continental"), "deuda_continental")
        .when(F.col("monto_deuda_max") == F.col("deuda_scotiaBank"), "deuda_scotiaBank")
        .when(F.col("monto_deuda_max") == F.col("deuda_interbank"), "deuda_interbank")
        .when(F.col("monto_deuda_max") == F.col("deuda_otros"), "deuda_otros")
        .otherwise("OTROS")
    )

    df_base = df_base.withColumn(
        "seg_edad",
        F.when((F.floor(F.months_between(F.current_date(), F.col("fec_nac"))/12) >= 20) &
            (F.floor(F.months_between(F.current_date(), F.col("fec_nac"))/12) < 30), "a. 20 A 30")
        .when((F.floor(F.months_between(F.current_date(), F.col("fec_nac"))/12) >= 30) &
            (F.floor(F.months_between(F.current_date(), F.col("fec_nac"))/12) < 40), "b. 30 A 40")
        .when((F.floor(F.months_between(F.current_date(), F.col("fec_nac"))/12) >= 40) &
            (F.floor(F.months_between(F.current_date(), F.col("fec_nac"))/12) < 50), "c. 40 A 50")
        .when((F.floor(F.months_between(F.current_date(), F.col("fec_nac"))/12) >= 50) &
            (F.floor(F.months_between(F.current_date(), F.col("fec_nac"))/12) < 60), "d. 50 A 60")
        .when((F.floor(F.months_between(F.current_date(), F.col("fec_nac"))/12) >= 60) &
            (F.floor(F.months_between(F.current_date(), F.col("fec_nac"))/12) < 76), "e. 60 A 70")
        .otherwise('f. Otros')
    )

    df_base = df_base.withColumn(
        "mes_entrega_dif",
        F.floor(
            F.months_between(
                F.current_date(),
                F.col("mes_entrega")
            )
        )
    )

    df_base = df_base.withColumn(
        "seg_saldo_ppd",
        F.when(F.col("saldo_ppd") < 10000, "a. [0,10k)")
        .when((F.col("saldo_ppd") >= 10000) & (F.col("saldo_ppd") < 20000), "b. [10k,20k)")
        .when((F.col("saldo_ppd") >= 20000) & (F.col("saldo_ppd") < 30000), "c. [20k,30k)")
        .when((F.col("saldo_ppd") >= 30000) & (F.col("saldo_ppd") < 40000), "d. [30k,40k)")
        .when((F.col("saldo_ppd") >= 40000) & (F.col("saldo_ppd") < 50000), "e. [40k,50k)")
        .when((F.col("saldo_ppd") >= 50000) & (F.col("saldo_ppd") < 60000), "f. [50k,60k)")
        .when((F.col("saldo_ppd") >= 60000) & (F.col("saldo_ppd") < 70000), "g. [60k,70k)")
        .when((F.col("saldo_ppd") >= 70000) & (F.col("saldo_ppd") < 80000), "h. [70k,80k)")
        .when((F.col("saldo_ppd") >= 80000) & (F.col("saldo_ppd") < 90000), "i. [80k,90k)")
        .when(F.col("saldo_ppd") >= 90000, "j. [90k,+)")
        .otherwise("z. otros")
    )

    df_base = df_base.withColumn(
        "seg_tea_formato",
        F.when(F.col("tea_formato") < 0.10, "a. [0%,10%)")
        .when((F.col("tea_formato") >= 0.10) & (F.col("tea_formato") < 0.15), "b. [10%,15%)")
        .when((F.col("tea_formato") >= 0.15) & (F.col("tea_formato") < 0.20), "c. [15%,20%)")
        .when((F.col("tea_formato") >= 0.20) & (F.col("tea_formato") < 0.25), "d. [20%,25%)")
        .when((F.col("tea_formato") >= 0.25) & (F.col("tea_formato") < 0.30), "e. [25%,30%)")
        .when((F.col("tea_formato") >= 0.30) & (F.col("tea_formato") < 0.35), "f. [30%,35%)")
        .when((F.col("tea_formato") >= 0.35) & (F.col("tea_formato") < 0.40), "g. [35%,40%)")
        .when((F.col("tea_formato") >= 0.40) & (F.col("tea_formato") < 0.45), "h. [40%,45%)")
        .when(F.col("tea_formato") >= 0.45, "i. [45%,+)")
        .otherwise("z. otros")
    )

    df_base = df_base.withColumn(
        'edad',
        F.floor(F.months_between(F.current_date(), F.col("fec_nac")) / 12)
    )
     
    df_base = df_base.withColumn(
        "seg_tem_formato",
        F.when(F.col("tem_formato") < 0.01, "a. [0%,1%)")
        .when((F.col("tem_formato") >= 0.01) & (F.col("tem_formato") < 0.015), "b. [1%,1.5%)")
        .when((F.col("tem_formato") >= 0.015) & (F.col("tem_formato") < 0.02), "c. [1.5%,2%)")
        .when((F.col("tem_formato") >= 0.02) & (F.col("tem_formato") < 0.025), "d. [2%,2.5%)")
        .when((F.col("tem_formato") >= 0.025) & (F.col("tem_formato") < 0.03), "e. [2.5%,3%)")
        .when(F.col("tem_formato") >= 0.03, "f. [3%,+)")
        .otherwise("z. otros")
    )

    df_base = df_base.withColumn(
        "seg_tasa_app",
        F.when(F.col("tasa_app") < 0.10, "a. [0%,10%)")
        .when((F.col("tasa_app") >= 0.10) & (F.col("tasa_app") < 0.15), "b. [10%,15%)")
        .when((F.col("tasa_app") >= 0.15) & (F.col("tasa_app") < 0.20), "c. [15%,20%)")
        .when((F.col("tasa_app") >= 0.20) & (F.col("tasa_app") < 0.25), "d. [20%,25%)")
        .when((F.col("tasa_app") >= 0.25) & (F.col("tasa_app") < 0.30), "e. [25%,30%)")
        .when((F.col("tasa_app") >= 0.30) & (F.col("tasa_app") < 0.35), "f. [30%,35%)")
        .when((F.col("tasa_app") >= 0.35) & (F.col("tasa_app") < 0.40), "g. [35%,40%)")
        .when(F.col("tasa_app") >= 0.40, "h. [40%,+)")
        .otherwise("z. otros")
    )

    return df_base.withColumn(
        "vendor_lead_code",
        F.right(
            F.concat(F.lit("00000000"), F.col("vendor_lead_code")),
            F.lit(8)
        )
    )

def pool_tnumeros(spark,dni_maestra,tb_maestra,cross_list_01,tb_name_tnum,cross_columns_a1,dni_a1,fecha_ref_a1,peso_ref_a1,tb_a1,cross_columns_a2,dni_a2,fecha_ref_a2,peso_ref_a2,tb_a2,cross_columns_a3,dni_a3,fecha_ref_a3,peso_ref_a3,tb_a3,cross_columns_a4,dni_a4,fecha_ref_a4,peso_ref_a4,tb_a4,cross_columns_a5,dni_a5,fecha_ref_a5,peso_ref_a5,tb_a5):

    query = f"""
        SELECT DISTINCT vendor_lead_code,phone_number,tipo_telf,dias_antiguedad,peso
        FROM (
            SELECT 
                right('00000000'+a.{dni_maestra},8) AS vendor_lead_code,
                t.phone_number,
                t.tipo_telf,
                DATEDIFF(DAY, a.fecha_envio, GETDATE()) AS dias_antiguedad,
                1 as peso
            FROM DANTALION.dbo.{tb_maestra} a
            CROSS APPLY (
                VALUES
                    {cross_list_01}
            ) t(tipo_telf,phone_number)
            WHERE t.phone_number IS NOT NULL
            AND t.phone_number <> ''
            AND LEN(t.phone_number) = 9
            AND t.phone_number LIKE '9%'
            AND a.{dni_maestra} IS NOT NULL
            AND (a.retiro IS NULL or a.retiro='no_aplica')

            UNION

            SELECT 
                right('00000000'+a.{dni_a1},8) AS vendor_lead_code,
                t.phone_number,
                t.tipo_telf,
                DATEDIFF(DAY, a.{fecha_ref_a1}, GETDATE()) AS dias_antiguedad,
                {peso_ref_a1} as peso
            FROM DANTALION.dbo.{tb_a1} a
            CROSS APPLY (
                VALUES {cross_columns_a1}
            ) t(tipo_telf, phone_number)
            WHERE EXISTS (
                SELECT 1
                FROM DANTALION.dbo.{tb_maestra} b
                WHERE right('00000000'+b.{dni_maestra},8) = right('00000000'+a.{dni_a1},8)
            )
            AND t.phone_number IS NOT NULL
            AND t.phone_number <> ''
            AND LEN(t.phone_number) = 9
            AND t.phone_number LIKE '9%'
            AND a.{dni_a1} IS NOT NULL

            UNION

            SELECT 
                a.{dni_a2} AS vendor_lead_code,
                t.phone_number,
                t.tipo_telf,
                DATEDIFF(DAY, a.{fecha_ref_a2}, GETDATE()) AS dias_antiguedad,
                {peso_ref_a2} as peso
            FROM DANTALION.dbo.{tb_a2} a
            CROSS APPLY (
                VALUES {cross_columns_a2}
            ) t(tipo_telf, phone_number)
            WHERE EXISTS (
                SELECT 1
                FROM DANTALION.dbo.{tb_maestra} b
                WHERE right('00000000'+b.{dni_maestra},8) = right('00000000'+a.{dni_a2},8)
            )
            AND t.phone_number IS NOT NULL
            AND t.phone_number <> ''
            AND LEN(t.phone_number) = 9
            AND t.phone_number LIKE '9%'
            AND a.{dni_a2} IS NOT NULL

            UNION

            SELECT 
                a.{dni_a3} AS vendor_lead_code,
                t.phone_number,
                t.tipo_telf,
                DATEDIFF(DAY, a.{fecha_ref_a3}, GETDATE()) AS dias_antiguedad,
                {peso_ref_a3} as peso
            FROM DANTALION.dbo.{tb_a3} a
            CROSS APPLY (
                VALUES {cross_columns_a3}
            ) t(tipo_telf, phone_number)
            WHERE EXISTS (
                SELECT 1
                FROM DANTALION.dbo.{tb_maestra} b
                WHERE right('00000000'+b.{dni_maestra},8) = right('00000000'+a.{dni_a3},8)
            )
            AND t.phone_number IS NOT NULL
            AND t.phone_number <> ''
            AND LEN(t.phone_number) = 9
            AND t.phone_number LIKE '9%'
            AND a.{dni_a3} IS NOT NULL

            UNION

            SELECT 
                a.{dni_a4} AS vendor_lead_code,
                t.phone_number,
                t.tipo_telf,
                DATEDIFF(DAY, a.{fecha_ref_a4}, GETDATE()) AS dias_antiguedad,
                {peso_ref_a4} as peso
            FROM DANTALION.dbo.{tb_a4} a
            CROSS APPLY (
                VALUES {cross_columns_a4}
            ) t(tipo_telf, phone_number)
            WHERE EXISTS (
                SELECT 1
                FROM DANTALION.dbo.{tb_maestra} b
                WHERE right('00000000'+b.{dni_maestra},8) = right('00000000'+a.{dni_a4},8)
            )
            AND t.phone_number IS NOT NULL
            AND t.phone_number <> ''
            AND LEN(t.phone_number) = 9
            AND t.phone_number LIKE '9%'
            AND a.{dni_a4} IS NOT NULL

            UNION

            SELECT 
                a.{dni_a5} AS vendor_lead_code,
                t.phone_number,
                t.tipo_telf,
                DATEDIFF(DAY, a.{fecha_ref_a5}, GETDATE()) AS dias_antiguedad,
                {peso_ref_a5} as peso
            FROM DANTALION.dbo.{tb_a5} a
            CROSS APPLY (
                VALUES {cross_columns_a5}
            ) t(tipo_telf, phone_number)
            WHERE EXISTS (
                SELECT 1
                FROM DANTALION.dbo.{tb_maestra} b
                WHERE right('00000000'+b.{dni_maestra},8) = right('00000000'+a.{dni_a5},8)
            )
            AND t.phone_number IS NOT NULL
            AND t.phone_number <> ''
            AND LEN(t.phone_number) = 9
            AND t.phone_number LIKE '9%'
            AND a.{dni_a5} IS NOT NULL

        ) t
    """

    df_ref01=obtener_tabla_sql(spark,query,server_kishin,user_kishin,pwd_kishin,db_kishin)

    window_spec = Window.partitionBy("vendor_lead_code","phone_number").orderBy(F.col("peso").asc_nulls_last(),F.col("dias_antiguedad").asc_nulls_last())
    df_ref01 = df_ref01.withColumn("ref1_vici", row_number().over(window_spec))
    df_ref01 = df_ref01.filter(F.col('ref1_vici')==1).drop('ref1_vici','peso')    

    df_ref01 = df_ref01.withColumn(
        "vendor_lead_code",
        F.right(
            F.concat(F.lit("00000000"), F.col("vendor_lead_code")),
            F.lit(8)
        )
    )

    overwrite_table_SQL(spark,df_ref01,f'{tb_name_tnum}',server_kishin,user_kishin,pwd_kishin,'DANTALION')

def pool_tnumeros_bancos(spark,dni_maestra,tb_maestra,cross_list_01,tb_name_tnum):

    query = f"""
        SELECT 
            right('00000000'+a.{dni_maestra},8) AS vendor_lead_code,
            t.phone_number,
            t.tipo_telf,
            DATEDIFF(DAY, a.fecha_envio, GETDATE()) AS dias_antiguedad,
            1 as peso
        FROM DANTALION.dbo.{tb_maestra} a
        CROSS APPLY (
            VALUES
                {cross_list_01}
        ) t(tipo_telf,phone_number)
        WHERE t.phone_number IS NOT NULL
        AND t.phone_number <> ''
        AND LEN(t.phone_number) = 9
        AND t.phone_number LIKE '9%'
        AND a.{dni_maestra} IS NOT NULL
        AND (a.retiro IS NULL or a.retiro='no_aplica')
    """

    df_ref01=obtener_tabla_sql(spark,query,server_kishin,user_kishin,pwd_kishin,db_kishin)

    window_spec = Window.partitionBy("vendor_lead_code","phone_number").orderBy(F.col("peso").asc_nulls_last(),F.col("dias_antiguedad").asc_nulls_last())
    df_ref01 = df_ref01.withColumn("ref1_vici", row_number().over(window_spec))
    df_ref01 = df_ref01.filter(F.col('ref1_vici')==1).drop('ref1_vici','peso')    

    df_ref01 = df_ref01.withColumn(
        "vendor_lead_code",
        F.right(
            F.concat(F.lit("00000000"), F.col("vendor_lead_code")),
            F.lit(8)
        )
    )

    overwrite_table_SQL(spark,df_ref01,f'{tb_name_tnum}',server_kishin,user_kishin,pwd_kishin,'DANTALION')

def pool_tnumeros_valentina(spark,dni_valentina,tb_Numero,tb_name_tnum):

    cross_columns_a1=cross_columns_int
    dni_a1=dni_int
    fecha_ref_a1=fecha_ref_int
    peso_ref_a1=peso_int
    tb_a1=tb_int

    cross_columns_a2=cross_columns_web
    dni_a2=dni_web
    fecha_ref_a2=fecha_ref_web
    peso_ref_a2=peso_web
    tb_a2=tb_web

    cross_columns_a3=cross_columns_hu
    dni_a3=dni_hu
    fecha_ref_a3=fecha_ref_hu
    peso_ref_a3=peso_hu
    tb_a3=tb_hu

    cross_columns_a4=cross_columns_epc
    dni_a4=dni_epc
    fecha_ref_a4=fecha_ref_epc
    peso_ref_a4=peso_epc
    tb_a4=tb_epc

    cross_columns_a5=cross_columns_etk
    dni_a5=dni_etk
    fecha_ref_a5=fecha_ref_etk
    peso_ref_a5=peso_etk
    tb_a5=tb_etk

    query = f"""
        SELECT DISTINCT dni_cliente,contacto,tipo_telf,dias_antiguedad,peso
        FROM (
            SELECT dni_cliente,contacto,tipo_telf
            ,0 as dias_antiguedad
            ,1 as peso
            from DANTALION.dbo.{tb_Numero}  
            where dni_cliente is not null
            
            UNION

            SELECT 
                right('00000000'+a.{dni_a1},8) AS dni_cliente,
                t.contacto,
                t.tipo_telf,
                DATEDIFF(DAY, a.{fecha_ref_a1}, GETDATE()) AS dias_antiguedad,
                {peso_ref_a1} as peso
            FROM DANTALION.dbo.{tb_a1} a
            CROSS APPLY (
                VALUES {cross_columns_a1}
            ) t(tipo_telf, contacto)
            WHERE EXISTS (
                SELECT 1
                FROM DANTALION.dbo.{tb_Numero} b
                WHERE right('00000000'+b.{dni_valentina},8) = right('00000000'+a.{dni_a1},8)
            )
            AND t.contacto IS NOT NULL
            AND t.contacto <> ''
            AND LEN(t.contacto) = 9
            AND t.contacto LIKE '9%'
            AND a.{dni_a1} IS NOT NULL

            UNION

            SELECT 
                a.{dni_a2} AS dni_cliente,
                t.contacto,
                t.tipo_telf,
                DATEDIFF(DAY, a.{fecha_ref_a2}, GETDATE()) AS dias_antiguedad,
                {peso_ref_a2} as peso
            FROM DANTALION.dbo.{tb_a2} a
            CROSS APPLY (
                VALUES {cross_columns_a2}
            ) t(tipo_telf, contacto)
            WHERE EXISTS (
                SELECT 1
                FROM DANTALION.dbo.{tb_Numero} b
                WHERE right('00000000'+b.{dni_valentina},8) = right('00000000'+a.{dni_a2},8)
            )
            AND t.contacto IS NOT NULL
            AND t.contacto <> ''
            AND LEN(t.contacto) = 9
            AND t.contacto LIKE '9%'
            AND a.{dni_a2} IS NOT NULL

            UNION

            SELECT 
                a.{dni_a3} AS dni_cliente,
                t.contacto,
                t.tipo_telf,
                DATEDIFF(DAY, a.{fecha_ref_a3}, GETDATE()) AS dias_antiguedad,
                {peso_ref_a3} as peso
            FROM DANTALION.dbo.{tb_a3} a
            CROSS APPLY (
                VALUES {cross_columns_a3}
            ) t(tipo_telf, contacto)
            WHERE EXISTS (
                SELECT 1
                FROM DANTALION.dbo.{tb_Numero} b
                WHERE right('00000000'+b.{dni_valentina},8) = right('00000000'+a.{dni_a3},8)
            )
            AND t.contacto IS NOT NULL
            AND t.contacto <> ''
            AND LEN(t.contacto) = 9
            AND t.contacto LIKE '9%'
            AND a.{dni_a3} IS NOT NULL

            UNION

            SELECT 
                a.{dni_a4} AS dni_cliente,
                t.contacto,
                t.tipo_telf,
                DATEDIFF(DAY, a.{fecha_ref_a4}, GETDATE()) AS dias_antiguedad,
                {peso_ref_a4} as peso
            FROM DANTALION.dbo.{tb_a4} a
            CROSS APPLY (
                VALUES {cross_columns_a4}
            ) t(tipo_telf, contacto)
            WHERE EXISTS (
                SELECT 1
                FROM DANTALION.dbo.{tb_Numero} b
                WHERE right('00000000'+b.{dni_valentina},8) = right('00000000'+a.{dni_a4},8)
            )
            AND t.contacto IS NOT NULL
            AND t.contacto <> ''
            AND LEN(t.contacto) = 9
            AND t.contacto LIKE '9%'
            AND a.{dni_a4} IS NOT NULL

            UNION

            SELECT 
                a.{dni_a5} AS dni_cliente,
                t.contacto,
                t.tipo_telf,
                DATEDIFF(DAY, a.{fecha_ref_a5}, GETDATE()) AS dias_antiguedad,
                {peso_ref_a5} as peso
            FROM DANTALION.dbo.{tb_a5} a
            CROSS APPLY (
                VALUES {cross_columns_a5}
            ) t(tipo_telf, contacto)
            WHERE EXISTS (
                SELECT 1
                FROM DANTALION.dbo.{tb_Numero} b
                WHERE right('00000000'+b.{dni_valentina},8) = right('00000000'+a.{dni_a5},8)
            )
            AND t.contacto IS NOT NULL
            AND t.contacto <> ''
            AND LEN(t.contacto) = 9
            AND t.contacto LIKE '9%'
            AND a.{dni_a5} IS NOT NULL

        ) t
    """

    df_ref01=obtener_tabla_sql(spark,query,server_kishin,user_kishin,pwd_kishin,db_kishin)

    window_spec = Window.partitionBy("dni_cliente","contacto").orderBy(F.col("peso").asc_nulls_last(),F.col("dias_antiguedad").asc_nulls_last())
    df_ref01 = df_ref01.withColumn("ref1_vici", row_number().over(window_spec))
    df_ref01 = df_ref01.filter(F.col('ref1_vici')==1).drop('ref1_vici','peso')    

    df_ref01 = df_ref01.withColumn(
        "dni_cliente",
        F.right(
            F.concat(F.lit("00000000"), F.col("dni_cliente")),
            F.lit(8)
        )
    )
    overwrite_table_SQL(spark,df_ref01,f'{tb_name_tnum}',server_kishin,user_kishin,pwd_kishin,db_kishin)



def since_base_maestra_alfin(spark,fecha_mes_base):
    cl_base1 = fecha_a_nombre(fecha_mes_base)

    query = f"""
        select
            numero_documento as dni_cliente,
            cl_id,
            '12' as id_servicio,
            CONCAT(isnull(nombres,''),' ' ,isnull(apellido_paterno,''),' ',isnull(apellido_materno,'')) AS address1,
            CONCAT(isnull(nombres,''),',' ,isnull(apellido_paterno,'')) AS nombre_add,
            cast(Oferta_max as int) AS Oferta_max,
            distrito,
            departamento,
            provincia,
            lote,
            proveedor,
            nuevos_6m, nuevos_9m, nuevos_12m, nuevos_3m, nuevos_4m,  promocion,
            rango_sueldo,tipo_cliente_riegos,color,color_final,edad,grupo_tasa,
            CASE
                    WHEN grupo_monto = 'None' THEN NULL
                    ELSE grupo_monto 
                END AS grupo_monto,
            nombre_prioridad,perfil_ro,tipo_cliente,frescura,region_comercial,cliente_nuevo,
            campana,
            campania,
            tienda,
            plazo,
            case
                when cl_estado>1  then estado
                when cl_estado is null  then 'revisar'
                else 'no_aplica'
            end as retiro ,            
            propension,
            propension_ic,flg_deuda_plus,tipo_base,user_v3,nombre_base,prioridad,
            agencia_comercial,cl_carga,
            ISNULL(flg_aahh,'') AS flg_aahh,
            ISNULL(score_telefono,'') AS score_telefono,
            intensidad_max
        FROM VALENTINA.dbo.alfin_clientes
        where cl_base='{cl_base1}'
        """
    df_base=obtener_tabla_sql(spark,query,server_sa,user_sa,pwd_sa,db_sa)

    # query = f"""
    #         SELECT DISTINCT dni_cliente FROM valentina.dbo.tb_retirogestion_blacklist
    #     """
    # df_retiro_dni_contacto=obtener_tabla_sql(spark,query,server_sa,user_sa,pwd_sa,db_sa)
    # df_base=df_base.join(df_retiro_dni_contacto,['dni_cliente'],'leftanti')
    #     query = f"""
    # SELECT DISTINCT dni_cliente FROM valentina.dbo.tb_retirogestion_blacklist
    #         """
    #     df_retiro_dni_contacto=obtener_tabla_sql(spark,query,server_sa,user_sa,pwd_sa,db_sa)

    df_base= df_base.withColumn(
        "seg_oferta",
            F.when((F.col("Oferta_max") >= 1000) & (F.col("Oferta_max") <= 2000), "A. 1000 A 2000")
            .when((F.col("Oferta_max") > 2000) & (F.col("Oferta_max") <= 3000), "B. 2000 A 3000")
            .when((F.col("Oferta_max") > 3000) & (F.col("Oferta_max") <= 4000), "C. 3000 A 4000")
            .when((F.col("Oferta_max") > 4000) & (F.col("Oferta_max") <= 5000), "D. 4000 A 5000")
            .when((F.col("Oferta_max") > 5000) & (F.col("Oferta_max") <= 7000), "E. 5000 A 7000")
            .when((F.col("Oferta_max") > 7000) & (F.col("Oferta_max") <= 10000), "F. 7000 A 10000")
            .when((F.col("Oferta_max") > 10000) & (F.col("Oferta_max") <= 15000), "G. 10000 A 15000")
            .when((F.col("Oferta_max") > 15000) & (F.col("Oferta_max") <= 20000), "H. 15000 A 20000")
            .when((F.col("Oferta_max") > 20000) & (F.col("Oferta_max") <= 30000), "I. 20000 A 30000")
            .when(F.col("Oferta_max") > 30000, "J. 30000 A MÁS")
        )
    return df_base.withColumn(
        "seg_edad",
            F.when((F.col("edad") >= 20) & (F.col("edad") <= 30), "A. 20 A 30")
            .when((F.col("edad") > 30) & (F.col("edad") <= 40), "B. 30 A 40")
            .when((F.col("edad") > 40) & (F.col("edad") <= 50), "C. 40 A 50")
            .when((F.col("edad") > 50) & (F.col("edad") <= 60), "D. 50 A 60")
            .when((F.col("edad") > 60) & (F.col("edad") <= 70), "E. 60 A 70")
            .when(F.col("edad") > 70, "F. 70 A MÁS")
            .otherwise("G. OTROS")
        )

    # WHERE CAST(cl_carga AS DATE) >= CAST('{fecha_mes_base}' AS DATE)
    # AND CAST(cl_carga AS DATE) < DATEADD(MONTH, 1, CAST('{fecha_mes_base}' AS DATE))

def since_base_maestra_alfcc(spark,fecha_mes_base):
    cl_base1 = fecha_a_nombre(fecha_mes_base)

    query = f"""
        select   
            numero_documento as dni_cliente,
            cl_id,
            '17' as id_servicio,
            CONCAT(isnull(nombre,''),',') AS nombre_add,
            cast(oferta as int) as oferta_max,
            cast(oferta_electro as decimal(18,2)) as oferta_electro,
            cast(tasa_electro as float) as tasa_electro,
            cast(plazo_electro as int) as plazo_electro,
            cast(cme_electro as float) as cme_electro,
            cast(oferta_credicash as decimal(18,2)) as oferta_credicash,
            cast(tasa_credicash as float) as tasa_credicash,
            cast(plazo_credicash as int) as plazo_credicash,
            cast(cme_credicash as float) as cme_credicash,
            perfil,
            plazo,
            tasa,
            cme_disponible,
            lote,
            proveedor,
            msi,
            flg_credicash,
            propension,
            campana,
            cl_carga,
            obs_fcc,
            semaforo,
            region,
            tienda_ir,
            situacion_laboral,
            score_telefono,
            marca_pd,
            grupo_segmento,
            producto_externo,
            propension_electro,
            propension_credicash,
            flg_cruce_recurrente,
            flg_cruce as alta_demanda,
            demanda,
            prioridad,
            inicio,
            case
                when cl_estado>1  then 'retiro'
                when cl_estado is null  then 'revisar'
                else 'no_aplica'
            end as retiro ,           
            fin,
            producto_interno,
            case   
                when cast(flg_aahh as int) = 0 then 'no_emergente'
                when cast(flg_aahh as int) = 1 then 'emergente'
            end as flg_aahh,
            intensidad_max,
            frescura
        from valentina.dbo.alfcc_clientes 
        where cl_base='{cl_base1}'
        """
    df_base=obtener_tabla_sql(spark,query,server_sa,user_sa,pwd_sa,db_sa)

    df_base= df_base.withColumn(
        "seg_oferta",
            F.when((F.col("Oferta_max") >= 1000) & (F.col("Oferta_max") <= 2000), "A. 1000 A 2000")
            .when((F.col("Oferta_max") > 2000) & (F.col("Oferta_max") <= 3000), "B. 2000 A 3000")
            .when((F.col("Oferta_max") > 3000) & (F.col("Oferta_max") <= 4000), "C. 3000 A 4000")
            .when((F.col("Oferta_max") > 4000) & (F.col("Oferta_max") <= 5000), "D. 4000 A 5000")
            .when((F.col("Oferta_max") > 5000) & (F.col("Oferta_max") <= 7000), "E. 5000 A 7000")
            .when((F.col("Oferta_max") > 7000) & (F.col("Oferta_max") <= 10000), "F. 7000 A 10000")
            .when((F.col("Oferta_max") > 10000) & (F.col("Oferta_max") <= 15000), "G. 10000 A 15000")
            .when((F.col("Oferta_max") > 15000) & (F.col("Oferta_max") <= 20000), "H. 15000 A 20000")
            .when((F.col("Oferta_max") > 20000) & (F.col("Oferta_max") <= 30000), "I. 20000 A 30000")
            .when(F.col("Oferta_max") > 30000, "J. 30000 A MÁS")
        )
    df_base= df_base.withColumn(
        "seg_oferta_credicash",
            F.when((F.col("oferta_credicash") >= 1000) & (F.col("oferta_credicash") <= 2000), "A. 1000 A 2000")
            .when((F.col("oferta_credicash") > 2000) & (F.col("oferta_credicash") <= 3000), "B. 2000 A 3000")
            .when((F.col("oferta_credicash") > 3000) & (F.col("oferta_credicash") <= 4000), "C. 3000 A 4000")
            .when((F.col("oferta_credicash") > 4000) & (F.col("oferta_credicash") <= 5000), "D. 4000 A 5000")
            .when((F.col("oferta_credicash") > 5000) & (F.col("oferta_credicash") <= 7000), "E. 5000 A 7000")
            .when((F.col("oferta_credicash") > 7000) & (F.col("oferta_credicash") <= 10000), "F. 7000 A 10000")
            .when((F.col("oferta_credicash") > 10000) & (F.col("oferta_credicash") <= 15000), "G. 10000 A 15000")
            .when((F.col("oferta_credicash") > 15000) & (F.col("oferta_credicash") <= 20000), "H. 15000 A 20000")
            .when((F.col("oferta_credicash") > 20000) & (F.col("oferta_credicash") <= 30000), "I. 20000 A 30000")
            .when(F.col("oferta_credicash") > 30000, "J. 30000 A MÁS")
        )

    df_base= df_base.withColumn(
        "seg_oferta_electro",
            F.when((F.col("oferta_electro") >= 1000) & (F.col("oferta_electro") <= 2000), "A. 1000 A 2000")
            .when((F.col("oferta_electro") > 2000) & (F.col("oferta_electro") <= 3000), "B. 2000 A 3000")
            .when((F.col("oferta_electro") > 3000) & (F.col("oferta_electro") <= 4000), "C. 3000 A 4000")
            .when((F.col("oferta_electro") > 4000) & (F.col("oferta_electro") <= 5000), "D. 4000 A 5000")
            .when((F.col("oferta_electro") > 5000) & (F.col("oferta_electro") <= 7000), "E. 5000 A 7000")
            .when((F.col("oferta_electro") > 7000) & (F.col("oferta_electro") <= 10000), "F. 7000 A 10000")
            .when((F.col("oferta_electro") > 10000) & (F.col("oferta_electro") <= 15000), "G. 10000 A 15000")
            .when((F.col("oferta_electro") > 15000) & (F.col("oferta_electro") <= 20000), "H. 15000 A 20000")
            .when((F.col("oferta_electro") > 20000) & (F.col("oferta_electro") <= 30000), "I. 20000 A 30000")
            .when(F.col("oferta_electro") > 30000, "J. 30000 A MÁS")
        )

    df_base = df_base.withColumn(
        "seg_tasa_credicash",
        F.when(F.col("tasa_credicash") < 20, "a. [0,20%)")
        .when((F.col("tasa_credicash") >= 20) & (F.col("tasa_credicash") < 30), "b. [20%,30%)")
        .when((F.col("tasa_credicash") >= 30) & (F.col("tasa_credicash") < 40), "c. [30%,40%)")
        .when((F.col("tasa_credicash") >= 40) & (F.col("tasa_credicash") < 50), "d. [40%,50%)")
        .when((F.col("tasa_credicash") >= 50) & (F.col("tasa_credicash") < 60), "e. [50%,60%)")
        .when((F.col("tasa_credicash") >= 60) & (F.col("tasa_credicash") < 70), "f. [60%,70%)")
        .when((F.col("tasa_credicash") >= 70) & (F.col("tasa_credicash") < 80), "g. [70%,80%)")
        .when(F.col("tasa_credicash") >= 80, "h. [80%,+)")
        .otherwise("z. otros")
    )

    df_base = df_base.withColumn(
        "seg_tasa_electro",
        F.when(F.col("tasa_electro") < 20, "a. [0,20%)")
        .when((F.col("tasa_electro") >= 20) & (F.col("tasa_electro") < 30), "b. [20%,30%)")
        .when((F.col("tasa_electro") >= 30) & (F.col("tasa_electro") < 40), "c. [30%,40%)")
        .when((F.col("tasa_electro") >= 40) & (F.col("tasa_electro") < 50), "d. [40%,50%)")
        .when((F.col("tasa_electro") >= 50) & (F.col("tasa_electro") < 60), "e. [50%,60%)")
        .when((F.col("tasa_electro") >= 60) & (F.col("tasa_electro") < 70), "f. [60%,70%)")
        .when((F.col("tasa_electro") >= 70) & (F.col("tasa_electro") < 80), "g. [70%,80%)")
        .when(F.col("tasa_electro") >= 80, "h. [80%,+)")
        .otherwise("z. otros")
    )


    df_base= df_base.withColumn(
        "seg_cme_electro",
            F.when((F.col("cme_electro") >= 1000) & (F.col("cme_electro") <= 2000), "A. 1000 A 2000")
            .when((F.col("cme_electro") > 2000) & (F.col("cme_electro") <= 3000), "B. 2000 A 3000")
            .when((F.col("cme_electro") > 3000) & (F.col("cme_electro") <= 4000), "C. 3000 A 4000")
            .when((F.col("cme_electro") > 4000) & (F.col("cme_electro") <= 5000), "D. 4000 A 5000")
            .when((F.col("cme_electro") > 5000) & (F.col("cme_electro") <= 7000), "E. 5000 A 7000")
            .when((F.col("cme_electro") > 7000) & (F.col("cme_electro") <= 10000), "F. 7000 A 10000")
            .when((F.col("cme_electro") > 10000) & (F.col("cme_electro") <= 15000), "G. 10000 A 15000")
            .when((F.col("cme_electro") > 15000) & (F.col("cme_electro") <= 20000), "H. 15000 A 20000")
            .when((F.col("cme_electro") > 20000) & (F.col("cme_electro") <= 30000), "I. 20000 A 30000")
            .when(F.col("cme_electro") > 30000, "J. 30000 A MÁS")
        )
    df_base= df_base.withColumn(
        "seg_cme_credicash",
            F.when((F.col("cme_credicash") >= 1000) & (F.col("cme_credicash") <= 2000), "A. 1000 A 2000")
            .when((F.col("cme_credicash") > 2000) & (F.col("cme_credicash") <= 3000), "B. 2000 A 3000")
            .when((F.col("cme_credicash") > 3000) & (F.col("cme_credicash") <= 4000), "C. 3000 A 4000")
            .when((F.col("cme_credicash") > 4000) & (F.col("cme_credicash") <= 5000), "D. 4000 A 5000")
            .when((F.col("cme_credicash") > 5000) & (F.col("cme_credicash") <= 7000), "E. 5000 A 7000")
            .when((F.col("cme_credicash") > 7000) & (F.col("cme_credicash") <= 10000), "F. 7000 A 10000")
            .when((F.col("cme_credicash") > 10000) & (F.col("cme_credicash") <= 15000), "G. 10000 A 15000")
            .when((F.col("cme_credicash") > 15000) & (F.col("cme_credicash") <= 20000), "H. 15000 A 20000")
            .when((F.col("cme_credicash") > 20000) & (F.col("cme_credicash") <= 30000), "I. 20000 A 30000")
            .when(F.col("cme_credicash") > 30000, "J. 30000 A MÁS")
        )    
    return df_base

def since_tnumeros(spark,tb_name,dni_1):
    query = f"""
        select 
        ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS indice_num
        ,Telefonos as phone_number,{dni_1} as vendor_lead_code,REPLACE(LTRIM(RTRIM(Tipo_Telf)),'  ',' ')  as tipo_telf from DANTALION.dbo.{tb_name}
        """
    df_tnumer=obtener_tabla_sql(spark,query,server_kishin,user_kishin,pwd_kishin,db_kishin)
    window_spec = Window.partitionBy("vendor_lead_code",'phone_number').orderBy(col("vendor_lead_code"))
    df_tnumer = df_tnumer.withColumn("ref_02", row_number().over(window_spec))

    df_tnumer = df_tnumer.withColumn(
        "tipo_telf",
        F.trim(F.regexp_replace("tipo_telf", r"\s+", " "))
    )
    df_tnumer = df_tnumer.withColumn(
            "vendor_lead_code",
            F.right(
                F.concat(F.lit("00000000"), F.col("vendor_lead_code")),
                F.lit(8)
            )
        )

    return  df_tnumer.filter(col("ref_02") == 1).drop('ref_02')

def since_tnumeros_valentina(spark,tb_valentina_cliente,fecha_mes_base):
    cl_base1 = fecha_a_nombre(fecha_mes_base)
    query = f"""
        SELECT 
        a.NUMERO_DOCUMENTO as dni_cliente,
        t.contacto,
        t.tipo_telf,
        a.cl_carga
        FROM VALENTINA.dbo.{tb_valentina_cliente} a
        CROSS APPLY (
        VALUES
            (a.cl_telf1, 'cel01'),
            (a.cl_telf2, 'cel02'),
            (a.cl_telf3, 'cel03'),
            (a.cl_telf4, 'cel04'),
            (a.cl_telf5, 'cel05'),
            (a.cl_telf6, 'cel06'),
            (a.cl_telf7, 'cel07'),
            (a.cl_telf8, 'cel08'),
            (a.cl_telf9, 'cel09'),
            (a.cl_telf10, 'cel10')
        ) t(contacto, tipo_telf)
        WHERE 
        t.contacto IS NOT NULL
        AND t.contacto <> ''
        AND LEN(t.contacto) = 9
        AND t.contacto LIKE '9%'
        and cl_base='{cl_base1}'
        """
    df_tnumero=obtener_tabla_sql(spark,query,server_sa,user_sa,pwd_sa,db_sa)

    window_spec = Window.partitionBy("dni_cliente").orderBy(F.col("cl_carga").desc_nulls_last())
    df_tnumero = df_tnumero.withColumn("ref_01", row_number().over(window_spec))
    return df_tnumero.filter(F.col("ref_01") == 1).drop('ref_01','cl_carga')

# def update_tnumeros_valentina(spark,tb_valentina_cliente,fecha_mes_base,tb_tnumero):
#     cl_base1 = fecha_a_nombre(fecha_mes_base)
#     query = f"""
#         SELECT 
#             a.NUMERO_DOCUMENTO as dni_cliente,
#             t.contacto,
#             t.tipo_telf,
#             a.cl_carga
#         FROM VALENTINA.dbo.{tb_valentina_cliente} a

#         OUTER APPLY (
#             VALUES
#                 (a.cl_telf1, 'cel01'),
#                 (a.cl_telf2, 'cel02'),
#                 (a.cl_telf3, 'cel03'),
#                 (a.cl_telf4, 'cel04'),
#                 (a.cl_telf5, 'cel05'),
#                 (a.cl_telf6, 'cel06'),
#                 (a.cl_telf7, 'cel07'),
#                 (a.cl_telf8, 'cel08'),
#                 (a.cl_telf9, 'cel09'),
#                 (a.cl_telf10, 'cel10'),
#                 (a.cl_movil, 'cel11'),
#                 (a.cl_celular, 'cel12'),
#                 (a.cl_telefono, 'cel13')
#         ) t(contacto, tipo_telf)

#         WHERE 
#             a.cl_base = '{cl_base1}'
#             AND (
#                 (
#                     t.contacto IS NOT NULL
#                     AND t.contacto <> ''
#                     AND LEN(t.contacto) = 9
#                     AND t.contacto LIKE '9%'
#                 )
#                 OR t.contacto IS NULL   
#             )
        
#         """
#     df_tnumero=obtener_tabla_sql(spark,query,server_sa,user_sa,pwd_sa,db_sa)

#     window_spec = Window.partitionBy("dni_cliente").orderBy(F.col("contacto").desc_nulls_last())
#     df_tnumero = df_tnumero.withColumn("ref_01", row_number().over(window_spec))
#     df_tnumero=df_tnumero.filter(F.col("ref_01") == 1).drop('ref_01','cl_carga')

#     overwrite_table_SQL(spark,df_tnumero,tb_tnumero,server_kishin,user_kishin,pwd_kishin,db_kishin)



# generar listas

def lista_generada(spark,fecha_mes_base,tipi_cond1,tipi_cond2,tipi_cond3,tb_tipolofia,servidor_01,tipi_cod,tipi_resp_cod,tipi_descrip,tipi_estado,tipi_resp_estado,tipi_subdescripcion,tnum_tb,tnum_dni,tlista_generada,get_base):

    df_vicidial=since_vicidial(spark,fecha_mes_base,tipi_cond1,tipi_cond2,tipi_cond3,tb_tipolofia,servidor_01,tipi_cod,tipi_resp_cod,tipi_descrip,tipi_estado,tipi_resp_estado,tnum_tb,tnum_dni)
    
    # window_spec = Window.partitionBy("title","vendor_lead_code","phone_number").orderBy(F.col("fecha_hora_llamada").desc_nulls_last())
    window_spec = Window.partitionBy("vendor_lead_code","phone_number").orderBy(F.col("fecha_hora_llamada").desc_nulls_last())
    df_vicidial = df_vicidial.withColumn("ref1_vici", row_number().over(window_spec))
    df_vicidial = df_vicidial.filter(F.col('ref1_vici')==1).drop('ref1_vici')
    
    df_vicidial = df_vicidial.select('vendor_lead_code', 'phone_number', 'call_result', 'tramo', 'descripcion', 'peso', 'mejor_codigo_cli', 'ult_codigo_cli', 'ult_call_result', 'fecha_llamada_1', 'fecha_hora_llamada_1', 'q_intentos', 'mejor_codigo_telf', 'q_intentos_telf', 'q_intentos_dia_1','duracion')

    mapping = {
        "q_intentos_dia_1": "q_intentos_dia",
        "fecha_llamada_1": "fecha_llamada",
        "fecha_hora_llamada_1": "fecha_hora_llamada"
    }

    df_vicidial = df_vicidial.toDF(*[
        mapping.get(col, col) for col in df_vicidial.columns
    ])

    df_base_vigente = get_base(spark)
    df_tnumeric=since_tnumeros(spark,tnum_tb,tnum_dni)

    dic_telf = {v: i+1 for i, v in enumerate(listas_tnumeros)}

    mapping_expr = F.create_map([F.lit(x) for x in chain(*dic_telf.items())])

    df_tnumeric = df_tnumeric.withColumn(
        "indice_tpo_telf",
        mapping_expr[F.col("tipo_telf")]
    )
        
    df_lista=df_tnumeric.join(df_base_vigente,['vendor_lead_code'],'left')
    df_lista=df_lista.join(df_vicidial,['vendor_lead_code','phone_number'],'left')
    
    window_spec = Window.orderBy("vendor_lead_code")

    df_lista = df_lista.withColumn(
        "dni_unico",
        dense_rank().over(window_spec)
    )

    query = f"""
        SELECT {tipi_cod} as mejor_codigo_cli ,{tipi_estado} AS mejor_estado_tipi_cli
        , case
            when {tipi_cod}='CALLBK' then 'VOLVER A LLAMAR - call'
            else {tipi_descrip} 
        end as mejor_descripcion_cli 
        , case
            when {tipi_cod}='CALLBK' then 'AGENDAMIENTO - call'
            else {tipi_subdescripcion}
        end as mejor_sub_descripcion
        ,case 
            when {tipi_cod}='CALLBK' then 1200
            else PESO 
        end as mejor_peso_cli  
        FROM ODIN.dbo.{tb_tipolofia}
        where LEFT({tipi_cod},1)='{tipi_resp_cod}' or {tipi_estado}='{tipi_resp_estado}' or {tipi_cod}='CALLBK'
        """
    df_tipi1=obtener_tabla_sql(spark,query,server_zeus,user_zeus,pwd_zeus,db_zeus)

    df_tipi2 = df_tipi1.selectExpr(
        "mejor_codigo_cli as ult_codigo_cli",
        "mejor_estado_tipi_cli as ult_estado_tipi_cli",
        "mejor_descripcion_cli as ult_descripcion_cli",
        "mejor_sub_descripcion as ult_sub_descripcion_cli",
        "mejor_peso_cli as ult_peso_cli"
    )

    df_tipi3 = df_tipi1.selectExpr(
        "mejor_codigo_cli as mejor_codigo_telf",
        "mejor_estado_tipi_cli as mejor_estado_tipi_telf",
        "mejor_descripcion_cli as mejor_descripcion_telf",
        "mejor_sub_descripcion as mejor_sub_descripcion_telf",
        "mejor_peso_cli as mejor_peso_telf"
    )

    df_lista=df_lista.join(df_tipi1,['mejor_codigo_cli'],'left')
    df_lista=df_lista.join(df_tipi2,['ult_codigo_cli'],'left')
    df_lista=df_lista.join(df_tipi3,['mejor_codigo_telf'],'left')


    window_spec = Window.partitionBy("title",'vendor_lead_code','phone_number').orderBy(col("fecha_llamada").desc())
    # window_spec = Window.partitionBy('vendor_lead_code','phone_number').orderBy(col("fecha_llamada").desc())
    df_lista = df_lista.withColumn("ref_01", row_number().over(window_spec))
    df_lista = df_lista.filter(col("ref_01") == 1).drop('ref_01')

    df_lista = df_lista.withColumn(
        'mejor_descripcion_cli',
        F.when(F.col('mejor_codigo_cli') == 'INCALL', 'EN LLAMADA')
        .when((F.col('mejor_codigo_cli') == 'DCMX') & (F.col('duracion') > 15), 'NO TIPIFICO A TIEMPO')
        .when(F.col('mejor_codigo_cli') == 'DCMX', 'LINEA SATURADA')
        .otherwise(F.col('mejor_descripcion_cli'))
    )

    df_lista = df_lista.withColumn(
        'ult_descripcion_cli',
        F.when(F.col('ult_codigo_cli') == 'INCALL', 'EN LLAMADA')
        .when((F.col('ult_codigo_cli') == 'DCMX') & (F.col('duracion') > 15), 'NO TIPIFICO A TIEMPO')
        .when(F.col('ult_codigo_cli') == 'DCMX', 'LINEA SATURADA')
        .otherwise(F.col('ult_descripcion_cli'))
    )

    df_lista = df_lista.withColumn(
        'mejor_descripcion_telf',
        F.when(F.col('mejor_codigo_telf') == 'INCALL', 'EN LLAMADA')
        .when((F.col('mejor_codigo_telf') == 'DCMX') & (F.col('duracion') > 15), 'NO TIPIFICO A TIEMPO')
        .when(F.col('mejor_codigo_telf') == 'DCMX', 'LINEA SATURADA')
        .otherwise(F.col('mejor_descripcion_telf'))
    ).drop('duracion')

    cols_prioridad = [
        'vendor_lead_code', 'phone_number', 'title',
        'first_name', 'last_name', 'address1', 'address2', 'address3',
        'city', 'province', 'email', 'security_phrase', 'comments'
    ]

    cols_existentes = [c for c in cols_prioridad if c in df_lista.columns]

    cols_restantes = [c for c in df_lista.columns if c not in cols_existentes]

    df_lista = df_lista.select(cols_existentes + cols_restantes)
    df_lista = df_lista.toDF(*[
        re.sub(r'[^a-zA-Z0-9_]', '', c)
        for c in df_lista.columns
    ])
    overwrite_table_SQL(spark,df_lista,f'{tlista_generada}',server_sa,user_sa,pwd_sa,'CRONOX')
    print(f'tabla {tlista_generada} actualizada')

def lista_generada_valentina(spark,fecha_mes_base,ls_una_vez,ls_casilla,ls_ocupado,tlista_generada,tb_tipolofia,tb_gestiones,tb_cliente,get_base,tb_tnumero):

    df_vicidial=since_valentina(spark,fecha_mes_base,tb_tipolofia,tb_gestiones)
    df_base_vigente = get_base(spark,fecha_mes_base)
    df_base_vigente = df_base_vigente.withColumn(
        "dni_cliente",
        F.right(
            F.concat(F.lit("00000000"), F.col("dni_cliente")),
            F.lit(8)
        )
    )

    # query = f""" 
    #     SELECT dni_cliente, contacto, tipo_telf from DANTALION.dbo.{tb_tnumero}
    # """
    # df_tnumeric=obtener_tabla_sql(spark,query,server_kishin,user_kishin,pwd_kishin,db_kishin)
    df_tnumeric=since_tnumeros_valentina(spark,tb_cliente,fecha_mes_base)
    df_tnumeric = df_tnumeric.withColumn(
        "dni_cliente",
        F.right(
            F.concat(F.lit("00000000"), F.col("dni_cliente")),
            F.lit(8)
        )
    )

    df_mejor_telf = df_vicidial.filter(
        (F.col("duracion").isNotNull()) &
        (F.col("dni_cliente").isNotNull())
    ).select('dni_cliente','contacto','fecha_llamada','peso','codigo','cod_attempt','q_intentos_telef')

    window_spec = Window.partitionBy("dni_cliente","contacto").orderBy(F.col("peso").asc_nulls_last(),F.col("fecha_llamada").desc())
    df_mejor_telf = df_mejor_telf.withColumn("n15_mejor_tel", row_number().over(window_spec))

    window_part = Window.partitionBy("dni_cliente","contacto")

    df_mejor_telf = df_mejor_telf.withColumn(
        "mejor15_codigo_telf",
        F.max(
            F.when(F.col("n15_mejor_tel") == 1, F.col("codigo"))
        ).over(window_part)
    )

    window_spec = Window.partitionBy("dni_cliente").orderBy(F.col("peso").asc_nulls_last())
    df_mejor_telf = df_mejor_telf.withColumn("n15_mejor_cli", row_number().over(window_spec))

    window_part = Window.partitionBy("dni_cliente")

    df_mejor_telf = df_mejor_telf.withColumn(
        "mejor15_codigo_cli",
        F.max(
            F.when(F.col("n15_mejor_cli") == 1, F.col("codigo"))
        ).over(window_part)
    )

    window_spec = Window.partitionBy("dni_cliente",'contacto').orderBy(F.col("n15_mejor_tel").asc_nulls_last())
    df_mejor_telf = df_mejor_telf.withColumn("re", row_number().over(window_spec))
    df_mejor_telf=df_mejor_telf.filter(F.col('re')==1)
    df_mejor_telf=df_mejor_telf.select('dni_cliente','contacto','mejor15_codigo_cli','mejor15_codigo_telf','cod_attempt','q_intentos_telef')
    df_tnumeric=df_tnumeric.join(df_mejor_telf,['dni_cliente','contacto'],'left')

    dic_telf = {v: i+1 for i, v in enumerate(listas_tnumeros)}

    mapping_expr = F.create_map([F.lit(x) for x in chain(*dic_telf.items())])

    df_tnumeric = df_tnumeric.withColumn(
        "indice_tpo_telf",
        mapping_expr[F.col("tipo_telf")]
    )

    cod = F.col("mejor15_codigo_telf")
    attempt = F.col("cod_attempt")
    phone = F.col("contacto")

    df_tnumeric = df_tnumeric.withColumn(
        "contacto_02",
        F.when(cod.isNotNull(), 
                F.when((cod.isin(ls_una_vez) == False) & cod.isNotNull(), phone)
                .when(cod.isin(ls_casilla) & (attempt < 3) & cod.isNotNull(), phone)
                .when(cod.isin(ls_ocupado) & (attempt < 2) & cod.isNotNull(), phone)
        ).otherwise(phone)
    )

    df_tnumeric = df_tnumeric.withColumn(
        "indice_tpo_telf",
        F.when(F.col("contacto_02").isNotNull(), F.col("indice_tpo_telf"))
    )

    window_spec = Window.partitionBy("dni_cliente")

    df_tnumeric = df_tnumeric.withColumn(
        "min_numero",
        F.min("indice_tpo_telf").over(window_spec)
    )

    df_tnumeric = df_tnumeric.withColumn(
        "contacto_04",
        F.max(
            F.when((F.col("indice_tpo_telf") == F.col("min_numero")), F.col("contacto"))
            .otherwise(F.col("contacto_02"))
        ).over(window_spec)
    )

    df_tnumeric = df_tnumeric.withColumn(
        "contacto_02",
        F.when((F.col("contacto_02").isNull())&(F.col("tipo_telf")=='BBDD CEL01'), F.col("contacto_04")).otherwise(F.col("contacto_02"))
    ).drop('contacto_04')

    window_spec = Window.partitionBy("dni_cliente").orderBy(F.col("n_mejor_resul").asc_nulls_last())
    df_vicidial = df_vicidial.withColumn("ref_01", row_number().over(window_spec))
    df_vicidial = df_vicidial.filter(F.col("ref_01") == 1)
    df_vicidial=df_vicidial.select('dni_cliente', 'duracion', 'ult_codigo_result', 'fecha_llamada','mejor_codigo_cli','mejor_codigo_cli_dia','dni_ejecutivo','fecha_llamada_1')

        
    df_lista=df_tnumeric.join(df_base_vigente,['dni_cliente'],'left')
    df_lista=df_lista.join(df_vicidial,['dni_cliente'],'left')
    df_lista = df_lista.withColumn("fecha_llamada", col('fecha_llamada_1')).drop('fecha_llamada_1')

    window_spec = Window.orderBy("dni_cliente")

    df_lista = df_lista.withColumn(
        "dni_unico",
        dense_rank().over(window_spec)
    )

    query = f"""
        SELECT 
        id_banco as mejor_codigo_cli,
        nivel_1 as mejor_estado_tipi_cli,
        nivel_2 as mejor_sub_descripcion,
        nombre as mejor_descripcion_cli,
        id as mejor_peso_cli
        FROM VALENTINA.dbo.{tb_tipolofia}
        where estado='a'
        """
        
    df_tipi1=obtener_tabla_sql(spark,query,server_sa,user_sa,pwd_sa,db_sa)

    df_tipi2 = df_tipi1.selectExpr(
        "mejor_codigo_cli as mejor15_codigo_cli",
        "mejor_estado_tipi_cli as mejor15_estado_tipi_cli",
        "mejor_descripcion_cli as mejor15_descripcion_cli",
        "mejor_sub_descripcion as mejor15_sub_descripcion_cli",
        "mejor_peso_cli as mejor15_peso_cli"
    )

    df_tipi3 = df_tipi1.selectExpr(
        "mejor_codigo_cli as mejor15_codigo_telf",
        "mejor_estado_tipi_cli as mejor15_estado_tipi_telf",
        "mejor_descripcion_cli as mejor15_descripcion_telf",
        "mejor_sub_descripcion as mejor15_sub_descripcion_telf",
        "mejor_peso_cli as mejor15_peso_telf"
    )

    df_tipi4 = df_tipi1.selectExpr(
        "mejor_codigo_cli as mejor_codigo_cli_dia",
        "mejor_estado_tipi_cli as mejor_estado_tipi_cli_dia",
        "mejor_descripcion_cli as mejor_descripcion_cli_dia",
        "mejor_sub_descripcion as mejor_sub_descripcion_cli_dia",
        "mejor_peso_cli as mejor_peso_cli_dia"
    )

    df_lista=df_lista.join(df_tipi1,['mejor_codigo_cli'],'left')
    df_lista=df_lista.join(df_tipi2,['mejor15_codigo_cli'],'left')
    df_lista=df_lista.join(df_tipi3,['mejor15_codigo_telf'],'left')
    df_lista=df_lista.join(df_tipi4,['mejor_codigo_cli_dia'],'left')

    window_spec = Window.partitionBy('dni_cliente','contacto').orderBy(col("fecha_llamada").desc())
    df_lista = df_lista.withColumn("ref_01", row_number().over(window_spec))
    df_lista = df_lista.filter(col("ref_01") == 1).drop('ref_01')

    query = f"""
        SELECT DISTINCT dni_cliente,contacto,retiro4
        FROM (
            SELECT dni_cliente,celular as contacto,
            case
                when tipificacion in (4,5) then 'seguimiento -1'
                else 'no llamar -1'
            end as retiro4
            FROM valentina.dbo.alfin_gestion
            WHERE DNI_ejecutivo!='99999999' 
            and tipificacion in ('4','5','16','17')
            and CAST(fecha AS DATE) >= DATEADD(MONTH, -1, CAST('{fecha_mes_base}' AS DATE))
            AND CAST(fecha AS DATE) < DATEADD(MONTH, 0, CAST('{fecha_mes_base}' AS DATE))
            UNION ALL
            SELECT DISTINCT dni_cliente,CELULAR as contacto, 'excluir' as retiro4
            FROM valentina.dbo.Excluir_Gestion_TARGET where servicio in ('ALFIN','ALFCC')
        ) t
        """
    df_retiro_dni_contacto=obtener_tabla_sql(spark,query,server_sa,user_sa,pwd_sa,db_sa)

    query = f"""
            SELECT DISTINCT contacto,retiro2
                FROM (
                    SELECT celular as contacto,'indecopi' as retiro2 
                    FROM valentina.dbo.alfin_indecopi where celular is not null
                    UNION ALL
                    select CELULAR as contacto,'retiro' as retiro2 from valentina.dbo.alfin_cel_excluir
                ) t
            """
    df_retiro_contacto=obtener_tabla_sql(spark,query,server_sa,user_sa,pwd_sa,db_sa)

    query = f"""
        SELECT dni as dni_cliente ,'venta' as retiro1
        FROM VALENTINA.dbo.alfin_ventas 
        WHERE CAST(fecha AS DATE) >= DATEADD(MONTH, -1, CAST('{fecha_mes_base}' AS DATE))
        AND CAST(fecha AS DATE) < DATEADD(MONTH, 1, CAST('{fecha_mes_base}' AS DATE))
        """
    df_retiro_dni=obtener_tabla_sql(spark,query,server_sa,user_sa,pwd_sa,db_sa)

    df_lista=df_lista.join(df_retiro_dni_contacto,['contacto','dni_cliente'],'left')
    df_lista=df_lista.join(df_retiro_dni,['dni_cliente'],'left')
    df_lista=df_lista.join(df_retiro_contacto,['contacto'],'left')

    df_lista = df_lista.withColumn("retiro", when(F.col('retiro1')=='venta','venta')
                                            .when(F.col('retiro2')=='indecopi','indecopi')
                                            .when(F.col('retiro2')=='retiro','retiro')
                                            .when(F.col('retiro4')=='no llamar -1','no volver a llamar')
                                            .when(F.col('retiro4')=='seguimiento -1','seguimiento')
                                            .when(F.col('retiro')!='no_aplica',F.col('retiro'))
                                            .otherwise(F.lit('no aplica'))
                                            ).drop('retiro1','retiro2')
    df_lista = df_lista.withColumn("retiro_id", when(F.col('retiro')=='venta',1)
                                            .when(F.col('retiro')=='indecopi',2)
                                            .when(F.col('retiro')=='retiro',3)
                                            .when(F.col('retiro')=='no volver a llamar',4)
                                            .when(F.col('retiro')=='seguimiento',5)
                                            .otherwise(F.lit(10))
                                            )

    df_lista = df_lista.withColumn(
        "cabecera_carga",
        F.concat_ws(",", "contacto", "cl_id", 'id_servicio', "nombre_add")
    ).drop('nombre_add')

    cols_prioridad = [
        'cabecera_carga','dni_cliente', 'contacto', 'cl_id', 'id_servicio',
    ]


    window_spec = Window.partitionBy('dni_cliente','contacto').orderBy(col("retiro_id").asc())
    df_lista = df_lista.withColumn("ref_01", row_number().over(window_spec))
    df_lista = df_lista.filter(col("ref_01") == 1).drop('ref_01','retiro_id')

    cols_existentes = [c for c in cols_prioridad if c in df_lista.columns]

    cols_restantes = [c for c in df_lista.columns if c not in cols_existentes]

    df_lista = df_lista.select(cols_existentes + cols_restantes)
    df_lista = df_lista.toDF(*[
        re.sub(r'[^a-zA-Z0-9_]', '', c)
        for c in df_lista.columns
    ])
    overwrite_table_SQL(spark,df_lista,f'{tlista_generada}',server_sa,user_sa,pwd_sa,'CRONOX')
    print(f'tabla {tlista_generada} actualizada')


def lista_generada_valentina_actual(spark,fecha_mes_base,ls_una_vez,ls_casilla,ls_ocupado,tlista_generada,tb_tipolofia,tb_gestiones,tb_cliente,get_base,name_campana,app_campana):

    ventas_valentina_mes(name_campana,fecha_mes_base,user_valentina,pwd_valentina,server_valentina,port_mysql,db_valentina)
    df_venta=cargar_archivo_csv(spark,'tmp_vent.csv',';',True)

    df_vicidial=since_valentina_actual(spark,fecha_mes_base,tb_tipolofia,tb_gestiones,name_campana,app_campana)
    df_base_vigente = get_base(spark,fecha_mes_base)
    df_base_vigente = df_base_vigente.withColumn(
        "dni_cliente",
        F.right(
            F.concat(F.lit("00000000"), F.col("dni_cliente")),
            F.lit(8)
        )
    )

    df_tnumeric=since_tnumeros_valentina(spark,tb_cliente,fecha_mes_base)
    df_tnumeric = df_tnumeric.withColumn(
        "dni_cliente",
        F.right(
            F.concat(F.lit("00000000"), F.col("dni_cliente")),
            F.lit(8)
        )
    )

    df_mejor_telf = df_vicidial.filter(
        (F.col("duracion").isNotNull()) &
        (F.col("dni_cliente").isNotNull())
    ).select('dni_cliente','contacto','fecha_llamada','peso','codigo','cod_attempt','q_intentos_telef')

    window_spec = Window.partitionBy("dni_cliente","contacto").orderBy(F.col("peso").asc_nulls_last(),F.col("fecha_llamada").desc())
    df_mejor_telf = df_mejor_telf.withColumn("n15_mejor_tel", row_number().over(window_spec))

    window_part = Window.partitionBy("dni_cliente","contacto")

    df_mejor_telf = df_mejor_telf.withColumn(
        "mejor15_codigo_telf",
        F.max(
            F.when(F.col("n15_mejor_tel") == 1, F.col("codigo"))
        ).over(window_part)
    )

    window_spec = Window.partitionBy("dni_cliente").orderBy(F.col("peso").asc_nulls_last())
    df_mejor_telf = df_mejor_telf.withColumn("n15_mejor_cli", row_number().over(window_spec))

    window_part = Window.partitionBy("dni_cliente")

    df_mejor_telf = df_mejor_telf.withColumn(
        "mejor15_codigo_cli",
        F.max(
            F.when(F.col("n15_mejor_cli") == 1, F.col("codigo"))
        ).over(window_part)
    )

    window_spec = Window.partitionBy("dni_cliente",'contacto').orderBy(F.col("n15_mejor_tel").asc_nulls_last())
    df_mejor_telf = df_mejor_telf.withColumn("re", row_number().over(window_spec))
    df_mejor_telf=df_mejor_telf.filter(F.col('re')==1)
    df_mejor_telf=df_mejor_telf.select('dni_cliente','contacto','mejor15_codigo_cli','mejor15_codigo_telf','cod_attempt','q_intentos_telef')
    df_tnumeric=df_tnumeric.join(df_mejor_telf,['dni_cliente','contacto'],'left')

    dic_telf = {v: i+1 for i, v in enumerate(listas_tnumeros)}

    mapping_expr = F.create_map([F.lit(x) for x in chain(*dic_telf.items())])

    df_tnumeric = df_tnumeric.withColumn(
        "indice_tpo_telf",
        mapping_expr[F.col("tipo_telf")]
    )

    cod = F.col("mejor15_codigo_telf")
    attempt = F.col("cod_attempt")
    phone = F.col("contacto")

    df_tnumeric = df_tnumeric.withColumn(
        "contacto_02",
        F.when(cod.isNotNull(), 
                F.when((cod.isin(ls_una_vez) == False) & cod.isNotNull(), phone)
                .when(cod.isin(ls_casilla) & (attempt < 3) & cod.isNotNull(), phone)
                .when(cod.isin(ls_ocupado) & (attempt < 2) & cod.isNotNull(), phone)
        ).otherwise(phone)
    )

    df_tnumeric = df_tnumeric.withColumn(
        "indice_tpo_telf",
        F.when(F.col("contacto_02").isNotNull(), F.col("indice_tpo_telf"))
    )

    window_spec = Window.partitionBy("dni_cliente")

    df_tnumeric = df_tnumeric.withColumn(
        "min_numero",
        F.min("indice_tpo_telf").over(window_spec)
    )

    df_tnumeric = df_tnumeric.withColumn(
        "contacto_04",
        F.max(
            F.when((F.col("indice_tpo_telf") == F.col("min_numero")), F.col("contacto"))
            .otherwise(F.col("contacto_02"))
        ).over(window_spec)
    )

    df_tnumeric = df_tnumeric.withColumn(
        "contacto_02",
        F.when((F.col("contacto_02").isNull())&(F.col("tipo_telf")=='BBDD CEL01'), F.col("contacto_04")).otherwise(F.col("contacto_02"))
    ).drop('contacto_04')

    window_spec = Window.partitionBy("dni_cliente").orderBy(F.col("n_mejor_resul").asc_nulls_last())
    df_vicidial = df_vicidial.withColumn("ref_01", row_number().over(window_spec))
    df_vicidial = df_vicidial.filter(F.col("ref_01") == 1)
    df_vicidial=df_vicidial.select('dni_cliente', 'duracion', 'ult_codigo_result', 'fecha_llamada','mejor_codigo_cli','mejor_codigo_cli_dia','dni_ejecutivo','fecha_llamada_1')

        
    df_lista=df_tnumeric.join(df_base_vigente,['dni_cliente'],'left')
    df_lista=df_lista.join(df_vicidial,['dni_cliente'],'left')
    df_lista = df_lista.withColumn("fecha_llamada", col('fecha_llamada_1')).drop('fecha_llamada_1')

    window_spec = Window.orderBy("dni_cliente")

    df_lista = df_lista.withColumn(
        "dni_unico",
        dense_rank().over(window_spec)
    )

    query = f"""
        SELECT 
        id_banco as mejor_codigo_cli,
        nivel_1 as mejor_estado_tipi_cli,
        nivel_2 as mejor_sub_descripcion,
        nombre as mejor_descripcion_cli,
        id as mejor_peso_cli
        FROM VALENTINA.dbo.{tb_tipolofia}
        where estado='a'
        """
        
    df_tipi1=obtener_tabla_sql(spark,query,server_sa,user_sa,pwd_sa,db_sa)

    df_tipi2 = df_tipi1.selectExpr(
        "mejor_codigo_cli as mejor15_codigo_cli",
        "mejor_estado_tipi_cli as mejor15_estado_tipi_cli",
        "mejor_descripcion_cli as mejor15_descripcion_cli",
        "mejor_sub_descripcion as mejor15_sub_descripcion_cli",
        "mejor_peso_cli as mejor15_peso_cli"
    )

    df_tipi3 = df_tipi1.selectExpr(
        "mejor_codigo_cli as mejor15_codigo_telf",
        "mejor_estado_tipi_cli as mejor15_estado_tipi_telf",
        "mejor_descripcion_cli as mejor15_descripcion_telf",
        "mejor_sub_descripcion as mejor15_sub_descripcion_telf",
        "mejor_peso_cli as mejor15_peso_telf"
    )

    df_tipi4 = df_tipi1.selectExpr(
        "mejor_codigo_cli as mejor_codigo_cli_dia",
        "mejor_estado_tipi_cli as mejor_estado_tipi_cli_dia",
        "mejor_descripcion_cli as mejor_descripcion_cli_dia",
        "mejor_sub_descripcion as mejor_sub_descripcion_cli_dia",
        "mejor_peso_cli as mejor_peso_cli_dia"
    )

    df_lista=df_lista.join(df_tipi1,['mejor_codigo_cli'],'left')
    df_lista=df_lista.join(df_tipi2,['mejor15_codigo_cli'],'left')
    df_lista=df_lista.join(df_tipi3,['mejor15_codigo_telf'],'left')
    df_lista=df_lista.join(df_tipi4,['mejor_codigo_cli_dia'],'left')

    window_spec = Window.partitionBy('dni_cliente','contacto').orderBy(col("fecha_llamada").desc())
    df_lista = df_lista.withColumn("ref_01", row_number().over(window_spec))
    df_lista = df_lista.filter(col("ref_01") == 1).drop('ref_01')

    query = f"""
        SELECT DISTINCT dni_cliente,contacto,retiro4
        FROM (
            SELECT dni_cliente,celular as contacto,
            case
                when tipificacion in (4,5) then 'seguimiento -1'
                else 'no llamar -1'
            end as retiro4
            FROM valentina.dbo.alfin_gestion
            WHERE DNI_ejecutivo!='99999999' 
            and tipificacion in ('4','5','16','17')
            and CAST(fecha AS DATE) >= DATEADD(MONTH, -1, CAST('{fecha_mes_base}' AS DATE))
            AND CAST(fecha AS DATE) < DATEADD(MONTH, 0, CAST('{fecha_mes_base}' AS DATE))
            UNION ALL
            SELECT DISTINCT dni_cliente,CELULAR as contacto, 'excluir' as retiro4
            FROM valentina.dbo.Excluir_Gestion_TARGET where servicio in ('ALFIN','ALFCC')
        ) t
        """
    df_retiro_dni_contacto=obtener_tabla_sql(spark,query,server_sa,user_sa,pwd_sa,db_sa)

    query = f"""
            SELECT DISTINCT contacto,retiro2
                FROM (
                    SELECT celular as contacto,'indecopi' as retiro2 
                    FROM valentina.dbo.alfin_indecopi where celular is not null
                    UNION ALL
                    select CELULAR as contacto,'retiro' as retiro2 from valentina.dbo.alfin_cel_excluir
                ) t
            """
    df_retiro_contacto=obtener_tabla_sql(spark,query,server_sa,user_sa,pwd_sa,db_sa)

    query = f"""
        SELECT dni as dni_cliente ,'venta' as retiro1
        FROM VALENTINA.dbo.alfin_ventas 
        WHERE CAST(fecha AS DATE) >= DATEADD(MONTH, -1, CAST('{fecha_mes_base}' AS DATE))
        AND CAST(fecha AS DATE) < DATEADD(MONTH, 1, CAST('{fecha_mes_base}' AS DATE))
        """
    df_retiro_dni=obtener_tabla_sql(spark,query,server_sa,user_sa,pwd_sa,db_sa)

    df_lista=df_lista.join(df_retiro_dni_contacto,['contacto','dni_cliente'],'left')
    df_lista=df_lista.join(df_retiro_dni,['dni_cliente'],'left')
    df_lista=df_lista.join(df_retiro_contacto,['contacto'],'left')

    df_lista = df_lista.withColumn("retiro", when(F.col('retiro1')=='venta','venta')
                                            .when(F.col('retiro2')=='indecopi','indecopi')
                                            .when(F.col('retiro2')=='retiro','retiro')
                                            .when(F.col('retiro4')=='no llamar -1','no volver a llamar')
                                            .when(F.col('retiro4')=='seguimiento -1','seguimiento')
                                            .when(F.col('retiro')!='no_aplica',F.col('retiro'))
                                            .otherwise(F.lit('no aplica'))
                                            ).drop('retiro1','retiro2')
    df_lista = df_lista.withColumn("retiro_id", when(F.col('retiro')=='venta',1)
                                            .when(F.col('retiro')=='indecopi',2)
                                            .when(F.col('retiro')=='retiro',3)
                                            .when(F.col('retiro')=='no volver a llamar',4)
                                            .when(F.col('retiro')=='seguimiento',5)
                                            .otherwise(F.lit(10))
                                            )

    df_lista = df_lista.withColumn(
        "cabecera_carga",
        F.concat_ws(",", "contacto", "cl_id", 'id_servicio', "nombre_add")
    ).drop('nombre_add')

    cols_prioridad = [
        'cabecera_carga','dni_cliente', 'contacto', 'cl_id', 'id_servicio',
    ]

    window_spec = Window.partitionBy('dni_cliente','contacto').orderBy(col("retiro_id").asc())
    df_lista = df_lista.withColumn("ref_01", row_number().over(window_spec))
    df_lista = df_lista.filter(col("ref_01") == 1).drop('ref_01','retiro_id')

    cols_existentes = [c for c in cols_prioridad if c in df_lista.columns]

    cols_restantes = [c for c in df_lista.columns if c not in cols_existentes]

    df_lista = df_lista.select(cols_existentes + cols_restantes)
    df_lista = df_lista.toDF(*[
        re.sub(r'[^a-zA-Z0-9_]', '', c)
        for c in df_lista.columns
    ])
    df_lista=df_lista.join(df_venta,['cl_id'],'left')
    overwrite_table_SQL(spark,df_lista,f'{tlista_generada}',server_sa,user_sa,pwd_sa,'CRONOX')
    print(f'tabla {tlista_generada} actualizada')




def generar_tb_vigente(spark,fecha_mes_base,t_maestra,t_name_vigente,t_mumeros,cols_drop):
    query=f'''
        select
        distinct 
        RIGHT(CONCAT('0000000', a.CODDOC), 8) as CODDOC
        ,CAST(YEAR(a.FECHA_ENVIO) AS VARCHAR(4)) + '-' + RIGHT('0' + CAST(MONTH(a.FECHA_ENVIO) AS VARCHAR(2)), 2) + '-01' AS fecha_envio_actual
        ,CAST(YEAR(b.FECHA_ENVIO) AS VARCHAR(4)) + '-' + RIGHT('0' + CAST(MONTH(b.FECHA_ENVIO) AS VARCHAR(2)), 2) + '-01' AS fecha_envio
        from DANTALION.dbo.{t_maestra} a
        inner join DANTALION.dbo.{t_maestra} b
        on a.CODDOC=b.CODDOC
        where a.FECHA_ENVIO>=cast('{fecha_mes_base}' as date)
        and a.FECHA_ENVIO<=EOMONTH(cast('{fecha_mes_base}' as date))
        and b.FECHA_ENVIO < DATEADD(MONTH,-1,EOMONTH(cast('{fecha_mes_base}' as date)))
        and b.FECHA_ENVIO >= DATEADD(MONTH,-3,cast('{fecha_mes_base}' as date))
    '''
    df_ref = obtener_tabla_sql(spark,query,server_kishin,user_kishin,pwd_kishin,db_kishin)

    query=f'''
        select 
            ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS Nro,*
        from DANTALION.dbo.{t_maestra} 
        where FECHA_ENVIO>=cast('{fecha_mes_base}' as date)
        and FECHA_ENVIO<=EOMONTH(cast('{fecha_mes_base}' as date))
    '''
    df_vigente = obtener_tabla_sql(spark,query,server_kishin,user_kishin,pwd_kishin,db_kishin)

    df_vigente = df_vigente.drop(*cols_drop)

    query=f'''
        select distinct CODDOC, 1 as FLAT2
        from DANTALION.dbo.{t_mumeros} 
    '''
    df_tnum = obtener_tabla_sql(spark,query,server_kishin,user_kishin,pwd_kishin,db_kishin)


    df_ref = df_ref.withColumn("fecha_envio", F.to_date("fecha_envio")) \
        .withColumn("fecha_envio_actual", F.to_date("fecha_envio_actual"))

    df_ref = df_ref.withColumn(
        "dif_mes",
        (F.year("fecha_envio_actual") - F.year("fecha_envio")) * 12 +
        (F.month("fecha_envio_actual") - F.month("fecha_envio"))
    )

    df_ref = df_ref.withColumn('FRESCURA_TARGET', F.lit(0))

    df_ref = df_ref.withColumn(
        'REP1',
        F.when(F.col('dif_mes') == 1, 1).otherwise(0)
    )

    df_ref = df_ref.withColumn(
        'REP2',
        F.when(F.col('dif_mes').isin(1, 2), 1).otherwise(0)
    )

    df_ref = df_ref.withColumn(
        'REP3',
        F.when(F.col('dif_mes').isin(1, 2, 3), 1).otherwise(0)
    )

    df_ref = df_ref.withColumn(
        'REP_ref',
        F.col('REP1')+F.col('REP1')+F.col('REP1')
    )

    window_spec = Window.partitionBy("CODDOC").orderBy(F.col("REP_ref").desc_nulls_last())
    df_ref = df_ref.withColumn("ref_01", row_number().over(window_spec))
    df_ref = df_ref.filter(F.col("ref_01") == 1)
    df_ref=df_ref.select('CODDOC', 'FRESCURA_TARGET', 'REP1', 'REP2', 'REP3')

    df_vigente = df_vigente.orderBy(F.col('Nro').asc())
    df_vigente = df_vigente.withColumn('Nro', F.lit(1))

    df_vigente = df_vigente.join(df_ref, ['CODDOC'], 'left')
    df_vigente = df_vigente.join(df_tnum, ['CODDOC'], 'left')
    df_vigente = df_vigente.withColumn(
        'FRESCURA_TARGET',
        F.coalesce(F.col('FRESCURA_TARGET'), F.lit(1))
    )

    df_vigente = df_vigente.withColumn(
        'FLAT2',
        F.coalesce(F.col('FLAT2'), F.lit(0))
    )

    df_vigente = df_vigente.withColumn(
        "Segmentacion_Montos",
        F.when(F.col("LINEA_SAE").cast("double") > 19999, "20000 a 24999")
        .when(F.col("LINEA_SAE").cast("double") > 14999, "15000 a 19999")
        .when(F.col("LINEA_SAE").cast("double") > 9999,  "10000 a 14999")
        .when(F.col("LINEA_SAE").cast("double") > 4999,  "5000 a 9999")
        .when(F.col("LINEA_SAE").cast("double") > 1999,  "2000 a 4999")
        .otherwise("1000 a 1999")
    )
    if 'FRESCURA_TARGET' not in cols_drop:
        df_vigente=df_vigente.drop('FRESCURA_TARGET','Segmentacion_Montos')

    # df_vigente = df_vigente.withColumn(
    #     "CODDOC",
    #     F.right(
    #         F.concat(F.lit("00000000"), F.col("CODDOC")),
    #         F.lit(8)
    #     )
    # )

    overwrite_table_SQL(spark,df_vigente,t_name_vigente,server_zeus,user_zeus,pwd_zeus,'ODIN')
    print(f'{t_name_vigente} actualizado en zeus')
    df_vigente=df_vigente.drop('Nro')
    overwrite_table_SQL(spark,df_vigente,t_name_vigente,server_kishin,user_kishin,pwd_kishin,'Dantalion')
    print(f'{t_name_vigente} actualizado en kishin')
    overwrite_table_SQL(spark,df_vigente,t_name_vigente,server_sa,user_sa,pwd_sa,'ODIN')
    print(f'{t_name_vigente} actualizado en cronox')

def exportar_lista(df,nombre_archivo_xlsx,columna_ref):
    cols_prioridad = [
        'vendor_lead_code', 'phone_number', 'phone_number_02', 'title',
        'first_name', 'last_name', 'address1', 'address2', 'address3',
        'city', 'province', 'email', 'security_phrase', 'comments','mejor15_descripcion_telf'
        ]
    cols_existentes = [c for c in cols_prioridad if c in df.columns]

    df = df.select(cols_existentes ).orderBy(F.col(f'{columna_ref}').desc())

    de_resultado = df.toPandas()

    ruta_archivo = os.path.join(ruta_csv, f'{nombre_archivo_xlsx}')

    de_resultado.to_excel(ruta_archivo, index=False)

