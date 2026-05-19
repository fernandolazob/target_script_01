
ruta_archivo_generado = "C:\\Users\\DATA\\Documents\\datos\\generado"
ruta_base = 'C:\\Users\\DATA\\Documents\\datos\\recepcion_base'

ruta_csv='C:\\Users\\DATA\\Documents\\datos\\05_subir_csv'


# sql server
port = '1433'

user_zeus = "Zeus"
pwd_zeus = "target12345"
server_zeus = "192.168.2.12"
db_zeus = "THOTH"

user_sa = "sa"
pwd_sa = "target2023$"
server_sa = "192.168.2.50"
db_sa = "CRONOX"

user_kishin = "kishin"
pwd_kishin = "Leto0891.1"
server_kishin = "192.168.2.15"
db_kishin = "DANTALION"

user_datatg = "DataTg"
pwd_datatg = "target2024$$"
server_datatg = "192.168.3.26"
db_datatg = "BD_DINERS_TC"


# mysql
port_mysql = '3306'

user_valentina = "flazo"
pwd_valentina = "T4rg3t2026$$"
server_valentina = "db.mastermold.dev"
db_valentina = "crm_target"


fuente_int='BBDD INT'
tb_int='tConsoliddoNumerosInterno'
peso_int=20
cross_columns_int="('int01', a.int1),('int02', a.int2)"
fecha_ref_int='fecha'
dni_int='dni'

fuente_web='BBDD ip'
tb_web='tConsolidadoNumerosweb'
peso_web=30
cross_columns_web="('ip01',a.ip1),('ip02',a.ip2),('ip03',a.ip3),('ip04',a.ip4),('ip05',a.ip5),('ip06',a.ip6),('ip07',a.ip7),('ip08',a.ip8),('ip09',a.ip9),('ip10',a.ip10),('ip11',a.ip11),('ip12',a.ip12),('ip13',a.ip13),('ip14',a.ip14),('ip15',a.ip15),('ip16',a.ip16),('ip17',a.ip17),('ip18',a.ip18),('ip19',a.ip19),('ip20',a.ip20),('ip21',a.ip21),('ip22',a.ip22),('ip23',a.ip23),('ip24',a.ip24)"
fecha_ref_web='fecha_carga'
dni_web='dni'

fuente_hu='BBDD HU'
tb_hu='tConsolidadoNumerosHUNUS'
peso_hu=40
cross_columns_hu="('hu01',a.hu1),('hu02',a.hu2),('hu03',a.hu3),('hu04',a.hu4),('hu05',a.hu5),('hu06',a.hu6),('hu07',a.hu7),('hu08',a.hu8),('hu09',a.hu9),('hu10',a.hu10),('hu11',a.hu11),('hu12',a.hu12),('hu13',a.hu13),('hu14',a.hu14),('hu15',a.hu15),('hu16',a.hu16),('hu17',a.hu17),('hu18',a.hu18),('hu19',a.hu19),('hu20',a.hu20),('hu21',a.hu21),('hu22',a.hu22),('hu23',a.hu23),('hu24',a.hu24)"
fecha_ref_hu='fecha_carga'
dni_hu='dni'

fuente_epc='BBDD EPC'
tb_epc='tConsolidadoNumerosNMT'
peso_epc=50
cross_columns_epc="('nmt01',a.nmt1), ('nmt02',a.nmt2), ('nmt03',a.nmt3), ('nmt04',a.nmt4), ('nmt05',a.nmt5), ('nmt06',a.nmt6), ('nmt07',a.nmt7), ('nmt08',a.nmt8), ('nmt09',a.nmt9), ('nmt10',a.nmt10), ('nmt11',a.nmt11), ('nmt12',a.nmt12), ('nmt13',a.nmt13), ('nmt14',a.nmt14), ('nmt15',a.nmt15), ('nmt16',a.nmt16), ('nmt17',a.nmt17), ('nmt18',a.nmt18), ('nmt19',a.nmt19), ('nmt20',a.nmt20), ('nmt21',a.nmt21), ('nmt22',a.nmt22), ('nmt23',a.nmt23), ('nmt24',a.nmt24)"
fecha_ref_epc='fecha_carga'
dni_epc='dni'

fuente_etk='BBDD ETK'
tb_etk='tConsolidadoNumerosVCZ'
peso_etk=60
cross_columns_etk="('vcz01',a.vcz1),('vcz02',a.vcz2),('vcz03',a.vcz3),('vcz04',a.vcz4),('vcz05',a.vcz5),('vcz06',a.vcz6),('vcz07',a.vcz7),('vcz08',a.vcz8),('vcz09',a.vcz9),('vcz10',a.vcz10),('vcz11',a.vcz11),('vcz12',a.vcz12),('vcz13',a.vcz13),('vcz14',a.vcz14),('vcz15',a.vcz15),('vcz16',a.vcz16),('vcz17',a.vcz17),('vcz18',a.vcz18),('vcz19',a.vcz19),('vcz20',a.vcz20),('vcz21',a.vcz21),('vcz22',a.vcz22),('vcz23',a.vcz23),('vcz24',a.vcz24)"
fecha_ref_etk='fecha_carga'
dni_etk='dni'

# entonces aqui las campañas actuales
fuente_dinners_tc='BBDD dinnersTC'
tb_dinners_tc='base_maestra_diners_tc'
peso_dinners_tc=1
cross_columns_dinners_tc="('cel01',a.cel01), ('cel02',a.cel02), ('cel03',a.cel03), ('cel04',a.cel04),('cel05',a.cel05), ('cel06',a.cel06), ('cel07',a.cel07), ('cel08',a.cel08),('cel09',a.cel09), ('cel10',a.cel10)"
fecha_ref_dinners_tc='fecha_envio'
dni_dinners_tc='NUMERO_DOCUMENTO'

fuente_dinners_pp='BBDD dinnersPP'
tb_dinners_pp='base_maestra_diners'
peso_dinners_pp=2
cross_columns_dinners_pp = "('cel01',a.cel01), ('cel02',a.cel02), ('cel03',a.cel03), ('cel04',a.cel04), ('cel05',a.cel05), ('cel06',a.cel06), ('cel07',a.cel07), ('cel08',a.cel08)"
fecha_ref_dinners_pp='fecha_envio'
dni_dinners_pp='NumDoc'

fuente_cenco_pp='BBDD cencoPP'
tb_cenco_pp='base_maestra_cencosud_ppff'
peso_cenco_pp=3
cross_columns_cenco_pp = "('cel01',a.cel01), ('cel02',a.cel02), ('cel03',a.cel03), ('cel04',a.cel04), ('cel05',a.cel05), ('cel06',a.cel06), ('cel07',a.cel07), ('cel08',a.cel08)"
fecha_ref_cenco_pp='fecha_envio'
dni_cenco_pp='CODDOC'

fuente_cenco_tc='BBDD cencotc'
tb_cenco_tc='base_maestra_cencosud_tc'
peso_cenco_tc=4
cross_columns_cenco_tc="('cel01',a.cel01), ('cel02',a.cel02), ('cel03',a.cel03), ('cel04',a.cel04),('cel05',a.cel05), ('cel06',a.cel06), ('cel07',a.cel07), ('cel08',a.cel08)"
fecha_ref_cenco_tc='fecha_envio'
dni_cenco_tc='CODDOC'

fuente_efec_consumo='BBDD efe_consumo'
tb_efec_consumo='base_maestra_efectiva'
peso_efec_consumo=5
cross_columns_efec_consumo="""
('cel01',a.cel01), ('cel02',a.cel02), ('cel03',a.cel03), ('cel04',a.cel04),
('cel05',a.cel05), ('cel06',a.cel06), ('cel07',a.cel07), ('cel08',a.cel08),
('cel09',a.cel09), ('cel10',a.cel10), ('cel11',a.cel11), ('cel12',a.cel12),
('cel13',a.cel13), ('cel14',a.cel14), ('cel15',a.cel15)
"""
fecha_ref_efec_consumo='fecha_envio'
dni_efec_consumo='dni'

fuente_efec_negocio='BBDD efe_negocio'
tb_efec_negocio='base_maestra_efectiva_negocios'
peso_efec_negocio=6
cross_columns_efec_negocio="""
('cel01',a.cel01), ('cel02',a.cel02), ('cel03',a.cel03), ('cel04',a.cel04),
('cel05',a.cel05), ('cel06',a.cel06), ('cel07',a.cel07), ('cel08',a.cel08),
('cel09',a.cel09), ('cel10',a.cel10), ('cel11',a.cel11), ('cel12',a.cel12),
('cel13',a.cel13), ('cel14',a.cel14), ('cel15',a.cel15)
"""
fecha_ref_efec_negocio='fecha_envio'
dni_efec_negocio='NUMDOCUMENTO'



fuente_cel='BBDD CEL'
peso_cel=10

tb_maestra_cenco_ppff='base_maestra_cencosud_ppff_vigente'
dni_maestra_cenco_ppff='CODDOC'
cross_list_cenco_ppff = """
('cel01',a.cel01), ('cel02',a.cel02), ('cel03',a.cel03), ('cel04',a.cel04),
('cel05',a.cel05), ('cel06',a.cel06), ('cel07',a.cel07), ('cel08',a.cel08)
"""

tb_maestra_cenco_tc='base_maestra_cencosud_tc_vigente'
dni_maestra_cenco_tc='CODDOC'
cross_list_cenco_tc = """
('cel01',a.cel01), ('cel02',a.cel02), ('cel03',a.cel03), ('cel04',a.cel04),
('cel05',a.cel05), ('cel06',a.cel06), ('cel07',a.cel07), ('cel08',a.cel08)
"""

tb_maestra_dinners_tc='base_maestra_diners_tc_vigente'
dni_maestra_dinners_tc='NUMERO_DOCUMENTO'
cross_list_dinners_tc = """
('cel01',a.cel01), ('cel02',a.cel02), ('cel03',a.cel03), ('cel04',a.cel04),
('cel05',a.cel05), ('cel06',a.cel06), ('cel07',a.cel07), ('cel08',a.cel08),
('cel09',a.cel09), ('cel10',a.cel10)
"""

tb_maestra_dinners_pp='base_maestra_diners_vigente'
dni_maestra_dinners_pp='NumDoc'
cross_list_dinners_pp = """
('cel01',a.cel01), ('cel02',a.cel02), ('cel03',a.cel03), ('cel04',a.cel04),
('cel05',a.cel05), ('cel06',a.cel06), ('cel07',a.cel07), ('cel08',a.cel08)
"""

tb_maestra_efectiva='_vigentebase_maestra_efectiva'
dni_maestra_efe_consumo='dni'
cross_list_efectiva = """
('cel01',a.cel01), ('cel02',a.cel02), ('cel03',a.cel03), ('cel04',a.cel04),
('cel05',a.cel05), ('cel06',a.cel06), ('cel07',a.cel07), ('cel08',a.cel08),
('cel09',a.cel09), ('cel10',a.cel10), ('cel11',a.cel11), ('cel12',a.cel12),
('cel13',a.cel13), ('cel14',a.cel14), ('cel15',a.cel15)
"""

tb_maestra_efectiva_negocio='base_maestra_efectiva_negocios_vigente'
dni_maestra_efe_negocio='NUMDOCUMENTO'
cross_list_efectiva_negocio = """
('cel01',a.cel01), ('cel02',a.cel02), ('cel03',a.cel03), ('cel04',a.cel04),
('cel05',a.cel05), ('cel06',a.cel06), ('cel07',a.cel07), ('cel08',a.cel08),
('cel09',a.cel09), ('cel10',a.cel10), ('cel11',a.cel11), ('cel12',a.cel12),
('cel13',a.cel13), ('cel14',a.cel14), ('cel15',a.cel15)
"""

tb_maestra_alfin='alfin_clientes'
dni_maestra_alfin='numero_documento'
cross_list_alfin = """
('cel01',a.cl_telf1), ('cel02',a.cl_telf2), ('cel03',a.cl_telf3), ('cel04',a.cl_telf4),
('cel05',a.cl_telf5), ('cel06',a.cl_telf6), ('cel07',a.cl_telf7), ('cel08',a.cl_telf8),
('cel09',a.cl_telf9), ('cel10',a.cl_telf10), ('cel11',a.cl_movil), ('cel12',a.cl_celular),
('cel12',a.cl_telefono)
"""

tb_maestra_credicash='alfcc_clientes'
dni_maestra_credicash='numero_documento'
cross_list_credicash = """
('cel01',a.cl_telf1), ('cel02',a.cl_telf2), ('cel03',a.cl_telf3), ('cel04',a.cl_telf4),
('cel05',a.cl_telf5), ('cel06',a.cl_telf6), ('cel07',a.cl_telf7), ('cel08',a.cl_telf8),
('cel09',a.cl_telf9), ('cel10',a.cl_telf10), ('cel11',a.cl_movil), ('cel12',a.cl_celular),
('cel12',a.cl_telefono)
"""



listas_tnumeros = [
    # CEL 1
    'bbdd cel01','bbdd cel02','bbdd cel03','bbdd cel04','bbdd cel05',
    'bbdd cel06','bbdd cel07','bbdd cel08','bbdd cel09','bbdd cel10',
    'bbdd cel11','bbdd cel12','bbdd cel13','bbdd cel14','bbdd cel15',

    # INT 2
    'bbdd int01','bbdd int02',

    # EIPhasta 24
    'bbdd ip01','bbdd ip02','bbdd ip03','bbdd ip04','bbdd ip05',
    'bbdd ip06','bbdd ip07','bbdd ip08','bbdd ip09','bbdd ip10',
    'bbdd ip11','bbdd ip12','bbdd ip13','bbdd ip14','bbdd ip15',
    'bbdd ip16','bbdd ip17','bbdd ip18','bbdd ip19','bbdd ip20',
    'bbdd ip21','bbdd ip22','bbdd ip23','bbdd ip24',

    # HU hasta 24
    'bbdd hu01','bbdd hu02','bbdd hu03','bbdd hu04','bbdd hu05',
    'bbdd hu06','bbdd hu07','bbdd hu08','bbdd hu09','bbdd hu10',
    'bbdd hu11','bbdd hu12','bbdd hu13','bbdd hu14','bbdd hu15',
    'bbdd hu16','bbdd hu17','bbdd hu18','bbdd hu19','bbdd hu20',
    'bbdd hu21','bbdd hu22','bbdd hu23','bbdd hu24',

    # NMT hasta 24
    'bbdd nmt01','bbdd nmt02','bbdd nmt03','bbdd nmt04','bbdd nmt05',
    'bbdd nmt06','bbdd nmt07','bbdd nmt08','bbdd nmt09','bbdd nmt10',
    'bbdd nmt11','bbdd nmt12','bbdd nmt13','bbdd nmt14','bbdd nmt15',
    'bbdd nmt16','bbdd nmt17','bbdd nmt18','bbdd nmt19','bbdd nmt20',
    'bbdd nmt21','bbdd nmt22','bbdd nmt23','bbdd nmt24',

    # VCZ hasta 24
    'bbdd vcz01','bbdd vcz02','bbdd vcz03','bbdd vcz04','bbdd vcz05',
    'bbdd vcz06','bbdd vcz07','bbdd vcz08','bbdd vcz09','bbdd vcz10',
    'bbdd vcz11','bbdd vcz12','bbdd vcz13','bbdd vcz14','bbdd vcz15',
    'bbdd vcz16','bbdd vcz17','bbdd vcz18','bbdd vcz19','bbdd vcz20',
    'bbdd vcz21','bbdd vcz22','bbdd vcz23','bbdd vcz24'
]
