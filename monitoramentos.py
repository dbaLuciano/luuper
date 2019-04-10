###########################################################################################
##                                         DBCONTROL
## DBModule - Monitoramentos
## Autor: Luciano Alvarenga Maciel Pires
## Data: 12/03/2018
##
## -> DDLs de verificacao/monitoramento/insercao encapsuladas como funcao
###########################################################################################
## Versao 1
## 12/03/2018 1.0 Luciano - Criacao do script
## 03/08/2018 1.3 Inclusao de monitoramento dos servicos de conexao Oracle (services)
##
## Desenvolvimento / autoria / propriedades intelecuais reservadas para Luciano Alvarenga
###########################################################################################
import cx_Oracle
import ddl_dbmon as ddl
import alarme
from ora_target import ora_target as class_target
from db_dbmon import dbmon as class_dbmon
from decimal import Decimal
import threading

def get_tablespace_dados(target,sq):
    """Modulo DBMON - Coleta de tablespace Dados e Undo (9i/10g/11g) """
    TARGET = target.upper()
    ora_target = class_target(target.upper())
    dbmon = class_dbmon()
    conn_target = ora_target.conn
    cur_target = ora_target.get_cursor()
    conn_dbmon = dbmon.conn
    cur_dbmon = dbmon.new_cursor_dbmon()
    try:
        t = threading.Timer(180,conn_target.cancel)
        t.start()
        if ora_target.versao_bd >= "12":
            cur_target.execute(ddl.ddl_consulta('tablespace_dados_v2_12'))
        else:
            cur_target.execute(ddl.ddl_consulta('tablespace_dados_v2_10'))
        t.cancel()
        for row in cur_target.fetchall():
            cur_dbmon.execute (ddl.ddl_insert('tablespace_dados_v1'),
                               vTARGET=target.upper(),
                               ID_CHECKLIST=int(sq),
                               TABLESPACE=row[0],
                               CURR_TOTAL_MB=row[1],
                               CUR_FREE_MB=row[2],
                               CUR_USED_MB=row[3],
                               PCT_CUR_FREE=row[4],
                               MAX_CAN_EXT2=row[5],
                               PCT_MAX_FREE=row[6],
                               PCT_MAX_USED=row[7],
                               CONTENTS=row[8])
            if str(row[8])=="PERMANENT" and float(row[7])>85:
                alarme.grava_alarme(TARGET,"TBL_DADOS_SIZE","Avaliar tablespace "+ row[0] +", %MaxUsed:["+str(round(Decimal(row[7]),2))+ "], em: " + TARGET )
        conn_dbmon.commit()
        cur_dbmon.close()
        conn_dbmon.close()
        print("  Procedimento de coleta de tablepsace em " +TARGET+ " realizado com sucesso")
        return("SUCESSO")
    except cx_Oracle.DatabaseError as e:
        error, = e.args
        #Gravando erro target,alarme,descricao
        print("!!!!  Erro registrado em " + TARGET + ", durante a execucao do monitoramento de tablespaces" )
        alarme.grava_alarme(TARGET,"TABLESPACE_INTERNO","Erro gerado: " + str(error.code) + ": " + str(error.message) )
        cur_dbmon.close()
        conn_dbmon.rollback()
        conn_dbmon.close()
        return("FALHA")

def get_tablespace_temp(target,sq):
    """Modulo DBMON - Coleta de tablespace Temporaria (9i/10g/11g) """
    monit="tablespace_temp_v1"
    TARGET = target.upper()
    ora_target = class_target(target.upper())
    dbmon = class_dbmon()
    conn_target = ora_target.conn
    cur_target = ora_target.get_cursor()
    conn_dbmon = dbmon.conn
    cur_dbmon = dbmon.new_cursor_dbmon()
    try:
        t = threading.Timer(60,conn_target.cancel)
        t.start()
        cur_target.execute(ddl.ddl_consulta(monit))
        t.cancel()
        for row in cur_target.fetchall():
            cur_dbmon.execute (ddl.ddl_insert(monit),
                               vTARGET=TARGET,
                               ID_CHECKLIST=int(sq),
                               TABLESPACE=row[0],
                               CURR_TOTAL_MB=row[1],
                               CUR_FREE_MB=row[2],
                               CUR_USED_MB=row[3],
                               PCT_CUR_FREE=row[4],
                               MAX_CAN_EXT2=row[5],
                               PCT_MAX_FREE=row[6],
                               PCT_MAX_USED=row[7],
                               CONTENTS=row[8])
        conn_dbmon.commit()
        cur_dbmon.close()
        conn_dbmon.close()
    except cx_Oracle.DatabaseError as e:
        error, = e.args
        #Gravando erro target,alarme,descricao
        print("!!!!  Erro registrado em " + TARGET + ", durante a execucao do monitoramento: " + str(monit) )
        alarme.grava_alarme(TARGET,"monit_"+monit,"Erro gerado: ORA-" + str(error.code) )
        cur_dbmon.close()
        conn_dbmon.rollback()
        conn_dbmon.close()
        return("FALHA")
    print("  Procedimento "+monit+ " em " +TARGET+ " realizado com sucesso")
    return("SUCESSO")

def get_index_unusable(target,sq):
    """Modulo DBMON - Coleta de indices unusable (9i/10g/11g) """
    monit="index_unusable" 
    TARGET = target.upper()
    ora_target = class_target(target.upper())
    dbmon = class_dbmon()
    conn_target = ora_target.conn
    cur_target = ora_target.get_cursor()
    conn_dbmon = dbmon.conn
    cur_dbmon = dbmon.new_cursor_dbmon()
    i=0
    try:
        strsql="select 1 from target_config_threshold where target='"+TARGET+"' and THRESHOLD='"+monit+"__disable'"
        #Verifica se o monitoramento está desativado para o ambiente
        cur_dbmon.execute(strsql)
        resultado=cur_dbmon.fetchall()
        if len(resultado)!=0:
            return("CANCELADO")

        t = threading.Timer(60,conn_target.cancel)
        t.start()
        cur_target.execute(ddl.ddl_consulta(monit))
        t.cancel()
        for row in cur_target.fetchall():
                cur_dbmon.execute (ddl.ddl_insert(monit),
                                vTARGET=TARGET,
                                ID_CHECKLIST=int(sq),
                                OWNER=row[0],
                                TABLE_NAME=row[1],
                                INDEX_NAME=row[2],
                                STATUS=row[3]
                )
                alarme.grava_alarme(TARGET,"INDEX_UNUSABLE","Indices invalidos foram encontrados em "+ TARGET + "." )
                i+=1
        print("  Procedimento "+monit+ " em " +TARGET+ " realizado com sucesso")
        if i!=0:
            print("    [" + str(i) + "] Indices invalidos encontrados no ambiente")
        conn_dbmon.commit()
        cur_dbmon.close()
        conn_dbmon.close()
        return("SUCESSO")
    except cx_Oracle.DatabaseError as e:
        error, = e.args
        #Gravando erro target,alarme,descricao
        print("!!!!  Erro registrado em " + TARGET + ", durante a execucao do monitoramento: " + str(monit) )
        alarme.grava_alarme(TARGET,"monit_"+monit,"Erro gerado: " + str(error.code) )
        cur_dbmon.close()
        conn_dbmon.rollback()
        conn_dbmon.close()
        return("FALHA")

def get_asm_diskgroup(target,sq):
    """Modulo DBMON - Coleta do tamanho dos diskgroups ASM (9i/10g/11g/12c) """
    monit="asm_diskgroup"
    TARGET = target.upper()
    ora_target = class_target(target.upper())
    dbmon = class_dbmon()
    conn_target = ora_target.conn
    cur_target = ora_target.get_cursor()
    conn_dbmon = dbmon.conn
    cur_dbmon = dbmon.new_cursor_dbmon()

    try:
        strsql="UPDATE ORA_ASM_DISKGROUP SET COLETA_ATIVA=0 WHERE COLETA_ATIVA=1 AND TARGET='"+TARGET+"'"
        cur_dbmon.execute (strsql)
        conn_dbmon.commit()

        t = threading.Timer(150,conn_target.cancel)
        t.start()
        cur_target.execute(ddl.ddl_consulta(monit))
        t.cancel()
        for row in cur_target.fetchall():
            cur_dbmon.execute (ddl.ddl_insert(monit)
                               ,vTARGET=TARGET
                               ,ID_CHECKLIST=int(sq)
                               ,NAME=row[0]
                               ,TOTAL_MB=row[1]
                               ,USED_MB=row[2]
                               ,FREE_MB=row[3]
                               ,PCT_USED=row[4]
                               ,PCT_FREE=row[5])
        conn_dbmon.commit()
        cur_dbmon.close()
        conn_dbmon.close()
        print("  Procedimento "+monit+ " em " +TARGET+ " realizado com sucesso")
        return("SUCESSO")
    except cx_Oracle.DatabaseError as e:
        error, = e.args
        cur_dbmon.close()
        conn_dbmon.rollback()
        conn_dbmon.close()
        #Gravando erro target,alarme,descricao
        alarme.grava_alarme(TARGET,"monit_"+monit,"Erro gerado: " + str(error.code) )
        return("FALHA")

def check_asm_diskgroup(target):
    """Modulo DBMON - Verificacao de diskgroup que estao acima da % """
    monit="check_asm_dg_space"
    TARGET=target.upper()
    info=class_target(TARGET)
    dbmon = class_dbmon()
    conn_dbmon = dbmon.conn
    cur_dbmon = dbmon.new_cursor_dbmon()

    strsql="select name, free_mb, pct_free from ora_asm_diskgroup where coleta_ativa=1 and target=:vTARGET"
    cur_dbmon.execute(strsql,vTARGET=TARGET)
    diskgroups=cur_dbmon.fetchall()

    #Se nao houver registros de ASM dispara alarme ASM_NODISKGROUP
    if len(diskgroups)==0:
        alarme.grava_alarme(TARGET,'ASM_NODISKGROUP',TARGET + ' com problemas na execucao de consulta ASM')
        return

    for diskgroup in diskgroups:
        diskgroup_name=diskgroup[0]
        diskgroup_free_mb=round(Decimal(diskgroup[1]),2)
        diskgroup_pct_free=round(Decimal(diskgroup[2]),2)

        if diskgroup_pct_free<15 and diskgroup_free_mb/1024<100:
            #Verifico se existe threshold custom para o diskgroup
            strsql_custom = """select round(to_number(valor),2)
                            from TARGET_CONFIG_THRESHOLD 
                            where target='"""+TARGET+"' and THRESHOLD='asm_diskgroup_min_free_mb' and tipo='"+diskgroup_name+"'"
            cur_dbmon.execute(strsql_custom)
            thresholds = cur_dbmon.fetchall()
            if len(thresholds)!=0:
                metrica=round(Decimal(thresholds[0]),2)
                #Monitoramento existente
                if metrica > diskgroup_free_mb:
                    mensagem='Avaliar threshold no diskgroup '+ diskgroup_name +' em ' +TARGET+ ': ' + str(diskgroup_pct_free) + '%Livre, ' + str(diskgroup_free_mb) + '(Mb) Livre'
                    alarme.grava_alarme(target,'ASM_DISKGROUP_EXHAUST',mensagem )
            else:
                mensagem='Avaliar diskgroup '+ diskgroup_name +' em ' +TARGET+ ': ' + str(diskgroup_pct_free) + '%Livre, ' + str(diskgroup_free_mb) + '(Mb) Livre'
                alarme.grava_alarme(target,'ASM_DISKGROUP_EXHAUST',mensagem )

def tool_rman_last_sessionkey(conn_dbmon,target,tipo):
    """Retorna o ultimo session_key na base do dbmon se nao houver o valor sera 0"""
    cur_dbmon = cx_Oracle.Cursor(conn_dbmon)

    cur_dbmon.execute("""select nvl(max(session_key),0) 
                         from ORA_RMAN 
                         WHERE TARGET=:vTARGET 
                         AND TIPO=:vTIPO"""
                     ,vTARGET=target,vTIPO=tipo)
    for row in cur_dbmon.fetchall():
        ultimo_session_key=row[0]
    return(ultimo_session_key)

def tool_rman_upd_coleta_ativa(conn_dbmon,target,tipo):
    """Atualiza a ultima coleta como coleta ativa (facilitando assim a busca na app)"""
    cur_dbmon = cx_Oracle.Cursor(conn_dbmon)
     
    #Apago o ponteiro atual
    cur_dbmon.execute("""UPDATE ORA_RMAN SET COLETA_ATIVA=0 
                         WHERE TARGET=:vTARGET 
                         AND COLETA_ATIVA=1
                         AND TIPO=:vTIPO"""
                     ,vTARGET=target
                     ,vTIPO=tipo)
    #Sem fazer commit atualizo a ultimo backup com a coleta_ativa
    cur_dbmon.execute("""UPDATE ORA_RMAN SET COLETA_ATIVA=1
                         WHERE TARGET=:vTARGET
                         AND TIPO=:vTIPO
                         AND SESSION_KEY=(select max(SESSION_KEY) 
                                          from ORA_RMAN
                                          WHERE TARGET=:vTARGET
                                          AND TIPO=:vTIPO)"""
                     ,vTARGET=target
                     ,vTIPO=tipo)
    conn_dbmon.commit()
    #Encerrando as conexões feitas
    cur_dbmon.close()

def tool_rman_channel(conn_target,target,session_key):
    """Dado um target e uma session_key identificador de backup, 
       e feita uma avaliacao se há historico e quantos canais foram usados
     """
    print("tool_rman_channel iniciado para " +target + ":" + str(session_key) ) 
    cur_target = cx_Oracle.Cursor(conn_target)
    #Check se tem informacoes do backup em 
    cur_target.execute ("select count(1) from GV$RMAN_OUTPUT where SESSION_RECID=:SK",SK=session_key)
    for row in cur_target.fetchall():
        if row[0]==0:
            return(0)
    cur_target.execute(""" select count(1) quantidade_canais 
                               from (select distinct SUBSTR(OUTPUT,9,INSTR(OUTPUT,':')-9 )  
                                     from GV$RMAN_OUTPUT 
                                     where SESSION_RECID =:SK 
                                     and output like '%channel%starting%')""",SK=session_key)
    for row in cur_target.fetchall():
        quantidade_canais=row[0]
    cur_target.close()
    return(quantidade_canais)

def get_rman_backup_full(target,sq):
    """Modulo DBMON - Coleta de informacoes do ultimo backup full (9i,10g,11g,12c) """
    monit="rman_backup_full"
    TARGET = target.upper()
    ora_target = class_target(target.upper())
    dbmon = class_dbmon()
    conn_target = ora_target.conn
    cur_target = ora_target.get_cursor()
    conn_dbmon = dbmon.conn
    cur_dbmon = dbmon.new_cursor_dbmon()
    try:
        #Coleto o ultimo session_key
        ultimo_session_key=tool_rman_last_sessionkey(conn_dbmon,TARGET,'F')
        #print ('    Ultimo: ' + str(ultimo_session_key) )

        t = threading.Timer(90,conn_target.cancel)
        #t.start()
        cur_target.execute(ddl.ddl_consulta(monit)
                        ,Vses_key=ultimo_session_key)
        #t.cancel()
        for row in cur_target.fetchall():
            qtde_canais=tool_rman_channel(conn_target,target,row[0])
            #print('Numero de canais: ' + int(qtde_canais) )

            cur_dbmon.execute (ddl.ddl_insert(monit),
                               vTARGET=TARGET,
                               ID_CHECKLIST=int(sq),
                               SESSION_KEY=row[0],
                               STATUS=row[2],
                               START_TIME=row[3],
                               END_TIME=row[4],
                               TEMPO_SEGUNDOS=row[5],
                               INPUT_BYTES=row[6],
                               OUTPUT_BYTES=row[7],
                               INPUT_BYTES_PER_SEC=row[8],
                               OUTPUT_BYTES_PER_SEC=row[9],
                               CANAIS=int(qtde_canais)
                              )
        conn_dbmon.commit()
        #Atualiza ultima coleta como coleta ativa
        tool_rman_upd_coleta_ativa(conn_dbmon,target,'F')
        #Fecho conexao com o target
        cur_target.close()

        #Fase 2 - Verificacao se a base possui ou nao erro
        # - Utilizo a funcao falarme_rman no banco de dados que avalia os parametros padroes e 
        # tambem os valores configurados de thresholds especificos e faz a avaliacao se existe 
        # backup completed ou completed with errors (que sera removido) e gera ou nao o alerta
        #
        # Feature nova - Se o backup estiver OK ele já encerra o chamado automáticamente.
        cur_dbmon.execute("select falarme_rman(:vTARGET,:vTIPO) from dual"
                          ,vTARGET=TARGET
                          ,vTIPO='F')
        for row in cur_dbmon.fetchall():
            if row[0] != "OK":
                alarme.grava_alarme(TARGET,"RMAN_BACKUP_FULL",row[0] )
                conn_dbmon.commit()
            #Nao tem erro
            else:
                retorno=alarme.check_alarme_ativo(TARGET,"RMAN_BACKUP_FULL")
                if retorno==1 :
                    alarme.finaliza_alarme(TARGET,"RMAN_BACKUP_FULL","Alarme normalizado automaticamente.")
        cur_dbmon.close()
        conn_dbmon.close()
        print("  Procedimento "+monit+ " em " +TARGET+ " realizado com sucesso")
        return("SUCESSO")

    except cx_Oracle.DatabaseError as e:
        error, = e.args
        #Gravando erro target,alarme,descricao
        print("!!!!  Erro registrado em " + TARGET + ", durante a execucao do monitoramento: " + str(monit) )
        alarme.grava_alarme(TARGET,"monit_"+monit,"Erro gerado: " + str(error.code) )
        cur_dbmon.close()
        conn_dbmon.rollback()
        conn_dbmon.close()
        return("FALHA")

def get_rman_backup_arch(target,sq):
    """Modulo DBMON - Coleta de informacoes do ultimo backup full (9i,10g,11g,12c) """
    monit="rman_backup_arch"
    TARGET = target.upper()
    ora_target = class_target(target.upper())
    dbmon = class_dbmon()
    conn_target = ora_target.conn
    cur_target = ora_target.get_cursor()
    conn_dbmon = dbmon.conn
    cur_dbmon = dbmon.new_cursor_dbmon()
    try:
        t = threading.Timer(60,conn_target.cancel)
        t.start()
        cur_target.execute(ddl.ddl_consulta(monit))
        t.cancel()
        for row in cur_target.fetchall():
            cur_dbmon.execute (ddl.ddl_insert(monit),
                               vTARGET=TARGET,
                               ID_CHECKLIST=int(sq),
                               SESSION_KEY=row[0],
                               STATUS=row[2],
                               START_TIME=row[3],
                               END_TIME=row[4],
                               TEMPO_SEGUNDOS=row[5])
        conn_dbmon.commit()
        cur_dbmon.close()
        conn_dbmon.close()
        print("  Procedimento "+monit+ " em " +TARGET+ " realizado com sucesso")
        return("SUCESSO")
    except cx_Oracle.DatabaseError as e:
        error, = e.args
        #Gravando erro target,alarme,descricao
        print("!!!!  Erro registrado em " + TARGET + ", durante a execucao do monitoramento: " + str(monit) )
        alarme.grava_alarme(TARGET,"monit_"+monit,"Erro gerado: " + str(error.code) )
        cur_dbmon.close()
        conn_dbmon.rollback()
        conn_dbmon.close()
        return("FALHA")

def get_dataguard_info(target,sq):
    """Modulo DBMON - Informacoes do Dataguard (9i,10g,11g,12c) """
    monit="dataguard_info"
    TARGET = target.upper()
    ora_target = class_target(target.upper())
    dbmon = class_dbmon()
    conn_target = ora_target.conn
    cur_target = ora_target.get_cursor()
    conn_dbmon = dbmon.conn
    cur_dbmon = dbmon.new_cursor_dbmon()
    try:
        #Procedimento para definir qual coleta está ativa
        str_update="UPDATE ORA_DATAGUARD_INFO SET COLETA_ATIVA=0 WHERE COLETA_ATIVA=1 and TARGET='"+TARGET+"'"
        cur_dbmon.execute (str_update)
        conn_dbmon.commit()

        t = threading.Timer(60,conn_target.cancel)
        t.start()
        cur_target.execute(ddl.ddl_consulta(monit))
        t.cancel()
        for row in cur_target.fetchall():
            cur_dbmon.execute (ddl.ddl_insert(monit),
                               vTARGET=TARGET,
                               ID_CHECKLIST=int(sq),
                               DEST_ID=row[0],
                               DESTINATION=row[1],
                               STATUS=row[2],
                               REGISTER=row[3],
                               TRANSMIT_MODE=row[4],
                               SCHEDULE=row[5],
                               LOG_SEQUENCE=row[6],
                               VALID_NOW=row[7],
                               VALID_TYPE=row[8],
                               VALID_ROLE=row[9],
                               VERIFY=row[10],
                               FAIL_DATE=row[11],
                               FAIL_SEQUENCE=row[12],
                               ERROR=row[13])
            if row[7] != 'YES' :
                if row[13].upper != 'NONE':
                    alarme.grava_alarme(TARGET,"DATAGUARD_ERROR","Erro registrado no dataguard dest_id="+str(row[0]) + " erro: ["+str(row[13]) + "] em: " + TARGET + ", durante o checklist "+str(sq)+"." )
        conn_dbmon.commit()
        cur_dbmon.close()
        conn_dbmon.close()
        print("  Procedimento "+monit+ " em " +TARGET+ " realizado com sucesso")
        return("SUCESSO")
    except cx_Oracle.DatabaseError as e:
        error, = e.args
        #Gravando erro target,alarme,descricao
        print("!!!!  Erro registrado em " + TARGET + ", durante a execucao do monitoramento: " + str(monit) )
        alarme.grava_alarme(TARGET,"monit_"+monit,"Erro gerado: " + str(error.code) )
        cur_dbmon.close()
        conn_dbmon.rollback()
        conn_dbmon.close()
        return("FALHA")

def get_dataguard_gap(target,sq):
    """Modulo DBMON - Gaps entre os ambientes Dataguard (9i,10g,11g,12c) """
    monit="dataguard_gap"
    TARGET = target.upper()
    ora_target = class_target(target.upper())
    dbmon = class_dbmon()
    conn_target = ora_target.conn
    cur_target = ora_target.get_cursor()
    conn_dbmon = dbmon.conn
    cur_dbmon = dbmon.new_cursor_dbmon()
    try:
        #Procedimento para definir qual coleta está ativa
        strsql="UPDATE ORA_DATAGUARD_GAP SET COLETA_ATIVA=0 WHERE COLETA_ATIVA=1 and TARGET='"+TARGET+"'"
        cur_dbmon.execute (strsql)
        conn_dbmon.commit()
        
        #Verifica se existem thresholds cadastrados
        strsql="select valor from target_config_threshold where target='"+TARGET+"' and THRESHOLD='"+monit+"'"
        cur_dbmon.execute(strsql)
        resultado=cur_dbmon.fetchall()
        if len(resultado)!=0:
            for valor in resultado:
                gap_value=valor[0]
        else:
            #Valor padrão aceito para gap
            gap_value=30

        t = threading.Timer(60,conn_target.cancel)
        t.start()
        cur_target.execute(ddl.ddl_consulta(monit))
        t.cancel()
        for row in cur_target.fetchall():
            cur_dbmon.execute (ddl.ddl_insert(monit),
                                vTARGET=TARGET
                                ,ID_CHECKLIST=int(sq)
                                ,DEST_ID=row[0]
                                ,THREAD=row[1]
                                ,SEQUENCIA_ONLINE=row[2]
                                ,SEQUENCIA_APLICADA=row[3]
                                ,SEQUENCIA_REPLICA=row[4]
                                ,GAP=row[5]
                             )
            #iLuciano 24/04/2018
            if row[5] >=  gap_value:
                alarme.grava_alarme(TARGET,"DATAGUARD_GAP","Foi detectado "+str(row[5]) + " archives de gap em "+ TARGET + ", durante o checklist "+str(sq)+"." )
        conn_dbmon.commit()
        cur_dbmon.close()
        conn_dbmon.close()
        print("  Procedimento "+monit+ " em " +TARGET+ " realizado com sucesso")
        return("SUCESSO")
    except cx_Oracle.DatabaseError as e:
        error, = e.args
        #Gravando erro target,alarme,descricao
        print("!!!!  Erro registrado em " + TARGET + ", durante a execucao do monitoramento: " + str(monit) )
        alarme.grava_alarme(TARGET,"monit_"+monit,"Erro gerado: " + str(error.code) )
        cur_dbmon.close()
        conn_dbmon.rollback()
        conn_dbmon.close()
        return("FALHA")


def get_option_spatial(target,sq):
    """Modulo DBMON - Verificacao da existencia do produto Spatial """
    monit="option_spatial"
    TARGET = target.upper()
    ora_target = class_target(target.upper())
    dbmon = class_dbmon()
    conn_target = ora_target.conn
    cur_target = ora_target.get_cursor()
    conn_dbmon = dbmon.conn
    cur_dbmon = dbmon.new_cursor_dbmon()
    try:
        t = threading.Timer(60,conn_target.cancel)
        t.start()
        cur_target.execute(ddl.ddl_consulta(monit))
        t.cancel()
        for row in cur_target.fetchall():
            cur_dbmon.execute (ddl.ddl_insert(monit),
                                vTARGET=TARGET
                                ,ID_CHECKLIST=int(sq)
                                ,PRODUTO=row[0]
                                ,INSTALADO=row[1]
                                ,QUANT_UTILIZADA=row[2]
                                ,ATUALMENTE_UTILIZADA=row[3]
                             )
        conn_dbmon.commit()
        cur_dbmon.close()
        conn_dbmon.close()
        print("  Procedimento "+monit+ " em " +TARGET+ " realizado com sucesso")
        return("SUCESSO")
    except cx_Oracle.DatabaseError as e:
        error, = e.args
        #Gravando erro target,alarme,descricao
        print("!!!!  Erro registrado em " + TARGET + ", durante a execucao do monitoramento: " + str(monit) )
        alarme.grava_alarme(TARGET,"monit_"+monit,"Erro gerado: " + str(error.code) )
        cur_dbmon.close()
        conn_dbmon.rollback()
        conn_dbmon.close()
        return("FALHA")

def get_database_info(target,sq):
    """Modulo DBMON - Coleta de informacoes de database """
    monit="database_info"
    TARGET = target.upper()
    ora_target = class_target(target.upper())
    dbmon = class_dbmon()
    conn_target = ora_target.conn
    cur_target = ora_target.get_cursor()
    conn_dbmon = dbmon.conn
    cur_dbmon = dbmon.new_cursor_dbmon()

    dbmon.limpa_stage(TARGET,'database')
    try:
        #Inicializando as variaveis
        str_produto            =""
        str_produto_status     =""
        str_versao             =""
        str_versao_completa    =""
        str_bundle_data        =""
        str_bundle_ACTION      =""
        str_bundle_informacao  =""
        #Coletando as informacoes das versoes para saber qual monitoramento se aplica
        cur_target.execute(ddl.ddl_consulta("database_version"))
        for r_db_info in cur_target.fetchall():
            str_produto            =r_db_info[0]
            str_produto_status     =r_db_info[1]
            str_versao             =r_db_info[2]
            str_versao_completa    =r_db_info[3]
        #Coletando as informacoes de bundle patch
        cur_target.execute(ddl.ddl_consulta("database_bundle"))
        for r_db_info in cur_target.fetchall():
            str_bundle_data        =r_db_info[0]
            str_bundle_ACTION      =r_db_info[1]
            if str_bundle_ACTION=="APPLY":
                str_bundle_informacao=r_db_info[3]
            else:
                str_bundle_informacao=r_db_info[2]
        #Comentado em 10/09/2018 - Sumir com a mensagem do output
        #print("Versao coletada: " + str(str_versao) )
        if str(str_versao) >= "12":
            cur_target.execute(ddl.ddl_consulta("database_info_12c"))
        else:
            cur_target.execute(ddl.ddl_consulta("database_info"))
        for row in cur_target.fetchall():
            cur_dbmon.execute (ddl.ddl_insert(monit),
                                vTARGET=TARGET
                                ,ID_CHECKLIST=int(sq)
                                ,PRODUTO = str_produto
                                ,PRODUTO_STATUS =str_produto_status
                                ,VERSAO = str_versao
                                ,VERSAO_COMPLETA = str_versao_completa
                                ,BUNDLE_DATA = str_bundle_data
                                ,BUNDLE_ACTION = str_bundle_ACTION
                                ,BUNDLE_INFORMACAO = str_bundle_informacao
                                ,NAME=row[0]
                                ,DB_UNIQUE_NAME=row[1]
                                ,LOG_MODE=row[3]
                                ,VERSION_TIME=row[4]
                                ,OPEN_MODE=row[5]
                                ,PROTECTION_MODE=row[6]
                                ,PROTECTION_LEVEL=row[7]
                                ,DATABASE_ROLE=row[8]
                                ,GUARD_STATUS=row[9]
                                ,SWITCHOVER_STATUS=row[10]
                                ,DATAGUARD_BROKER=row[11]
                                ,SUPPLEMENTAL_LOG_DATA_MIN=row[12]
                                ,SUPPLEMENTAL_LOG_DATA_PK=row[13]
                                ,SUPPLEMENTAL_LOG_DATA_UI=row[14]
                                ,SUPPLEMENTAL_LOG_DATA_FK=row[15]
                                ,SUPPLEMENTAL_LOG_DATA_ALL=row[16]
                                ,FORCE_LOGGING=row[17]
                                ,PLATFORM_NAME=row[18]
                                ,FLASHBACK_ON=row[19]
                                ,CDB=row[20]
                             )
        conn_dbmon.commit()
        
        cur_dbmon.execute("select count(1) from ORA_database where target=:vTARGET",vTARGET=TARGET)
        for row in cur_dbmon.fetchall():
            if int(row[0]) != 0 : ##Entao existe o registro
                #BUNDLE_DATA / BUNDLE_ACTION / BUNDLE_INFORMACAO
                cur_dbmon.execute(ddl.ddl_check_diff("ck_database_diff","BUNDLE_DATA,BUNDLE_ACTION,BUNDLE_INFORMACAO"),vID_CHECKLIST=sq,vTARGET=TARGET)
                for row_ck in cur_dbmon.fetchall():
                    if str(row_ck[0]) != str(0) :
                        print ("["+ TARGET +"] Detectado alteracoes no bundle patch - " + str(row[0]) )
                        alarme.grava_alarme(TARGET,"DATABASE_D_BUNDLE","Detectado alteracoes do bundle patch do target " + TARGET + ", durante o checklist:" + str(sq) + "." )
                #Produto/versao
                cur_dbmon.execute(ddl.ddl_check_diff("ck_database_diff","PRODUTO,PRODUTO_STATUS,VERSAO,VERSAO_COMPLETA"),vID_CHECKLIST=sq,vTARGET=TARGET)
                for row_ck in cur_dbmon.fetchall():
                    if str(row_ck[0]) != str(0) :
                        print ("["+ TARGET +"] Detectado alteracoes na versao do banco - " + str(row[0]) )
                        alarme.grava_alarme(TARGET,"DATABASE_D_VERSION","Detectado alteracoes no produto ou versao do target " + TARGET + ", durante o checklist:" + str(sq) + "." )
                #LOG_MODE
                cur_dbmon.execute(ddl.ddl_check_diff("ck_database_diff","LOG_MODE"),vID_CHECKLIST=sq,vTARGET=TARGET)
                for row_ck in cur_dbmon.fetchall():
                    if str(row_ck[0]) != str(0) :
                        print ("["+ TARGET +"] Detectado alteracoes no LOG_MODE - " + str(row[0]) )
                        alarme.grava_alarme(TARGET,"DATABASE_D_LOG_MODE","Detectado alteracoes nas config de LOG_MODE do target " + TARGET + ", durante o checklist:" + str(sq) + "." )
                #FORCE_LOGGING / FLASHBACK
                cur_dbmon.execute(ddl.ddl_check_diff("ck_database_diff","FORCE_LOGGING,FLASHBACK_ON"),vID_CHECKLIST=sq,vTARGET=TARGET)
                for row_ck in cur_dbmon.fetchall():
                    if str(row_ck[0]) != str(0) :
                        print ("["+ TARGET +"] Detectado alteracoes no FORCE_LOGGING ou FLASHBACK - " + str(row[0]) )
                        alarme.grava_alarme(TARGET,"DATABASE_D_LOG_MODE","Detectado alteracoes nas config de Flashback / ForceLogging do target " + TARGET + ", durante o checklist:" + str(sq) + "." )
        #Atualiza registros dentro do ora_database
        cur_dbmon.execute(ddl.ddl_merge(monit),vID_CHECKLIST=sq,vTARGET=TARGET)
        conn_dbmon.commit()
        cur_dbmon.close()
        conn_dbmon.close()
        print("  Procedimento "+monit+ " em " +TARGET+ " realizado com sucesso")
        return("SUCESSO")
    except cx_Oracle.DatabaseError as e:
        error, = e.args
        #Gravando erro target,alarme,descricao
        print("!!!!  Erro registrado em " + TARGET + ", durante a execucao do monitoramento: " + str(monit) )
        alarme.grava_alarme(TARGET,"monit_"+monit,"Erro gerado: " + str(error.code) )
        print("Erro gerado: " + str(error.message) )
        cur_dbmon.close()
        conn_dbmon.rollback()
        conn_dbmon.close()
        return("FALHA")

#Monitoramento modificado em 03/08/2018
def get_database_services(target,sq):
    """Modulo DBMON - Verificacao de status de servicos que estao online no banco """
    monit="database_services"
    #Timeout da consulta
    timeout_consulta=30
    retencao_stage=7 #Dias em que os dados vao estar na stage
    TARGET = target.upper()
    ora_target = class_target(target.upper())
    dbmon = class_dbmon()
    conn_target = ora_target.conn
    cur_target = ora_target.get_cursor()
    conn_dbmon = dbmon.conn
    cur_dbmon = dbmon.new_cursor_dbmon()

    #Eliminando stage onde os dados ultrapassarem 7 dias
    cur_dbmon.execute("""delete from STAGE_DBSERVICE where target=:vTARGET""",vTARGET=target)
    conn_dbmon.commit()

    #Faco o insert na stage para avaliar a quantidade de servicos online e verificar se bate com a nova coleta
    cur_dbmon.execute("""insert into STAGE_DBSERVICE select target, name, count(1) quantidade from ORA_DBSERVICE where target=:vTARGET group by target,name """,vTARGET=target)
    conn_dbmon.commit()

    cur_dbmon.execute("""delete from ORA_DBSERVICE where target=:vTARGET""",vTARGET=target)
    conn_dbmon.commit()

    try:
        t = threading.Timer(timeout_consulta,conn_target.cancel)
        t.start()
        cur_target.execute(ddl.ddl_consulta(monit))
        t.cancel()
        for row in cur_target.fetchall():
            cur_dbmon.execute (ddl.ddl_insert(monit),
                               vTARGET=TARGET,
                               ID_CHECKLIST=int(sq),
                               INST_ID=row[0],
                               SERVICE_ID=row[1],
                               NAME=row[2]
                              )
        conn_dbmon.commit()
        #Encerrando conexao com o target
        cur_target.close()
        conn_target.close() 
        
        cur_dbmon.execute("""INSERT INTO STAGE_DBSERVICE 
                             SELECT TARGET, NAME, COUNT(1) QUANT 
                             FROM ORA_DBSERVICE 
                             WHERE TARGET=:vTARGET
                             GROUP BY TARGET, NAME""",vTARGET=target)
        
        #Verificando quais os targets que possuem servicos desligados
        cur_dbmon.execute(ddl.ddl_check("ck_database_services"),vTARGET=TARGET)
        for row in cur_dbmon.fetchall():
            alarme.grava_alarme(TARGET,"CHECK_DBSERVICES","Avaliar o servico " + str(row[0]) + " pois existem menos servicos onlines na(s) Instancia(s)." )
        cur_dbmon.close()
        conn_dbmon.close()
        #Saida padrao
        print("  Procedimento "+monit+ " em " +TARGET+ " realizado com sucesso")
        
    except cx_Oracle.DatabaseError as e:
        error, = e.args
        #Gravando erro target,alarme,descricao
        print("!!!!  Erro registrado em " + TARGET + " - ORA-" + str(error.code) )
        cur_dbmon.close()
        conn_dbmon.rollback()
        conn_dbmon.close()
        return("FALHA")

def get_database_instance(target,sq):
    """Modulo DBMON - Coleta de informacoes da instancia """
    monit="instance_info"
    TARGET = target.upper()
    ora_target = class_target(target.upper())
    dbmon = class_dbmon()
    conn_target = ora_target.conn
    cur_target = ora_target.get_cursor()
    conn_dbmon = dbmon.conn
    cur_dbmon = dbmon.new_cursor_dbmon()
    try:
        #Limpando stage
        cur_dbmon.execute("delete from STAGE_INSTANCE where target=:vTARGET",vTARGET=TARGET)
        conn_dbmon.commit()

        #Coletando informacoes do target
        cur_target.execute(ddl.ddl_consulta(monit))

        for row in cur_target.fetchall():
            cur_dbmon.execute(ddl.ddl_insert(monit),vTARGET=TARGET,
                             ID_CHECKLIST=int(sq),
                             INSTANCE_NUMBER=row[0], 
                             INSTANCE_NAME=row[1], 
                             HOST_NAME=row[2],
                             VERSION=row[3],
                             STARTUP_TIME=row[4], 
                             STATUS=row[5], 
                             PARALLEL=row[6],
                             THREAD=row[7],
                             ARCHIVER=row[8],
                             LOG_SWITCH_WAIT=row[9],
                             SHUTDOWN_PENDING=row[10],
                             DATABASE_STATUS=row[11],
                             INSTANCE_ROLE=row[12])
            conn_dbmon.commit()

        #Atualizo todas as instancias para o status de 'OFFLINE'
        
        cur_dbmon.execute("""update ORA_INSTANCE set STATUS='OFFLINE' WHERE TARGET=:vTARGET""",vTARGET=TARGET)
        conn_dbmon.commit()

        #Atualizar as informacoes das maquinas coletadas
        cur_dbmon.execute("""update ORA_INSTANCE set (HOST_NAME, STARTUP_TIME, STATUS, ARCHIVER, LOG_SWITCH_WAIT, SHUTDOWN_PENDING, DATABASE_STATUS, INSTANCE_ROLE) =
         ( SELECT HOST_NAME, 
                  STARTUP_TIME, 
                  STATUS, 
                  ARCHIVER,
                  LOG_SWITCH_WAIT, 
                  SHUTDOWN_PENDING, 
                  DATABASE_STATUS, 
                  INSTANCE_ROLE
           FROM STAGE_INSTANCE 
           WHERE ORA_INSTANCE.TARGET = STAGE_INSTANCE.TARGET 
           AND ORA_INSTANCE.INSTANCE_NAME = STAGE_INSTANCE.INSTANCE_NAME ) WHERE TARGET=:vTARGET
                                                                          and INSTANCE_NAME in 
                                                                              (select instance_name from stage_instance
                                                                              where target=:vTARGET) """,vTARGET=TARGET)
        conn_dbmon.commit()
        
        #Inserindo instancias que não existem.
        cur_dbmon.execute("""INSERT INTO ORA_INSTANCE
                             SELECT * FROM 
                                 STAGE_INSTANCE WHERE TARGET=:vTARGET and NOT EXISTS (SELECT 1 
                                                                                      FROM ORA_INSTANCE 
                                                                                      WHERE STAGE_INSTANCE.TARGET = ORA_INSTANCE.TARGET
                                                                                      AND STAGE_INSTANCE.INSTANCE_NUMBER = ORA_INSTANCE.INSTANCE_NUMBER 
                                                                                      AND STAGE_INSTANCE.INSTANCE_NAME = ORA_INSTANCE.INSTANCE_NAME)""",vTARGET=TARGET)
        conn_dbmon.commit()
        #Verificando status da instancia
        cur_dbmon.execute(ddl.ddl_check("ck_instance_status"),vTARGET=TARGET)
        for row in cur_dbmon.fetchall():
            alarme.grava_alarme(TARGET,"INSTANCE_DOWN","Avaliar a instancia " + row[0] + ", em: " + row[1] + " está com o status " + row[2]+ " durante o checklist:" + str(sq) + "." )
        
        cur_dbmon.execute("select instance_name from ora_instance where nvl(status,'OFFLINE') <>'OPEN' and target=:vTARGET",vTARGET=TARGET)
        for row in cur_dbmon.fetchall():
            alarme.grava_alarme(TARGET,"INSTANCE_DOWN","Avaliar a instancia "+ row[0] + ", que nao foi encontrada ONLINE em "+ TARGET)

        #Verificacao de instance startup
        cur_dbmon.execute(ddl.ddl_check("ck_instance_startup"),vTARGET=TARGET)
        for row in cur_dbmon.fetchall():
            alarme.grava_alarme(TARGET,"INSTANCE_STARTUP","Queda registrada em " + TARGET + ", InstanceName["+row[0]+"] com o startup registrado em "+ row[2] +" durante o checklist:" + str(sq) + "." )

    except cx_Oracle.DatabaseError as e:
        error, = e.args
        #Gravando erro target,alarme,descricao
        print("!!!!  Erro registrado em " + TARGET + " - ORA-" + str(error.code) )
        cur_dbmon.close()
        conn_dbmon.rollback()
        conn_dbmon.close()
        return("FALHA")

def get_users(target,sq):
    """Modulo DBMON - Coleta de informacoes de usuarios """
    monit="users"
    TARGET = target.upper()
    ora_target = class_target(target.upper())
    dbmon = class_dbmon()
    conn_target = ora_target.conn
    if ora_target.status_conn==0:
        print ('---->> Conexao com o target ' + ora_target.target + ' com problemas de conexao')
        exit(1)
    cur_target = ora_target.get_cursor()
    conn_dbmon = dbmon.conn
    cur_dbmon = dbmon.new_cursor_dbmon()

    try:
        #Limpando historico de acima de 90 dias de historico
        cur_dbmon.execute("delete from ORA_H_USERS WHERE DATA_COLETA < SYSDATE-90 AND TARGET=:vTARGET",vTARGET=TARGET)
        conn_dbmon.commit()

        #Copiando o conteudo para o historico
        cur_dbmon.execute("insert into ORA_H_USERS select * from ORA_USERS where TARGET=:vTARGET",vTARGET=TARGET)
        conn_dbmon.commit()

        cur_dbmon.execute("delete from ORA_USERS where TARGET=:vTARGET",vTARGET=TARGET)
        conn_dbmon.commit()

        #Se for 12 consulta a cdb_users ou 10/11 dba_users
        if ora_target.versao_bd >="12" :
          cur_target.execute(ddl.ddl_consulta("users_12c"))
          for row in cur_target.fetchall():
            cur_dbmon.execute(ddl.ddl_insert(monit),vTARGET=TARGET
                                      ,ID_CHECKLIST=int(sq)
                                      ,USERNAME=row[0]
                                      ,ACCOUNT_STATUS=row[1]
                                      ,LOCK_DATE=row[2]
                                      ,EXPIRY_DATE=row[3]
                                      ,CREATED=row[4]
                                      ,PROFILE=row[5]
                                      ,COMMON=row[6]
                                      ,LAST_LOGIN=row[7] ) 
        else:
          cur_target.execute(ddl.ddl_consulta("users_10g"))
          for row in cur_target.fetchall():
            cur_dbmon.execute(ddl.ddl_insert(monit),vTARGET=TARGET
                                      ,ID_CHECKLIST=int(sq)
                                      ,USERNAME=row[0]
                                      ,ACCOUNT_STATUS=row[1]
                                      ,LOCK_DATE=row[2]
                                      ,EXPIRY_DATE=row[3]
                                      ,CREATED=row[4]
                                      ,PROFILE=row[5]
                                      ,COMMON=''
                                      ,LAST_LOGIN='' )
        conn_dbmon.commit()
    except cx_Oracle.DatabaseError as e:
        error, = e.args
        #Gravando erro target,alarme,descricao
        print("!!!!  Erro registrado em " + TARGET + " - ORA-" + str(error.code) + str(error) )
        cur_dbmon.close()
        conn_dbmon.rollback()
        conn_dbmon.close()
        return("FALHA")

def get_SegInfo_CkPrograms(target):
    """Procedimento da seguranca para coleta de sessoes com programas de manipulação de dados"""
    TARGET = target.upper()
    ora_target = class_target(target.upper())
    dbmon = class_dbmon()
    conn_target = ora_target.conn
    try:
        #Disparo da consulta
        cur_target = ora_target.get_cursor()
        cur_target.execute(ddl.ddl_consulta('SegInfo_CkPrograms'))
        status_processa=dbmon.processa_rs(ora_target.target,'SegInfo_CkPrograms',cur_target)
        if status_processa == 1:
            print("  Procedimento para coleta de programas indevidos da SI em " +TARGET+ ", realizado com sucesso")
        else:
            print("  [ERRO] Erro ao processar programas indevidos")
        
    except cx_Oracle.DatabaseError as e:
        error, = e.args
        #Gravando erro target,alarme,descricao
        print("!!!!  Erro registrado em " + TARGET + " - ORA-" + str(error.code) + str(error) )
        cur_dbmon.close()
        conn_dbmon.rollback()
        conn_dbmon.close()
        return("FALHA")

def dispara_coleta(target,monitoramento,seq):
    erro=0
    id_checklist=int(seq)
    #Coleta de informacoes de tablespaces de dados e Undo
    if monitoramento=="tablespace_dados":
        proc=get_tablespace_dados(target,seq)
        if proc=="FALHA":
            print("[ERRO]["+monitoramento+"]"+"["+target+"]")

    #Coleta de informacoes de tablespaces temporarias
    if monitoramento=="tablespace_temp":
        proc=get_tablespace_temp(target,seq)
        if proc=="FALHA":
            print("[ERRO]["+monitoramento+"]"+"["+target+"]")
        
    #Coleta de indices com status de Unusable
    if monitoramento=="index_unusable":
        proc=get_index_unusable(target,seq)
        if proc=="CANCELADO":
            print("Monitoramento cancelado para o ambiente " + target)
        if proc=="FALHA":
            print("[ERRO]["+monitoramento+"]"+"["+target+"]")

    #Coleta de espaco disponivel nos diskgroups
    if monitoramento=="asm_diskgroup":
        proc=get_asm_diskgroup(target,seq)
        if proc=="FALHA":
            print("[ERRO]["+monitoramento+"]"+"["+target+"]")
        check_asm_diskgroup(target)

    #Coleta de informacoes de backup full
    if monitoramento=="rman_backup_full":
        proc=get_rman_backup_full(target,seq)
        if proc=="FALHA":
            print("[ERRO]["+monitoramento+"]"+"["+target+"]")

    #Coleta de informacoes de backup de archive
    if monitoramento=="rman_backup_arch":
        #proc=get_rman_backup_arch(target,seq)
        #if proc=="FALHA":
        #    print("[ERRO]["+monitoramento+"]"+"["+target+"]")
        print("Monitoramento em desenvolvimento")

    #Coleta de dataguards
    if monitoramento=="dataguard_info":
        proc=get_dataguard_info(target,seq)
        if proc=="FALHA":
            print("[ERRO]["+monitoramento+"]"+"["+target+"]")

    #Coleta de gaps do dataguard
    if monitoramento=="dataguard_gap":
        proc=get_dataguard_gap(target,seq)
        if proc=="FALHA":
            print("[ERRO]["+monitoramento+"]"+"["+target+"]")

    #Coleta do produto Spatial and Graphs
    if monitoramento=="option_spatial":
        proc=get_option_spatial(target,seq)
        if proc=="FALHA":
            print("[ERRO]["+monitoramento+"]"+"["+target+"]")
    
    #Coleta das informacoes do database
    if monitoramento=="database_info":
        proc=get_database_info(target,seq)
        if proc=="FALHA":
              print("[ERRO]["+monitoramento+"]"+"["+target+"]")

    #Coleta de informacoes de servico
    if monitoramento=="database_services":
        proc=get_database_services(target,seq)
        if proc=="FALHA":
              print("[ERRO]["+monitoramento+"]"+"["+target+"]")

    #Coleta de informacoes de instancia
    if monitoramento=="database_instance":
        proc=get_database_instance(target,seq)
        if proc=="FALHA":
              print("[ERRO]["+monitoramento+"]"+"["+target+"]")

    #Coleta de usuarios
    if monitoramento=="users":
        proc=get_users(target,seq)
        if proc=="FALHA":
              print("[ERRO]["+monitoramento+"]"+"["+target+"]")

    #Coleta Seg da Informacao
    if monitoramento=="SegInfo_CkPrograms":
        proc=get_SegInfo_CkPrograms(target)
        if proc=="FALHA":
            print("[ERRO]["+monitoramento+"]"+"["+target+"]")