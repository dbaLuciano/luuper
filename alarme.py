import cx_Oracle
from settings import Settings

def grava_alarme(target,alarme,descricao):
    """Grava um novo alarme"""
    #conectando
    config_dbmon = Settings()
    dbmon_string = config_dbmon.conn_dbmon
    conn_dbmon = cx_Oracle.Connection(dbmon_string)
    cur_dbmon = cx_Oracle.Cursor(conn_dbmon)
    #check se nao existe
    check=check_alarme_ativo(target,alarme)
    if check==0:
        #cadastro novo alarme
        strinsert="""
        INSERT INTO TARGET_ALARME (TARGET, ALARME, DATA_ALARME, DESCRICAO, ISRECONHECIDO)
              VALUES (:binTarget, 
                      :binAlarme, 
                      sysdate, 
                      :binDescricao, 
                      0)
         """
        cur_dbmon.execute(strinsert,
                          binTarget=target,
                          binAlarme=alarme,
                          binDescricao=descricao)
        conn_dbmon.commit()
    if check==1:
        #atualizo o alarme informando a data do alarme atualizada
        strupdate=""" UPDATE TARGET_ALARME SET 
                            DATA_ULTIMO_ALARME=SYSDATE 
                      WHERE TARGET=:binTarget 
                      AND ALARME=:binAlarme 
                      AND ISRECONHECIDO=0 """
        cur_dbmon.execute(strupdate
                          ,binTarget=target
                          ,binAlarme=alarme
                         )
        conn_dbmon.commit()
    cur_dbmon.close()
    conn_dbmon.close()


def check_alarme_ativo(target,alarme):
    """Verifica se existe um alarme ativado para o ambiente sem reconhecimento Ret[0]-Nao existe Ret[1]-Existem Alarmes """
    config_dbmon = Settings()
    dbmon_string = config_dbmon.conn_dbmon
    conn_dbmon = cx_Oracle.Connection(dbmon_string)
    cur_dbmon = cx_Oracle.Cursor(conn_dbmon)
    strsql="""SELECT count(1) QUANTIDADE
              FROM TARGET_ALARME
              WHERE TARGET=:binTarget
              AND ISRECONHECIDO=0
              AND ALARME=:binAlarme"""
    cur_dbmon.execute(strsql
                      ,binTarget=target
                      ,binAlarme=alarme)
    for row in cur_dbmon.fetchall():
        quant=row[0]
    #Se quantidade != 0 entao existe alarme
    if quant==0:
        return(0)
    else:
        return(1)
    cur_dbmon.close()
    conn_dbmon.close()


def finaliza_alarme(target,alarme,texto):
    config_dbmon = Settings()
    dbmon_string = config_dbmon.conn_dbmon
    conn_dbmon = cx_Oracle.Connection(dbmon_string)
    cur_dbmon = cx_Oracle.Cursor(conn_dbmon)
    strsql="""UPDATE TARGET_ALARME SET 
                            ISRECONHECIDO=1,
                            DATA_RECONHECIMENTO=sysdate,
                            RECONHECIDO_POR='PYTHON',
                            TEXTO_CONCLUSAO=:descricao
                    WHERE TARGET=:binTarget 
                    AND ALARME=:binAlarme 
                    AND ISRECONHECIDO=0"""
    cur_dbmon.execute(strsql
                    ,binTarget=target
                    ,binAlarme=alarme
                    ,descricao=texto)
    conn_dbmon.commit()
    return("OK")
    cur_dbmon.close()
    conn_dbmon.close()