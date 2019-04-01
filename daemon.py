import cx_Oracle
from ora_target import ora_target as class_target
import alarme
from monitoramentos import dispara_coleta as coleta

def pega_target(id_checklist,id_fila):
    dbmon_string = "dbmon/dbmon123@scandbmon:1521/dbmon"
    conn_dbmon = cx_Oracle.Connection(dbmon_string)
    cur_dbmon =  cx_Oracle.Cursor(conn_dbmon)
    target = cur_dbmon.var(cx_Oracle.FIXED_CHAR)
    cur_dbmon.callproc("NXT_TARGET_CHECKLIST",(int(id_checklist),int(id_fila),target))
    target=target.getvalue()
    target=target.lstrip()
    target=target.rstrip()
    return target
    cur_dbmon.close()
    conn_dbmon.close()

def marca_processado(id_checklist, target, id_fila):
    """ Atualiza fila de processos marcando o target como processado """
    dbmon_string = "dbmon/dbmon123@scandbmon:1521/dbmon"
    conn_dbmon = cx_Oracle.Connection(dbmon_string)
    cur_dbmon =  cx_Oracle.Cursor(conn_dbmon)
    strupd="""update controle_checklist_fila set 
                        status='PROCESSADO', 
                        data_final=sysdate,
                        id_fila='"""+str(id_fila)+"""' 
                    where id_checklist='"""+ str(id_checklist) +"""' 
                    and target='"""+str(target) +"""'"""
    cur_dbmon.execute(strupd)
    conn_dbmon.commit()

def marca_cancelado(id_checklist, target, id_fila):
    """ Atualiza fila de processos marcando o target como processado """
    dbmon_string = "dbmon/dbmon123@scandbmon:1521/dbmon"
    conn_dbmon = cx_Oracle.Connection(dbmon_string)
    cur_dbmon =  cx_Oracle.Cursor(conn_dbmon)
    strupd="""update controle_checklist_fila set 
                        status='CANCELADO', 
                        data_final=sysdate,
                        id_fila='"""+str(id_fila)+"""' 
                    where id_checklist='"""+ str(id_checklist) +"""' 
                    and target='"""+str(target) +"""'"""
    cur_dbmon.execute(strupd)
    conn_dbmon.commit()


def checklist_dbmond(id_checklist,id_fila):
   dbmon_string = "dbmon/dbmon123@scandbmon:1521/dbmon"
   conn_dbmon = cx_Oracle.Connection(dbmon_string)
   cur_dbmon =  cx_Oracle.Cursor(conn_dbmon)
   
   str_monitoramentos=""" 
    select monitoramento 
    from checklist_monitoramento 
    where checklist = (select checklist 
                       from controle_checklist 
                       where id_checklist='"""+ str(id_checklist) +"""')
    order by seq"""
   cur_dbmon.execute(str_monitoramentos)
   
   #Lista de monitoramentos
   monitoramentos=[]

   for row in cur_dbmon.fetchall():
        monitoramentos.append(row[0])
   
   target=""
   target=pega_target(id_checklist,id_fila)

   while target!="0":
        TARGET = class_target( target.upper() )
        if TARGET.status_conn == 0 :
            print("Conexao com o target "+ target + " nao estabelecida.")
            alarme.grava_alarme(target,"CHECK_CONN","Problemas ao estabelecer conexao com o target")
            marca_cancelado(id_checklist,target,id_fila)
        else:
            for monitor in range(len(monitoramentos)):
                coleta(target ,str(monitoramentos[monitor]) ,str(id_checklist) )
                marca_processado(id_checklist,target,id_fila)
                #Atualiza ultima coleta
        #Pega o proximo target ou "0" para cancelar
        target=pega_target(id_checklist,id_fila)
   
   print("Fila "+ str(id_fila) + "encerrada (Daemon V2) ")