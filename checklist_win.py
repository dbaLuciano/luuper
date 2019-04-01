import monitoramentos as monit
import engine_checklist as checklist
import cx_Oracle
import daemon
import os
import time

if __name__ == '__main__':

    print ("=====================================================================================\n\n")
    print ("CHECKLIST - 7hrs \n\n")
    checklist=checklist.checklist('checklist_7hrs','A',0)
    print ("=====================================================================================\n\n")

    dbmon_string = "dbmon/dbmon123@scandbmon:1521/dbmon"
    conn_dbmon = cx_Oracle.Connection(dbmon_string)
    cur_dbmon = cx_Oracle.Cursor(conn_dbmon)

    monitoramentos=[]
    targets=[]
    monitoramentos=checklist.get_monitoramentos()
    targets=checklist.get_targets()

    print ("Targets: " + str(len(targets)))

    print("\nMonitoramentos que seram executados")
    for i in range(len(monitoramentos)):
        print('\t'+monitoramentos[i])

    #Execucao serial - descontinuada em 26/03/2018
    for target in range(len(targets)):
        target_processando=targets[target]
        checklist.atualiza_fila(str(targets[target]),"EXECUTANDO")
        for monitor in range(len(monitoramentos)):
             monitoramento_processando=monitoramentos[monitor]
             coleta(targets[target] ,str(monitoramentos[monitor]) ,checklist.id_checklist )
        checklist.atualiza_fila(target_processando,"PROCESSADO")
