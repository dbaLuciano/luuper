import monitoramentos as monit
import engine_checklist as checklist
import cx_Oracle
import daemon
import os
import time
import sys ##Fazer leitura do parametro de entrada
valor = sys.argv[1]

if __name__ == '__main__':

    print ("=====================================================================================\n\n")
    print ("Executando checklist "+ str(valor) ) 
    checklist=checklist.checklist(valor,'A',0)
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
    
    for i in range(15):
        pid = os.fork()
        if pid != 0:
            print("Processo deamon id %d criado" % pid)
        else:
            daemon.checklist_dbmond(checklist.id_checklist,i)
            os._exit(0)
    
    checklist.status="Executando"
    checklist.tarefa="processando_fila"
    checklist.update_status()
    quantidade_pendente=0
    i=0
    while quantidade_pendente!=0:
        quantidade_pendente=checklist.check_pendente_fila()
        print("[ " + str(quantidade_pendente) + " ] Pendentes para execucao")
        time.sleep(10)
        if i >= 540: ##estiver a 90minutos em execucao
            checklist.status="CANCELADO"
            checklist.tarefa="TEMPO_EXCEDIDO"
            checklist.update_status()
            print("Checklist interrompido devido tempo de execucao excedido")
            exit(1)
    checklist.status="PROCESSADO"
    checklist.tarefa="FINALIZADO"
    checklist.update_status()
    print("Procedimento de execucao do checklist encerrada com sucesso")