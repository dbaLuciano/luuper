import cx_Oracle

class checklist():
    def __init__(self,ck_name,tipo_execucao,seq):
        #Verificando parametros informados
        self.ident_checklist="Tesla::v1"
        self.id_checklist=seq
        self.checklist=ck_name.lower()
        self.tipo_execucao=tipo_execucao.upper()
        
        if self.id_checklist==0 :
            self.inicializa_checklist()
        else:
            self.set_checklist()

    def set_checklist(self):
        dbmon_string = "dbmon/dbmon123@scandbmon:1521/dbmon"
        conn_dbmon = cx_Oracle.Connection(dbmon_string)
        cur_dbmon = cx_Oracle.Cursor(conn_dbmon)
        strsql="select CHECKLIST, IDENTIFICACAO, TIPO_EXECUCAO, STATUS, TAREFA from controle_checklist where id_checklist='"+str(self.id_checklist)+"'"
        cur_dbmon.execute(strsql)
        for row in cur_dbmon.fetchall():
            self.checklist=row[0]
            self.ident_checklist=row[1]
            self.tipo_execucao=row[2]
            self.status=row[3]
            self.tarefa=row[4]
    
    def inicializa_checklist(self):
        self.status="Executando"
        self.tarefa="Preparando"
        #Verificando parametros informados (Retorno SUCESSO / FALHA)
        check=self.check_param()

        if check=="SUCESSO":
            dbmon_string = "dbmon/dbmon123@scandbmon:1521/dbmon"
            conn_dbmon = cx_Oracle.Connection(dbmon_string)
            cur_dbmon = cx_Oracle.Cursor(conn_dbmon)
            if self.tipo_execucao=="A":
                self.novo_idChecklist()
            #Salvando checklist
            verificacao = self.salva_checklist()
            #Atualizando status - build_queue
            self.tarefa="build_queue"
            self.update_status()
            self.prepara_fila()
            #Verificando quantidade de targets a serem processados
            quant=self.check_quantidade()
            print ("Targets a serem processados: " + str(quant) )
        else:
            print ("\n\nParametros informados incorretamente\n\n")
            self.about()

    def check_param(self):
        dbmon_string = "dbmon/dbmon123@scandbmon:1521/dbmon"
        conn_dbmon = cx_Oracle.Connection(dbmon_string)
        cur_dbmon = cx_Oracle.Cursor(conn_dbmon)
        strsql="select count(1) quantidade from checklist where checklist='"+self.checklist+"'"
        cur_dbmon.execute(strsql)
        for row in cur_dbmon.fetchall():
            quant=row[0]
        
        if quant==0 :
            print("Checklist informado não existe!")
            return("FALHA")
        
        if self.tipo_execucao not in ["A","M"] :
            print("Parametros de tipo execucao informado incorretamente. [A]Automatico / [M]Execucao Manual")
            return("FALHA")
        return("SUCESSO")

    def novo_idChecklist (self):
        """ Procedimento para gerar um novo id de checklist """
        dbmon_string = "dbmon/dbmon123@scandbmon:1521/dbmon"
        conn_dbmon = cx_Oracle.Connection(dbmon_string)
        cur_dbmon = cx_Oracle.Cursor(conn_dbmon)
        #Id_Checklist
        try:
            strsql="select TESLA_SEQ.NEXTVAL from dual"
            cur_dbmon.execute(strsql)
            for row in cur_dbmon.fetchall():
                identificacao=row[0]
            
            self.id_checklist = int(identificacao)
            conn_dbmon.commit()
            cur_dbmon.close()
            conn_dbmon.close()
        except:
            print("Erros na geracao de um novo id para o checklist")
            exit(1)
    
    def salva_checklist(self):
        """ Procedimento para salvar o checklist realizando insert na tabela controle_checklist """
        dbmon_string = "dbmon/dbmon123@scandbmon:1521/dbmon"
        conn_dbmon = cx_Oracle.Connection(dbmon_string)
        cur_dbmon = cx_Oracle.Cursor(conn_dbmon)
        try:
            strsql="""INSERT INTO CONTROLE_CHECKLIST(ID_CHECKLIST
                                                    ,CHECKLIST
                                                    ,IDENTIFICACAO
                                                    ,TIPO_EXECUCAO 
                                                    ,STATUS
                                                    ,TAREFA
                                                    ,DATA_INICIAL
                                                    ,DATA_FINAL
                                                    ,DATA_MODIFICACAO )
                                             VALUES (:binID_CHECKLIST
                                                    ,:binCHECKLIST
                                                    ,:binIDENTIFICACAO
                                                    ,:binTIPO_EXECUCAO 
                                                    ,:binSTATUS
                                                    ,:binTAREFA
                                                    ,sysdate
                                                    ,null
                                                    ,sysdate)"""
            print ("Salvando execucao do checklist na tabela de controles")
            print ("IdChecklist..........................: " + str(self.id_checklist) )
            print ("Checklist............................: " + self.checklist)
            print ("Identificacao........................: " + self.ident_checklist)
            print ("Execucao [A-Automatica]/[M-Manual]...: " + self.tipo_execucao)
            print ("Status...............................: " + self.status)
            print ("Tarefa...............................: " + self.tarefa)
            cur_dbmon.execute(strsql
                              ,binID_CHECKLIST=self.id_checklist
                              ,binCHECKLIST=self.checklist
                              ,binIDENTIFICACAO=self.ident_checklist
                              ,binTIPO_EXECUCAO=self.tipo_execucao
                              ,binSTATUS=self.status
                              ,binTAREFA=self.tarefa)
            conn_dbmon.commit()
            cur_dbmon.close()
            conn_dbmon.close()
            return(0)
        except:
           print("Erro na execucao do insert na tabela controle_checklist")
           conn_dbmon.rollback()
           cur_dbmon.close()
           conn_dbmon.close()
           return(1)

    def prepara_fila(self):
        """ Procedimento para geracao da fila de ambientes para serem realizados o checklist """
        dbmon_string = "dbmon/dbmon123@scandbmon:1521/dbmon"
        conn_dbmon = cx_Oracle.Connection(dbmon_string)
        cur_dbmon = cx_Oracle.Cursor(conn_dbmon)

        try:
            strsql="""
            SELECT TARGET,TIPO_AMBIENTE
                   FROM ALL_TARGETS_MONITORAMENTO
                   WHERE TIPO_AMBIENTE in (SELECT TIPO_AMBIENTE 
                                        FROM CHECKLIST_TIPOAMBIENTE 
                                        WHERE CHECKLIST='"""+self.checklist+"""'
                                        ) """
            cur_dbmon.execute(strsql)
            strIns=""" INSERT INTO CONTROLE_CHECKLIST_FILA (
                                                           ID_CHECKLIST
                                                           ,TARGET
                                                           ,TIPO_AMBIENTE
                                                           ,STATUS)VALUES
                                                           ( :binID_CHECKLIST
                                                             ,:binTARGET
                                                             ,:binTIPO_AMBIENTE
                                                             ,'AGUARDANDO'
                                                            )"""
            cur_insDbmon = cx_Oracle.Cursor(conn_dbmon)
            for row in cur_dbmon.fetchall():
                cur_insDbmon.execute(strIns
                                     ,binID_CHECKLIST=self.id_checklist
                                     ,binTARGET=row[0]
                                     ,binTIPO_AMBIENTE=row[1]
                                     )
            conn_dbmon.commit()
            cur_dbmon.close()
            cur_insDbmon.close()
            conn_dbmon.close()
            print("Fila concluida com sucesso!")
        except cx_Oracle.DatabaseError as error :
            print("\n\n[ERRO] Problemas na gravacao da fila")
            print("Oracle-Error-Code:", error.code)
            print("Oracle-Error-Message:", error.message)
            print("\n\n[FIM_ERRO]")
 
    def update_status(self):
        """ Procedimento para atualizar status e tarefa do checklist - Tabela Controle_checklist"""
        dbmon_string = "dbmon/dbmon123@scandbmon:1521/dbmon"
        conn_dbmon = cx_Oracle.Connection(dbmon_string)
        cur_dbmon = cx_Oracle.Cursor(conn_dbmon)
        try:
            if self.status!="PROCESSADO":
                strsql=""" UPDATE CONTROLE_CHECKLIST SET status=:binStatus, tarefa=:binTarefa, DATA_MODIFICACAO=sysdate where id_checklist=:binIDCHECKLIST"""
            else:
                strsql=""" UPDATE CONTROLE_CHECKLIST SET status=:binStatus, tarefa=:binTarefa, data_final=sysdate, DATA_MODIFICACAO=sysdate where id_checklist=:binIDCHECKLIST"""
            cur_dbmon.execute(strsql
                              ,binTarefa=self.tarefa
                              ,binStatus=self.status
                              ,binIDCHECKLIST=self.id_checklist
                              )
            conn_dbmon.commit()
            cur_dbmon.close()
            conn_dbmon.close()
        except:
            print ("Problemas ao atualizar a tabela controle_checklist Metodo:update_status")

    def check_quantidade(self):
        """ Procedimento para validar quantidade de objetos a serem processados - Tabela CONTROLE_CHECKLIST_FILA"""
        dbmon_string = "dbmon/dbmon123@scandbmon:1521/dbmon"
        conn_dbmon = cx_Oracle.Connection(dbmon_string)
        cur_dbmon = cx_Oracle.Cursor(conn_dbmon)
        strsql=""" select count(1) from controle_checklist_fila where id_checklist=:binIDCHECKLIST"""
        cur_dbmon.execute(strsql,
                          binIDCHECKLIST=self.id_checklist)
        for row in cur_dbmon.fetchall():
            quant=row[0]
        return(int(quant) )

    def about(self):
        print ("""
              ==================================================================================================
              Procedimento checklist versao 1.0
              Desenvolvido por Luciano Alvarenga Maciel Pires
              Data: 20/03/2018
              
              Para utilizar o procedimento siga os passos abaixo:
              checklist('<checklist_desejado>','<M/A>')

              Sendo:
                <checklist_desejado> - Checklist a ser realizado. 
                    Obs. Em caso de duvidas consulte a tabela checklist que exibe os 
                    checklists cadastrados

                <M/A> - Define a forma de execucao do procedimento, sendo:
                        M - Manual (execucao para utilizacao dos DBAs e para fins de levantamento)
                        A - Automatico (somente para uso exclusivo de execucao agendada)
              ==================================================================================================
               """)

    def get_monitoramentos(self):
        """ Pega lista de monitoramentos """
        dbmon_string = "dbmon/dbmon123@scandbmon:1521/dbmon"
        conn_dbmon = cx_Oracle.Connection(dbmon_string)
        cur_dbmon = cx_Oracle.Cursor(conn_dbmon)
        strsql="select monitoramento from checklist_monitoramento where checklist='" + self.checklist + "' order by seq "
        monitoramentos=[]
        cur_dbmon.execute(strsql)
        for row in cur_dbmon.fetchall():
            monitoramentos.append(row[0])
        return(monitoramentos)
    
    def get_targets(self):
        """ Pega os targets que serao executados no checklist"""
        dbmon_string = "dbmon/dbmon123@scandbmon:1521/dbmon"
        conn_dbmon = cx_Oracle.Connection(dbmon_string)
        cur_dbmon = cx_Oracle.Cursor(conn_dbmon)
        strsql="select target from controle_checklist_fila where id_checklist='" + str(self.id_checklist) +"' and status='AGUARDANDO'"
        targets=[]
        cur_dbmon.execute(strsql)
        for row in cur_dbmon.fetchall():
            targets.append(row[0])
        return(targets)

    def atualiza_target_fila(self, target, status):
        dbmon_string = "dbmon/dbmon123@scandbmon:1521/dbmon"
        conn_dbmon = cx_Oracle.Connection(dbmon_string)
        cur_dbmon = cx_Oracle.Cursor(conn_dbmon)
        if status=="EXECUTANDO":
           str_update= "update controle_checklist_fila set status='" + str(status.upper()) + "', data_inicial=sysdate "
           str_update+="where target='"+ str(target) +"' and id_checklist='"+str(self.id_checklist)+"'"
           cur_dbmon.execute(str_update)
        if status=="PROCESSADO":
           str_update= "update controle_checklist_fila set status='" + str(status.upper()) + "', data_final=sysdate "
           str_update+="where target='"+ str(target) +"' and id_checklist='"+str(self.id_checklist)+"'"
           cur_dbmon.execute(str_update)
        conn_dbmon.commit()
        cur_dbmon.close()
        conn_dbmon.close()

    def check_pendente_fila(self):
        """ Procedimento para retornar a quantidade de itens pendente no checklist """
        dbmon_string = "dbmon/dbmon123@scandbmon:1521/dbmon"
        conn_dbmon = cx_Oracle.Connection(dbmon_string)
        cur_dbmon = cx_Oracle.Cursor(conn_dbmon)
        strsql=""" select count(1) from controle_checklist_fila where ID_CHECKLIST=:binIDCHECKLIST and status not in ('PROCESSADO','CANCELADO') """
        cur_dbmon.execute(strsql,
                          binIDCHECKLIST=str(self.id_checklist) )
        for row in cur_dbmon.fetchall():
            quant=row[0]
        cur_dbmon.close()
        conn_dbmon.close()
