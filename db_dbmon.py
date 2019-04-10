import cx_Oracle
import ddl_dbmon as ddl

class dbmon():
    def __init__(self):
       #Definicoes de conexoes
       scanname="scandbmon"
       service="dbmon"
       username="dbmon"
       password="dbmon123"
       porta="1521"
       service="dbmon"
       strConnDbmon = username + "/" + password + "@" + scanname + ":" + porta + "/" + service
       self.conn  = cx_Oracle.Connection(strConnDbmon)

    def new_cursor_dbmon(self):
        "Procedimento para criar um cursor para a classe dbmon"
        return(cx_Oracle.Cursor(self.conn))
    
    def commit_dbmon(self):
        "Realiza um commit dentro da conexao do dbmon"
        self.conn_dbmon.commit()
    
    def close_dbmon(self):
        "Encerra conexao com o servidor"
        self.conn_dbmon.close()
    
    def rollback_dbmon(self):
        "Executa rollback da transacao"
        self.conn_dbmon.rollback()

    def encerra_cursor_dbmon(self,cursor):
        cursor.close()

    def exists_target(self,target,tipo):
        """Valida a existencia de um target"""
        cursor = dbmon.new_cursor_dbmon(self)
        cursor.execute("select count(1) from target where target=:vTARGET and target_tipo=:vTIPO",vTARGET=target,vTIPO=tipo)
        for row in cursor.fetchall():
            quant=row[0]
        if quant== 0 :
            return(0)
        else:
            return(1)

    def get_ora_info(self,target):
        """Coleta informacoes basicas do ambiente retornando uma tupla das informacoes
            DATABASE, SCAN_NAME, SERVICE_NAME, DESCRICAO, PORTA, TIPO_AMBIENTE, CLASSIFICACAO, VERSAO_BD
        """
        strsql="""select 
                        database, 
                        scan_name, 
                        service_name, 
                        descricao, 
                        porta, 
                        tipo_ambiente_desc, 
                        classificacao,
                        versao_bd
                from all_databases where target=:vTARGET"""
        cursor = dbmon.new_cursor_dbmon(self)
        cursor.execute(strsql,vTARGET=target.upper())
        for row in cursor.fetchall():
            database=row[0]
            scan_name=row[1]
            service_name=row[2]
            descricao=row[3]
            porta=row[4]
            tipo_ambiente=row[5]
            classificacao=row[6]
            versao_bd=row[7]
        return(database,scan_name, service_name, descricao, porta, tipo_ambiente, classificacao, versao_bd)

    def get_ora_string(self,target):
          """ retorna informacoes de conexao para ser utilizados exclusivamente em target.py """
          cursor = dbmon.new_cursor_dbmon(self)
          cursor.execute("select dbmon_conn(:vTARGET) string, scan_name,  porta, service_name from all_databases where target=:vTARGET",vTARGET=target.upper())
          for row in cursor.fetchall():
              conn = row[0]
              scanname = row[1]
              porta = row[2]
              service = row[3]
          cursor.close()
          return(conn + "@" + scanname + ":" + porta + "/" + service)
    
    def processa_rs(self,TARGET,rsname,recordSet):
        try:
           if rsname=="SegInfo_CkPrograms":
               cursor=self.new_cursor_dbmon()
               cursor.execute("delete from Stage_SegInfo_CkPrograms where target=:vTARGET",vTARGET=TARGET)
               self.conn.commit()
               for row in recordSet.fetchall():
                    cursor.execute("""insert into Stage_SegInfo_CkPrograms 
                                       (TARGET,
                                        USERNAME,
                                        OSUSER,
                                        MACHINE,
                                        PROGRAM,
                                        MODULE,
                                        ACTION) VALUES (:vTARGET,
                                                        :vUSERNAME,
                                                        nvl(:vOSUSER,'-'),
                                                        nvl(:vMACHINE,'-'),
                                                        nvl(:vPROGRAM,'-'),
                                                        nvl(:vMODULE,'-'),
                                                        nvl(:vACTION,'-') )""",vTARGET=TARGET
                                                                    ,vUSERNAME=row[0]
                                                                    ,vOSUSER=row[1]
                                                                    ,vMACHINE=row[2]
                                                                    ,vPROGRAM=row[3]
                                                                    ,vMODULE=row[4]
                                                                    ,vACTION=row[5])
               self.conn.commit()
               cursor.execute(ddl.ddl_merge('SegInfo_CkPrograms'),vTARGET=TARGET)
               self.conn.commit()
               return(1)
        except cx_Oracle.DatabaseError as e:
           error, = e.args
           #Gravando erro target,alarme,descricao
           print("!!!!  Erro registrado em " + TARGET + " - ORA-" + str(error.code) + str(error) )
           return(0)

    def limpa_stage(self,TARGET,stage):
        """Limpa stage -  Stage:[database]"""
        try:
           if stage=="database":
               cursor=self.new_cursor_dbmon()
               cursor.execute("delete from stage_database where target=:vTARGET",vTARGET=TARGET)
               self.conn.commit()
               del(cursor)
               return(1)
        except cx_Oracle.DatabaseError as e:
           error, = e.args
           #Gravando erro target,alarme,descricao
           print("[!!! ERRO !!!][db_dbmon][limpa_stage]["+Stage+"][" + TARGET + "] " + str(error.code) + str(error) )
           return(0)

    def build_inicial(self,TARGET,build):
        """ Estagio inicial para processamento de dados de tablespaces """
        if build=="tablespace_dados"
           try:
              cursor = self.new_cursor_dbmon()
              cursor.execute("insert into ora_h_tablespace select * from ora_tablespace where target=:vTARGET",vTARGET=TARGET)
              self.conn.commit()
              cursor.execute("delete from ora_tablespace where target=:vTARGET",vTARGET=TARGET)
              self.conn.commit()
           except cx_Oracle.DatabaseError as e:
              error, = e.args
              #Gravando erro target,alarme,descricao
              print("[!!! ERRO !!!][db_dbmon][processa_tablespace][" + TARGET + "] " + str(error.code) + str(error) )
              return(0)