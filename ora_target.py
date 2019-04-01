import cx_Oracle
from db_dbmon import dbmon as db_dbmon

class ora_target():
    def __init__(self,target):
        """Class de target """
        self.target=target.upper()
        self.status_conn = 0
        self.database=""
        self.scanname=""
        self.service_name=""
        self.descricao=""
        self.porta=""
        self.tipo_ambiente=""
        self.classificacao=""
        self.versao_bd=""
        self.conn=""
        self.get_dbmon_target_info()
                
    def get_dbmon_target_info(self):
        idbmon = db_dbmon()
        """Funcao para coletar informacoes do target no DBMON utilizando db_dbmon """
        self.database, self.scanname, self.service_name, self.descricao, self.porta, self.tipo_ambiente, self.classificacao, self.versao_bd = idbmon.get_ora_info(self.target)
        string=idbmon.get_ora_string(self.target)
        try:
           self.conn = cx_Oracle.Connection(string)
           self.status_conn = 1
        except cx_Oracle.DatabaseError as e:
           error, = e.args
           print ("")
           print ("Atenção!! Conexao com o target "+ self.target +" com problemas!")
           print ("Error: " + error.message)
           print ("")
           self.status_conn = 0 
           
        
    def print_target_info(self):
        """ Imprimi informacoes basicas do target """
        print ("")
        print ("  ----------- " + self.target+ " -----------")
        print ("  Database.......: " + self.database)
        print ("  ScanName.......: " + self.scanname)
        print ("  Service........: " + self.service_name)
        print ("  Descricao......: " + self.descricao)
        print ("  Porta..........: " + self.porta)
        print ("  Tipo Ambiente..: " + self.tipo_ambiente)
        print ("  Versao.........: " + self.versao_bd)

    def get_cursor(self):
        """Funcao criada para retornar um cursor cx_Oracle """
        return( cx_Oracle.Cursor(self.conn) ) 
