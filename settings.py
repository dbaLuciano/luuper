class Settings():
    """ Armazena configuracoes do dbmon global """

    def __init__(self):
        #valores de conexao
        self.db_user="dbmon"
        self.db_pwd="dbmon123"
        self.scanname="scandbmon"
        self.port="1521"
        self.service="dbmon"
        self.conn_dbmon=str(self.db_user)+"/"+str(self.db_pwd)+"@"+str(self.scanname)+":"+str(self.port)+"/"+self.service