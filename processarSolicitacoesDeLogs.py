from  mysql import connector
from mysql.connector import cursor, Error
import os
import struct

def buscarSolicitacoes(cursor: cursor):
   query = f"""SELECT
                  *
               FROM
                  solicitacao_log;
            """# WHERE
                 #  status = 0
   
   cursor.execute(query)
   return cursor.fetchall()

def recuperarParametrosCounicacao(codEquipamento: int) -> list:
   
   sql = f"""
      SELECT 
         `host`, `porta`, `modbus_id`,
      FROM
         `modbus_tcp`
      WHERE
         `modbus_tcp`.`cod_equipamento` = {codEquipamento} 
          AND `modbus_tcp`.`ativo` = 1   
   """
   
   # mudar host para as variáveis de ambiente
   try:
      with connector.connect(user = os.environ['USER'], 
                              password = os.environ['PASSWORD'],
                              host = os.environ['HOST'],
                              database = os.environ['DATABASE']) as con:
         # print(f"user: {os.environ['USER']}")
         # print(f"password: {os.environ['PASSWORD']}")
         # print(f"database: {os.environ['DATABASE']}")

         with con.cursor() as cursor:
               cursor.execute(sql)
               result = cursor.fetchone()
               
               # host, porta, modbusId, codTipoEquipamento
               return result[0], result[1], result[2], result[3] #, codEquipamento
   except connector.InterfaceError as e:
      print(f"Erro de interface MySQL: {e}")
   except connector.DatabaseError as e:
      print(f"Erro de banco de dados MySQL: {e}")
   except connector.OperationalError as e:
      print(f"Erro operacional MySQL: {e}")
   except connector.IntegrityError as e:
      print(f"Erro de integridade MySQL: {e}")
   except connector.ProgrammingError as e:
      print(f"Erro de programação MySQL: {e}")
   except connector.DataError as e:
      print(f"Erro de dados MySQL: {e}")
   except Error as e:
      print(f"Erro de conexão MySQL: {e}")



def main():
   try:
      with connector.connect(user = os.environ['USER'],
                             password = os.environ['PASSWORD'],
                             host = os.environ['HOST'],
                             database = os.environ['DATABASE']) as conexaoComBanco:
         with conexaoComBanco.cursor() as cursor:
            solicitacoes = buscarSolicitacoes(cursor)

            for solicitacao in solicitacoes:
               id, codEquipamento, codTipoLog = solicitacao

               parametrosComunicacao = f"""
                  SELECT 
                     `host`, `porta`, `modbus_id`,
                  FROM
                     `modbus_tcp`
                  WHERE
                     `modbus_tcp`.`cod_equipamento` = {codEquipamento} 
                     AND `modbus_tcp`.`ativo` = 1   
               """
               cursor.execute(parametrosComunicacao)
               host, porta, modbusId = cursor.fetchone()


   except connector.InterfaceError as e:
      print(f"Erro de interface MySQL: {e}")
   except connector.DatabaseError as e:
      print(f"Erro de banco de dados MySQL: {e}")
   except connector.OperationalError as e:
      print(f"Erro operacional MySQL: {e}")
   except connector.IntegrityError as e:
      print(f"Erro de integridade MySQL: {e}")
   except connector.ProgrammingError as e:
      print(f"Erro de programação MySQL: {e}")
   except connector.DataError as e:
      print(f"Erro de dados MySQL: {e}")
   except Error as e:
      print(f"Erro de conexão MySQL: {e}")




if __name__ == "__main__": main()