import mysql.connector
# from mysql.connector import pooling
import os
import psutil
import subprocess
import datetime
import sys, errno 
import time
from signal import signal, SIGPIPE, SIG_DFL

from recuperaLogs import main as recuperaLogs

#Ignore SIG_PIPE and don't throw exceptions on it... (http://docs.python.org/library/signal.html)  
   # https://www.javatpoint.com/broken-pipe-error-in-python
signal(SIGPIPE,SIG_DFL)

def popularTabelaSolicitacoesLog(conexaoComBanco: mysql.connector,
                                 cursor: mysql.connector.cursor):

   for x in range(2):
      query = f"""INSERT INTO solicitacao_log (cod_equipamento, cod_tipo_log)
                  SELECT 
                     cod_equipamento, 1
                  FROM
                     modbus_tcp
                  WHERE
                     cod_equipamento IN (SELECT 
                              mt.cod_equipamento
                        FROM
                              modbus_tcp mt
                                 LEFT JOIN
                              equipamentos eq ON eq.codigo = mt.cod_equipamento
                                 LEFT JOIN
                              usinas us ON eq.cod_usina = us.codigo
                                 LEFT JOIN
                              leituras lt ON lt.cod_equipamento = eq.codigo
                        WHERE
                              cod_tipo_conexao = 1 AND eq.ativo = 1
                                 AND us.ativo = 1
                                 AND lt.cod_campo = 3
                                 AND TIMESTAMPDIFF(MINUTE,
                                 lt.data_cadastro,
                                 NOW()) < 2)
                        AND cod_tipo_conexao = 1 
            UNION 
                  SELECT 
                     cod_equipamento, 0
                  FROM
                     modbus_tcp
                  WHERE
                     cod_equipamento IN (SELECT 
                              mt.cod_equipamento
                        FROM
                              modbus_tcp mt
                                 LEFT JOIN
                              equipamentos eq ON eq.codigo = mt.cod_equipamento
                                 LEFT JOIN
                              usinas us ON eq.cod_usina = us.codigo
                                 LEFT JOIN
                              leituras lt ON lt.cod_equipamento = eq.codigo
                        WHERE
                              cod_tipo_conexao = 1 AND eq.ativo = 1
                                 AND us.ativo = 1
                                 AND lt.cod_campo = 3
                                 AND TIMESTAMPDIFF(MINUTE,
                                 lt.data_cadastro,
                                 NOW()) < 2)
                        AND cod_tipo_conexao = 1
                        """
      try:
         # conexaoComBanco.reconnect()
         cursor.execute(query)
         conexaoComBanco.commit()
      except mysql.connector.IntegrityError:
         pass       


def recuperarParametrosCounicacao(pool, codEquipamento: int) -> list:
   
   sql = f"""
      SELECT 
         host, porta, modbus_id,
      FROM
        modbus_tcp
      WHERE
        modbus_tcp.cod_equipamento = {codEquipamento} 
          AND modbus_tcp.ativo = 1   
   """

   try:
      with mysql.connector.connect(user = os.environ['LOGS_USER'], 
                              password = os.environ['LOGS_PASSWORD'],
                              host = os.environ['LOGS_HOST'],
                              database = os.environ['LOGS_DATABASE']) as con:

         with con.cursor() as cursor:
               # con.reconnect()
               cursor.execute(sql)
               result = cursor.fetchone()
               # cursor.close()
               # con.close()
               
               
               return result[0], result[1], result[2], result[3]
         
   except mysql.connector.InterfaceError as e:
      print(f"Erro de interface MySQL: {e}")
   except mysql.connector.DatabaseError as e:
      print(f"Erro de banco de dados MySQL: {e}")
   except mysql.connector.OperationalError as e:
      print(f"Erro operacional MySQL: {e}")
   except mysql.connector.IntegrityError as e:
      print(f"Erro de integridade MySQL: {e}")
   except mysql.connector.ProgrammingError as e:
      print(f"Erro de programação MySQL: {e}")
   except mysql.connector.DataError as e:
      print(f"Erro de dados MySQL: {e}")
   except mysql.connector.Error as e:
      print(f"Erro de conexão MySQL: {e}")


def buscarSolicitacoes(cursor: mysql.connector.cursor):
   query = f"""SELECT
                  *
               FROM
                  solicitacao_log
               ORDER BY
                  cod_tipo_log
            """# LIMIT 15
   
   
   cursor.execute(query)
   return cursor.fetchall()


def processar_solicitacoes(pool):
   try:
      with pool.get_connection() as conexaoComBanco:
         with conexaoComBanco.cursor() as cursor:
            solicitacoes = buscarSolicitacoes(cursor)
            

      with pool.get_connection() as conexaoComBanco:
         with conexaoComBanco.cursor() as cursor:
            for solicitacao in solicitacoes:
               idSolicitacao, codEquipamento, codTipoLog = solicitacao

               # Buscar parâmetros de comunicação
               parametrosComunicacao = f"""
                     SELECT 
                        host, porta, modbus_id
                     FROM
                        modbus_tcp
                     WHERE
                        cod_equipamento = {codEquipamento}
                        AND ativo = 1
               """
               cursor.execute(parametrosComunicacao)
               resultado = cursor.fetchone()

               if resultado:
                  host, porta, modbusId = resultado
                  
                  recuperaLogs(str(idSolicitacao), str(codEquipamento),
                              str(modbusId), host, str(porta), str(codTipoLog))
                  
               # Apagar a linha correspondente à solicitação
               deleteRow = f"DELETE FROM solicitacao_log WHERE id = {idSolicitacao}"
               cursor.execute(deleteRow)
               conexaoComBanco.commit()

                  
               time.sleep(2)


   except mysql.connector.DatabaseError as e:
      # print(f"Erro de banco de dados MySQL: {e}")
      with open("logProcessarSolicitacoesLogs.txt", 'a') as file:
         file.write(f"{datetime.datetime.now()}       eq:{codEquipamento}        'Erro de banco de dados MySQL: {e}'\n")
   except mysql.connector.OperationalError as e:
      # print(f"Erro operacional MySQL: {e}")
      with open("logProcessarSolicitacoesLogs.txt", 'a') as file:
         file.write(f"{datetime.datetime.now()}       eq:{codEquipamento}        'Erro operacional MySQL: {e}'\n")
   except mysql.connector.ProgrammingError as e:
      # print(f"Erro de programação MySQL: {e}")
      with open("logProcessarSolicitacoesLogs.txt", 'a') as file:
         file.write(f"{datetime.datetime.now()}       eq:{codEquipamento}        'Erro de programação MySQL: {e}'\n")
   except mysql.connector.DataError as e:
      # print(f"Erro de dados MySQL: {e}")
      with open("logProcessarSolicitacoesLogs.txt", 'a') as file:
         file.write(f"{datetime.datetime.now()}       eq:{codEquipamento}        'Erro de dados MySQL: {e}'\n")
   except mysql.connector.Error as e:
      # print(f"Erro de conexão MySQL: {e}")
      with open("logProcessarSolicitacoesLogs.txt", 'a') as file:
         file.write(f"{datetime.datetime.now()}       eq:{codEquipamento}        'Erro de conexão MySQL: {e}'\n")



def main():
   inicio = time.time()
   print(15*'-' + 3*' ' + f'Início da execução em {datetime.datetime.now()}' + 3*' ' + 15*'-')
   try:
      pool = mysql.connector.pooling.MySQLConnectionPool(
         pool_name="MySqlPool",
         pool_size=5,
         user=os.environ['LOGS_USER'],
         password=os.environ['LOGS_PASSWORD'],
         host=os.environ['LOGS_HOST'],
         database=os.environ['LOGS_DATABASE']
      )

      # Conexão para popular a tabela
      with pool.get_connection() as conexaoComBanco:
         with conexaoComBanco.cursor() as cursor:
            popularTabelaSolicitacoesLog(conexaoComBanco, cursor)
            
      # chamando diretamente a função, dentro dela tem um loop que puxa o log de uma por vez
      processar_solicitacoes(pool)



   except mysql.connector.InterfaceError as e:
      with open("logProcessarSolicitacoesLogs.txt", 'a') as file:
         file.write(f"{datetime.datetime.now()} - Erro de interface MySQL: {e}\n")
   except IOError as e: 
      if e.errno == errno.EPIPE: 
         # print(f"IOError: {e}")
         pass
   except mysql.connector.errors.OperationalError as e:
      pass
   finally:
      with pool.get_connection() as conexaoComBanco:
         with conexaoComBanco.cursor() as cursor:
            truncate = f"truncate table solicitacao_log"
            cursor.execute(truncate)
            conexaoComBanco.commit()


   fim = time.time()
   print(f"Fim da execução   {datetime.datetime.now()}   tempo de execução: {(fim-inicio):.2f} segundos\n")

if __name__ == "__main__": main()