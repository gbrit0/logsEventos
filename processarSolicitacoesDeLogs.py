import mysql.connector
# from mysql.connector import pooling
import os
import psutil
import subprocess
import datetime
import sys
import time
from signal import signal, SIGPIPE, SIG_DFL
import errno 



def buscarSolicitacoes(cursor: mysql.connector.cursor):
   query = f"""SELECT
                  *
               FROM
                  solicitacao_log
               LIMIT
                  10
            """
   
   
   cursor.execute(query)
   return cursor.fetchall()


def popularTabelaSolicitacoesLog(conexaoComBanco: mysql.connector,
                                 cursor: mysql.connector.cursor):

   for x in range(2):
      query = f"""INSERT INTO solicitacao_log (cod_equipamento, cod_tipo_log)
                  SELECT 
                     cod_equipamento,
                     {x}
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
                                 NOW()) < 5)
                  AND cod_tipo_conexao = 1
                        """
      try:
         conexaoComBanco.reconnect()
         cursor.execute(query)
         conexaoComBanco.commit()
      except mysql.connector.IntegrityError:
         pass       


def recuperarParametrosCounicacao(codEquipamento: int) -> list:
   
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
      with mysql.connector.connect(user = os.environ['MYSQL_USER'], 
                              password = os.environ['MYSQL_PASSWORD'],
                              host = os.environ['MYSQL_HOST'],
                              database = os.environ['MYSQL_DATABASE']) as con:

         with con.cursor() as cursor:
               con.reconnect()
               cursor.execute(sql)
               result = cursor.fetchone()
               
               
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
      


def processar_solicitacoes(pool, solicitacoes):
   try:
      
    processes = []

    for solicitacao in solicitacoes:
      idSolicitacao, codEquipamento, codTipoLog = solicitacao

        # Apagar a linha correspondente à solicitação
      deleteRow = f"DELETE FROM solicitacao_log WHERE id = {idSolicitacao}"

      with pool.get_connection() as conexaoComBanco:
         with conexaoComBanco.cursor() as cursor:
            conexaoComBanco.reconnect()
            cursor.execute(deleteRow)
            conexaoComBanco.commit()

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
      with pool.get_connection() as conexaoComBanco:
         with conexaoComBanco.cursor() as cursor:
            conexaoComBanco.reconnect()
            cursor.execute(parametrosComunicacao)
            resultado = cursor.fetchone()

      if resultado:
         host, porta, modbusId = resultado
         process = subprocess.Popen([sys.executable, 'recuperaLogs.py',
                                       str(idSolicitacao), str(codEquipamento),
                                       str(modbusId), host, str(porta), str(codTipoLog)],
                                       stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
         processes.append((process, idSolicitacao))
         
      time.sleep(1)

    # Monitorar e aguardar a conclusão de todos os subprocessos
      for process, idSolicitacao in processes:

         ps_process = psutil.Process(process.pid)
        
         while process.poll() is None:
            memory_info = ps_process.memory_info()
            cpu_percent = ps_process.cpu_percent(interval=1)

            with open("logRecursosSubprocessos.txt", 'a') as log_file:
                log_file.write(f"{datetime.datetime.now()} - Subprocesso ID {process.pid} "
                               f"Equipamento {idSolicitacao}: Uso de memória: {memory_info.rss / 1024 ** 2} MB, "
                               f"Uso de CPU: {cpu_percent}%\n")
         
        # Após a conclusão
         stdout, stderr = process.communicate()
         if process.returncode != 0:
            with open("logProcessarSolicitacoesLogs.txt", 'a') as file:
               file.write(f"{datetime.datetime.now()} - Erro ao executar recuperaLogs.py para o equipamento {codEquipamento}\n"
                        f"Saída padrão: {stdout}\nErro padrão: {stderr}\n")


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
   #Ignore SIG_PIPE and don't throw exceptions on it... (http://docs.python.org/library/signal.html)  
   # https://www.javatpoint.com/broken-pipe-error-in-python
   signal(SIGPIPE,SIG_DFL)

    
   inicio = time.time()
   
   try:
      pool = mysql.connector.pooling.MySQLConnectionPool(
         pool_name="MySqlPool",
         pool_size=32,
         user=os.environ['MYSQL_USER'],
         password=os.environ['MYSQL_PASSWORD'],
         host=os.environ['MYSQL_HOST'],
         database=os.environ['MYSQL_DATABASE']
      )

      # Conexão inicial para popular a tabela
      with pool.get_connection() as conexaoComBanco:
         with conexaoComBanco.cursor() as cursor:
            popularTabelaSolicitacoesLog(conexaoComBanco, cursor)
            time.sleep(5) 

      # Conexão para processar as solicitações
      
      while True:
         with pool.get_connection() as conexaoComBanco:
            with conexaoComBanco.cursor() as cursor:
               conexaoComBanco.reconnect()
               solicitacoes = buscarSolicitacoes(cursor)
               if not solicitacoes:
                  break
               processar_solicitacoes(pool, solicitacoes)

   except mysql.connector.InterfaceError as e:
      with open("logProcessarSolicitacoesLogs.txt", 'a') as file:
         file.write(f"{datetime.datetime.now()} - Erro de interface MySQL: {e}\n")
   except IOError as e: 
      if e.errno == errno.EPIPE: 
         print(e)
   finally:
      with mysql.connector.connect(user=os.environ['MYSQL_USER'],
                     password=os.environ['MYSQL_PASSWORD'],
                     host=os.environ['MYSQL_HOST'],
                     database=os.environ['MYSQL_DATABASE']) as conexaoComBanco:
         with conexaoComBanco.cursor() as cursor:
            truncate = f"truncate table solicitacao_log"
            conexaoComBanco.reconnect()
            cursor.execute(truncate)
            conexaoComBanco.commit()


   fim = time.time()
   print(f"{datetime.datetime.now()}   tempo de execução: {(fim-inicio):.2f} segundos")

if __name__ == "__main__": main()