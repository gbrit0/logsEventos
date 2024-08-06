import os
import subprocess
import datetime
import sys
import time
import mysql.connector
from memory_profiler import profile


def buscarSolicitacoes(cursor: mysql.connector.cursor):
   query = f"""SELECT
                  *
               FROM
                  solicitacao_log
               LIMIT
                  30
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
      

def main():
   inicio = time.time()
   
   try:
      with mysql.connector.connect(user=os.environ['MYSQL_USER'],
                       password=os.environ['MYSQL_PASSWORD'],
                       host=os.environ['MYSQL_HOST'],
                       database=os.environ['MYSQL_DATABASE']) as conexaoComBanco:
         with conexaoComBanco.cursor() as cursor:
            
            popularTabelaSolicitacoesLog(conexaoComBanco, cursor)
            time.sleep(5)
            

            while buscarSolicitacoes(cursor):
               solicitacoes = buscarSolicitacoes(cursor)
               processes = []
               for solicitacao in solicitacoes:
                     idSolicitacao, codEquipamento, codTipoLog = solicitacao


                     # Apagar as linhas antes de executar as demais operações pois, caso essas falhem,
                     # garante-se a exclusão da solicitação
                     deleteRow = f"delete from solicitacao_log where id = {idSolicitacao}"
                     cursor.execute(deleteRow)
                     conexaoComBanco.commit()

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

                     # print(f"tipo:{type(resultado)}")
                     # print(f"{resultado}")
                  

                     if resultado:
                        host, porta, modbusId = resultado
                        process = subprocess.Popen([sys.executable, 'recuperaLogs.py',
                                                   str(idSolicitacao), str(codEquipamento),
                                                   str(modbusId), host, str(porta), str(codTipoLog)],
                                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                        processes.append((process, idSolicitacao))


               # Aguardar a conclusão de todos os subprocessos e tratar a saída
               for process, idSolicitacao in processes:
                     stdout, stderr = process.communicate()
                     if process.returncode != 0:
                        with open("logProcessarSolicitacoesLogs.txt", 'a') as file:
                           file.write(f"""{datetime.datetime.now()}       Erro ao executar recuperaLogs.py para o equipamento {codEquipamento}
                                             Saída padrão: {stdout}
                                             Erro padrão: {stderr}\n""")
                     

   except mysql.connector.InterfaceError as e:
      # print(f"Erro de interface MySQL: {e}")
      with open("logProcessarSolicitacoesLogs.txt", 'a') as file:
         file.write(f"{datetime.datetime.now()}       eq:{codEquipamento}        'Erro de interface MySQL: {e}'\n")
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
   finally:
      with mysql.connector.connect(user=os.environ['MYSQL_USER'],
                       password=os.environ['MYSQL_PASSWORD'],
                       host=os.environ['MYSQL_HOST'],
                       database=os.environ['MYSQL_DATABASE']) as conexaoComBanco:
         with conexaoComBanco.cursor() as cursor:
            truncate = f"truncate table solicitacao_log"
            cursor.execute(truncate)
            conexaoComBanco.commit()

   fim = time.time()
   print(f"tempo de execução: {(fim-inicio):.2f} segundos")

if __name__ == "__main__": main()