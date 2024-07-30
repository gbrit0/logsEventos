import mysql.connector
import os
import subprocess
import datetime
import sys

def wait_for_processes(processes):
    for process in processes:
        process.wait()

def buscarSolicitacoes(cursor: mysql.connector.cursor):
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
         host, porta, modbus_id,
      FROM
        modbus_tcp
      WHERE
        modbus_tcp.cod_equipamento = {codEquipamento} 
          AND modbus_tcp.ativo = 1   
   """#testes.
   
   # mudar host para as variáveis de ambiente
   try:
      with mysql.connector.connect(user = os.environ['USER'], 
                              password = os.environ['PASSWORD'],
                              host = os.environ['HOST'],
                              database = 'teste') as con: # os.environ['DATABASE']
         # print(f"user: {os.environ['USER']}")
         # print(f"password: {os.environ['PASSWORD']}")
         # print(f"database: {os.environ['DATABASE']}")

         with con.cursor() as cursor:
               cursor.execute(sql)
               result = cursor.fetchone()
               
               # host, porta, modbusId, codTipoEquipamento
               return result[0], result[1], result[2], result[3] #, codEquipamento
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
   try:
      with mysql.connector.connect(user=os.environ['USER'],
                       password=os.environ['PASSWORD'],
                       host=os.environ['HOST'],
                       database=os.environ['DATABASE']) as conexaoComBanco:
         with conexaoComBanco.cursor() as cursor:
            solicitacoes = buscarSolicitacoes(cursor)
            processes = []
            for solicitacao in solicitacoes:
                  idSolicitacao, codEquipamento, codTipoLog = solicitacao

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
                  host, porta, modbusId = cursor.fetchone()

                  process = subprocess.Popen([sys.executable, 'recuperaLogs.py',
                                             str(idSolicitacao), str(codEquipamento),
                                             str(modbusId), host, str(porta), str(codTipoLog)],
                                             stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                  processes.append((process, idSolicitacao))

            # Aguardar a conclusão de todos os subprocessos e tratar a saída
            for process, idSolicitacao in processes:
                  stdout, stderr = process.communicate()
                  if process.returncode == 0:
                     deleteRow = f"delete from solicitacao_log where id = {idSolicitacao}"
                     cursor.execute(deleteRow)
                     conexaoComBanco.commit()
                  else:
                     with open("logProcessarSolicitacoesLogs.txt", 'a') as file:
                        file.write(f"{datetime.datetime.now()}       Erro ao executar recuperaLogs.py para a solicitação {idSolicitacao}/
                                   /         Saída padrão: {stdout}        Erro padrão: {stderr}")
                     # print(f"Erro ao executar recuperaLogs.py para a solicitação {idSolicitacao}")
                     # print(f"Saída padrão: {stdout}")
                     # print(f"Erro padrão: {stderr}")

            

   except mysql.connector.InterfaceError as e:
      # print(f"Erro de interface MySQL: {e}")
      with open("log.txt", 'a') as file:
         file.write(f"{datetime.datetime.now()}       {idSolicitacao}        'Erro de interface MySQL: {e}'\n")
   except mysql.connector.DatabaseError as e:
      # print(f"Erro de banco de dados MySQL: {e}")
      with open("log.txt", 'a') as file:
         file.write(f"{datetime.datetime.now()}       {idSolicitacao}        'Erro de banco de dados MySQL: {e}'\n")
   except mysql.connector.OperationalError as e:
      # print(f"Erro operacional MySQL: {e}")
      with open("log.txt", 'a') as file:
         file.write(f"{datetime.datetime.now()}       {idSolicitacao}        'Erro operacional MySQL: {e}'\n")
   except mysql.connector.IntegrityError as e:
      # print(f"Erro de integridade MySQL: {e}")
      with open("log.txt", 'a') as file:
         file.write(f"{datetime.datetime.now()}       {idSolicitacao}        'Erro de integridade MySQL: {e}'\n")
   except mysql.connector.ProgrammingError as e:
      # print(f"Erro de programação MySQL: {e}")
      with open("log.txt", 'a') as file:
         file.write(f"{datetime.datetime.now()}       {idSolicitacao}        'Erro de programação MySQL: {e}'\n")
   except mysql.connector.DataError as e:
      # print(f"Erro de dados MySQL: {e}")
      with open("log.txt", 'a') as file:
         file.write(f"{datetime.datetime.now()}       {idSolicitacao}        'Erro de dados MySQL: {e}'\n")
   except mysql.connector.Error as e:
      # print(f"Erro de conexão MySQL: {e}")
      with open("log.txt", 'a') as file:
         file.write(f"{datetime.datetime.now()}       {idSolicitacao}        'Erro de conexão MySQL: {e}'\n")




if __name__ == "__main__": main()