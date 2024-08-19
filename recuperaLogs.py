import struct
import socket
import time, datetime
import os
import argparse
import mysql.connector # type: ignore
# from memory_profiler import profile


def conectarComModbus(idSolicitacao: str, host: str, porta: int): #  -> socket.socket
   try:
      con = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
      con.settimeout(100)
      con.connect((host, porta))
   except TimeoutError as e:
      with open("logRecuperaLogs.txt", 'a', encoding='utf-8') as file:
         file.write(f"{datetime.datetime.now()}       {idSolicitacao}        'Erro de conexão com Modbus: {e}'\n")
      # print(f"Erro de conexão com Modbus: {e}")
      return
   finally:
      return con
      
# @profile

def gerarRequisicao(transactionId: int = 0, unitId: int = 1, startingAddress: int = 0, tipoLog: int = 0, **codTipoEquipamento: int) -> bytes:
   """tipoLog= 0 para eventos ou 1 para alarmes
      tipoLog = 3: teste de conexão e recupera modelo do controlador
   """
   
   codFuncao = 67
   codCampo = startingAddress
   quantidade = 84

   if codTipoEquipamento == 182:
      quantidade = 3
   elif tipoLog == 3:
      codFuncao = 4
      codCampo = 59900
      quantidade = 1

   return struct.pack(
      '>3H2B2H',
      transactionId,
      0,
      6,
      unitId,
      codFuncao,
      codCampo,
      quantidade
   )


def recuperarParametrosCounicacao(idSolicitacao, codEquipamento: int, conexaoComBanco, cursor) -> list:
   
   sql = f"""
      SELECT 
         `host`, `porta`, `modbus_id`, `equipamentos`.`cod_tipo_equipamento`
      FROM
         `modbus_tcp`
            LEFT JOIN
         `equipamentos` ON `modbus_tcp`.`cod_equipamento` = `equipamentos`.`codigo`
      WHERE
         `modbus_tcp`.`cod_equipamento` = {codEquipamento} AND `modbus_tcp`.`ativo` = 1   
   """
   
   # mudar host para as variáveis de ambiente
   try:
      # with connector.connect( user=os.environ['USER'], 
      #                         password=os.environ['PASSWORD'],
      #                         host='192.168.4.50',
      #                         database=os.environ['DATABASE']) as con:
      #    # print(f"user: {os.environ['USER']}")
      #    # print(f"password: {os.environ['PASSWORD']}")
      #    # print(f"database: {os.environ['DATABASE']}")

      #    with con.cursor() as cursor:
               cursor.execute(sql)
               result = cursor.fetchone()
               
               # host, porta, modbusId, codTipoEquipamento
               return result[0], result[1], result[2], result[3] #, codEquipamento
   except mysql.connector.InterfaceError as e:
      # print(f"Erro de interface MySQL: {e}")
      with open("logRecuperaLogs.txt", 'a', encoding='utf-8') as file:
         file.write(f"{datetime.datetime.now()}       id:{idSolicitacao}        'Erro de interface MySQL: {e}'\n")
   except mysql.connector.DatabaseError as e:
      # print(f"Erro de banco de dados MySQL: {e}")
      with open("logRecuperaLogs.txt", 'a', encoding='utf-8') as file:
         file.write(f"{datetime.datetime.now()}       id:{idSolicitacao}        'Erro de banco de dados MySQL: {e}'\n")
   except mysql.connector.OperationalError as e:
      # print(f"Erro operacional MySQL: {e}")
      with open("logRecuperaLogs.txt", 'a', encoding='utf-8') as file:
         file.write(f"{datetime.datetime.now()}       id:{idSolicitacao}        'Erro operacional MySQL: {e}'\n")
   except mysql.connector.IntegrityError as e:
      # print(f"Erro de integridade MySQL: {e}")
      with open("logRecuperaLogs.txt", 'a', encoding='utf-8') as file:
         file.write(f"{datetime.datetime.now()}       id:{idSolicitacao}        'Erro de integridade MySQL: {e}'\n")
   except mysql.connector.ProgrammingError as e:
      # print(f"Erro de programação MySQL: {e}")
      with open("logRecuperaLogs.txt", 'a', encoding='utf-8') as file:
         file.write(f"{datetime.datetime.now()}       id{idSolicitacao}        'Erro de programação MySQL: {e}'\n")
   except mysql.connector.DataError as e:
      # print(f"Erro de dados MySQL: {e}")
      with open("logRecuperaLogs.txt", 'a', encoding='utf-8') as file:
         file.write(f"{datetime.datetime.now()}       id:{idSolicitacao}        'Erro de dados MySQL: {e}'\n")
   except mysql.connector.Error as e:
      # print(f"Erro de conexão MySQL: {e}")
      with open("logRecuperaLogs.txt", 'a', encoding='utf-8') as file:
         file.write(f"{datetime.datetime.now()}       id:{idSolicitacao}        'Erro de conexão MySQL: {e}'\n")

   



def processarRespostaModbus(codTipoEquipamento, resp: bytes) -> str:
   try:
      data = struct.unpack(
         """>3H83B30h28b""",
         resp
      )
      
   except struct.error as e:
      # print(f"struct error: {e}")
      # with open("logRecuperaLogs.txt", 'a', encoding='utf-8') as file:
      #    file.write(f"{datetime.datetime.now()}       {idSolicitacao}        'struct error: {e}'\n")
      return (None, None, None)
   
   if data[86] == 0:
      return (None, None, None)

   text =  data[6:86]
   text = extrair_texto(text)

   date = datetime.datetime(year=data[86], month=data[87], day=data[88], 
                           hour=data[89], minute=data[90], second=data[91], microsecond=data[92]*1000)
   # print(text, data, date)
   return [text, data, date]

def extrair_texto(caracteres):
    texto = []
    i = 0
    

    while i < len(caracteres):
         if caracteres[i] == 0x00 and caracteres[i + 1] == 0x00:
            break
         if caracteres[i] != 0x00:
            texto.append(chr(caracteres[i]))
         i += 1

    return "".join(texto)



def escreverLogNoBancoLinhaALinha(conexaoComBanco, cursor, codEquipamento, codTipoEquipamento, nomeEvent, textEvent, date, tipoLog = 0):
   
   if tipoLog == 1: 
      tabela , coluna1, coluna2 = ['event_alarm', 'nome_alarm', 'text_alarm']
   else:
      tabela, coluna1, coluna2 = ['event_log', 'nome_event', 'text_event']

   sql = f"""
      INSERT INTO `{tabela}` (cod_equipamento, cod_tipo_equipamento, {coluna1}, {coluna2}, data_cadastro) 
         VALUES ({codEquipamento}, {codTipoEquipamento}, '{nomeEvent}', '{textEvent[93:]}', '{date}')    
   """
   # de text[86:93] está a data
   # de text[93:] estão os valores dos parâmetros
   try:
      cursor.execute(sql)
      conexaoComBanco.commit()
      
   except mysql.connector.Error as e:
      print(f"erro de conexao MySQL: {e}")



def escreverLogNoBanco(pool, values, tipoLog):
   if tipoLog == 1: 
      tabela , coluna1, coluna2 = ['event_alarm', 'nome_alarm', 'text_alarm']
   else:
      tabela, coluna1, coluna2 = ['event_log', 'nome_event', 'text_event']

   sql = f"""
      INSERT IGNORE INTO `{tabela}` (cod_equipamento, cod_tipo_equipamento, {coluna1}, {coluna2}, data_cadastro) 
         VALUES (%s, %s, %s, %s, %s)    
   """
   tentativas = 0
   maxTentativas = 3

   while tentativas < maxTentativas:
      try:
         with pool.get_connection() as conexaoComBanco:
               with conexaoComBanco.cursor() as cursor:
                  cursor.executemany(sql, values)
                  conexaoComBanco.commit()
         break  
      except mysql.connector.errors.InternalError as e:
         tentativas += 1
         if tentativas < maxTentativas:
            time.sleep(3)  
         else: 
            raise e
      
      



def buscarColunasPorTipoEquipamento(codTipoEquipamento: int, cursor):
   query = f"""
      SELECT colunas from colunas_por_tipo_equipamento WHERE cod_tipo_equipamento = {codTipoEquipamento};
   """

   cursor.execute(query)
   return cursor.fetchone()
   


def buscarLogsNoBanco(cursor, codEquipamento, tipoLog):
   if tipoLog == 1: 
      tabela , coluna1, coluna2 = ['event_alarm', 'nome_alarm', 'text_alarm']
   else:
      tabela, coluna1, coluna2 = ['event_log', 'nome_event', 'text_event']
   query = f"""
      SELECT 
         data_cadastro, cod_equipamento, cod_tipo_equipamento, {coluna1}, {coluna2} 
      FROM 
         {tabela}
      WHERE 
         cod_equipamento = {codEquipamento} 
      ORDER BY 
         data_cadastro 
      DESC 
      LIMIT 500
   """
   cursor.execute(query)
   return cursor.fetchall()



def expandirTextEvent(log, colunas):
   dataCadastro, codEquipamento, codTipoEquipamento, nomeEvent, textEvents  = log
   textEvents = textEvents.strip('()').split(', ')
   # print(type(textEvents))
   textEventsExpandido = {}
   for index, coluna in enumerate(colunas):
      textEventsExpandido[coluna] = textEvents[index]

   logExpandido = {
      'data_cadastro': dataCadastro,
      'cod_equipamento': codEquipamento,
      'codTipoEquipamento': codTipoEquipamento,
      'nome_event': nomeEvent,

   }

   logExpandido.update(textEventsExpandido)
   
   return logExpandido
   

def processarLogs(logs, colunas):
    todosLogs = []
    for log in logs:
        logExpandido = expandirTextEvent(log, colunas)
        todosLogs.append(logExpandido)
    return todosLogs


def buscarUltimaLinhaLog(codEquipamento, cursor, tipoLog = 0):
   if tipoLog == 1: 
      tabela , colunaNome, colunaText = ['event_alarm', 'nome_alarm', 'text_alarm']
   else:
      tabela, colunaNome, colunaText = ['event_log', 'nome_event', 'text_event']

   query = f"""
      SELECT 
         cod_equipamento, cod_tipo_equipamento, {colunaNome}, {colunaText},  data_cadastro 
      FROM {tabela} 
      WHERE cod_equipamento = {codEquipamento} 
      HAVING max(data_cadastro)
      ORDER BY data_cadastro DESC 
      LIMIT 1
   """

   cursor.execute(query)
   return cursor.fetchone()

   


# def abreConexaoComBancoEExecutaFuncao(func, **kwargs):
   
#    with mysql.connector.connect(user=os.environ['MYSQL_USER'], 
#                           password=os.environ['MYSQL_PASSWORD'], 
#                           host=os.environ['MYSQL_HOST'], 
#                           database=os.environ['MYSQL_DATABASE']) as conexaoComBanco:
#       with conexaoComBanco.cursor() as cursor:
#             func(conexaoComBanco=conexaoComBanco, cursor=cursor, **kwargs)


def testaConexaoModbusERecuperaTipoEquipamento(idSolicitacao, host, porta):
   req = gerarRequisicao(tipoLog=3)
   try:
      with conectarComModbus(idSolicitacao, host, porta) as conexaoComModbus:
         conexaoComModbus.send(req)
         return struct.unpack(
            ">3H3BH",
            conexaoComModbus.recv(1024))[6]
   except:
      return 0


def fetchLog(idSolicitacao: int,
             codEquipamento: int,
             modbusId: int,
             host: str,
             porta: int,
             tipoLog = 0):

   try:
      pool = mysql.connector.pooling.MySQLConnectionPool(
         pool_name="MySqlPool",
         pool_size=5,
         user=os.environ['MYSQL_USER'],
         password=os.environ['MYSQL_PASSWORD'],
         host=os.environ['MYSQL_HOST'],
         database=os.environ['MYSQL_DATABASE']
      )

      with pool.get_connection() as conexaoComBanco:
         with conexaoComBanco.cursor() as cursor:
            codTipoEquipamento = testaConexaoModbusERecuperaTipoEquipamento(idSolicitacao, host, porta)

            if codTipoEquipamento == 0: # codTipoEquipamento == 0 quer dizer que não foi possível conectrar com o modbus
               with open("logRecuperaLogs.txt", 'a', encoding='utf-8') as file:
                  file.write(f"{datetime.datetime.now()}       id:{id}        'Conexão com o equipamento {codEquipamento} não estabelecida'\n")
                  return
            else:      
               ultimaLinha = buscarUltimaLinhaLog(codEquipamento, cursor, tipoLog)

               if ultimaLinha is None:
                  ultimaLinha = (0, 0, '', '', datetime.datetime(1900,1,1,0,0,0,0))
               # print(f"ultimaLinha: {ultimaLinha}")

         if tipoLog == 1:  # Log Alarmes
            if codTipoEquipamento == 88:
               ran = range(500, 651)
            elif codTipoEquipamento == 182:
               ran = range(500, 1000, 3)
            else:
               ran = range(500, 1000)
         elif codTipoEquipamento == 88:
            ran = range(151)
         elif codTipoEquipamento == 182:
               ran = range(0, 500, 3)
         else:
            # Log Eventos
            ran = range(500)



      with conectarComModbus(idSolicitacao, host, porta) as conexaoComModbus:
         try:
            values = []
            for startingAddress in ran:
               req = gerarRequisicao(startingAddress,modbusId,startingAddress, tipoLog, codTipoEquipamento=codTipoEquipamento) # startingAddress é sempre o mesmo número que o transactionId
               conexaoComModbus.send(req)
               res = conexaoComModbus.recv(1024)
               # print(res)
               try:
                  nomeEvent, textEvent, date = processarRespostaModbus(codTipoEquipamento, res)
               
                  linha = (codEquipamento, codTipoEquipamento, nomeEvent, date)
                  if linha[3] >= ultimaLinha[4] and textEvent != ultimaLinha[3]:    #  Existem casos em que o mesmo alarme/evento se repetem com o mesmo horário (ultimaLinha[3] é a data e hora)
                                                                                    #  para esses casos vou considerar apenas um dos alarme/eventos. O que realmente importa é o nome
                                                                                    #  então exibir apenas um é o suficiente.
                     # escreverLogNoBancoLinhaALinha(conexaoComBanco, 
                     #                               cursor, codEquipamento, 
                     #                               codTipoEquipamento, 
                     #                               nomeEvent, textEvent, 
                     #                               date, tipoLog)

                     values.append((str(codEquipamento), str(codTipoEquipamento), str(nomeEvent), str(textEvent[93:]), str(date)))
                     # print(str(codEquipamento), str(codTipoEquipamento), str(nomeEvent), str(textEvent[93:]), str(date))
                  
               except mysql.connector.IntegrityError as e:  # Integrity Error aconteceu durante a execução devido ao Unique adicionado nas tabelas no banco
                                                            # modificando a exceção para 'pass' para pular para a próxima iteração e ignorar os valores repetidos
                  print(f"Erro de integridade MySQL: {e}")
                  with open("logRecuperaLogs.txt", 'a') as file:
                     file.write(f"{datetime.datetime.now()}       id:{idSolicitacao}        'Erro de integridade MySQL: {e}'\n") 
               except TypeError as e: # O TypeError aqui vai indicar que a resposta do modbus foi vazia, logo, chegou ao fim do log e deve ser encerrado o fetchLog
                  # print(f"type error: {e}")
                  return 1
               except Exception as e:
                  print(f"erro ao processar resposta modbus: {e}")
                  return 0
               
               
         except mysql.connector.Error as e:
            print(f"erro na comunicacao com o banco de dados: {e}")
            return 0
         except ConnectionResetError as e:
            print(f"Erro de conexao: {e}")
            return 0
         except TimeoutError as e:
            print(f"{e}")
            return 0
         finally:
            # print(values)
            escreverLogNoBanco(pool, values, tipoLog)

   except TimeoutError as e:
      print(f"Erro de conexão com Modbus: {e}")
      return 1

def buscarSolicitacoes(cursor: mysql.connector.cursor):
   query = f"""SELECT
                  *
               FROM
                  solicitacao_log;
            """# WHERE
                 #  status = 0
   
   cursor.execute(query)
   return cursor.fetchall()


def main(idSolicitacao, codEquipamento, modbusId, host, porta, codTipoLog): # idSolicitacao, codEquipamento, modbusId, host, porta, codTipoLog
   inicio = time.time()

   

   fetchLog(idSolicitacao, codEquipamento, modbusId, host, porta, codTipoLog)

  

   fim = time.time()
   print(f"tempo de execução: {(fim-inicio):.2f} segundos")


if __name__ == "__main__":
   parser = argparse.ArgumentParser(description='Recupera Logs de Equipamento')
   parser.add_argument('idSolicitacao', type=int, help='ID da Solicitação')
   parser.add_argument('codEquipamento', type=int, help='Código do Equipamento')
   parser.add_argument('modbusId', type=int, help='ID Modbus')
   parser.add_argument('host', type=str, help='Host')
   parser.add_argument('porta', type=int, help='Porta')
   parser.add_argument('codTipoLog', type=int, help='Código do Tipo de Log')

   args = parser.parse_args()
   main(args.idSolicitacao, args.codEquipamento, args.modbusId, args.host, args.porta, args.codTipoLog)
   