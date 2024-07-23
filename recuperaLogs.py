import struct
import socket
import time, datetime
from mysql import connector
from mysql.connector import Error
from mysql.connector import cursor
import pandas as pd
import os



def conectarComModbus(host: str, porta: int): #  -> socket.socket
   try:
      con = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
      con.settimeout(100)
      con.connect((host, porta))
   except TimeoutError as e:
      print(f"Erro de conexão com Modbus: {e}")
      return
   finally:
      return con
      



def gerarRequisicao(transactionId: int = 0, unitId: int = 1, startingAddress: int = 0) -> bytes:
   """tipoLog: 'E' para eventos ou 'A' para alarmes"""
   
   return struct.pack(
      '>3H2B2H',
      transactionId,
      0,
      6,
      unitId,
      67,
      startingAddress,
      84
   )



def recuperarParametrosCounicacao(codEquipamento: int) -> list:
   
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
      with connector.connect(user=os.environ['USER'], 
                              password=os.environ['PASSWORD'],
                              host='192.168.4.50',
                              database=os.environ['DATABASE']) as con:
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

   



def processarRespostaModbus(resp: bytes) -> str:
   try:
      data = struct.unpack(
         """>3H83B30h28b""",
         resp
      )
      
   except struct.error as e:
      print(f"struct error: {e}")
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
      
   except Error as e:
      print(f"erro de conexao MySQL: {e}")



def buscarColunasPorTipoEquipamento(codTipoEquipamento: int, cursor):
   query = f"""
      SELECT colunas from colunas_por_tipo_equipamento WHERE cod_tipo_equipamento = {codTipoEquipamento};
   """

   cursor.execute(query)
   return cursor.fetchone()
   


def buscarLogsNoBanco(cursor, codEquipamento):
   query = f"""
      SELECT 
         data_cadastro, cod_equipamento, cod_tipo_equipamento, nome_event, text_event 
      FROM 
         testelogs 
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
      LIMIT 1;
   """

   cursor.execute(query)
   return cursor.fetchone()

   


def abreConexaoComBancoEExecutaFuncao(func, **kwargs):
   
   with connector.connect(user=os.environ['USER'], 
                          password=os.environ['PASSWORD'], 
                          host='192.168.4.50', 
                          database=os.environ['DATABASE']) as conexaoComBanco:
      with conexaoComBanco.cursor() as cursor:
            func(conexaoComBanco=conexaoComBanco, cursor=cursor, **kwargs)


def fetchLog(codEquipamento: int, 
             codTipoEquipamento: int,
             modbusId: int,
             conexaoComBanco: connector,
             cursor: cursor,
             tipoLog = 0):
   host, porta, _, _ = recuperarParametrosCounicacao(codEquipamento)

   with conectarComModbus(host, porta) as conexaoComModbus:
      if not conexaoComBanco and not conexaoComModbus:
         print(f"erro de conexao com banco e/ou com modbus")
         return
      # cursor = conexaoComBanco.cursor()
      
      # print(f"modbusId: {modbusId}")
      # print(f"codTipoEquipamento: {codTipoEquipamento}")

      ultimaLinha = buscarUltimaLinhaLog(codEquipamento, cursor, tipoLog)
      if ultimaLinha is None:
         ultimaLinha = (0, 0, '', '', datetime.datetime(1900,1,1,0,0,0,0))
      print(f"ultimaLinha: {ultimaLinha}")

      if tipoLog == 1:  # Log Alarmes
         ran = range(500, 1000)
      else: # Log Eventos
         ran = range(500)

      try:
         for startingAddress in ran:
            req = gerarRequisicao(startingAddress,modbusId,startingAddress) # startingAddress é sempre o mesmo número que o transactionId
            conexaoComModbus.send(req)
            res = conexaoComModbus.recv(1024)
            print(res)
            try:
               nomeEvent, textEvent, date = processarRespostaModbus(res)
               linha = (codEquipamento, codTipoEquipamento, nomeEvent, date)
               if linha[3] >= ultimaLinha[4] and textEvent != ultimaLinha[3]: # Existem casos em que o mesmo alarme/evento se repetem com o mesmo horário (ultimaLinha[3] é a data e hora)
                                                                              #  para esses casos vou considerar apenas um dos alarme/eventos. O que realmente importa é o nome
                                                                              #  então exibir apenas um é o suficiente.
                  escreverLogNoBancoLinhaALinha(conexaoComBanco, 
                                                cursor, codEquipamento, 
                                                codTipoEquipamento, 
                                                nomeEvent, textEvent, 
                                                date, tipoLog)
               return 1
               
            except TypeError as e: # O TypeError aqui vai indicar que a resposta do modbus foi vazia, logo, chegou ao fim do log e deve ser encerrado o fetchLog
               print(f"type error: {e}")
               break
            except Exception as e:
               print(f"erro ao processar resposta modbus: {e}")
               return 0
         
               
      except Error as e:
         print(f"erro na comunicacao com o banco de dados: {e}")
         return 0
      except ConnectionResetError as e:
         print(f"Erro de conexao: {e}")
         return 0
      except TimeoutError as e:
         print(f"{e}")
         return 0


def buscarSolicitacoes(cursor: cursor):
   query = f"""SELECT
                  *
               FROM
                  solicitacao_log
               WHERE
                  status = 0;"""
   
   cursor.execute(query)
   return cursor.fetchall()


def processarSolicitacoesDeLogs(conexaoComBanco: connector,
                                cursor: cursor):
         
   solicitacoes = buscarSolicitacoes(cursor)
   print(solicitacoes)

   for solicitacao in solicitacoes:
      print(solicitacao)
      idSolicitacao, codEquipamento, tipoLog, _ = solicitacao

      _, _, modbusId, codTipoEquipamento = recuperarParametrosCounicacao(codEquipamento)
      
         
      if fetchLog(codEquipamento, codTipoEquipamento, modbusId, conexaoComBanco, cursor, tipoLog):
         
         # preenche o log_logs com status positivo
         query = f"""INSERT INTO log_logs (cod_solicitacao_log, status)
                        VALUES ({idSolicitacao}, 1)"""

         cursor.execute(query)
         conexaoComBanco.commit()

      else:
         # preenche o log_logs com status negativo
         query = f"""INSERT INTO log_logs (cod_solicitacao_log, status)
                        VALUES ({idSolicitacao}, 0)"""

         cursor.execute(query)
         conexaoComBanco.commit()

      # muda o status para atendido, independente do status em log_logs
      query = f"""UPDATE `teste`.`solicitacao_log` SET `status` = '1' WHERE (`id` = {idSolicitacao});"""

      cursor.execute(query)
      conexaoComBanco.commit()


def popularTabelaSolicitacoesLog(conexaoComBanco: connector,
                                 cursor: cursor):
   
   for x in range(2):
      query = f"""INSERT INTO solicitacao_log (cod_equipamento, cod_tipo_log)
                  SELECT
                     cod_equipamento,
                     {x}
                  FROM
                     modbus_tcp
                  WHERE
                     cod_tipo_conexao = 1;"""
      # cod_tipo_equipamento in (SELECT DISTINCT cod_equipamento FROM leituras) AND 
      cursor.execute(query)
      conexaoComBanco.commit()
   

def main():
   inicio = time.time()


   abreConexaoComBancoEExecutaFuncao(processarSolicitacoesDeLogs)
   

   fim = time.time()
   print(f"tempo de execução: {(fim-inicio):.2f} segundos")


if __name__ == "__main__": main()