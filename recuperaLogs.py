import struct
import socket
import time, datetime
import mysql.connector
from mysql.connector import Error
import pandas as pd
import re



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
   with mysql.connector.connect( user='root', 
                                 password='025supergerasol',
                                 host='192.168.4.50',
                                 database='sup_geral') as con:
      with con.cursor() as cursor:
         cursor.execute(sql)
         result = cursor.fetchone()
         
         # host, porta, modbusId, codTipoEquipamento
         return result[0], result[1], result[2], result[3]#, codEquipamento



def processarRespostaModbus(resp: bytes) -> str:
   try:   
      data = struct.unpack(
            """>3H83B30h28b""",
            resp
         )
      # print(data)
   except struct.error as e:
      print(f"struct error: {e}")
      return
   # except Exception as e:
   #    print(e)
   
   if data[86] == 0:
      return None

   text = [chr(x) for x in data[6:86] if (x >= 32 and x <= 127)]
   # text = ''
   # for byte in data[6:86]:
   #    if byte == 1:
   #       break
   #    if 32 <= byte <= 126:
   #       text += chr(byte)

   # # Usar regex para dividir na primeira ocorrência de espaço duplo
   # text = re.split(r' {2}', text, maxsplit=1)

   # # A parte desejada está antes do primeiro espaço duplo
   # text = text[0]
   


   # Juntar a lista de caracteres em uma string
   text = ''.join(text)

   date = datetime.datetime(year=data[86], month=data[87], day=data[88], 
                            hour=data[89], minute=data[90], second=data[91], microsecond=data[92]*1000)

   return text, data, date



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


def fetchLog(codEquipamento, tipoLog = 0):
   host, porta, modbusId, codTipoEquipamento = recuperarParametrosCounicacao(codEquipamento)

   with conectarComModbus(host, porta) as con:
      with mysql.connector.connect(user='root', password='025supergerasol',
                                 host='127.0.0.1',
                                 database='testes') as conexaoComBanco:
         with conexaoComBanco.cursor() as cursor:

            ultimaLinha = buscarUltimaLinhaLog(codEquipamento, cursor, tipoLog)
            
            if not ultimaLinha:
               ultimaLinha = (0, 0, '', datetime.datetime(1900,1,1,0,0,0,0))

            if tipoLog == 1:  # Log Alarmes
               ran = range(500, 1000)
            else: # Log Eventos
               ran = range(500)

            try:
               for startingAddress in ran:
                  req = gerarRequisicao(startingAddress,modbusId,startingAddress) # startingAddress é sempre o mesmo número que o transactionId
                  con.send(req)
                  res = con.recv(1024)
                  # print(res)
                  try:
                     nomeEvent, textEvent, date = processarRespostaModbus(res)
                     linha = (codEquipamento, codTipoEquipamento, nomeEvent, date)
                     
                     if linha[3] >= ultimaLinha[3] and textEvent != ultimaLinha[3]: # Existem casos em que o mesmo alarme/evento se repetem com o mesmo horário (ultimaLinha[3] é a data e hora)
                                                                                    #  para esses casos vou considerar apenas um dos alarme/eventos. O que realmente importa é o nome
                                                                                    #  então exibir apenas um é o suficiente.
                        escreverLogNoBancoLinhaALinha(conexaoComBanco, cursor, codEquipamento, codTipoEquipamento, nomeEvent, textEvent, date, tipoLog)
                     
                  except TypeError as e: # O TypeError aqui vai indicar que a resposta do modbus foi vazia, logo, chegou ao fim do log e deve ser encerrado o fetchLog
                     break
                  except Exception as e:
                     print(f"Erro ao processar resposta Modbus: {e}")
                     break
                     
            except Error as e:
               print(f"Erro na comunicacao com o banco de dados: {e}")
               return
            except ConnectionResetError as e:
               print(f"Erro de conexao: {e}")
               return
            except TimeoutError as e:
               print(f"{e}")
               return
               

def abreAsConexoesExecutaFuncao(codEquipamento, func, **kwargs):
   host, porta, modbusId, codTipoEquipamento = recuperarParametrosCounicacao(codEquipamento)
   
   with conectarComModbus(host, porta) as con:
      with mysql.connector.connect(user='root', password='025supergerasol',
                                 host='127.0.0.1',
                                 database='testes') as conexaoComBanco:
         with conexaoComBanco.cursor() as cursor:
            func(con, cursor, kwargs)




def main(tipoLog = 0): # precisa receber também o codigo do equipamento
   inicio = time.time()#2747
   
   
   fetchLog(1868, 1) 


   fim = time.time()
   print(f"tempo de execução: {(fim-inicio):.2f} segundos")



if __name__ == "__main__": main()