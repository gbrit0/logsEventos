import struct
import socket
import time, datetime
import os
import argparse
import traceback
import mysql.connector # type: ignore
# from memory_profiler import profile
from signal import signal, SIGPIPE, SIG_DFL

signal(SIGPIPE,SIG_DFL)


def conectarComModbus(idSolicitacao: str, host: str, porta: int): #  -> socket.socket
   try:
      con = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
      con.settimeout(30)
      # print(f"host: {host} {type(host)}")
      # print(f"porta: {porta} {type(porta)}")
      con.connect((host, int(porta)))
      # print("Conexão com o Modbus estabelecida")
   except TimeoutError as e:
      with open("logRecuperaLogs.txt", 'a', encoding='utf-8') as file:
         file.write(f"{datetime.datetime.now()} {idSolicitacao} Timeout em conectarComModbus: {e}")
      # print(f"Erro de conexão com Modbus: {e}")
      return
   except OSError as e:
      return """Não foi possível conectar com o Modbus. Erro de rota. Provavelmente a comunicação é via conversor. Verificar o cod_tipo_conexao em modbus_tcp"""
   except Exception as e:
      print(f"Erro em conectarComModbus: {e, traceback.format_exc()}")
   finally:
      return con
      
# @profile

def gerarRequisicao(transactionId: int = 0, unitId: int = 1, startingAddress: int = 0, tipoLog: int = 0, codTipoEquipamento: int = 88) -> bytes:
   """tipoLog= 0 para eventos ou 1 para alarmes
      tipoLog = 3: teste de conexão e recupera modelo do controlador
   """
   
   codFuncao = 67
   codCampo = startingAddress
   
   # print(f'geraRequisicao codTipoEquipamento: {codTipoEquipamento}')

   if codTipoEquipamento == 182 or codTipoEquipamento == 93:
      quantidade = 3
   elif tipoLog == 3:
      codFuncao = 4
      codCampo = 59900
      quantidade = 1
   else:
      quantidade = 84
      
   # print(f'quantidade: {quantidade}')
   return struct.pack(
      '>3H2B2H',
      int(transactionId),
      0,
      6,
      int(unitId),
      int(codFuncao),
      int(codCampo),
      int(quantidade)
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
               cursor.excute(sql)
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

   

def hexParaDatetime(hex):
   diasHoras = hex[:6]  
   minutosSegundos = hex[6:] 
   
   dias = int(diasHoras, 16)  # --> somando 8 na data inicial
   diasComFracao = dias / 24
   dias = int(diasComFracao)
   horasFracionais = diasComFracao - dias
   horas = int(horasFracionais * 24)
   
   segundosComFracao = int(minutosSegundos, 16) / 10
   segundos = int(segundosComFracao)
   microsegundos = int((segundosComFracao - segundos) * 1_000_000)
   minutes = segundos // 60
   segundosRestantes = segundos % 60
   
   dataInicial = datetime.datetime(year=1780, month=datetime.datetime.now().month + 1 , day=1,  microsecond=1) # Adicionando 1 microssegundo para evitar 0's nesse campo e ñ bugar no banco
   
   dataFinal = dataInicial + datetime.timedelta(days=dias-1, hours=horas, minutes=minutes, seconds=segundosRestantes, microseconds=microsegundos)
   
   return dataFinal





def processarRespostaModbus(codTipoEquipamento, resp: bytes):
   # print("Entrando em processarRespostaModbus") 
   if codTipoEquipamento == 182 or codTipoEquipamento == 93:
      # print("Entrou no loop if codTipoEquipamento == 182 or codTipoEquipamento == 93...")
      # j = 1
      for i in range(0,228,76):
         # j+=1
         # print(f"{j}ª vez no loop")
         # print(resp[i:76+i].hex())
         text = extrair_texto(resp[0+i:19+i], codTipoEquipamento)
         # print("saiu de extrair texto")
         data = struct.unpack(
            '>26h',
            resp[24+i:76+i]
         )
         # print("data unpacked")
         date = resp[19+i:24+i].hex()
         date = hexParaDatetime(date)
         # print(f"text: {text}")
         # print(f"data: {data}")
         # print(f"date month: {date.month}")
         # print(f"datetime.now().month: {datetime.datetime.now().month}")
         
         if date.month == datetime.datetime.now().month:
            yield [text, data, date]  
            # print(f'{resp[19+i:24+i].hex()},{date}') 
         else:
            return


   else:
      try:
         # print(resp.hex())
         data = struct.unpack(
            """>3H83B30h28b""",
            resp
         )
         # print(f"Data: {data}")
         
      except struct.error as e:
         # print(f"struct error em processarRespostaModbus: {e}")
         # with open("logRecuperaLogs.txt", 'a', encoding='utf-8') as file:
         #    file.write(f"{datetime.datetime.now()}       {idSolicitacao}        'struct error: {e}'\n")
         return e# (None, None, None)
      
      # if data[86] == 0:
      #    # return 'campo de data vazio'# (None, None, None)
      #    return

      text =  data[6:86]
      text = extrair_texto(text, codTipoEquipamento)
      # print(f"Text: {text}")

      date = datetime.datetime(year=data[86], month=data[87], day=data[88], 
                              hour=data[89], minute=data[90], second=data[91], microsecond=data[92]*1000)
      # print(f"Date: {date}")
      # print([text, data, date])
      yield [text, data, date]
   

def extrair_texto(caracteres, codTipoEquipamento):
   texto = []
   i = 0
   if codTipoEquipamento == 182 or codTipoEquipamento == 93:

      while i < len(caracteres):
         try:   
            if caracteres[i] == 0x20 and caracteres[i + 1] == 0x20:
               break
            if caracteres[i] != 0x00 and caracteres[i] != 0x23:
               texto.append(chr(caracteres[i]))
            i += 1
         except IndexError:
            break

   else:

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
      cursor.excute(sql)
      conexaoComBanco.commit()
      
   except mysql.connector.Error as e:
      print(f"Equipamento: {codEquipamento} {datetime.datetime.now()} erro de conexao MySQL: {e}")



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

   with pool.get_connection() as conexaoComBanco:
      with conexaoComBanco.cursor() as cursor:
         while tentativas < maxTentativas:
            try:
               cursor.executemany(sql, values)
               conexaoComBanco.commit()
               break  
            except mysql.connector.errors.InternalError as e:
               tentativas += 1
               if tentativas < maxTentativas:
                  time.sleep(3)  
               else: 
                  raise e
            except Exception as e:
               print(f"{datetime.datetime.now()} Erro em escreverLogNoBanco: {e}")
      
      



def buscarColunasPorTipoEquipamento(codTipoEquipamento: int, cursor):
   query = f"""
      SELECT colunas from colunas_por_tipo_equipamento WHERE cod_tipo_equipamento = {codTipoEquipamento};
   """

   cursor.excute(query)
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
   cursor.excute(query)
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


def buscarUltimaLinhaLog(codEquipamento, pool, tipoLog = 0):

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
   with pool.get_connection() as conexaoComBanco:
      with conexaoComBanco.cursor() as cursor:
         cursor.execute(query)
         return cursor.fetchone()
   return None

   


# def abreConexaoComBancoEexcutaFuncao(func, **kwargs):
   
#    with mysql.connector.connect(user=os.environ['MYSQL_USER'], 
#                           password=os.environ['MYSQL_PASSWORD'], 
#                           host=os.environ['MYSQL_HOST'], 
#                           database=os.environ['MYSQL_DATABASE']) as conexaoComBanco:
#       with conexaoComBanco.cursor() as cursor:
#             func(conexaoComBanco=conexaoComBanco, cursor=cursor, **kwargs)


def testaConexaoModbusERecuperaTipoEquipamento(idSolicitacao, host, porta:int):
   # print(f"Entrou em testaConexaoModbus...")
   req = gerarRequisicao(tipoLog=3)
   # print(f"Requisição: {req}")
   try:
      with conectarComModbus(idSolicitacao, host, porta) as conexaoComModbus:
         # print("Conectou com o modbus")
         conexaoComModbus.send(req)
         # print("requisição enviada")
         resposta = struct.unpack(
            ">3H3BH",
            conexaoComModbus.recv(1024))
         # print("resposta recebida")
         # print(f"Resposta = {resposta}")
         codTipoEquipamento = resposta[6]
         # print(f"codTipoEquipamento = {codTipoEquipamento}")
         # print(f"Saindo de testaConexaoModbus...")
         return codTipoEquipamento
   except TimeoutError as e:
      return e
   except BrokenPipeError as e:
      return e
   except Exception as e:
      print(f"Erro em testaConexaoModbusERecuperaTipoEquipamento: {e, traceback.format_exc()}")
      


def fetchLog(idSolicitacao: int,
             codEquipamento: int,
             modbusId: int,
             host: str,
             porta: int,
             tipoLog = 0):

   try:
      pool = mysql.connector.pooling.MySQLConnectionPool(
         pool_name="MySqlPool",
         pool_size=32,
         user=os.environ['MYSQL_USER'],
         password=os.environ['MYSQL_PASSWORD'],
         host=os.environ['MYSQL_HOST'],
         database=os.environ['MYSQL_DATABASE']
      )

      codTipoEquipamento = testaConexaoModbusERecuperaTipoEquipamento(idSolicitacao, host, porta)
      # print(f'codEquipamento - {codEquipamento} - codTipoEquipamento - {codTipoEquipamento}')

      if codTipoEquipamento == 0: # codTipoEquipamento == 0 quer dizer que não foi possível conectrar com o modbus
         with open("logRecuperaLogs.txt", 'a', encoding='utf-8') as file:
            file.write(f"{datetime.datetime.now()}       id:{id}        'Conexão com o equipamento {codEquipamento} não estabelecida'\n")
            return
      else:      
         ultimaLinha = buscarUltimaLinhaLog(codEquipamento, pool, tipoLog)

         if ultimaLinha is None:
            ultimaLinha = (0, 0, '', '', datetime.datetime(1900,1,1,0,0,0,0))
         
      # print(f"ultimaLinha: {ultimaLinha}")

      if tipoLog == 1:  # Log Alarmes
         if codTipoEquipamento == 88:
            ran = range(500, 651)
         elif codTipoEquipamento == 182 or codTipoEquipamento == 93:
            ran = range(500, 998, 3)
         else:
            ran = range(500, 1000)
      elif codTipoEquipamento == 88:
         ran = range(151)
      elif codTipoEquipamento == 182 or codTipoEquipamento == 93:
            ran = range(0, 500, 3)
      else:
         # Log Eventos
         ran = range(500)

         # print(f'ran - {ran}')

      with conectarComModbus(idSolicitacao, host, porta) as conexaoComModbus:
         try:
            values = []
            
            # if tipoLog == 0:
            registerValue = 0
            if tipoLog == 1:
               registerValue = 1
               
            req = struct.pack(
                  '>3H2B2HBH',
                  0, #transaction Id
                  0, # protocol id
                  9, # length
                  18, #unit Id
                  16, # function code (16)
                  58900, # reference number
                  1, # word count
                  2, #byte count
                  registerValue 
               )

            conexaoComModbus.send(req)
            res = conexaoComModbus.recv(1024)
            # print(f"Len res = {len(res)}")

            assert res == b'\x00\x00\x00\x00\x00\x06\x12\x10\xe6\x14\x00\x01'

            for startingAddress in ran:
               req = gerarRequisicao(startingAddress,modbusId,startingAddress, tipoLog,codTipoEquipamento ) # startingAddress é sempre o mesmo número que o transactionId
               # print(f'requisição {startingAddress} - {req.hex()}')
               conexaoComModbus.send(req)
               res = conexaoComModbus.recv(1024)
               # print(f'res - {res}')
               if codTipoEquipamento == 182 or codTipoEquipamento == 93:
                  respostas = processarRespostaModbus(codTipoEquipamento, res[9:])
                  # print(f"respostas: {respostas}")

                  try:
                     for resposta in respostas:
                        # print(f'resposta:{resposta}')
                        
                        nomeEvent, textEvent, date = resposta
                        # print(f"nomeEvent - {nomeEvent}")
                        # print(f"textEvent - {textEvent}")
                        # print(f"{date} {nomeEvent}")
                        
                        # values.append((str(codEquipamento), str(codTipoEquipamento), str(nomeEvent), str(textEvent), str(date)))
                        # print(str(codEquipamento), str(codTipoEquipamento), str(nomeEvent), str(textEvent), str(date))
                     
                        linha = (codEquipamento, codTipoEquipamento, nomeEvent, date)
                        if linha[3] >= ultimaLinha[4] and textEvent != ultimaLinha[3]:    #  and textEvent != ultimaLinha[3]Existem casos em que o mesmo alarme/evento se repetem com o mesmo horário (ultimaLinha[3] é a data e hora)
                                                                                          #  para esses casos vou considerar apenas um dos alarme/eventos. O que realmente importa é o nome
                                                                                          #  então exibir apenas um é o suficiente.
                           values.append((str(codEquipamento), str(codTipoEquipamento), str(nomeEvent), str(textEvent[93:]), str(date)))
                           # print(str(codEquipamento), str(codTipoEquipamento), str(nomeEvent), str(date))
                        
                  except mysql.connector.IntegrityError as e:  # Integrity Error aconteceu durante a excução devido ao Unique adicionado nas tabelas no banco
                                                               # modificando a exceção para 'pass' para pular para a próxima iteração e ignorar os valores repetidos
                     print(f"Equipamento: {codEquipamento} {datetime.datetime.now()} Erro de integridade MySQL: {e}")
                     with open("logRecuperaLogs.txt", 'a') as file:
                        file.write(f"{datetime.datetime.now()}       id:{idSolicitacao}        'Erro de integridade MySQL: {e}'\n") 
                  except TypeError as e: # O TypeError aqui vai indicar que a resposta do modbus foi vazia, logo, chegou ao fim do log e deve ser encerrado o fetchLog
                     # print(f"type error: {e}")
                     return 1
                  except Exception as e:
                     print(f"Equipamento: {codEquipamento} {datetime.datetime.now()} Erro ao processar resposta modbus em fetchLog: {e}")
                     return 0
               
               else:

                  try:
                        resposta = next(processarRespostaModbus(codTipoEquipamento, res)) # Usando 'next' pois processarRespostaModbus, por conta do yield
                                                                                          # no tratamento das respostas dos ASC, é uma função geradora. 
                                                                                          # Antes estava usando return e tava dando pau
                        # print(f'resposta:{resposta}')
                        
                        
                        nomeEvent, textEvent, date = resposta
                        # print(f"nomeEvent - {nomeEvent}")
                        # print(f"textEvente - {textEvent}")
                        # print(f"dataEvente - {date}")
                     
                        linha = (codEquipamento, codTipoEquipamento, nomeEvent, date)
                        if linha[3] >= ultimaLinha[4] and textEvent != ultimaLinha[3]:    #   Existem casos em que o mesmo alarme/evento se repetem com o mesmo horário (ultimaLinha[3] é a data e hora)
                                                                                       #  para esses casos vou considerar apenas um dos alarme/eventos. O que realmente importa é o nome
                                                                                       #  então exibir apenas um é o suficiente.
                           values.append((str(codEquipamento), str(codTipoEquipamento), str(nomeEvent), str(textEvent[93:]), str(date)))
                           # print(str(codEquipamento), str(codTipoEquipamento), str(nomeEvent), str(date))
                     
                  except mysql.connector.IntegrityError as e:  # Integrity Error aconteceu durante a excução devido ao Unique adicionado nas tabelas no banco
                                                               # modificando a exceção para 'pass' para pular para a próxima iteração e ignorar os valores repetidos
                     print(f"Equipamento: {codEquipamento} {datetime.datetime.now()} Erro de integridade MySQL: {e}")
                     with open("logRecuperaLogs.txt", 'a') as file:
                        file.write(f"{datetime.datetime.now()}       id:{idSolicitacao}        'Erro de integridade MySQL: {e}'\n") 
                  except TypeError as e: # O TypeError aqui vai indicar que a resposta do modbus foi vazia, logo, chegou ao fim do log e deve ser encerrado o fetchLog
                     # print(f"type error: {e}")
                     return
                  except ValueError as e: # ValueError acontecerá quando o campo de data for 0
                                          # o que significa que chegou ao fim do log
                     break
                  except Exception as e:
                     print(f"Equipamento: {codEquipamento} {datetime.datetime.now()} Erro ao processar resposta modbus em fetchLog {e}")
                     return
                  
               

         except mysql.connector.Error as e:
            print(f"Equipamento: {codEquipamento} {datetime.datetime.now()} erro na comunicacao com o banco de dados {e}")
            return 0
         except ConnectionResetError as e:
            print(f"Equipamento: {codEquipamento} {datetime.datetime.now()} Erro de conexao: {e}")
            return 0
         except TimeoutError as e:
            print(f"Equipamento: {codEquipamento} {datetime.datetime.now()} {e}")
            return 0
         finally:
            # print(values)
            escreverLogNoBanco(pool, values, tipoLog)

   except TimeoutError as e:
      print(f"Equipamento: {codEquipamento} {datetime.datetime.now()} {e}")
      return
   except BrokenPipeError as e:
      print(f"Equipamento: {codEquipamento} {datetime.datetime.now()} {e}")
      return
   except Exception as e:
      print(f"Equipamento: {codEquipamento} {datetime.datetime.now()} {e}")
      
   

def buscarSolicitacoes(cursor: mysql.connector.cursor):
   query = f"""SELECT
                  *
               FROM
                  solicitacao_log;
            """# WHERE
                 #  status = 0
   
   cursor.excute(query)
   return cursor.fetchall()


def main(idSolicitacao, codEquipamento, modbusId, host, porta, codTipoLog): # idSolicitacao, codEquipamento, modbusId, host, porta, codTipoLog
   inicio = time.time()

   fetchLog(idSolicitacao, codEquipamento, modbusId, host, porta, codTipoLog)

   fim = time.time()
   print(f"Equipamento: {codEquipamento}   {datetime.datetime.now()}   tempo de excução: {(fim-inicio):.2f} segundos")


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