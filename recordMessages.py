import datetime
import pytz
from datetime import datetime

def record_raw(file_name, source, msg):
    curr_time = datetime.now()
    date_time = curr_time.strftime("%Y-%m-%d %H:%M:%S,")
    with open(file_name, "a+") as f:
        f.write(date_time + source + ',' + msg + "\n")
        f.close()

def record_decoded(file_name, msg):
    try:
        open(file_name, "r")
        with open(file_name, "a+") as d:
            curr_time = datetime.now()
            date_time = curr_time.strftime("%Y-%m-%d %H:%M:%S,")
            d.write(date_time + msg + "\n")
            d.close()
        #print("Arquivo já existente, o log será adicionado a ele.")
    except:
        print("Arquivo não existe, um novo log será criado.")
        with open(file_name, "a+") as d:
            d.write("Data/Hora Inclusão,Data/Hora Evento,IMEI,Sequência,"
                    "Tipo Mensagem,Tipo Dispositivo,Versão Protocolo,Versão Firmware,"
                    "Alimentação Externa,Analog Input Status,"
                    "Satélites,Duração da Ignição,"
                    "Velocidade,Azimuth,Altitude,Latitude,Longitude,MCC,MNC,LAC,Cell ID, Realtime positioning, GPS valido,"
                    "Hodômetro Total,Horímetro Total,"
                    "RAT\n")

            curr_time = datetime.now()
            date_time = curr_time.strftime("%Y-%m-%d %H:%M:%S")
            d.write(date_time + msg + "\n")
            d.close()



def hex_to_timestamp(hex_value):
    hex_value = str(hex_value)

    try:
        if len(hex_value) == 12:
            # Formato padrão: 6 bytes (YYMMDDHHMMSS)
            year = 2000 + int(hex_value[0:2], 16)
            month = int(hex_value[2:4], 16)
            day = int(hex_value[4:6], 16)
            hour = int(hex_value[6:8], 16)
            minute = int(hex_value[8:10], 16)
            second = int(hex_value[10:12], 16)

        elif len(hex_value) == 14:
            # Formato alternativo: 7 bytes (YYYYMMDDHHMMSS)
            year = int(hex_value[0:4], 16)
            month = int(hex_value[4:6], 16)
            day = int(hex_value[6:8], 16)
            hour = int(hex_value[8:10], 16)
            minute = int(hex_value[10:12], 16)
            second = int(hex_value[12:14], 16)

        else:
            # Formato inválido
            raise ValueError("Tamanho inválido de string hexadecimal para data/hora.")

        # Validação segura de data
        dt = datetime(year, month, day, hour, minute, second)

    except Exception:
        # Fallback seguro
        dt = datetime(2020, 1, 1, 0, 0, 0)

    return dt


def converter_para_brasil(dt_utc):
    """
    Converte um datetime UTC para o timezone do Brasil e o formata como "yy-mm-dd HH:MM:SS"

    Args:
        dt_utc (datetime ou str): Objeto datetime em timezone UTC ou string em vários formatos possíveis

    Returns:
        str: Data e hora formatadas no timezone do Brasil
    """
    # Verificar se a entrada é uma string
    if isinstance(dt_utc, str):
        try:
            # Tenta diversos formatos possíveis
            formatos = [
                "%Y%m%d%H%M%S",  # 20250408223920
                "%Y-%m-%d %H:%M:%S",  # 2025-04-08 22:39:20
                "%y-%m-%d %H:%M:%S"  # 25-04-08 22:39:20
            ]

            for formato in formatos:
                try:
                    dt_utc = datetime.strptime(dt_utc, formato)
                    break  # Se conseguir converter, sai do loop
                except ValueError:
                    continue
            else:
                # Se nenhum formato funcionar (o loop terminou sem break)
                return f"Erro: Não foi possível converter a string '{dt_utc}' para datetime"

        except Exception as e:
            return f"Erro: {str(e)}"

    # Agora temos certeza que dt_utc é um objeto datetime
    # Verificar se o datetime tem timezone definido
    if dt_utc.tzinfo is None or dt_utc.tzinfo.utcoffset(dt_utc) is None:
        # Se não tiver timezone, assume que é UTC
        dt_utc = pytz.UTC.localize(dt_utc)
    elif dt_utc.tzinfo != pytz.UTC:
        # Se tiver timezone mas não for UTC, converte para UTC primeiro
        dt_utc = dt_utc.astimezone(pytz.UTC)

    # Converter para timezone do Brasil
    timezone_brasil = pytz.timezone('America/Sao_Paulo')
    dt_brasil = dt_utc.astimezone(timezone_brasil)

    # Formatar conforme solicitado
    formato_brasil = dt_brasil.strftime("%Y-%m-%d %H:%M:%S")

    return formato_brasil


def separar_partes_comando(command_string):
    # Verifica se existe o caracter ":" na string
    if ":" in command_string:
        # Divide a string no primeiro ":" encontrado
        partes = command_string.split(":", 1)
        primeira_parte = partes[0]  # Parte antes do ":"
        segunda_parte = partes[1]  # Parte depois do ":"

        # Verifica se a primeira parte existe (não está vazia)
        if primeira_parte.strip() == "":
            return False, "Primeira parte está vazia", "", ""

        # Verifica se a primeira parte é um número válido diferente de zero
        try:
            numero = int(primeira_parte)
            if numero == 0:
                return False, "Número antes do ':' é zero", primeira_parte, segunda_parte
            else:
                return True, "Primeira parte válida", primeira_parte, segunda_parte
        except ValueError:
            return False, "Primeira parte não é um número válido", primeira_parte, segunda_parte
    else:
        # Retorna erro se não encontrar ":"
        return False, "Comando não contém o caracter ':'", "", ""


# Exemplo de uso
'''comando = "8674886297271:AT+GTFRI=gv58cg,1,0,,0,0000,0000,60,60,1000,1000,,0,3600,00068102,0,,0,FFFF$"
valido, mensagem, primeira_parte, segunda_parte = separar_partes_comando(comando)

if valido:
    print(f"Comando válido: {mensagem}")
    print(f"Primeira parte: {primeira_parte}")
    print(f"Segunda parte: {segunda_parte}")
else:
    print(f"Comando inválido: {mensagem}")
    if primeira_parte or segunda_parte:
        print(f"Primeira parte: {primeira_parte}")
        print(f"Segunda parte: {segunda_parte}")'''