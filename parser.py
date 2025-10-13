from decoder_gt06V4 import parser_gt06V4
from datetime import datetime
import os
import re
import csv
import pandas as pd

def main():
    print("=" * 60)
    print("GT06 MESSAGE PARSER - VERSÃO CSV")
    print("=" * 60)
    
    # Solicita o IMEI do usuário
    imei = input("Digite o IMEI do dispositivo: ").strip()
    if not imei:
        print("IMEI não informado. Usando 'default_imei'")
        imei = "default_imei"
    
    print(f"Arquivo CSV de saída: {imei}.csv")
    print("=" * 60)
    print("Opções disponíveis:")
    print("1. Digite mensagens hexadecimais individuais")
    print("2. Digite 'csv <caminho>' para processar arquivo CSV")
    print("3. Digite 'help' para ajuda")
    print("4. Digite 'quit' para sair")
    print("=" * 60)
    print()

    while True:
        try:
            # Recebe input do usuário
            cmd = input(">> ").strip()
            
            if not cmd:
                continue
            
            # Comandos especiais
            if cmd.lower() == 'quit':
                print("Encerrando parser...")
                break
            elif cmd.lower() == 'help':
                show_help()
                continue
            elif cmd.lower().startswith('csv '):
                file_path = cmd[4:].strip()
                process_csv_file(file_path, imei)
                continue
            
            # Processamento de mensagem individual
            process_single_message(cmd, imei)
            
        except KeyboardInterrupt:
            print("\n\nEncerrando parser...")
            break
        except Exception as e:
            print(f"[ERRO] Erro geral: {e}")
            print("-" * 60)
            print()

def show_help():
    """Exibe ajuda do sistema"""
    print("\n" + "=" * 60)
    print("AJUDA - GT06 MESSAGE PARSER CSV")
    print("=" * 60)
    print("COMANDOS:")
    print("  <hex_message>     - Analisa uma mensagem hexadecimal")
    print("  csv <caminho>     - Processa arquivo CSV com mensagens")
    print("  help              - Exibe esta ajuda")
    print("  quit              - Sair do programa")
    print()
    print("FORMATO DO ARQUIVO CSV:")
    print("  - Deve conter as colunas: 'lmsmensagem' e 'lmsdatahorainc'")
    print("  - Coluna 'lmsmensagem': dados hexadecimais (com ou sem aspas)")
    print("  - Coluna 'lmsdatahorainc': timestamp de inclusão")
    print("  - Linhas vazias na coluna 'lmsmensagem' são ignoradas")
    print()
    print("EXEMPLOS:")
    print("  >> 787811160A0A19120E29C606418C654064E1C40A1C000C0D0A")
    print("  >> csv dados_rastreamento.csv")
    print("=" * 60)
    print()

def process_single_message(cmd, imei):
    """Processa uma única mensagem hexadecimal"""
    # Remove espaços e converte para maiúsculas
    hex_data = cmd.replace(" ", "").upper()
    
    # Valida se é hexadecimal válido
    if not is_valid_hex(hex_data):
        print("[ERRO] Formato hexadecimal inválido!")
        return
    
    # Timestamp
    now = datetime.now()
    timestamp = now.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    
    print(f"\n{timestamp} - Analisando: {hex_data}")
    print("-" * 60)
    
    # Analisa a mensagem e salva no CSV
    analyze_message(hex_data, imei, timestamp)
    
    print("-" * 60)
    print()

def process_csv_file(file_path, imei):
    """Processa arquivo CSV com múltiplas mensagens"""
    print(f"\n[CSV] Processando arquivo: {file_path}")
    print("=" * 60)
    
    if not os.path.exists(file_path):
        print(f"[ERRO] Arquivo não encontrado: {file_path}")
        return
    
    try:
        # Lê o arquivo CSV
        print("[CSV] Carregando dados do arquivo CSV...")
        df = pd.read_csv(file_path)
        
        # Verifica se as colunas necessárias existem
        required_columns = ['lmsmensagem', 'lmsdatahorainc']
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            print(f"[ERRO] Colunas obrigatórias não encontradas: {missing_columns}")
            print(f"[INFO] Colunas disponíveis: {list(df.columns)}")
            return
        
        # Remove linhas onde lmsmensagem está vazia ou NaN
        df_clean = df.dropna(subset=['lmsmensagem'])
        df_clean = df_clean[df_clean['lmsmensagem'].str.strip() != '']
        
        total_rows = len(df)
        valid_rows = len(df_clean)
        processed = 0
        errors = 0
        
        print(f"[CSV] Total de linhas no arquivo: {total_rows}")
        print(f"[CSV] Linhas válidas para processamento: {valid_rows}")
        print("-" * 60)
        
        # Processa cada linha válida
        for index, row in df_clean.iterrows():
            try:
                # Extrai os dados
                hex_message = str(row['lmsmensagem']).strip()
                timestamp_inc = str(row['lmsdatahorainc']).strip()
                
                # Remove aspas se existirem
                hex_message = hex_message.strip('"\'')
                
                # Remove espaços e converte para maiúsculas
                hex_data = hex_message.replace(" ", "").upper()
                
                print(f"\n[LINHA {index + 1}] Processando...")
                print(f"[DADOS] Hex: {hex_data[:50]}{'...' if len(hex_data) > 50 else ''}")
                print(f"[TIMESTAMP] Inclusão: {timestamp_inc}")
                print("-" * 40)
                
                # Valida se é hexadecimal válido
                if not is_valid_hex(hex_data):
                    print(f"[ERRO] Linha {index + 1}: Formato hexadecimal inválido")
                    errors += 1
                    continue
                
                # Formata o timestamp se necessário
                formatted_timestamp = format_timestamp(timestamp_inc)
                
                # Analisa a mensagem e salva no CSV
                analyze_message(hex_data, imei, formatted_timestamp)
                processed += 1
                
            except Exception as e:
                print(f"[ERRO] Linha {index + 1}: {e}")
                errors += 1
        
        # Resumo do processamento
        print("\n" + "=" * 60)
        print("RESUMO DO PROCESSAMENTO CSV")
        print("=" * 60)
        print(f"Total de linhas no arquivo: {total_rows}")
        print(f"Linhas válidas: {valid_rows}")
        print(f"Mensagens processadas: {processed}")
        print(f"Erros encontrados: {errors}")
        print(f"Taxa de sucesso: {(processed/max(valid_rows, 1)*100):.1f}%")
        print("=" * 60)
        print()
        
    except Exception as e:
        print(f"[ERRO] Erro ao processar arquivo CSV: {e}")
        import traceback
        traceback.print_exc()

def format_timestamp(timestamp_str):
    """Formata timestamp do CSV para o formato esperado"""
    try:
        # Tenta diferentes formatos de timestamp
        formats_to_try = [
            "%Y-%m-%d %H:%M:%S.%f",  # 2025-05-03 00:03:23.724
            "%Y-%m-%d %H:%M:%S",     # 2025-05-03 00:03:23
            "%d/%m/%Y %H:%M:%S",     # 03/05/2025 00:03:23
            "%Y/%m/%d %H:%M:%S",     # 2025/05/03 00:03:23
        ]
        
        for fmt in formats_to_try:
            try:
                dt = datetime.strptime(timestamp_str, fmt)
                return dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
            except ValueError:
                continue
        
        # Se não conseguir converter, retorna o original
        print(f"[AVISO] Não foi possível formatar timestamp: {timestamp_str}")
        return timestamp_str
        
    except Exception as e:
        print(f"[AVISO] Erro ao formatar timestamp {timestamp_str}: {e}")
        return timestamp_str

def analyze_message(hex_data, imei, timestamp):
    """Analisa uma mensagem (GT06 ou outras) e salva no CSV"""
    # Verifica se é mensagem GT06
    if is_gt06_message(hex_data):
        print("[PROTOCOLO] GT06 detectado")
        analyze_gt06_message(hex_data, imei, timestamp)
    else:
        print("[PROTOCOLO] Protocolo não identificado ou não GT06")
        print(f"[RAW] Dados: {hex_data}")
        
        # Salva mensagem não identificada no CSV
        save_to_csv(imei, timestamp, hex_data, "Protocolo não identificado", "N/A", "N/A", "N/A")
        
        # Tenta identificar outros padrões
        identify_other_protocols(hex_data)

def identify_other_protocols(hex_data):
    """Tenta identificar outros protocolos comuns"""
    if hex_data.startswith("7979"):
        print("[POSSÍVEL] Protocolo GT09/GT02 detectado")
    elif hex_data.startswith("2424"):  # $$
        print("[POSSÍVEL] Protocolo baseado em ASCII detectado")
    elif len(hex_data) >= 4:
        # Verifica se pode ser ASCII
        try:
            ascii_test = bytes.fromhex(hex_data[:20]).decode('ascii', errors='ignore')
            if ascii_test.isprintable():
                print(f"[ASCII] Possível conteúdo ASCII: {ascii_test}")
        except:
            pass

def is_valid_hex(hex_string):
    """Verifica se a string é hexadecimal válida"""
    try:
        int(hex_string, 16)
        return len(hex_string) % 2 == 0  # Deve ter número par de caracteres
    except ValueError:
        return False

def is_gt06_message(hex_data):
    """Verifica se a mensagem é do protocolo GT06"""
    return hex_data.startswith("7878") and hex_data.endswith("0D0A")

def analyze_gt06_message(hex_data, imei, timestamp):
    """Analisa mensagem GT06 usando o parser existente"""
    try:
        # Extrai informações básicas do protocolo GT06
        if len(hex_data) < 12:
            print("[ERRO] Mensagem GT06 muito curta")
            save_to_csv(imei, timestamp, hex_data, "GT06", "Erro", "Mensagem muito curta", "N/A")
            return
        
        # Estrutura básica GT06: 7878 + LENGTH + PROTOCOL + DATA + SERIAL + CRC + 0D0A
        length = int(hex_data[4:6], 16)
        protocol = hex_data[6:8]
        
        print(f"[GT06] Length: {length}")
        print(f"[GT06] Protocol: 0x{protocol}")
        
        # Mapeia protocolos conhecidos
        protocol_map = {
            "01": "Login",
            "13": "Heartbeat",
            "16": "GPS Location",
            "32": "Alarm",
            "15": "Command Response",
            "80": "Command/Configuration",
            "05": "General Response"
        }
        
        protocol_name = protocol_map.get(protocol, "Desconhecido")
        print(f"[GT06] Tipo: {protocol_name}")
        
        # Extrai serial (sempre nos últimos 4 chars antes do CRC)
        serial_start = 6 + (length - 5) * 2
        serial = "N/A"
        if serial_start + 4 <= len(hex_data) - 6:  # -6 para CRC + stop bits
            serial = hex_data[serial_start:serial_start + 4]
            print(f"[GT06] Serial: 0x{serial}")
        
        # Chama o parser original se disponível
        parser_result = "N/A"
        try:
            result = parser_gt06V4(hex_data, imei, timestamp)
            if result:
                parser_result = str(result)
                print(f"[PARSER] Resultado: {result}")
        except Exception as parser_error:
            parser_result = f"Erro no parser: {parser_error}"
            print(f"[PARSER] Erro no parser GT06: {parser_error}")
        
        # Salva no CSV
        save_to_csv(imei, timestamp, hex_data, "GT06", protocol_name, serial, parser_result)
        
        # Análise detalhada por protocolo
        analyze_protocol_details(hex_data, protocol, length)
        
    except Exception as e:
        print(f"[ERRO] Erro na análise GT06: {e}")
        save_to_csv(imei, timestamp, hex_data, "GT06", "Erro", f"Erro na análise: {e}", "N/A")

def save_to_csv(imei, timestamp, hex_data, protocol, message_type, serial, parser_result):
    """Salva a mensagem no arquivo CSV"""
    filename = f"{imei}.csv"
    file_exists = os.path.isfile(filename)
    
    try:
        with open(filename, 'a', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['Data/Hora Inclusão', 'hex_data', 'protocol', 'message_type', 'serial', 'parser_result']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            # Escreve cabeçalho se o arquivo não existir
            if not file_exists:
                writer.writeheader()
            
            # Escreve os dados
            writer.writerow({
                'Data/Hora Inclusão': timestamp,
                'hex_data': hex_data,
                'protocol': protocol,
                'message_type': message_type,
                'serial': serial,
                'parser_result': parser_result
            })
        
        print(f"[CSV] Mensagem salva em {filename}")
        
    except Exception as e:
        print(f"[ERRO] Erro ao salvar no CSV: {e}")

def analyze_protocol_details(hex_data, protocol, length):
    """Análise detalhada baseada no tipo de protocolo"""
    try:
        data_start = 8  # Após 7878 + LENGTH + PROTOCOL
        
        if protocol == "01":  # Login
            analyze_login_message(hex_data, data_start, length)
        elif protocol == "16":  # GPS Location
            analyze_gps_message(hex_data, data_start, length)
        elif protocol == "13":  # Heartbeat
            analyze_heartbeat_message(hex_data, data_start, length)
        elif protocol == "80":  # Command/Configuration
            analyze_command_message(hex_data, data_start, length)
        
    except Exception as e:
        print(f"[ERRO] Erro na análise detalhada: {e}")

def analyze_login_message(hex_data, start, length):
    """Analisa mensagem de login"""
    print("[LOGIN] Analisando dados de login...")
    if length >= 10:  # IMEI (8 bytes) + outras informações
        imei_hex = hex_data[start:start + 16]  # 8 bytes = 16 chars hex
        print(f"[LOGIN] IMEI (hex): {imei_hex}")
        
        # Converte IMEI para decimal (BCD)
        try:
            imei_str = ""
            for i in range(0, len(imei_hex), 2):
                byte_val = imei_hex[i:i+2]
                imei_str += byte_val
            print(f"[LOGIN] IMEI: {imei_str}")
        except:
            print(f"[LOGIN] Erro ao decodificar IMEI")

def analyze_gps_message(hex_data, start, length):
    """Analisa mensagem GPS"""
    print("[GPS] Analisando dados de localização...")
    # Implementação simplificada - você pode expandir conforme necessário
    if length >= 20:
        print(f"[GPS] Dados brutos: {hex_data[start:start + 40]}")

def analyze_heartbeat_message(hex_data, start, length):
    """Analisa mensagem de heartbeat"""
    print("[HBD] Mensagem de heartbeat detectada")
    if length >= 5:
        # Informações básicas do heartbeat
        hbd_data = hex_data[start:start + 10]
        print(f"[HBD] Dados: {hbd_data}")

def analyze_command_message(hex_data, start, length):
    """Analisa mensagem de comando"""
    print("[CMD] Analisando comando/configuração...")
    try:
        # Pula os 4 bytes do server flag (00000001)
        cmd_start = start + 8
        cmd_data = hex_data[cmd_start:]
        
        # Remove serial e CRC do final
        cmd_end = len(cmd_data) - 8  # 4 bytes serial + 2 bytes CRC + 2 bytes stop
        if cmd_end > 0:
            command_hex = cmd_data[:cmd_end]
            
            # Tenta converter para ASCII
            try:
                command_ascii = bytes.fromhex(command_hex).decode('ascii', errors='ignore')
                print(f"[CMD] Comando ASCII: {command_ascii}")
            except:
                print(f"[CMD] Comando (hex): {command_hex}")
                
    except Exception as e:
        print(f"[CMD] Erro ao analisar comando: {e}")

if __name__ == "__main__":
    main()