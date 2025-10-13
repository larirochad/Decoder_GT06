from decoder_gt06V4 import parser_gt06V4
from datetime import datetime
import os
import re
import csv
import pandas as pd
from pathlib import Path

def main():
    print("=" * 60)
    print("GT06 MESSAGE PARSER - VERSÃO CSV")
    print("=" * 60)
    
    # Solicita o IMEI do usuário
    imei = input("Digite o IMEI do dispositivo (ou deixe em branco para modo pasta): ").strip()
    if not imei:
        print("IMEI não informado. Modo individual desabilitado.")
        imei = None
    else:
        print(f"Arquivo CSV de saída: {imei}.csv")
    
    print("=" * 60)
    print("Opções disponíveis:")
    print("1. Digite mensagens hexadecimais individuais")
    print("2. Digite 'csv <caminho>' para processar arquivo CSV")
    print("3. Digite 'folder <caminho>' para processar pasta com CSVs")
    print("4. Digite 'help' para ajuda")
    print("5. Digite 'quit' para sair")
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
                if not imei:
                    print("[ERRO] Necessário informar IMEI para processar arquivo individual")
                    continue
                file_path = cmd[4:].strip()
                process_csv_file(file_path, imei)
                continue
            elif cmd.lower().startswith('folder '):
                folder_path = cmd[7:].strip()
                process_folder(folder_path)
                continue
            
            # Processamento de mensagem individual
            if not imei:
                print("[ERRO] Necessário informar IMEI para processar mensagens individuais")
                continue
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
    print("  folder <caminho>  - Processa todos os CSVs de uma pasta")
    print("  help              - Exibe esta ajuda")
    print("  quit              - Sair do programa")
    print()
    print("FORMATO DO ARQUIVO CSV:")
    print("  - Deve conter as colunas: 'lmsmensagem' e 'lmsdatahorainc'")
    print("  - Coluna 'lmsmensagem': dados hexadecimais (com ou sem aspas)")
    print("  - Coluna 'lmsdatahorainc': timestamp de inclusão")
    print("  - Linhas vazias na coluna 'lmsmensagem' são ignoradas")
    print()
    print("MODO PASTA:")
    print("  - Processa todos os arquivos .csv da pasta especificada")
    print("  - Cada CSV gera um arquivo: <nome_original>_decoded.csv")
    print("  - O IMEI é extraído do nome do arquivo original")
    print("  - Arquivos já processados (*_decoded.csv) são ignorados")
    print()
    print("EXEMPLOS:")
    print("  >> 787811160A0A19120E29C606418C654064E1C40A1C000C0D0A")
    print("  >> csv dados_rastreamento.csv")
    print("  >> folder C:/Mapeamento/dia30")
    print("  >> folder ./dados")
    print("=" * 60)
    print()

def process_folder(folder_path):
    """Processa todos os arquivos CSV de uma pasta"""
    print(f"\n[FOLDER] Processando pasta: {folder_path}")
    print("=" * 60)
    
    if not os.path.exists(folder_path):
        print(f"[ERRO] Pasta não encontrada: {folder_path}")
        return
    
    if not os.path.isdir(folder_path):
        print(f"[ERRO] O caminho especificado não é uma pasta: {folder_path}")
        return
    
    try:
        # Lista todos os arquivos CSV na pasta
        csv_files = []
        for file in os.listdir(folder_path):
            # Ignora arquivos já processados (_decoded.csv)
            if file.endswith('.csv') and not file.endswith('_decoded.csv'):
                csv_files.append(file)
        
        if not csv_files:
            print("[AVISO] Nenhum arquivo CSV encontrado na pasta (arquivos *_decoded.csv são ignorados)")
            return
        
        print(f"[FOLDER] Encontrados {len(csv_files)} arquivo(s) CSV para processar")
        print("-" * 60)
        
        total_files = len(csv_files)
        processed_files = 0
        failed_files = 0
        
        # Processa cada arquivo CSV
        for idx, csv_file in enumerate(csv_files, 1):
            print(f"\n[{idx}/{total_files}] Processando arquivo: {csv_file}")
            print("=" * 60)
            
            # Caminho completo do arquivo
            input_path = os.path.join(folder_path, csv_file)
            
            # Extrai o IMEI do nome do arquivo (remove extensão .csv)
            file_imei = os.path.splitext(csv_file)[0]
            
            # Remove zero à esquerda se existir (IMEI tem 15 dígitos)
            if file_imei.startswith('0') and len(file_imei) == 16:
                file_imei = file_imei[1:]
            
            # Nome do arquivo de saída
            output_filename = f"{file_imei}_decoded.csv"
            output_path = os.path.join(folder_path, output_filename)
            
            print(f"[INFO] IMEI extraído: {file_imei}")
            print(f"[INFO] Arquivo de saída: {output_filename}")
            print("-" * 60)
            
            try:
                # Processa o arquivo CSV individual
                success = process_csv_file_for_folder(input_path, file_imei, output_path)
                
                if success:
                    processed_files += 1
                    print(f"[OK] Arquivo {csv_file} processado com sucesso!")
                else:
                    failed_files += 1
                    print(f"[FALHA] Erro ao processar {csv_file}")
                    
            except Exception as e:
                failed_files += 1
                print(f"[ERRO] Exceção ao processar {csv_file}: {e}")
                import traceback
                traceback.print_exc()
            
            print("=" * 60)
        
        # Resumo final
        print("\n" + "=" * 60)
        print("RESUMO DO PROCESSAMENTO DA PASTA")
        print("=" * 60)
        print(f"Total de arquivos encontrados: {total_files}")
        print(f"Arquivos processados com sucesso: {processed_files}")
        print(f"Arquivos com falha: {failed_files}")
        print(f"Taxa de sucesso: {(processed_files/max(total_files, 1)*100):.1f}%")
        print(f"Pasta: {folder_path}")
        print("=" * 60)
        print()
        
    except Exception as e:
        print(f"[ERRO] Erro ao processar pasta: {e}")
        import traceback
        traceback.print_exc()

def process_csv_file_for_folder(input_path, imei, output_path):
    """Processa arquivo CSV individual para o modo pasta"""
    try:
        # Lê o arquivo CSV
        print("[CSV] Carregando dados do arquivo CSV...")
        df = pd.read_csv(input_path)
        
        # Verifica se as colunas necessárias existem
        required_columns = ['lmsmensagem', 'lmsdatahorainc']
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            print(f"[ERRO] Colunas obrigatórias não encontradas: {missing_columns}")
            print(f"[INFO] Colunas disponíveis: {list(df.columns)}")
            return False
        
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
        
        # Remove arquivo de saída se já existir
        if os.path.exists(output_path):
            os.remove(output_path)
            print(f"[INFO] Arquivo de saída anterior removido")
        
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
                
                # Valida se é hexadecimal válido
                if not is_valid_hex(hex_data):
                    print(f"[ERRO] Linha {index + 1}: Formato hexadecimal inválido")
                    errors += 1
                    continue
                
                # Formata o timestamp se necessário
                formatted_timestamp = format_timestamp(timestamp_inc)
                
                # Analisa a mensagem e salva no CSV de saída
                analyze_message_to_file(hex_data, imei, formatted_timestamp, output_path)
                processed += 1
                
                # Mostra progresso a cada 100 linhas
                if processed % 100 == 0:
                    print(f"[PROGRESSO] {processed}/{valid_rows} mensagens processadas...")
                
            except Exception as e:
                print(f"[ERRO] Linha {index + 1}: {e}")
                errors += 1
        
        # Resumo do processamento
        print("-" * 60)
        print(f"Mensagens processadas: {processed}")
        print(f"Erros encontrados: {errors}")
        print(f"Taxa de sucesso: {(processed/max(valid_rows, 1)*100):.1f}%")
        print(f"Arquivo salvo: {output_path}")
        
        return processed > 0
        
    except Exception as e:
        print(f"[ERRO] Erro ao processar arquivo CSV: {e}")
        import traceback
        traceback.print_exc()
        return False

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
    output_path = f"{imei}.csv"
    analyze_message_to_file(hex_data, imei, timestamp, output_path)
    
    print("-" * 60)
    print()

def process_csv_file(file_path, imei):
    """Processa arquivo CSV com múltiplas mensagens"""
    output_path = f"{imei}.csv"
    return process_csv_file_for_folder(file_path, imei, output_path)

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
        return timestamp_str
        
    except Exception as e:
        return timestamp_str

def analyze_message_to_file(hex_data, imei, timestamp, output_path):
    """Analisa uma mensagem (GT06 ou outras) e salva no arquivo CSV especificado"""
    # Verifica se é mensagem GT06
    if is_gt06_message(hex_data):
        protocol = "GT06"
        analyze_gt06_message_to_file(hex_data, imei, timestamp, output_path)
    else:
        protocol = "Protocolo não identificado"
        
        # Salva mensagem não identificada no CSV
        save_to_csv_file(imei, timestamp, hex_data, protocol, "N/A", "N/A", "N/A", output_path)

def analyze_gt06_message_to_file(hex_data, imei, timestamp, output_path):
    """Analisa mensagem GT06 usando o parser existente e salva no arquivo"""
    try:
        # Extrai informações básicas do protocolo GT06
        if len(hex_data) < 12:
            save_to_csv_file(imei, timestamp, hex_data, "GT06", "Erro", "Mensagem muito curta", "N/A", output_path)
            return
        
        # Estrutura básica GT06: 7878 + LENGTH + PROTOCOL + DATA + SERIAL + CRC + 0D0A
        length = int(hex_data[4:6], 16)
        protocol = hex_data[6:8]
        
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
        
        # Extrai serial (sempre nos últimos 4 chars antes do CRC)
        serial_start = 6 + (length - 5) * 2
        serial = "N/A"
        if serial_start + 4 <= len(hex_data) - 6:
            serial = hex_data[serial_start:serial_start + 4]
        
        # Chama o parser original se disponível
        parser_result = "N/A"
        try:
            result = parser_gt06V4(hex_data, imei, timestamp)
            if result:
                parser_result = str(result)
        except Exception as parser_error:
            parser_result = f"Erro no parser: {parser_error}"
        
        # Salva no CSV
        save_to_csv_file(imei, timestamp, hex_data, "GT06", protocol_name, serial, parser_result, output_path)
        
    except Exception as e:
        save_to_csv_file(imei, timestamp, hex_data, "GT06", "Erro", f"Erro na análise: {e}", "N/A", output_path)

def save_to_csv_file(imei, timestamp, hex_data, protocol, message_type, serial, parser_result, output_path):
    """Salva a mensagem no arquivo CSV especificado"""
    file_exists = os.path.isfile(output_path)
    
    try:
        with open(output_path, 'a', newline='', encoding='utf-8') as csvfile:
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
        
    except Exception as e:
        print(f"[ERRO] Erro ao salvar no CSV: {e}")

def is_valid_hex(hex_string):
    """Verifica se a string é hexadecimal válida"""
    try:
        int(hex_string, 16)
        return len(hex_string) % 2 == 0
    except ValueError:
        return False
    

def is_gt06_message(hex_data):
    """Verifica se a mensagem é do protocolo GT06"""
    return hex_data.startswith("7878") and hex_data.endswith("0D0A")

if __name__ == "__main__":
    main()