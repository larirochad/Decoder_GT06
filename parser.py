from decoder_gt06V4 import parser_gt06V4
from datetime import datetime

def main():
    print("=" * 60)
    print("GT06 MESSAGE PARSER - VERSÃO TERMINAL")
    print("=" * 60)
    print("Digite mensagens em formato hexadecimal para análise.")
    print("=" * 60)
    print()

    while True:
        try:
            # Recebe input do usuário
            cmd = input(">> ").strip()
            
            if not cmd:
                continue
            
            # Remove espaços e converte para maiúsculas
            hex_data = cmd.replace(" ", "").upper()
            
            # Valida se é hexadecimal válido
            if not is_valid_hex(hex_data):
                print("[ERRO] Formato hexadecimal inválido!")
                continue
            
            # Timestamp
            now = datetime.now()
            timestamp = now.strftime("%d/%m/%Y,%H:%M:%S")
            
            print(f"\n{timestamp} - Analisando: {hex_data}")
            print("-" * 60)
            
            # Verifica se é mensagem GT06
            if is_gt06_message(hex_data):
                print("[PROTOCOLO] GT06 detectado")
                analyze_gt06_message(hex_data)
            
            print("-" * 60)
            print()
            
        except KeyboardInterrupt:
            print("\n\nEncerrando parser...")
            break
        except Exception as e:
            print(f"[ERRO] Erro ao processar mensagem: {e}")
            print("-" * 60)
            print()

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

def analyze_gt06_message(hex_data):
    """Analisa mensagem GT06 usando o parser existente"""
    try:
        # Extrai informações básicas do protocolo GT06
        if len(hex_data) < 12:
            print("[ERRO] Mensagem GT06 muito curta")
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
        if serial_start + 4 <= len(hex_data) - 6:  # -6 para CRC + stop bits
            serial = hex_data[serial_start:serial_start + 4]
            print(f"[GT06] Serial: 0x{serial}")
        
        # Chama o parser original se disponível
        try:
            # IMEI fictício para teste (você pode modificar conforme necessário)
            test_imei = "123456789012345"
            result = parser_gt06V4(hex_data, test_imei)
            if result:
                print(f"[PARSER] Resultado: {result}")
        except Exception as parser_error:
            print(f"[PARSER] Erro no parser GT06: {parser_error}")
        
        # Análise detalhada por protocolo
        analyze_protocol_details(hex_data, protocol, length)
        
    except Exception as e:
        print(f"[ERRO] Erro na análise GT06: {e}")

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