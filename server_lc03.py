import socket
import threading
import os
import time
import json
from datetime import datetime
import ifaddr  
from decoder_gt06V4 import *
# CORREÇÃO: Ajustar o nome do arquivo para coincidir com o nome real
from recordMessages import record_decoded_organized  # Movido para cima para evitar importação dupla

# Cache global
imei_cache = {} 
command_queue = {}  # {imei: {'commands': [], 'current_index': 0, 'waiting_ack': False, 'last_command': '', 'client_socket': None}}
client_connections = {}  # {imei: client_socket} - para envio manual de comandos
config_file_commands = "comandos_config.txt"
state_file = "command_state.json"
raw_log_directory = "logs/"
os.makedirs(raw_log_directory, exist_ok=True)

# Adicione esta linha para inicializar o contador serial
command_serial_counter = 1  # Inicia em 1 para evitar valor zero

# Crie um único arquivo para todas as mensagens
raw_combined_file = os.path.join(raw_log_directory, "raw.csv")

if not os.path.exists(raw_combined_file):
    with open(raw_combined_file, "w") as f:
        f.write("Data/Hora,Direção,Tipo,Mensagem\n")


def get_next_serial():
    """Retorna próximo número serial para comandos"""
    global command_serial_counter
    

    # Inicializa se não existir
    if 'command_serial_counter' not in globals():
        command_serial_counter = 1
    
    serial = command_serial_counter
    command_serial_counter = (command_serial_counter + 1) % 0xFFFF
    if command_serial_counter == 0:
        command_serial_counter = 1
    return serial


def calcular_crc_itu(data_bytes):
    """Calcula CRC-ITU para o protocolo GT06"""
    crc = 0xFFFF

    # Calcula CRC-ITU de acordo com o algoritmo
    for byte in data_bytes:
        crc ^= byte << 8
        for _ in range(8):
            if crc & 0x8000:
                crc = (crc << 1) ^ 0x1021
            else:
                crc <<= 1
            crc &= 0xFFFF
    return crc

def comando_para_protocolo_gt06(comando):
    # print(f"[DEBUG] Comando recebido para formatação: '{comando}'")
    comando = comando.strip()

    # Constantes fixas
    start_bit = bytes([0x78, 0x78])
    protocol_number = bytes([0x80])
    stop_bit = bytes([0x0D, 0x0A])
    server_flag = bytes([0x00, 0x00, 0x00, 0x01])

    # Comando em ASCII
    comando_ascii = comando.encode('ascii')
    # print(f"[DEBUG] Comando ASCII (bytes): {comando_ascii}")

    # Comprimento do comando: server_flag (4 bytes) + comando ASCII
    length_of_command = bytes([len(server_flag) + len(comando_ascii)])

    # Serial incremental (mantém fora do bloco de comando!)
    serial_number = get_next_serial()
    serial_bytes = serial_number.to_bytes(2, 'big')

    # Construindo o "Information Content"
    information_content = (
        length_of_command +
        server_flag +
        comando_ascii
    )

    # Packet Length: protocol(1) + length_of_command(1) + flag(4) + comando + serial(2) + crc(2)
    packet_length = len(protocol_number) + len(information_content) + len(serial_bytes) + 2  # CRC

    # CRC: do Packet Length até Serial
    data_for_crc = (
        bytes([packet_length]) +
        protocol_number +
        information_content +
        serial_bytes
    )
    crc = calcular_crc_itu(data_for_crc)
    crc_bytes = crc.to_bytes(2, 'big')

    # Pacote final
    pacote_completo = (
        start_bit +
        bytes([packet_length]) +
        protocol_number +
        information_content +
        serial_bytes +
        crc_bytes +
        stop_bit
    )

    # print(f"[DEBUG] Pacote GT06 final: {pacote_completo.hex().upper()}")
    return pacote_completo

# Carrega configurações
def carregar_configuracao(arquivo):
    configuracoes = {}
    if os.path.exists(arquivo):
        with open(arquivo, "r") as f:
            for linha in f:
                if "=" in linha:
                    chave, valor = linha.strip().split("=", 1)
                    configuracoes[chave] = valor
    return configuracoes

# Thread para entrada manual de comandos
def worker_comandos_manuais():
    """Thread para receber comandos manuais do usuário"""
    print("\n" + "="*60)
    print("COMANDOS DISPONÍVEIS:")
    print("@ - Iniciar configuração automática para todos os dispositivos")
    print("@IMEI - Iniciar configuração automática para IMEI específico")
    print("IMEI:COMANDO - Enviar comando específico para um IMEI")
    print("list - Listar dispositivos conectados")
    print("status - Ver status das filas de comandos")
    print("Exemplo: 0865209077286178:RESET#")
    print("="*60)
    print("Aguardando dispositivos conectarem...")
    print("Digite comandos quando dispositivos estiverem conectados.\n")
    
    while True:
        try:
            cmd = input().strip()
            if not cmd:
                continue
                
            # Comando para listar dispositivos conectados
            if cmd.lower() == "list":
                if client_connections:
                    print("\n--- DISPOSITIVOS CONECTADOS ---")
                    for imei, socket in client_connections.items():
                        status = "Ativo" if socket else "Desconectado"
                        print(f"IMEI: {imei} - Status: {status}")
                    print("--------------------------------\n")
                else:
                    print("Nenhum dispositivo conectado.\n")
                continue
            
            # Comando para ver status das filas
            if cmd.lower() == "status":
                if command_queue:
                    print("\n--- STATUS DAS FILAS ---")
                    for imei, data in command_queue.items():
                        if data.get('manual_mode', False):
                            print(f"IMEI: {imei} [COMANDO MANUAL]")
                            print(f"  Comando: {data.get('last_command', 'N/A')}")
                            print(f"  Aguardando ACK: {'Sim' if data['waiting_ack'] else 'Não'}")
                        else:
                            total = len(data['commands'])
                            atual = data['current_index']
                            aguardando = "Sim" if data['waiting_ack'] else "Não"
                            print(f"IMEI: {imei} [CONFIGURAÇÃO AUTOMÁTICA]")
                            print(f"  Progresso: {atual}/{total}")
                            print(f"  Ultimo comando: {data.get('last_command', 'N/A')}")
                            print(f"  Aguardando ACK: {aguardando}")
                        print()
                else:
                    print("Nenhuma fila de comandos ativa.\n")
                continue
            
            # Configuração automática para todos os dispositivos
            if cmd == "@":
                if client_connections:
                    print("Iniciando configuração automática para todos os dispositivos...")
                    for imei in client_connections.keys():
                        if inicializar_comandos_manual(imei):
                            threading.Timer(1.0, lambda i=imei: enviar_proximo_comando(i)).start()
                    print(f"Configuração iniciada para {len(client_connections)} dispositivos.")
                else:
                    print("Nenhum dispositivo conectado.")
                continue
            
            # Configuração automática para IMEI específico
            if cmd.startswith("@") and len(cmd) > 1:
                imei_target = cmd[1:]
                if imei_target in client_connections:
                    print(f"Iniciando configuração automática para IMEI: {imei_target}")
                    if inicializar_comandos_manual(imei_target):
                        threading.Timer(1.0, lambda: enviar_proximo_comando(imei_target)).start()
                        print("Configuração iniciada.")
                    else:
                        print("Erro ao inicializar comandos.")
                else:
                    print(f"IMEI {imei_target} não está conectado.")
                continue
            
            # Comando específico para IMEI (formato: IMEI:COMANDO)
            if ":" in cmd:
                imei_target, comando = cmd.split(":", 1)
                imei_target = imei_target.strip()
                comando = comando.strip()
                
                print(f"[DEBUG] IMEI: '{imei_target}', Comando: '{comando}'")
                
                if imei_target in client_connections:
                    client_socket = client_connections[imei_target]
                    if client_socket:
                        enviar_comando_manual(imei_target, comando, client_socket)
                    else:
                        print(f"Socket não disponível para IMEI {imei_target}")
                else:
                    print(f"IMEI {imei_target} não encontrado. Use 'list' para ver dispositivos conectados.")
                continue
            
            print("Comando não reconhecido. Digite 'list' para ver dispositivos ou use o formato IMEI:COMANDO")
            
        except KeyboardInterrupt:
            print("\nEncerrando entrada manual de comandos...")
            break
        except Exception as e:
            print(f"Erro no processamento do comando: {e}")

# Envia comando manual para dispositivo específico
def enviar_comando_manual(imei, comando, client_socket):
    """Envia comando manual para um dispositivo específico"""
    try:
        # Verifica se já existe uma fila de comandos aguardando ACK
        if imei in command_queue and command_queue[imei]['waiting_ack']:
            print(f"[AVISO] IMEI {imei} ainda aguarda ACK do comando anterior: {command_queue[imei]['last_command']}")
            resposta = input("Deseja continuar mesmo assim? (s/N): ").lower()
            if resposta != 's':
                return False
        
        # Converte comando para o formato correto do protocolo GT06
        comando_bytes = comando_para_protocolo_gt06(comando)
        
        # print(f"[MANUAL] Enviando para IMEI {imei}: {comando}")
        # print(f"[TX] {comando_bytes.hex().upper()}")
        
        client_socket.sendall(comando_bytes)
        
        # Grava o comando enviado como SACK
        record_combined_message(raw_combined_file, "S", "CMD", comando_bytes.hex().upper())
        
        # Cria entrada na fila para aguardar ACK
        command_queue[imei] = {
            'commands': [comando],  # Lista com apenas este comando
            'current_index': 0,
            'waiting_ack': True,
            'last_command': comando,
            'client_socket': client_socket,
            'last_sent_time': time.time(),
            'manual_mode': True  # Flag para indicar que é comando manual
        }
        
        print(f"[MANUAL] Comando enviado. Aguardando ACK...")
        return True
        
    except Exception as e:
        print(f"[ERRO] Falha ao enviar comando manual: {e}")
        return False

# Inicializa comandos manualmente (sem verificar estado salvo)
def inicializar_comandos_manual(imei):
    """Inicializa fila de comandos manualmente, sempre do início"""
    comandos = carregar_comandos(config_file_commands)
    if not comandos:
        print(f"[AVISO] Nenhum comando encontrado em {config_file_commands}")
        return False
    
    client_socket = client_connections.get(imei)
    if not client_socket:
        print(f"[ERRO] Socket não disponível para IMEI {imei}")
        return False
    
    # Para comandos manuais, sempre começa do início
    command_queue[imei] = {
        'commands': comandos,
        'current_index': 0,
        'waiting_ack': False,
        'last_command': '',
        'client_socket': client_socket,
        'last_sent_time': 0
    }
    
    return True

# Carrega comandos do arquivo txt
def carregar_comandos(arquivo):
    comandos = []
    if os.path.exists(arquivo):
        with open(arquivo, "r", encoding="utf-8") as f:
            for linha in f:
                linha = linha.strip()
                if linha and not linha.startswith("#"):  # Ignora linhas vazias e comentários
                    comandos.append(linha)
    return comandos

# Salva estado dos comandos
def salvar_estado():
    estado = {}
    for imei, data in command_queue.items():
        estado[imei] = {
            'current_index': data['current_index'],
            'last_command': data['last_command'],
            'commands_total': len(data['commands'])
        }
    
    with open(state_file, "w") as f:
        json.dump(estado, f, indent=2)

# Carrega estado dos comandos
def carregar_estado():
    if os.path.exists(state_file):
        try:
            with open(state_file, "r") as f:
                return json.load(f)
        except:
            return {}
    return {}

# Calcula o checksum XOR
def calculate_xor(data_hex):
    data_bytes = bytes.fromhex(data_hex)
    xor = 0
    for b in data_bytes:
        xor ^= b
    return f"{xor:02X}"

# Monta pacote ACK genérico
def create_ack(protocol_number, serial_hex):
    base = "7878"
    length = "05"
    proto = protocol_number.zfill(2)
    serial = serial_hex.zfill(4).upper()
    pre_checksum = length + proto + serial
    checksum = calculate_xor(pre_checksum)
    return bytes.fromhex(base + pre_checksum + checksum + "0D0A")

# Envia próximo comando da fila
def enviar_proximo_comando(imei):
    if imei not in command_queue:
        return False
    
    queue_data = command_queue[imei]
    
    if queue_data['current_index'] >= len(queue_data['commands']):
        print(f"[COMANDOS] Todos os comandos foram enviados para IMEI {imei}")
        # Limpa a fila
        del command_queue[imei]
        salvar_estado()
        return False
    
    comando = queue_data['commands'][queue_data['current_index']]
    client_socket = queue_data['client_socket']
    
    if not client_socket:
        print(f"[ERRO] Socket não disponível para IMEI {imei}")
        return False
    
    try:
        # Converte comando para o formato correto do protocolo GT06
        comando_bytes = comando_para_protocolo_gt06(comando)
        
        print(f"[COMANDO] Enviando para IMEI {imei}: {comando}")
        print(f"[TX] {comando_bytes.hex().upper()}")
        
        client_socket.sendall(comando_bytes)
        
        # Grava o comando enviado como SACK
        record_combined_message(raw_combined_file, "S", "CMD", comando_bytes.hex().upper())
        
        # Atualiza estado
        queue_data['waiting_ack'] = True
        queue_data['last_command'] = comando
        queue_data['last_sent_time'] = time.time()
        
        salvar_estado()
        return True
        
    except Exception as e:
        print(f"[ERRO] Falha ao enviar comando: {e}")
        return False

# Inicializa fila de comandos para um IMEI
def inicializar_comandos(imei, client_socket):
    comandos = carregar_comandos(config_file_commands)
    if not comandos:
        print(f"[AVISO] Nenhum comando encontrado em {config_file_commands}")
        return False
    
    # Verifica se há estado salvo
    estado_salvo = carregar_estado()
    index_inicial = 0
    
    if imei in estado_salvo:
        index_inicial = estado_salvo[imei].get('current_index', 0)
        print(f"[RECUPERAÇÃO] Continuando do comando {index_inicial + 1} para IMEI {imei}")
    
    command_queue[imei] = {
        'commands': comandos,
        'current_index': index_inicial,
        'waiting_ack': False,
        'last_command': '',
        'client_socket': client_socket,
        'last_sent_time': 0
    }
    
    print(f"[COMANDOS] Inicializada fila com {len(comandos)} comandos para IMEI {imei}")
    return True

# Verifica se a resposta contém confirmação do comando
def verificar_resposta_comando(hex_data, imei):
    """Verifica se o pacote recebido é uma resposta ao comando enviado"""
    if imei not in command_queue:
        return False
    
    queue_data = command_queue[imei]
    if not queue_data['waiting_ack']:
        return False
    
    protocol_number = hex_data[6:8]
    
    # Protocolo 15 é a resposta padrão para comandos no GT06
    if protocol_number == "15":
        # print(f"[RESPOSTA] Protocolo 15 detectado - resposta de comando")
        return True
    
    # Alguns dispositivos podem usar protocolo 80 para responder
    if protocol_number == "80":
        print(f"[RESPOSTA] Protocolo 80 detectado - resposta de comando")
        return True
    
    # Protocolo 05 também pode ser usado para confirmação
    if protocol_number == "05":
        print(f"[RESPOSTA] Protocolo 05 detectado - confirmação simples")
        return True
    
    return False

# Processa ACK recebido
def processar_ack_comando(imei):
    if imei not in command_queue:
        return
    
    queue_data = command_queue[imei]
    
    if queue_data['waiting_ack']:
        print(f"[ACK] Confirmação recebida para: {queue_data['last_command']}")
        
        # Se é comando manual, apenas limpa a fila
        if queue_data.get('manual_mode', False):
            del command_queue[imei]
            print(f"[MANUAL] Comando concluído com sucesso!")
            return
        
        # Se é configuração automática, avança para próximo comando
        queue_data['current_index'] += 1
        queue_data['waiting_ack'] = False
        
        # Salva estado
        salvar_estado()
        
        # Aguarda um pouco antes do próximo comando
        threading.Timer(2.0, lambda: enviar_proximo_comando(imei)).start()

# Verifica timeout de comandos
def verificar_timeout_comandos():
    while True:
        time.sleep(5)  # Verifica a cada 5 segundos
        
        for imei, queue_data in list(command_queue.items()):
            if queue_data['waiting_ack']:
                tempo_espera = time.time() - queue_data['last_sent_time']
                
                if tempo_espera > 15:  # Timeout reduzido para 15 segundos
                    print(f"[TIMEOUT] Comando não confirmado para IMEI {imei}: {queue_data['last_command']}")
                    
                    # Reenviar comando ou pular para próximo
                    print(f"[TIMEOUT] Reenviando comando...")
                    queue_data['waiting_ack'] = False
                    enviar_proximo_comando(imei)

# Encontra IP da interface VPN (Wintun, TAP, tun)
def get_vpn_ip():
    adapters = ifaddr.get_adapters()
    for adapter in adapters:
        if any(term in adapter.nice_name.lower() for term in ["wintun", "tap", "tun"]):
            for ip in adapter.ips:
                if isinstance(ip.ip, str) and not ip.ip.startswith("127."):
                    return ip.ip
    return None

# Lida com conexão do cliente
def handle_client(client_socket, address):
    print(f"[{datetime.now()}] Conexão de {address}")
    imei = None
    
    try:
        while True:
            data = client_socket.recv(1024)
            if not data:
                break
            hex_data = data.hex().upper()
            print(f"[RX] {hex_data}")
            
            # Grava a mensagem RAW recebida
            record_combined_message(raw_combined_file, "D", "RAW", hex_data)

            if hex_data.startswith("7878") and hex_data.endswith("0D0A"):
                protocol_number = hex_data[6:8]
                data_len = int(hex_data[4:6], 16)

                # Serial está sempre nos últimos 4 caracteres antes do checksum
                serial_start = 6 + (data_len - 5) * 2  # pula protocolo + dados
                serial_number = hex_data[serial_start:serial_start + 4]

                # Verifica se é resposta a comando enviado
                if imei and verificar_resposta_comando(hex_data, imei):
                    processar_ack_comando(imei)
                
                # Montar ACK baseado no protocolo
                if protocol_number in ["01", "13", "16", "32", "15", "80"]:  # login, hbd, gps, respostas
                    ack = create_ack(protocol_number, serial_number)
                    print(f"[SACK] Enviando ACK para fd {protocol_number}")
                    client_socket.sendall(ack)
                    
                    # Grava a resposta SACK enviada
                    record_combined_message(raw_combined_file, "S", "SACK", ack.hex().upper())
                    
                    if protocol_number == "01":  # Login
                        imei = hex_data[8:24] 
                        imei_cache[address] = imei
                        client_connections[imei] = client_socket  # Registra conexão
                        print(f"[LOGIN] IMEI registrado: {imei} - Socket disponível para comandos")
                        if imei in command_queue:
                            print(f"[RECONEXÃO] Dispositivo {imei} reconectado. Retomando envio de comandos...")
                            command_queue[imei]['client_socket'] = client_socket
                            queue_data = command_queue[imei]
                            # Reenviar o comando pendente se ainda esperando ACK
                            if queue_data['waiting_ack']:
                                threading.Timer(2.0, lambda: enviar_proximo_comando(imei)).start()
                            else:
                                # Se não estava esperando ACK, avança normalmente
                                threading.Timer(2.0, lambda: enviar_proximo_comando(imei)).start()
                
                else:
                    print(f"[!] Protocolo {protocol_number} não tratado - enviando ACK genérico")
                    ack = create_ack(protocol_number, serial_number)
                    client_socket.sendall(ack)
                    # Grava a resposta SACK enviada
                    record_combined_message(raw_combined_file, "S", "SACK", ack.hex().upper())
               
                # Parser de mensagens
                if not imei:
                    imei = imei_cache.get(address)
                
                if imei:
                    msg_to_send = parser_gt06V4(hex_data, imei)
                    
                    # Salva dados decodificados no arquivo específico do IMEI
                    if msg_to_send:  
                        record_decoded_organized(imei, msg_to_send)
                        print(f"[LOG] Dados salvos para IMEI {imei}: logs/{imei}_decoded.csv")

    except Exception as e:
        print(f"[ERRO] {e}")
    finally:
        # Limpa referência do socket
        if imei:
            if imei in command_queue:
                command_queue[imei]['client_socket'] = None
            if imei in client_connections:
                del client_connections[imei]
        
        client_socket.close()
        print(f"[{datetime.now()}] Desconectado: {address}")

# Inicia o servidor
def start_server(ip, port):
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    
    try:
        server.bind((ip, port))
        server.listen(5)
        print(f"[OK] Servidor iniciado em {ip}:{port}")
        print(f"[INFO] Arquivo de comandos: {config_file_commands}")
        print(f"[INFO] Arquivo de estado: {state_file}")
        print(f"[INFO] Logs serão salvos em: logs/IMEI_decoded.csv")
        print("[MODO] Configurações serão enviadas via comando manual (@) com protocolo GT06 corrigido")
    except:
        print("[ERRO] Não foi possível iniciar o servidor. Verifique IP e porta.")
        return

    # Inicia thread de verificação de timeout
    timeout_thread = threading.Thread(target=verificar_timeout_comandos, daemon=True)
    timeout_thread.start()
    
    # Inicia thread para comandos manuais
    comandos_thread = threading.Thread(target=worker_comandos_manuais, daemon=True)
    comandos_thread.start()

    while True:
        client_sock, addr = server.accept()
        thread = threading.Thread(target=handle_client, args=(client_sock, addr))
        thread.start()

def main():
    server_ip = get_vpn_ip()
    server_port =9117
    if not server_ip:
        print("[ERRO] VPN (Wintun/TAP/tun) não detectada. Conecte a VPN e tente novamente.")
        exit()

    print(f"[VPN Detectada] IP: {server_ip}")
    start_server(ip=server_ip, port=server_port)

if __name__ == "__main__":
    main()