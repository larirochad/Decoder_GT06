import socket
import threading
import os
from datetime import datetime
import ifaddr  
from decoder_gt06V4 import *

imei_cache = {} 
active_connections = {}  # Dicionário para armazenar conexões ativas

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

# Encontra IP da interface VPN (Wintun, TAP, tun)
def get_vpn_ip():
    adapters = ifaddr.get_adapters()
    for adapter in adapters:
        if any(term in adapter.nice_name.lower() for term in ["wintun", "tap", "tun"]):
            for ip in adapter.ips:
                if isinstance(ip.ip, str) and not ip.ip.startswith("127."):
                    return ip.ip
    return None

# Thread para enviar comandos manualmente
def command_sender():
    while True:
        cmd = input("Digite o comando (ou 'list' para ver conexões): ")
        
        if cmd.lower() == 'list':
            print("\nConexões ativas:")
            for addr, conn_info in active_connections.items():
                print(f"{addr} - IMEI: {conn_info.get('imei', 'Desconhecido')}")
            continue
        
        if not active_connections:
            print("Nenhum dispositivo conectado.")
            continue
            
        # Se não especificado, envia para o primeiro dispositivo
        target_addr = next(iter(active_connections))
        client_socket = active_connections[target_addr]['socket']
        
        try:
            if cmd.startswith("7878") and cmd.endswith("0D0A"):
                # Comando em formato hexadecimal
                client_socket.sendall(bytes.fromhex(cmd))
            else:
                # Comando em formato ASCII
                client_socket.sendall(cmd.encode())
                
            print(f"[TX] Comando enviado para {target_addr}")
        except Exception as e:
            print(f"[ERRO] Falha ao enviar comando: {e}")

# Lida com conexão do cliente
def handle_client(client_socket, address):
    print(f"[{datetime.now()}] Conexão de {address}")
    active_connections[address] = {'socket': client_socket}
    
    try:
        while True:
            data = client_socket.recv(1024)
            if not data:
                break
                
            hex_data = data.hex().upper()
            print(f"[RX] {hex_data}")

            if hex_data.startswith("7878") and hex_data.endswith("0D0A"):
                protocol_number = hex_data[6:8]
                data_len = int(hex_data[4:6], 16)

                # Serial está sempre nos últimos 4 caracteres antes do checksum
                serial_start = 6 + (data_len - 5) * 2  # pula protocolo + dados
                serial_number = hex_data[serial_start:serial_start + 4]

                # Montar ACK baseado no protocolo
                if protocol_number in ["01", "13", "16", "32", "15"]:  # login, hbd, gps
                    ack = create_ack(protocol_number, serial_number)
                    client_socket.sendall(ack)
                    print(f"[TX] ACK enviado: {ack.hex().upper()}")
                    
                    if protocol_number == "01":
                        imei = hex_data[8:24] 
                        imei_cache[address] = imei
                        active_connections[address]['imei'] = imei
                else:
                    print(f"[!] Protocolo {protocol_number} não tratado (ainda)")
               
                imei = imei_cache.get(address)
                msg_to_send = parser_gt06V4(hex_data, imei)

    except Exception as e:
        print(f"[ERRO] {e}")
    finally:
        client_socket.close()
        if address in active_connections:
            del active_connections[address]
        print(f"[{datetime.now()}] Desconectado: {address}")

# Inicia o servidor
def start_server(ip, port):
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        server.bind((ip, port))
        server.listen(5)
        print(f"[OK] Servidor iniciado em {ip}:{port}")
        
        # Inicia thread para enviar comandos
        threading.Thread(target=command_sender, daemon=True).start()
    except Exception as e:
        print(f"[ERRO] Não foi possível iniciar o servidor: {e}")
        return

    while True:
        try:
            client_sock, addr = server.accept()
            thread = threading.Thread(target=handle_client, args=(client_sock, addr))
            thread.start()
        except KeyboardInterrupt:
            print("\nEncerrando servidor...")
            break
        except Exception as e:
            print(f"[ERRO] Erro ao aceitar conexão: {e}")

def main():
    config_file = "config.txt"
    config = carregar_configuracao(config_file)

    server_ip = get_vpn_ip()
    if not server_ip:
        print("[ERRO] VPN (Wintun/TAP/tun) não detectada. Conecte a VPN e tente novamente.")
        exit()

    print(f"[VPN Detectada] IP: {server_ip}")

    server_port = int(config.get("server_port", 9117))

    start_server(ip=server_ip, port=server_port)

if __name__ == "__main__":
    main()