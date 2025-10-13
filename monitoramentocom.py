import serial
import time
import csv
from datetime import datetime
import os

class SerialRebootMonitor:
    def __init__(self, port='COM9', baudrate=115200, timeout=1, csv_filename='reboot_monitor.csv'):
        self.port = port
        self.baudrate = baudrate
        self.timeout = timeout
        self.csv_filename = csv_filename
        self.setup_csv()
        
    def setup_csv(self):
        """Configura o arquivo CSV se não existir"""
        if not os.path.exists(self.csv_filename):
            with open(self.csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(['Timestamp', 'Event', 'Port', 'Details'])
                
    def log_event(self, event, details=''):
        """Registra um evento no CSV"""
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        
        with open(self.csv_filename, 'a', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow([timestamp, event, self.port, details])
        
        print(f"[{timestamp}] {event} - {details}")

    def wait_for_port(self):
        """Aguarda a porta ficar disponível"""
        print(f"Aguardando porta {self.port} ficar disponível...")
        while True:
            try:
                # Tenta abrir a porta para testar se existe
                with serial.Serial(self.port, self.baudrate, timeout=0.1) as test_ser:
                    self.log_event('PORT_AVAILABLE', f'Porta {self.port} disponível\n')
                    return True
            except serial.SerialException:
                print(".", end="", flush=True)  # Mostra progresso
                time.sleep(1)
            except KeyboardInterrupt:
                print("\nMonitoramento cancelado pelo usuário.")
                return False

    def monitor_serial(self):
        """Monitor principal com reconexão automática"""
        self.log_event('MONITOR_START', f'Iniciando monitoramento da porta {self.port}')
        
        while True:
            try:
                # Aguarda porta estar disponível
                if not self.wait_for_port():
                    break
                    
                # Conecta à porta
                with serial.Serial(self.port, self.baudrate, timeout=self.timeout) as ser:
                    self.log_event('CONNECTED', f'Conectado a {self.port} ({self.baudrate}bps)')
                    print(f"🔗 Monitorando {self.port} - Aguardando dados...\n")
                    
                    last_line = ""
                    connection_start = time.time()
                    
                    while True:
                        try:
                            line = ser.readline().decode(errors='ignore').strip()
                            
                            if line:
                                # Exibe a linha recebida
                                print(f"📨 {line}")
                                
                                # Detecta reboot por palavras-chave
                                reboot_keywords = ["BOOT", "RESET", "HEART", "IMEI", "START", "INIT", "READY"]
                                if any(keyword in line.upper() for keyword in reboot_keywords):
                                    duration = time.time() - connection_start
                                    self.log_event('REBOOT_DETECTED', f'Reboot por palavra-chave: "{line}" (conectado {duration:.1f}s)')
                                    print("🔄 >>> REBOOT DETECTADO POR MENSAGEM!")
                                
                                # Detecta reinício por linha repetida
                                if last_line and line == last_line and len(line) > 10:
                                    self.log_event('REBOOT_SUSPECTED', f'Possível reboot - linha repetida: "{line}"')
                                    print("🔄 >>> Possível reboot (linha repetida)")
                                    
                                last_line = line
                                
                        except serial.SerialException as e:
                            # Erro de leitura - provavelmente desconectou
                            duration = time.time() - connection_start
                            self.log_event('Reboot')
                            print(f"Reboot detectado após {duration:.1f} segundos de conexão.\n")
                            break  # Sai do loop interno para reconectar
                            
            except serial.SerialException as e:
                # Erro de conexão - dispositivo desconectou
                if "GetOverlappedResult failed" in str(e) or "dispositivo conectado" in str(e):
                    self.log_event('DEVICE_DISCONNECTED', f'Dispositivo desconectado: {str(e)}')
                    print("🔌 >>> DISPOSITIVO DESCONECTADO - Aguardando reconexão...")
                else:
                    self.log_event('CONNECTION_ERROR', f'Erro de conexão: {str(e)}')
                    print(f"❌ Erro ao acessar porta: {e}")
                
                print("⏳ Tentando reconectar em 2 segundos...")
                time.sleep(2)
                
            except KeyboardInterrupt:
                self.log_event('MONITOR_STOP', 'Monitoramento finalizado pelo usuário')
                print("\n👋 Monitoramento finalizado pelo usuário.")
                break
                
            except Exception as e:
                self.log_event('UNKNOWN_ERROR', f'Erro inesperado: {str(e)}')
                print(f"💥 Erro inesperado: {e}")
                time.sleep(5)

def main():
    # Configurações
    PORT = 'COM9'
    BAUDRATE = 115200
    TIMEOUT = 1
    CSV_FILE = 'com9_reboot_monitor.csv'
    
    print("=== Monitor de Reboot via Serial ===")
    print(f"Porta: {PORT}")
    print(f"Baudrate: {BAUDRATE}")
    print(f"Arquivo CSV: {CSV_FILE}")
    print("Pressione Ctrl+C para parar\n")
    
    # Cria e inicia o monitor
    monitor = SerialRebootMonitor(PORT, BAUDRATE, TIMEOUT, CSV_FILE)
    monitor.monitor_serial()

if __name__ == "__main__":
    main()