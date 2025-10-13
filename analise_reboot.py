import pandas as pd
from datetime import datetime, timedelta
import numpy as np

def adicionar_diffs(input_file, output_file, heartbeat_interval=240):
    """
    Analisa logs do dispositivo J16 (Protocolo GT06) para identificar reboots
    e classificar seus possíveis motivos baseado nos intervalos de tempo.
    
    Args:
        input_file: Arquivo CSV com os logs parseados
        output_file: Arquivo CSV de saída com análises
        heartbeat_interval: Intervalo configurado para heartbeat em segundos (padrão: 240s)
    """
    
    # Carregar dados
    df = pd.read_csv(input_file, sep=",")
    df.columns = df.columns.str.strip()
    
    # Verificar se as colunas necessárias existem
    required_columns = ['Sequência', 'Tipo Mensagem', 'Data/Hora Evento']
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        print(f"Erro: Colunas não encontradas: {missing_columns}")
        print(f"Colunas disponíveis: {list(df.columns)}")
        return
    
    # Converter coluna de data/hora para datetime
    df['Data/Hora Evento'] = pd.to_datetime(df['Data/Hora Evento'], errors='coerce')
    
    # Ordenar por data/hora
    df = df.sort_values('Data/Hora Evento').reset_index(drop=True)
    
    # Inicializar colunas de análise
    df['Reboot_Detectado'] = False
    df['Motivo_Reboot'] = ''
    df['Intervalo_Desde_Ultima'] = np.nan
    df['Serial_Reset'] = False
    df['Observacoes'] = ''
    
    # Detectar reboots baseado no Serial Number (Sequência)
    reboots_detectados = []
    
    for i in range(1, len(df)):
        current_serial = df.loc[i, 'Sequência']
        prev_serial = df.loc[i-1, 'Sequência']
        current_msg_type = df.loc[i, 'Tipo Mensagem']
        current_time = df.loc[i, 'Data/Hora Evento']
        prev_time = df.loc[i-1, 'Data/Hora Evento']
        
        # Detectar reset do serial number
        # Condições: serial atual é muito menor que o anterior OU é 0 ou 1 após sequência alta
        serial_reset = False
        
        if pd.notna(current_serial) and pd.notna(prev_serial):
            # Reset detectado se:
            # 1. Serial atual é 0 ou 1 após uma sequência maior
            # 2. Serial atual é significativamente menor que o anterior (diferença > 10)
            if (current_serial in [0, 1] and prev_serial > 10) or \
               (prev_serial - current_serial > 10 and current_serial < 5):
                serial_reset = True
                df.loc[i, 'Serial_Reset'] = True
                
                # Verificar se é mensagem de Login (comportamento esperado após reboot)
                if 'Login' in str(current_msg_type):
                    df.loc[i, 'Reboot_Detectado'] = True
                    
                    # Calcular intervalo desde a última mensagem com timestamp válido
                    if pd.notna(current_time) and pd.notna(prev_time):
                        intervalo = (current_time - prev_time).total_seconds() / 60  # em minutos
                        df.loc[i, 'Intervalo_Desde_Ultima'] = intervalo
                        
                        # Classificar motivo do reboot baseado no intervalo
                        motivo, observacao = classificar_motivo_reboot(
                            intervalo, heartbeat_interval, current_msg_type, i, df
                        )
                        df.loc[i, 'Motivo_Reboot'] = motivo
                        df.loc[i, 'Observacoes'] = observacao
                        
                        reboots_detectados.append({
                            'indice': i,
                            'data_hora': current_time,
                            'intervalo_minutos': intervalo,
                            'motivo': motivo,
                            'serial_anterior': prev_serial,
                            'serial_atual': current_serial
                        })
    
    # Calcular intervalos entre mensagens consecutivas para análise geral
    df['Intervalo_Entre_Mensagens'] = np.nan
    for i in range(1, len(df)):
        if pd.notna(df.loc[i, 'Data/Hora Evento']) and pd.notna(df.loc[i-1, 'Data/Hora Evento']):
            intervalo = (df.loc[i, 'Data/Hora Evento'] - df.loc[i-1, 'Data/Hora Evento']).total_seconds()
            df.loc[i, 'Intervalo_Entre_Mensagens'] = intervalo
    
    # Salvar arquivo processado
    df.to_csv(output_file, index=False)
    
    # Gerar relatório de reboots
    gerar_relatorio_reboots(reboots_detectados, heartbeat_interval)
    
    return df

def classificar_motivo_reboot(intervalo_minutos, heartbeat_interval, msg_type, indice, df):
    """
    Classifica o motivo do reboot baseado no intervalo de tempo e contexto.
    
    Returns:
        tuple: (motivo, observacao)
    """
    
    # Converter heartbeat_interval para minutos
    heartbeat_minutes = heartbeat_interval / 60
    
    # Cenário A: Reboot por Falha de Conexão GPRS (20 minutos)
    if 18 <= intervalo_minutos <= 25:
        return ("Falha de Conexão GPRS", 
                f"Intervalo de {intervalo_minutos:.1f} min sugere reboot automático por falha de GPRS (esperado: ~20 min)")
    
    # Cenário B: Reboot por Falha de ACK (10 minutos)
    elif 8 <= intervalo_minutos <= 15:
        return ("Falha de ACK do Servidor", 
                f"Intervalo de {intervalo_minutos:.1f} min sugere reboot por falha de ACK (esperado: ~10 min)")
    
    # Cenário C: Reboot por Comando Remoto (muito rápido)
    elif intervalo_minutos <= 5:
        return ("Comando Remoto (RESET#)", 
                f"Intervalo muito curto ({intervalo_minutos:.1f} min) sugere reboot por comando remoto")
    
    # Cenário D: Desligamento/Ligamento do Veículo ou Problema de Energia
    elif intervalo_minutos > 30:
        return ("Desligamento do Veículo/Energia", 
                f"Intervalo longo ({intervalo_minutos:.1f} min) sugere desligamento do veículo ou problema de energia")
    
    # Intervalo intermediário - incerto
    else:
        return ("Motivo Incerto", 
                f"Intervalo de {intervalo_minutos:.1f} min não corresponde aos padrões conhecidos")

def gerar_relatorio_reboots(reboots, heartbeat_interval):
    """
    Gera um relatório resumido dos reboots detectados.
    """
    print("\n" + "="*80)
    print("RELATÓRIO DE ANÁLISE DE REBOOTS - DISPOSITIVO J16 (Protocolo GT06)")
    print("="*80)
    
    if not reboots:
        print("✅ Nenhum reboot detectado nos logs analisados.")
        return
    
    print(f"📊 Total de reboots detectados: {len(reboots)}")
    print(f"⏱️  Intervalo de heartbeat configurado: {heartbeat_interval}s ({heartbeat_interval/60:.1f} min)")
    print("\n" + "-"*80)
    
    # Agrupar por motivo
    motivos = {}
    for reboot in reboots:
        motivo = reboot['motivo']
        if motivo not in motivos:
            motivos[motivo] = []
        motivos[motivo].append(reboot)
    
    # Exibir estatísticas por motivo
    for motivo, eventos in motivos.items():
        print(f"\n🔍 {motivo}: {len(eventos)} ocorrência(s)")
        for evento in eventos:
            print(f"   📅 {evento['data_hora'].strftime('%Y-%m-%d %H:%M:%S')} "
                  f"(intervalo: {evento['intervalo_minutos']:.1f} min)")
    
    print("\n" + "-"*80)
    print("LEGENDA DOS MOTIVOS:")
    print("• Falha de Conexão GPRS: Dispositivo não conseguiu conectar por 3 tentativas (~20 min)")
    print("• Falha de ACK do Servidor: Dispositivo não recebeu resposta por 3 tentativas (~10 min)")
    print("• Comando Remoto (RESET#): Reboot forçado via comando da plataforma")
    print("• Desligamento do Veículo/Energia: Veículo desligado ou problema de alimentação")
    print("• Motivo Incerto: Intervalo não corresponde aos padrões conhecidos")
    print("="*80)

def analisar_padrao_heartbeat(df, heartbeat_interval=240):
    """
    Analisa se o padrão de heartbeat está sendo respeitado.
    """
    heartbeats = df[df['Tipo Mensagem'].str.contains('Heartbeat', na=False)].copy()
    
    if len(heartbeats) < 2:
        print("⚠️  Poucos heartbeats encontrados para análise de padrão.")
        return
    
    heartbeats = heartbeats.sort_values('Data/Hora Evento').reset_index()
    intervals = []
    
    for i in range(1, len(heartbeats)):
        if pd.notna(heartbeats.loc[i, 'Data/Hora Evento']) and pd.notna(heartbeats.loc[i-1, 'Data/Hora Evento']):
            interval = (heartbeats.loc[i, 'Data/Hora Evento'] - heartbeats.loc[i-1, 'Data/Hora Evento']).total_seconds()
            intervals.append(interval)
    
    if intervals:
        avg_interval = np.mean(intervals)
        print(f"\n📈 ANÁLISE DE PADRÃO DE HEARTBEAT:")
        print(f"   Intervalo configurado: {heartbeat_interval}s")
        print(f"   Intervalo médio observado: {avg_interval:.1f}s")
        print(f"   Diferença: {abs(avg_interval - heartbeat_interval):.1f}s")
        
        if abs(avg_interval - heartbeat_interval) > 30:
            print("   ⚠️  Diferença significativa detectada!")
        else:
            print("   ✅ Padrão de heartbeat dentro do esperado.")

if __name__ == "__main__":
    input_file = "logs/0865209077286707_decoded.csv"
    output_file = "0865209077286707_decoded_analyzed.csv"
    
    # Configurar intervalo de heartbeat (padrão: 240s)
    heartbeat_interval = 240
    
    print("🔍 Iniciando análise de logs do dispositivo J16...")
    print(f"📁 Arquivo de entrada: {input_file}")
    print(f"💾 Arquivo de saída: {output_file}")
    print(f"⏱️  Intervalo de heartbeat configurado: {heartbeat_interval}s")
    
    try:
        df = adicionar_diffs(input_file, output_file, heartbeat_interval)
        
        if df is not None:
            # Análise adicional do padrão de heartbeat
            analisar_padrao_heartbeat(df, heartbeat_interval)
            
            print(f"\n✅ Arquivo processado e salvo como {output_file}")
            print(f"📊 Total de registros analisados: {len(df)}")
            
    except FileNotFoundError:
        print(f"❌ Erro: Arquivo '{input_file}' não encontrado.")
    except Exception as e:
        print(f"❌ Erro durante o processamento: {str(e)}")