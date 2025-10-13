import pandas as pd
import os
from typing import Dict, List, Tuple, Optional
from datetime import timedelta

def format_timedelta(td):
    """Formata timedelta para HH:MM:SS"""
    if pd.isna(td):
        return None
    total_seconds = int(td.total_seconds())
    horas, resto = divmod(total_seconds, 3600)
    minutos, segundos = divmod(resto, 60)
    return f"{horas:02}:{minutos:02}:{segundos:02}"

def calcular_distancia_hodometro(df: pd.DataFrame) -> Dict:
    """Calcula a distância percorrida baseada no hodômetro"""
    resultado = {
        'primeiro_km': None,
        'ultimo_km': None,
        'distancia_percorrida': None,
        'data_primeiro': None,
        'data_ultimo': None,
        'total_registros_validos': 0
    }
    
    # Verificar se a coluna existe
    if 'Hodômetro Total' not in df.columns:
        print("⚠️ Coluna 'Hodômetro Total' não encontrada!")
        return resultado
    
    # Converter para numérico, forçando valores inválidos para NaN
    df_work = df.copy()
    df_work['Hodômetro Total'] = pd.to_numeric(df_work['Hodômetro Total'], errors='coerce')
    
    # Filtrar apenas registros com hodômetro válido (não NaN e > 0)
    registros_validos = df_work.dropna(subset=['Hodômetro Total'])
    registros_validos = registros_validos[registros_validos['Hodômetro Total'] > 0]
    
    if len(registros_validos) == 0:
        print("⚠️ Nenhum registro válido de hodômetro encontrado!")
        return resultado
    
    # Ordenar por data/hora para pegar primeiro e último cronologicamente
    registros_validos = registros_validos.sort_values('Data/Hora Evento')
    
    primeiro_registro = registros_validos.iloc[0]
    ultimo_registro = registros_validos.iloc[-1]
    
    resultado['primeiro_km'] = primeiro_registro['Hodômetro Total']
    resultado['ultimo_km'] = ultimo_registro['Hodômetro Total']
    resultado['data_primeiro'] = primeiro_registro['Data/Hora Evento']
    resultado['data_ultimo'] = ultimo_registro['Data/Hora Evento']
    resultado['total_registros_validos'] = len(registros_validos)
    
    # Calcular distância percorrida
    if resultado['ultimo_km'] >= resultado['primeiro_km']:
        resultado['distancia_percorrida'] = resultado['ultimo_km'] - resultado['primeiro_km']
    else:
        # Caso especial: hodômetro pode ter sido zerado
        resultado['distancia_percorrida'] = resultado['ultimo_km']
        print(f"⚠️ Possível reset do hodômetro detectado (último < primeiro)")
    
    return resultado

def contar_viagens(df: pd.DataFrame) -> Dict:
    """Conta viagens baseadas nos eventos IGN→IGF"""
    resultado = {
        'total_viagens_completas': 0,
        'igf_sem_ign': 0,
        'ign_sem_igf': 0,
        'detalhes_viagens': [],
        'igf_orfaos': [],
        'ign_orfaos': []
    }
    
    # Filtrar apenas eventos de ignição e remover duplicatas
    df_ignicao = df[df['Tipo Mensagem'].isin(['IGN', 'IGF'])].copy()
    
    # Converter datas para datetime
    df_ignicao["Data/Hora Evento"] = pd.to_datetime(df_ignicao["Data/Hora Evento"], errors="coerce")
    
    df_ignicao = remover_mensagens_duplicadas(df_ignicao)
    df_ignicao = df_ignicao.sort_values('Data/Hora Evento').reset_index(drop=True)
    
    if len(df_ignicao) == 0:
        print("⚠️ Nenhum evento de ignição encontrado!")
        return resultado
    
    print(f"🔍 Analisando {len(df_ignicao)} eventos de ignição...")
    
    i = 0
    while i < len(df_ignicao):
        evento_atual = df_ignicao.iloc[i]
        
        if evento_atual['Tipo Mensagem'] == 'IGN':
            # Procurar o próximo IGF
            igf_encontrado = False
            j = i + 1
            
            while j < len(df_ignicao):
                proximo_evento = df_ignicao.iloc[j]
                
                if proximo_evento['Tipo Mensagem'] == 'IGF':
                    # Viagem completa encontrada: IGN → IGF
                    resultado['total_viagens_completas'] += 1
                    
                    # Calcular duração da viagem
                    duracao = proximo_evento['Data/Hora Evento'] - evento_atual['Data/Hora Evento']
                    
                    resultado['detalhes_viagens'].append({
                        'viagem_numero': resultado['total_viagens_completas'],
                        'ignicao_ligada': evento_atual['Data/Hora Evento'],
                        'ignicao_desligada': proximo_evento['Data/Hora Evento'],
                        'duracao': duracao,
                        'duracao_formatada': format_timedelta(duracao),
                        'sequencia_ign': evento_atual['Sequência'],
                        'sequencia_igf': proximo_evento['Sequência']
                    })
                    
                    igf_encontrado = True
                    i = j + 1  # Continuar após o IGF encontrado
                    break
                    
                elif proximo_evento['Tipo Mensagem'] == 'IGN':
                    # Encontrou outro IGN antes do IGF - IGN órfão
                    break
                    
                j += 1
            
            if not igf_encontrado:
                # IGN sem IGF correspondente
                resultado['ign_sem_igf'] += 1
                resultado['ign_orfaos'].append({
                    'data_hora': evento_atual['Data/Hora Evento'],
                    'sequencia': evento_atual['Sequência'],
                    'status': 'IGN sem IGF correspondente'
                })
                i += 1
        
        elif evento_atual['Tipo Mensagem'] == 'IGF':
            # IGF sem IGN anterior - IGF órfão
            resultado['igf_sem_ign'] += 1
            resultado['igf_orfaos'].append({
                'data_hora': evento_atual['Data/Hora Evento'],
                'sequencia': evento_atual['Sequência'],
                'status': 'IGF sem IGN anterior'
            })
            i += 1
        
        else:
            i += 1
    
    return resultado

def adicionar_diffs(df: pd.DataFrame) -> pd.DataFrame:
    """Adiciona colunas de diferença de tempo para posicionamento e modo econômico, além da coluna LOG"""
    df_work = df.copy()
    
    # Converte Data/Hora Inclusão e Data/Hora Evento
    df_work["Data/Hora Inclusão"] = pd.to_datetime(df_work["Data/Hora Inclusão"], errors="coerce")
    df_work["Data/Hora Evento"] = pd.to_datetime(df_work["Data/Hora Evento"], errors="coerce")

    # Ordena por Data/Hora Inclusão e depois pela Sequência
    df_work.sort_values(by=["Data/Hora Inclusão", "Sequência"], inplace=True, ignore_index=True)

    # Inicializa colunas de diferença
    df_work["LOG"] = None
    df_work["Diff_Posicionamento"] = None
    df_work["Diff_ModoEco"] = None

    # Calcular coluna LOG para todas as linhas que têm Data/Hora Evento válida
    for i, row in df_work.iterrows():
        inclusao_time = row["Data/Hora Inclusão"]
        evento_time = row["Data/Hora Evento"]
        
        # Se ambas as datas são válidas, calcular a diferença
        if pd.notna(inclusao_time) and pd.notna(evento_time):
            diff_log = inclusao_time - evento_time
            df_work.at[i, "LOG"] = format_timedelta(diff_log)

    last_pos_time = None
    last_eco_time = None

    # Percorre linha a linha para calcular diferenças de posicionamento e modo eco
    for i, row in df_work.iterrows():
        tipo = row["Tipo Mensagem"]
        evento_time = row["Data/Hora Evento"]

        # Reset para posicionamento
        if tipo == "IGN":
            last_pos_time = evento_time

        # Reset para modo eco
        if tipo == "IGF":
            last_eco_time = evento_time

        if tipo == "Posicionamento por tempo em movimento":
            if last_pos_time is None:
                df_work.at[i, "Diff_Posicionamento"] = None
            else:
                diff = evento_time - last_pos_time
                df_work.at[i, "Diff_Posicionamento"] = format_timedelta(diff)
            last_pos_time = evento_time

        if tipo == "Modo econômico":
            if last_eco_time is None:
                df_work.at[i, "Diff_ModoEco"] = None
            else:
                diff = evento_time - last_eco_time
                df_work.at[i, "Diff_ModoEco"] = format_timedelta(diff)
            last_eco_time = evento_time

    # 🔀 Reordenar colunas para colocar LOG na 3ª posição e os "Diff_" logo após "Tipo Mensagem"
    cols = list(df_work.columns)
    
    # Remover as colunas que queremos reposicionar
    for c in ["LOG", "Diff_Posicionamento", "Diff_ModoEco"]:
        if c in cols:
            cols.remove(c)
    
    # Inserir LOG na 3ª posição (índice 2)
    cols.insert(2, "LOG")
    
    # Inserir Diff_ logo após "Tipo Mensagem"
    if "Tipo Mensagem" in cols:
        idx = cols.index("Tipo Mensagem")
        cols[idx+1:idx+1] = ["Diff_Posicionamento", "Diff_ModoEco"]
    
    df_work = df_work[cols]

    return df_work


def remover_mensagens_duplicadas(df: pd.DataFrame) -> pd.DataFrame:
    """Remove mensagens duplicadas baseadas em Tipo Mensagem, Sequência e Data/Hora Evento"""
    df_clean = df.copy()
    
    # Contar duplicatas antes da limpeza
    duplicatas = df_clean.duplicated(subset=['Tipo Mensagem', 'Sequência', 'Data/Hora Evento'], keep='first')
    total_duplicatas = duplicatas.sum()
    
    if total_duplicatas > 0:
        print(f"🧹 Removendo {total_duplicatas} mensagens duplicadas...")
    
    # Remover duplicatas mantendo a primeira ocorrência
    df_clean = df_clean.drop_duplicates(subset=['Tipo Mensagem', 'Sequência', 'Data/Hora Evento'], keep='first')
    
    return df_clean

def contar_reboots(df: pd.DataFrame) -> Tuple[int, List[Dict]]:
    """Conta quantas vezes houve reboot (sequência zerada)"""
    reboots = []
    reboot_count = 0
    
    df_sorted = df.sort_values(['Data/Hora Inclusão', 'Sequência']).reset_index(drop=True)
    
    for i in range(1, len(df_sorted)):
        seq_anterior = df_sorted.iloc[i-1]['Sequência']
        seq_atual = df_sorted.iloc[i]['Sequência']
        
        # Reboot detectado: sequência atual menor que anterior e sequência atual baixa (0-10)
        if seq_atual < seq_anterior and seq_atual <= 10:
            reboot_count += 1
            reboots.append({
                'Reboot_Numero': reboot_count,
                'Data_Hora': df_sorted.iloc[i]['Data/Hora Inclusão'],
                'Sequencia_Anterior': seq_anterior,
                'Sequencia_Nova': seq_atual,
                'Tipo_Mensagem': df_sorted.iloc[i]['Tipo Mensagem']
            })
    
    return reboot_count, reboots

def analisar_intervalos_tempo(df: pd.DataFrame) -> Tuple[List[Dict], List[Dict]]:
    """Analisa intervalos de tempo para posicionamento e modo econômico"""
    anomalias_posicionamento = []
    anomalias_modo_eco = []
    
    # Tolerância de 2 segundos
    tolerancia = timedelta(seconds=2)
    tempo_esperado_pos = timedelta(minutes=3)  # 3 minutos
    tempo_esperado_eco = timedelta(hours=1)    # 1 hora
    
    for i, row in df.iterrows():
        # Análise de Posicionamento
        if row['Tipo Mensagem'] == 'Posicionamento por tempo em movimento':
            diff_str = row['Diff_Posicionamento']
            if not pd.isna(diff_str) and isinstance(diff_str, str):
                try:
                    # Converter string HH:MM:SS para timedelta
                    parts = diff_str.split(':')
                    diff_time = timedelta(hours=int(parts[0]), minutes=int(parts[1]), seconds=int(parts[2]))
                    
                    # Verificar se está fora do esperado (3min ± 2s)
                    if not (tempo_esperado_pos - tolerancia <= diff_time <= tempo_esperado_pos + tolerancia):
                        anomalias_posicionamento.append({
                            'Sequencia': row['Sequência'],
                            'Data_Hora': row['Data/Hora Evento'],
                            'Tempo_Esperado': '00:03:00',
                            'Tempo_Real': diff_str,
                            'Diferenca_Segundos': (diff_time - tempo_esperado_pos).total_seconds(),
                            'Status': 'Fora da tolerância (±2s)'
                        })
                except (ValueError, IndexError, TypeError):
                    print(f"⚠️ Erro ao processar diff posicionamento: {diff_str}")
        
        # Análise de Modo Econômico
        if row['Tipo Mensagem'] == 'Modo econômico':
            diff_str = row['Diff_ModoEco']
            if not pd.isna(diff_str) and isinstance(diff_str, str):
                try:
                    # Converter string HH:MM:SS para timedelta
                    parts = diff_str.split(':')
                    diff_time = timedelta(hours=int(parts[0]), minutes=int(parts[1]), seconds=int(parts[2]))
                    
                    # Verificar se está fora do esperado (1h ± 2s)
                    if not (tempo_esperado_eco - tolerancia <= diff_time <= tempo_esperado_eco + tolerancia):
                        anomalias_modo_eco.append({
                            'Sequencia': row['Sequência'],
                            'Data_Hora': row['Data/Hora Evento'],
                            'Tempo_Esperado': '01:00:00',
                            'Tempo_Real': diff_str,
                            'Diferenca_Segundos': (diff_time - tempo_esperado_eco).total_seconds(),
                            'Status': 'Fora da tolerância (±2s)'
                        })
                except (ValueError, IndexError, TypeError):
                    print(f"⚠️ Erro ao processar diff modo econômico: {diff_str}")
    
    return anomalias_posicionamento, anomalias_modo_eco

def detectar_anomalias_ignicao(df: pd.DataFrame) -> List[Dict]:
    """Detecta anomalias de ignição (IGN/IGF consecutivos não duplicados)"""
    anomalias = []
    
    # Filtrar apenas eventos de ignição e remover duplicatas
    df_ignicao = df[df['Tipo Mensagem'].isin(['IGN', 'IGF'])].copy()
    df_ignicao = remover_mensagens_duplicadas(df_ignicao)
    df_ignicao = df_ignicao.sort_values('Data/Hora Evento').reset_index(drop=True)
    
    for i in range(len(df_ignicao) - 1):
        atual = df_ignicao.iloc[i]
        proximo = df_ignicao.iloc[i + 1]
        
        # Detectar IGN seguido de IGN ou IGF seguido de IGF
        if atual['Tipo Mensagem'] == proximo['Tipo Mensagem']:
            # Verificar se NÃO são mensagens duplicadas (sequências diferentes)
            if atual['Sequência'] != proximo['Sequência']:
                anomalias.append({
                    'Tipo_Anomalia': f'{atual["Tipo Mensagem"]} Consecutivos',
                    'Sequencia_1': atual['Sequência'],
                    'Sequencia_2': proximo['Sequência'],
                    'Data_Hora_1': atual['Data/Hora Evento'],
                    'Data_Hora_2': proximo['Data/Hora Evento'],
                    'Diferenca_Tempo': proximo['Data/Hora Evento'] - atual['Data/Hora Evento'],
                    'Status': 'Possível perda de evento intermediário'
                })
    
    return anomalias

def detectar_anomalias_velocidade(df: pd.DataFrame) -> List[Dict]:
    """Detecta anomalias de velocidade (excesso/retorno consecutivos não duplicados)"""
    anomalias = []
    
    # Filtrar apenas eventos de velocidade e remover duplicatas
    df_velocidade = df[df['Tipo Mensagem'].isin(['Excesso de velocidade', 'Retorno de velocidade'])].copy()
    df_velocidade = remover_mensagens_duplicadas(df_velocidade)
    df_velocidade = df_velocidade.sort_values('Data/Hora Evento').reset_index(drop=True)
    
    for i in range(len(df_velocidade) - 1):
        atual = df_velocidade.iloc[i]
        proximo = df_velocidade.iloc[i + 1]
        
        # Detectar eventos consecutivos do mesmo tipo
        if atual['Tipo Mensagem'] == proximo['Tipo Mensagem']:
            # Verificar se NÃO são mensagens duplicadas (sequências diferentes)
            if atual['Sequência'] != proximo['Sequência']:
                anomalias.append({
                    'Tipo_Anomalia': f'{atual["Tipo Mensagem"]} Consecutivos',
                    'Sequencia_1': atual['Sequência'],
                    'Sequencia_2': proximo['Sequência'],
                    'Data_Hora_1': atual['Data/Hora Evento'],
                    'Data_Hora_2': proximo['Data/Hora Evento'],
                    'Diferenca_Tempo': proximo['Data/Hora Evento'] - atual['Data/Hora Evento'],
                    'Status': 'Possível perda de evento intermediário'
                })
    
    return anomalias
def detectar_anomalias_velocidade(df: pd.DataFrame) -> List[Dict]:
    """Detecta anomalias de velocidade (excesso/retorno consecutivos não duplicados)"""
    anomalias = []
    
    # Filtrar apenas eventos de velocidade e remover duplicatas
    df_velocidade = df[df['Tipo Mensagem'].isin(['Excesso de velocidade', 'Retorno de velocidade'])].copy()
    df_velocidade = remover_mensagens_duplicadas(df_velocidade)
    df_velocidade = df_velocidade.sort_values('Data/Hora Evento').reset_index(drop=True)
    
    for i in range(len(df_velocidade) - 1):
        atual = df_velocidade.iloc[i]
        proximo = df_velocidade.iloc[i + 1]
        
        # Detectar eventos consecutivos do mesmo tipo
        if atual['Tipo Mensagem'] == proximo['Tipo Mensagem']:
            # Verificar se NÃO são mensagens duplicadas (sequências diferentes)
            if atual['Sequência'] != proximo['Sequência']:
                anomalias.append({
                    'Tipo_Anomalia': f'{atual["Tipo Mensagem"]} Consecutivos',
                    'Sequencia_1': atual['Sequência'],
                    'Sequencia_2': proximo['Sequência'],
                    'Data_Hora_1': atual['Data/Hora Evento'],
                    'Data_Hora_2': proximo['Data/Hora Evento'],
                    'Diferenca_Tempo': proximo['Data/Hora Evento'] - atual['Data/Hora Evento'],
                    'Status': 'Possível perda de evento intermediário'
                })
    
    return anomalias

def detectar_mensagens_log_pos_igf(df: pd.DataFrame) -> List[Dict]:
    """
    Detecta grupos de mensagens em modo LOG (diff > 1min entre inclusão e evento)
    que ocorrem SOMENTE após eventos IGF (ignição desligada).
    
    Retorna informações sobre cada grupo de mensagens em LOG detectado após IGF.
    """
    anomalias_log = []
    
    # Ordenar por Data/Hora Inclusão
    df_sorted = df.sort_values('Data/Hora Inclusão').reset_index(drop=True)
    
    # Limiar de 1 minuto (60 segundos)
    limiar_log = timedelta(minutes=1)
    
    i = 0
    while i < len(df_sorted):
        row = df_sorted.iloc[i]
        
        # Detectar IGF
        if row['Tipo Mensagem'] == 'IGF':
            igf_data = row['Data/Hora Evento']
            igf_seq = row['Sequência']
            
            # Procurar mensagens em LOG após este IGF
            mensagens_log = []
            j = i + 1
            
            while j < len(df_sorted):
                msg = df_sorted.iloc[j]
                
                # Calcular diferença entre inclusão e evento
                inclusao_time = msg['Data/Hora Inclusão']
                evento_time = msg['Data/Hora Evento']
                
                if pd.notna(inclusao_time) and pd.notna(evento_time):
                    diff_log = inclusao_time - evento_time
                    
                    # Se a diferença é maior que 1 minuto, é mensagem em LOG
                    if diff_log > limiar_log:
                        mensagens_log.append({
                            'sequencia': msg['Sequência'],
                            'tipo_mensagem': msg['Tipo Mensagem'],
                            'data_hora_evento': evento_time,
                            'data_hora_inclusao': inclusao_time,
                            'diferenca_log': diff_log,
                            'diferenca_log_formatada': format_timedelta(diff_log)
                        })
                        j += 1
                    else:
                        # Diferença <= 1 min, fim do grupo de mensagens em LOG
                        break
                else:
                    j += 1
            
            # Se encontrou mensagens em LOG após IGF, registrar anomalia
            if mensagens_log:
                # Calcular estatísticas do grupo
                total_mensagens = len(mensagens_log)
                primeira_msg = mensagens_log[0]
                ultima_msg = mensagens_log[-1]
                
                # Duração total do período em LOG
                duracao_total = ultima_msg['data_hora_inclusao'] - primeira_msg['data_hora_evento']
                
                # Diferença média de LOG
                diffs_segundos = [msg['diferenca_log'].total_seconds() for msg in mensagens_log]
                diff_media = sum(diffs_segundos) / len(diffs_segundos)
                diff_max = max(diffs_segundos)
                diff_min = min(diffs_segundos)
                
                anomalias_log.append({
                    'IGF_Sequencia': igf_seq,
                    'IGF_Data_Hora': igf_data,
                    'Total_Mensagens_LOG': total_mensagens,
                    'Primeira_Mensagem_LOG': primeira_msg['data_hora_evento'],
                    'Ultima_Mensagem_LOG': ultima_msg['data_hora_evento'],
                    'Duracao_Total_Periodo': format_timedelta(duracao_total),
                    'Diff_LOG_Media_Segundos': f"{diff_media:.0f}",
                    'Diff_LOG_Minima_Segundos': f"{diff_min:.0f}",
                    'Diff_LOG_Maxima_Segundos': f"{diff_max:.0f}",
                    'Tipos_Mensagens': ', '.join(set([msg['tipo_mensagem'] for msg in mensagens_log])),
                    'Sequencias': f"{primeira_msg['sequencia']} a {ultima_msg['sequencia']}",
                    'Detalhes_Mensagens': mensagens_log
                })
                
                # Pular as mensagens já processadas
                i = j
            else:
                i += 1
        else:
            i += 1
    
    return anomalias_log

def gerar_relatorio_completo(input_file: str, output_dir: str = "relatorios"):
    """Função principal que gera relatório completo de análises"""
    
    print("="*100)
    print("🔍 INICIANDO ANÁLISE COMPLETA DE ANOMALIAS")
    print("="*100)
    
    # Carregar dados
    print(f"📂 Carregando arquivo: {input_file}")
    df = pd.read_csv(input_file, sep=",")
    df.columns = df.columns.str.strip()
    print(f"✅ Arquivo carregado: {len(df)} registros")
    
    # Criar diretório de saída
    os.makedirs(output_dir, exist_ok=True)
    
    # 1. Calcular distância do hodômetro
    info_hodometro = calcular_distancia_hodometro(df)
    
    # 2. Contar viagens
    info_viagens = contar_viagens(df)
    
    # 3. Adicionar diferenças de tempo (incluindo LOG)
    df_com_diffs = adicionar_diffs(df)
    
    # 4. Contar reboots
    num_reboots, lista_reboots = contar_reboots(df_com_diffs)
    
    # 5. Analisar intervalos de tempo
    anomalias_pos, anomalias_eco = analisar_intervalos_tempo(df_com_diffs)
    
    # 6. Detectar anomalias de ignição
    anomalias_ignicao = detectar_anomalias_ignicao(df_com_diffs)
    
    # 7. Detectar anomalias de velocidade
    anomalias_velocidade = detectar_anomalias_velocidade(df_com_diffs)
    
    anomalias_log_pos_igf = detectar_mensagens_log_pos_igf(df_com_diffs)

    # ==========================
    # GERAR TEXTO DO RELATÓRIO
    # ==========================
    relatorio_txt = []
    relatorio_txt.append("="*100)
    relatorio_txt.append("📋 RELATÓRIO COMPLETO DE ANÁLISE")
    relatorio_txt.append("="*100)

    relatorio_txt.append(f"📊 RESUMO GERAL:")
    relatorio_txt.append(f"   📁 Total de Registros: {len(df)}")
    relatorio_txt.append(f"   📅 Período: {df_com_diffs['Data/Hora Evento'].min()} até {df_com_diffs['Data/Hora Evento'].max()}")
    relatorio_txt.append(f"\n🚗 INFORMAÇÕES DO HODÔMETRO:")
    if info_hodometro['distancia_percorrida'] is not None:
        relatorio_txt.append(f"   🏁 Primeiro KM válido: {info_hodometro['primeiro_km']:.2f} km ({info_hodometro['data_primeiro']})")
        relatorio_txt.append(f"   🏆 Último KM válido: {info_hodometro['ultimo_km']:.2f} km ({info_hodometro['data_ultimo']})")
        relatorio_txt.append(f"   📏 Distância percorrida no período: {info_hodometro['distancia_percorrida']:.2f} km")
        relatorio_txt.append(f"   📊 Total de registros válidos de hodômetro: {info_hodometro['total_registros_validos']}")
    else:
        relatorio_txt.append(f"   ⚠️ Não foi possível calcular a distância (dados insuficientes)")

    # INFORMAÇÕES DAS VIAGENS
    relatorio_txt.append(f"\n🛣️ INFORMAÇÕES DAS VIAGENS:")
    relatorio_txt.append(f"   ✅ Viagens completas (IGN→IGF): {info_viagens['total_viagens_completas']}")
    relatorio_txt.append(f"   🔴 IGN sem IGF correspondente: {info_viagens['ign_sem_igf']}")
    relatorio_txt.append(f"   🟠 IGF sem IGN anterior: {info_viagens['igf_sem_ign']}")
    
    # Detalhes das viagens completas
    if info_viagens['detalhes_viagens']:
        relatorio_txt.append(f"   🚗 DETALHES DAS VIAGENS COMPLETAS:")
        for viagem in info_viagens['detalhes_viagens']:
            relatorio_txt.append(f"      {viagem['viagem_numero']:2d}. Início: {viagem['ignicao_ligada']}")
            relatorio_txt.append(f"          Fim:    {viagem['ignicao_desligada']}")
            relatorio_txt.append(f"          Duração: {viagem['duracao_formatada']} (Seq: {viagem['sequencia_ign']}→{viagem['sequencia_igf']})")
    
    # IGN órfãos
    if info_viagens['ign_orfaos']:
        relatorio_txt.append(f"   🔴 IGN ÓRFÃOS (sem IGF correspondente):")
        for i, ign in enumerate(info_viagens['ign_orfaos'], 1):
            relatorio_txt.append(f"      {i:2d}. {ign['data_hora']} - Seq: {ign['sequencia']}")
    
    # IGF órfãos
    if info_viagens['igf_orfaos']:
        relatorio_txt.append(f"   🟠 IGF ÓRFÃOS (sem IGN anterior):")
        for i, igf in enumerate(info_viagens['igf_orfaos'], 1):
            relatorio_txt.append(f"      {i:2d}. {igf['data_hora']} - Seq: {igf['sequencia']}")

    relatorio_txt.append(f"\n🔄 REBOOTS DETECTADOS:")
    relatorio_txt.append(f"   🔢 Total: {num_reboots}")
    if lista_reboots:
        for reboot in lista_reboots:
            relatorio_txt.append(f"   📅 {reboot['Data_Hora']} - Seq: {reboot['Sequencia_Anterior']} → {reboot['Sequencia_Nova']}")
    relatorio_txt.append(f"\n⏰ ANOMALIAS DE INTERVALOS:")
    relatorio_txt.append(f"   🎯 Posicionamento (esperado 3min ±2s): {len(anomalias_pos)} anomalias")
    relatorio_txt.append(f"   💤 Modo Econômico (esperado 1h ±2s): {len(anomalias_eco)} anomalias")

    # TODAS as anomalias de posicionamento
    if anomalias_pos:
        relatorio_txt.append(f"   📍 DETALHES COMPLETOS - Posicionamento:")
        for i, anom in enumerate(anomalias_pos, 1):
            relatorio_txt.append(f"      {i:3d}. Seq {anom['Sequencia']} ({anom['Data_Hora']}): {anom['Tempo_Real']} (diff: {anom['Diferenca_Segundos']:.0f}s)")

    # TODAS as anomalias de modo econômico
    if anomalias_eco:
        relatorio_txt.append(f"   💤 DETALHES COMPLETOS - Modo Econômico:")
        for i, anom in enumerate(anomalias_eco, 1):
            relatorio_txt.append(f"      {i:3d}. Seq {anom['Sequencia']} ({anom['Data_Hora']}): {anom['Tempo_Real']} (diff: {anom['Diferenca_Segundos']:.0f}s)")

    relatorio_txt.append(f"\n🔥 ANOMALIAS DE IGNIÇÃO:")
    relatorio_txt.append(f"   🚨 Total: {len(anomalias_ignicao)} anomalias")
    # TODAS as anomalias de ignição
    if anomalias_ignicao:
        relatorio_txt.append(f"   🔥 DETALHES COMPLETOS - Ignição:")
        for i, anom in enumerate(anomalias_ignicao, 1):
            relatorio_txt.append(f"      {i:3d}. {anom['Tipo_Anomalia']}: Seq {anom['Sequencia_1']} → {anom['Sequencia_2']}")
            relatorio_txt.append(f"           Data: {anom['Data_Hora_1']} → {anom['Data_Hora_2']}")
            relatorio_txt.append(f"           Intervalo: {anom['Diferenca_Tempo']}")

    relatorio_txt.append(f"🏃 ANOMALIAS DE VELOCIDADE:")
    relatorio_txt.append(f"   🚨 Total: {len(anomalias_velocidade)} anomalias")
    # TODAS as anomalias de velocidade
    if anomalias_velocidade:
        relatorio_txt.append(f"   🏃 DETALHES COMPLETOS - Velocidade:")
        for i, anom in enumerate(anomalias_velocidade, 1):
            relatorio_txt.append(f"      {i:3d}. {anom['Tipo_Anomalia']}: Seq {anom['Sequencia_1']} → {anom['Sequencia_2']}")
            relatorio_txt.append(f"           Data: {anom['Data_Hora_1']} → {anom['Data_Hora_2']}")
            relatorio_txt.append(f"           Intervalo: {anom['Diferenca_Tempo']}")

    relatorio_txt.append(f"\n📝 MENSAGENS EM LOG APÓS IGF:")
    relatorio_txt.append(f"   🚨 Total de ocorrências: {len(anomalias_log_pos_igf)}")
    if anomalias_log_pos_igf:
        relatorio_txt.append(f"   📝 DETALHES COMPLETOS - Mensagens em LOG após IGF:")
        for i, anom in enumerate(anomalias_log_pos_igf, 1):
            relatorio_txt.append(f"      {i:3d}. IGF (Seq {anom['IGF_Sequencia']}) em {anom['IGF_Data_Hora']}")
            relatorio_txt.append(f"           → {anom['Total_Mensagens_LOG']} mensagens em LOG detectadas")
            relatorio_txt.append(f"           → Sequências: {anom['Sequencias']}")
            relatorio_txt.append(f"           → Período: {anom['Primeira_Mensagem_LOG']} até {anom['Ultima_Mensagem_LOG']}")
            

    relatorio_txt.append(f"\n🎉 ANÁLISE CONCLUÍDA!")
    relatorio_txt.append(f"📁 Todos os arquivos foram salvos em: {output_dir}/")
    relatorio_txt.append("="*100)
    relatorio_txt.append("✅ RELATÓRIO FINALIZADO COM SUCESSO!")
    relatorio_txt.append("="*100)

    # ==========================
    # SALVAR ARQUIVO .TXT
    # ==========================
    txt_output = os.path.splitext(input_file)[0] + ".txt"
    with open(txt_output, "w", encoding="utf-8") as f:
        f.write("\n".join(relatorio_txt))
    print(f"\n💾 Relatório em TXT salvo em: {txt_output}")
    
    # Salvar os CSVs (como já fazia)
    df_com_diffs.to_csv(os.path.join(output_dir, "dados_processados.csv"), sep=",", index=False)
    if lista_reboots:
        pd.DataFrame(lista_reboots).to_csv(os.path.join(output_dir, "relatorio_reboots.csv"), index=False)
    if anomalias_pos:
        pd.DataFrame(anomalias_pos).to_csv(os.path.join(output_dir, "anomalias_posicionamento.csv"), index=False)
    if anomalias_eco:
        pd.DataFrame(anomalias_eco).to_csv(os.path.join(output_dir, "anomalias_modo_economico.csv"), index=False)
    if info_hodometro['distancia_percorrida'] is not None:
        pd.DataFrame([info_hodometro]).to_csv(os.path.join(output_dir, "relatorio_hodometro.csv"), index=False)
    if info_viagens['detalhes_viagens']:
        pd.DataFrame(info_viagens['detalhes_viagens']).to_csv(os.path.join(output_dir, "relatorio_viagens.csv"), index=False)
    if info_viagens['ign_orfaos'] or info_viagens['igf_orfaos']:
        todos_orfaos = info_viagens['ign_orfaos'] + info_viagens['igf_orfaos']
        pd.DataFrame(todos_orfaos).to_csv(os.path.join(output_dir, "eventos_orfaos.csv"), index=False)
    if anomalias_ignicao or anomalias_velocidade:
        todas = anomalias_ignicao + anomalias_velocidade
        pd.DataFrame(todas).to_csv(os.path.join(output_dir, "anomalias_eventos.csv"), index=False)

    return relatorio_txt


if __name__ == "__main__":
    # Configuração do arquivo de entrada
    input_file = "logs/865209077386580_decoded.csv"
    
    try:
        # Verificar se o arquivo existe
        if not os.path.exists(input_file):
            print(f"❌ Arquivo não encontrado: {input_file}")
            print("Por favor, verifique o caminho do arquivo.")
        else:
            # Executar análise completa
            relatorio = gerar_relatorio_completo(input_file)
            
            print("\n" + "="*100)
            print("✅ RELATÓRIO FINALIZADO COM SUCESSO!")
            print("="*100)
            
    except Exception as e:
        print(f"❌ Erro durante a análise: {str(e)}")
        import traceback
        traceback.print_exc()