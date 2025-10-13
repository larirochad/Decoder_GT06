import pandas as pd
import os
from typing import Dict, List, Tuple, Optional

def remover_mensagens_duplicadas(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove mensagens duplicadas baseadas em Tipo Mensagem, Sequência e Data/Hora Evento.
    Mantém apenas a primeira ocorrência de cada mensagem única.
    """
    # Criar uma chave única para identificar mensagens duplicadas
    df_clean = df.copy()
    
    # Contar duplicatas antes da limpeza
    duplicatas = df_clean.duplicated(subset=['Tipo Mensagem', 'Sequência', 'Data/Hora Evento'], keep='first')
    total_duplicatas = duplicatas.sum()
    
    if total_duplicatas > 0:
        print(f"🧹 Removendo {total_duplicatas} mensagens duplicadas...")
        
        # Mostrar algumas duplicatas encontradas
        duplicatas_df = df_clean[duplicatas].head(5)
        for _, row in duplicatas_df.iterrows():
            print(f"   - {row['Tipo Mensagem']} (seq {row['Sequência']}) @ {row['Data/Hora Evento']}")
        
        if total_duplicatas > 5:
            print(f"   ... e mais {total_duplicatas - 5} mensagens duplicadas")
    
    # Remover duplicatas mantendo a primeira ocorrência
    df_clean = df_clean.drop_duplicates(subset=['Tipo Mensagem', 'Sequência', 'Data/Hora Evento'], keep='first')
    
    return df_clean

def detectar_anomalias_ignicao(df_original: pd.DataFrame, df_analise: pd.DataFrame) -> List[Dict]:
    """
    Detecta anomalias de ignição comparando dados ordenados vs dados originais.
    
    Args:
        df_original: DataFrame com dados na ordem original de chegada
        df_analise: DataFrame ordenado por Data/Hora Evento
    
    Returns:
        Lista de dicionários com anomalias detectadas e classificadas
    """
    anomalias = []
    
    # Filtrar apenas eventos de ignição e remover duplicatas
    df_ignicao_analise = df_analise[df_analise['Tipo Mensagem'].isin(['IGN', 'IGF'])].copy()
    df_ignicao_analise = remover_mensagens_duplicadas(df_ignicao_analise)
    df_ignicao_analise = df_ignicao_analise.sort_values('Data/Hora Evento').reset_index(drop=True)
    
    print(f"🔍 Analisando {len(df_ignicao_analise)} eventos de ignição...")
    
    # Detectar anomalias na sequência ordenada
    for i in range(len(df_ignicao_analise) - 1):
        atual = df_ignicao_analise.iloc[i]
        proximo = df_ignicao_analise.iloc[i + 1]
        
        # Detectar IGN seguido de IGN (anomalia potencial)
        if atual['Tipo Mensagem'] == 'IGN' and proximo['Tipo Mensagem'] == 'IGN':
            # Verificar se são mensagens idênticas (mesma sequência e data/hora)
            if (atual['Sequência'] == proximo['Sequência'] and 
                atual['Data/Hora Evento'] == proximo['Data/Hora Evento']):
                # Mesma mensagem recebida duas vezes - não é anomalia
                print(f"⚠️  Ignorando mensagem duplicada: IGN (seq {atual['Sequência']}) @ {atual['Data/Hora Evento']}")
                continue
            
            # Validar se é anomalia real ou de ordenação
            classificacao, justificativa = validar_anomalia_ignicao(
                df_original, df_ignicao_analise, atual, proximo
            )
            
            anomalias.append({
                'Tipo_Anomalia': 'Ignição Duplicada',
                'Mensagem_1': f"IGN (seq {atual['Sequência']}) @ {atual['Data/Hora Evento']}",
                'Mensagem_2': f"IGN (seq {proximo['Sequência']}) @ {proximo['Data/Hora Evento']}",
                'Classificacao_Final': classificacao,
                'Justificativa': justificativa,
                'Data_Inicio': atual['Data/Hora Evento'],
                'Data_Fim': proximo['Data/Hora Evento'],
                'Seq_1': atual['Sequência'],
                'Seq_2': proximo['Sequência']
            })
        
        # Detectar IGF seguido de IGF (anomalia potencial)
        elif atual['Tipo Mensagem'] == 'IGF' and proximo['Tipo Mensagem'] == 'IGF':
            # Verificar se são mensagens idênticas (mesma sequência e data/hora)
            if (atual['Sequência'] == proximo['Sequência'] and 
                atual['Data/Hora Evento'] == proximo['Data/Hora Evento']):
                # Mesma mensagem recebida duas vezes - não é anomalia
                print(f"⚠️  Ignorando mensagem duplicada: IGF (seq {atual['Sequência']}) @ {atual['Data/Hora Evento']}")
                continue
            
            classificacao, justificativa = validar_anomalia_ignicao(
                df_original, df_ignicao_analise, atual, proximo
            )
            
            anomalias.append({
                'Tipo_Anomalia': 'Desligamento Duplicado',
                'Mensagem_1': f"IGF (seq {atual['Sequência']}) @ {atual['Data/Hora Evento']}",
                'Mensagem_2': f"IGF (seq {proximo['Sequência']}) @ {proximo['Data/Hora Evento']}",
                'Classificacao_Final': classificacao,
                'Justificativa': justificativa,
                'Data_Inicio': atual['Data/Hora Evento'],
                'Data_Fim': proximo['Data/Hora Evento'],
                'Seq_1': atual['Sequência'],
                'Seq_2': proximo['Sequência']
            })
    
    return anomalias

def analisar_sequencia_logica(df_analise: pd.DataFrame, idx_inicio: int, idx_fim: int) -> Tuple[bool, str]:
    """
    Analisa se a sequência de números entre dois índices tem lógica crescente.
    
    Args:
        df_analise: DataFrame ordenado por Data/Hora Evento
        idx_inicio: Índice inicial da análise
        idx_fim: Índice final da análise
    
    Returns:
        Tupla com (tem_logica, descricao)
    """
    # Extrair a sequência de números entre os índices
    sequencia = df_analise.iloc[idx_inicio:idx_fim+1]['Sequência'].tolist()
    
    if len(sequencia) < 3:
        return True, "Sequência muito pequena para análise"
    
    # Debug: mostrar a sequência analisada
    print(f"🔍 Analisando sequência: {sequencia}")
    
    # Verificar se há reboots (quando a sequência zera mas continua crescente)
    reboots = []
    for i in range(1, len(sequencia)):
        # Reboot: quando o número atual é menor que o anterior E é um número baixo (0-10)
        if sequencia[i] < sequencia[i-1] and sequencia[i] <= 10:
            reboots.append(i)
    
    # Se há reboots, analisar cada segmento separadamente
    if reboots:
        print(f"🔄 Detectados {len(reboots)} possível(is) reboot(s) nas posições: {reboots}")
        segmentos = []
        inicio_segmento = 0
        
        for reboot_idx in reboots:
            segmento = sequencia[inicio_segmento:reboot_idx]
            if len(segmento) > 1:
                segmentos.append(segmento)
            inicio_segmento = reboot_idx
        
        # Adicionar último segmento
        if inicio_segmento < len(sequencia):
            segmentos.append(sequencia[inicio_segmento:])
        
        print(f"📊 Segmentos após reboots: {segmentos}")
        
        # Verificar se cada segmento é crescente
        for i, segmento in enumerate(segmentos):
            if len(segmento) > 1:
                for j in range(1, len(segmento)):
                    if segmento[j] < segmento[j-1]:
                        return False, f"Segmento {i+1} não é crescente: {segmento[j-1]} → {segmento[j]} (ida e volta detectada)"
        
        return True, f"Sequência com {len(reboots)} reboot(s) - todos os segmentos são crescentes"
    
    # Sem reboots - verificar se toda a sequência é crescente
    for i in range(1, len(sequencia)):
        if sequencia[i] < sequencia[i-1]:
            return False, f"Sequência não crescente: {sequencia[i-1]} → {sequencia[i]} (ida e volta detectada)"
    
    return True, "Sequência crescente sem reboots"

def validar_anomalia_ignicao(df_original: pd.DataFrame, df_analise: pd.DataFrame, msg1: pd.Series, msg2: pd.Series) -> Tuple[str, str]:
    """
    Valida se uma anomalia detectada é real ou causada por ordenação.
    
    Args:
        df_original: DataFrame com dados na ordem original
        df_analise: DataFrame ordenado por Data/Hora Evento
        msg1: Primeira mensagem da anomalia
        msg2: Segunda mensagem da anomalia
    
    Returns:
        Tupla com (classificacao, justificativa)
    """
    # Encontrar as mensagens no DataFrame original
    msg1_original = df_original[df_original['Sequência'] == msg1['Sequência']].iloc[0]
    msg2_original = df_original[df_original['Sequência'] == msg2['Sequência']].iloc[0]
    
    # Obter índices originais
    idx1 = df_original[df_original['Sequência'] == msg1['Sequência']].index[0]
    idx2 = df_original[df_original['Sequência'] == msg2['Sequência']].index[0]
    
    # Obter índices na análise ordenada por data/hora
    idx1_analise = df_analise[df_analise['Sequência'] == msg1['Sequência']].index[0]
    idx2_analise = df_analise[df_analise['Sequência'] == msg2['Sequência']].index[0]
    
    # Verificar se as mensagens estão próximas na ordem original
    if abs(idx1 - idx2) <= 3:  # Mensagens próximas na ordem original
        # Verificar se há eventos intermediários que explicam a anomalia
        inicio = min(idx1, idx2)
        fim = max(idx1, idx2)
        
        eventos_intermediarios = df_original.iloc[inicio:fim+1]
        eventos_ignicao_intermediarios = eventos_intermediarios[
            eventos_intermediarios['Tipo Mensagem'].isin(['IGN', 'IGF'])
        ]
        
        if len(eventos_ignicao_intermediarios) > 2:  # Há eventos entre as mensagens
            return "Anomalia de Ordenação", f"A ordenação por data/hora agrupou mensagens que estavam separadas na ordem original. Há {len(eventos_ignicao_intermediarios)-2} eventos de ignição entre elas."
        else:
            return "Anomalia Real Confirmada", "As mensagens estão próximas na ordem original sem eventos intermediários explicativos."
    else:
        # Mensagens distantes na ordem original - análise mais sofisticada
        
        # Analisar toda a sequência entre as mensagens na ordem ordenada por data/hora
        inicio_analise = min(idx1_analise, idx2_analise)
        fim_analise = max(idx1_analise, idx2_analise)
        
        # Verificar se a sequência tem lógica crescente
        tem_logica, descricao_logica = analisar_sequencia_logica(df_analise, inicio_analise, fim_analise)
        
        if tem_logica:
            # Se a sequência tem lógica crescente (mesmo com reboots), NÃO é anomalia
            return "Anomalia de Ordenação", f"Sequência com lógica crescente entre as mensagens: {descricao_logica}. A ordenação por data/hora causou o agrupamento de mensagens que estavam separadas."
        else:
            # Se a sequência não tem lógica (idas e voltas), é uma anomalia real
            return "Anomalia Real Confirmada", f"Sequência sem lógica entre as mensagens: {descricao_logica}. Possível perda de pacote real."

# def detectar_anomalias_velocidade(df_original: pd.DataFrame, df_analise: pd.DataFrame) -> List[Dict]:
#     """
#     Detecta anomalias de velocidade comparando dados ordenados vs dados originais.
#     """
#     anomalias = []
    
#     # Filtrar apenas eventos de velocidade
#     df_velocidade_analise = df_analise[df_analise['Tipo Mensagem'].isin(['Excesso de velocidade', 'Retorno de velocidade'])].copy()
#     df_velocidade_analise = df_velocidade_analise.sort_values('Data/Hora Evento').reset_index(drop=True)
    
#     print(f"🏃 Analisando {len(df_velocidade_analise)} eventos de velocidade...")
    
#     # Detectar anomalias na sequência ordenada
#     for i in range(len(df_velocidade_analise) - 1):
#         atual = df_velocidade_analise.iloc[i]
#         proximo = df_velocidade_analise.iloc[i + 1]
        
#         # Detectar eventos consecutivos iguais
#         if atual['Tipo Mensagem'] == proximo['Tipo Mensagem']:
#             classificacao, justificativa = validar_anomalia_velocidade(
#                 df_original, atual, proximo
#             )
            
#             anomalias.append({
#                 'Tipo_Anomalia': f'{atual["Tipo Mensagem"]} Consecutivos',
#                 'Mensagem_1': f"{atual['Tipo Mensagem']} (seq {atual['Sequência']}) @ {atual['Data/Hora Evento']}",
#                 'Mensagem_2': f"{proximo['Tipo Mensagem']} (seq {proximo['Sequência']}) @ {proximo['Data/Hora Evento']}",
#                 'Classificacao_Final': classificacao,
#                 'Justificativa': justificativa,
#                 'Data_Inicio': atual['Data/Hora Evento'],
#                 'Data_Fim': proximo['Data/Hora Evento'],
#                 'Seq_1': atual['Sequência'],
#                 'Seq_2': proximo['Sequência']
#             })
    
#     return anomalias

# def validar_anomalia_velocidade(df_original: pd.DataFrame, msg1: pd.Series, msg2: pd.Series) -> Tuple[str, str]:
#     """
#     Valida se uma anomalia de velocidade é real ou causada por ordenação.
#     """
#     # Encontrar as mensagens no DataFrame original
#     msg1_original = df_original[df_original['Sequência'] == msg1['Sequência']].iloc[0]
#     msg2_original = df_original[df_original['Sequência'] == msg2['Sequência']].iloc[0]
    
#     # Obter índices originais
#     idx1 = df_original[df_original['Sequência'] == msg1['Sequência']].index[0]
#     idx2 = df_original[df_original['Sequência'] == msg2['Sequência']].index[0]
    
#     # Verificar proximidade na ordem original
#     if abs(idx1 - idx2) <= 3:
#         return "Anomalia de Ordenação", "A ordenação por data/hora agrupou mensagens que estavam separadas na ordem original."
#     else:
#         if abs(msg1['Sequência'] - msg2['Sequência']) <= 5:
#             return "Anomalia Real Confirmada", "Mensagens com sequências próximas mas distantes na ordem original - possível perda de pacote."
#         else:
#             return "Anomalia de Ordenação", "Mensagens com sequências distantes - a ordenação por data/hora causou o agrupamento."

def eventos(df: pd.DataFrame) -> pd.DataFrame:
    """
    Função principal que implementa o algoritmo de detecção de anomalias.
    
    Passo 1: Preparação e Armazenamento dos Dados
    Passo 2: Análise Baseada na Data/Hora (Primeira Tentativa)
    Passo 3: Validação da Anomalia (O Passo Inteligente)
    Passo 4: Relatório Final
    """
    try:
        # PASSO 1: Preparação e Armazenamento dos Dados
        print("🔄 PASSO 1: Preparação e Armazenamento dos Dados")
        
        # Verificar se as colunas necessárias existem
        required_columns = ['Tipo Mensagem', 'Sequência', 'Data/Hora Evento']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"Colunas obrigatórias não encontradas: {missing_columns}")
        
        # Carregar Dados Originais: Manter ordem de chegada intacta
        df_original = df.copy()
        df_original['Data/Hora Evento'] = pd.to_datetime(df_original['Data/Hora Evento'])
        print(f"✅ Dados originais preservados: {len(df_original)} registros")
        
        # Remover mensagens duplicadas dos dados originais
        df_original = remover_mensagens_duplicadas(df_original)
        print(f"✅ Dados originais limpos: {len(df_original)} registros únicos")
        
        # Criar Cópia para Análise: Ordenar por Data/Hora
        df_analise = df_original.copy()
        df_analise = df_analise.sort_values('Data/Hora Evento').reset_index(drop=True)
        print(f"✅ Cópia para análise criada e ordenada por Data/Hora")
        
        # PASSO 2: Análise Baseada na Data/Hora (Primeira Tentativa)
        print(f"\n🔍 PASSO 2: Análise Baseada na Data/Hora")
        
        # Detectar anomalias de ignição
        anomalias_ignicao = detectar_anomalias_ignicao(df_original, df_analise)
        
        # # Detectar anomalias de velocidade
        # anomalias_velocidade = detectar_anomalias_velocidade(df_original, df_analise)
        
        # Combinar todas as anomalias
        todas_anomalias = anomalias_ignicao #+ anomalias_velocidade
        
        # PASSO 4: Relatório Final
        print(f"\n📊 PASSO 4: Relatório Final")
        
        if todas_anomalias:
            df_resultado = pd.DataFrame(todas_anomalias)
            df_resultado = df_resultado.sort_values('Data_Inicio').reset_index(drop=True)
            
            # Estatísticas gerais
            total_anomalias = len(df_resultado)
            anomalias_reais = len(df_resultado[df_resultado['Classificacao_Final'] == 'Anomalia Real Confirmada'])
            anomalias_ordenacao = len(df_resultado[df_resultado['Classificacao_Final'] == 'Anomalia de Ordenação'])
            
            print(f"\n{'='*100}")
            print(f"RELATÓRIO DE ANOMALIAS DETECTADAS")
            print(f"{'='*100}")
            print(f"📈 Total de Anomalias: {total_anomalias}")
            print(f"🔴 Anomalias Reais Confirmadas: {anomalias_reais}")
            print(f"🟡 Anomalias de Ordenação: {anomalias_ordenacao}")
            
            # Estatísticas por tipo
            ignicao_reais = len(df_resultado[(df_resultado['Tipo_Anomalia'].str.contains('Ignição|Desligamento')) & 
                                           (df_resultado['Classificacao_Final'] == 'Anomalia Real Confirmada')])
            velocidade_reais = len(df_resultado[(df_resultado['Tipo_Anomalia'].str.contains('velocidade')) & 
                                              (df_resultado['Classificacao_Final'] == 'Anomalia Real Confirmada')])
            
            print(f"\n📊 BREAKDOWN POR CATEGORIA:")
            print(f"   🔥 Ignição - Anomalias Reais: {ignicao_reais}")
            print(f"   🏃 Velocidade - Anomalias Reais: {velocidade_reais}")
            
            # Mostrar detalhes das anomalias
            print(f"\n📋 DETALHES DAS ANOMALIAS:")
            for idx, row in df_resultado.iterrows():
                status_icon = "🔴" if row['Classificacao_Final'] == 'Anomalia Real Confirmada' else "🟡"
                print(f"\n{status_icon} Anomalia #{idx + 1} - {row['Tipo_Anomalia']}")
                print(f"   Classificação: {row['Classificacao_Final']}")
                print(f"   Mensagem 1: {row['Mensagem_1']}")
                print(f"   Mensagem 2: {row['Mensagem_2']}")
                print(f"   Justificativa: {row['Justificativa']}")
            
            return df_resultado
        else:
            print("\n✅ NENHUMA ANOMALIA DETECTADA!")
            print("Todos os eventos estão corretamente sequenciados.")
            return pd.DataFrame()
    
    except Exception as e:
        print(f"❌ Erro durante a análise: {str(e)}")
        import traceback
        traceback.print_exc()
        return pd.DataFrame()

def mostrar_sequencia_mensagens(df: pd.DataFrame, tipo_filtro: str):
    """Função auxiliar para mostrar a sequência de mensagens filtradas"""
    if tipo_filtro == 'ignicao':
        filtro = ['IGN', 'IGF']
        titulo = "IGNIÇÃO"
    # else:
    #     filtro = ['Excesso de velocidade', 'Retorno de velocidade']
    #     titulo = "VELOCIDADE"
    
    df_filtrado = df[df['Tipo Mensagem'].isin(filtro)].copy()
    df_filtrado = df_filtrado.sort_values('Data/Hora Evento').reset_index(drop=True)
    
    print(f"\n📋 SEQUÊNCIA DE MENSAGENS - {titulo}:")
    print("-" * 80)
    for idx, row in df_filtrado.head(20).iterrows():  # Mostrar apenas primeiras 20
        print(f"{row['Data/Hora Evento']} | Seq: {row['Sequência']:3} | {row['Tipo Mensagem']}")
    
    if len(df_filtrado) > 20:
        print(f"... (mostrando apenas as primeiras 20 de {len(df_filtrado)} mensagens)")

def gerar_relatorio_detalhado(df_resultado: pd.DataFrame) -> None:
    """Gera um relatório detalhado das anomalias encontradas"""
    if df_resultado.empty:
        return
    
    print(f"\n{'='*120}")
    print(f"RELATÓRIO DETALHADO DE ANOMALIAS")
    print(f"{'='*120}")
    
    # Agrupar por classificação
    anomalias_reais = df_resultado[df_resultado['Classificacao_Final'] == 'Anomalia Real Confirmada']
    anomalias_ordenacao = df_resultado[df_resultado['Classificacao_Final'] == 'Anomalia de Ordenação']
    
    if not anomalias_reais.empty:
        print(f"\n🔴 ANOMALIAS REAIS CONFIRMADAS ({len(anomalias_reais)}):")
        print("-" * 80)
        for idx, row in anomalias_reais.iterrows():
            print(f"\n{idx + 1}. {row['Tipo_Anomalia']}")
            print(f"   📅 Período: {row['Data_Inicio']} → {row['Data_Fim']}")
            print(f"   📨 {row['Mensagem_1']}")
            print(f"   📨 {row['Mensagem_2']}")
            print(f"   💡 {row['Justificativa']}")
    
    if not anomalias_ordenacao.empty:
        print(f"\n🟡 ANOMALIAS DE ORDENAÇÃO ({len(anomalias_ordenacao)}):")
        print("-" * 80)
        for idx, row in anomalias_ordenacao.iterrows():
            print(f"\n{idx + 1}. {row['Tipo_Anomalia']}")
            print(f"   📅 Período: {row['Data_Inicio']} → {row['Data_Fim']}")
            print(f"   📨 {row['Mensagem_1']}")
            print(f"   📨 {row['Mensagem_2']}")
            print(f"   💡 {row['Justificativa']}")

if __name__ == "__main__":
    try:
        # Verificar se o arquivo existe
        arquivo_csv = 'logs/0868022036382344_decoded.csv'
        if not os.path.exists(arquivo_csv):
            print(f"❌ Arquivo não encontrado: {arquivo_csv}")
            print("Por favor, verifique o caminho do arquivo.")
        else:
            print(f"📂 Carregando arquivo: {arquivo_csv}")
            df_exemplo = pd.read_csv(arquivo_csv, encoding='utf-8', low_memory=False)
            print(f"✅ Arquivo carregado: {len(df_exemplo)} linhas e {len(df_exemplo.columns)} colunas")
            
            # Mostrar informações sobre mensagens relevantes
            tipos_relevantes = ['IGN', 'IGF', 'Excesso de velocidade', 'Retorno de velocidade']
            print(f"\n📊 CONTAGEM DE MENSAGENS RELEVANTES:")
            for tipo in tipos_relevantes:
                count = len(df_exemplo[df_exemplo['Tipo Mensagem'] == tipo])
                if count > 0:
                    print(f"   {tipo}: {count} mensagens")
            
            # Mostrar sequência de mensagens (opcional, para debug)
            resposta = input("\n🔍 Deseja ver a sequência de mensagens filtradas? (s/n): ").lower()
            if resposta == 's':
                mostrar_sequencia_mensagens(df_exemplo, 'ignicao')
                # mostrar_sequencia_mensagens(df_exemplo, 'velocidade')
            
            # Executar análise principal com novo algoritmo
            print(f"\n🚀 Iniciando análise de anomalias com algoritmo inteligente...")
            resultado = eventos(df_exemplo)
            
            if not resultado.empty:
                # Salvar resultado em arquivo
                resultado.to_csv('anomalias_detectadas.csv', index=False, encoding='utf-8')
                print(f"\n💾 Resultado detalhado salvo em: anomalias_detectadas.csv")
                
                # Gerar relatório detalhado
                gerar_relatorio_detalhado(resultado)
                
                # Estatísticas finais
                anomalias_reais = len(resultado[resultado['Classificacao_Final'] == 'Anomalia Real Confirmada'])
                anomalias_ordenacao = len(resultado[resultado['Classificacao_Final'] == 'Anomalia de Ordenação'])
                
                print(f"\n{'='*80}")
                print(f"RESUMO EXECUTIVO")
                print(f"{'='*80}")
                print(f"📊 Total de Anomalias Detectadas: {len(resultado)}")
                print(f"🔴 Anomalias Reais (Requerem Atenção): {anomalias_reais}")
                print(f"🟡 Anomalias de Ordenação (Falsos Positivos): {anomalias_ordenacao}")
                
                if anomalias_reais > 0:
                    print(f"\n⚠️  ATENÇÃO: {anomalias_reais} anomalias reais foram detectadas!")
                    print("   Estas podem indicar problemas no dispositivo ou perda de pacotes.")
                else:
                    print(f"\n✅ Nenhuma anomalia real detectada!")
                    print("   Todos os problemas identificados são causados pela ordenação temporal.")
            else:
                print(f"\n🎉 EXCELENTE! Nenhuma anomalia foi detectada!")
                print("   Os dados estão perfeitamente sequenciados.")
            
    except FileNotFoundError:
        print(f"❌ Erro: Arquivo '{arquivo_csv}' não encontrado.")
    except Exception as e:
        print(f"❌ Erro inesperado: {str(e)}")
        import traceback
        traceback.print_exc()