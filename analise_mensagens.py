import pandas as pd
from typing import List, Dict, Tuple

def analisar_inconsistencias_telemetria(input_file, output_file=None):
    """
    Analisa inconsistências em dados de telemetria veicular
    
    Args:
        input_file: Caminho para o arquivo CSV de entrada
        output_file: Caminho para o arquivo de saída (opcional)
    """
    
    # Carregar os dados
    df = pd.read_csv(input_file, sep=",")
    df.columns = df.columns.str.strip()
    
    print(f"📊 Carregados {len(df)} registros do arquivo {input_file}")
    print(f"🔍 Colunas disponíveis: {list(df.columns)}")
    
    # Verificar se a coluna 'Tipo Mensagem' existe
    if 'Tipo Mensagem' not in df.columns:
        print("❌ Coluna 'Tipo Mensagem' não encontrada!")
        print(f"Colunas disponíveis: {list(df.columns)}")
        return None
    
    # Definir os tipos de mensagem baseado nas suas imagens
    TIPOS_IGNICAO_ON = ['IGN']  # Mensagens que indicam ignição ON
    TIPOS_IGNICAO_OFF = ['IGF']  # Mensagens que indicam ignição OFF
    TIPOS_MODO_ECO = ['Modo econômico']
    TIPOS_MOVIMENTO = ['Posicionamento por tempo em movimento']
    
    # Adicionar coluna de classificação
    def classificar_mensagem(tipo_msg):
        if pd.isna(tipo_msg):
            return 'OTHER'
        tipo_msg = str(tipo_msg).strip()
        
        if tipo_msg in TIPOS_IGNICAO_ON:
            return 'IGN_ON'
        elif tipo_msg in TIPOS_IGNICAO_OFF:
            return 'IGN_OFF'
        elif tipo_msg in TIPOS_MODO_ECO:
            return 'ECO_MODE'
        elif tipo_msg in TIPOS_MOVIMENTO:
            return 'MOVEMENT_TIME'
        else:
            return 'OTHER'
    
    df['Classificacao'] = df['Tipo Mensagem'].apply(classificar_mensagem)
    
    # Debug: Mostrar distribuição dos tipos
    print("\n📈 Distribuição dos tipos de mensagem:")
    print(df['Classificacao'].value_counts())
    
    # Criar blocos baseados no estado da ignição
    blocos = []
    bloco_atual = None
    inconsistencias = []
    
    for idx, row in df.iterrows():
        tipo_msg = row['Classificacao']
        
        if tipo_msg == 'IGN_ON':
            # Fechar bloco anterior se existir
            if bloco_atual is not None:
                blocos.append(bloco_atual)
            
            # Iniciar novo bloco ON
            bloco_atual = {
                'tipo': 'ON',
                'inicio_idx': idx,
                'inicio_timestamp': row.get('timestamp', idx),
                'mensagens': [idx],
                'fim_idx': None
            }
        
        elif tipo_msg == 'IGN_OFF':
            # Fechar bloco anterior se existir
            if bloco_atual is not None:
                blocos.append(bloco_atual)
            
            # Iniciar novo bloco OFF
            bloco_atual = {
                'tipo': 'OFF',
                'inicio_idx': idx,
                'inicio_timestamp': row.get('timestamp', idx),
                'mensagens': [idx],
                'fim_idx': None
            }
        
        else:
            # Adicionar mensagem ao bloco atual
            if bloco_atual is not None:
                bloco_atual['mensagens'].append(idx)
                bloco_atual['fim_idx'] = idx
    
    # Adicionar último bloco
    if bloco_atual is not None:
        blocos.append(bloco_atual)
    
    print(f"\n🔧 {len(blocos)} blocos de ignição identificados")
    
    # Analisar inconsistências em cada bloco
    for i, bloco in enumerate(blocos):
        for msg_idx in bloco['mensagens']:
            row = df.iloc[msg_idx]
            tipo_msg = row['Classificacao']
            
            # Regra 1: Não deveria haver modo econômico durante ignição ON
            if bloco['tipo'] == 'ON' and tipo_msg == 'ECO_MODE':
                inconsistencias.append({
                    'bloco_idx': i,
                    'bloco_tipo': bloco['tipo'],
                    'linha_csv': msg_idx + 2,  # +2 porque CSV começa em 1 e tem header
                    'tipo_inconsistencia': 'ECO_MODE_DURANTE_IGN_ON',
                    'descricao': f"Modo econômico encontrado durante ignição ON",
                    'tipo_mensagem': row['Tipo Mensagem'],
                    'timestamp': row.get('timestamp', 'N/A'),
                    'dados_linha': row.to_dict()
                })
            
            # Regra 2: Não deveria haver posicionamento por tempo durante ignição OFF
            elif bloco['tipo'] == 'OFF' and tipo_msg == 'MOVEMENT_TIME':
                inconsistencias.append({
                    'bloco_idx': i,
                    'bloco_tipo': bloco['tipo'],
                    'linha_csv': msg_idx + 2,  # +2 porque CSV começa em 1 e tem header
                    'tipo_inconsistencia': 'MOVEMENT_TIME_DURANTE_IGN_OFF',
                    'descricao': f"Posicionamento por tempo em movimento encontrado durante ignição OFF",
                    'tipo_mensagem': row['Tipo Mensagem'],
                    'timestamp': row.get('timestamp', 'N/A'),
                    'dados_linha': row.to_dict()
                })
    
    # Gerar relatório
    relatorio = gerar_relatorio_inconsistencias(inconsistencias, blocos, df)
    print(relatorio)
    
    # Salvar resultados se especificado
    if output_file:
        salvar_resultados(inconsistencias, blocos, df, output_file)
    
    return inconsistencias, blocos, df

def gerar_relatorio_inconsistencias(inconsistencias, blocos, df):
    """Gera relatório detalhado das inconsistências"""
    
    relatorio = "\n" + "="*60 + "\n"
    relatorio += "           RELATÓRIO DE INCONSISTÊNCIAS\n"
    relatorio += "="*60 + "\n\n"
    
    if not inconsistencias:
        relatorio += "✅ Nenhuma inconsistência encontrada!\n"
        relatorio += f"📊 Analisados {len(blocos)} blocos de ignição com {len(df)} mensagens totais.\n"
        return relatorio
    
    relatorio += f"🚨 TOTAL DE INCONSISTÊNCIAS: {len(inconsistencias)}\n"
    relatorio += f"📊 Blocos analisados: {len(blocos)}\n"
    relatorio += f"📈 Total de mensagens: {len(df)}\n\n"
    
    # Agrupar por tipo de inconsistência
    tipos_inconsistencia = {}
    for inc in inconsistencias:
        tipo = inc['tipo_inconsistencia']
        if tipo not in tipos_inconsistencia:
            tipos_inconsistencia[tipo] = []
        tipos_inconsistencia[tipo].append(inc)
    
    # Relatório por tipo
    for tipo, lista_inc in tipos_inconsistencia.items():
        relatorio += f"📍 {tipo.replace('_', ' ')}: {len(lista_inc)} ocorrências\n"
        relatorio += "-" * 50 + "\n"
        
        for inc in lista_inc:
            relatorio += f"   🔸 Linha {inc['linha_csv']} do CSV\n"
            relatorio += f"     Bloco: {inc['bloco_idx']} (Ignição {inc['bloco_tipo']})\n"
            relatorio += f"     Tipo: {inc['tipo_mensagem']}\n"
            relatorio += f"     Timestamp: {inc['timestamp']}\n"
            relatorio += f"     Descrição: {inc['descricao']}\n\n"
    
    return relatorio

def salvar_resultados(inconsistencias, blocos, df_original, output_file):
    """Salva os resultados em um arquivo CSV"""
    
    if not inconsistencias:
        print("✅ Nenhuma inconsistência para salvar.")
        return
    
    # Converter inconsistências para DataFrame
    df_inconsistencias = pd.DataFrame(inconsistencias)
    
    # Salvar arquivo
    df_inconsistencias.to_csv(output_file, index=False)
    print(f"💾 Inconsistências salvas em: {output_file}")

def main():
    """Função principal"""
    input_file = "0865209077378991_decoded.csv"
    output_file = "logs/inconsistencias_encontradas.csv"
    
    print("🚀 Iniciando análise de inconsistências em telemetria...")
    
    try:
        inconsistencias, blocos, df = analisar_inconsistencias_telemetria(input_file, output_file)
        
        if inconsistencias:
            print(f"\n✅ Análise concluída! {len(inconsistencias)} inconsistências encontradas.")
            print(f"📁 Resultados salvos em: {output_file}")
        else:
            print(f"\n✅ Análise concluída! Nenhuma inconsistência encontrada.")
            
    except FileNotFoundError:
        print(f"❌ Arquivo não encontrado: {input_file}")
    except Exception as e:
        print(f"❌ Erro durante a análise: {str(e)}")

if __name__ == "__main__":
    main()