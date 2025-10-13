import pandas as pd
import folium
from pathlib import Path
import numpy as np
import json

def gerar_mapa_trajetos_completo(csv_path, output_path='mapa_trajetos_final.html'):
    """
    Gera um mapa interativo completo com trajetos e eventos
    """
    
    # Carregar dados
    try:
        df = pd.read_csv(csv_path, encoding='latin1', sep=',')
        print(f"✅ Dados carregados: {len(df)} registros")
    except Exception as e:
        print(f"❌ Erro ao carregar arquivo: {e}")
        return None
    
    # Processar dados
    df = df.copy()
    df['Data/Hora Evento'] = pd.to_datetime(df['Data/Hora Evento'], errors='coerce')
    
    # Filtrar dados válidos
    df = df.dropna(subset=['Latitude', 'Longitude', 'Data/Hora Evento'])
    df = df[(df['Latitude'] != 0) & (df['Longitude'] != 0)]
    df = df.sort_values('Data/Hora Evento')
    
    if df.empty:
        print("❌ Nenhum dado válido encontrado!")
        return None
    
    # Criar coluna de dia
    df['Dia'] = df['Data/Hora Evento'].dt.strftime('%d/%m/%Y')
    
    # Mapear tipos de eventos
    def mapear_evento(row):
        tipo = str(row.get('Tipo Mensagem', '')).strip()
        return {
            'IGN': 'IGN',
            'IGF': 'IGF',
            'Posicionamento por tempo em movimento': 'Posicionamento',
            'Modo econômico': 'Modo Eco',
            'Exesso de velocidade': 'Excesso Velocidade',
            'Retorno de velocidade': 'Retorno Velocidade',
            'Heartbeat': 'Heartbeat'
        }.get(tipo, tipo)
    
    df['tipo_evento'] = df.apply(mapear_evento, axis=1)
    
    # Filtrar eventos relevantes
    eventos_relevantes = ['IGN', 'IGF', 'Posicionamento', 'Modo Eco', 
                         'Excesso Velocidade', 'Retorno Velocidade']
    df_filtrado = df[df['tipo_evento'].isin(eventos_relevantes)].copy()
    
    print(f"📊 Eventos filtrados: {len(df_filtrado)} de {len(df)} registros")
    
    # Extrair viagens (IGN/IGF)
    ignicoes = df_filtrado[df_filtrado['tipo_evento'] == 'IGN'].copy()
    desligamentos = df_filtrado[df_filtrado['tipo_evento'] == 'IGF'].copy()
    
    viagens = []
    for idx, ign in ignicoes.iterrows():
        ign_time = ign['Data/Hora Evento']
        igfs_posteriores = desligamentos[desligamentos['Data/Hora Evento'] > ign_time]
        
        if not igfs_posteriores.empty:
            igf = igfs_posteriores.iloc[0]
            viagens.append({'IGN': ign_time, 'IGF': igf['Data/Hora Evento']})
        else:
            # Viagem sem fim definido - até final do dia
            eventos_do_dia = df_filtrado[df_filtrado['Dia'] == ign['Dia']]
            if not eventos_do_dia.empty:
                ultimo = eventos_do_dia.iloc[-1]['Data/Hora Evento']
                viagens.append({'IGN': ign_time, 'IGF': ultimo})
    
    # Atribuir viagens aos eventos
    def get_viagem_idx(row):
        evento_time = row['Data/Hora Evento']
        for i, v in enumerate(viagens):
            if v['IGN'] <= evento_time <= v['IGF']:
                return i
        return None
    
    df_filtrado['viagem_idx'] = df_filtrado.apply(get_viagem_idx, axis=1)
    df_filtrado = df_filtrado.dropna(subset=['viagem_idx'])
    df_filtrado['viagem_idx'] = df_filtrado['viagem_idx'].astype(int)
    
    print(f"🚗 Viagens identificadas: {len(viagens)}")
    print(f"📍 Eventos em viagens: {len(df_filtrado)}")
    
    # Criar mapa
    lat_centro = df_filtrado['Latitude'].mean()
    lon_centro = df_filtrado['Longitude'].mean()
    
    m = folium.Map(
        location=[lat_centro, lon_centro], 
        zoom_start=12,
        tiles='OpenStreetMap'
    )
    
    # Cores para eventos
    cores_eventos = {
        'IGN': '#00FF00',           # Verde brilhante
        'IGF': '#FF0000',           # Vermelho
        'Excesso Velocidade': '#FF4500',  # Laranja
        'Retorno Velocidade': '#32CD32',  # Verde lima
        'Posicionamento': '#FFD700',      # Dourado
        'Modo Eco': '#00BFFF'            # Azul ciano
    }
    
    # Cores para linhas das viagens
    cores_linhas = ['#FF1493', '#00CED1', '#FF6347', '#9370DB', '#32CD32', 
                   '#FF8C00', '#20B2AA', '#DA70D6', '#FF69B4', '#87CEEB']
    
    # Grupos por dia
    dias_unicos = sorted(df_filtrado['Dia'].unique())
    grupos_dia = {}
    
    for i, dia in enumerate(dias_unicos):
        # Mostrar apenas os 3 primeiros dias por padrão
        show = i < 3
        grupo = folium.FeatureGroup(name=f"📅 {dia}", show=show)
        grupos_dia[dia] = grupo
        m.add_child(grupo)
    
    # Preparar dados para o JavaScript
    viagens_data = {}
    
    # Adicionar trajetos e eventos
    for viagem_idx in sorted(df_filtrado['viagem_idx'].unique()):
        viagem_df = df_filtrado[df_filtrado['viagem_idx'] == viagem_idx].sort_values('Data/Hora Evento')
        
        if viagem_df.empty:
            continue
        
        dia_viagem = viagem_df.iloc[0]['Dia']
        grupo = grupos_dia[dia_viagem]
        
        # Linha da viagem
        coords = list(zip(viagem_df['Latitude'], viagem_df['Longitude']))
        cor_linha = cores_linhas[viagem_idx % len(cores_linhas)]
        
        if len(coords) > 1:
            # Criar polyline com ID único para controle
            linha = folium.PolyLine(
                locations=coords,
                color=cor_linha,
                weight=4,
                opacity=0.8,
                tooltip=f'🚗 Viagem {viagem_idx + 1} - {dia_viagem} (Clique para isolar)'
            )
            
            # Adicionar propriedade personalizada para identificar a viagem
            linha.add_to(grupo)
            
            # Armazenar dados da viagem para o JavaScript
            viagens_data[f'viagem_{int(viagem_idx)}'] = {
                'coords': [(float(lat), float(lon)) for lat, lon in coords],
                'color': str(cor_linha),
                'id': int(viagem_idx)
            }

        
        # Marcadores dos eventos
        for _, evento in viagem_df.iterrows():
            tipo = evento['tipo_evento']
            cor = cores_eventos.get(tipo, '#808080')
            
            # Popup detalhado
            velocidade = evento.get('Velocidade', 'N/A')
            if pd.notna(velocidade) and velocidade != 'N/A':
                vel_text = f"{velocidade} km/h"
            else:
                vel_text = "N/A"
            
            popup_html = f"""
            <div style='font-family: Arial, sans-serif; min-width: 220px; color: #333;'>
                <div style='background: {cor}; color: white; padding: 8px; margin: -9px -9px 8px -9px; 
                           border-radius: 4px 4px 0 0; text-align: center; font-weight: bold;'>
                    {tipo}
                </div>
                <p style='margin: 6px 0;'><b>📅 Data/Hora:</b><br>
                   {evento['Data/Hora Evento'].strftime('%d/%m/%Y %H:%M:%S')}</p>
                <p style='margin: 6px 0;'><b>🚗 Viagem:</b> {evento['viagem_idx'] + 1}</p>
                <p style='margin: 6px 0;'><b>📍 Coordenadas:</b><br>
                   {evento['Latitude']:.6f}, {evento['Longitude']:.6f}</p>
                <p style='margin: 6px 0;'><b>🏃 Velocidade:</b> {vel_text}</p>
                <p style='margin: 6px 0;'><b>📱 IMEI:</b> {evento.get('IMEI', 'N/A')}</p>
            </div>
            """
            
            # Diferentes estilos para IGN/IGF
            if tipo == 'IGN':
                # Marcador especial para ignição
                marker = folium.Marker(
                    location=[evento['Latitude'], evento['Longitude']],
                    popup=folium.Popup(popup_html, max_width=250),
                    tooltip=f"▶️ Ignição - {evento['Data/Hora Evento'].strftime('%H:%M')}",
                    icon=folium.Icon(color='green', icon='play', prefix='fa')
                )
                marker.add_to(grupo)
                
            elif tipo == 'IGF':
                # Marcador especial para desligamento
                marker = folium.Marker(
                    location=[evento['Latitude'], evento['Longitude']],
                    popup=folium.Popup(popup_html, max_width=250),
                    tooltip=f"⏹️ Desligamento - {evento['Data/Hora Evento'].strftime('%H:%M')}",
                    icon=folium.Icon(color='red', icon='stop', prefix='fa')
                )
                marker.add_to(grupo)
                
            else:
                # CircleMarker para outros eventos
                marker = folium.CircleMarker(
                    location=[evento['Latitude'], evento['Longitude']],
                    radius=6,
                    color='white',
                    weight=2,
                    fill=True,
                    fill_color=cor,
                    fill_opacity=0.9,
                    popup=folium.Popup(popup_html, max_width=250),
                    tooltip=f"{tipo} - {evento['Data/Hora Evento'].strftime('%H:%M')}"
                )
                marker.add_to(grupo)
    
    # Controle de camadas
    folium.LayerControl(collapsed=False, position='topright').add_to(m)
    
    # JavaScript para isolar viagens
    isolate_script = f"""
    <script>
    // Dados das viagens
    var viagensData = {json.dumps(viagens_data)};
    var originalStates = {{}};
    var isolated = false;
    var isolatedTripId = null;
    var tripLines = {{}};
    
    // Criar o mapa Leaflet
    var map = L.map('map').setView([-29.38, -51.08], 13); // coordenadas iniciais e zoom

    // Adicionar camada base (OpenStreetMap, por exemplo)
    L.tileLayer('https://{{s}}.tile.openstreetmap.org/{{z}}/{{x}}/{{y}}.png', {{
        attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
        }}).addTo(map);

    // Inicializar após o mapa carregar
    map.whenReady(function() {{
        setTimeout(function() {{
            // Mapear todas as linhas do mapa
            map.eachLayer(function(layer) {{
                if (layer instanceof L.Polyline) {{
                    // Encontrar a qual viagem esta linha pertence
                    for (var tripId in viagensData) {{
                        var viagem = viagensData[tripId];
                        var layerCoords = layer.getLatLngs().map(function(latLng) {{
                            return [latLng.lat, latLng.lng];
                        }});
                        
                        // Comparar coordenadas para identificar a viagem
                        if (JSON.stringify(layerCoords) === JSON.stringify(viagem.coords)) {{
                            layer.viagemId = viagem.id;
                            tripLines[viagem.id] = layer;
                            
                            // Adicionar evento de clique
                            layer.on('click', function(e) {{
                                isolateTrip(this.viagemId);
                            }});
                            break;
                        }}
                    }}
                }}
            }});
            console.log('Sistema de isolamento de viagens carregado');
        }}, 1000);
    }});
    
    function isolateTrip(tripId) {{
        console.log('Isolando viagem:', tripId);
        
        // Se já está isolada a mesma viagem, restaurar todas
        if (isolated && isolatedTripId === tripId) {{
            restoreAllTrips();
            return;
        }}
        
        // Salvar estados originais se ainda não salvou
        if (!isolated) {{
            saveOriginalStates();
        }}
        
        isolated = true;
        isolatedTripId = tripId;
        
        // Ocultar todas as linhas primeiro
        hideAllTrips();
        
        // Mostrar apenas a viagem selecionada
        showTrip(tripId);
        
        // Atualizar botão de reset
        updateResetButton(true);
    }}
    
    function saveOriginalStates() {{
        // Salvar o estado original de todas as linhas
        for (var tripId in tripLines) {{
            var layer = tripLines[tripId];
            originalStates[tripId] = {{
                opacity: layer.options.opacity,
                weight: layer.options.weight
            }};
        }}
    }}
    
    function hideAllTrips() {{
        for (var tripId in tripLines) {{
            var layer = tripLines[tripId];
            layer.setStyle({{opacity: 0.1, weight: 2}});
        }}
    }}
    
    function showTrip(tripId) {{
        var layer = tripLines[tripId];
        if (layer) {{
            layer.setStyle({{opacity: 1.0, weight: 6}});
            // Centralizar mapa na viagem
            setTimeout(function() {{
                map.fitBounds(layer.getBounds(), {{padding: [20, 20]}});
            }}, 100);
        }}
    }}
    
    function restoreAllTrips() {{
        console.log('Restaurando todas as viagens');
        isolated = false;
        isolatedTripId = null;
        
        for (var tripId in tripLines) {{
            var layer = tripLines[tripId];
            var originalState = originalStates[tripId];
            if (originalState) {{
                layer.setStyle({{
                    opacity: originalState.opacity, 
                    weight: originalState.weight
                }});
            }} else {{
                layer.setStyle({{opacity: 0.8, weight: 4}});
            }}
        }}
        
        updateResetButton(false);
    }}
    
    function updateResetButton(show) {{
        var resetBtn = document.getElementById('resetTripsBtn');
        if (resetBtn) {{
            resetBtn.style.display = show ? 'block' : 'none';
        }}
    }}
    </script>
    """
    
    m.get_root().html.add_child(folium.Element(isolate_script))
    
    # Botão de reset (inicialmente oculto)
    reset_button = '''
    <div id="resetTripsBtn" style="position: fixed; top: 120px; right: 20px; z-index: 9999;
                                  display: none; background: #e74c3c; color: white; 
                                  border: none; padding: 12px 16px; border-radius: 8px;
                                  cursor: pointer; font-family: Arial, sans-serif;
                                  font-weight: bold; box-shadow: 0 4px 12px rgba(0,0,0,0.3);
                                  transition: all 0.3s ease;"
         onclick="restoreAllTrips()"
         onmouseover="this.style.background='#c0392b'"
         onmouseout="this.style.background='#e74c3c'">
        🔄 Mostrar Todas as Viagens
    </div>
    '''
    
    m.get_root().html.add_child(folium.Element(reset_button))
    
    # Legenda (canto inferior esquerdo)
    legenda_html = '''
    <div style="position: fixed; bottom: 20px; left: 20px; z-index: 9999; 
                background: rgba(255,255,255,0.95); border: 3px solid #2c3e50; 
                border-radius: 12px; padding: 16px 20px; font-family: Arial, sans-serif; 
                font-size: 13px; box-shadow: 0 6px 20px rgba(0,0,0,0.3); max-width: 280px;
                backdrop-filter: blur(10px);">
        <h3 style="margin: 0 0 12px 0; color: #2c3e50; text-align: center; 
                   font-size: 16px; border-bottom: 2px solid #3498db; padding-bottom: 8px;">
            🗺️ Legenda de Eventos
        </h3>
    '''
    
    eventos_na_legenda = df_filtrado['tipo_evento'].unique()
    emojis = {
        'IGN': '▶️', 'IGF': '⏹️', 'Excesso Velocidade': '⚡', 
        'Retorno Velocidade': '✅', 'Posicionamento': '📍', 'Modo Eco': '💚'
    }
    
    for tipo in eventos_na_legenda:
        cor = cores_eventos.get(tipo, '#808080')
        emoji = emojis.get(tipo, '🔵')
        legenda_html += f'''
        <div style="margin: 8px 0; display: flex; align-items: center; padding: 4px 0;">
            <span style="display: inline-block; width: 16px; height: 16px; 
                        background: {cor}; border-radius: 50%; margin-right: 12px;
                        border: 2px solid white; box-shadow: 0 2px 4px rgba(0,0,0,0.2);"></span>
            <span style="color: #2c3e50; font-weight: 600; font-size: 12px;">
                {emoji} {tipo}
            </span>
        </div>'''
    
    legenda_html += '''
        <div style="margin-top: 12px; padding-top: 10px; border-top: 2px solid #ecf0f1; 
                    text-align: center; font-size: 11px; color: #7f8c8d; font-style: italic;">
            💡 Clique nas linhas para isolar viagens<br>
            🔍 Clique nos marcadores para detalhes
        </div>
    </div>'''
    
    m.get_root().html.add_child(folium.Element(legenda_html))
    
    # Painel de informações (canto superior esquerdo)
    info_html = f'''
    <div style="position: fixed; top: 20px; left: 20px; z-index: 9999;
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                color: white; border-radius: 12px; padding: 16px 20px;
                font-family: Arial, sans-serif; font-size: 14px;
                box-shadow: 0 6px 20px rgba(0,0,0,0.3); min-width: 240px;
                backdrop-filter: blur(10px);">
        <h3 style="margin: 0 0 12px 0; text-align: center; font-size: 18px; 
                   text-shadow: 1px 1px 2px rgba(0,0,0,0.3);">
            📊 Resumo Geral
        </h3>
        <div style="background: rgba(255,255,255,0.1); border-radius: 8px; padding: 10px; margin-bottom: 8px;">
            <p style="margin: 4px 0;"><b>🗓️ Período:</b> {len(dias_unicos)} dia(s)</p>
            <p style="margin: 4px 0;"><b>🚗 Total de Viagens:</b> {len(viagens)}</p>
            <p style="margin: 4px 0;"><b>📍 Eventos Mapeados:</b> {len(df_filtrado)}</p>
        </div>
        <div style="background: rgba(255,255,255,0.1); border-radius: 8px; padding: 8px; font-size: 12px;">
            <p style="margin: 2px 0; color: #FFD700;"><b>🖱️ Clique numa linha colorida</b></p>
            <p style="margin: 2px 0; opacity: 0.9;">para isolar apenas essa viagem</p>
        </div>
    </div>
    '''
    
    m.get_root().html.add_child(folium.Element(info_html))
    
    # Salvar arquivo
    output_file = Path(output_path)
    m.save(str(output_file))
    
    print(f"\n🎉 Mapa gerado com sucesso!")
    print(f"📂 Arquivo salvo: {output_file.absolute()}")
    print(f"📊 Estatísticas:")
    print(f"   • Dias processados: {len(dias_unicos)}")
    print(f"   • Viagens encontradas: {len(viagens)}")
    print(f"   • Eventos plotados: {len(df_filtrado)}")
    print(f"   • Tipos de evento: {', '.join(df_filtrado['tipo_evento'].unique())}")
    print(f"\n✨ Nova funcionalidade:")
    print(f"   • Clique nas linhas coloridas para isolar viagens individuais")
    print(f"   • Clique novamente na mesma linha ou use o botão 'Mostrar Todas' para restaurar")
    
    return str(output_file.absolute())

# Exemplo de uso
if __name__ == "__main__":
    # Teste com o arquivo fornecido
    arquivo_csv = 'logs/0869412074576791_decoded.csv'
    
    try:
        caminho_mapa = gerar_mapa_trajetos_completo(arquivo_csv)
        if caminho_mapa:
            print(f"\n✅ Abra o arquivo no navegador: {caminho_mapa}")
        
    except Exception as e:
        print(f"❌ Erro geral: {e}")
        print("💡 Verifique se o arquivo CSV existe e está no formato correto.")