import psycopg2
import pandas as pd
from datetime import datetime, timedelta

try:
    conn = psycopg2.connect(
        dbname="logserverPROD",
        user="engenharia",
        password="G075@7412",
        host="dblogservers.gs.interno",
        port="5432"
    )
    print("✅ Conexão bem sucedida!")
    conn.close()
except Exception as e:
    print("❌ Erro:", e)

# # Intervalo total de pesquisa
# data_inicio = datetime(2024, 1, 1)
# data_fim = datetime(2024, 4, 30)

# # Janela de no máximo 10 dias
# janela = timedelta(days=1)

# # DataFrame acumulador
# df_total = pd.DataFrame()

# imei = "869412074614139"

# # Loop de busca
# data_atual = data_inicio
# while data_atual < data_fim:
#     proximo = min(data_atual + janela, data_fim)

#     query = f"""

#             SELECT *
#             FROM tablogmensagemservers ls
#             WHERE ls.lmsdatahorainc BETWEEN '{data_atual.strftime('%Y-%m-%d %H:%M:%S')}'
#                                     AND '{proximo.strftime('%Y-%m-%d %H:%M:%S')}'
#             AND lmsserie IN ({imei})
#     """

#     df_temp = pd.read_sql(query, conn)
#     df_total = pd.concat([df_total, df_temp], ignore_index=True)

#     print(f"✅ Dados de {data_atual.date()} até {proximo.date()} importados ({len(df_temp)} registros)")

#     data_atual = proximo

# # Fecha conexão
# conn.close()

# # Salva em CSV
# df_total.to_csv("dados_coletados.csv", index=False, encoding="utf-8")
# print(f"salvo com {len(df_total)} registros.")