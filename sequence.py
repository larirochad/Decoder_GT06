import pandas as pd
import os

def verificar_sequencia(df: pd.DataFrame) -> pd.DataFrame:
    """
    Verifica a consistência da coluna 'Sequência' após ordenar por 'Data/Hora Inclusão'.
    Identifica saltos ou retrocessos que não sejam causados por reinício em 0.
    """

    # Garantir ordenação
    df = df.sort_values(by="Data/Hora Inclusão").reset_index(drop=True)

    problemas = []
    seq_anterior = None

    for i, row in df.iterrows():
        seq = row["Sequência"]
        datahora = row["Data/Hora Inclusão"]

        if seq_anterior is not None:
            # Verificar se houve incremento esperado
            if seq == 0 and seq_anterior != 0:
                # Possível reboot → esperado, não é problema
                pass
            elif seq == seq_anterior + 1:
                # Incremento normal → ok
                pass
            else:
                # Retrocesso ou salto incorreto
                problemas.append({
                    "Index": i,
                    "Data/Hora Inclusão": datahora,
                    "Sequência Anterior": seq_anterior,
                    "Sequência Atual": seq,
                    "Observação": "Retrocesso/Salto detectado"
                })

        seq_anterior = seq

    return pd.DataFrame(problemas)


if __name__ == "__main__":
    # Caminho do arquivo
    input_path = os.path.join("logs", "0869412074557940_decoded.csv")

    # Leitura do CSV
    df = pd.read_csv(input_path, encoding="utf-8")

    # Verificação da sequência
    resultado = verificar_sequencia(df)

    if resultado.empty:
        print("Nenhum problema encontrado na sequência.")
    else:
        print("Problemas encontrados:")
        print(resultado)
        # Salvar para consulta
        resultado.to_csv("problemas_sequencia.csv", index=False, encoding="utf-8")
        print("Relatório salvo em problemas_sequencia.csv")
