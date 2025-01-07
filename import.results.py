import pandas as pd
import mysql.connector

# Configurações do banco de dados
db_config = {
    'host': 'localhost',       
    'user': 'astrosena',     
    'password': 'qaz741',   
    'database': 'astro_sena' 
}

excel_file = 'data/lottery_results.xlsx'  

def importar_dados_para_mysql():
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()

        df = pd.read_excel(excel_file)

        sql_insert = """
        INSERT INTO lottery_results (
            concurso, data_sorteio, bola1, bola2, bola3, bola4, bola5, bola6,
            ganhadores_6, cidade_uf, rateio_6, ganhadores_5, rateio_5,
            ganhadores_4, rateio_4, acumulado_6, arrecadacao_total,
            estimativa_premio, acumulado_virada, observacao
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """

        for _, row in df.iterrows():
            cursor.execute(sql_insert, tuple(row))

        conn.commit()
        print(f"Dados inseridos com sucesso: {len(df)} registros.")

    except mysql.connector.Error as err:
        print(f"Erro ao conectar ao banco: {err}")
    except Exception as e:
        print(f"Erro geral: {e}")
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()

importar_dados_para_mysql()
