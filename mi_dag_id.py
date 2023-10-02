from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import datetime, timedelta

import json
import pandas as pd
import numpy as np
import logging
import requests

from pandas import json_normalize
from datetime import datetime
from sqlalchemy import create_engine, text

import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# Define los argumentos del DAG
default_args = {
    'owner': 'laura',
    'depends_on_past': False,
    'start_date': datetime(2023, 9, 26),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Crea el objeto DAG
with DAG(
    'mi_dag_id',
    default_args=default_args,
    description='Mi DAG de ejemplo en Airflow',
    schedule_interval=timedelta(days=1),  # Programación diaria
    catchup=False,  # Evita la ejecución de tareas pasadas al inicio
) as dag:
  
    def obtener_info_de_mercado_libre():
        logging.info("Se obtiene información de la pagina mercado libre")
        dfs = []
       
        app_id = '7408665960354485'
        category_id = 'MLA1577'
        url = f'https://api.mercadolibre.com/sites/MLA/search?category={category_id}&app_id={app_id}'
        response = requests.get(url)

        if response.status_code == 200:
            data = response.json()
            productos = data['results']
            for producto in productos:
                content = json.loads(response.text)
                dfs.append(pd.DataFrame([content]))                  
            df = pd.concat(dfs, ignore_index=True, sort=False)
            return df
        else:
            print(f'Error al realizar la solicitud. Código de estado: {response.status_code}')

    def process_info(data):
        logging.info("Se transforma la información antes de ser almacenada")
        df_destino = pd.DataFrame(columns=['id','site_id', 'title','price', 'sold_quantity','thumbnail'])
        contador_filas=0
        for index, row in data.iterrows():
                json_data = row['results']
                df_destino = json_normalize(json_data)
                contador_filas += 1
                if contador_filas == 50:
                    break
        df_final = pd.DataFrame()
        df_final[['id','site_id', 'title','price', 'sold_quantity','thumbnail']] = df_destino[['id','site_id', 'title','price', 'sold_quantity','thumbnail']]
        df_final['created_date'] = datetime.today().date()

        return df_final
    
    def informar_ventas (data)
        logging.info("Se verifica si las la información supera los 7.000.000 de ventas")
        def_informar= df_final.query('price*sold_quantity > 7000000')
       
        # Se convierte el data frame en formato html
        df_html = df.to_html()

        logging.info("Se configura el servidor que envia el correo")
        smtp_server = 'smtp.example.com'  # Cambia esto al servidor de tu proveedor de correo
        smtp_port = 587  # Puerto de SMTP (587 es común para TLS)
        smtp_user = 'tu_usuario@example.com'  #  dirección de correo electrónico
        smtp_password = 'tu_contraseña'  # Tu contraseña de correo electrónico

        # Configurar el servidor SMTP
        server = smtplib.SMTP(smtp_server, smtp_port)
        server.starttls()  # Habilitar TLS (Transport Layer Security) para seguridad
        server.login(smtp_user, smtp_password)
        # Crear el mensaje de correo electrónico
        mensaje = MIMEMultipart()
        mensaje['From'] = smtp_user
        mensaje['To'] = 'destinatario@example.com'  # Reemplaza con la dirección de correo del destinatario
        mensaje['Subject'] = 'Productos con ventas > a los 7 millones'
        # Agregar el DataFrame en formato HTML como parte del mensaje
        mensaje.attach(MIMEText(df_html, 'html'))
        # Enviar el correo electrónico
        server.sendmail(smtp_user, 'destinatario@example.com', mensaje.as_string())
        # Cerrar la conexión con el servidor SMTP
        server.quit()

    def save_info(df_final):
        logging.info("Se guarda la información procesada")
        engine = create_engine('sqlite:///productos_meli.sqlite3', echo=True)
        df_final.to_sql('productos', con=engine, if_exists="replace")


if __name__ == "__main__":
    data = obtener_info_de_mercado_libre()
    transformed_data = process_info(data)
    # Se deja comentado este metodo por que se deberia primero configurar el correo desde donde se envia el mail con la información de los productos.
    # informar_ventas(transformed_data)
    save_info(transformed_data)





