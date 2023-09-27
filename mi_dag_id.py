import requests
import pandas as pd
import json

def obtener_info_de_mercado_libre():
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

 #print(f'Nombre: {producto["title"]}, Precio: {producto["price"]}')
    

