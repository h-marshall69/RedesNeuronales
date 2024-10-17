import requests

url = "https://www.senamhi.gob.pe/include/ajax-informacion-mensual.php"

headers = {
    'Accept': 'application/json, text/javascript, */*; q=0.01',
    'Content-Type': 'application/x-www-form-urlencoded',
    'User-Agent': 'Mozilla/5.0',
}

# Carga útil (payload)
data = {
    'fecha': '1982-10',
}

# Enviar la solicitud POST
response = requests.post(url, headers=headers, data=data)

def filtrar_datos(data):
    return next((entry for entry in data['content'][0]['detalle'] if entry['nomEsta'] == 'MUELLE ENAFER'), None)


#print(filtrar_datos(response.json()))

print(filtrar_datos(response.json()))