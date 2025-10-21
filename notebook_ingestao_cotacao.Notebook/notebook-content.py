# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "f327a5f3-17f6-4f06-a20f-fe4cb42d42ae",
# META       "default_lakehouse_name": "lakehouse_cotacao",
# META       "default_lakehouse_workspace_id": "0855be40-1b57-45a7-ac5f-34fb36a1b482",
# META       "known_lakehouses": [
# META         {
# META           "id": "f327a5f3-17f6-4f06-a20f-fe4cb42d42ae"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# ## Link com a API
# https://docs.awesomeapi.com.br/api-de-moedas

# MARKDOWN ********************

# ## Pares de moedas

# CELL ********************

import requests
import pandas as pd
from bs4 import BeautifulSoup

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# URL do XML
url_par = "https://economia.awesomeapi.com.br/xml/available"
response = requests.get(url_par)

# Usa BeautifulSoup para lidar com XML flexível
soup = BeautifulSoup(response.content, "xml")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Extrai todas as tags diretamente
data = []
for tag in soup.find_all():
    if tag.name != "xml":  # ignora a declaração
        data.append({"sigla_par_moeda": tag.name, "descroca_par_moeda": tag.text})

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cria DataFrame
df_par = pd.DataFrame(data)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Nomes das moedas

# CELL ********************

# URL do XML
url_moeda = "https://economia.awesomeapi.com.br/xml/available/uniq"
response = requests.get(url_moeda)

# Usa BeautifulSoup para lidar com XML flexível
soup = BeautifulSoup(response.content, "xml")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Extrai todas as tags diretamente
data = []
for tag in soup.find_all():
    if tag.name != "xml":  # ignora a declaração
        data.append({"sigla_moeda": tag.name, "nome_moeda": tag.text})

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cria DataFrame
df_moeda = pd.DataFrame(data)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
