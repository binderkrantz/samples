import requests
import pprint
import json
import polars as pl
import duckdb


url = "https://pokeapi.co/api/v2/pokemon/"
params = {'limit': 100}
fields = ['id', 'name', 'height', 'weight', 'stats']

def request(url:str):
    resp = requests.get(url, params=params)
    data = resp.json()
    data_out = data.get('results', data)
    while data.get('next'):
        resp = requests.get(url)
        data = resp.json()
        data_out += data.get('results', data)
        url = data.get('next')
    return data_out

def filter_dicts(dicts, keys):
    '''filters a list of dictionaries by keys'''
    return [{k:v for k,v in d.items() if k in keys} for d in dicts]

pokemon_urls = [resp['url'] for resp in request(url)]

pokemons = [request(url) for url in pokemon_urls[:2]]
pokemons_clean = filter_dicts(pokemons, fields)

df = pl.DataFrame(pokemons_clean)
print(df.explode('stats'))
print(df.explode('stats').unnest('stats'))