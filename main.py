# -*- coding: utf-8 -*-
"""
Created on Fri Oct  8 20:24:47 2021
Desafio CTD
@author: Andre Accioly Vieira
"""
from os import environ
from flask import Flask, jsonify, request, Response
from flask_restful import Resource, Api, reqparse
import requests
import json
import mysql.connector
from datetime import date,datetime
import decimal
from itertools import groupby
from collections import defaultdict

app = Flask(__name__)
api = Api(app) 

class Encoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, datetime):
            return o.isoformat()
        if isinstance(o, decimal.Decimal):
            return str(o)

        return json.JSONEncoder.default(self, o)

## 
# Por simplicidade, a leitura do banco de daos e consequente separação em Classes do modelo foi feita no corpo da própria API
#   
# Cria conexao
db = mysql.connector.connect(host='127.0.0.1',
                         user='root',
                         passwd='password',
                         db='desafio_selecao',
                         port=8000)

#Declara um cursor p/ ler os dados do db no formato dictionary
cursor1 = db.cursor(dictionary=True)
cursor2 = db.cursor(dictionary=True)

#Lê os dados (filtra somente as resposatas válidas)
cursor1.execute("SELECT * FROM respostas_diagnostico;") 

#Fetch do resultado //Query1
result1 = cursor1.fetchall()

with open('respostas.json', 'w') as f:
    f.write(json.dumps(result1, cls=Encoder))

cursor1.close()

cursor2.execute("SELECT * FROM respostas_diagnostico WHERE data_submissao IS NOT NULL;")

#Fetch do resultado //Query2
result2 = cursor2.fetchall()

with open('respostas-clean.json', 'w') as f:
    f.write(json.dumps(result2, cls=Encoder))

cursor2.close()

##
#Landing Page
##
@app.route('/') 
def index():
    return ('<table style="width:100%" style="font-size:15;">'
            '<tr><th>Diagnóstico de Tecnologia da Informação e Comunicação - Prefeitura de São Paulo!</th></tr>'
            '<tr></tr>'
            '<tr><td style="width:33%"><a href="http://127.0.0.1:5000/respostas/2019">Questão 1</a></td></tr>'
            '<tr><td style="width:33%"><a href="http://127.0.0.1:5000/pessoas/2019">Questão 2a</a></td></tr>'
            '<tr><td style="width:33%"><a href="http://127.0.0.1:5000/pessoas/2019/SPTUR">Questão 2b</a></td></tr>'
            '<tr><td style="width:33%"><a href="http://127.0.0.1:5000/custos">Questão 3</a></td></tr>'
            '<tr><td style="width:33%"><a href="http://127.0.0.1:5000/desktops-secretarias">Questão 4</a></td></tr>'
            '<tr><td style="width:33%"><a href="http://127.0.0.1:5000/respostas-clean">Questão 6</a></td></tr>'
            '</table>')

###
# (Questao 1)
# Retorna a lista de orgaos e tipo de orgao dado o ano
#  
@app.route('/respostas/<ano>') 
def get_orgaos(ano):
    with open('respostas.json') as json_file:
        data = json.load(json_file)
    
    #initialises the dictionary with values as list
    respostaDict = defaultdict(list)    
    
    for resposta in data:        
        if resposta['ano_diagnostico'] == int(ano):
           respostaDict['orgaos'].append((resposta['orgao'],resposta['tipo_orgao']))

    return Response(json.dumps(respostaDict['orgaos']), mimetype='application/json')  # return data 

###
# (Questao 2)
# Dado o ano, retorna quantas pessoas trabalham em cada orgao e o total
# 
@app.route('/pessoas/<ano>') 
def get_pessoas(ano):
    with open('respostas.json') as json_file:
        data = json.load(json_file)  
    
    #initialises the dictionary with values as list
    respostaDict = defaultdict(list)
        
    totalPessoal = 0

    for resposta in data:
        if resposta['ano_diagnostico'] == int(ano):
           respostaDict['pessoal'].append((resposta['orgao'],resposta['qtd_equipe']))
           totalPessoal = totalPessoal + int(resposta['qtd_equipe'])
    
    def gen():
        yield json.dumps(respostaDict['pessoal'])
        yield ' Total Pessoal: ' + str(totalPessoal)
    return Response(gen())    
    
###
# (Questao 2)
# Dado o ano e o orgao, retorna quantas pessoas trabalham no orgao 
#
@app.route('/pessoas/<ano>/<orgao>') 
def get_pessoas_orgaos(ano,orgao):
    with open('respostas.json') as json_file:
        data = json.load(json_file)   
    
    #initialises the dictionary with values as list
    respostaDict = defaultdict(list)

    for resposta in data:
        if (resposta['ano_diagnostico'] == int(ano)) and (resposta['orgao'] == orgao):
           respostaDict['pessoal_orgaos'].append((resposta['orgao'],resposta['qtd_equipe']))
   
    return Response(json.dumps(respostaDict['pessoal_orgaos']), mimetype='application/json')  # return data

###
# (Questao 3)
# Custo por tipo_orgao (cada pessoa ganha $12500)
#
@app.route('/custos')  
def get_custos():
    
    SALARIO_CONST = 12500
        
    with open('respostas-clean.json') as json_file:
        data = json.load(json_file)      
    
    #initialises the dictionary with values as list
    respostaDict = defaultdict(list)    
         
    data.sort(key=lambda x: x['tipo_orgao']) #sort the data         
    orgaos = groupby(data, lambda x: x['tipo_orgao'])# then use groupby with the same key
        
    for orgao in orgaos:
        total_pessoal = 0
        custo = 0
        for resposta in data:
            if resposta['tipo_orgao'] == orgao[0]:
                total_pessoal = int(resposta['qtd_equipe'])+total_pessoal 
        custo = total_pessoal*SALARIO_CONST
        respostaDict['custos'].append([orgao[0],total_pessoal,custo])

    return Response(json.dumps(respostaDict['custos']), mimetype='application/json')  # return data

###
# (Questao 4)
# Retorna a quantidade de desktops proprios e alocados por orgaos tipo Secretaria 
# Obs: Como nao foi informado se desktops proprios deveriam ser somados a desktops proprios antigos, mantive separados
#
@app.route('/desktops-secretarias') 
def get_secretarias():   

    with open('respostas.json') as json_file:
        data = json.load(json_file)   
    
    #initialises the dictionary with values as list
    respostaDict = defaultdict(list)

    for resposta in data:
        if resposta['tipo_orgao'] == 'Secretaria':
            respostaDict['secretarias'].append((resposta['orgao'], resposta['desktop_proprio'],resposta['desktop_locado'],int(float(resposta['desktop_proprio_antigo']))))     
    
    return Response(json.dumps(respostaDict['secretarias']), mimetype='application/json')  # return data

############
# A Questão 5 foiresolvida pela criação de um segundo cursor que gera um arquivo 'respostas-clean.json'
# A coluna DATA ULTIMA ATUALIZACAO 
############
@app.route('/atualiza-datas' , methods = ['POST']) 
def update():  
    with open('respostas-clean.json') as json_file:
        json_decoded = json.load(json_file)

    json_decoded['date_updated'] = datetime.now()

    with open('respostas-clean', 'w') as json_file:
        json.dump(json_decoded, json_file)
    
    return {}

# Questao 6
# Retorna a quantidade de desktops proprios e alocados por orgaos tipo Secretaria 
# Obs: Como nao foi informado se desktops proprios deveriam ser somados a desktops proprios antigos, mantive separados
# 
@app.route('/respostas-clean') 
def get_respostas_clean():   

    with open('respostas-clean.json') as json_file:
        data = json.load(json_file)   
    
    #initialises the dictionary with values as list
    respostaDict = defaultdict(list)

    for resposta in data:
        respostaDict['clean'].append((resposta['orgao'], resposta['qtd_equipe'], resposta['utiliza_metodologia'], resposta['desktop_proprio'],resposta['desktop_locado'],int(float(resposta['desktop_proprio_antigo']))))     
    
    return Response(json.dumps(respostaDict['clean']), mimetype='application/json')  # return data


if __name__ == '__main__':
    HOST = environ.get('SERVER_HOST', 'localhost')
    try:
        PORT = int(environ.get('SERVER_PORT', '5000'))
    except ValueError:
        PORT = 5000
    app.run(HOST, PORT)# run our Flask app
    