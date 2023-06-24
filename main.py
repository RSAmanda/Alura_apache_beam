
import re # expressões regulares
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.options.pipeline_options import PipelineOptions
from prometheus_client import write_to_textfile

pipeline_options = PipelineOptions(argv=None)
pipeline = beam.Pipeline(options=pipeline_options)

colunas_dengue = [
                'id',
                'data_iniSE',
                'casos',
                'ibge_code',
                'cidade',
                'uf',
                'cep',
                'latitude',
                'longitude']

def lista_para_dicionario(elemento, colunas):
    """
    Recebe duas listas e retorna um dicionário
    """
    return dict(zip(colunas, elemento))

def texto_para_lista(elemento, delimitador='|'):
    """
    Recebe um texto e um delimitador
    Retorna uma lista de elementos pelo delimitador
    """
    return elemento.split(delimitador)

def trata_datas(elemento):
    """
    Recebe um dicionário e cria um novo campo com ANO-MES
    Retorna o mesmo dicionário com o novo campo.
    """
    elemento['ano_mes'] = '-'.join(elemento['data_iniSE'].split('-')[:2])
    return elemento

def chave_uf(elemento):
    """
    Receber um dicionaŕio
    Retornar uma tupla com o estado(UF) e o elemento (UF, dicionário)
    """
    chave = elemento['uf']
    return (chave, elemento) 

def casos_dengue(elemento):
    """
    Recebe uma tupla ('RS', [{}, {}])
    Retorna uma tupla ('RS-2014-12', 8.0)
    """
    uf, registros = elemento
    for registro in registros:
        if bool(re.search(r'\d', registro['casos'])):
            yield (f"{uf}-{registro['ano_mes']}", float(registro['casos']))
        else:
            yield (f"{uf}-{registro['ano_mes']}", float(0.0))

def chave_uf_ane_mes_de_lista(elemento):
    """
    Receber uma lista de elementos
    Retornar uma tupla contendo uma chave e o valor de chuva em mm
    """
    data, mm, uf=elemento
    ano_mes = '-'.join(data.split('-')[:2])
    chave = f'{uf}-{ano_mes}'
    if float(mm) < 0:
        mm = 0.0
    else:
        mm = float(mm)
    return chave, mm

def arredonda(elemento):
    """
    Recebe uma tupla 
    Retorna a tupla com valor arredondado em uma casa
    """
    chave, mm = elemento
    return (chave, round(mm, 1))

def filtra_campos_vazios(elemento):
    """
    Remove elementos que tenham elementos vazios
    Recebe uma tupla
    Retorna a tupla sem os valores vazios
    """
    chave, dados = elemento
    if all([
        dados['chuvas'],
        dados['dengue']
    ]):
        return True
    return False

def descompactar_elementos(elem):
    """
    Receber uma tupla e retorna uma tupla descompactada (separando uf, ano, mes e os dados)
    """
    chave, dados = elem
    chuva = dados['chuvas'][0]
    dengue = dados['dengue'][0]
    uf, ano, mes = chave.split('-')
    return uf, ano, mes, str(chuva), str(dengue)

def preparar_csv(elemento, delimitador = ';'):
    """
    Receber uma tupla
    Retornar uma string delimitada
    """
    return f"{delimitador}".join(elemento)


#print("casos de dengue")
dengue = ( #pcollection
    pipeline
    | "Leitura do dataset de dengue" >> # cada | é um passo da pipeline
        ReadFromText('casos_dengue.txt', skip_header_lines=1)
    | "De texto para lista" >> beam.Map(texto_para_lista)
    | "De lista para dicionário" >> beam.Map(lista_para_dicionario, colunas_dengue)
    | "Criar um campo ano_mes" >> beam.Map(trata_datas)
    | "Criar chave pelo estado" >> beam.Map(chave_uf)
    | "Agrupar pelo estado" >> beam.GroupByKey()
    | "Descompactar casos de dengue" >>beam.FlatMap(casos_dengue)
    | "Soma dos casos pela chave" >> beam.CombinePerKey(sum)
    #| "Mostrar resultados" >> beam.Map(print)
)

#print("acumulado de chuva")
chuvas = (
    pipeline
    | "Leitura do dataset de chuvas" >>
        ReadFromText('chuvas.csv', skip_header_lines=1)
    # não pode ter dois pipes com o mesmo nome!
    | "De texto para lista (chuvas)" >> beam.Map(texto_para_lista, delimitador = ',')
    | "Criando a chave UF-ANO-MES" >> beam.Map(chave_uf_ane_mes_de_lista)
    | "Soma dos total de chuvas pela chave" >> beam.CombinePerKey(sum)
    | "Arredondar os resultados de chuva" >> beam.Map(arredonda)
    #| "Mostrar resultados (chuva)" >> beam.Map(print)
)

#pcollection do resultado
resultado = (
    #(chuvas, dengue)
    #| "Emplilha as pcols" >> beam.Flatten() # empilhou as pcollections chuva e dengue
    #| "Agrupa as pcols" >> beam.GroupByKey()
    ({'chuvas': chuvas, 'dengue': dengue})
    | "Mesclar pcols" >> beam.CoGroupByKey()
    | "Filtrar dados vazios" >> beam.Filter(filtra_campos_vazios)
    | "Descompactar elementos" >> beam.Map(descompactar_elementos)
    | "Preparar csv" >> beam.Map(preparar_csv)
    #| "Mostrar resultado da união" >> beam.Map(print)
)
header = 'UF; ANO; MES; CHUVA; DENGUE'
resultado | 'Criar arquivo CSV' >> beam.io.WriteToText('resultado', file_name_suffix='.csv', header = header)

pipeline.run()


















