from pytrends.request import TrendReq
import pandas as pd

# Inicializar a conexão com o Google Trends
pytrends = TrendReq(hl='pt-BR', tz=180)

# Lista de termos relacionados à moda
fashion_terms = ["moda feminina", "moda masculina", "roupas de verão", "streetwear", "tendências de inverno"]

# Definir os parâmetros da pesquisa
pytrends.build_payload(fashion_terms, timeframe='now 7-d', geo='BR')

# Obter interesse ao longo do tempo
interest_over_time = pytrends.interest_over_time()
print("Interesse ao longo do tempo:")
print(interest_over_time)

# Obter termos relacionados (com tratamento de erro)
try:
    related_queries = pytrends.related_queries()
    if related_queries:
        print("\nConsultas relacionadas:")
        for term, data in related_queries.items():
            print(f"\nTermo: {term}")
            if data and 'top' in data and isinstance(data['top'], pd.DataFrame):
                print("Principais buscas relacionadas:")
                print(data['top'])
            else:
                print("Sem dados principais.")

            if data and 'rising' in data and isinstance(data['rising'], pd.DataFrame):
                print("Tendências emergentes:")
                print(data['rising'])
            else:
                print("Sem tendências emergentes.")
    else:
        print("\nNenhuma consulta relacionada encontrada.")

except IndexError:
    print("\nErro ao buscar consultas relacionadas. Pode não haver dados disponíveis.")

# Obter tendências diárias (rising queries)
try:
    trending_searches = pytrends.trending_searches(pn="brazil")
    print("\nPesquisas em alta no Brasil:")
    print(trending_searches)
except Exception as e:
    print("\nErro ao buscar tendências diárias:", str(e))
