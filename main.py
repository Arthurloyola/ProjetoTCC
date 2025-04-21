import requests
import csv
import os
import re
import mysql.connector
from datetime import datetime
from collections import Counter

# Função para conectar ao MySQL
def connect_to_mysql(host, user, password, database):
    """
    Estabelece conexão com o banco de dados MySQL
    
    Args:
        host (str): Host do servidor MySQL
        user (str): Nome de usuário
        password (str): Senha de acesso
        database (str): Nome do banco de dados
        
    Returns:
        mysql.connector.connection.MySQLConnection: Objeto de conexão MySQL
    """
    try:
        connection = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )
        print("Conexão com MySQL estabelecida com sucesso!")
        return connection
    except mysql.connector.Error as err:
        print(f"Erro de conexão com MySQL: {err}")
        return None

# Função para criar as tabelas no MySQL se não existirem
def create_mysql_tables(connection):
    """
    Cria as tabelas necessárias no banco de dados MySQL
    
    Args:
        connection: Objeto de conexão MySQL
        
    Returns:
        bool: True se as tabelas foram criadas com sucesso, False caso contrário
    """
    try:
        cursor = connection.cursor()
        
        # Tabela para tendências
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS fashion_trends (
            id INT AUTO_INCREMENT PRIMARY KEY,
            title VARCHAR(255),
            link TEXT,
            snippet TEXT,
            source_date VARCHAR(255),
            source VARCHAR(255),
            collection_date DATETIME
        )
        """)
        
        # Tabela para palavras-chave
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS fashion_keywords (
            id INT AUTO_INCREMENT PRIMARY KEY,
            keyword VARCHAR(255),
            count INT,
            ranking INT,
            collection_date DATETIME
        )
        """)
        
        connection.commit()
        print("Tabelas criadas/verificadas com sucesso!")
        return True
    
    except mysql.connector.Error as err:
        print(f"Erro ao criar tabelas: {err}")
        return False

# Função para inserir tendências no MySQL
def insert_fashion_trends(connection, trend_data):
    """
    Insere dados de tendências de moda no MySQL
    
    Args:
        connection: Objeto de conexão MySQL
        trend_data (list): Lista de dicionários com os dados das tendências
        
    Returns:
        int: Número de registros inseridos
    """
    try:
        cursor = connection.cursor()
        
        query = """
        INSERT INTO fashion_trends 
        (title, link, snippet, source_date, source, collection_date) 
        VALUES (%s, %s, %s, %s, %s, %s)
        """
        
        collection_date = datetime.now()
        count = 0
        
        for trend in trend_data:
            # Extrair os dados do dicionário
            title = trend.get("título", "")
            link = trend.get("link", "")
            snippet = trend.get("snippet", "")
            source_date = trend.get("data", "")
            source = trend.get("fonte", "")
            
            # Inserir no banco de dados
            cursor.execute(query, (title, link, snippet, source_date, source, collection_date))
            count += 1
        
        connection.commit()
        print(f"{count} tendências inseridas no MySQL com sucesso!")
        return count
    
    except mysql.connector.Error as err:
        print(f"Erro ao inserir tendências no MySQL: {err}")
        return 0

# Função para inserir palavras-chave no MySQL
def insert_fashion_keywords(connection, top_keywords):
    """
    Insere dados de palavras-chave de moda no MySQL
    
    Args:
        connection: Objeto de conexão MySQL
        top_keywords (list): Lista de dicionários com as palavras-chave mais populares
        
    Returns:
        int: Número de registros inseridos
    """
    try:
        cursor = connection.cursor()
        
        query = """
        INSERT INTO fashion_keywords 
        (keyword, count, ranking, collection_date) 
        VALUES (%s, %s, %s, %s)
        """
        
        collection_date = datetime.now()
        count = 0
        
        for i, keyword_data in enumerate(top_keywords):
            # Extrair os dados do dicionário
            keyword = keyword_data.get("palavra", "")
            keyword_count = keyword_data.get("contagem", 0)
            ranking = i + 1
            
            # Inserir no banco de dados
            cursor.execute(query, (keyword, keyword_count, ranking, collection_date))
            count += 1
        
        connection.commit()
        print(f"{count} palavras-chave inseridas no MySQL com sucesso!")
        return count
    
    except mysql.connector.Error as err:
        print(f"Erro ao inserir palavras-chave no MySQL: {err}")
        return 0

# Funções existentes mantidas do seu código original
def search_fashion_trends(api_key, num_results=30):
    """
    Busca tendências de moda usando a SerpAPI e retorna os resultados
    
    Args:
        api_key (str): Sua chave de API da SerpAPI
        num_results (int): Número de resultados a serem obtidos
        
    Returns:
        list: Lista de dicionários com os dados das tendências
    """
    base_url = "https://serpapi.com/search"
    
    # Parâmetros para a busca
    params = {
        "q": "fashion trends 2025",
        "api_key": api_key,
        "engine": "google",
        "gl": "us",
        "hl": "en",
        "num": num_results
    }
    
    print("Buscando tendências de moda...")
    response = requests.get(base_url, params=params)
    
    if response.status_code != 200:
        print(f"Erro na requisição: {response.status_code}")
        return []
    
    data = response.json()
    
    # Extrair resultados orgânicos
    results = []
    if "organic_results" in data:
        for item in data["organic_results"]:
            trend_data = {
                "título": item.get("title", ""),
                "link": item.get("link", ""),
                "snippet": item.get("snippet", ""),
                "data": item.get("date", "")
            }
            results.append(trend_data)
    
    # Extrair resultados de notícias se disponíveis
    if "news_results" in data:
        for item in data["news_results"]:
            trend_data = {
                "título": item.get("title", ""),
                "link": item.get("link", ""),
                "snippet": item.get("snippet", ""),
                "data": item.get("date", ""),
                "fonte": item.get("source", "")
            }
            results.append(trend_data)
    
    return results

def get_top_fashion_keywords(api_key, num_keywords=10):
    """
    Obtém as palavras-chave mais pesquisadas relacionadas à moda
    usando a API de sugestões de pesquisa do Google via SerpAPI
    
    Args:
        api_key (str): Sua chave de API da SerpAPI
        num_keywords (int): Número de palavras-chave a retornar
        
    Returns:
        list: Lista das top palavras-chave mais pesquisadas
    """
    print("Buscando top palavras-chave de moda...")
    
    # Primeira busca - termos relacionados a "fashion trends"
    params_trends = {
        "q": "fashion trends",
        "api_key": api_key,
        "engine": "google_autocomplete"
    }
    
    response_trends = requests.get("https://serpapi.com/search", params=params_trends)
    
    # Segunda busca - termos relacionados a "fashion"
    params_fashion = {
        "q": "fashion",
        "api_key": api_key,
        "engine": "google_autocomplete"
    }
    
    response_fashion = requests.get("https://serpapi.com/search", params=params_fashion)
    
    # Terceira busca - termos relacionados a "fashion 2025"
    params_2025 = {
        "q": "fashion 2025",
        "api_key": api_key,
        "engine": "google_autocomplete"
    }
    
    response_2025 = requests.get("https://serpapi.com/search", params=params_2025)
    
    keywords = []
    
    # Processar resultados da primeira busca
    if response_trends.status_code == 200:
        data = response_trends.json()
        if "suggestions" in data:
            for suggestion in data["suggestions"]:
                keywords.append(suggestion.get("value", ""))
    
    # Processar resultados da segunda busca
    if response_fashion.status_code == 200:
        data = response_fashion.json()
        if "suggestions" in data:
            for suggestion in data["suggestions"]:
                keywords.append(suggestion.get("value", ""))
    
    # Processar resultados da terceira busca
    if response_2025.status_code == 200:
        data = response_2025.json()
        if "suggestions" in data:
            for suggestion in data["suggestions"]:
                keywords.append(suggestion.get("value", ""))
    
    # Contar ocorrência de palavras significativas nas keywords
    all_words = []
    for keyword in keywords:
        # Limpar e extrair palavras significativas (ignorar palavras comuns como "the", "and", etc)
        words = re.findall(r'\b[a-zA-Z]{3,}\b', keyword.lower())
        # Filtrar palavras muito comuns
        stopwords = ["the", "and", "for", "with", "what", "how", "are", "can", "from", "fashion"]
        filtered_words = [word for word in words if word not in stopwords]
        all_words.extend(filtered_words)
    
    # Contar e obter as top palavras-chave
    word_counts = Counter(all_words)
    top_words = word_counts.most_common(num_keywords)
    
    # Formatar os resultados
    top_keywords = [{"palavra": word, "contagem": count} for word, count in top_words]
    
    return top_keywords

# Nova função principal com integração MySQL
def run_fashion_research_with_mysql(api_key, mysql_config, num_results=30, num_keywords=10):
    """
    Função principal que executa a pesquisa de moda e armazena os resultados no MySQL
    
    Args:
        api_key (str): Sua chave de API da SerpAPI
        mysql_config (dict): Configurações de conexão com o MySQL
        num_results (int): Número de resultados de tendências desejados
        num_keywords (int): Número de palavras-chave a retornar
        
    Returns:
        tuple: (trends_count, keywords_count) - Contador de registros inseridos
    """
    try:
        # Conectar ao MySQL
        connection = connect_to_mysql(
            mysql_config['host'],
            mysql_config['user'],
            mysql_config['password'],
            mysql_config['database']
        )
        
        if not connection:
            print("Não foi possível conectar ao MySQL. Encerrando.")
            return 0, 0
        
        # Criar as tabelas se não existirem
        if not create_mysql_tables(connection):
            print("Erro ao criar tabelas. Encerrando.")
            connection.close()
            return 0, 0
        
        # Obter dados de tendências
        trend_data = search_fashion_trends(api_key, num_results)
        
        # Obter top palavras-chave
        top_keywords = get_top_fashion_keywords(api_key, num_keywords)
        
        trends_count = 0
        keywords_count = 0
        
        # Inserir dados no MySQL
        if trend_data:
            trends_count = insert_fashion_trends(connection, trend_data)
        
        if top_keywords:
            keywords_count = insert_fashion_keywords(connection, top_keywords)
        
        # Exibir resumo dos resultados
        print("\n=== RESUMO DA PESQUISA DE MODA ===")
        
        if trends_count > 0:
            print(f"✓ {trends_count} tendências de moda armazenadas no MySQL")
        else:
            print("✗ Não foi possível obter tendências de moda")
        
        if keywords_count > 0:
            print(f"✓ {keywords_count} palavras-chave armazenadas no MySQL")
            print("\nTop 10 palavras-chave relacionadas à moda:")
            for i, keyword in enumerate(top_keywords[:10], 1):
                print(f"{i}. {keyword['palavra']} ({keyword['contagem']} ocorrências)")
        else:
            print("✗ Não foi possível obter palavras-chave de moda")
        
        # Fechar a conexão
        connection.close()
        
        return trends_count, keywords_count
    
    except Exception as e:
        print(f"Erro ao processar: {str(e)}")
        return 0, 0

# Função para configurar e executar em intervalos regulares
def schedule_fashion_research(api_key, mysql_config, interval_hours=24):
    """
    Configura a execução da pesquisa de moda em intervalos regulares
    
    Args:
        api_key (str): Sua chave de API da SerpAPI
        mysql_config (dict): Configurações de conexão com o MySQL
        interval_hours (int): Intervalo em horas entre as execuções
        
    Returns:
        None
    """
    import time
    
    print(f"Configurando pesquisa de moda para executar a cada {interval_hours} horas")
    
    try:
        while True:
            # Executar a pesquisa
            print(f"\n[{datetime.now()}] Iniciando pesquisa de moda...")
            run_fashion_research_with_mysql(api_key, mysql_config)
            
            # Aguardar o intervalo
            print(f"\nAguardando {interval_hours} horas até a próxima execução...")
            time.sleep(interval_hours * 3600)  # Converter horas para segundos
    
    except KeyboardInterrupt:
        print("\nPesquisa interrompida pelo usuário.")
    
    except Exception as e:
        print(f"\nErro durante a execução programada: {str(e)}")

if __name__ == "__main__":
    # Configurações MySQL
    mysql_config = {
        'host': '127.0.0.1',  # Altere para o host do seu servidor MySQL
        'user': 'root',       # Altere para seu usuário MySQL
        'password': '1234',  # Altere para sua senha MySQL
        'database': 'fashion_trends_db'  # Nome do banco de dados (deve existir)
    }
    
    # Sua chave de API SerpAPI
    API_KEY = "711715b0dfcaae2c9a68f87fefa693140450e46c7c00f64ac0b3ed9f05a6e6b0"
    
    # Opção 1: Executar uma única vez
    run_fashion_research_with_mysql(API_KEY, mysql_config)
    
    # Opção 2: Executar em intervalos regulares (a cada 24 horas)
    # Descomente a linha abaixo para ativar o agendamento
    # schedule_fashion_research(API_KEY, mysql_config, interval_hours=24)