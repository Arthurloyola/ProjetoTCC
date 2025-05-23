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
            title VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
            link TEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
            snippet TEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
            source_date VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
            source VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
            collection_date DATETIME
        )
        """)
        
        # Tabela para palavras-chave
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS fashion_keywords (
            id INT AUTO_INCREMENT PRIMARY KEY,
            keyword VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
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

# Função modificada para buscar especificamente sites brasileiros
def search_fashion_trends(api_key, num_results=30):
    """
    Busca tendências de moda em sites brasileiros usando a SerpAPI
    
    Args:
        api_key (str): Sua chave de API da SerpAPI
        num_results (int): Número de resultados a serem obtidos
        
    Returns:
        list: Lista de dicionários com os dados das tendências
    """
    base_url = "https://serpapi.com/search"
    
    # Sites brasileiros de moda conhecidos
    brazilian_fashion_sites = [
        "site:vogue.globo.com",
        "site:elle.com.br", 
        "site:marieclaire.globo.com",
        "site:glamour.globo.com",
        "site:harpersbazaar.com.br",
        "site:ffwmag.com",
        "site:capricho.abril.com.br",
        "site:blogjuliapepper.com",
        "site:garotasestupidas.com",
        "site:trendstyle.com.br"
    ]
    
    # Múltiplas buscas com termos em português focando em sites brasileiros
    search_queries = [
        "tendências de moda 2025 Brasil",
        "moda feminina tendências brasileira",
        "fashion week São Paulo tendências",
        "moda sustentável Brasil 2025",
        "street style brasileiro moda",
        "marcas brasileiras moda tendências"
    ]
    
    all_results = []
    
    # Busca 1: Sites específicos brasileiros
    for site in brazilian_fashion_sites:
        for base_query in ["tendências moda 2025", "moda feminina 2025"]:
            query = f"{base_query} {site}"
            
            params = {
                "q": query,
                "api_key": api_key,
                "engine": "google",
                "gl": "br",
                "hl": "pt-br",
                "num": 5,
                "cr": "countryBR",  # Restringir ao Brasil
                "lr": "lang_pt"
            }
            
            print(f"Buscando em: {site}")
            response = requests.get(base_url, params=params)
            
            if response.status_code == 200:
                data = response.json()
                all_results.extend(extract_results_from_response(data))
    
    # Busca 2: Termos gerais com filtro para sites .com.br
    for query in search_queries:
        enhanced_query = f'{query} site:.com.br OR site:.br'
        
        params = {
            "q": enhanced_query,
            "api_key": api_key,
            "engine": "google",
            "gl": "br",
            "hl": "pt-br",
            "num": 10,
            "cr": "countryBR",
            "lr": "lang_pt"
        }
        
        print(f"Buscando: {query}")
        response = requests.get(base_url, params=params)
        
        if response.status_code == 200:
            data = response.json()
            all_results.extend(extract_results_from_response(data))
    
    # Busca 3: Google News Brasil para tendências recentes
    news_params = {
        "q": "tendências moda Brasil 2025",
        "api_key": api_key,
        "engine": "google_news",
        "gl": "br",
        "hl": "pt-br",
        "num": 15
    }
    
    print("Buscando notícias de moda brasileira...")
    news_response = requests.get(base_url, params=news_params)
    
    if news_response.status_code == 200:
        news_data = news_response.json()
        if "news_results" in news_data:
            for item in news_data["news_results"]:
                trend_data = {
                    "título": item.get("title", ""),
                    "link": item.get("link", ""),
                    "snippet": item.get("snippet", ""),
                    "data": item.get("date", ""),
                    "fonte": item.get("source", "")
                }
                all_results.append(trend_data)
    
    # Filtrar apenas sites brasileiros
    brazilian_results = filter_brazilian_sites(all_results)
    
    # Remover duplicatas baseado no título
    unique_results = []
    seen_titles = set()
    
    for result in brazilian_results:
        title_clean = result["título"].lower().strip()
        if title_clean not in seen_titles and title_clean and len(title_clean) > 10:
            seen_titles.add(title_clean)
            unique_results.append(result)
    
    return unique_results[:num_results]

def extract_results_from_response(data):
    """
    Extrai resultados de uma resposta da API
    """
    results = []
    
    # Extrair resultados orgânicos
    if "organic_results" in data:
        for item in data["organic_results"]:
            trend_data = {
                "título": item.get("title", ""),
                "link": item.get("link", ""),
                "snippet": item.get("snippet", ""),
                "data": item.get("date", ""),
                "fonte": extract_domain(item.get("link", ""))
            }
            results.append(trend_data)
    
    # Extrair resultados de notícias
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

def filter_brazilian_sites(results):
    """
    Filtra resultados para manter apenas sites brasileiros
    """
    # Domínios brasileiros conhecidos
    brazilian_domains = [
        '.com.br', '.br', 'globo.com', 'abril.com.br', 
        'vogue.globo.com', 'elle.com.br', 'marieclaire.globo.com',
        'glamour.globo.com', 'harpersbazaar.com.br', 'ffwmag.com',
        'capricho.abril.com.br', 'uol.com.br', 'g1.globo.com',
        'folha.uol.com.br', 'estadao.com.br', 'ig.com.br',
        'r7.com', 'terra.com.br', 'yahoo.com.br'
    ]
    
    # Sites de moda brasileiros específicos
    fashion_sites_br = [
        'blogjuliapepper.com', 'garotasestupidas.com', 'trendstyle.com.br',
        'fashionbubbles.com', 'justlia.com.br', 'blogdamariah.com.br',
        'vilamulher.com.br', 'delas.ig.com.br', 'mdemulher.abril.com.br'
    ]
    
    filtered_results = []
    
    for result in results:
        link = result.get("link", "").lower()
        fonte = result.get("fonte", "").lower()
        
        # Verificar se é um site brasileiro
        is_brazilian = False
        
        # Verificar domínios brasileiros
        for domain in brazilian_domains:
            if domain in link or domain in fonte:
                is_brazilian = True
                break
        
        # Verificar sites específicos de moda brasileira
        for site in fashion_sites_br:
            if site in link or site in fonte:
                is_brazilian = True
                break
        
        if is_brazilian:
            filtered_results.append(result)
    
    return filtered_results

def extract_domain(url):
    """
    Extrai o domínio de uma URL
    """
    try:
        import urllib.parse
        parsed = urllib.parse.urlparse(url)
        return parsed.netloc
    except:
        return ""

# Função modificada para palavras-chave de sites brasileiros
def get_top_fashion_keywords(api_key, num_keywords=10):
    """
    Obtém as palavras-chave mais pesquisadas relacionadas à moda brasileira
    usando a API de sugestões de pesquisa do Google via SerpAPI
    
    Args:
        api_key (str): Sua chave de API da SerpAPI
        num_keywords (int): Número de palavras-chave a retornar
        
    Returns:
        list: Lista das top palavras-chave mais pesquisadas
    """
    print("Buscando top palavras-chave de moda brasileira...")
    
    # Termos base focados no Brasil
    base_terms = [
        "moda brasileira tendências",
        "fashion week são paulo",
        "marcas brasileiras moda", 
        "estilistas brasileiros",
        "moda sustentável brasil",
        "street style brasileiro",
        "moda praia brasileira",
        "moda festa junina",
        "carnaval moda brasil"
    ]
    
    all_keywords = []
    
    for term in base_terms:
        # Buscar sugestões com foco no Brasil
        params = {
            "q": term,
            "api_key": api_key,
            "engine": "google_autocomplete",
            "gl": "br",
            "hl": "pt-br"
        }
        
        response = requests.get("https://serpapi.com/search", params=params)
        
        if response.status_code == 200:
            data = response.json()
            if "suggestions" in data:
                for suggestion in data["suggestions"]:
                    keyword = suggestion.get("value", "")
                    if keyword and any(word in keyword.lower() for word in ["brasil", "brasileiro", "brasileira", "são paulo", "rio", "moda"]):
                        all_keywords.append(keyword)
    
    # Buscar também termos relacionados em sites brasileiros
    site_specific_terms = [
        "vogue brasil moda",
        "elle brasil tendências",
        "marie claire brasil fashion"
    ]
    
    for term in site_specific_terms:
        params = {
            "q": term,
            "api_key": api_key,
            "engine": "google_autocomplete",
            "gl": "br",
            "hl": "pt-br"
        }
        
        response = requests.get("https://serpapi.com/search", params=params)
        
        if response.status_code == 200:
            data = response.json()
            if "suggestions" in data:
                for suggestion in data["suggestions"]:
                    keyword = suggestion.get("value", "")
                    if keyword:
                        all_keywords.append(keyword)
    
    # Processar e extrair palavras significativas em português
    all_words = []
    for keyword in all_keywords:
        # Limpar e extrair palavras significativas
        words = re.findall(r'\b[a-zA-ZÀ-ÿ]{3,}\b', keyword.lower())
        # Filtrar palavras muito comuns em português
        stopwords = [
            "para", "com", "que", "uma", "das", "dos", "como", "onde", 
            "quando", "qual", "quais", "são", "tem", "ter", "mais", 
            "muito", "pela", "pelo", "por", "anos", "ano", "brasil",
            "the", "and", "for", "with", "what", "how", "moda", "fashion"
        ]
        filtered_words = [word for word in words if word not in stopwords and len(word) > 3]
        all_words.extend(filtered_words)
    
    # Contar e obter as top palavras-chave
    word_counts = Counter(all_words)
    top_words = word_counts.most_common(num_keywords)
    
    # Formatar os resultados
    top_keywords = [{"palavra": word, "contagem": count} for word, count in top_words]
    
    return top_keywords

# Função principal com integração MySQL (mantida igual)
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
            print("\nPrimeiras 5 tendências encontradas:")
            for i, trend in enumerate(trend_data[:5], 1):
                print(f"{i}. {trend['título']}")
                print(f"   Fonte: {trend['fonte']}")
                print(f"   Link: {trend['link'][:60]}...")
                print("")
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

# Função para configurar e executar em intervalos regulares (mantida igual)
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
    #schedule_fashion_research(API_KEY, mysql_config, interval_hours=24)