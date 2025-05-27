import requests
import csv
import os
import re
import mysql.connector
from datetime import datetime
from collections import Counter

# Função para conectar ao MySQL
def connect_to_mysql(host, user, password, database):
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

        # NOVA TABELA PARA PEOPLE ALSO ASK
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS fashion_related_questions (
            id INT AUTO_INCREMENT PRIMARY KEY,
            question TEXT,
            answer TEXT,
            source VARCHAR(255),
            link TEXT,
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
            title = trend.get("título", "")
            link = trend.get("link", "")
            snippet = trend.get("snippet", "")
            source_date = trend.get("data", "")
            source = trend.get("fonte", "")

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
            keyword = keyword_data.get("palavra", "")
            keyword_count = keyword_data.get("contagem", 0)
            ranking = i + 1

            cursor.execute(query, (keyword, keyword_count, ranking, collection_date))
            count += 1

        connection.commit()
        print(f"{count} palavras-chave inseridas no MySQL com sucesso!")
        return count

    except mysql.connector.Error as err:
        print(f"Erro ao inserir palavras-chave no MySQL: {err}")
        return 0

# NOVA FUNÇÃO PARA INSERIR RELATED QUESTIONS
def insert_related_questions(connection, related_questions):
    try:
        cursor = connection.cursor()

        query = """
        INSERT INTO fashion_related_questions 
        (question, answer, source, link, collection_date) 
        VALUES (%s, %s, %s, %s, %s)
        """

        collection_date = datetime.now()
        count = 0

        for item in related_questions:
            question = item.get("pergunta", "")
            answer = item.get("resposta", "")
            source = item.get("fonte", "")
            link = item.get("link", "")

            cursor.execute(query, (question, answer, source, link, collection_date))
            count += 1

        connection.commit()
        print(f"{count} perguntas relacionadas inseridas no MySQL com sucesso!")
        return count

    except mysql.connector.Error as err:
        print(f"Erro ao inserir perguntas relacionadas no MySQL: {err}")
        return 0

# Função para buscar tendências e perguntas relacionadas
def search_fashion_trends(api_key, num_results=30):
    base_url = "https://serpapi.com/search"

    params = {
        "q": "tendências de moda 2025",
        "api_key": api_key,
        "engine": "google",
        "gl": "BR",
        "hl": "pt",
        "num": num_results
    }

    print("Buscando tendências de moda...")
    response = requests.get(base_url, params=params)

    if response.status_code != 200:
        print(f"Erro na requisição: {response.status_code}")
        return [], []

    data = response.json()

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

    # NOVO BLOCO: EXTRAIR RELATED QUESTIONS
    related_questions = []
    if "related_questions" in data:
        for item in data["related_questions"]:
            question = item.get("question", "")
            answer = item.get("answer", "")
            source = item.get("source", {}).get("name", "")
            link = item.get("source", {}).get("link", "")
            related_questions.append({
                "pergunta": question,
                "resposta": answer,
                "fonte": source,
                "link": link
            })

    return results, related_questions

def get_top_fashion_keywords(api_key, num_keywords=10):
    print("Buscando top palavras-chave de moda...")

    params_trends = {
        "q": "fashion trends",
        "api_key": api_key,
        "engine": "google_autocomplete"
    }

    response_trends = requests.get("https://serpapi.com/search", params=params_trends)

    params_fashion = {
        "q": "fashion",
        "api_key": api_key,
        "engine": "google_autocomplete"
    }

    response_fashion = requests.get("https://serpapi.com/search", params=params_fashion)

    params_2025 = {
        "q": "fashion 2025",
        "api_key": api_key,
        "engine": "google_autocomplete"
    }

    response_2025 = requests.get("https://serpapi.com/search", params=params_2025)

    keywords = []

    if response_trends.status_code == 200:
        data = response_trends.json()
        if "suggestions" in data:
            for suggestion in data["suggestions"]:
                keywords.append(suggestion.get("value", ""))

    if response_fashion.status_code == 200:
        data = response_fashion.json()
        if "suggestions" in data:
            for suggestion in data["suggestions"]:
                keywords.append(suggestion.get("value", ""))

    if response_2025.status_code == 200:
        data = response_2025.json()
        if "suggestions" in data:
            for suggestion in data["suggestions"]:
                keywords.append(suggestion.get("value", ""))

    all_words = []
    for keyword in keywords:
        words = re.findall(r'\b[a-zA-Z]{3,}\b', keyword.lower())
        stopwords = ["the", "and", "for", "with", "what", "how", "are", "can", "from", "fashion"]
        filtered_words = [word for word in words if word not in stopwords]
        all_words.extend(filtered_words)

    word_counts = Counter(all_words)
    top_words = word_counts.most_common(num_keywords)

    top_keywords = [{"palavra": word, "contagem": count} for word, count in top_words]

    return top_keywords

# Função principal com integração MySQL
def run_fashion_research_with_mysql(api_key, mysql_config, num_results=30, num_keywords=10):
    try:
        connection = connect_to_mysql(
            mysql_config['host'],
            mysql_config['user'],
            mysql_config['password'],
            mysql_config['database']
        )

        if not connection:
            print("Não foi possível conectar ao MySQL. Encerrando.")
            return 0, 0, 0

        if not create_mysql_tables(connection):
            print("Erro ao criar tabelas. Encerrando.")
            connection.close()
            return 0, 0, 0

        trend_data, related_questions = search_fashion_trends(api_key, num_results)

        top_keywords = get_top_fashion_keywords(api_key, num_keywords)

        trends_count = insert_fashion_trends(connection, trend_data) if trend_data else 0
        keywords_count = insert_fashion_keywords(connection, top_keywords) if top_keywords else 0
        related_count = insert_related_questions(connection, related_questions) if related_questions else 0

        print("\n=== RESUMO DA PESQUISA DE MODA ===")
        print(f"✓ {trends_count} tendências de moda armazenadas no MySQL" if trends_count else "✗ Não foi possível obter tendências de moda")
        print(f"✓ {keywords_count} palavras-chave armazenadas no MySQL" if keywords_count else "✗ Não foi possível obter palavras-chave de moda")
        print(f"✓ {related_count} perguntas relacionadas armazenadas no MySQL" if related_count else "✗ Não foi possível obter perguntas relacionadas")

        if top_keywords:
            print("\nTop 10 palavras-chave relacionadas à moda:")
            for i, keyword in enumerate(top_keywords[:10], 1):
                print(f"{i}. {keyword['palavra']} ({keyword['contagem']} ocorrências)")

        connection.close()
        return trends_count, keywords_count, related_count

    except Exception as e:
        print(f"Erro ao processar: {str(e)}")
        return 0, 0, 0

def schedule_fashion_research(api_key, mysql_config, interval_hours=24):
    import time

    print(f"Configurando pesquisa de moda para executar a cada {interval_hours} horas")

    try:
        while True:
            print(f"\n[{datetime.now()}] Iniciando pesquisa de moda...")
            run_fashion_research_with_mysql(api_key, mysql_config)
            print(f"\nAguardando {interval_hours} horas até a próxima execução...")
            time.sleep(interval_hours * 3600)

    except KeyboardInterrupt:
        print("\nPesquisa interrompida pelo usuário.")

    except Exception as e:
        print(f"\nErro durante a execução programada: {str(e)}")

if __name__ == "__main__":
    mysql_config = {
        'host': '127.0.0.1',
        'user': 'root',
        'password': '1234',
        'database': 'fashion_trends_db'
    }

    API_KEY = "7124cfaaf1b0105232240cd0bf15a648185be8035f3a4747d7f1a31730e202a0"

    run_fashion_research_with_mysql(API_KEY, mysql_config)
    # schedule_fashion_research(API_KEY, mysql_config, interval_hours=24)

    #
