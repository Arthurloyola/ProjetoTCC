import requests
import json
from datetime import datetime, timedelta
import time
import os
from collections import Counter
import re
import mysql.connector
from mysql.connector import Error

class FashionTrendsAnalyzer:
    def __init__(self, api_key, mysql_config):
        """
        Inicializa o analisador de tendências de moda
        
        Args:
            api_key (str): Sua chave da API do SerpApi
            mysql_config (dict): Configurações do banco MySQL
        """
        self.api_key = api_key
        self.mysql_config = mysql_config
        self.base_url = "https://serpapi.com/search"
        self.fashion_keywords = [
            "moda feminina", "moda masculina", "roupa feminina", "roupa masculina",
            "vestido", "calça jeans", "blusa", "camisa", "tênis", "sapato",
            "bolsa", "acessórios", "moda praia", "lingerie", "moda infantil",
            "jaqueta", "casaco", "shorts", "saia", "body", "cropped",
            "moletom", "camiseta", "regata", "blazer", "calçado feminino",
            "sandália", "bota", "sneaker", "moda verão", "moda inverno"
        ]
        
        # Inicializa a conexão com o banco
        self.setup_database()
        
    def setup_database(self):
        """
        Configura o banco de dados e cria as tabelas necessárias
        """
        try:
            connection = mysql.connector.connect(**self.mysql_config)
            cursor = connection.cursor()
            
            # Cria a tabela principal de análises
            create_analysis_table = """
            CREATE TABLE IF NOT EXISTS fashion_trends_analysis (
                id INT AUTO_INCREMENT PRIMARY KEY,
                analysis_date DATETIME NOT NULL,
                total_keywords_analyzed INT,
                average_score DECIMAL(5,2),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
            
            # Cria a tabela de palavras-chave e resultados
            create_keywords_table = """
            CREATE TABLE IF NOT EXISTS fashion_keywords_results (
                id INT AUTO_INCREMENT PRIMARY KEY,
                analysis_id INT,
                keyword VARCHAR(255) NOT NULL,
                popularity_score INT NOT NULL,
                result_count INT,
                trend_status VARCHAR(100),
                trend_indicators TEXT,
                related_searches TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (analysis_id) REFERENCES fashion_trends_analysis(id) ON DELETE CASCADE,
                INDEX idx_keyword (keyword),
                INDEX idx_popularity_score (popularity_score),
                INDEX idx_analysis_date (created_at)
            )
            """
            
            # Cria a tabela de indicadores de tendência
            create_indicators_table = """
            CREATE TABLE IF NOT EXISTS trend_indicators (
                id INT AUTO_INCREMENT PRIMARY KEY,
                analysis_id INT,
                indicator VARCHAR(100) NOT NULL,
                frequency INT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (analysis_id) REFERENCES fashion_trends_analysis(id) ON DELETE CASCADE,
                INDEX idx_indicator (indicator)
            )
            """
            
            cursor.execute(create_analysis_table)
            cursor.execute(create_keywords_table)
            cursor.execute(create_indicators_table)
            
            connection.commit()
            print("✅ Banco de dados configurado com sucesso!")
            
        except Error as e:
            print(f"❌ Erro ao configurar banco de dados: {e}")
        finally:
            if connection.is_connected():
                cursor.close()
                connection.close()
    
    def get_google_search_data(self, keyword, num_results=10):
        """
        Coleta dados de pesquisa do Google via SerpApi
        
        Args:
            keyword (str): Palavra-chave para pesquisar
            num_results (int): Número de resultados para analisar
        
        Returns:
            dict: Dados de pesquisa da palavra-chave
        """
        params = {
            "engine": "google",
            "q": f"{keyword} site:br OR inurl:br OR intitle:brasil",
            "gl": "br",  # Brasil
            "hl": "pt",  # Português
            "num": num_results,
            "api_key": self.api_key
        }
        
        try:
            response = requests.get(self.base_url, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Erro ao buscar dados para '{keyword}': {e}")
            return None
            
    def get_search_suggestions(self, base_keyword):
        """
        Obtém sugestões de pesquisa relacionadas
        
        Args:
            base_keyword (str): Palavra-chave base
            
        Returns:
            list: Lista de sugestões
        """
        params = {
            "engine": "google_autocomplete",
            "q": base_keyword,
            "gl": "br",  # Brasil
            "hl": "pt",  # Português
            "api_key": self.api_key
        }
        
        try:
            response = requests.get(self.base_url, params=params)
            response.raise_for_status()
            data = response.json()
            
            suggestions = []
            if "suggestions" in data:
                for suggestion in data["suggestions"]:
                    suggestions.append(suggestion.get("value", ""))
            
            return suggestions
        except requests.exceptions.RequestException as e:
            print(f"Erro ao buscar sugestões para '{base_keyword}': {e}")
            return []
    
    def get_related_searches(self, keyword):
        """
        Obtém pesquisas relacionadas do Google
        
        Args:
            keyword (str): Palavra-chave principal
            
        Returns:
            list: Lista de pesquisas relacionadas
        """
        params = {
            "engine": "google",
            "q": keyword,
            "gl": "br",
            "hl": "pt",
            "api_key": self.api_key
        }
        
        try:
            response = requests.get(self.base_url, params=params)
            response.raise_for_status()
            data = response.json()
            
            related_searches = []
            if "related_searches" in data:
                for search in data["related_searches"]:
                    related_searches.append(search.get("query", ""))
            
            return related_searches
        except requests.exceptions.RequestException as e:
            print(f"Erro ao buscar pesquisas relacionadas para '{keyword}': {e}")
            return []
    
    def analyze_search_results(self, search_data, keyword):
        """
        Analisa os resultados de pesquisa para determinar popularidade
        
        Args:
            search_data (dict): Dados de pesquisa do Google
            keyword (str): Palavra-chave analisada
            
        Returns:
            dict: Análise da popularidade
        """
        if not search_data:
            return {
                "popularity_score": 0,
                "result_count": 0,
                "trend_indicators": [],
                "related_searches": []
            }
        
        # Conta resultados orgânicos
        organic_results = search_data.get("organic_results", [])
        result_count = len(organic_results)
        
        # Analisa títulos e snippets para indicadores de tendência
        trend_indicators = []
        trend_keywords = ["2024", "2025", "nova", "tendência", "moda", "popular", "em alta", "viral"]
        
        for result in organic_results:
            title = result.get("title", "").lower()
            snippet = result.get("snippet", "").lower()
            
            for trend_word in trend_keywords:
                if trend_word in title or trend_word in snippet:
                    trend_indicators.append(trend_word)
        
        # Calcula score de popularidade baseado em múltiplos fatores
        popularity_score = result_count * 10  # Base score
        
        # Bonus por indicadores de tendência
        popularity_score += len(trend_indicators) * 5
        
        # Verifica se há shopping results (indica interesse comercial)
        if "shopping_results" in search_data:
            popularity_score += len(search_data["shopping_results"]) * 3
        
        # Verifica knowledge graph (indica alta relevância)
        if "knowledge_graph" in search_data:
            popularity_score += 20
        
        # Pega pesquisas relacionadas
        related_searches = []
        if "related_searches" in search_data:
            for search in search_data["related_searches"]:
                related_searches.append(search.get("query", ""))
        
        return {
            "popularity_score": min(popularity_score, 100),  # Cap em 100
            "result_count": result_count,
            "trend_indicators": list(set(trend_indicators)),
            "related_searches": related_searches[:5]
        }
    
    def collect_expanded_keywords(self):
        """
        Expande a lista de palavras-chave usando autocomplete
        
        Returns:
            list: Lista expandida de palavras-chave
        """
        expanded_keywords = self.fashion_keywords.copy()
        
        print("Coletando sugestões de palavras-chave...")
        base_terms = ["moda", "roupa", "vestido", "calça", "tênis"]
        
        for base_term in base_terms:
            print(f"Buscando sugestões para: {base_term}")
            suggestions = self.get_search_suggestions(base_term)
            
            for suggestion in suggestions:
                if suggestion and suggestion not in expanded_keywords:
                    # Filtra sugestões relevantes para moda
                    fashion_terms = [
                        "moda", "roupa", "vestido", "calça", "blusa", "sapato", 
                        "tênis", "bolsa", "acessório", "shorts", "saia", "casaco"
                    ]
                    if any(term in suggestion.lower() for term in fashion_terms):
                        expanded_keywords.append(suggestion)
            
            time.sleep(1)  # Rate limiting
        
        return expanded_keywords[:50]  # Limita para não exceder quotas
    
    def analyze_fashion_trends(self):
        """
        Executa a análise completa das tendências de moda
        
        Returns:
            list: Lista de análises ordenada por popularidade
        """
        print("Iniciando análise de tendências de moda no Brasil...")
        
        # Coleta palavras-chave expandidas
        all_keywords = self.collect_expanded_keywords()
        print(f"Analisando {len(all_keywords)} palavras-chave...")
        
        results = []
        
        for i, keyword in enumerate(all_keywords):
            print(f"Processando ({i+1}/{len(all_keywords)}): {keyword}")
            
            # Coleta dados de pesquisa
            search_data = self.get_google_search_data(keyword)
            
            if search_data:
                analysis = self.analyze_search_results(search_data, keyword)
                
                # Determina status da tendência baseado nos indicadores
                trend_status = self.determine_trend_status(analysis)
                
                result = {
                    "keyword": keyword,
                    "popularity_score": analysis["popularity_score"],
                    "result_count": analysis["result_count"],
                    "trend_status": trend_status,
                    "trend_indicators": analysis["trend_indicators"],
                    "related_searches": analysis["related_searches"]
                }
                
                results.append(result)
                print(f"  Score: {analysis['popularity_score']}, Status: {trend_status}")
            else:
                print(f"  Sem dados disponíveis")
            
            # Pausa para respeitar rate limits
            time.sleep(1.5)
        
        # Ordena por score de popularidade
        results.sort(key=lambda x: x["popularity_score"], reverse=True)
        
        return results
    
    def determine_trend_status(self, analysis):
        """
        Determina o status da tendência baseado na análise
        
        Args:
            analysis (dict): Dados da análise
            
        Returns:
            str: Status da tendência
        """
        score = analysis["popularity_score"]
        indicators = analysis["trend_indicators"]
        
        # Verifica indicadores específicos de alta
        high_trend_indicators = ["2025", "nova", "tendência", "em alta", "viral"]
        has_high_indicators = any(ind in indicators for ind in high_trend_indicators)
        
        if score >= 80 and has_high_indicators:
            return "🔥 Em alta (tendência forte)"
        elif score >= 60 and has_high_indicators:
            return "📈 Crescendo (tendência moderada)"
        elif score >= 80:
            return "⭐ Popular (interesse alto)"
        elif score >= 40:
            return "📊 Estável (interesse moderado)"
        elif score >= 20:
            return "📉 Baixo interesse"
        else:
            return "⚪ Interesse mínimo"
    
    def save_to_database(self, results):
        """
        Salva os resultados no banco de dados MySQL
        
        Args:
            results (list): Lista de resultados da análise
        """
        try:
            connection = mysql.connector.connect(**self.mysql_config)
            cursor = connection.cursor()
            
            # Insere registro da análise principal
            analysis_date = datetime.now()
            total_keywords = len(results)
            avg_score = sum(r['popularity_score'] for r in results) / len(results) if results else 0
            
            insert_analysis = """
            INSERT INTO fashion_trends_analysis (analysis_date, total_keywords_analyzed, average_score)
            VALUES (%s, %s, %s)
            """
            cursor.execute(insert_analysis, (analysis_date, total_keywords, avg_score))
            analysis_id = cursor.lastrowid
            
            print(f"✅ Análise principal salva com ID: {analysis_id}")
            
            # Insere os resultados das palavras-chave
            insert_keyword = """
            INSERT INTO fashion_keywords_results 
            (analysis_id, keyword, popularity_score, result_count, trend_status, trend_indicators, related_searches)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            
            keyword_data = []
            all_indicators = []
            
            for result in results:
                trend_indicators_str = json.dumps(result['trend_indicators']) if result['trend_indicators'] else None
                related_searches_str = json.dumps(result['related_searches']) if result['related_searches'] else None
                
                keyword_data.append((
                    analysis_id,
                    result['keyword'],
                    result['popularity_score'],
                    result['result_count'],
                    result['trend_status'],
                    trend_indicators_str,
                    related_searches_str
                ))
                
                # Coleta indicadores para estatísticas
                all_indicators.extend(result['trend_indicators'])
            
            cursor.executemany(insert_keyword, keyword_data)
            print(f"✅ {len(keyword_data)} palavras-chave salvas no banco")
            
            # Salva estatísticas dos indicadores de tendência
            if all_indicators:
                indicator_counts = Counter(all_indicators)
                insert_indicator = """
                INSERT INTO trend_indicators (analysis_id, indicator, frequency)
                VALUES (%s, %s, %s)
                """
                
                indicator_data = [(analysis_id, indicator, count) for indicator, count in indicator_counts.items()]
                cursor.executemany(insert_indicator, indicator_data)
                print(f"✅ {len(indicator_data)} indicadores de tendência salvos")
            
            connection.commit()
            print(f"✅ Todos os dados salvos com sucesso no banco de dados!")
            
            # Exibe estatísticas da análise salva
            self.display_database_summary(analysis_id, connection)
            
        except Error as e:
            print(f"❌ Erro ao salvar no banco de dados: {e}")
            if connection:
                connection.rollback()
        finally:
            if connection.is_connected():
                cursor.close()
                connection.close()
    
    def display_database_summary(self, analysis_id, connection):
        """
        Exibe um resumo dos dados salvos no banco
        
        Args:
            analysis_id (int): ID da análise
            connection: Conexão ativa com o banco
        """
        try:
            cursor = connection.cursor()
            
            # Busca top 10 palavras
            cursor.execute("""
                SELECT keyword, popularity_score, trend_status 
                FROM fashion_keywords_results 
                WHERE analysis_id = %s 
                ORDER BY popularity_score DESC 
                LIMIT 10
            """, (analysis_id,))
            
            top_keywords = cursor.fetchall()
            
            print(f"\n{'='*60}")
            print("📊 RESUMO DOS DADOS SALVOS NO BANCO")
            print(f"{'='*60}")
            print(f"ID da Análise: {analysis_id}")
            print(f"Data: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
            
            print("\n🏆 TOP 10 PALAVRAS MAIS POPULARES:")
            for i, (keyword, score, status) in enumerate(top_keywords, 1):
                print(f"{i:2d}. {keyword} - Score: {score}/100 - {status}")
            
            # Conta por categoria
            cursor.execute("""
                SELECT 
                    CASE 
                        WHEN trend_status LIKE '%Em alta%' THEN 'Em Alta'
                        WHEN trend_status LIKE '%Crescendo%' THEN 'Crescendo'
                        WHEN trend_status LIKE '%Popular%' THEN 'Popular'
                        WHEN trend_status LIKE '%Estável%' THEN 'Estável'
                        WHEN trend_status LIKE '%Baixo%' THEN 'Baixo Interesse'
                        ELSE 'Interesse Mínimo'
                    END as categoria,
                    COUNT(*) as quantidade
                FROM fashion_keywords_results 
                WHERE analysis_id = %s 
                GROUP BY categoria
                ORDER BY quantidade DESC
            """, (analysis_id,))
            
            categories = cursor.fetchall()
            
            print("\n📈 DISTRIBUIÇÃO POR CATEGORIA:")
            for categoria, quantidade in categories:
                print(f"   {categoria}: {quantidade} palavras")
            
        except Error as e:
            print(f"❌ Erro ao exibir resumo: {e}")
    
    def get_latest_analysis_summary(self):
        """
        Recupera e exibe um resumo da análise mais recente
        """
        try:
            connection = mysql.connector.connect(**self.mysql_config)
            cursor = connection.cursor()
            
            # Busca a análise mais recente
            cursor.execute("""
                SELECT id, analysis_date, total_keywords_analyzed, average_score
                FROM fashion_trends_analysis 
                ORDER BY analysis_date DESC 
                LIMIT 1
            """)
            
            latest_analysis = cursor.fetchone()
            
            if latest_analysis:
                analysis_id, date, total_keywords, avg_score = latest_analysis
                
                print(f"\n{'='*60}")
                print("📊 ÚLTIMA ANÁLISE NO BANCO DE DADOS")
                print(f"{'='*60}")
                print(f"ID: {analysis_id}")
                print(f"Data: {date.strftime('%d/%m/%Y %H:%M:%S')}")
                print(f"Total de palavras: {total_keywords}")
                print(f"Score médio: {avg_score:.1f}/100")
                
                # Top 5 da última análise
                cursor.execute("""
                    SELECT keyword, popularity_score, trend_status
                    FROM fashion_keywords_results 
                    WHERE analysis_id = %s 
                    ORDER BY popularity_score DESC 
                    LIMIT 5
                """, (analysis_id,))
                
                top_5 = cursor.fetchall()
                print("\n🏆 TOP 5 MAIS POPULARES:")
                for i, (keyword, score, status) in enumerate(top_5, 1):
                    print(f"{i}. {keyword} - {score}/100 - {status}")
            else:
                print("❌ Nenhuma análise encontrada no banco de dados")
                
        except Error as e:
            print(f"❌ Erro ao buscar dados do banco: {e}")
        finally:
            if connection.is_connected():
                cursor.close()
                connection.close()

def main():
    """
    Função principal para executar a análise
    """
    # Configurações do banco MySQL
    mysql_config = {
        'host': '127.0.0.1',
        'user': 'root',
        'password': '1234',
        'database': 'fashion_trends_db'
    }
    
    # IMPORTANTE: Substitua pela sua chave da API do SerpApi
    API_KEY = "64238f6eff431de14ff35f98b2270a953ff75c89d41bf7e08e8bc18d22715eec"
    
    if not API_KEY or API_KEY == "SUA_CHAVE_API_AQUI":
        print("❌ ERRO: Você precisa inserir sua chave da API do SerpApi!")
        print("1. Acesse: https://serpapi.com/")
        print("2. Crie uma conta gratuita")
        print("3. Copie sua API key")
        print("4. Substitua pela sua chave real no código")
        return
    
    # Cria o analisador
    analyzer = FashionTrendsAnalyzer(API_KEY, mysql_config)
    
    # Mostra a última análise se existir
    analyzer.get_latest_analysis_summary()
    
    try:
        # Pergunta se o usuário quer executar nova análise
        response = input("\n🤔 Deseja executar uma nova análise? (s/n): ").lower().strip()
        
        if response in ['s', 'sim', 'y', 'yes']:
            # Executa a análise
            results = analyzer.analyze_fashion_trends()
            
            if results:
                # Salva os resultados no banco
                analyzer.save_to_database(results)
                print(f"\n✅ Análise concluída e salva no banco de dados!")
            else:
                print("❌ Nenhum resultado encontrado. Verifique sua API key e conexão.")
        else:
            print("👋 Análise cancelada. Até logo!")
            
    except Exception as e:
        print(f"❌ Erro durante a execução: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
