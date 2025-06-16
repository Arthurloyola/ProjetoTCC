import requests
import json
from datetime import datetime
from collections import Counter
import mysql.connector
from mysql.connector import Error
import time

class FashionTrendsAnalyzer:
    def __init__(self, api_key, mysql_config):
        self.api_key = api_key
        self.mysql_config = mysql_config
        self.base_url = "https://serpapi.com/search"
        
        # Lista focada de palavras-chave
        self.fashion_keywords = [
            "moda feminina 2025",
            "moda masculina 2025", 
            "vestido tendência",
            "calça jeans moda",
            "tênis em alta",
            "bolsa feminina",
            "moda verão 2025",
            "look do dia",
            "outfit tendência",
            "roupa casual"
        ]
        
        self.setup_database()
        
    def setup_database(self):
        try:
            connection = mysql.connector.connect(**self.mysql_config)
            cursor = connection.cursor()
            
            create_analysis_table = """
            CREATE TABLE IF NOT EXISTS fashion_trends_analysis (
                id INT AUTO_INCREMENT PRIMARY KEY,
                analysis_date DATETIME NOT NULL,
                total_keywords_analyzed INT,
                average_score DECIMAL(5,2),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
            
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
                INDEX idx_popularity_score (popularity_score)
            )
            """
            
            create_indicators_table = """
            CREATE TABLE IF NOT EXISTS trend_indicators (
                id INT AUTO_INCREMENT PRIMARY KEY,
                analysis_id INT,
                indicator VARCHAR(100) NOT NULL,
                frequency INT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (analysis_id) REFERENCES fashion_trends_analysis(id) ON DELETE CASCADE
            )
            """
            
            cursor.execute(create_analysis_table)
            cursor.execute(create_keywords_table)
            cursor.execute(create_indicators_table)
            
            connection.commit()
            print("✅ Banco de dados configurado")
            
        except Error as e:
            print(f"❌ Erro no banco: {e}")
        finally:
            if connection.is_connected():
                cursor.close()
                connection.close()
    
    def get_google_search_data(self, keyword):
        params = {
            "engine": "google",
            "q": f"{keyword} site:br OR inurl:br",
            "gl": "br",
            "hl": "pt",
            "num": 5,
            "api_key": self.api_key
        }
        
        try:
            response = requests.get(self.base_url, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Erro API para '{keyword}': {e}")
            return None
    
    def analyze_search_results(self, search_data, keyword):
        if not search_data:
            return {
                "popularity_score": 0,
                "result_count": 0,
                "trend_indicators": [],
                "related_searches": []
            }
        
        organic_results = search_data.get("organic_results", [])
        result_count = len(organic_results)
        
        trend_indicators = []
        trend_keywords = ["2024", "2025", "nova", "tendência", "moda", "popular", "em alta", "viral", "trend"]
        
        for result in organic_results:
            title = result.get("title", "").lower()
            snippet = result.get("snippet", "").lower()
            
            for trend_word in trend_keywords:
                if trend_word in title or trend_word in snippet:
                    trend_indicators.append(trend_word)
        
        popularity_score = result_count * 15
        popularity_score += len(trend_indicators) * 8
        
        if "shopping_results" in search_data:
            popularity_score += len(search_data["shopping_results"]) * 5
        
        if "knowledge_graph" in search_data:
            popularity_score += 25
        
        related_searches = []
        if "related_searches" in search_data:
            for search in search_data["related_searches"][:3]:
                related_searches.append(search.get("query", ""))
        
        return {
            "popularity_score": min(popularity_score, 100),
            "result_count": result_count,
            "trend_indicators": list(set(trend_indicators)),
            "related_searches": related_searches
        }
    
    def determine_trend_status(self, analysis):
        score = analysis["popularity_score"]
        indicators = analysis["trend_indicators"]
        
        high_trend_indicators = ["2025", "nova", "tendência", "em alta", "viral", "trend"]
        moderate_trend_indicators = ["2024", "moda", "popular"]
        
        has_high_indicators = any(ind in indicators for ind in high_trend_indicators)
        has_moderate_indicators = any(ind in indicators for ind in moderate_trend_indicators)
        
        if score >= 75 and has_high_indicators:
            return "🔥 Em alta (tendência forte)"
        elif score >= 60 and has_high_indicators:
            return "📈 Crescendo (tendência moderada)"
        elif score >= 70:
            return "⭐ Popular (interesse alto)"
        elif score >= 45 and has_moderate_indicators:
            return "📊 Estável com potencial"
        elif score >= 30:
            return "📊 Estável (interesse moderado)"
        elif score >= 15:
            return "📉 Baixo interesse"
        else:
            return "⚪ Interesse mínimo"
    
    def analyze_fashion_trends(self):
        print("Iniciando análise de tendências...")
        
        results = []
        
        for i, keyword in enumerate(self.fashion_keywords):
            print(f"Processando ({i+1}/{len(self.fashion_keywords)}): {keyword}")
            
            search_data = self.get_google_search_data(keyword)
            
            if search_data:
                analysis = self.analyze_search_results(search_data, keyword)
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
                print(f"  Score: {analysis['popularity_score']}/100")
            
            time.sleep(2.0)
        
        results.sort(key=lambda x: x["popularity_score"], reverse=True)
        return results
    
    def save_to_database(self, results):
        try:
            connection = mysql.connector.connect(**self.mysql_config)
            cursor = connection.cursor()
            
            # Análise principal
            analysis_date = datetime.now()
            total_keywords = len(results)
            avg_score = sum(r['popularity_score'] for r in results) / len(results) if results else 0
            
            insert_analysis = """
            INSERT INTO fashion_trends_analysis (analysis_date, total_keywords_analyzed, average_score)
            VALUES (%s, %s, %s)
            """
            cursor.execute(insert_analysis, (analysis_date, total_keywords, avg_score))
            analysis_id = cursor.lastrowid
            
            # Keywords
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
                
                all_indicators.extend(result['trend_indicators'])
            
            cursor.executemany(insert_keyword, keyword_data)
            
            # Indicadores
            if all_indicators:
                indicator_counts = Counter(all_indicators)
                insert_indicator = """
                INSERT INTO trend_indicators (analysis_id, indicator, frequency)
                VALUES (%s, %s, %s)
                """
                
                indicator_data = [(analysis_id, indicator, count) for indicator, count in indicator_counts.items()]
                cursor.executemany(insert_indicator, indicator_data)
            
            connection.commit()
            print(f"✅ Dados salvos no banco (ID: {analysis_id})")
            
            # Mostra ranking
            print(f"\n📊 RANKING DE POPULARIDADE:")
            for i, result in enumerate(results, 1):
                print(f"{i:2d}. {result['keyword']:<25} - {result['popularity_score']:2d}/100 - {result['trend_status']}")
            
        except Error as e:
            print(f"❌ Erro ao salvar: {e}")
            if connection:
                connection.rollback()
        finally:
            if connection.is_connected():
                cursor.close()
                connection.close()

def main():
    mysql_config = {
        'host': '127.0.0.1',
        'user': 'root',
        'password': '1234',
        'database': 'fashion_trends_db'
    }
    
    API_KEY = "3dc7aeb100a9dfb6f8046cf5dc83707bf09a2f32723a26b9e3ed4ba3d0aa997d"
    
    if not API_KEY:
        print("❌ Erro: Configure sua API key")
        return
    
    analyzer = FashionTrendsAnalyzer(API_KEY, mysql_config)
    
    try:
        results = analyzer.analyze_fashion_trends()
        
        if results:
            analyzer.save_to_database(results)
            print(f"\n✅ Análise concluída! Requisições usadas: {len(results)}")
        else:
            print("❌ Nenhum resultado encontrado")
            
    except Exception as e:
        print(f"❌ Erro: {e}")

if __name__ == "__main__":
    main()