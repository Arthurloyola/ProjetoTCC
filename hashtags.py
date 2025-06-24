import requests
import json
import time
import os
from datetime import datetime
from typing import List, Dict, Any
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

class FashionTrendsAnalyzer:
    def __init__(self, serpapi_key: str):
        self.serpapi_key = serpapi_key
        self.base_url = "https://serpapi.com/search"
        self.db_url = 'mysql+pymysql://root:123456789@127.0.0.1/fashion_trends_db'
        self.engine: Engine = create_engine(self.db_url, echo=False, future=True)
        self.request_count = 0
        self.max_requests = 10
        
    def setup_database(self):
        """Cria as tabelas necessárias no banco de dados"""
        with self.engine.begin() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS fashion_hashtags (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    hashtag VARCHAR(255) NOT NULL,
                    platform VARCHAR(50) NOT NULL,
                    search_query VARCHAR(255) NOT NULL,
                    rank_position INT,
                    related_content TEXT,
                    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    INDEX idx_hashtag (hashtag),
                    INDEX idx_platform (platform),
                    INDEX idx_extracted_at (extracted_at)
                )
            """))

            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS social_trends (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    platform VARCHAR(50) NOT NULL,
                    trend_title VARCHAR(500) NOT NULL,
                    trend_description TEXT,
                    source_url VARCHAR(1000),
                    engagement_indicator TEXT,
                    search_query VARCHAR(255) NOT NULL,
                    rank_position INT,
                    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    INDEX idx_platform (platform),
                    INDEX idx_extracted_at (extracted_at)
                )
            """))
        
        print("✅ Tabelas do banco de dados configuradas com sucesso!")
    
    def make_serpapi_request(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Faz requisição para a SerpAPI com controle de limite"""
        if self.request_count >= self.max_requests:
            print(f"⚠️ Limite de {self.max_requests} requisições atingido!")
            return {}
        
        params['api_key'] = self.serpapi_key
        
        try:
            response = requests.get(self.base_url, params=params, timeout=30)
            self.request_count += 1
            print(f"📡 Requisição {self.request_count}/{self.max_requests} - Status: {response.status_code}")
            
            if response.status_code == 200:
                return response.json()
            else:
                print(f"❌ Erro na requisição: {response.status_code}")
                return {}
        except requests.exceptions.RequestException as e:
            print(f"❌ Erro de conexão: {e}")
            return {}
        
        time.sleep(1)
    
    def search_fashion_hashtags(self) -> List[Dict[str, Any]]:
        """Busca hashtags populares de moda"""
        hashtag_data = []
        search_queries = [
            "hashtags moda 2024 populares",
            "trending fashion hashtags instagram",
            "hashtags tendencia moda brasileira"
        ]
        
        for query in search_queries:
            if self.request_count >= self.max_requests:
                break
                
            params = {
                'q': query,
                'engine': 'google',
                'num': 10,
                'hl': 'pt-br',
                'gl': 'br'
            }
            
            results = self.make_serpapi_request(params)
            
            if 'organic_results' in results:
                for i, result in enumerate(results['organic_results'][:5]):
                    content = f"{result.get('title', '')} {result.get('snippet', '')}"
                    hashtags = self.extract_hashtags_from_text(content)
                    
                    for hashtag in hashtags:
                        hashtag_data.append({
                            'hashtag': hashtag,
                            'platform': 'Instagram/TikTok',
                            'search_query': query,
                            'rank_position': i + 1,
                            'related_content': result.get('snippet', '')[:500]
                        })
        
        return hashtag_data

    def search_tiktok_fashion_trends(self) -> List[Dict[str, Any]]:
        trends_data = []
        tiktok_queries = [
            "tendencias moda TikTok 2024",
            "fashion trends TikTok brasil",
            "moda viral TikTok"
        ]
        
        for query in tiktok_queries:
            if self.request_count >= self.max_requests:
                break
                
            params = {
                'q': query,
                'engine': 'google',
                'num': 8,
                'hl': 'pt-br',
                'gl': 'br'
            }
            
            results = self.make_serpapi_request(params)
            
            if 'organic_results' in results:
                for i, result in enumerate(results['organic_results'][:4]):
                    trends_data.append({
                        'platform': 'TikTok',
                        'trend_title': result.get('title', '')[:500],
                        'trend_description': result.get('snippet', '')[:1000],
                        'source_url': result.get('link', ''),
                        'engagement_indicator': self.extract_engagement_indicators(result.get('snippet', '')),
                        'search_query': query,
                        'rank_position': i + 1
                    })
        
        return trends_data

    def search_instagram_fashion_trends(self) -> List[Dict[str, Any]]:
        trends_data = []
        instagram_queries = [
            "tendencias moda Instagram 2024 brasil",
            "fashion influencers Instagram trends"
        ]
        
        for query in instagram_queries:
            if self.request_count >= self.max_requests:
                break
                
            params = {
                'q': query,
                'engine': 'google',
                'num': 8,
                'hl': 'pt-br',
                'gl': 'br'
            }
            
            results = self.make_serpapi_request(params)
            
            if 'organic_results' in results:
                for i, result in enumerate(results['organic_results'][:4]):
                    trends_data.append({
                        'platform': 'Instagram',
                        'trend_title': result.get('title', '')[:500],
                        'trend_description': result.get('snippet', '')[:1000],
                        'source_url': result.get('link', ''),
                        'engagement_indicator': self.extract_engagement_indicators(result.get('snippet', '')),
                        'search_query': query,
                        'rank_position': i + 1
                    })
        
        return trends_data

    def extract_hashtags_from_text(self, text: str) -> List[str]:
        import re
        hashtags = re.findall(r'#\w+', text.lower())
        fashion_keywords = ['moda', 'fashion', 'style', 'outfit', 'look', 'trend', 'ootd', 'style']
        relevant_hashtags = []
        
        for hashtag in set(hashtags):
            if any(keyword in hashtag for keyword in fashion_keywords) or len(hashtag) > 3:
                relevant_hashtags.append(hashtag)
        
        return relevant_hashtags[:10]
    
    def extract_engagement_indicators(self, text: str) -> str:
        import re
        patterns = [
            r'\d+[kKmM]?\s*(?:views|visualizações|curtidas|likes|shares|comentários)',
            r'viral',
            r'trending',
            r'popular',
            r'milhões?\s*de\s*visualizações'
        ]
        
        indicators = []
        for pattern in patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            indicators.extend(matches)
        
        return ', '.join(indicators[:3])

    def save_hashtags_to_db(self, hashtags_data: List[Dict[str, Any]]):
        if not hashtags_data:
            print("⚠️ Nenhum dado de hashtag para salvar")
            return

        with self.engine.begin() as conn:
            insert_query = text("""
                INSERT INTO fashion_hashtags 
                (hashtag, platform, search_query, rank_position, related_content)
                VALUES (:hashtag, :platform, :search_query, :rank_position, :related_content)
            """)
            
            for data in hashtags_data:
                conn.execute(insert_query, data)
        
        print(f"✅ {len(hashtags_data)} hashtags salvos no banco de dados!")

    def save_trends_to_db(self, trends_data: List[Dict[str, Any]]):
        if not trends_data:
            print("⚠️ Nenhum dado de tendência para salvar")
            return

        with self.engine.begin() as conn:
            insert_query = text("""
                INSERT INTO social_trends 
                (platform, trend_title, trend_description, source_url, engagement_indicator, search_query, rank_position)
                VALUES (:platform, :trend_title, :trend_description, :source_url, :engagement_indicator, :search_query, :rank_position)
            """)
            
            for data in trends_data:
                conn.execute(insert_query, data)

        print(f"✅ {len(trends_data)} tendências salvas no banco de dados!")

    def run_analysis(self):
        print("🚀 Iniciando análise de tendências de moda...")
        print(f"📊 Limite de requisições: {self.max_requests}")
        
        self.setup_database()
        
        print("\n📱 Buscando hashtags populares de moda...")
        hashtags_data = self.search_fashion_hashtags()
        self.save_hashtags_to_db(hashtags_data)
        
        print("\n🎵 Buscando tendências de moda no TikTok...")
        tiktok_trends = self.search_tiktok_fashion_trends()
        self.save_trends_to_db(tiktok_trends)
        
        print("\n📸 Buscando tendências de moda no Instagram...")
        instagram_trends = self.search_instagram_fashion_trends()
        self.save_trends_to_db(instagram_trends)
        
        print(f"\n✅ Análise concluída!")
        print(f"📊 Total de requisições utilizadas: {self.request_count}/{self.max_requests}")
        print(f"📱 Hashtags coletados: {len(hashtags_data)}")
        print(f"🎵 Tendências TikTok: {len(tiktok_trends)}")
        print(f"📸 Tendências Instagram: {len(instagram_trends)}")

def main():
    SERPAPI_KEY = "3dc7aeb100a9dfb6f8046cf5dc83707bf09a2f32723a26b9e3ed4ba3d0aa997d"
    
    if SERPAPI_KEY == "SUA_SERPAPI_KEY_AQUI":
        print("❌ Por favor, configure sua chave da SerpAPI na variável SERPAPI_KEY")
        return
    
    analyzer = FashionTrendsAnalyzer(SERPAPI_KEY)
    analyzer.run_analysis()

if __name__ == "__main__":
    main()
