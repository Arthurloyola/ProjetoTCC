import requests
import json
from collections import Counter
import re
import time
import mysql.connector
from datetime import datetime

class OptimizedFashionBrandsExtractor:
    def __init__(self, api_key, db_config):
        self.api_key = api_key
        self.base_url = "https://serpapi.com/search.json"
        self.db_config = db_config
        
        # Lista expandida de marcas para melhor detecção
        self.known_brands = {
            # Marcas internacionais populares
            'zara', 'h&m', 'nike', 'adidas', 'gucci', 'prada', 'chanel', 
            'dior', 'versace', 'armani', 'calvin klein', 'tommy hilfiger',
            'ralph lauren', 'lacoste', 'hugo boss', 'burberry', 'fendi',
            'balenciaga', 'saint laurent', 'givenchy', 'hermès', 'cartier',
            'rolex', 'louis vuitton', 'coach', 'michael kors', 'kate spade',
            'tory burch', 'marc jacobs', 'diesel', 'levi\'s', 'gap',
            'uniqlo', 'forever 21', 'urban outfitters', 'american eagle',
            'hollister', 'abercrombie', 'victoria\'s secret', 'under armour',
            'puma', 'reebok', 'converse', 'vans', 'timberland', 'supreme',
            'off-white', 'stone island', 'moncler', 'canada goose',
            # Marcas brasileiras
            'renner', 'c&a', 'riachuelo', 'marisa', 'lojas americanas',
            'arezzo', 'schutz', 'melissa', 'havaianas', 'osklen',
            'animale', 'farm', 'colcci', 'ellus', 'forum', 'shoulder',
            'damyller', 'richards', 'polo wear', 'dudalina', 'aramis'
        }
        
        # Inicializar banco de dados
        self.init_database()
    
    def init_database(self):
        """Inicializa as tabelas do banco de dados"""
        try:
            connection = mysql.connector.connect(**self.db_config)
            cursor = connection.cursor()
            
            # Criar tabela de análises
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS brand_analysis (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    analysis_date DATETIME NOT NULL,
                    total_mentions INT NOT NULL,
                    total_requests INT NOT NULL,
                    analysis_type VARCHAR(50) NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Criar tabela de marcas
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS brand_rankings (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    analysis_id INT NOT NULL,
                    brand_name VARCHAR(100) NOT NULL,
                    mentions_count INT NOT NULL,
                    ranking_position INT NOT NULL,
                    percentage DECIMAL(5,2) NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (analysis_id) REFERENCES brand_analysis(id) ON DELETE CASCADE
                )
            """)
            
            # Criar tabela de dados brutos (opcional - para auditoria)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS search_results_raw (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    analysis_id INT NOT NULL,
                    search_query VARCHAR(255),
                    result_title TEXT,
                    result_snippet TEXT,
                    result_url VARCHAR(500),
                    brands_found JSON,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (analysis_id) REFERENCES brand_analysis(id) ON DELETE CASCADE
                )
            """)
            
            connection.commit()
            cursor.close()
            connection.close()
            print("✅ Banco de dados inicializado com sucesso!")
            
        except mysql.connector.Error as e:
            print(f"❌ Erro ao inicializar banco de dados: {e}")
    
    def single_comprehensive_search(self, num_results=100):
        """Faz uma única busca abrangente para capturar máximo de marcas"""
        query = "marcas de moda populares 2025 trending fashion brands zara nike adidas h&m"
        
        params = {
            "engine": "google",
            "q": query,
            "api_key": self.api_key,
            "num": num_results,
            "gl": "br",
            "hl": "pt"
        }
        
        try:
            print(f"🔍 Fazendo busca única otimizada...")
            response = requests.get(self.base_url, params=params)
            response.raise_for_status()
            return response.json(), query
        except requests.exceptions.RequestException as e:
            print(f"Erro na requisição: {e}")
            return None, query
    
    def search_google_shopping(self, query="roupas moda", num_results=50):
        """Busca adicional no Google Shopping para capturar mais marcas"""
        params = {
            "engine": "google_shopping",
            "q": query,
            "api_key": self.api_key,
            "num": num_results,
            "gl": "br",
            "hl": "pt"
        }
        
        try:
            print(f"🛍️ Buscando no Google Shopping...")
            response = requests.get(self.base_url, params=params)
            response.raise_for_status()
            return response.json(), query
        except requests.exceptions.RequestException as e:
            print(f"Erro na busca shopping: {e}")
            return None, query
    
    def extract_brands_from_results(self, search_results):
        """Extrai marcas dos resultados de busca de forma otimizada"""
        brands_found = []
        results_data = []
        
        if not search_results:
            return brands_found, results_data
        
        # Extrair de resultados orgânicos
        for result in search_results.get('organic_results', []):
            title = result.get('title', '')
            snippet = result.get('snippet', '')
            url = result.get('link', '')
            
            text_content = f"{title} {snippet}".lower()
            result_brands = self.find_brands_in_text(text_content)
            brands_found.extend(result_brands)
            
            # Guardar dados brutos para o banco
            results_data.append({
                'title': title,
                'snippet': snippet,
                'url': url,
                'brands_found': result_brands
            })
        
        # Extrair de shopping results
        for result in search_results.get('shopping_results', []):
            title = result.get('title', '')
            source = result.get('source', '')
            
            text_content = f"{title} {source}".lower()
            result_brands = self.find_brands_in_text(text_content)
            brands_found.extend(result_brands)
            
            results_data.append({
                'title': title,
                'snippet': f"Shopping: {source}",
                'url': result.get('link', ''),
                'brands_found': result_brands
            })
        
        # Extrair de knowledge graph
        if 'knowledge_graph' in search_results:
            kg = search_results['knowledge_graph']
            title = kg.get('title', '')
            description = kg.get('description', '')
            
            text_content = f"{title} {description}".lower()
            result_brands = self.find_brands_in_text(text_content)
            brands_found.extend(result_brands)
            
            results_data.append({
                'title': f"Knowledge Graph: {title}",
                'snippet': description,
                'url': '',
                'brands_found': result_brands
            })
        
        return brands_found, results_data
    
    def find_brands_in_text(self, text):
        """Encontra marcas conhecidas no texto com regex otimizado"""
        found_brands = []
        text_lower = text.lower()
        
        for brand in self.known_brands:
            pattern = r'\b' + re.escape(brand.lower()) + r'\b'
            matches = re.findall(pattern, text_lower)
            if matches:
                found_brands.extend([brand.title()] * len(matches))
        
        return found_brands
    
    def save_to_database(self, top_brands, total_mentions, total_requests, analysis_type, raw_data=None):
        """Salva os resultados no banco de dados MySQL"""
        try:
            connection = mysql.connector.connect(**self.db_config)
            cursor = connection.cursor()
            
            # 1. Inserir análise principal
            analysis_query = """
                INSERT INTO brand_analysis (analysis_date, total_mentions, total_requests, analysis_type)
                VALUES (%s, %s, %s, %s)
            """
            analysis_data = (datetime.now(), total_mentions, total_requests, analysis_type)
            cursor.execute(analysis_query, analysis_data)
            analysis_id = cursor.lastrowid
            
            # 2. Inserir rankings das marcas
            ranking_query = """
                INSERT INTO brand_rankings (analysis_id, brand_name, mentions_count, ranking_position, percentage)
                VALUES (%s, %s, %s, %s, %s)
            """
            
            ranking_data = []
            for position, (brand, count) in enumerate(top_brands, 1):
                percentage = (count / total_mentions) * 100 if total_mentions > 0 else 0
                ranking_data.append((analysis_id, brand, count, position, percentage))
            
            cursor.executemany(ranking_query, ranking_data)
            
            # 3. Inserir dados brutos (se fornecidos)
            if raw_data:
                raw_query = """
                    INSERT INTO search_results_raw (analysis_id, search_query, result_title, result_snippet, result_url, brands_found)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """
                
                for query, results in raw_data.items():
                    for result in results:
                        brands_json = json.dumps(result['brands_found'])
                        raw_data_tuple = (
                            analysis_id, 
                            query,
                            result['title'][:500],  # Limitar tamanho
                            result['snippet'][:1000],  # Limitar tamanho
                            result['url'][:500],  # Limitar tamanho
                            brands_json
                        )
                        cursor.execute(raw_query, raw_data_tuple)
            
            connection.commit()
            cursor.close()
            connection.close()
            
            print(f"✅ Dados salvos no banco! ID da análise: {analysis_id}")
            return analysis_id
            
        except mysql.connector.Error as e:
            print(f"❌ Erro ao salvar no banco de dados: {e}")
            return None
    
    def get_top_fashion_brands_optimized(self, use_shopping=True, top_n=20):
        """Função principal otimizada - máximo 2 requisições com salvamento no BD"""
        all_brands = []
        total_requests = 0
        raw_data = {}
        
        print("🚀 Iniciando análise otimizada de marcas de moda...")
        
        # Requisição 1: Busca principal otimizada
        main_results, main_query = self.single_comprehensive_search(num_results=100)
        total_requests += 1
        
        if main_results:
            main_brands, main_raw = self.extract_brands_from_results(main_results)
            all_brands.extend(main_brands)
            raw_data[main_query] = main_raw
            print(f"✅ Busca principal: {len(main_brands)} menções encontradas")
        
        # Requisição 2 (opcional): Google Shopping
        if use_shopping:
            shopping_results, shopping_query = self.search_google_shopping("moda feminina masculina", 50)
            total_requests += 1
            
            if shopping_results:
                shopping_brands, shopping_raw = self.extract_brands_from_results(shopping_results)
                all_brands.extend(shopping_brands)
                raw_data[shopping_query] = shopping_raw
                print(f"✅ Google Shopping: {len(shopping_brands)} menções encontradas")
        
        # Analisar frequência
        brand_counter = Counter(all_brands)
        top_brands = brand_counter.most_common(top_n)
        
        # Determinar tipo de análise
        analysis_type = "Balanceada (Web + Shopping)" if use_shopping else "Rápida (Web apenas)"
        
        # Salvar no banco de dados
        analysis_id = self.save_to_database(
            top_brands, 
            len(all_brands), 
            total_requests, 
            analysis_type,
            raw_data
        )
        
        print(f"\n📊 ANÁLISE CONCLUÍDA!")
        print(f"Total de requisições usadas: {total_requests}")
        print(f"Total de menções analisadas: {len(all_brands)}")
        
        return top_brands, len(all_brands), total_requests, analysis_id
    
    def get_historical_data(self, limit=5):
        """Recupera dados históricos do banco"""
        try:
            connection = mysql.connector.connect(**self.db_config)
            cursor = connection.cursor(dictionary=True)
            
            query = """
                SELECT ba.*, 
                       COUNT(br.id) as brands_analyzed,
                       AVG(br.mentions_count) as avg_mentions
                FROM brand_analysis ba
                LEFT JOIN brand_rankings br ON ba.id = br.analysis_id
                GROUP BY ba.id
                ORDER BY ba.created_at DESC
                LIMIT %s
            """
            
            cursor.execute(query, (limit,))
            results = cursor.fetchall()
            
            cursor.close()
            connection.close()
            
            return results
            
        except mysql.connector.Error as e:
            print(f"❌ Erro ao recuperar dados históricos: {e}")
            return []

# Exemplo de uso automatizado
def main():
    # Configurações
    API_KEY = "3dc7aeb100a9dfb6f8046cf5dc83707bf09a2f32723a26b9e3ed4ba3d0aa997d"
    DB_CONFIG = {
        'host': '127.0.0.1',
        'user': 'root',
        'password': '1234',
        'database': 'fashion_trends_db'
    }
    
    print("🚀 INICIANDO ANÁLISE AUTOMÁTICA DE MARCAS DE MODA")
    print("=" * 60)
    
    # Inicializar extrator
    extractor = OptimizedFashionBrandsExtractor(API_KEY, DB_CONFIG)
    
    # Executar análise balanceada automaticamente
    top_brands, total_mentions, requests_used, analysis_id = extractor.get_top_fashion_brands_optimized(
        use_shopping=True, top_n=20
    )
    
    # Exibir resultados
    display_results(top_brands, total_mentions, requests_used, analysis_id)
    
    print("\n✅ ANÁLISE CONCLUÍDA E DADOS SALVOS NO BANCO!")

def display_results(top_brands, total_mentions, requests_used, analysis_id):
    """Exibe os resultados da análise"""
    print(f"\n🏆 TOP 20 MARCAS DE MODA MAIS BUSCADAS:")
    print("-" * 70)
    
    for i, (brand, count) in enumerate(top_brands, 1):
        percentage = (count / total_mentions) * 100 if total_mentions > 0 else 0
        bar = "█" * min(int(percentage), 25)
        print(f"{i:2d}. {brand:<20} | {count:3d} ({percentage:4.1f}%) {bar}")
    
    # Estatísticas
    efficiency = total_mentions / requests_used if requests_used > 0 else 0
    print(f"\n📊 ESTATÍSTICAS DA ANÁLISE:")
    print(f"   • ID da Análise: {analysis_id}")
    print(f"   • Total de menções: {total_mentions}")
    print(f"   • Requisições usadas: {requests_used}")
    print(f"   • Eficiência: {efficiency:.1f} menções/requisição")
    print(f"   • Custo estimado: ~${requests_used * 0.01:.3f} USD")
    print(f"   • Data/Hora: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

if __name__ == "__main__":
    main()