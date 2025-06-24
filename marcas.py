import requests
import json
from collections import Counter
import re
import time
from datetime import datetime
from sqlalchemy import create_engine, Column, Integer, String, DateTime, ForeignKey, Text, DECIMAL, JSON as SA_JSON, func
from sqlalchemy.orm import declarative_base, sessionmaker, relationship

Base = declarative_base()

class BrandAnalysis(Base):
    __tablename__ = 'brand_analysis'

    id = Column(Integer, primary_key=True)
    analysis_date = Column(DateTime, nullable=False)
    total_mentions = Column(Integer, nullable=False)
    total_requests = Column(Integer, nullable=False)
    analysis_type = Column(String(50), nullable=False)
    created_at = Column(DateTime, default=func.now())
    rankings = relationship("BrandRanking", back_populates="analysis", cascade="all, delete-orphan")
    raw_results = relationship("SearchResultRaw", back_populates="analysis", cascade="all, delete-orphan")

class BrandRanking(Base):
    __tablename__ = 'brand_rankings'

    id = Column(Integer, primary_key=True)
    analysis_id = Column(Integer, ForeignKey('brand_analysis.id'), nullable=False)
    brand_name = Column(String(100), nullable=False)
    mentions_count = Column(Integer, nullable=False)
    ranking_position = Column(Integer, nullable=False)
    percentage = Column(DECIMAL(5,2), nullable=False)
    created_at = Column(DateTime, default=func.now())
    analysis = relationship("BrandAnalysis", back_populates="rankings")

class SearchResultRaw(Base):
    __tablename__ = 'search_results_raw'

    id = Column(Integer, primary_key=True)
    analysis_id = Column(Integer, ForeignKey('brand_analysis.id'), nullable=False)
    search_query = Column(String(255))
    result_title = Column(Text)
    result_snippet = Column(Text)
    result_url = Column(String(500))
    brands_found = Column(SA_JSON)
    created_at = Column(DateTime, default=func.now())
    analysis = relationship("BrandAnalysis", back_populates="raw_results")

class OptimizedFashionBrandsExtractor:
    def __init__(self, api_key, db_url):
        self.api_key = api_key
        self.base_url = "https://serpapi.com/search.json"
        self.engine = create_engine(db_url)
        self.Session = sessionmaker(bind=self.engine)
        self.init_database()

        self.known_brands = {
            'zara', 'h&m', 'nike', 'adidas', 'gucci', 'prada', 'chanel', 
            'dior', 'versace', 'armani', 'calvin klein', 'tommy hilfiger',
            'ralph lauren', 'lacoste', 'hugo boss', 'burberry', 'fendi',
            'balenciaga', 'saint laurent', 'givenchy', 'hermès', 'cartier',
            'rolex', 'louis vuitton', 'coach', 'michael kors', 'kate spade',
            'tory burch', 'marc jacobs', 'diesel', "levi's", 'gap',
            'uniqlo', 'forever 21', 'urban outfitters', 'american eagle',
            'hollister', 'abercrombie', "victoria's secret", 'under armour',
            'puma', 'reebok', 'converse', 'vans', 'timberland', 'supreme',
            'off-white', 'stone island', 'moncler', 'canada goose',
            'renner', 'c&a', 'riachuelo', 'marisa', 'lojas americanas',
            'arezzo', 'schutz', 'melissa', 'havaianas', 'osklen',
            'animale', 'farm', 'colcci', 'ellus', 'forum', 'shoulder',
            'damyller', 'richards', 'polo wear', 'dudalina', 'aramis'
        }

    def init_database(self):
        Base.metadata.create_all(self.engine)
        print("✅ Banco de dados inicializado com sucesso!")

    def single_comprehensive_search(self, num_results=100):
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
            print("🔍 Fazendo busca única otimizada...")
            response = requests.get(self.base_url, params=params)
            response.raise_for_status()
            return response.json(), query
        except requests.exceptions.RequestException as e:
            print(f"Erro na requisição: {e}")
            return None, query

    def search_google_shopping(self, query="roupas moda", num_results=50):
        params = {
            "engine": "google_shopping",
            "q": query,
            "api_key": self.api_key,
            "num": num_results,
            "gl": "br",
            "hl": "pt"
        }
        try:
            print("🛍️ Buscando no Google Shopping...")
            response = requests.get(self.base_url, params=params)
            response.raise_for_status()
            return response.json(), query
        except requests.exceptions.RequestException as e:
            print(f"Erro na busca shopping: {e}")
            return None, query

    def extract_brands_from_results(self, search_results):
        brands_found = []
        results_data = []
        if not search_results:
            return brands_found, results_data

        for result in search_results.get('organic_results', []):
            title = result.get('title', '')
            snippet = result.get('snippet', '')
            url = result.get('link', '')
            text_content = f"{title} {snippet}".lower()
            result_brands = self.find_brands_in_text(text_content)
            brands_found.extend(result_brands)
            results_data.append({
                'title': title,
                'snippet': snippet,
                'url': url,
                'brands_found': result_brands
            })

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
        found_brands = []
        text_lower = text.lower()
        for brand in self.known_brands:
            pattern = r'\b' + re.escape(brand.lower()) + r'\b'
            matches = re.findall(pattern, text_lower)
            if matches:
                found_brands.extend([brand.title()] * len(matches))
        return found_brands

    def get_top_fashion_brands_optimized(self, use_shopping=True, top_n=20):
        all_brands = []
        total_requests = 0
        raw_data = {}
        print("🚀 Iniciando análise otimizada de marcas de moda...")

        main_results, main_query = self.single_comprehensive_search(num_results=100)
        total_requests += 1
        if main_results:
            main_brands, main_raw = self.extract_brands_from_results(main_results)
            all_brands.extend(main_brands)
            raw_data[main_query] = main_raw
            print(f"✅ Busca principal: {len(main_brands)} menções encontradas")

        if use_shopping:
            shopping_results, shopping_query = self.search_google_shopping("moda feminina masculina", 50)
            total_requests += 1
            if shopping_results:
                shopping_brands, shopping_raw = self.extract_brands_from_results(shopping_results)
                all_brands.extend(shopping_brands)
                raw_data[shopping_query] = shopping_raw
                print(f"✅ Google Shopping: {len(shopping_brands)} menções encontradas")

        brand_counter = Counter(all_brands)
        top_brands = brand_counter.most_common(top_n)
        analysis_type = "Balanceada (Web + Shopping)" if use_shopping else "Rápida (Web apenas)"

        analysis_id = self.save_to_database(
            top_brands,
            len(all_brands),
            total_requests,
            analysis_type,
            raw_data
        )

        print("\n📊 ANÁLISE CONCLUÍDA!")
        print(f"Total de requisições usadas: {total_requests}")
        print(f"Total de menções analisadas: {len(all_brands)}")
        return top_brands, len(all_brands), total_requests, analysis_id

    def save_to_database(self, top_brands, total_mentions, total_requests, analysis_type, raw_data=None):
        session = self.Session()
        try:
            analysis = BrandAnalysis(
                analysis_date=datetime.now(),
                total_mentions=total_mentions,
                total_requests=total_requests,
                analysis_type=analysis_type
            )
            session.add(analysis)
            session.flush()

            for position, (brand, count) in enumerate(top_brands, 1):
                percentage = (count / total_mentions) * 100 if total_mentions > 0 else 0
                ranking = BrandRanking(
                    analysis_id=analysis.id,
                    brand_name=brand,
                    mentions_count=count,
                    ranking_position=position,
                    percentage=percentage
                )
                session.add(ranking)

            if raw_data:
                for query, results in raw_data.items():
                    for result in results:
                        raw_entry = SearchResultRaw(
                            analysis_id=analysis.id,
                            search_query=query,
                            result_title=result['title'][:500],
                            result_snippet=result['snippet'][:1000],
                            result_url=result['url'][:500],
                            brands_found=result['brands_found']
                        )
                        session.add(raw_entry)

            session.commit()
            print(f"✅ Dados salvos no banco! ID da análise: {analysis.id}")
            return analysis.id
        except Exception as e:
            session.rollback()
            print(f"❌ Erro ao salvar no banco de dados: {e}")
            return None
        finally:
            session.close()

    def get_historical_data(self, limit=5):
        session = self.Session()
        try:
            results = session.query(
                BrandAnalysis,
                func.count(BrandRanking.id).label("brands_analyzed"),
                func.avg(BrandRanking.mentions_count).label("avg_mentions")
            ).outerjoin(BrandRanking).group_by(BrandAnalysis.id)
            results = results.order_by(BrandAnalysis.created_at.desc()).limit(limit).all()

            formatted = []
            for row in results:
                ba = row[0]
                formatted.append({
                    'id': ba.id,
                    'analysis_date': ba.analysis_date,
                    'total_mentions': ba.total_mentions,
                    'total_requests': ba.total_requests,
                    'analysis_type': ba.analysis_type,
                    'created_at': ba.created_at,
                    'brands_analyzed': row.brands_analyzed,
                    'avg_mentions': float(row.avg_mentions or 0)
                })
            return formatted
        except Exception as e:
            print(f"❌ Erro ao recuperar dados históricos: {e}")
            return []
        finally:
            session.close()

def display_results(top_brands, total_mentions, requests_used, analysis_id):
    print(f"\n🏆 TOP 20 MARCAS DE MODA MAIS BUSCADAS:")
    print("-" * 70)
    for i, (brand, count) in enumerate(top_brands, 1):
        percentage = (count / total_mentions) * 100 if total_mentions > 0 else 0
        bar = "█" * min(int(percentage), 25)
        print(f"{i:2d}. {brand:<20} | {count:3d} ({percentage:4.1f}%) {bar}")

    efficiency = total_mentions / requests_used if requests_used > 0 else 0
    print(f"\n📊 ESTATÍSTICAS DA ANÁLISE:")
    print(f"   • ID da Análise: {analysis_id}")
    print(f"   • Total de menções: {total_mentions}")
    print(f"   • Requisições usadas: {requests_used}")
    print(f"   • Eficiência: {efficiency:.1f} menções/requisição")
    print(f"   • Custo estimado: ~${requests_used * 0.01:.3f} USD")
    print(f"   • Data/Hora: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

def main():
    db_url = "mysql+pymysql://root:123456789@localhost/fashion_trends_db"
    API_KEY = "3dc7aeb100a9dfb6f8046cf5dc83707bf09a2f32723a26b9e3ed4ba3d0aa997d"

    print("🚀 INICIANDO ANÁLISE AUTOMÁTICA DE MARCAS DE MODA")
    print("=" * 60)

    extractor = OptimizedFashionBrandsExtractor(API_KEY, db_url)

    top_brands, total_mentions, requests_used, analysis_id = extractor.get_top_fashion_brands_optimized(
        use_shopping=True, top_n=20
    )

    display_results(top_brands, total_mentions, requests_used, analysis_id)

if __name__ == "__main__":
    main()
