import requests
import json
from datetime import datetime
from collections import Counter
import time

from sqlalchemy import (
    create_engine, Column, Integer, String, DateTime, Text, ForeignKey, DECIMAL
)
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import sessionmaker, relationship

Base = declarative_base()


class FashionTrendsAnalysis(Base):
    __tablename__ = 'fashion_trends_analysis'

    id = Column(Integer, primary_key=True)
    analysis_date = Column(DateTime, nullable=False)
    total_keywords_analyzed = Column(Integer)
    average_score = Column(DECIMAL(5, 2))
    created_at = Column(DateTime, default=datetime.utcnow)

    keywords = relationship("FashionKeywordResult", back_populates="analysis", cascade="all, delete-orphan")
    indicators = relationship("TrendIndicator", back_populates="analysis", cascade="all, delete-orphan")


class FashionKeywordResult(Base):
    __tablename__ = 'fashion_keywords_results'

    id = Column(Integer, primary_key=True)
    analysis_id = Column(Integer, ForeignKey('fashion_trends_analysis.id'))
    keyword = Column(String(255), nullable=False)
    popularity_score = Column(Integer, nullable=False)
    result_count = Column(Integer)
    trend_status = Column(String(100))
    trend_indicators = Column(Text)
    related_searches = Column(Text)
    created_at = Column(DateTime, default=datetime.utcnow)

    analysis = relationship("FashionTrendsAnalysis", back_populates="keywords")


class TrendIndicator(Base):
    __tablename__ = 'trend_indicators'

    id = Column(Integer, primary_key=True)
    analysis_id = Column(Integer, ForeignKey('fashion_trends_analysis.id'))
    indicator = Column(String(100), nullable=False)
    frequency = Column(Integer, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)

    analysis = relationship("FashionTrendsAnalysis", back_populates="indicators")


class FashionTrendsAnalyzer:
    def __init__(self, api_key, db_url):
        self.api_key = api_key
        self.db_url = db_url
        self.base_url = "https://serpapi.com/search"

        self.fashion_keywords = [
            "moda feminina 2025", "moda masculina 2025", "vestido tendência",
            "calça jeans moda", "tênis em alta", "bolsa feminina",
            "moda verão 2025", "look do dia", "outfit tendência", "roupa casual"
        ]

        self.engine = create_engine(self.db_url)
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine)

        print("✅ Banco de dados configurado")

    def get_google_search_data(self, keyword):
        params = {
            "engine": "google",
            "q": f"{keyword} site:br OR inurl:br",
            "gl": "br", "hl": "pt", "num": 5,
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
            return {"popularity_score": 0, "result_count": 0, "trend_indicators": [], "related_searches": []}

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

        popularity_score = result_count * 15 + len(trend_indicators) * 8
        if "shopping_results" in search_data:
            popularity_score += len(search_data["shopping_results"]) * 5
        if "knowledge_graph" in search_data:
            popularity_score += 25

        related_searches = [
            s.get("query", "") for s in search_data.get("related_searches", [])[:3]
        ]

        return {
            "popularity_score": min(popularity_score, 100),
            "result_count": result_count,
            "trend_indicators": list(set(trend_indicators)),
            "related_searches": related_searches
        }

    def determine_trend_status(self, analysis):
        score = analysis["popularity_score"]
        indicators = analysis["trend_indicators"]

        high = ["2025", "nova", "tendência", "em alta", "viral", "trend"]
        moderate = ["2024", "moda", "popular"]

        if score >= 75 and any(ind in indicators for ind in high):
            return "🔥 Em alta (tendência forte)"
        elif score >= 60 and any(ind in indicators for ind in high):
            return "📈 Crescendo (tendência moderada)"
        elif score >= 70:
            return "⭐ Popular (interesse alto)"
        elif score >= 45 and any(ind in indicators for ind in moderate):
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

                results.append({
                    "keyword": keyword,
                    "popularity_score": analysis["popularity_score"],
                    "result_count": analysis["result_count"],
                    "trend_status": trend_status,
                    "trend_indicators": analysis["trend_indicators"],
                    "related_searches": analysis["related_searches"]
                })

                print(f"  Score: {analysis['popularity_score']}/100")

            time.sleep(2.0)

        results.sort(key=lambda x: x["popularity_score"], reverse=True)
        return results

    def save_to_database(self, results):
        session = self.Session()
        try:
            now = datetime.now()
            avg_score = sum(r['popularity_score'] for r in results) / len(results) if results else 0

            analysis_entry = FashionTrendsAnalysis(
                analysis_date=now,
                total_keywords_analyzed=len(results),
                average_score=avg_score
            )

            all_indicators = []

            for result in results:
                keyword_entry = FashionKeywordResult(
                    keyword=result["keyword"],
                    popularity_score=result["popularity_score"],
                    result_count=result["result_count"],
                    trend_status=result["trend_status"],
                    trend_indicators=json.dumps(result["trend_indicators"]) if result["trend_indicators"] else None,
                    related_searches=json.dumps(result["related_searches"]) if result["related_searches"] else None
                )
                analysis_entry.keywords.append(keyword_entry)
                all_indicators.extend(result["trend_indicators"])

            indicator_counts = Counter(all_indicators)
            for indicator, count in indicator_counts.items():
                indicator_entry = TrendIndicator(indicator=indicator, frequency=count)
                analysis_entry.indicators.append(indicator_entry)

            session.add(analysis_entry)
            session.commit()

            print(f"✅ Dados salvos no banco (ID: {analysis_entry.id})")

            print("\n📊 RANKING DE POPULARIDADE:")
            for i, result in enumerate(results, 1):
                print(f"{i:2d}. {result['keyword']:<25} - {result['popularity_score']:2d}/100 - {result['trend_status']}")

        except Exception as e:
            session.rollback()
            print(f"❌ Erro ao salvar: {e}")
        finally:
            session.close()


def main():
    db_url = "mysql+pymysql://root:123456789@localhost/fashion_trends_db"
    API_KEY = "3dc7aeb100a9dfb6f8046cf5dc83707bf09a2f32723a26b9e3ed4ba3d0aa997d"

    if not API_KEY:
        print("❌ Erro: Configure sua API key")
        return

    analyzer = FashionTrendsAnalyzer(API_KEY, db_url)

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
