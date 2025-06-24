import requests
from bs4 import BeautifulSoup
import time
from urllib.parse import urljoin, urlparse
import urllib3
import hashlib
from datetime import datetime
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

# Desabilita avisos SSL
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Configuração da conexão SQLAlchemy
mysql_config = {
    'host': 'localhost',
    'user': 'root',
    'password': '123456789',
    'database': 'fashion_trends_db'
}

connection_url = f"mysql+pymysql://{mysql_config['user']}:{mysql_config['password']}@{mysql_config['host']}/{mysql_config['database']}"
engine = create_engine(connection_url, echo=False, pool_pre_ping=True)

def executar_sql(query, params=None, fetch=False, many=False):
    try:
        with engine.begin() as conn:
            if many:
                conn.execute(text(query), params)
                return
            result = conn.execute(text(query), params or {})
            return result.fetchall() if fetch else result
    except SQLAlchemyError as e:
        print(f"❌ Erro SQL: {e}")

def criar_tabela():
    executar_sql("DROP TABLE IF EXISTS fashion_articles;")
    create_table_query = """
    CREATE TABLE fashion_articles (
        id INT AUTO_INCREMENT PRIMARY KEY,
        titulo VARCHAR(500) NOT NULL,
        url TEXT NOT NULL,
        site VARCHAR(200) NOT NULL,
        data_coleta DATETIME DEFAULT CURRENT_TIMESTAMP,
        url_hash VARCHAR(64) UNIQUE,
        INDEX idx_site (site),
        INDEX idx_data_coleta (data_coleta),
        INDEX idx_url_hash (url_hash)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    """
    executar_sql(create_table_query)
    print("✅ Tabela 'fashion_articles' criada com sucesso!")

def salvar_no_banco(resultados):
    if not resultados:
        print("Nenhum resultado para salvar.")
        return

    insert_query = """
    INSERT INTO fashion_articles (titulo, url, site, data_coleta, url_hash)
    VALUES (:titulo, :url, :site, :data_coleta, :url_hash)
    ON DUPLICATE KEY UPDATE 
        titulo = VALUES(titulo),
        data_coleta = VALUES(data_coleta);
    """
    now = datetime.now()
    dados_para_inserir = [{
        'titulo': item['titulo'],
        'url': item['url'],
        'site': item['site'],
        'data_coleta': now,
        'url_hash': hashlib.sha256(item['url'].encode('utf-8')).hexdigest()
    } for item in resultados]

    try:
        with engine.begin() as conn:
            conn.execute(text(insert_query), dados_para_inserir)
        print(f"✅ {len(dados_para_inserir)} artigos inseridos/atualizados.")
    except SQLAlchemyError as e:
        print(f"❌ Erro ao salvar no banco: {e}")

def verificar_estatisticas():
    try:
        total = executar_sql("SELECT COUNT(*) FROM fashion_articles", fetch=True)[0][0]
        hoje = executar_sql("SELECT COUNT(*) FROM fashion_articles WHERE DATE(data_coleta) = CURDATE()", fetch=True)[0][0]
        por_site = executar_sql("""
            SELECT site, COUNT(*) as quantidade 
            FROM fashion_articles 
            GROUP BY site 
            ORDER BY quantidade DESC
        """, fetch=True)

        print(f"\n📈 ESTATÍSTICAS:")
        print(f"📄 Total de artigos: {total}")
        print(f"🆕 Coletados hoje: {hoje}")
        print("\n📊 Artigos por site:")
        for site, qtd in por_site:
            print(f"   • {site}: {qtd} artigos")
    except SQLAlchemyError as e:
        print(f"Erro ao gerar estatísticas: {e}")

def listar_ultimos_artigos(limite=10):
    query = """
    SELECT titulo, url, site, data_coleta 
    FROM fashion_articles 
    ORDER BY data_coleta DESC 
    LIMIT :limite
    """
    artigos = executar_sql(query, {'limite': limite}, fetch=True)

    print(f"\n📰 ÚLTIMOS {limite} ARTIGOS COLETADOS:")
    print("=" * 80)
    for i, (titulo, url, site, data) in enumerate(artigos, 1):
        print(f"{i}. {titulo[:70]}...")
        print(f"   🌐 Site: {site}")
        print(f"   📅 Data: {data.strftime('%d/%m/%Y %H:%M')}")
        print(f"   🔗 URL: {url}")
        print("-" * 80)

def scrape_fashion_sites():
    sites = [
        'https://elle.com.br/moda',
        'https://ffw.uol.com.br',
        'https://www.glamour.com/fashion',
        'https://www.cosmopolitan.com/style-beauty/fashion/',
        'https://www.hypeness.com.br/categoria/moda/',
        'https://www.fashionbubbles.com',
        'https://www.caras.com.br/moda',
        'https://www.whowhatwear.com',
        'https://www.refinery29.com/en-us/fashion'
    ]

    headers = {
        'User-Agent': 'Mozilla/5.0',
        'Accept': 'text/html,application/xhtml+xml',
        'Accept-Language': 'pt-BR,pt;q=0.9,en;q=0.8',
        'Connection': 'keep-alive'
    }

    resultados = []
    for site in sites:
        try:
            print(f"🔍 Scraping: {site}")
            response = requests.get(site, headers=headers, timeout=15, verify=False)
            if response.status_code != 200:
                print(f"   ❌ Status {response.status_code}")
                continue

            soup = BeautifulSoup(response.content, 'html.parser')
            selectors = [
                'a[href*="moda"]', 'a[href*="fashion"]', 'a[href*="look"]',
                'a[href*="tendencia"]', 'h1 a', 'h2 a', 'h3 a',
                '.article-title a', '.post-title a', 'article a'
            ]

            encontrados = set()
            for selector in selectors:
                for link in soup.select(selector):
                    url = urljoin(site, link.get('href', ''))
                    titulo = link.get_text(strip=True)
                    if (titulo and 15 < len(titulo) < 500 and
                        url not in encontrados and
                        any(p in titulo.lower() for p in ['moda', 'tendência', 'look', 'estilo', 'fashion', 'trend', 'style', 'roupa'])):
                        encontrados.add(url)
                        resultados.append({'titulo': titulo, 'url': url, 'site': urlparse(site).netloc})
            print(f"   ✅ {len(encontrados)} artigos")
            time.sleep(2)
        except Exception as e:
            print(f"   ⚠️ Erro em {site}: {e.__class__.__name__}")
    return resultados

def main():
    print("🚀 Iniciando scraping de sites de moda...\n" + "=" * 60)
    criar_tabela()
    dados = scrape_fashion_sites()

    # Remover duplicatas por título
    unicos = []
    vistos = set()
    for d in dados:
        if d['titulo'] not in vistos:
            vistos.add(d['titulo'])
            unicos.append(d)

    print(f"\n✨ {len(unicos)} artigos únicos encontrados.")
    if unicos:
        salvar_no_banco(unicos)
        verificar_estatisticas()
        listar_ultimos_artigos(5)
    else:
        print("❌ Nenhum artigo relevante coletado.")

if __name__ == "__main__":
    main()
