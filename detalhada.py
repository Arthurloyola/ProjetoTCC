import requests
from bs4 import BeautifulSoup
import time
from urllib.parse import urljoin, urlparse
import urllib3
import mysql.connector
from mysql.connector import Error
from datetime import datetime
import hashlib

# Desabilita avisos SSL
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Configuração do banco de dados
mysql_config = {
    'host': '127.0.0.1',
    'user': 'root',
    'password': '1234',
    'database': 'fashion_trends_db'
}

def criar_tabela():
    """Cria a tabela no banco de dados se não existir"""
    try:
        connection = mysql.connector.connect(**mysql_config)
        cursor = connection.cursor()
        
        # Primeiro, remove a tabela se existir (para corrigir o problema do índice)
        cursor.execute("DROP TABLE IF EXISTS fashion_articles")
        
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
        
        cursor.execute(create_table_query)
        connection.commit()
        print("✅ Tabela 'fashion_articles' criada com sucesso!")
        
    except Error as e:
        print(f"❌ Erro ao criar tabela: {e}")
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

def salvar_no_banco(resultados):
    """Salva os resultados no banco de dados MySQL"""
    if not resultados:
        print("Nenhum resultado para salvar.")
        return
    
    try:
        connection = mysql.connector.connect(**mysql_config)
        cursor = connection.cursor()
        
        # Query de inserção com ON DUPLICATE KEY UPDATE para evitar duplicatas
        insert_query = """
        INSERT INTO fashion_articles (titulo, url, site, data_coleta, url_hash) 
        VALUES (%s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE 
            titulo = VALUES(titulo),
            data_coleta = VALUES(data_coleta)
        """
        
        dados_para_inserir = []
        data_atual = datetime.now()
        
        for item in resultados:
            # Cria um hash da URL para usar como chave única
            url_hash = hashlib.sha256(item['url'].encode('utf-8')).hexdigest()
            
            dados_para_inserir.append((
                item['titulo'],
                item['url'],
                item['site'],
                data_atual,
                url_hash
            ))
        
        # Executa inserção em lote
        cursor.executemany(insert_query, dados_para_inserir)
        connection.commit()
        
        print(f"✅ {len(dados_para_inserir)} registros processados no banco de dados!")
        print(f"📊 Artigos inseridos/atualizados com sucesso.")
        
    except Error as e:
        print(f"❌ Erro ao salvar no banco de dados: {e}")
        if connection:
            connection.rollback()
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

def verificar_estatisticas():
    """Mostra estatísticas dos dados no banco"""
    try:
        connection = mysql.connector.connect(**mysql_config)
        cursor = connection.cursor()
        
        # Total de artigos
        cursor.execute("SELECT COUNT(*) FROM fashion_articles")
        total = cursor.fetchone()[0]
        
        # Artigos por site
        cursor.execute("""
            SELECT site, COUNT(*) as quantidade 
            FROM fashion_articles 
            GROUP BY site 
            ORDER BY quantidade DESC
        """)
        por_site = cursor.fetchall()
        
        # Artigos coletados hoje
        cursor.execute("""
            SELECT COUNT(*) FROM fashion_articles 
            WHERE DATE(data_coleta) = CURDATE()
        """)
        hoje = cursor.fetchone()[0]
        
        print(f"\n📈 ESTATÍSTICAS DO BANCO DE DADOS:")
        print(f"📄 Total de artigos: {total}")
        print(f"🆕 Coletados hoje: {hoje}")
        print(f"\n📊 Artigos por site:")
        for site, quantidade in por_site:
            print(f"   • {site}: {quantidade} artigos")
        
    except Error as e:
        print(f"Erro ao verificar estatísticas: {e}")
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

def scrape_fashion_sites():
    # Lista de sites de moda para fazer scraping (sites mais confiáveis)
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
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'pt-BR,pt;q=0.9,en;q=0.8',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1'
    }
    
    resultados = []
    
    for site in sites:
        try:
            print(f"🔍 Fazendo scraping de: {site}")
            
            # Configurações da sessão para melhor compatibilidade
            session = requests.Session()
            session.headers.update(headers)
            
            response = session.get(
                site, 
                timeout=15,
                verify=False,  # Desabilita verificação SSL para sites problemáticos
                allow_redirects=True
            )
            
            if response.status_code == 200:
                soup = BeautifulSoup(response.content, 'html.parser')
                
                # Busca por diferentes tipos de elementos que podem conter artigos
                selectors = [
                    'a[href*="moda"]',
                    'a[href*="fashion"]', 
                    'a[href*="look"]',
                    'a[href*="tendencia"]',
                    'h1 a', 'h2 a', 'h3 a',
                    '.article-title a',
                    '.post-title a',
                    'article a'
                ]
                
                links_encontrados = set()  # Para evitar duplicatas
                
                for selector in selectors:
                    elementos = soup.select(selector)
                    for link in elementos:
                        if link.get('href'):
                            titulo = link.get_text(strip=True)
                            url = urljoin(site, link['href'])
                            
                            # Filtra títulos relevantes e evita duplicatas
                            if (titulo and len(titulo) > 15 and len(titulo) < 500 and 
                                url not in links_encontrados and
                                any(palavra in titulo.lower() for palavra in 
                                    ['moda', 'tendência', 'look', 'estilo', 'fashion', 'coleção', 
                                     'desfile', 'trend', 'outfit', 'style', 'wear', 'roupa'])):
                                
                                links_encontrados.add(url)
                                resultados.append({
                                    'titulo': titulo,
                                    'url': url,  
                                    'site': urlparse(site).netloc
                                })
                
                print(f"   ✅ {len(links_encontrados)} artigos encontrados")
            else:
                print(f"   ❌ Status {response.status_code}")
            
            # Pausa entre requisições
            time.sleep(3)
            
        except requests.exceptions.SSLError:
            print(f"   ⚠️ Erro SSL em {site} - tentando sem verificação SSL...")
            continue
        except requests.exceptions.ConnectionError:
            print(f"   ⚠️ Erro de conexão em {site} - site pode estar indisponível")
            continue
        except Exception as e:
            print(f"   ❌ Erro ao acessar {site}: {type(e).__name__}")
            continue
    
    return resultados

def listar_ultimos_artigos(limite=10):
    """Lista os últimos artigos coletados"""
    try:
        connection = mysql.connector.connect(**mysql_config)
        cursor = connection.cursor()
        
        query = """
        SELECT titulo, url, site, data_coleta 
        FROM fashion_articles 
        ORDER BY data_coleta DESC 
        LIMIT %s
        """
        
        cursor.execute(query, (limite,))
        artigos = cursor.fetchall()
        
        print(f"\n📰 ÚLTIMOS {limite} ARTIGOS COLETADOS:")
        print("=" * 80)
        
        for i, (titulo, url, site, data) in enumerate(artigos, 1):
            print(f"{i}. {titulo[:70]}...")
            print(f"   🌐 Site: {site}")
            print(f"   📅 Data: {data.strftime('%d/%m/%Y %H:%M')}")
            print(f"   🔗 URL: {url}")
            print("-" * 80)
        
    except Error as e:
        print(f"Erro ao listar artigos: {e}")
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

def main():
    print("🚀 Iniciando scraping de sites de moda...")
    print("=" * 60)
    
    # Cria a tabela se não existir
    criar_tabela()
    
    # Executa o scraping
    dados = scrape_fashion_sites()
    
    # Remove duplicatas baseado no título
    dados_unicos = []
    titulos_vistos = set()
    
    for item in dados:
        if item['titulo'] not in titulos_vistos:
            dados_unicos.append(item)
            titulos_vistos.add(item['titulo'])
    
    print(f"\n✨ Encontradas {len(dados_unicos)} matérias únicas sobre moda.")
    
    if dados_unicos:
        # Salva os resultados no banco
        salvar_no_banco(dados_unicos)
        
        # Mostra estatísticas
        verificar_estatisticas()
        
        # Lista os últimos artigos
        listar_ultimos_artigos(5)
    else:
        print("❌ Nenhum dado foi coletado.")

if __name__ == "__main__":
    main()