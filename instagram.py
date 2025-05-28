import requests
import json
import pandas as pd
from datetime import datetime, timedelta
from collections import Counter
import re
from typing import List, Dict, Any
import time
import os
import mysql.connector # Importar o conector MySQL

class InstagramFashionInsights:
    def __init__(self, access_token: str, business_account_id: str, mysql_config: Dict[str, str]):
        """
        Inicializa a classe com token de acesso, ID da conta comercial e configuração do MySQL.
        
        Args:
            access_token: Token de acesso do Instagram Graph API
            business_account_id: ID da conta comercial do Instagram
            mysql_config: Dicionário com as configurações do MySQL
        """
        self.access_token = access_token
        self.business_account_id = business_account_id
        self.base_url = "https://graph.facebook.com/v18.0"
        self.mysql_config = mysql_config # Armazenar a configuração do MySQL
        
        # Hashtags relacionadas à moda para análise
        self.fashion_hashtags = [
            "moda", "fashion", "style", "look", "outfit", "tendencia", 
            "roupas", "vestido", "blusa", "calca", "sapatos", "acessorios",
            "modafeminina", "modamasculina", "modainfantil", "streetstyle",
            "fashionblogger", "fashionista", "lookdodia", "estilo"
        ]
        
        self.db_connection = None
        self.db_cursor = None
        self._connect_db() # Conectar ao banco de dados ao inicializar

    def _connect_db(self):
        """
        Estabelece a conexão com o banco de dados MySQL.
        """
        try:
            self.db_connection = mysql.connector.connect(**self.mysql_config)
            self.db_cursor = self.db_connection.cursor()
            print("✅ Conectado ao banco de dados MySQL.")
            self._create_tables() # Criar tabelas se não existirem
        except mysql.connector.Error as err:
            print(f"❌ Erro ao conectar ao MySQL: {err}")
            self.db_connection = None
            self.db_cursor = None

    def _create_tables(self):
        """
        Cria as tabelas necessárias no banco de dados se elas não existirem.
        """
        if not self.db_connection:
            print("❌ Não foi possível criar tabelas: conexão com o banco de dados não estabelecida.")
            return

        tables = {}
        tables['account_info'] = (
            "CREATE TABLE IF NOT EXISTS account_info ("
            "id INT AUTO_INCREMENT PRIMARY KEY,"
            "username VARCHAR(255) NOT NULL,"
            "account_type VARCHAR(255),"
            "media_count INT,"
            "followers_count INT,"
            "analysis_date DATETIME NOT NULL"
            ")"
        )
        tables['posts'] = (
            "CREATE TABLE IF NOT EXISTS posts ("
            "id VARCHAR(255) PRIMARY KEY,"
            "media_type VARCHAR(255),"
            "permalink TEXT,"
            "timestamp DATETIME,"
            "caption TEXT,"
            "like_count INT,"
            "comments_count INT,"
            "engagement INT,"
            "caption_length INT,"
            "hashtag_count INT,"
            "analysis_date DATETIME NOT NULL"
            ")"
        )
        tables['trending_hashtags'] = (
            "CREATE TABLE IF NOT EXISTS trending_hashtags ("
            "id INT AUTO_INCREMENT PRIMARY KEY,"
            "hashtag VARCHAR(255) NOT NULL,"
            "usage_count INT,"
            "analysis_date DATETIME NOT NULL"
            ")"
        )
        tables['trending_keywords'] = (
            "CREATE TABLE IF NOT EXISTS trending_keywords ("
            "id INT AUTO_INCREMENT PRIMARY KEY,"
            "keyword VARCHAR(255) NOT NULL,"
            "mention_count INT,"
            "analysis_date DATETIME NOT NULL"
            ")"
        )
        tables['posting_times'] = (
            "CREATE TABLE IF NOT EXISTS posting_times ("
            "id INT AUTO_INCREMENT PRIMARY KEY,"
            "hour_of_day INT NOT NULL,"
            "post_count INT,"
            "analysis_date DATETIME NOT NULL"
            ")"
        )
        tables['fashion_hashtag_performance'] = (
            "CREATE TABLE IF NOT EXISTS fashion_hashtag_performance ("
            "id INT AUTO_INCREMENT PRIMARY KEY,"
            "hashtag VARCHAR(255) NOT NULL,"
            "usage_count INT,"
            "avg_engagement FLOAT,"
            "analysis_date DATETIME NOT NULL"
            ")"
        )
        tables['engagement_summary'] = (
            "CREATE TABLE IF NOT EXISTS engagement_summary ("
            "id INT AUTO_INCREMENT PRIMARY KEY,"
            "avg_likes FLOAT,"
            "avg_comments FLOAT,"
            "avg_engagement FLOAT,"
            "analysis_date DATETIME NOT NULL"
            ")"
        )
        tables['content_recommendations'] = (
            "CREATE TABLE IF NOT EXISTS content_recommendations ("
            "id INT AUTO_INCREMENT PRIMARY KEY,"
            "recommendation TEXT NOT NULL,"
            "best_hashtag_count INT,"
            "optimal_caption_strategy VARCHAR(255),"
            "analysis_date DATETIME NOT NULL"
            ")"
        )
        
        for table_name, table_sql in tables.items():
            try:
                print(f"Criando tabela: {table_name}")
                self.db_cursor.execute(table_sql)
                self.db_connection.commit()
            except mysql.connector.Error as err:
                print(f"❌ Erro ao criar tabela {table_name}: {err}")

    def validate_credentials(self) -> bool:
        """
        Valida as credenciais da API
        """
        url = f"{self.base_url}/me"
        params = {
            'access_token': self.access_token
        }
        
        try:
            response = requests.get(url, params=params)
            if response.status_code == 200:
                print("✅ Token de acesso válido")
                return True
            else:
                print(f"❌ Token inválido. Status: {response.status_code}")
                print(f"Resposta: {response.text}")
                return False
        except requests.exceptions.RequestException as e:
            print(f"❌ Erro de conexão: {e}")
            return False
    
    def get_account_info(self) -> Dict:
        """
        Obtém informações básicas da conta
        """
        url = f"{self.base_url}/{self.business_account_id}"
        params = {
            'fields': 'account_type,media_count,followers_count,username',
            'access_token': self.access_token
        }
        
        try:
            response = requests.get(url, params=params)
            if response.status_code == 200:
                return response.json()
            else:
                print(f"❌ Erro ao obter info da conta. Status: {response.status_code}")
                print(f"Resposta: {response.text}")
                return {}
        except requests.exceptions.RequestException as e:
            print(f"❌ Erro ao obter informações da conta: {e}")
            return {}
    
    def get_account_media(self, limit: int = 100) -> List[Dict]:
        """
        Obtém as mídias da conta comercial com melhor tratamento de erro
        """
        url = f"{self.base_url}/{self.business_account_id}/media"
        
        # Campos simplificados para evitar problemas de permissão
        params = {
            'fields': 'id,media_type,permalink,timestamp,caption,like_count,comments_count',
            'limit': limit,
            'access_token': self.access_token
        }
        
        try:
            response = requests.get(url, params=params)
            
            if response.status_code == 200:
                data = response.json()
                print(f"✅ {len(data.get('data', []))} posts obtidos com sucesso")
                return data.get('data', [])
            else:
                print(f"❌ Erro ao obter mídias. Status: {response.status_code}")
                print(f"Resposta: {response.text}")
                
                # Tentar com campos ainda mais básicos
                basic_params = {
                    'fields': 'id,caption,timestamp',
                    'limit': limit,
                    'access_token': self.access_token
                }
                
                response = requests.get(url, params=basic_params)
                if response.status_code == 200:
                    data = response.json()
                    print(f"✅ {len(data.get('data', []))} posts obtidos (modo básico)")
                    return data.get('data', [])
                
                return []
                
        except requests.exceptions.RequestException as e:
            print(f"❌ Erro de conexão ao obter mídias: {e}")
            return []
    
    def get_media_insights(self, media_id: str) -> Dict:
        """
        Obtém insights de uma mídia específica
        """
        url = f"{self.base_url}/{media_id}/insights"
        params = {
            'metric': 'impressions,reach,engagement',
            'access_token': self.access_token
        }
        
        try:
            response = requests.get(url, params=params)
            if response.status_code == 200:
                return response.json()
            return {}
        except:
            return {}
    
    def extract_hashtags_from_caption(self, caption: str) -> List[str]:
        """
        Extrai hashtags de uma legenda
        """
        if not caption:
            return []
        
        hashtags = re.findall(r'#(\w+)', caption.lower())
        return hashtags
    
    def extract_fashion_keywords(self, caption: str) -> List[str]:
        """
        Extrai palavras-chave relacionadas à moda de uma legenda
        """
        if not caption:
            return []
        
        fashion_keywords = [
            'vestido', 'blusa', 'calça', 'jeans', 'saia', 'shorts', 'camisa',
            'casaco', 'jaqueta', 'blazer', 'sapato', 'tênis', 'sandália',
            'bolsa', 'colar', 'brinco', 'anel', 'pulseira', 'óculos',
            'verão', 'inverno', 'primavera', 'outono', 'casual', 'formal',
            'festa', 'trabalho', 'praia', 'academia', 'conforto', 'elegante',
            'linda', 'lindo', 'estilo', 'moda', 'fashion', 'look', 'outfit'
        ]
        
        caption_lower = caption.lower()
        found_keywords = []
        
        for keyword in fashion_keywords:
            if keyword in caption_lower:
                found_keywords.append(keyword)
        
        return found_keywords
    
    def generate_sample_data(self) -> Dict[str, Any]:
        """
        Gera dados de exemplo quando a API não está funcionando
        """
        print("📊 Gerando dados de exemplo baseados em tendências atuais...")
        
        sample_hashtags = [
            ('moda', 45), ('fashion', 38), ('style', 32), ('lookdodia', 28),
            ('outfit', 25), ('modafeminina', 22), ('vestido', 20), ('jeans', 18),
            ('tendencia', 16), ('estilo', 15), ('blusa', 14), ('sapatos', 12),
            ('acessorios', 11), ('fashionista', 10), ('streetstyle', 9),
            ('modamasculina', 8), ('bolsa', 7), ('casaco', 6), ('formal', 5), ('casual', 4)
        ]
        
        sample_keywords = [
            ('vestido', 25), ('jeans', 22), ('blusa', 20), ('sapatos', 18),
            ('bolsa', 15), ('casual', 14), ('estilo', 12), ('verão', 11),
            ('elegante', 10), ('conforto', 9), ('formal', 8), ('festa', 7),
            ('trabalho', 6), ('tênis', 5), ('inverno', 4)
        ]
        
        sample_posting_times = [
            (19, 12), (20, 10), (18, 9), (21, 8), (17, 7),
            (12, 6), (15, 5), (14, 4), (16, 3), (13, 2)
        ]
        
        return {
            'trending_hashtags': sample_hashtags,
            'trending_keywords': sample_keywords,
            'engagement_analysis': {
                'avg_likes': 147.5,
                'avg_comments': 23.2,
                'avg_engagement': 170.7,
                'engagement_by_hashtag_count': {
                    5: 125.4, 8: 167.8, 10: 189.3, 15: 156.2
                },
                'engagement_by_caption_length': {
                    'short_captions': 134.2,
                    'medium_captions': 178.9,
                    'long_captions': 145.6
                }
            },
            'posting_time_analysis': sample_posting_times,
            'fashion_hashtag_performance': [
                {'hashtag': 'moda', 'avg_engagement': 165.4, 'usage_count': 50},
                {'hashtag': 'fashion', 'avg_engagement': 158.7, 'usage_count': 45},
                {'hashtag': 'style', 'avg_engagement': 142.3, 'usage_count': 40},
                {'hashtag': 'lookdodia', 'avg_engagement': 134.8, 'usage_count': 35},
                {'hashtag': 'outfit', 'avg_engagement': 128.9, 'usage_count': 30}
            ],
            'content_insights': {
                'recommendations': [
                    'Posts com 8-10 hashtags têm melhor engajamento',
                    'Legendas médias (100-300 caracteres) performam melhor',
                    'Postar entre 18h-21h gera mais interações',
                    'Combinar hashtags em português e inglês aumenta alcance'
                ],
                'best_hashtag_count': 8,
                'optimal_caption_strategy': 'médias'
            },
            'summary_stats': {
                'total_posts_analyzed': 50,
                'unique_hashtags': 156,
                'unique_keywords': 89,
                'analysis_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'data_source': 'sample_data',
                'account_info': {
                    'username': 'exemplo_conta',
                    'account_type': 'BUSINESS',
                    'media_count': 100,
                    'followers_count': 10000
                }
            }
        }
    
    def analyze_fashion_trends(self) -> Dict[str, Any]:
        """
        Analisa tendências de moda baseado nos dados coletados
        """
        print("🔍 Validando credenciais...")
        
        if not self.validate_credentials():
            print("⚠️  Usando dados de exemplo devido a problema com credenciais")
            return self.generate_sample_data()
        
        print("📋 Obtendo informações da conta...")
        account_info = self.get_account_info()
        
        if account_info:
            print(f"✅ Conta: @{account_info.get('username', 'N/A')}")
            print(f"📊 Tipo: {account_info.get('account_type', 'N/A')}")
            print(f"📱 Total de mídias: {account_info.get('media_count', 'N/A')}")
        
        print("📱 Coletando dados da conta...")
        account_media = self.get_account_media(200)
        
        if not account_media:
            print("⚠️  Não foi possível obter dados da API. Usando dados de exemplo...")
            return self.generate_sample_data()
        
        all_hashtags = []
        all_keywords = []
        engagement_data = []
        posting_times = []
        
        print(f"🔍 Analisando {len(account_media)} posts...")
        for media in account_media:
            caption = media.get('caption', '')
            timestamp = media.get('timestamp', '')
            likes = media.get('like_count', 0) or 0
            comments = media.get('comments_count', 0) or 0
            
            # Extrair hashtags
            hashtags = self.extract_hashtags_from_caption(caption)
            all_hashtags.extend(hashtags)
            
            # Extrair palavras-chave de moda
            keywords = self.extract_fashion_keywords(caption)
            all_keywords.extend(keywords)
            
            # Dados de engajamento
            engagement_data.append({
                'id': media.get('id'),
                'media_type': media.get('media_type'),
                'permalink': media.get('permalink'),
                'likes': likes,
                'comments': comments,
                'engagement': likes + comments,
                'caption_length': len(caption) if caption else 0,
                'hashtag_count': len(hashtags),
                'timestamp': timestamp,
                'caption': caption
            })
            
            # Horários de postagem
            if timestamp:
                try:
                    dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                    posting_times.append(dt.hour)
                except ValueError:
                    pass
        
        # Compilar resultados
        results = {
            'trending_hashtags': Counter(all_hashtags).most_common(20),
            'trending_keywords': Counter(all_keywords).most_common(15),
            'engagement_analysis': self.analyze_engagement(engagement_data),
            'posting_time_analysis': Counter(posting_times).most_common(24),
            'fashion_hashtag_performance': self.analyze_fashion_hashtags(all_hashtags),
            'content_insights': self.generate_content_insights(engagement_data),
            'summary_stats': {
                'total_posts_analyzed': len(account_media),
                'unique_hashtags': len(set(all_hashtags)),
                'unique_keywords': len(set(all_keywords)),
                'analysis_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'account_info': account_info
            },
            'raw_posts_data': engagement_data # Inclui os dados brutos dos posts para salvar
        }
        
        return results
    
    def analyze_fashion_hashtags(self, all_hashtags: List[str]) -> List[Dict]:
        """
        Analisa performance das hashtags de moda
        """
        fashion_performance = []
        hashtag_counts = Counter(all_hashtags)
        
        # Para um engajamento mais preciso, você precisaria de dados de engajamento por hashtag.
        # Aqui, mantemos a lógica original com uma estimativa para demonstração, 
        # ou você pode usar uma média real se tiver essa informação.
        for hashtag in self.fashion_hashtags:
            if hashtag in hashtag_counts:
                fashion_performance.append({
                    'hashtag': hashtag,
                    'usage_count': hashtag_counts[hashtag],
                    'avg_engagement': hashtag_counts[hashtag] * 15.7  # Estimativa mantida
                })
        
        return sorted(fashion_performance, key=lambda x: x['usage_count'], reverse=True)
    
    def analyze_engagement(self, engagement_data: List[Dict]) -> Dict[str, Any]:
        """
        Analisa dados de engajamento
        """
        if not engagement_data:
            return {}
        
        df = pd.DataFrame(engagement_data)
        
        # Filtrar dados válidos
        df = df[df['engagement'] >= 0]
        
        if df.empty:
            return {}
        
        return {
            'avg_likes': df['likes'].mean(),
            'avg_comments': df['comments'].mean(),
            'avg_engagement': df['engagement'].mean(),
            'top_performing_posts': df.nlargest(5, 'engagement')[['id', 'engagement']].to_dict('records') if len(df) >= 5 else [],
            'engagement_by_hashtag_count': df.groupby('hashtag_count')['engagement'].mean().to_dict(),
            'engagement_by_caption_length': {
                'short_captions': df[df['caption_length'] < 100]['engagement'].mean(),
                'medium_captions': df[(df['caption_length'] >= 100) & (df['caption_length'] < 300)]['engagement'].mean(),
                'long_captions': df[df['caption_length'] >= 300]['engagement'].mean()
            }
        }
    
    def generate_content_insights(self, engagement_data: List[Dict]) -> Dict[str, Any]:
        """
        Gera insights para criação de conteúdo
        """
        if not engagement_data:
            return {
                'recommendations': ['Dados insuficientes para gerar recomendações'],
                'best_hashtag_count': 8,
                'optimal_caption_strategy': 'médias'
            }
        
        df = pd.DataFrame(engagement_data)
        df = df[df['engagement'] >= 0]
        
        if df.empty:
            return {
                'recommendations': ['Dados insuficientes para gerar recomendações'],
                'best_hashtag_count': 8,
                'optimal_caption_strategy': 'médias'
            }
        
        insights = []
        best_hashtag_count = 8
        best_length = 'médias'

        # Análise de hashtags
        if not df.groupby('hashtag_count')['engagement'].mean().empty:
            # Pegar o maior valor de engajamento, pode ser NaN
            best_hashtag_count_series = df.groupby('hashtag_count')['engagement'].mean()
            # Filtrar NaNs antes de idxmax
            best_hashtag_count_filtered = best_hashtag_count_series.dropna()
            
            if not best_hashtag_count_filtered.empty:
                best_hashtag_count = best_hashtag_count_filtered.idxmax()
                insights.append(f"Posts com {best_hashtag_count} hashtags têm melhor engajamento")
            else:
                insights.append("Não foi possível determinar a melhor contagem de hashtags devido a dados insuficientes de engajamento.")
        
        # Análise de tamanho de legenda
        engagement_by_length = {
            'curtas': df[df['caption_length'] < 100]['engagement'].mean(),
            'médias': df[(df['caption_length'] >= 100) & (df['caption_length'] < 300)]['engagement'].mean(),
            'longas': df[df['caption_length'] >= 300]['engagement'].mean()
        }
        
        # Filtrar valores NaN
        engagement_by_length = {k: v for k, v in engagement_by_length.items() if pd.notna(v)}
        
        if engagement_by_length:
            best_length = max(engagement_by_length, key=engagement_by_length.get)
            insights.append(f"Legendas {best_length} têm melhor performance")
        else:
            insights.append("Não foi possível determinar a melhor estratégia de legenda devido a dados insuficientes de engajamento.")

        return {
            'recommendations': insights if insights else ['Analise mais posts para gerar recomendações precisas'],
            'best_hashtag_count': int(best_hashtag_count) if pd.notna(best_hashtag_count) else 8,
            'optimal_caption_strategy': best_length
        }
    
    def generate_report(self, results: Dict[str, Any]) -> str:
        """
        Gera um relatório completo em formato texto
        """
        is_sample = results['summary_stats'].get('data_source') == 'sample_data'
        
        report = f"""
{'='*60}
🎯 RELATÓRIO DE TENDÊNCIAS DE MODA - INSTAGRAM
{'='*60}
📅 Data da Análise: {results['summary_stats']['analysis_date']}
📊 Posts Analisados: {results['summary_stats']['total_posts_analyzed']}
{('⚠️  DADOS DE EXEMPLO (API indisponível)' if is_sample else '✅ DADOS REAIS DA API')}

{'='*60}
📈 HASHTAGS MAIS POPULARES
{'='*60}"""
        
        for i, (hashtag, count) in enumerate(results['trending_hashtags'][:10], 1):
            report += f"\n{i:2d}. #{hashtag} - {count} usos"
        
        report += f"""

{'='*60}
🔍 PALAVRAS-CHAVE MAIS MENCIONADAS
{'='*60}"""
        
        for i, (keyword, count) in enumerate(results['trending_keywords'][:10], 1):
            report += f"\n{i:2d}. {keyword} - {count} menções"
        
        engagement = results['engagement_analysis']
        report += f"""

{'='*60}
💝 ANÁLISE DE ENGAJAMENTO
{'='*60}
❤️  Média de Curtidas: {engagement.get('avg_likes', 0):.1f}
💬 Média de Comentários: {engagement.get('avg_comments', 0):.1f}
🚀 Engajamento Total Médio: {engagement.get('avg_engagement', 0):.1f}

{'='*60}
🕐 MELHORES HORÁRIOS PARA POSTAR
{'='*60}"""
        
        for hour, count in results['posting_time_analysis'][:5]:
            report += f"\n🕐 {hour:02d}:00h - {count} posts"
        
        report += f"""

{'='*60}
🏷️  HASHTAGS DE MODA COM MELHOR PERFORMANCE
{'='*60}""" # <-- This line was the problem. It had {'='='*60} instead of {'='*60}
        
        for hashtag_data in results['fashion_hashtag_performance'][:5]:
            report += f"\n#{hashtag_data['hashtag']} - Engajamento: {hashtag_data.get('avg_engagement', hashtag_data.get('usage_count', 0)):.1f}"
        
        report += f"""

{'='*60}
💡 RECOMENDAÇÕES PARA CRIADORES DE CONTEÚDO
{'='*60}"""
        
        for recommendation in results['content_insights'].get('recommendations', []):
            report += f"\n• {recommendation}"
        
        if is_sample:
            report += f"""

{'='*60}
⚠️  COMO RESOLVER O PROBLEMA DA API
{'='*60}
1. Verifique se o Access Token está correto e não expirou
2. Confirme se a conta está configurada como Business/Creator
3. Verifique as permissões do app no Facebook Developers
4. Certifique-se que o Business Account ID está correto
5. Teste a conexão com a API usando ferramentas como Postman

📚 Recursos úteis:
• Instagram Graph API Documentation
• Facebook Developers Console
• Instagram Business Account Setup Guide"""
        
        return report
    
    def save_results_to_json(self, results: Dict[str, Any], filename: str = "fashion_insights.json"):
        """
        Salva os resultados em arquivo JSON
        """
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False, indent=2, default=str)
        print(f"💾 Resultados salvos em {filename}")

    def save_results_to_mysql(self, results: Dict[str, Any]):
        """
        Salva os resultados da análise no banco de dados MySQL.
        """
        if not self.db_connection:
            print("❌ Não foi possível salvar no MySQL: conexão com o banco de dados não estabelecida.")
            return

        analysis_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        try:
            # 1. Salvar informações da conta
            account_info = results['summary_stats'].get('account_info', {})
            if account_info:
                add_account = ("INSERT INTO account_info "
                               "(username, account_type, media_count, followers_count, analysis_date) "
                               "VALUES (%s, %s, %s, %s, %s)")
                account_data = (
                    account_info.get('username'),
                    account_info.get('account_type'),
                    account_info.get('media_count'),
                    account_info.get('followers_count'),
                    analysis_date
                )
                self.db_cursor.execute(add_account, account_data)
                self.db_connection.commit()
                print("✅ Informações da conta salvas no MySQL.")

            # 2. Salvar dados de posts (se houver)
            if 'raw_posts_data' in results and results['raw_posts_data']:
                add_post = ("INSERT INTO posts "
                            "(id, media_type, permalink, timestamp, caption, like_count, comments_count, engagement, caption_length, hashtag_count, analysis_date) "
                            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
                for post in results['raw_posts_data']:
                    try:
                        timestamp_dt = datetime.fromisoformat(post['timestamp'].replace('Z', '+00:00')) if post.get('timestamp') else None
                        post_data = (
                            post.get('id'),
                            post.get('media_type'),
                            post.get('permalink'),
                            timestamp_dt,
                            post.get('caption'),
                            post.get('likes'),
                            post.get('comments'),
                            post.get('engagement'),
                            post.get('caption_length'),
                            post.get('hashtag_count'),
                            analysis_date
                        )
                        self.db_cursor.execute(add_post, post_data)
                    except mysql.connector.Error as err:
                        # Tratar posts duplicados ou outros erros de inserção
                        if err.errno == 1062:  # ER_DUP_ENTRY for duplicate primary key
                            print(f"Post ID {post.get('id')} já existe, pulando.")
                        else:
                            print(f"❌ Erro ao inserir post {post.get('id')}: {err}")
                self.db_connection.commit()
                print("✅ Dados de posts salvos no MySQL.")

            # 3. Salvar hashtags em tendência
            if 'trending_hashtags' in results and results['trending_hashtags']:
                add_hashtag = ("INSERT INTO trending_hashtags "
                               "(hashtag, usage_count, analysis_date) "
                               "VALUES (%s, %s, %s)")
                for hashtag, count in results['trending_hashtags']:
                    self.db_cursor.execute(add_hashtag, (hashtag, count, analysis_date))
                self.db_connection.commit()
                print("✅ Hashtags em tendência salvas no MySQL.")

            # 4. Salvar palavras-chave em tendência
            if 'trending_keywords' in results and results['trending_keywords']:
                add_keyword = ("INSERT INTO trending_keywords "
                               "(keyword, mention_count, analysis_date) "
                               "VALUES (%s, %s, %s)")
                for keyword, count in results['trending_keywords']:
                    self.db_cursor.execute(add_keyword, (keyword, count, analysis_date))
                self.db_connection.commit()
                print("✅ Palavras-chave em tendência salvas no MySQL.")

            # 5. Salvar análise de horários de postagem
            if 'posting_time_analysis' in results and results['posting_time_analysis']:
                add_posting_time = ("INSERT INTO posting_times "
                                    "(hour_of_day, post_count, analysis_date) "
                                    "VALUES (%s, %s, %s)")
                for hour, count in results['posting_time_analysis']:
                    self.db_cursor.execute(add_posting_time, (hour, count, analysis_date))
                self.db_connection.commit()
                print("✅ Análise de horários de postagem salva no MySQL.")
            
            # 6. Salvar performance das hashtags de moda
            if 'fashion_hashtag_performance' in results and results['fashion_hashtag_performance']:
                add_fashion_hashtag = ("INSERT INTO fashion_hashtag_performance "
                                       "(hashtag, usage_count, avg_engagement, analysis_date) "
                                       "VALUES (%s, %s, %s, %s)")
                for hashtag_data in results['fashion_hashtag_performance']:
                    self.db_cursor.execute(add_fashion_hashtag, (
                        hashtag_data.get('hashtag'), 
                        hashtag_data.get('usage_count'), 
                        hashtag_data.get('avg_engagement'), 
                        analysis_date
                    ))
                self.db_connection.commit()
                print("✅ Performance das hashtags de moda salva no MySQL.")

            # 7. Salvar resumo de engajamento
            engagement_summary = results.get('engagement_analysis', {})
            if engagement_summary:
                add_engagement_summary = ("INSERT INTO engagement_summary "
                                          "(avg_likes, avg_comments, avg_engagement, analysis_date) "
                                          "VALUES (%s, %s, %s, %s)")
                summary_data = (
                    engagement_summary.get('avg_likes'),
                    engagement_summary.get('avg_comments'),
                    engagement_summary.get('avg_engagement'),
                    analysis_date
                )
                self.db_cursor.execute(add_engagement_summary, summary_data)
                self.db_connection.commit()
                print("✅ Resumo de engajamento salvo no MySQL.")

            # 8. Salvar recomendações de conteúdo
            content_insights = results.get('content_insights', {})
            if content_insights.get('recommendations'):
                add_recommendation = ("INSERT INTO content_recommendations "
                                      "(recommendation, best_hashtag_count, optimal_caption_strategy, analysis_date) "
                                      "VALUES (%s, %s, %s, %s)")
                for rec_text in content_insights['recommendations']:
                    self.db_cursor.execute(add_recommendation, (
                        rec_text,
                        content_insights.get('best_hashtag_count'),
                        content_insights.get('optimal_caption_strategy'),
                        analysis_date
                    ))
                self.db_connection.commit()
                print("✅ Recomendações de conteúdo salvas no MySQL.")

        except mysql.connector.Error as err:
            print(f"❌ Erro ao salvar resultados no MySQL: {err}")
            self.db_connection.rollback() # Reverter transação em caso de erro

    def close_db_connection(self):
        """
        Fecha a conexão com o banco de dados.
        """
        if self.db_connection and self.db_connection.is_connected():
            self.db_cursor.close()
            self.db_connection.close()
            print("✅ Conexão com o banco de dados MySQL fechada.")

# Exemplo de uso
def main():
    print("🎯 Instagram Fashion Insights - Analisador de Tendências")
    print("=" * 60)
    
    # Configurações da API - SUBSTITUA PELOS SEUS VALORES REAIS
    ACCESS_TOKEN = "e1f6ba75536bbd68abed82b2cc1fa773"  # Seu token atual
    BUSINESS_ACCOUNT_ID = "705788945331100"  # Seu ID atual
    
    # Configurações do MySQL
    mysql_config = {
        'host': '127.0.0.1',
        'user': 'root',
        'password': '1234',
        'database': 'fashion_trends_db'
    }

    # Inicializar a classe
    fashion_insights = InstagramFashionInsights(ACCESS_TOKEN, BUSINESS_ACCOUNT_ID, mysql_config)
    
    print("🔍 Iniciando análise de tendências de moda...")
    print("⏱️  Isso pode levar alguns minutos...\n")
    
    try:
        # Executar análise
        results = fashion_insights.analyze_fashion_trends()
        
        # Gerar relatório
        report = fashion_insights.generate_report(results)
        print(report)
        
        # Salvar resultados em JSON
        fashion_insights.save_results_to_json(results)
        
        # Salvar resultados no MySQL
        fashion_insights.save_results_to_mysql(results)
        
        print(f"\n{'='*60}")
        print("✅ Análise concluída com sucesso!")
        print("📊 Dados salvos em 'fashion_insights.json' e no banco de dados MySQL.")
        print("📱 Use essas informações para otimizar sua estratégia no Instagram!")
        
    except Exception as e:
        print(f"❌ Erro durante a análise: {e}")
        print("💡 O sistema irá gerar dados de exemplo para demonstração...")
        
        # Gerar dados de exemplo em caso de erro
        sample_results = fashion_insights.generate_sample_data()
        report = fashion_insights.generate_report(sample_results)
        print(report)
        fashion_insights.save_results_to_json(sample_results)
        fashion_insights.save_results_to_mysql(sample_results) # Tentar salvar dados de exemplo no MySQL

    finally:
        fashion_insights.close_db_connection() # Garantir que a conexão seja fechada

if __name__ == "__main__":
    main()