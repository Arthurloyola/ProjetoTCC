import requests
import csv
import os
import re
from datetime import datetime
from collections import Counter

# Funções existentes mantidas do seu código original
def search_fashion_trends(api_key, num_results=30):
    """
    Busca tendências de moda usando a SerpAPI e retorna os resultados
    
    Args:
        api_key (str): Sua chave de API da SerpAPI
        num_results (int): Número de resultados a serem obtidos
        
    Returns:
        list: Lista de dicionários com os dados das tendências
    """
    base_url = "https://serpapi.com/search"
    
    # Parâmetros para a busca
    params = {
        "q": "fashion trends 2025",
        "api_key": api_key,
        "engine": "google",
        "gl": "us",
        "hl": "en",
        "num": num_results
    }
    
    print("Buscando tendências de moda...")
    response = requests.get(base_url, params=params)
    
    if response.status_code != 200:
        print(f"Erro na requisição: {response.status_code}")
        return []
    
    data = response.json()
    
    # Extrair resultados orgânicos
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
    
    # Extrair resultados de notícias se disponíveis
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
    
    return results

def get_top_fashion_keywords(api_key, num_keywords=10):
    """
    Obtém as palavras-chave mais pesquisadas relacionadas à moda
    usando a API de sugestões de pesquisa do Google via SerpAPI
    
    Args:
        api_key (str): Sua chave de API da SerpAPI
        num_keywords (int): Número de palavras-chave a retornar
        
    Returns:
        list: Lista das top palavras-chave mais pesquisadas
    """
    print("Buscando top palavras-chave de moda...")
    
    # Primeira busca - termos relacionados a "fashion trends"
    params_trends = {
        "q": "fashion trends",
        "api_key": api_key,
        "engine": "google_autocomplete"
    }
    
    response_trends = requests.get("https://serpapi.com/search", params=params_trends)
    
    # Segunda busca - termos relacionados a "fashion"
    params_fashion = {
        "q": "fashion",
        "api_key": api_key,
        "engine": "google_autocomplete"
    }
    
    response_fashion = requests.get("https://serpapi.com/search", params=params_fashion)
    
    # Terceira busca - termos relacionados a "fashion 2025"
    params_2025 = {
        "q": "fashion 2025",
        "api_key": api_key,
        "engine": "google_autocomplete"
    }
    
    response_2025 = requests.get("https://serpapi.com/search", params=params_2025)
    
    keywords = []
    
    # Processar resultados da primeira busca
    if response_trends.status_code == 200:
        data = response_trends.json()
        if "suggestions" in data:
            for suggestion in data["suggestions"]:
                keywords.append(suggestion.get("value", ""))
    
    # Processar resultados da segunda busca
    if response_fashion.status_code == 200:
        data = response_fashion.json()
        if "suggestions" in data:
            for suggestion in data["suggestions"]:
                keywords.append(suggestion.get("value", ""))
    
    # Processar resultados da terceira busca
    if response_2025.status_code == 200:
        data = response_2025.json()
        if "suggestions" in data:
            for suggestion in data["suggestions"]:
                keywords.append(suggestion.get("value", ""))
    
    # Contar ocorrência de palavras significativas nas keywords
    all_words = []
    for keyword in keywords:
        # Limpar e extrair palavras significativas (ignorar palavras comuns como "the", "and", etc)
        words = re.findall(r'\b[a-zA-Z]{3,}\b', keyword.lower())
        # Filtrar palavras muito comuns
        stopwords = ["the", "and", "for", "with", "what", "how", "are", "can", "from", "fashion"]
        filtered_words = [word for word in words if word not in stopwords]
        all_words.extend(filtered_words)
    
    # Contar e obter as top palavras-chave
    word_counts = Counter(all_words)
    top_words = word_counts.most_common(num_keywords)
    
    # Formatar os resultados
    top_keywords = [{"palavra": word, "contagem": count} for word, count in top_words]
    
    return top_keywords

# Novas funções para integração com Power BI

def create_powerbi_friendly_csv(trend_data, top_keywords, output_dir=None):
    """
    Cria arquivos CSV formatados para serem facilmente importados pelo Power BI
    
    Args:
        trend_data (list): Lista de dicionários com os dados das tendências
        top_keywords (list): Lista de dicionários com as palavras-chave mais populares
        output_dir (str, optional): Diretório de saída para os arquivos
        
    Returns:
        tuple: Caminhos dos arquivos salvos (trends_file, keywords_file)
    """
    if output_dir is None:
        output_dir = "powerbi_data"
    
    # Criar diretório se não existir
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    current_date = datetime.now().strftime("%Y%m%d")
    
    # Arquivo para tendências
    trends_file = os.path.join(output_dir, f"fashion_trends_{current_date}.csv")
    
    # Arquivo para palavras-chave
    keywords_file = os.path.join(output_dir, f"fashion_keywords_{current_date}.csv")
    
    # Salvar dados de tendências em formato adequado para Power BI
    if trend_data:
        # Adicionar uma coluna com ID e data de coleta para melhorar a análise no Power BI
        for i, item in enumerate(trend_data):
            item["id"] = i + 1
            item["data_coleta"] = datetime.now().strftime("%Y-%m-%d")
        
        headers = trend_data[0].keys()
        
        print(f"Salvando {len(trend_data)} resultados de tendências para Power BI em {trends_file}...")
        
        with open(trends_file, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=headers)
            writer.writeheader()
            writer.writerows(trend_data)
    
    # Salvar dados de palavras-chave em formato adequado para Power BI
    if top_keywords:
        # Adicionar data de coleta e ID para melhorar a análise no Power BI
        for i, item in enumerate(top_keywords):
            item["id"] = i + 1
            item["data_coleta"] = datetime.now().strftime("%Y-%m-%d")
            item["ranking"] = i + 1  # Adicionar posição no ranking
        
        print(f"Salvando {len(top_keywords)} palavras-chave para Power BI em {keywords_file}...")
        
        with open(keywords_file, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=["id", "palavra", "contagem", "ranking", "data_coleta"])
            writer.writeheader()
            writer.writerows(top_keywords)
    
    return trends_file, keywords_file

def create_relationships_file(output_dir=None):
    """
    Cria um arquivo CSV com informações sobre os relacionamentos entre as tabelas
    para facilitar a modelagem de dados no Power BI
    
    Args:
        output_dir (str, optional): Diretório de saída para os arquivos
        
    Returns:
        str: Caminho do arquivo de relacionamentos
    """
    if output_dir is None:
        output_dir = "powerbi_data"
    
    # Criar diretório se não existir
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    rel_file = os.path.join(output_dir, "relationships_info.csv")
    
    relationships = [
        {"from_table": "fashion_trends", "from_column": "data_coleta", "to_table": "fashion_keywords", "to_column": "data_coleta", "relationship": "many_to_many"}
    ]
    
    print(f"Criando arquivo de informações de relacionamento em {rel_file}...")
    
    with open(rel_file, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=["from_table", "from_column", "to_table", "to_column", "relationship"])
        writer.writeheader()
        writer.writerows(relationships)
    
    return rel_file

def create_powerbi_metadata(output_dir=None):
    """
    Cria um arquivo de metadados com informações úteis para configurar o Power BI
    
    Args:
        output_dir (str, optional): Diretório de saída para os arquivos
        
    Returns:
        str: Caminho do arquivo de metadados
    """
    if output_dir is None:
        output_dir = "powerbi_data"
    
    # Criar diretório se não existir
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    metadata_file = os.path.join(output_dir, "metadata.txt")
    
    metadata_content = """# Metadados para importação no Power BI

## Estrutura das tabelas

### Tabela: fashion_trends
- id: Identificador único da tendência
- título: Título da tendência encontrada
- link: URL da fonte
- snippet: Trecho de texto descritivo
- data: Data da tendência (se disponível)
- fonte: Fonte da informação (apenas para resultados de notícias)
- data_coleta: Data em que os dados foram coletados

### Tabela: fashion_keywords
- id: Identificador único da palavra-chave
- palavra: A palavra-chave de moda
- contagem: Número de vezes que a palavra apareceu nas sugestões de pesquisa
- ranking: Posição no ranking de popularidade
- data_coleta: Data em que os dados foram coletados

## Sugestões de visualizações

1. Nuvem de palavras com as palavras-chave mais populares
2. Gráfico de barras mostrando as contagens das top 10 palavras-chave
3. Linha do tempo com as tendências organizadas por data
4. Mapa de calor relacionando palavras-chave e temas das tendências
5. Cartões com métricas principais (número de tendências, palavra-chave mais comum)

## Atualização dos dados

- Os dados são baseados em pesquisas realizadas através da SerpAPI
- Atualize regularmente para acompanhar mudanças nas tendências
- A coluna data_coleta permite acompanhar a evolução das tendências ao longo do tempo
"""
    
    print(f"Criando arquivo de metadados em {metadata_file}...")
    
    with open(metadata_file, 'w', encoding='utf-8') as f:
        f.write(metadata_content)
    
    return metadata_file

def prepare_data_for_powerbi(api_key, output_dir=None, num_results=30, num_keywords=10):
    """
    Função principal para preparar todos os dados para o Power BI
    
    Args:
        api_key (str): Sua chave de API da SerpAPI
        output_dir (str, optional): Diretório de saída para os arquivos
        num_results (int): Número de resultados de tendências desejados
        num_keywords (int): Número de palavras-chave a retornar
        
    Returns:
        tuple: Diretório de saída e lista de arquivos criados
    """
    if output_dir is None:
        output_dir = "powerbi_data"
    
    try:
        # Obter dados de tendências
        trend_data = search_fashion_trends(api_key, num_results)
        
        # Obter top palavras-chave
        top_keywords = get_top_fashion_keywords(api_key, num_keywords)
        
        if not trend_data and not top_keywords:
            print("Não foi possível obter dados sobre moda")
            return None, []
        
        # Lista para armazenar caminhos de todos os arquivos criados
        all_files = []
        
        # Criar arquivos CSV amigáveis para Power BI
        trends_file, keywords_file = create_powerbi_friendly_csv(trend_data, top_keywords, output_dir)
        if trends_file:
            all_files.append(trends_file)
        if keywords_file:
            all_files.append(keywords_file)
        
        # Criar arquivo de relacionamentos
        rel_file = create_relationships_file(output_dir)
        all_files.append(rel_file)
        
        # Criar arquivo de metadados
        metadata_file = create_powerbi_metadata(output_dir)
        all_files.append(metadata_file)
        
        # Manter a funcionalidade original também
        # Criar o arquivo combinado para referência
        combined_file = os.path.join(output_dir, f"relatorio_completo_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
        
        with open(combined_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            
            # Seção de palavras-chave
            writer.writerow(["SEÇÃO: TOP PALAVRAS-CHAVE DE MODA"])
            writer.writerow(["Palavra", "Contagem", "Ranking"])
            for i, keyword in enumerate(top_keywords, 1):
                writer.writerow([keyword["palavra"], keyword["contagem"], i])
            
            # Linha em branco entre seções
            writer.writerow([])
            writer.writerow([])
            
            # Seção de tendências
            writer.writerow(["SEÇÃO: TENDÊNCIAS DE MODA"])
            if trend_data:
                headers = trend_data[0].keys()
                writer.writerow(headers)
                for trend in trend_data:
                    writer.writerow(trend.values())
        
        all_files.append(combined_file)
        
        print("\n=== ARQUIVOS PREPARADOS PARA POWER BI ===")
        for file in all_files:
            print(f"✓ {file}")
        
        print(f"\nTodos os arquivos foram salvos no diretório: {os.path.abspath(output_dir)}")
        print("\nInstruções para importar no Power BI:")
        print("1. Abra o Power BI Desktop")
        print("2. Clique em 'Obter dados' > 'Texto/CSV'")
        print("3. Selecione os arquivos CSV gerados")
        print("4. Siga as instruções no arquivo de metadados para configuração")
        
        return output_dir, all_files
        
    except Exception as e:
        print(f"Erro ao processar dados para Power BI: {str(e)}")
        return None, []

# Manter as funções originais do seu código
def save_to_csv(trend_data, top_keywords, filename=None):
    # [Seu código original da função]
    if not trend_data and not top_keywords:
        print("Nenhum dado para salvar")
        return None, None
    
    if filename is None:
        current_date = datetime.now().strftime("%Y%m%d_%H%M%S")
        base_filename = f"moda_{current_date}"
    else:
        base_filename = filename
    
    trends_file = f"{base_filename}_tendencias.csv"
    keywords_file = f"{base_filename}_top_keywords.csv"
    
    # Salvar dados de tendências
    if trend_data:
        # Determinar os cabeçalhos com base no primeiro item
        headers = trend_data[0].keys()
        
        print(f"Salvando {len(trend_data)} resultados de tendências em {trends_file}...")
        
        with open(trends_file, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=headers)
            writer.writeheader()
            writer.writerows(trend_data)
    
    # Salvar dados de palavras-chave
    if top_keywords:
        print(f"Salvando {len(top_keywords)} palavras-chave mais pesquisadas em {keywords_file}...")
        
        with open(keywords_file, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=["palavra", "contagem"])
            writer.writeheader()
            writer.writerows(top_keywords)
    
    return trends_file, keywords_file

def run_fashion_research(api_key, filename=None, num_results=30, num_keywords=10):
    # [Seu código original da função]
    try:
        # Obter dados de tendências
        trend_data = search_fashion_trends(api_key, num_results)
        
        # Obter top palavras-chave
        top_keywords = get_top_fashion_keywords(api_key, num_keywords)
        
        if not trend_data and not top_keywords:
            print("Não foi possível obter dados sobre moda")
            return None, None
        
        # Salvar dados
        trends_file, keywords_file = save_to_csv(trend_data, top_keywords, filename)
        
        # Exibir resumo dos resultados
        print("\n=== RESUMO DA PESQUISA DE MODA ===")
        
        if trends_file:
            print(f"✓ Tendências de moda salvas em: {trends_file}")
        else:
            print("✗ Não foi possível obter tendências de moda")
        
        if keywords_file:
            print(f"✓ Top {num_keywords} palavras-chave salvas em: {keywords_file}")
            print("\nTop 10 palavras-chave relacionadas à moda:")
            for i, keyword in enumerate(top_keywords[:10], 1):
                print(f"{i}. {keyword['palavra']} ({keyword['contagem']} ocorrências)")
        else:
            print("✗ Não foi possível obter palavras-chave de moda")
        
        return trends_file, keywords_file
    
    except Exception as e:
        print(f"Erro ao processar: {str(e)}")
        return None, None

def combine_results_csv(trends_file, keywords_file, output_file=None):
    # [Seu código original da função]
    if not trends_file or not keywords_file:
        print("Arquivos insuficientes para combinar")
        return None
    
    if output_file is None:
        current_date = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = f"relatorio_moda_completo_{current_date}.csv"
    
    print(f"Combinando resultados em {output_file}...")
    
    # Ler dados de tendências
    trends_data = []
    with open(trends_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        trends_data = list(reader)
    
    # Ler dados de palavras-chave
    keywords_data = []
    with open(keywords_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        keywords_data = list(reader)
    
    # Criar um único CSV com duas seções
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        
        # Seção de palavras-chave
        writer.writerow(["SEÇÃO: TOP PALAVRAS-CHAVE DE MODA"])
        writer.writerow(["Palavra", "Contagem"])
        for keyword in keywords_data:
            writer.writerow([keyword["palavra"], keyword["contagem"]])
        
        # Linha em branco entre seções
        writer.writerow([])
        writer.writerow([])
        
        # Seção de tendências
        writer.writerow(["SEÇÃO: TENDÊNCIAS DE MODA"])
        if trends_data:
            headers = trends_data[0].keys()
            writer.writerow(headers)
            for trend in trends_data:
                writer.writerow(trend.values())
    
    print(f"Arquivo combinado salvo como {output_file}")
    return output_file

if __name__ == "__main__":
    # Substitua com sua chave de API real da SerpAPI
    API_KEY = "711715b0dfcaae2c9a68f87fefa693140450e46c7c00f64ac0b3ed9f05a6e6b0"
    
    # Opção 1: Executar o código original que você já estava usando
    # trends_file, keywords_file = run_fashion_research(API_KEY)
    # if trends_file and keywords_file:
    #     combined_file = combine_results_csv(trends_file, keywords_file)
    
    # Opção 2: Executar a nova funcionalidade para preparar os dados para o Power BI
    output_dir, created_files = prepare_data_for_powerbi(API_KEY)