import requests
import datetime
import pandas as pd
import time

# ===================== CONFIG =====================
GITHUB_TOKEN = ""  # substitua pelo seu token
HEADERS = {"Authorization": f"Bearer {GITHUB_TOKEN}"}
GRAPHQL_URL = "https://api.github.com/graphql"

# ===================== QUERY =====================
QUERY = """
query($queryString: String!) {
  search(query: $queryString, type: REPOSITORY, first: 20) {
    edges {
      node {
        ... on Repository {
          nameWithOwner
          stargazerCount
          url
          defaultBranchRef {
            target {
              ... on Commit {
                history(first: 100) {
                  nodes {
                    committedDate
                  }
                }
              }
            }
          }
        }
      }
    }
  }
}
"""

# ===================== FUNÇÕES =====================
def run_query(query, variables):
    """Executa uma query GraphQL e trata erros e rate limit."""
    while True:
        response = requests.post(
            GRAPHQL_URL,
            json={"query": query, "variables": variables},
            headers=HEADERS,
        )
        if response.status_code == 200:
            data = response.json()
            if "errors" in data:
                print("⚠️ Erro na query:", data["errors"])
                return None
            return data["data"]
        elif response.status_code == 502:
            print("⚠️ Erro 502 do GitHub. Tentando novamente...")
            time.sleep(5)
        elif response.status_code == 403:
            print("⏳ Atingido rate limit. Aguardando 60 segundos...")
            time.sleep(60)
        else:
            print(f"❌ Erro {response.status_code}: {response.text}")
            return None

def detect_inactivity_periods(commit_dates, threshold_days=180):
    """Retorna lista de períodos de inatividade (tuplas de início, fim)."""
    commit_dates.sort()
    periods = []
    for i in range(1, len(commit_dates)):
        delta = (commit_dates[i] - commit_dates[i - 1]).days
        if delta >= threshold_days:
            periods.append((commit_dates[i - 1], commit_dates[i]))
    return periods

def analyze_repo(repo):
    """Analisa um repositório e identifica períodos de morte e ressurreição."""
    name = repo["nameWithOwner"]
    stars = repo["stargazerCount"]
    url = repo["url"]

    commits = repo.get("defaultBranchRef", {}).get("target", {}).get("history", {}).get("nodes", [])
    if not commits:
        return None

    commit_dates = [
        datetime.datetime.fromisoformat(c["committedDate"].replace("Z", "+00:00"))
        for c in commits
    ]
    commit_dates.sort()

    inactivity_periods = detect_inactivity_periods(commit_dates)

    if not inactivity_periods:
        return {
            "Nome": name,
            "Stargazers": stars,
            "URL": url,
            "Data de morte": None,
            "Data de ressurreição": None,
            "Morreu após reviver": 0,
        }

    morte = inactivity_periods[0][0].strftime("%Y-%m-%d")
    ressurreicao = inactivity_periods[0][1].strftime("%Y-%m-%d")

    morreu_de_novo = 1 if len(inactivity_periods) > 1 else 0

    return {
        "Nome": name,
        "Stargazers": stars,
        "URL": url,
        "Data de morte": morte,
        "Data de ressurreição": ressurreicao,
        "Morreu após reviver": morreu_de_novo,
    }

def get_monthly_intervals(year):
    """Gera tuplas (início, fim) para cada mês do ano."""
    intervals = []
    for month in range(1, 13):
        start = datetime.date(year, month, 1)
        if month == 12:
            end = datetime.date(year + 1, 1, 1) - datetime.timedelta(days=1)
        else:
            end = datetime.date(year, month + 1, 1) - datetime.timedelta(days=1)
        intervals.append((start, end))
    return intervals

# ===================== MAIN =====================
def main():
    year = 2022
    intervals = get_monthly_intervals(year)
    all_results = []

    print(f"🔍 Iniciando varredura do ano de {year} (mês a mês)...")

    for start, end in intervals:
        query_string = f"created:{start}..{end} sort:stars"
        print(f"➡️  Consultando repositórios criados entre {start} e {end}...")

        data = run_query(QUERY, {"queryString": query_string})
        if not data:
            print(f"⚠️ Nenhum dado retornado para {start}..{end}")
            continue

        for edge in data["search"]["edges"]:
            repo = edge["node"]
            analysis = analyze_repo(repo)
            if analysis:
                all_results.append(analysis)

        # Espera pequena para evitar rate limit
        time.sleep(2)

    # Remover duplicados por nome de repositório
    df = pd.DataFrame(all_results).drop_duplicates(subset=["Nome"])

    # Salvar resultados
    df.to_excel("repositorios_morte_ressurreicao_2022.xlsx", index=False)
    print(f"✅ Análise concluída! {len(df)} repositórios salvos em 'repositorios_morte_ressurreicao_2022.xlsx'.")

if __name__ == "__main__":
    main()
