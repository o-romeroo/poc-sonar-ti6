import requests
import datetime
import pandas as pd
import time
from tqdm import tqdm

# ===================== CONFIG =====================
GITHUB_TOKEN = ""
HEADERS = {"Authorization": f"Bearer {GITHUB_TOKEN}"}
GRAPHQL_URL = "https://api.github.com/graphql"

# ===================== QUERIES =====================
GRAPHQL_QUERY = """
query($queryString: String!) {
  search(query: $queryString, type: REPOSITORY, first: 20) {
    edges {
      node {
        ... on Repository {
          nameWithOwner
          stargazerCount
          url
          createdAt
          primaryLanguage { name }
          defaultBranchRef {
            target {
              ... on Commit {
                history(first: 100) {
                  nodes { committedDate }
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
    """Executa query GraphQL com retry e tratamento de rate limit."""
    for attempt in range(3):
        r = requests.post(GRAPHQL_URL, json={"query": query, "variables": variables}, headers=HEADERS)
        if r.status_code == 200:
            data = r.json()
            if "errors" in data:
                print("⚠️ Erro na query:", data["errors"])
                return None
            return data["data"]
        elif r.status_code == 502:
            print("⚠️ Erro 502 — retry em 5s...")
            time.sleep(5)
        elif r.status_code == 403:
            print("⏳ Rate limit atingido. Aguardando 60s...")
            time.sleep(60)
        else:
            print(f"❌ Erro {r.status_code}: {r.text[:150]}")
            time.sleep(10)
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
    """Analisa commits e identifica se o repositório morreu/ressuscitou."""
    name = repo["nameWithOwner"]
    stars = repo["stargazerCount"]
    url = repo["url"]
    lang = (repo.get("primaryLanguage") or {}).get("name", "N/A")

    commits = repo.get("defaultBranchRef", {}).get("target", {}).get("history", {}).get("nodes", [])
    if not commits:
        return None

    commit_dates = [
        datetime.datetime.fromisoformat(c["committedDate"].replace("Z", "+00:00"))
        for c in commits if c.get("committedDate")
    ]
    commit_dates.sort()
    inactivity_periods = detect_inactivity_periods(commit_dates)
    if not inactivity_periods:
        return None

    morte_dt, revive_dt = inactivity_periods[0]
    morreu_de_novo = 1 if len(inactivity_periods) > 1 else 0
    has_commits_after_revive = any(c > revive_dt for c in commit_dates)

    return {
        "Nome": name,
        "Linguagem": lang,
        "Stargazers": stars,
        "URL": url,
        "Data de morte": morte_dt.strftime("%Y-%m-%d"),
        "Data de ressurreição": revive_dt.strftime("%Y-%m-%d") if has_commits_after_revive else None,
        "Morreu após reviver": morreu_de_novo,
        "Commits analisados": len(commit_dates),
    }


# ===================== MAIN PIPELINE =====================
def main():
    print("🔍 Iniciando coleta — Fase 1: Validação limitada (1 token)")

    queries = {
        "Python": "stars:3000..60000 created:2016-01-01..2018-12-31 language:Python sort:stars-desc",
        "JavaScript": "stars:5000..40000 created:2016-01-01..2019-12-31 language:JavaScript sort:stars-desc",
        "Java": "stars:3000..25000 created:2015-01-01..2019-12-31 language:Java sort:stars-desc",
        "Geral": "stars:5000..40000 created:2016-01-01..2019-12-31 sort:stars-desc"
    }

    results_mortos, results_ressuscitados = [], []
    stats = {
        "Total coletados": 0,
        "Com commits válidos": 0,
        "Com períodos de inatividade": 0,
        "Mortos": 0,
        "Ressuscitados": 0,
    }

    for lang, query_string in queries.items():
        print(f"\n📦 Coletando {lang}...")
        data = run_query(GRAPHQL_QUERY, {"queryString": query_string})
        if not data:
            print(f"⚠️ Nenhum dado retornado para {lang}.")
            continue

        repos = [edge["node"] for edge in data["search"]["edges"]]
        stats["Total coletados"] += len(repos)

        for repo in tqdm(repos, desc=f"🔄 Analisando {lang}", ncols=90):
            try:
                analysis = analyze_repo(repo)
                if not analysis:
                    continue

                stats["Com commits válidos"] += 1
                stats["Com períodos de inatividade"] += 1

                if analysis["Data de ressurreição"]:
                    results_ressuscitados.append(analysis)
                    stats["Ressuscitados"] += 1
                else:
                    results_mortos.append(analysis)
                    stats["Mortos"] += 1
            except Exception as e:
                print(f"⚠️ Erro analisando {repo.get('nameWithOwner')}: {e}")
            time.sleep(1.2)

    # Geração dos datasets
    df_mortos = pd.DataFrame(results_mortos).drop_duplicates(subset=["Nome"])
    df_ressuscitados = pd.DataFrame(results_ressuscitados).drop_duplicates(subset=["Nome"])

    # Estatísticas agregadas
    total_final = stats["Mortos"] + stats["Ressuscitados"]
    taxa_ressuscitados = (
        (stats["Ressuscitados"] / total_final * 100) if total_final > 0 else 0
    )
    taxa_aproveitamento = (
        (total_final / stats["Total coletados"] * 100) if stats["Total coletados"] > 0 else 0
    )

    df_stats = pd.DataFrame([
        {"Métrica": "Total de repositórios coletados", "Valor": stats["Total coletados"]},
        {"Métrica": "Com commits válidos", "Valor": stats["Com commits válidos"]},
        {"Métrica": "Com períodos de inatividade (≥180 dias)", "Valor": stats["Com períodos de inatividade"]},
        {"Métrica": "Repositórios mortos", "Valor": stats["Mortos"]},
        {"Métrica": "Repositórios ressuscitados", "Valor": stats["Ressuscitados"]},
        {"Métrica": "Taxa de ressuscitados (%)", "Valor": f"{taxa_ressuscitados:.2f}%"},
        {"Métrica": "Taxa de aproveitamento (%)", "Valor": f"{taxa_aproveitamento:.2f}%"},
    ])

    # Exportação para Excel com múltiplas abas
    with pd.ExcelWriter("dataset_fase1_validacao.xlsx") as writer:
        df_mortos.to_excel(writer, sheet_name="Mortos", index=False)
        df_ressuscitados.to_excel(writer, sheet_name="Ressuscitados", index=False)
        df_stats.to_excel(writer, sheet_name="Estatísticas", index=False)

    print("\n📊 Resultado final:")
    print(f"💀 Mortos: {len(df_mortos)}")
    print(f"✨ Ressuscitados: {len(df_ressuscitados)}")
    print(f"📈 Taxa de aproveitamento: {taxa_aproveitamento:.2f}%")
    print("✅ Arquivo salvo em 'dataset_fase1_validacao.xlsx'.")

if __name__ == "__main__":
    main()
