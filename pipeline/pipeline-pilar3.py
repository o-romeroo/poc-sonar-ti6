import argparse
import datetime as dt
import json
import os
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
import requests
from tqdm import tqdm

GRAPHQL_URL = "https://api.github.com/graphql"
REQUEST_TIMEOUT = 30
RETRY_LIMIT = 3

COMMITS_WINDOW_QUERY = """
query($owner: String!, $name: String!, $first: Int!, $since: GitTimestamp, $until: GitTimestamp) {
  repository(owner: $owner, name: $name) {
    nameWithOwner
    defaultBranchRef {
      name
      target {
        ... on Commit {
          history(first: $first, since: $since, until: $until) {
            nodes {
              oid
              committedDate
              messageHeadline
            }
          }
        }
      }
    }
  }
}
"""


@dataclass
class SnapshotCommit:
    sha: str
    committed_at: dt.datetime
    message: str


class PipelineError(Exception):
    pass


def utc_from_str(date_str: str) -> dt.datetime:
    return dt.datetime.fromisoformat(date_str.replace("Z", "+00:00")).astimezone(dt.timezone.utc)


def to_iso8601(ts: dt.datetime) -> str:
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=dt.timezone.utc)
    return ts.astimezone(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def run_query(query: str, variables: Dict[str, object], headers: Dict[str, str]) -> Optional[Dict[str, object]]:
    for attempt in range(1, RETRY_LIMIT + 1):
        response = requests.post(
            GRAPHQL_URL,
            json={"query": query, "variables": variables},
            headers=headers,
            timeout=REQUEST_TIMEOUT,
        )
        if response.status_code == 200:
            payload = response.json()
            if "errors" in payload:
                messages = ", ".join(err.get("message", "unknown error") for err in payload["errors"])
                print(f"[warn] GraphQL error: {messages}")
                return None
            return payload.get("data")
        if response.status_code == 502:
            print("[warn] GraphQL 502, retrying in 5 seconds...")
            time.sleep(5)
            continue
        if response.status_code == 403:
            print("[info] GraphQL rate limit hit, waiting 60 seconds...")
            time.sleep(60)
            continue
        print(f"[error] GraphQL request failed (status {response.status_code}): {response.text[:200]}")
        time.sleep(10)
    return None


def fetch_commits(
    owner: str,
    name: str,
    since: Optional[dt.datetime],
    until: Optional[dt.datetime],
    headers: Dict[str, str],
    first: int = 100,
) -> List[Dict[str, str]]:
    variables: Dict[str, object] = {"owner": owner, "name": name, "first": first}
    if since is not None:
        variables["since"] = to_iso8601(since)
    if until is not None:
        variables["until"] = to_iso8601(until)
    data = run_query(COMMITS_WINDOW_QUERY, variables, headers)
    if not data:
        return []
    repo = data.get("repository")
    if not repo or not repo.get("defaultBranchRef"):
        return []
    target = repo["defaultBranchRef"].get("target") or {}
    history = target.get("history") or {}
    return history.get("nodes", [])


def pick_pre_death_commit(
    owner: str,
    name: str,
    death_date: dt.datetime,
    headers: Dict[str, str],
) -> Optional[SnapshotCommit]:
    search_windows = [90, 180, 365, 730]
    end_boundary = death_date + dt.timedelta(days=1)
    for window in search_windows:
        since = death_date - dt.timedelta(days=window)
        commits = fetch_commits(owner, name, since, end_boundary, headers)
        if not commits:
            continue
        parsed: List[SnapshotCommit] = []
        for node in commits:
            committed_at = utc_from_str(node["committedDate"])
            if committed_at <= end_boundary:
                parsed.append(
                    SnapshotCommit(
                        sha=node["oid"],
                        committed_at=committed_at,
                        message=node.get("messageHeadline", ""),
                    )
                )
        if parsed:
            parsed.sort(key=lambda c: c.committed_at, reverse=True)
            return parsed[0]
    return None


def pick_post_revive_commit(
    owner: str,
    name: str,
    revive_date: dt.datetime,
    headers: Dict[str, str],
) -> Optional[SnapshotCommit]:
    search_windows = [30, 90, 180, 365, 730]
    collected: Dict[str, SnapshotCommit] = {}
    for window in search_windows:
        since = revive_date
        until = revive_date + dt.timedelta(days=window)
        commits = fetch_commits(owner, name, since, until, headers)
        if not commits:
            continue
        for node in commits:
            commit_date_raw = node.get("committedDate")
            if not commit_date_raw:
                continue
            committed_at = utc_from_str(commit_date_raw)
            if committed_at >= revive_date:
                sha = node.get("oid")
                if not sha:
                    continue
                collected[sha] = SnapshotCommit(
                    sha=sha,
                    committed_at=committed_at,
                    message=node.get("messageHeadline", ""),
                )
        ordered = sorted(collected.values(), key=lambda c: c.committed_at)
        if len(ordered) >= 10:
            return ordered[9]
    return None


def ensure_git_clone(repo_url: str, destination: Path) -> None:
    if destination.exists():
        return
    destination.parent.mkdir(parents=True, exist_ok=True)
    subprocess.run(["git", "clone", "--quiet", repo_url, str(destination)], check=True)


def checkout_commit(repo_path: Path, commit_sha: str) -> None:
    subprocess.run(["git", "checkout", "--quiet", commit_sha], cwd=repo_path, check=True)


def clone_snapshot(repo_url: str, commit_sha: str, target_dir: Path) -> None:
    if not target_dir.exists():
        ensure_git_clone(repo_url, target_dir)
    checkout_commit(target_dir, commit_sha)


def run_pysonar(
    snapshot_dir: Path,
    sonar_token: str,
    project_key: str,
    organization: str,
    branch_label: str,
    report_dir: Path,
) -> Dict[str, object]:
    report_dir.mkdir(parents=True, exist_ok=True)
    report_path = report_dir / f"{branch_label}.json"
    cmd = [
        "pysonar",
        f"--sonar-token={sonar_token}",
        f"--sonar-project-key={project_key}",
        f"--sonar-organization={organization}",
        f"--sonar-project-base-dir={str(snapshot_dir)}",
    ]
    try:
        completed = subprocess.run(cmd, capture_output=True, text=True, check=False)
    except FileNotFoundError as exc:
        return {
            "status": "error",
            "exit_code": None,
            "message": "pysonar executable not found",
            "report_path": None,
        }
    status = "success" if completed.returncode == 0 else "error"
    return {
        "status": status,
        "exit_code": completed.returncode,
        "stdout": completed.stdout[-2000:],
        "stderr": completed.stderr[-2000:],
        "report_path": str(report_path) if report_path.exists() else None,
    }


def sanitize_branch_name(value: str) -> str:
    allowed = [c if c.isalnum() or c in "-_" else "-" for c in value]
    return "".join(allowed)[:100] or "snapshot"


def parse_dates(row: pd.Series) -> Dict[str, dt.datetime]:
    morte_str = str(row.get("Data de morte", "")).strip()
    revive_str = str(row.get("Data de ressurreição", "")).strip()
    if not revive_str:
        raise PipelineError("Linha sem data de ressurreição válida")
    try:
        revive_date = dt.datetime.strptime(revive_str, "%Y-%m-%d").replace(tzinfo=dt.timezone.utc)
    except ValueError as exc:
        raise PipelineError(f"Data de ressurreição inválida: {revive_str}") from exc
    death_date: Optional[dt.datetime] = None
    if morte_str:
        try:
            death_date = dt.datetime.strptime(morte_str, "%Y-%m-%d").replace(tzinfo=dt.timezone.utc)
        except ValueError:
            pass
    if death_date is None:
        death_date = revive_date - dt.timedelta(days=1)
    return {"death": death_date, "revive": revive_date}


def prepare_snapshots(
    row: pd.Series,
    headers: Dict[str, str],
    snapshots_root: Path,
) -> List[Dict[str, object]]:
    repo_full_name = row.get("Nome") or row.get("nameWithOwner")
    if not repo_full_name or "/" not in repo_full_name:
        raise PipelineError("Nome do repositório inválido")
    owner, name = repo_full_name.split("/", 1)
    repo_url = row.get("URL")
    if not repo_url:
        repo_url = f"https://github.com/{owner}/{name}"
    dates = parse_dates(row)
    pre_commit = pick_pre_death_commit(owner, name, dates["death"], headers)
    if not pre_commit:
        raise PipelineError("Não foi possível localizar commit pré-morte")
    post_commit = pick_post_revive_commit(owner, name, dates["revive"], headers)
    if not post_commit:
        raise PipelineError("Repositorio sem ao menos dez commits apos a ressurreição")

    base_dir = snapshots_root / f"{owner}__{name}"
    pre_dir = base_dir / "pre_morte"
    post_dir = base_dir / "pos_revive"

    clone_snapshot(repo_url, pre_commit.sha, pre_dir)
    checkout_commit(pre_dir, pre_commit.sha)

    clone_snapshot(repo_url, post_commit.sha, post_dir)
    checkout_commit(post_dir, post_commit.sha)

    pre_branch = sanitize_branch_name(f"{owner}-{name}-pre")
    post_branch = sanitize_branch_name(f"{owner}-{name}-post")

    return [
        {
            "repo": repo_full_name,
            "snapshot": "pre_morte",
            "commit": pre_commit.sha,
            "committed_at": pre_commit.committed_at.isoformat(),
            "path": str(pre_dir),
            "branch_label": pre_branch,
        },
        {
            "repo": repo_full_name,
            "snapshot": "pos_revive",
            "commit": post_commit.sha,
            "committed_at": post_commit.committed_at.isoformat(),
            "path": str(post_dir),
            "branch_label": post_branch,
        },
    ]


def load_dataframe(excel_path: Path) -> pd.DataFrame:
    df = pd.read_excel(excel_path)
    if "Data de ressurreição" not in df.columns:
        raise PipelineError("Planilha nao contem a coluna 'Data de ressurreição'")
    filtered = df[df["Data de ressurreição"].notna()].copy()
    if filtered.empty:
        raise PipelineError("Nenhum repositorio com valor na coluna 'Data de ressurreição'")
    return filtered


def main() -> None:
    parser = argparse.ArgumentParser(description="Pipeline Pilar 3 - analise de qualidade com Sonar")
    parser.add_argument("excel_path", type=str, help="Caminho do arquivo .xlsx de entrada")
    parser.add_argument("--output-root", type=str, default="outputs/pilar3", help="Diretorio base para snapshots e relatorios")
    parser.add_argument("--github-token", type=str, default=os.getenv("GITHUB_TOKEN"), help="Token do GitHub para GraphQL")
    parser.add_argument("--sonar-token", type=str, default=os.getenv("SONAR_TOKEN"), help="Token para autenticacao Sonar")
    parser.add_argument("--sonar-organization", type=str, default=os.getenv("SONAR_ORGANIZATION", "ti6"))
    parser.add_argument("--limit", type=int, default=None, help="Limite opcional de repositorios a processar")
    args = parser.parse_args()

    if not args.github_token:
        print("[error] Forneca um token do GitHub via --github-token ou variavel GITHUB_TOKEN.")
        sys.exit(1)
    if not args.sonar_token:
        print("[error] Forneca o token do Sonar via --sonar-token ou variavel SONAR_TOKEN.")
        sys.exit(1)

    headers = {"Authorization": f"Bearer {args.github_token}"}
    excel_path = Path(args.excel_path).resolve()
    if not excel_path.exists():
        print(f"[error] Arquivo nao encontrado: {excel_path}")
        sys.exit(1)

    output_root = Path(args.output_root).resolve()
    snapshots_root = output_root / "snapshots"
    reports_root = output_root / "sonar_reports"

    try:
        df = load_dataframe(excel_path)
    except PipelineError as exc:
        print(f"[error] {exc}")
        sys.exit(1)

    snapshot_records: List[Dict[str, object]] = []
    records: List[Dict[str, object]] = df.to_dict(orient="records")
    if args.limit:
        records = records[: args.limit]
    for row_dict in tqdm(records, desc="Preparando snapshots", unit="repo"):
        row_series = pd.Series(row_dict)
        try:
            row_snapshots = prepare_snapshots(
                row_series,
                headers,
                snapshots_root,
            )
            snapshot_records.extend(row_snapshots)
        except (PipelineError, subprocess.CalledProcessError) as exc:
            repo_name = row_series.get("Nome") or row_series.get("nameWithOwner") or "desconhecido"
            print(f"[warn] Falha ao processar {repo_name}: {exc}")

    if not snapshot_records:
        print("[warn] Nenhum snapshot preparado.")
        return

    # Referenciar todos os snapshots já existentes para análise Sonar
    # snapshot_records = []
    # for repo_dir in snapshots_root.glob("*"):
    #     if not repo_dir.is_dir():
    #         continue
    #     for snap_type in ["pre_morte", "pos_revive"]:
    #         snap_path = repo_dir / snap_type
    #         if snap_path.is_dir():
    #             snapshot_records.append({
    #                 "repo": repo_dir.name.replace("__", "/"),
    #                 "snapshot": snap_type,
    #                 "commit": None,
    #                 "committed_at": None,
    #                 "path": str(snap_path),
    #                 "branch_label": f"{repo_dir.name.replace('__', '-')}-{snap_type}",
    #             })

    results: List[Dict[str, object]] = []
    for snapshot in tqdm(snapshot_records, desc="Analisando snapshots", unit="snapshot"):
        snapshot_path = Path(str(snapshot["path"]))
        
        repo_full_name = str(snapshot["repo"])
        project_key = repo_full_name.replace("/", "_").replace(".", "-")

        report = run_pysonar(
            snapshot_path,
            args.sonar_token,
            project_key,
            args.sonar_organization,
            str(snapshot["branch_label"]),
            reports_root,
        )
        snapshot_result = dict(snapshot)
        snapshot_result["report"] = report
        results.append(snapshot_result)

    summary_path = output_root / "resumo_pilar3.json"
    output_root.mkdir(parents=True, exist_ok=True)
    with summary_path.open("w", encoding="utf-8") as fp:
        json.dump(results, fp, ensure_ascii=False, indent=2)
    print(f"[info] Resumo salvo em {summary_path}")


if __name__ == "__main__":
    main()