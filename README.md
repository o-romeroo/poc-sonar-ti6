# Esse projeto foi criado para provas de conceito da pesquisa de TI6

## Como utilizar o diagrama

Entre [nesse site](https://dreampuf.github.io/GraphvizOnline) e cole o código do diagrama.

## Sobre o pipeline-unificado.py

Esse script é responsável por encontrar repositórios que morreram e reviveram (e podem ter morrido novamente ou não).

Atualmente ele está configurado apenas para testes, pegando poucos repositórios, mas já é possível testar (`python pipeline-unificado-v1.py`) e verificar que o arquivo que ele gera realmente [...]

## Pipeline Pilar 3 (qualidade de código)

O script `pipeline/pipeline-pilar3.py` roda a coleta de snapshots pré-morte e pós-revive para cada repositório identificado na planilha de entrada e executa as análises do Sonar via `pysonar`. Exemplo de uso:

```bash
python pipeline/pipeline-pilar3.py validacao/repositorios_morte_ressurreicao_2022.xlsx \
	--github-token "<TOKEN_GITHUB>" \
	--sonar-token "<TOKEN_SONAR>"
```

Os resultados (snapshots, relatórios e resumo em JSON) são gravados em `outputs/pilar3/`.

### Como são definidas as regras para trazê-los?

Selecionamos repositórios que entre **2018 e 2025** tiveram pelo menos um período de **≥ 6 meses sem commits** (morte).
Depois verificamos se eles **ressuscitaram** (voltaram a ter commits) e se morreram novamente após esse retorno.

### Como queremos fazer o processo inteiro?

De forma ainda não completamente detalhada, mas já em versões iniciais, temos o diagrama abaixo para representar isso.

<img width="600px" src="diagrama-inicial.png">

### Árvore de artigos relacionados

A imagem abaixo apresenta a árvore de artigos relacionados utilizada no contexto do projeto TI6. Ela organiza a literatura que fundamenta o artigo atual (nó raiz) em quatro áreas temáticas: (1) Abandono e Sobrevivência, (2) Engajamento Comunitário, (3) Qualidade do Código e (4) Abordagens Complementares. Para cada artigo, são destacados nós de Semelhança (contribuições alinhadas ao nosso foco de revivência de repositórios) e Diferença (aspectos que divergem: escopo, nível de análise, contexto ou método). Essa estrutura facilita: (a) rastrear como construímos os critérios de “morte” e “ressurreição”; (b) justificar métricas e dimensões técnico-sociais usadas no pipeline; e (c) identificar lacunas para extensões futuras.

<img width="600px" src="arvore_artigos_relacionados_TI6.png">
