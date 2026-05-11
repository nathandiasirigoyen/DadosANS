
from __future__ import annotations

import csv
import ftplib
import gc
import shutil
import unicodedata
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path, PurePosixPath
from typing import Optional, Sequence, TypedDict
from urllib.parse import unquote, urljoin, urlparse
from zipfile import ZipFile

import polars as pl
import requests
from bs4 import BeautifulSoup


class ErroLeitura(TypedDict):
    arquivo: str
    erro: str


class ResultadoProcessamento(TypedDict):
    conteudo_zip: Optional[bytes]
    pasta_execucao: str
    pasta_ingestao: str
    manifesto_arquivos: str
    arquivos_lidos: list[str]
    arquivos_ignorados: list[str]
    erros: list[ErroLeitura]
    df_unificado: Optional[pl.DataFrame | pl.LazyFrame]


class ArquivoDescoberto(TypedDict):
    url: str
    caminho_relativo: str


class RegistroManifesto(TypedDict):
    caminho_local: str
    caminho_relativo: str
    nome_arquivo: str
    origem_url: str
    arquivo_origem: str
    grupo_mescla_padrao: str
    origem_tipo: str


@dataclass(slots=True)
class ContextoExecucao:
    pasta_execucao: Path
    pasta_ingestao: Path
    pasta_downloads: Path
    pasta_extraidos: Path
    manifesto_arquivos: Path
    total_planejado: int = 0
    processados: int = 0


def baixar_extrair_mesclar(
    url_origem: str,
    *,
    mesclar: bool = True,
    nome_pasta_execucao: Optional[str] = None,
    base_dir_execucao: Optional[Path | str] = None,
    pasta_saida_mesclas: Optional[Path | str] = None,
    retornar_df_unificado: bool = False,
    retornar_lazyframe_unificado: bool = False,
    colunas: Optional[Sequence[str]] = None,
    filtros: Optional[pl.Expr | Sequence[pl.Expr]] = None,
    where: Optional[str] = None,
    incluir_colunas_metadados: bool = True,
    reter_conteudo_zip_em_memoria: bool = False,
    profundidade_maxima_zip: int = 20,
) -> ResultadoProcessamento:
    """
Processa automaticamente a URL informada, identificando se ela aponta para:
1. um arquivo `.zip`,
2. um arquivo tabular compatível (`.csv`, `.xlsx`, `.xls`, `.ods`),
3. uma pasta FTP contendo múltiplos arquivos,
4. uma página HTTP/HTTPS com arquivos listados,
5. uma página HTTP/HTTPS ou pasta FTP com subpastas contendo arquivos.

Estratégia adotada
------------------
- A função cria uma pasta de execução dentro de `processamentos`, no formato
  `<nome>_<timestamp>`, contendo a estrutura de ingestão e o manifesto de arquivos.
- Arquivos tabulares brutos (`.csv`, `.xlsx`, `.xls`, `.ods`) são sempre
  baixados ou extraídos primeiro para diretórios temporários de staging, e
  não são mantidos como saída definitiva.
- O fluxo de processamento dos arquivos tabulares ocorre em duas etapas:
  1. bruto tabular temporário -> Parquet temporário
  2. leitura lazy do Parquet temporário com `scan_parquet(...)`, aplicando
     seleção de colunas, filtros e cláusula `where`, seguida da gravação do
     Parquet final definitivo
- Se a origem for um arquivo `.zip`, a função baixa o ZIP para staging
  temporário, extrai seu conteúdo, processa recursivamente ZIPs internos e
  converte os arquivos tabulares compatíveis encontrados em arquivos `.parquet`
  definitivos.
- Se a origem for um arquivo tabular compatível, a função baixa o arquivo para
  staging temporário, converte para Parquet temporário, lê esse Parquet em modo
  lazy, aplica colunas/filtros/where e grava o Parquet final definitivo.
- Se for pasta FTP, a função lista recursivamente os arquivos da estrutura
  remota, baixa os arquivos compatíveis mantendo a estrutura de diretórios
  relativa e processa cada item.
- Se for página HTTP/HTTPS, a função faz o parsing da listagem, identifica os
  links para arquivos compatíveis, percorre subpastas automaticamente quando
  necessário e processa os itens encontrados mantendo a estrutura relativa.
- Se qualquer uma das origens anteriores contiver arquivos `.zip`, a função
  baixa, extrai e processa recursivamente esses ZIPs internos.
- Após a ingestão, a função pode opcionalmente criar arquivos de mescla por
  grupo em formato `.parquet`, utilizando leitura lazy com Polars.

Extensões suportadas
--------------------
Entradas tabulares compatíveis:
- `.csv`
- `.xlsx`
- `.xls`
- `.ods`

Entradas compactadas:
- `.zip`

CSV
---
Para arquivos CSV, a função tenta automaticamente múltiplos encodings:
- `utf-8`
- `utf-8-sig`
- `latin1`

Além disso, tenta inferir o separador automaticamente. Caso a inferência
falhe, utiliza `;` como fallback.

Regras de staging e persistência
--------------------------------
- Arquivos tabulares brutos não são persistidos como saída definitiva em
  `ingestao/downloads`.
- Os brutos são manipulados apenas em diretórios temporários:
  - `ingestao/_brutos_temporarios`
  - `ingestao/_extraidos_temporarios`
  - `ingestao/_csv_utf8_temporario` (quando necessário para transcodificação)
  - `ingestao/_parquet_temporario`
- Após conversão e materialização bem-sucedidas, os arquivos temporários são
  removidos.
- Apenas os arquivos `.parquet` definitivos são registrados no manifesto.

Regra adicional de ignorar arquivos
-----------------------------------
Arquivos cujo nome contenha a palavra `dicionario` são ignorados,
independentemente de:
- acentuação
- letras maiúsculas/minúsculas

Regra adicional de mesclagem por grupo
--------------------------------------
A função identifica automaticamente um grupo de mesclagem a partir do nome
do arquivo de origem, considerando o último bloco do nome (separado por `_`).

Exemplos:
- `AC_202401_HOSP_CONS.zip` -> grupo `CONS`
- `AC_202401_HOSP_DET.zip` -> grupo `DET`
- `AC_202401_HOSP_REM.zip` -> grupo `REM`

Arquivos do mesmo grupo podem ser concatenados entre si durante a etapa de
mesclagem. Ao final, a função:
- salva um arquivo `.parquet` por grupo na pasta atual de saída
  (`Path.cwd()` ou `pasta_saida_mesclas`, se informada), com nomes no padrão
  `mescla_<GRUPO>.parquet`, quando `mesclar=True`;
- opcionalmente retorna um `LazyFrame` unificado;
- opcionalmente retorna um `DataFrame` Polars unificado, coletado a partir
  do plano lazy final;
- adiciona as colunas `arquivo_origem` e `grupo_mescla` ao resultado unificado,
  quando `incluir_colunas_metadados=True`;
- imprime no console os comandos prontos de leitura do Polars para os arquivos
  Parquet de ingestão e para os arquivos Parquet mesclados, se houver.

Seleção de colunas, filtros e cláusula WHERE
--------------------------------------------
A função permite reduzir o volume processado já na etapa lazy de leitura do
Parquet temporário, aplicando:
- `colunas`: lista opcional de colunas a selecionar
- `filtros`: uma expressão Polars (`pl.Expr`) ou sequência de expressões
  Polars aplicadas com `.filter(...)`
- `where`: cláusula SQL opcional aplicada sobre o LazyFrame por meio de
  `pl.SQLContext`, com a consulta:
  `SELECT * FROM dados WHERE <where>`

A ordem geral do processamento lazy é:
1. leitura do Parquet com `pl.scan_parquet(...)`
2. aplicação de `filtros`
3. aplicação de `where`
4. seleção das `colunas`, quando informadas
5. gravação do Parquet final com `sink_parquet(...)`

Pasta de execução
-----------------
- A função cria uma pasta `processamentos`, caso ela ainda não exista.
- Dentro dela, cria a pasta da execução no formato:
  `<nome_informado_pelo_usuario>_<timestamp>`.
- Se `nome_pasta_execucao` não for informado, a função solicita esse valor
  via `input()`.

Estrutura típica gerada
-----------------------
Dentro da pasta da execução, a estrutura gerada inclui, em geral:
- `ingestao/downloads`
- `ingestao/extraidos`
- `ingestao/_brutos_temporarios`
- `ingestao/_extraidos_temporarios`
- `ingestao/_csv_utf8_temporario`
- `ingestao/_parquet_temporario`
- `manifesto_arquivos.csv`

Logs de execução
----------------
A função exibe logs visuais no console, mostrando:
- fase da execução;
- subfunção em execução;
- diretório, pasta ou subpasta atual;
- arquivo atual;
- progresso global.

Parâmetros
----------
url_origem : str
    URL da origem dos dados. Pode ser:
    - URL HTTP/HTTPS/FTP de um arquivo `.zip`
    - URL HTTP/HTTPS/FTP de um arquivo tabular compatível
    - URL FTP apontando para uma pasta
    - URL HTTP/HTTPS apontando para uma página com listagem de arquivos
    - URL HTTP/HTTPS/FTP apontando para uma estrutura com subpastas

mesclar : bool, default True
    Se True, executa a etapa de mesclagem por grupo e grava arquivos finais
    no padrão `mescla_<GRUPO>.parquet`.
    Se False, não grava as mesclas e apenas imprime os comandos de leitura dos
    arquivos Parquet ingeridos.

nome_pasta_execucao : Optional[str], default None
    Nome-base da pasta da execução, que será criado antes do timestamp.
    Exemplo: `ans_igr` -> `processamentos/ans_igr_20260324_001530`.
    Se None, a função solicitará esse valor ao usuário via `input()`.

base_dir_execucao : Optional[Path | str], default None
    Diretório base onde a pasta `processamentos` será criada.
    Se None, usa `Path.cwd()`.

pasta_saida_mesclas : Optional[Path | str], default None
    Pasta onde os arquivos `mescla_<GRUPO>.parquet` serão salvos.
    Se None, usa `Path.cwd()`.

retornar_df_unificado : bool, default False
    Se True, monta e retorna um `pl.DataFrame` unificado ao final, coletando
    o resultado das mesclas geradas.
    Se False, evita esse custo adicional de materialização em memória.

retornar_lazyframe_unificado : bool, default False
    Se True, retorna um `pl.LazyFrame` unificado ao final, composto a partir
    dos arquivos Parquet de mescla gerados.

colunas : Optional[Sequence[str]], default None
    Lista de colunas a manter no processamento lazy.
    Se None, mantém todas as colunas disponíveis.

filtros : Optional[pl.Expr | Sequence[pl.Expr]], default None
    Filtro ou sequência de filtros Polars aplicados ao LazyFrame por meio de
    `.filter(...)`.

where : Optional[str], default None
    Cláusula SQL opcional aplicada sobre o LazyFrame utilizando `pl.SQLContext`.
    Exemplo:
    `where="ano = 2024 AND uf = 'DF'"`

incluir_colunas_metadados : bool, default True
    Se True, adiciona as colunas:
    - `arquivo_origem`
    - `grupo_mescla`
    durante a etapa de mesclagem.

reter_conteudo_zip_em_memoria : bool, default False
    Se True e a URL original apontar diretamente para um `.zip`, tenta retornar
    os bytes do ZIP em `conteudo_zip`, quando ainda disponíveis.

profundidade_maxima_zip : int, default 20
    Limite de profundidade para recursão de ZIP dentro de ZIP.

Retorno
-------
ResultadoProcessamento
    Estrutura com:
    - `conteudo_zip`: bytes do ZIP original, quando aplicável e solicitado
    - `pasta_execucao`: pasta da execução criada dentro de `processamentos`
    - `pasta_ingestao`: pasta de staging/ingestão
    - `manifesto_arquivos`: caminho do manifesto CSV
    - `arquivos_lidos`: lista de arquivos lidos com sucesso
    - `arquivos_ignorados`: lista de arquivos ignorados
    - `erros`: lista de erros ocorridos
    - `df_unificado`: `pl.DataFrame` ou `pl.LazyFrame` unificado, quando solicitado

Efeitos colaterais
------------------
- Cria diretórios locais de execução e staging.
- Baixa arquivos remotos via HTTP/HTTPS/FTP.
- Extrai arquivos ZIP, inclusive de forma recursiva.
- Gera arquivos `.parquet` definitivos em `ingestao/downloads` e/ou
  `ingestao/extraidos`.
- Gera arquivos `mescla_<GRUPO>.parquet` na pasta de saída, quando
  `mesclar=True`.
- Escreve e atualiza o arquivo `manifesto_arquivos.csv`.
- Remove arquivos temporários brutos e intermediários após sucesso.
- Imprime no console comandos prontos do Polars para leitura dos arquivos
  Parquet originais e dos arquivos Parquet mesclados, se houver.
- Se ocorrer erro ao salvar algum arquivo de saída, o erro será registrado na
  lista `erros`, sem interromper necessariamente o processamento dos demais
  grupos ou arquivos.

Raises
------
requests.HTTPError
    Se ocorrer erro ao baixar um arquivo via HTTP/HTTPS.

ValueError
    Se a URL não for suportada; se o nome informado para a pasta de execução
    for inválido; se `colunas` for informada sem nomes válidos; se nenhuma
    coluna solicitada existir no arquivo; se a profundidade máxima de ZIP for
    excedida; ou se nenhum arquivo compatível for ingerido/processado com
    sucesso.

FileNotFoundError
    Se a geração de algum arquivo Parquet temporário ou definitivo falhar
    silenciosamente e o arquivo esperado não existir ao final da etapa.
"""

    extensoes_tabulares_suportadas: set[str] = {".csv", ".xlsx", ".xls", ".ods"}
    extensoes_processaveis: set[str] = extensoes_tabulares_suportadas | {".zip"}
    encodings_csv_teste: tuple[str, ...] = ("utf-8", "utf-8-sig", "latin1")
    chunk_size_download: int = 1024 * 1024

    arquivos_lidos: list[str] = []
    arquivos_ignorados: list[str] = []
    erros: list[ErroLeitura] = []
    caminhos_mesclas_geradas: dict[str, Path] = {}

    colunas_extracao: Optional[list[str]] = (
        [str(coluna).strip() for coluna in colunas if str(coluna).strip()]
        if colunas is not None
        else None
    )
    if colunas is not None and not colunas_extracao:
        raise ValueError("O parâmetro 'colunas' foi informado, mas não contém nomes válidos.")

    if filtros is None:
        filtros_extracao: list[pl.Expr] = []
    elif isinstance(filtros, pl.Expr):
        filtros_extracao = [filtros]
    else:
        filtros_extracao = list(filtros)

    def criar_barra_progresso(processados: int, total: int, largura: int = 26) -> str:
        if total <= 0:
            return "░" * largura
        percentual = min(max(processados / total, 0.0), 1.0)
        preenchidos = int(round(percentual * largura))
        return "█" * preenchidos + "░" * (largura - preenchidos)

    def imprimir_log(
        *,
        fase: str,
        funcao: str,
        etapa: str,
        caminho: Optional[str] = None,
        arquivo: Optional[str] = None,
        processados: Optional[int] = None,
        total: Optional[int] = None,
        icone: str = "ℹ️",
    ) -> None:
        print("\n" + "═" * 108)
        print(f"{icone} FASE : {fase}")
        print(f"🔧 FUNÇÃO : {funcao}")
        print(f"🧭 ETAPA : {etapa}")
        if caminho:
            print(f"📂 CAMINHO: {caminho}")
        if arquivo:
            print(f"📄 ARQUIVO: {arquivo}")
        if processados is not None and total is not None:
            barra = criar_barra_progresso(processados, total)
            percentual = (processados / total * 100) if total > 0 else 0.0
            faltam = max(total - processados, 0)
            print(f"📊 STATUS : [{barra}] {processados}/{total} ({percentual:,.1f}%)")
            print(f"⏳ FALTAM : {faltam}")
        print("═" * 108)

    def normalizar_nome_pasta_usuario(nome: str) -> str:
        nome_limpo = nome.strip()
        if not nome_limpo:
            raise ValueError("O nome da pasta de execução não pode ser vazio.")
        nome_normalizado = unicodedata.normalize("NFKD", nome_limpo)
        nome_sem_acento = "".join(c for c in nome_normalizado if not unicodedata.combining(c))
        nome_sem_acento = (
            nome_sem_acento.replace(" ", "_").replace("-", "_").replace("/", "_").replace("\\", "_")
        )
        nome_saida = "".join(c if c.isalnum() or c == "_" else "_" for c in nome_sem_acento).strip("_")
        while "__" in nome_saida:
            nome_saida = nome_saida.replace("__", "_")
        if not nome_saida:
            raise ValueError("O nome informado para a pasta da execução é inválido.")
        return nome_saida

    def obter_nome_base_pasta_execucao() -> str:
        if nome_pasta_execucao is not None:
            return normalizar_nome_pasta_usuario(nome_pasta_execucao)
        return normalizar_nome_pasta_usuario(
            input("Informe o nome da pasta de processamento (será usado antes do timestamp): ")
        )

    def criar_pasta_timestamp(base_dir: Optional[Path | str] = None) -> ContextoExecucao:
        base = Path(base_dir) if base_dir is not None else Path.cwd()
        pasta_processamentos = base / "processamentos"
        pasta_processamentos.mkdir(parents=True, exist_ok=True)

        pasta_execucao = pasta_processamentos / f"{obter_nome_base_pasta_execucao()}_{datetime.now():%Y%m%d_%H%M%S}"
        pasta_ingestao = pasta_execucao / "ingestao"
        pasta_downloads = pasta_ingestao / "downloads"
        pasta_extraidos = pasta_ingestao / "extraidos"
        manifesto_arquivos = pasta_execucao / "manifesto_arquivos.csv"

        pasta_downloads.mkdir(parents=True, exist_ok=True)
        pasta_extraidos.mkdir(parents=True, exist_ok=True)
        (pasta_ingestao / "_brutos_temporarios").mkdir(parents=True, exist_ok=True)
        (pasta_ingestao / "_extraidos_temporarios").mkdir(parents=True, exist_ok=True)
        (pasta_ingestao / "_csv_utf8_temporario").mkdir(parents=True, exist_ok=True)
        (pasta_ingestao / "_parquet_temporario").mkdir(parents=True, exist_ok=True)

        with manifesto_arquivos.open("w", newline="", encoding="utf-8-sig") as arquivo_manifesto:
            writer = csv.DictWriter(
                arquivo_manifesto,
                fieldnames=[
                    "caminho_local",
                    "caminho_relativo",
                    "nome_arquivo",
                    "origem_url",
                    "arquivo_origem",
                    "grupo_mescla_padrao",
                    "origem_tipo",
                ],
            )
            writer.writeheader()

        return ContextoExecucao(
            pasta_execucao=pasta_execucao,
            pasta_ingestao=pasta_ingestao,
            pasta_downloads=pasta_downloads,
            pasta_extraidos=pasta_extraidos,
            manifesto_arquivos=manifesto_arquivos,
        )

    contexto = criar_pasta_timestamp(base_dir_execucao)

    def definir_total_arquivos(total: int, descricao: str) -> None:
        contexto.total_planejado = max(total, 0)
        contexto.processados = 0
        imprimir_log(
            fase="planejamento",
            funcao="definir_total_arquivos",
            etapa=f"Total de arquivos planejado: {descricao}",
            caminho=str(contexto.pasta_execucao),
            arquivo=f"{contexto.total_planejado} item(ns)",
            processados=contexto.processados,
            total=contexto.total_planejado,
            icone="🧮",
        )

    def atualizar_progresso_global(
        *,
        fase: str,
        funcao: str,
        etapa: str,
        caminho: Optional[str] = None,
        arquivo: Optional[str] = None,
        icone: str = "✅",
    ) -> None:
        contexto.processados += 1
        imprimir_log(
            fase=fase,
            funcao=funcao,
            etapa=etapa,
            caminho=caminho,
            arquivo=arquivo,
            processados=contexto.processados,
            total=contexto.total_planejado,
            icone=icone,
        )

    def nome_deve_ser_ignorado(nome_arquivo: str) -> bool:
        nome_base = Path(nome_arquivo).stem
        nome_normalizado = unicodedata.normalize("NFKD", nome_base)
        nome_sem_acento = "".join(c for c in nome_normalizado if not unicodedata.combining(c)).lower()
        return "dicionario" in nome_sem_acento

    def obter_grupo_mescla(nome_arquivo: str, grupo_padrao: Optional[str] = None) -> str:
        if grupo_padrao:
            return grupo_padrao
        nome_base = Path(nome_arquivo).stem
        nome_normalizado = unicodedata.normalize("NFKD", nome_base)
        nome_sem_acento = "".join(c for c in nome_normalizado if not unicodedata.combining(c))
        partes_nome = [parte for parte in nome_sem_acento.replace("-", "_").split("_") if parte]
        if not partes_nome:
            return "SEM_GRUPO"
        if (
            len(partes_nome) >= 4
            and partes_nome[0].lower() == "pda"
            and partes_nome[-1].isdigit()
            and len(partes_nome[-1]) == 6
        ):
            return f"{partes_nome[1].upper()}_{partes_nome[2].upper()}_{partes_nome[-1][:4]}"
        return partes_nome[-1].upper()

    def normalizar_nome_arquivo_saida(grupo_mescla: str) -> str:
        grupo_normalizado = unicodedata.normalize("NFKD", grupo_mescla)
        grupo_sem_acento = "".join(c for c in grupo_normalizado if not unicodedata.combining(c)).upper()
        nome_normalizado = "".join(c if c.isalnum() else "_" for c in grupo_sem_acento).strip("_")
        while "__" in nome_normalizado:
            nome_normalizado = nome_normalizado.replace("__", "_")
        return nome_normalizado or "SEM_GRUPO"

    def detectar_tipo_origem(url: str) -> str:
        url_parseada = urlparse(url)
        esquema = url_parseada.scheme.lower()
        extensao = Path(url_parseada.path).suffix.lower()
        if esquema not in {"http", "https", "ftp"}:
            raise ValueError(f"Esquema de URL não suportado: {esquema}")
        if extensao == ".zip":
            return "arquivo_zip"
        if extensao in extensoes_tabulares_suportadas:
            return "arquivo_tabular"
        if esquema == "ftp":
            return "ftp_pasta"
        if esquema in {"http", "https"}:
            return "http_pagina"
        raise ValueError("URL não suportada.")

    def escrever_manifesto(registro: RegistroManifesto) -> None:
        with contexto.manifesto_arquivos.open("a", newline="", encoding="utf-8-sig") as arquivo_manifesto:
            writer = csv.DictWriter(
                arquivo_manifesto,
                fieldnames=[
                    "caminho_local",
                    "caminho_relativo",
                    "nome_arquivo",
                    "origem_url",
                    "arquivo_origem",
                    "grupo_mescla_padrao",
                    "origem_tipo",
                ],
            )
            writer.writerow(registro)

    def baixar_http_para_arquivo(url: str, destino: Path) -> None:
        destino.parent.mkdir(parents=True, exist_ok=True)
        with requests.get(url, stream=True, timeout=300) as resposta:
            resposta.raise_for_status()
            with destino.open("wb") as arquivo_saida:
                for bloco in resposta.iter_content(chunk_size=chunk_size_download):
                    if bloco:
                        arquivo_saida.write(bloco)

    def baixar_ftp_para_arquivo(url: str, destino: Path) -> None:
        destino.parent.mkdir(parents=True, exist_ok=True)
        url_parseada = urlparse(url)
        host = url_parseada.hostname or ""
        caminho_remoto = unquote(url_parseada.path)
        if not host or not caminho_remoto:
            raise ValueError(f"URL FTP inválida: {url}")
        diretorio_remoto = str(PurePosixPath(caminho_remoto).parent)
        nome_arquivo = PurePosixPath(caminho_remoto).name
        with ftplib.FTP(host, timeout=300) as ftp:
            ftp.login()
            if diretorio_remoto not in {"", ".", "/"}:
                ftp.cwd(diretorio_remoto)
            with destino.open("wb") as arquivo_saida:
                ftp.retrbinary(f"RETR {nome_arquivo}", arquivo_saida.write)

    def baixar_arquivo_remoto_para_disco(url: str, destino: Path) -> None:
        esquema = urlparse(url).scheme.lower()
        if esquema in {"http", "https"}:
            baixar_http_para_arquivo(url, destino)
            return
        if esquema == "ftp":
            baixar_ftp_para_arquivo(url, destino)
            return
        raise ValueError(f"Esquema de URL não suportado: {esquema}")

    def listar_arquivos_ftp(url_ftp: str) -> list[ArquivoDescoberto]:
        url_parseada = urlparse(url_ftp)
        host = url_parseada.hostname or ""
        caminho_pasta = unquote(url_parseada.path).rstrip("/")
        if not host:
            raise ValueError(f"URL FTP inválida: {url_ftp}")
        resultados: list[ArquivoDescoberto] = []

        def montar_url_ftp(caminho_relativo: str) -> str:
            return f"ftp://{host}/{caminho_relativo.lstrip('/')}"

        def listar_recursivamente(ftp: ftplib.FTP, caminho_atual: str) -> list[str]:
            arquivos_encontrados_local: list[str] = []
            try:
                for nome, fatos in ftp.mlsd(caminho_atual or "."):
                    tipo_item = fatos.get("type", "")
                    if nome in {".", ".."}:
                        continue
                    caminho_item = f"{caminho_atual}/{nome}" if caminho_atual else nome
                    if tipo_item == "file":
                        arquivos_encontrados_local.append(caminho_item)
                    elif tipo_item == "dir":
                        arquivos_encontrados_local.extend(listar_recursivamente(ftp, caminho_item))
                return arquivos_encontrados_local
            except (ftplib.error_perm, AttributeError):
                diretorio_original = ftp.pwd()
                try:
                    if caminho_atual:
                        ftp.cwd(caminho_atual)
                    for nome in ftp.nlst():
                        nome_limpo = PurePosixPath(nome).name
                        if nome_limpo in {".", ".."}:
                            continue
                        try:
                            ftp.cwd(nome_limpo)
                            subdiretorio = f"{caminho_atual}/{nome_limpo}" if caminho_atual else nome_limpo
                            arquivos_encontrados_local.extend(listar_recursivamente(ftp, subdiretorio))
                            ftp.cwd("..")
                        except ftplib.error_perm:
                            caminho_arquivo = f"{caminho_atual}/{nome_limpo}" if caminho_atual else nome_limpo
                            arquivos_encontrados_local.append(caminho_arquivo)
                finally:
                    ftp.cwd(diretorio_original)
                return arquivos_encontrados_local

        with ftplib.FTP(host, timeout=300) as ftp:
            ftp.login()
            caminhos_brutos = list(dict.fromkeys(listar_recursivamente(ftp, caminho_pasta)))

        raiz = PurePosixPath(caminho_pasta)
        for caminho_remoto in caminhos_brutos:
            nome_arquivo = PurePosixPath(caminho_remoto).name
            if Path(nome_arquivo).suffix.lower() not in extensoes_processaveis:
                continue
            rel = str(PurePosixPath(caminho_remoto).relative_to(raiz))
            resultados.append({"url": montar_url_ftp(caminho_remoto), "caminho_relativo": rel})
        return resultados

    def listar_arquivos_http(
        url_pasta: str,
        urls_visitadas: Optional[set[str]] = None,
        caminho_base_raiz: Optional[PurePosixPath] = None,
    ) -> list[ArquivoDescoberto]:
        if urls_visitadas is None:
            urls_visitadas = set()
        base_url = url_pasta if url_pasta.endswith("/") else f"{url_pasta}/"
        if base_url in urls_visitadas:
            return []
        urls_visitadas.add(base_url)

        resposta = requests.get(base_url, timeout=300)
        resposta.raise_for_status()
        soup = BeautifulSoup(resposta.text, "html.parser")
        url_base_parseada = urlparse(base_url)
        host_base = url_base_parseada.netloc
        caminho_base_atual = PurePosixPath(url_base_parseada.path.rstrip("/"))
        if caminho_base_raiz is None:
            caminho_base_raiz = caminho_base_atual

        arquivos_encontrados: list[ArquivoDescoberto] = []
        for link in soup.find_all("a", href=True):
            href = link["href"].strip()
            if not href or href in {"../", "./", "/"} or href.startswith("#") or href.startswith("?"):
                continue
            url_absoluta = urljoin(base_url, href)
            url_absoluta_parseada = urlparse(url_absoluta)
            if url_absoluta_parseada.netloc != host_base:
                continue
            caminho_absoluto = PurePosixPath(url_absoluta_parseada.path.rstrip("/"))
            try:
                caminho_relativo = str(caminho_absoluto.relative_to(caminho_base_raiz))
            except Exception:
                continue
            nome_arquivo = caminho_absoluto.name
            if not nome_arquivo:
                continue
            if href.endswith("/"):
                arquivos_encontrados.extend(listar_arquivos_http(url_absoluta, urls_visitadas, caminho_base_raiz))
                continue
            if Path(nome_arquivo).suffix.lower() not in extensoes_processaveis:
                continue
            arquivos_encontrados.append({"url": url_absoluta, "caminho_relativo": caminho_relativo})

        vistos: set[tuple[str, str]] = set()
        saida: list[ArquivoDescoberto] = []
        for item in arquivos_encontrados:
            chave = (item["url"], item["caminho_relativo"])
            if chave not in vistos:
                vistos.add(chave)
                saida.append(item)
        return saida

    def inferir_encoding_e_separador_csv(caminho_csv: Path) -> tuple[str, str]:
        amostra_bytes = caminho_csv.read_bytes()[:100_000]
        for encoding in encodings_csv_teste:
            try:
                texto = amostra_bytes.decode(encoding)
                try:
                    dialeto = csv.Sniffer().sniff(texto, delimiters=[",", ";", "\t", "|"])
                    separador = dialeto.delimiter
                except Exception:
                    separador = ";"
                return encoding, separador
            except Exception:
                continue
        raise ValueError(f"Falha ao inferir encoding do CSV: {caminho_csv}")

    def apagar_arquivo_staging(caminho_arquivo: Path) -> None:
        try:
            gc.collect()
            if caminho_arquivo.exists() and caminho_arquivo.is_file():
                caminho_arquivo.unlink()
        except Exception as erro:
            erros.append({"arquivo": str(caminho_arquivo), "erro": f"Falha ao apagar arquivo bruto temporário: {erro}"})

    def limpar_csvs_em_downloads() -> None:
        """
        Segurança adicional: remove qualquer bruto tabular que, por código antigo
        ou execução parcial, tenha ficado dentro de `ingestao/downloads`.
        Não remove Parquet.
        """
        if not contexto.pasta_downloads.exists():
            return
        for caminho in contexto.pasta_downloads.rglob("*"):
            if caminho.is_file() and caminho.suffix.lower() in extensoes_tabulares_suportadas:
                apagar_arquivo_staging(caminho)

    def aplicar_filtros_where_lazy(lf: pl.LazyFrame) -> pl.LazyFrame:
        for filtro in filtros_extracao:
            lf = lf.filter(filtro)
        if where:
            contexto_sql = pl.SQLContext()
            contexto_sql.register("dados", lf)
            lf = contexto_sql.execute(f"SELECT * FROM dados WHERE {where.strip()}", eager=False)
        return lf

    def selecionar_colunas_parquet_lazy(*, lf: pl.LazyFrame, caminho_arquivo: Path) -> pl.LazyFrame:
        if colunas_extracao is None:
            return lf
        colunas_disponiveis = lf.collect_schema().names()
        ausentes = [coluna for coluna in colunas_extracao if coluna not in colunas_disponiveis]
        if ausentes:
            raise ValueError(
                f"Coluna(s) não encontrada(s) em {caminho_arquivo}: {ausentes}. "
                f"Colunas disponíveis: {colunas_disponiveis}"
            )
        return lf.select(colunas_extracao)

    def converter_arquivo_tabular_para_parquet_bruto_temporario(
        caminho_entrada: Path,
        caminho_saida_parquet_temporario: Path,
        *,
        apagar_bruto_apos_conversao: bool = True,
    ) -> None:
        """
        Converte o bruto tabular para um Parquet temporário SEM aplicar colunas/filtros/where.
        A aplicação lazy ocorre depois, via scan_parquet, atendendo ao requisito do usuário.
        """
        caminho_saida_parquet_temporario.parent.mkdir(parents=True, exist_ok=True)
        arquivos_temporarios: list[Path] = []

        def transcodificar_csv_para_utf8_temporario_local(caminho_csv: Path, *, encoding_origem: str) -> Path:
            pasta_temporaria = contexto.pasta_ingestao / "_csv_utf8_temporario"
            pasta_temporaria.mkdir(parents=True, exist_ok=True)
            caminho_saida_utf8 = pasta_temporaria / f"{caminho_csv.stem}_utf8.csv"
            with caminho_csv.open("r", encoding=encoding_origem, newline="") as entrada:
                with caminho_saida_utf8.open("w", encoding="utf-8", newline="") as saida:
                    shutil.copyfileobj(entrada, saida)
            arquivos_temporarios.append(caminho_saida_utf8)
            return caminho_saida_utf8

        def criar_lazyframe_para_conversao(caminho_arquivo: Path) -> pl.LazyFrame:
            extensao = caminho_arquivo.suffix.lower()
            if extensao == ".csv":
                encoding, separador = inferir_encoding_e_separador_csv(caminho_arquivo)
                caminho_csv_para_scan = caminho_arquivo
                encoding_scan = "utf8"
                if encoding == "latin1":
                    caminho_csv_para_scan = transcodificar_csv_para_utf8_temporario_local(
                        caminho_arquivo,
                        encoding_origem=encoding,
                    )
                elif encoding == "utf-8-sig":
                    encoding_scan = "utf8-lossy"
                return pl.scan_csv(
                    caminho_csv_para_scan,
                    separator=separador,
                    encoding=encoding_scan,
                    low_memory=True,
                    ignore_errors=True,
                )
            if extensao == ".xlsx":
                return pl.read_excel(caminho_arquivo, engine="openpyxl").lazy()
            if extensao in {".xls", ".ods"}:
                return pl.read_excel(caminho_arquivo, engine="calamine").lazy()
            raise ValueError(f"Extensão não suportada para conversão Parquet: {caminho_arquivo.suffix}")

        conversao_concluida = False
        lf: Optional[pl.LazyFrame] = None
        try:
            lf = criar_lazyframe_para_conversao(caminho_entrada)
            lf.sink_parquet(caminho_saida_parquet_temporario)
            if not caminho_saida_parquet_temporario.exists():
                raise FileNotFoundError(f"O Parquet temporário não foi gerado: {caminho_saida_parquet_temporario}")
            conversao_concluida = True
        finally:
            del lf
            gc.collect()
            for arquivo_temporario in arquivos_temporarios:
                apagar_arquivo_staging(arquivo_temporario)
            if apagar_bruto_apos_conversao and conversao_concluida:
                apagar_arquivo_staging(caminho_entrada)

    def materializar_parquet_final_a_partir_de_parquet(
        caminho_parquet_origem: Path,
        caminho_saida_parquet_final: Path,
        *,
        apagar_parquet_origem_apos_sucesso: bool = True,
    ) -> None:
        """
        Lê um Parquet com scan_parquet (lazy), aplica filtros/where/colunas e grava o Parquet final.
        """
        caminho_saida_parquet_final.parent.mkdir(parents=True, exist_ok=True)
        lf: Optional[pl.LazyFrame] = None
        materializacao_ok = False
        try:
            lf = pl.scan_parquet(caminho_parquet_origem)
            lf = aplicar_filtros_where_lazy(lf)
            lf = selecionar_colunas_parquet_lazy(lf=lf, caminho_arquivo=caminho_parquet_origem)
            lf.sink_parquet(caminho_saida_parquet_final)
            if not caminho_saida_parquet_final.exists():
                raise FileNotFoundError(f"O Parquet final não foi gerado: {caminho_saida_parquet_final}")
            materializacao_ok = True
        finally:
            del lf
            gc.collect()
            if apagar_parquet_origem_apos_sucesso and materializacao_ok:
                apagar_arquivo_staging(caminho_parquet_origem)

    def converter_tabular_em_parquet_final(
        caminho_tabular_bruto: Path,
        caminho_saida_parquet_final: Path,
        *,
        apagar_bruto_apos_conversao: bool = True,
    ) -> None:
        """
        Pipeline mínimo exigido pelo requisito:
        1) bruto tabular (temporário) -> parquet temporário
        2) scan_parquet(lazy) + filtros/where/colunas -> parquet final
        """
        pasta_parquet_temporario = contexto.pasta_ingestao / "_parquet_temporario"
        pasta_parquet_temporario.mkdir(parents=True, exist_ok=True)
        caminho_parquet_temporario = pasta_parquet_temporario / f"{caminho_tabular_bruto.stem}.parquet"

        converter_arquivo_tabular_para_parquet_bruto_temporario(
            caminho_tabular_bruto,
            caminho_parquet_temporario,
            apagar_bruto_apos_conversao=apagar_bruto_apos_conversao,
        )
        materializar_parquet_final_a_partir_de_parquet(
            caminho_parquet_temporario,
            caminho_saida_parquet_final,
            apagar_parquet_origem_apos_sucesso=True,
        )

    def extrair_zip_recursivamente(
        *,
        caminho_zip: Path,
        origem_url: str,
        grupo_mescla_padrao: Optional[str],
        prefixo_origem_zip: Optional[str] = None,
        profundidade_atual: int = 0,
    ) -> None:
        if profundidade_atual > profundidade_maxima_zip:
            raise ValueError(f"Profundidade máxima de ZIP excedida ({profundidade_maxima_zip}): {caminho_zip}")

        nome_zip_atual = caminho_zip.name
        cadeia_origem_zip = f"{prefixo_origem_zip}::{nome_zip_atual}" if prefixo_origem_zip else nome_zip_atual
        pasta_destino_zip = contexto.pasta_extraidos / caminho_zip.stem
        if profundidade_atual > 0:
            pasta_destino_zip = contexto.pasta_extraidos / f"{caminho_zip.stem}_nivel_{profundidade_atual}"
        pasta_destino_zip.mkdir(parents=True, exist_ok=True)

        pasta_extraidos_temporarios = contexto.pasta_ingestao / "_extraidos_temporarios" / caminho_zip.stem
        pasta_extraidos_temporarios.mkdir(parents=True, exist_ok=True)

        imprimir_log(
            fase="ingestão",
            funcao="extrair_zip_recursivamente",
            etapa=f"Extraindo ZIP para staging temporário (nível {profundidade_atual})",
            caminho=str(pasta_extraidos_temporarios),
            arquivo=nome_zip_atual,
            processados=contexto.processados,
            total=contexto.total_planejado,
            icone="🗜️",
        )

        with ZipFile(caminho_zip) as arquivo_zip:
            infos_arquivos = [info for info in arquivo_zip.infolist() if not info.is_dir()]
            total_interno = len(infos_arquivos)
            processados_interno = 0

            for info in infos_arquivos:
                nome_interno = info.filename
                caminho_interno = Path(nome_interno)
                nome_item = caminho_interno.name
                extensao_item = caminho_interno.suffix.lower()

                if nome_deve_ser_ignorado(nome_item):
                    arquivos_ignorados.append(f"{cadeia_origem_zip}::{nome_interno}")
                    processados_interno += 1
                    continue

                destino_item = pasta_extraidos_temporarios / caminho_interno
                destino_item.parent.mkdir(parents=True, exist_ok=True)

                try:
                    with arquivo_zip.open(info) as origem, destino_item.open("wb") as destino:
                        shutil.copyfileobj(origem, destino)

                    if extensao_item == ".zip":
                        grupo_zip_interno = grupo_mescla_padrao or obter_grupo_mescla(nome_item)
                        extrair_zip_recursivamente(
                            caminho_zip=destino_item,
                            origem_url=origem_url,
                            grupo_mescla_padrao=grupo_zip_interno,
                            prefixo_origem_zip=cadeia_origem_zip,
                            profundidade_atual=profundidade_atual + 1,
                        )
                        apagar_arquivo_staging(destino_item)
                        processados_interno += 1
                        continue

                    if extensao_item in extensoes_tabulares_suportadas:
                        caminho_parquet_saida = pasta_destino_zip / caminho_interno.with_suffix(".parquet")
                        caminho_parquet_saida.parent.mkdir(parents=True, exist_ok=True)

                        imprimir_log(
                            fase="ingestão",
                            funcao="extrair_zip_recursivamente",
                            etapa="Convertendo extraído temporário -> Parquet temporário -> Parquet final lazy",
                            caminho=str(caminho_parquet_saida.parent),
                            arquivo=caminho_parquet_saida.name,
                            processados=processados_interno,
                            total=total_interno,
                            icone="🪵",
                        )

                        converter_tabular_em_parquet_final(
                            destino_item,
                            caminho_parquet_saida,
                            apagar_bruto_apos_conversao=True,
                        )

                        escrever_manifesto(
                            {
                                "caminho_local": str(caminho_parquet_saida),
                                "caminho_relativo": str(
                                    (Path("extraidos") / pasta_destino_zip.name / caminho_interno.with_suffix(".parquet")).as_posix()
                                ),
                                "nome_arquivo": caminho_parquet_saida.name,
                                "origem_url": origem_url,
                                "arquivo_origem": f"{cadeia_origem_zip}::{nome_interno}",
                                "grupo_mescla_padrao": grupo_mescla_padrao or "",
                                "origem_tipo": "zip_extraido_convertido_parquet",
                            }
                        )
                    else:
                        arquivos_ignorados.append(f"{cadeia_origem_zip}::{nome_interno}")
                        apagar_arquivo_staging(destino_item)

                    processados_interno += 1
                except Exception as erro:
                    erros.append({"arquivo": f"{cadeia_origem_zip}::{nome_interno}", "erro": f"Falha ao processar item interno do ZIP: {erro}"})
                    apagar_arquivo_staging(destino_item)
                    processados_interno += 1

    def ingerir_arquivo_unico(url: str) -> None:
        nome_arquivo = Path(urlparse(url).path).name
        extensao = Path(nome_arquivo).suffix.lower()

        if nome_deve_ser_ignorado(nome_arquivo):
            arquivos_ignorados.append(nome_arquivo)
            atualizar_progresso_global(
                fase="ingestão",
                funcao="ingerir_arquivo_unico",
                etapa="Arquivo ignorado pela regra do nome",
                caminho=url,
                arquivo=nome_arquivo,
                icone="🚫",
            )
            return

        pasta_brutos_temporarios = contexto.pasta_ingestao / "_brutos_temporarios"
        pasta_brutos_temporarios.mkdir(parents=True, exist_ok=True)
        destino_download = pasta_brutos_temporarios / nome_arquivo

        imprimir_log(
            fase="ingestão",
            funcao="ingerir_arquivo_unico",
            etapa="Baixando arquivo único para staging temporário",
            caminho=url,
            arquivo=nome_arquivo,
            processados=contexto.processados,
            total=contexto.total_planejado,
            icone="🌐",
        )
        baixar_arquivo_remoto_para_disco(url, destino_download)

        if extensao == ".zip":
            grupo_zip = obter_grupo_mescla(nome_arquivo)
            extrair_zip_recursivamente(
                caminho_zip=destino_download,
                origem_url=url,
                grupo_mescla_padrao=grupo_zip,
                prefixo_origem_zip=None,
                profundidade_atual=0,
            )
            apagar_arquivo_staging(destino_download)
            atualizar_progresso_global(
                fase="ingestão",
                funcao="ingerir_arquivo_unico",
                etapa="ZIP processado e bruto temporário removido",
                caminho=str(destino_download.parent),
                arquivo=nome_arquivo,
                icone="✅",
            )
            limpar_csvs_em_downloads()
            return

        if extensao in extensoes_tabulares_suportadas:
            caminho_parquet_saida = contexto.pasta_downloads / f"{destino_download.stem}.parquet"
            caminho_parquet_saida.parent.mkdir(parents=True, exist_ok=True)

            imprimir_log(
                fase="ingestão",
                funcao="ingerir_arquivo_unico",
                etapa="Convertendo bruto temporário -> Parquet temporário -> Parquet final lazy",
                caminho=str(caminho_parquet_saida.parent),
                arquivo=caminho_parquet_saida.name,
                processados=contexto.processados,
                total=contexto.total_planejado,
                icone="🪵",
            )

            converter_tabular_em_parquet_final(
                destino_download,
                caminho_parquet_saida,
                apagar_bruto_apos_conversao=True,
            )

            escrever_manifesto(
                {
                    "caminho_local": str(caminho_parquet_saida),
                    "caminho_relativo": caminho_parquet_saida.name,
                    "nome_arquivo": caminho_parquet_saida.name,
                    "origem_url": url,
                    "arquivo_origem": nome_arquivo,
                    "grupo_mescla_padrao": "",
                    "origem_tipo": "arquivo_tabular_convertido_parquet",
                }
            )

            limpar_csvs_em_downloads()
            atualizar_progresso_global(
                fase="ingestão",
                funcao="ingerir_arquivo_unico",
                etapa="Arquivo tabular convertido para Parquet final, bruto temporário removido e manifesto registrado",
                caminho=str(caminho_parquet_saida.parent),
                arquivo=caminho_parquet_saida.name,
                icone="✅",
            )
            return

        arquivos_ignorados.append(nome_arquivo)
        apagar_arquivo_staging(destino_download)
        atualizar_progresso_global(
            fase="ingestão",
            funcao="ingerir_arquivo_unico",
            etapa="Arquivo ignorado por extensão incompatível e bruto temporário removido",
            caminho=str(destino_download.parent),
            arquivo=nome_arquivo,
            icone="⚠️",
        )

    def ingerir_listagem_remota(arquivos_descobertos: list[ArquivoDescoberto]) -> None:
        pasta_brutos_temporarios = contexto.pasta_ingestao / "_brutos_temporarios"
        pasta_brutos_temporarios.mkdir(parents=True, exist_ok=True)

        for item in arquivos_descobertos:
            url_arquivo = item["url"]
            caminho_relativo = item["caminho_relativo"]
            nome_arquivo = Path(caminho_relativo).name
            extensao = Path(nome_arquivo).suffix.lower()

            if nome_deve_ser_ignorado(nome_arquivo):
                arquivos_ignorados.append(nome_arquivo)
                atualizar_progresso_global(
                    fase="ingestão",
                    funcao="ingerir_listagem_remota",
                    etapa="Arquivo ignorado pela regra do nome",
                    caminho=str(Path(caminho_relativo).parent),
                    arquivo=nome_arquivo,
                    icone="🚫",
                )
                continue

            destino_download = pasta_brutos_temporarios / Path(caminho_relativo)
            destino_download.parent.mkdir(parents=True, exist_ok=True)

            try:
                imprimir_log(
                    fase="ingestão",
                    funcao="ingerir_listagem_remota",
                    etapa="Baixando item remoto para staging temporário",
                    caminho=str(Path(caminho_relativo).parent),
                    arquivo=nome_arquivo,
                    processados=contexto.processados,
                    total=contexto.total_planejado,
                    icone="📡",
                )
                baixar_arquivo_remoto_para_disco(url_arquivo, destino_download)

                if extensao == ".zip":
                    grupo_zip = obter_grupo_mescla(nome_arquivo)
                    extrair_zip_recursivamente(
                        caminho_zip=destino_download,
                        origem_url=url_arquivo,
                        grupo_mescla_padrao=grupo_zip,
                        prefixo_origem_zip=None,
                        profundidade_atual=0,
                    )
                    apagar_arquivo_staging(destino_download)
                    atualizar_progresso_global(
                        fase="ingestão",
                        funcao="ingerir_listagem_remota",
                        etapa=f"ZIP processado e bruto temporário removido (grupo={grupo_zip})",
                        caminho=str(destino_download.parent),
                        arquivo=nome_arquivo,
                        icone="✅",
                    )
                    limpar_csvs_em_downloads()
                    continue

                if extensao in extensoes_tabulares_suportadas:
                    caminho_parquet_saida = contexto.pasta_downloads / Path(caminho_relativo).with_suffix(".parquet")
                    caminho_parquet_saida.parent.mkdir(parents=True, exist_ok=True)

                    imprimir_log(
                        fase="ingestão",
                        funcao="ingerir_listagem_remota",
                        etapa="Convertendo bruto remoto -> Parquet temporário -> Parquet final lazy",
                        caminho=str(caminho_parquet_saida.parent),
                        arquivo=caminho_parquet_saida.name,
                        processados=contexto.processados,
                        total=contexto.total_planejado,
                        icone="🪵",
                    )

                    converter_tabular_em_parquet_final(
                        destino_download,
                        caminho_parquet_saida,
                        apagar_bruto_apos_conversao=True,
                    )

                    escrever_manifesto(
                        {
                            "caminho_local": str(caminho_parquet_saida),
                            "caminho_relativo": str(Path(caminho_relativo).with_suffix(".parquet")),
                            "nome_arquivo": caminho_parquet_saida.name,
                            "origem_url": url_arquivo,
                            "arquivo_origem": nome_arquivo,
                            "grupo_mescla_padrao": "",
                            "origem_tipo": "arquivo_remoto_convertido_parquet",
                        }
                    )
                    limpar_csvs_em_downloads()
                    atualizar_progresso_global(
                        fase="ingestão",
                        funcao="ingerir_listagem_remota",
                        etapa="Arquivo tabular convertido para Parquet final, bruto temporário removido e manifesto registrado",
                        caminho=str(caminho_parquet_saida.parent),
                        arquivo=caminho_parquet_saida.name,
                        icone="✅",
                    )
                    continue

                arquivos_ignorados.append(nome_arquivo)
                apagar_arquivo_staging(destino_download)
                atualizar_progresso_global(
                    fase="ingestão",
                    funcao="ingerir_listagem_remota",
                    etapa="Arquivo ignorado por extensão incompatível e bruto temporário removido",
                    caminho=str(destino_download.parent),
                    arquivo=nome_arquivo,
                    icone="⚠️",
                )
            except Exception as erro:
                erros.append({"arquivo": nome_arquivo, "erro": str(erro)})
                apagar_arquivo_staging(destino_download)
                atualizar_progresso_global(
                    fase="ingestão",
                    funcao="ingerir_listagem_remota",
                    etapa="Erro ao baixar/processar item remoto",
                    caminho=str(destino_download.parent),
                    arquivo=nome_arquivo,
                    icone="❌",
                )

    def ingerir_origem() -> None:
        tipo_origem = detectar_tipo_origem(url_origem)
        imprimir_log(
            fase="ingestão",
            funcao="ingerir_origem",
            etapa=f"Origem detectada: {tipo_origem}",
            caminho=url_origem,
            arquivo=Path(urlparse(url_origem).path).name or url_origem,
            processados=0,
            total=0,
            icone="🚀",
        )
        
        if tipo_origem in {"arquivo_zip", "arquivo_tabular"}:
            definir_total_arquivos(1, url_origem)
            ingerir_arquivo_unico(url_origem)
            return

        if tipo_origem == "ftp_pasta":
            arquivos_descobertos = listar_arquivos_ftp(url_origem)
            definir_total_arquivos(len(arquivos_descobertos), url_origem)
            ingerir_listagem_remota(arquivos_descobertos)
            return

        if tipo_origem == "http_pagina":
            arquivos_descobertos = listar_arquivos_http(url_origem)
            definir_total_arquivos(len(arquivos_descobertos), url_origem)
            ingerir_listagem_remota(arquivos_descobertos)
            return

    def ler_manifesto() -> pl.DataFrame:
        if not contexto.manifesto_arquivos.exists() or contexto.manifesto_arquivos.stat().st_size == 0:
            return pl.DataFrame()
        return pl.read_csv(contexto.manifesto_arquivos, encoding="utf8-lossy")

    def imprimir_chamados_leitura_parquet(caminhos_arquivos: dict[str, Path]) -> None:
        manifesto_df = ler_manifesto()
        print("\n" + "═" * 108)
        print("📌 COMANDOS PRONTOS PARA LEITURA DOS ARQUIVOS PARQUET NO POLARS")
        print("Cole os comandos abaixo no notebook:")
        print("import polars as pl")

        if not manifesto_df.is_empty():
            print("# Arquivos Parquet de ingestão")
            caminhos_impressos: set[str] = set()
            for registro in manifesto_df.iter_rows(named=True):
                caminho = Path(str(registro.get("caminho_local") or ""))
                caminho_str = str(caminho)
                if not caminho_str or caminho_str in caminhos_impressos:
                    continue
                caminhos_impressos.add(caminho_str)
                nome_variavel = f"lf_origem_{normalizar_nome_pasta_usuario(caminho.stem).lower()}"
                print(f'{nome_variavel} = pl.scan_parquet(r"{caminho_str}")')
            if caminhos_arquivos:
                print()

        if caminhos_arquivos:
            print("# Arquivos Parquet mesclados por grupo")
            for grupo in sorted(caminhos_arquivos):
                caminho = caminhos_arquivos[grupo]
                nome_variavel = f"lf_mescla_{normalizar_nome_arquivo_saida(grupo).lower()}"
                print(f'{nome_variavel} = pl.scan_parquet(r"{str(caminho)}")')
        print("═" * 108)

    def mesclar_arquivos_ingeridos() -> Optional[pl.DataFrame | pl.LazyFrame]:
        pasta_saida = Path(pasta_saida_mesclas) if pasta_saida_mesclas is not None else Path.cwd()
        manifesto_df = ler_manifesto()
        total_registros = manifesto_df.height

        definir_total_arquivos(total_registros, "mesclagem lazy com saída Parquet")

        if total_registros == 0:
            raise ValueError("Nenhum arquivo compatível foi ingerido para processamento.")

        if not mesclar:
            imprimir_chamados_leitura_parquet({})
            return None

        def selecionar_colunas_mescla(lf: pl.LazyFrame, caminho_arquivo: Path) -> pl.LazyFrame:
            if colunas_extracao is None:
                return lf
            colunas_saida = list(colunas_extracao)
            if incluir_colunas_metadados:
                colunas_saida.extend(["arquivo_origem", "grupo_mescla"])
            colunas_saida = list(dict.fromkeys(colunas_saida))
            colunas_disponiveis = lf.collect_schema().names()
            ausentes = [c for c in colunas_saida if c not in colunas_disponiveis]
            if ausentes:
                raise ValueError(
                    f"Coluna(s) não encontrada(s) em {caminho_arquivo}: {ausentes}. "
                    f"Colunas disponíveis: {colunas_disponiveis}"
                )
            return lf.select(colunas_saida)

        lazyframes_por_grupo: dict[str, list[pl.LazyFrame]] = {}

        for registro in manifesto_df.iter_rows(named=True):
            caminho_local = Path(str(registro.get("caminho_local") or ""))
            nome_arquivo = str(registro.get("nome_arquivo") or "")
            arquivo_origem = str(registro.get("arquivo_origem") or "")
            grupo_padrao_bruto = str(registro.get("grupo_mescla_padrao") or "").strip()
            grupo_padrao = grupo_padrao_bruto if grupo_padrao_bruto else None

            try:
                if nome_deve_ser_ignorado(nome_arquivo):
                    arquivos_ignorados.append(nome_arquivo)
                    atualizar_progresso_global(
                        fase="mesclagem",
                        funcao="mesclar_arquivos_ingeridos",
                        etapa="Arquivo ignorado por regra de nome",
                        caminho=str(caminho_local.parent),
                        arquivo=nome_arquivo,
                        icone="🚫",
                    )
                    continue

                if caminho_local.suffix.lower() != ".parquet":
                    raise ValueError(f"Manifesto deveria apontar para Parquet, mas apontou para: {caminho_local}")

                grupo_mescla = obter_grupo_mescla(nome_arquivo, grupo_padrao)
                lf = pl.scan_parquet(caminho_local)

                if incluir_colunas_metadados:
                    lf = lf.with_columns(
                        pl.lit(arquivo_origem).alias("arquivo_origem"),
                        pl.lit(grupo_mescla).alias("grupo_mescla"),
                    )

                lf = selecionar_colunas_mescla(lf, caminho_local)
                lazyframes_por_grupo.setdefault(grupo_mescla, []).append(lf)

                if arquivo_origem not in arquivos_lidos:
                    arquivos_lidos.append(arquivo_origem)

                atualizar_progresso_global(
                    fase="mesclagem",
                    funcao="mesclar_arquivos_ingeridos",
                    etapa=f"Plano lazy criado para o grupo {grupo_mescla}",
                    caminho=str(caminho_local.parent),
                    arquivo=nome_arquivo,
                    icone="✅",
                )
            except Exception as erro:
                erros.append({"arquivo": arquivo_origem or nome_arquivo, "erro": f"Falha ao criar plano lazy: {erro}"})
                atualizar_progresso_global(
                    fase="mesclagem",
                    funcao="mesclar_arquivos_ingeridos",
                    etapa="Erro ao criar plano lazy",
                    caminho=str(caminho_local.parent),
                    arquivo=nome_arquivo,
                    icone="❌",
                )

        if not lazyframes_por_grupo:
            raise ValueError("Nenhum plano LazyFrame foi criado com sucesso.")

        grupos_processados = 0
        for grupo_mescla, lazyframes_grupo in lazyframes_por_grupo.items():
            caminho_saida = pasta_saida / f"mescla_{normalizar_nome_arquivo_saida(grupo_mescla)}.parquet"
            caminho_saida.parent.mkdir(parents=True, exist_ok=True)
            try:
                lf_grupo = pl.concat(lazyframes_grupo, how="diagonal_relaxed")
                lf_grupo.sink_parquet(caminho_saida)
                caminhos_mesclas_geradas[grupo_mescla] = caminho_saida
                grupos_processados += 1
                imprimir_log(
                    fase="saída",
                    funcao="mesclar_arquivos_ingeridos",
                    etapa=f"Parquet salvo com sucesso para o grupo {grupo_mescla}",
                    caminho=str(caminho_saida.parent),
                    arquivo=caminho_saida.name,
                    processados=grupos_processados,
                    total=len(lazyframes_por_grupo),
                    icone="✅",
                )
            except Exception as erro:
                erros.append({"arquivo": caminho_saida.name, "erro": f"Falha ao salvar Parquet com sink_parquet: {erro}"})

        if not caminhos_mesclas_geradas:
            raise ValueError("Nenhum arquivo Parquet de mescla foi gerado com sucesso.")

        limpar_csvs_em_downloads()
        imprimir_chamados_leitura_parquet(caminhos_mesclas_geradas)

        if not retornar_df_unificado and not retornar_lazyframe_unificado:
            return None

        lazyframes_finais = [pl.scan_parquet(caminho) for caminho in caminhos_mesclas_geradas.values() if caminho.exists()]
        if not lazyframes_finais:
            return None
        lf_unificado = pl.concat(lazyframes_finais, how="diagonal_relaxed")
        if retornar_lazyframe_unificado:
            return lf_unificado
        return lf_unificado.collect()

    imprimir_log(
        fase="inicialização",
        funcao="baixar_extrair_mesclar",
        etapa="Início do processamento",
        caminho=url_origem,
        arquivo=Path(urlparse(url_origem).path).name or url_origem,
        processados=0,
        total=0,
        icone="🚀",
    )

    ingerir_origem()
    limpar_csvs_em_downloads()

    manifesto_df = ler_manifesto()
    if manifesto_df.is_empty():
        if erros:
            exemplos = "; ".join(
                f"{item['arquivo']}: {item['erro']}"
                for item in erros[:5]
            )
            raise ValueError(
                "Nenhum arquivo foi materializado com sucesso e o manifesto ficou vazio. "
                f"Primeiros erros de ingestão/materialização: {exemplos}"
            )
        raise ValueError(
            "Nenhum arquivo foi materializado com sucesso e o manifesto ficou vazio."
        )

    df_unificado = mesclar_arquivos_ingeridos()
    limpar_csvs_em_downloads()

    conteudo_zip: Optional[bytes] = None
    if reter_conteudo_zip_em_memoria:
        tipo_origem = detectar_tipo_origem(url_origem)
        if tipo_origem == "arquivo_zip":
            nome_zip = Path(urlparse(url_origem).path).name
            caminho_zip = contexto.pasta_ingestao / "_brutos_temporarios" / nome_zip
            if caminho_zip.exists():
                conteudo_zip = caminho_zip.read_bytes()

    imprimir_log(
        fase="finalização",
        funcao="baixar_extrair_mesclar",
        etapa="Processamento concluído com sucesso",
        caminho=str(contexto.pasta_execucao),
        arquivo="resultado final",
        processados=contexto.processados,
        total=contexto.total_planejado,
        icone="🏁",
    )

    return {
        "conteudo_zip": conteudo_zip,
        "pasta_execucao": str(contexto.pasta_execucao),
        "pasta_ingestao": str(contexto.pasta_ingestao),
        "manifesto_arquivos": str(contexto.manifesto_arquivos),
        "arquivos_lidos": arquivos_lidos,
        "arquivos_ignorados": arquivos_ignorados,
        "erros": erros,
        "df_unificado": df_unificado,
    }
