from __future__ import annotations

import csv
import ftplib
import shutil
import unicodedata
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path, PurePosixPath
from typing import IO, Iterator, Optional, TypedDict
from urllib.parse import unquote, urljoin, urlparse
from zipfile import ZipFile

import polars as pl
import requests
from bs4 import BeautifulSoup

class ErroLeitura(TypedDict):
    arquivo: str
    erro: str


class ResultadoMesclagem(TypedDict):
    manifesto_arquivos: str
    arquivos_lidos: list[str]
    arquivos_ignorados: list[str]
    erros: list[ErroLeitura]
    caminhos_mesclas: dict[str, str]
    df_unificado: Optional[pl.DataFrame]


class ErroLeitura(TypedDict):
    """
    Estrutura padronizada para registro de erros ocorridos no processamento.
    """

    arquivo: str
    erro: str


class ResultadoProcessamento(TypedDict):
    """
    Estrutura de retorno do processamento principal.
    """

    conteudo_zip: Optional[bytes]
    pasta_execucao: str
    pasta_ingestao: str
    manifesto_arquivos: str
    arquivos_lidos: list[str]
    arquivos_ignorados: list[str]
    erros: list[ErroLeitura]
    df_unificado: Optional[pl.DataFrame]


class ArquivoDescoberto(TypedDict):
    """
    Representa um arquivo remoto descoberto em uma origem HTTP/HTTPS/FTP.
    """

    url: str
    caminho_relativo: str


class RegistroManifesto(TypedDict):
    """
    Linha persistida no manifesto de ingestão.
    """

    caminho_local: str
    caminho_relativo: str
    nome_arquivo: str
    origem_url: str
    arquivo_origem: str
    grupo_mescla_padrao: str
    origem_tipo: str


@dataclass(slots=True)
class ContextoExecucao:
    """
    Armazena os caminhos principais e contadores da execução.
    """

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
    chunk_size_csv: int = 100_000,
    retornar_df_unificado: bool = False,
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
    - Se for arquivo `.zip`, cria a pasta `processamentos` (caso não exista),
      cria uma subpasta de execução no formato `<nome>_<timestamp>`, baixa o ZIP
      para esta pasta, extrai os arquivos compatíveis nela e trata
      recursivamente ZIPs internos.
    - Se for arquivo tabular compatível, baixa o arquivo e o registra para
      processamento posterior. Se necessário, converte para CSV.
    - Se for pasta FTP, lista recursivamente os arquivos da estrutura e baixa
      os arquivos compatíveis mantendo a mesma estrutura do diretório remoto.
    - Se for página HTTP/HTTPS, faz o parsing da listagem, identifica links
      para arquivos compatíveis e, se necessário, percorre subpastas
      automaticamente, baixando os arquivos compatíveis e mantendo a estrutura.
    - Se qualquer uma das opções anteriores contiver arquivos `.zip`, a função
      baixa, extrai e processa recursivamente esses arquivos `.zip`.
    - Se houver apenas 1 arquivo compatível ingerido, a função não dispara a
      mesclagem, não gera arquivo `mescla_<GRUPO>.csv` e imprime apenas os
      comandos de leitura do(s) arquivo(s) original(is) baixado(s)/gerado(s).

    Extensões suportadas
    --------------------
    - `.csv`
    - `.xlsx`
    - `.xls`
    - `.ods`

    CSV
    ---
    Para arquivos CSV, a função tenta automaticamente múltiplos encodings:
    - `utf-8`
    - `utf-8-sig`
    - `latin1`

    Além disso, tenta inferir o separador automaticamente. Caso a inferência
    falhe, utiliza `;` como fallback.

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

    Arquivos do mesmo grupo são concatenados entre si somente quando houver
    mais de um arquivo compatível ingerido. Ao final, a função:
    - salva um arquivo `.csv` por grupo na pasta atual de saída
      (`Path.cwd()` ou `pasta_saida_mesclas`, se informada), com nomes no
      padrão `mescla_<GRUPO>.csv`, quando a mesclagem for aplicável;
    - opcionalmente retorna um `DataFrame` Polars unificado;
    - adiciona as colunas `arquivo_origem` e `grupo_mescla` ao resultado
      unificado, quando ele for montado;
    - imprime no console os comandos de leitura do Polars para os arquivos
      originais baixados/gerados e também para os arquivos mesclados, se houver.

    Pasta de execução
    -----------------
    - A função cria uma pasta `processamentos`, caso ela ainda não exista.
    - Dentro dela, cria a pasta da execução no formato:
      `<nome_informado_pelo_usuario>_<timestamp>`.
    - Se `nome_pasta_execucao` não for informado, a função solicita esse valor
      via `input()`.

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
    mesclar: bool, default True
        Chama a função de mescla quando = True
    url_origem : str
        URL da origem dos dados. Pode ser:
        - URL HTTP/HTTPS/FTP de um arquivo `.zip`
        - URL HTTP/HTTPS/FTP de um arquivo tabular compatível
        - URL FTP apontando para uma pasta
        - URL HTTP/HTTPS apontando para uma página com listagem de arquivos
        - URL HTTP/HTTPS/FTP apontando para uma estrutura com subpastas
    nome_pasta_execucao : Optional[str], default None
        Nome-base da pasta da execução, que será criado antes do timestamp.
        Exemplo: `ans_igr` -> `processamentos/ans_igr_20260324_001530`.
        Se None, a função solicitará esse valor ao usuário via `input()`.
    base_dir_execucao : Optional[Path | str], default None
        Diretório base onde a pasta `processamentos` será criada.
        Se None, usa `Path.cwd()`.
    pasta_saida_mesclas : Optional[Path | str], default None
        Pasta onde os arquivos `mescla_<GRUPO>.csv` serão salvos.
        Se None, usa `Path.cwd()`.
    chunk_size_csv : int, default 100_000
        Quantidade de linhas por lote na leitura de arquivos CSV.
    retornar_df_unificado : bool, default False
        Se True, monta e retorna um `DataFrame` unificado no final.
        Se False, evita esse custo adicional de memória.
    reter_conteudo_zip_em_memoria : bool, default False
        Se True e a URL original apontar diretamente para um `.zip`, retorna os
        bytes do ZIP em `conteudo_zip`.
    profundidade_maxima_zip : int, default 20
        Limite de profundidade para recursão de ZIP dentro de ZIP.

    Retorno
    -------
    ResultadoProcessamento
        Estrutura com:
        - `conteudo_zip`: bytes do ZIP baixado, quando aplicável e solicitado
        - `pasta_execucao`: pasta da execução criada dentro de `processamentos`
        - `pasta_ingestao`: pasta de staging/ingestão
        - `manifesto_arquivos`: caminho do manifesto CSV
        - `arquivos_lidos`: lista de arquivos lidos com sucesso
        - `arquivos_ignorados`: lista de arquivos ignorados
        - `erros`: lista de erros ocorridos
        - `df_unificado`: DataFrame Polars unificado, quando solicitado

    Efeitos colaterais
    ------------------
    Para cada grupo de mesclagem encontrado, a função salva um arquivo `.csv`
    na pasta de saída, no formato:
    - `mescla_CONS.csv`
    - `mescla_DET.csv`
    - `mescla_REM.csv`
    - etc.

    Também imprime no console comandos prontos do Polars para leitura dos
    arquivos originais baixados/gerados e dos arquivos mesclados, se houver.
    Se ocorrer erro ao salvar algum arquivo de saída, o erro será registrado na
    lista `erros`, sem interromper o processamento dos demais grupos.

    Raises
    ------
    requests.HTTPError
        Se ocorrer erro ao baixar um arquivo via HTTP/HTTPS.
    ValueError
        Se a URL não for suportada, se nenhum arquivo compatível for lido
        ou se o nome informado para a pasta da execução for inválido.
    """

    extensoes_tabulares_suportadas: set[str] = {".csv", ".xlsx", ".xls", ".ods"}
    extensoes_processaveis: set[str] = extensoes_tabulares_suportadas | {".zip"}
    encodings_csv_teste: tuple[str, ...] = ("utf-8", "utf-8-sig", "latin1")
    chunk_size_download: int = 1024 * 1024

    arquivos_lidos: list[str] = []
    arquivos_ignorados: list[str] = []
    erros: list[ErroLeitura] = []
    caminhos_mesclas_geradas: dict[str, Path] = {}

    def criar_barra_progresso(processados: int, total: int, largura: int = 26) -> str:
        if total <= 0:
            return "░" * largura
        percentual: float = min(max(processados / total, 0.0), 1.0)
        preenchidos: int = int(round(percentual * largura))
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
            barra: str = criar_barra_progresso(processados, total)
            percentual: float = (processados / total * 100) if total > 0 else 0.0
            faltam: int = max(total - processados, 0)
            print(f"📊 STATUS : [{barra}] {processados}/{total} ({percentual:,.1f}%)")
            print(f"⏳ FALTAM : {faltam}")
        print("═" * 108)

    def normalizar_nome_pasta_usuario(nome: str) -> str:
        nome_limpo: str = nome.strip()
        if not nome_limpo:
            raise ValueError("O nome da pasta de execução não pode ser vazio.")

        nome_normalizado: str = unicodedata.normalize("NFKD", nome_limpo)
        nome_sem_acento: str = "".join(
            caractere
            for caractere in nome_normalizado
            if not unicodedata.combining(caractere)
        )
        nome_sem_acento = (
            nome_sem_acento
            .replace(" ", "_")
            .replace("-", "_")
            .replace("/", "_")
            .replace("\\", "_")
        )

        caracteres_processados: list[str] = []
        for caractere in nome_sem_acento:
            caracteres_processados.append(
                caractere if caractere.isalnum() or caractere == "_" else "_"
            )

        nome_saida: str = "".join(caracteres_processados).strip("_")
        while "__" in nome_saida:
            nome_saida = nome_saida.replace("__", "_")

        if not nome_saida:
            raise ValueError("O nome informado para a pasta da execução é inválido.")
        return nome_saida

    def obter_nome_base_pasta_execucao() -> str:
        if nome_pasta_execucao is not None:
            return normalizar_nome_pasta_usuario(nome_pasta_execucao)
        nome_informado: str = input(
            "Informe o nome da pasta de processamento (será usado antes do timestamp): "
        )
        return normalizar_nome_pasta_usuario(nome_informado)

    def criar_pasta_timestamp(base_dir: Optional[Path | str] = None) -> ContextoExecucao:
        base: Path = Path(base_dir) if base_dir is not None else Path.cwd()
        pasta_processamentos: Path = base / "processamentos"
        pasta_processamentos.mkdir(parents=True, exist_ok=True)

        nome_base_pasta: str = obter_nome_base_pasta_execucao()
        timestamp: str = datetime.now().strftime("%Y%m%d_%H%M%S")

        pasta_execucao: Path = pasta_processamentos / f"{nome_base_pasta}_{timestamp}"
        pasta_ingestao: Path = pasta_execucao / "ingestao"
        pasta_downloads: Path = pasta_ingestao / "downloads"
        pasta_extraidos: Path = pasta_ingestao / "extraidos"
        manifesto_arquivos: Path = pasta_execucao / "manifesto_arquivos.csv"

        pasta_downloads.mkdir(parents=True, exist_ok=True)
        pasta_extraidos.mkdir(parents=True, exist_ok=True)

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

    contexto: ContextoExecucao = criar_pasta_timestamp(base_dir_execucao)

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
        nome_base: str = Path(nome_arquivo).stem
        nome_normalizado: str = unicodedata.normalize("NFKD", nome_base)
        nome_sem_acento: str = "".join(
            caractere
            for caractere in nome_normalizado
            if not unicodedata.combining(caractere)
        ).lower()
        return "dicionario" in nome_sem_acento

    def obter_grupo_mescla(nome_arquivo: str, grupo_padrao: Optional[str] = None) -> str:
        if grupo_padrao:
            return grupo_padrao

        nome_base: str = Path(nome_arquivo).stem
        nome_normalizado: str = unicodedata.normalize("NFKD", nome_base)
        nome_sem_acento: str = "".join(
            caractere
            for caractere in nome_normalizado
            if not unicodedata.combining(caractere)
        )

        nome_sem_acento = nome_sem_acento.replace("-", "_")
        partes_nome: list[str] = [parte for parte in nome_sem_acento.split("_") if parte]

        if not partes_nome:
            return "SEM_GRUPO"

        # PATCH MÍNIMO:
        # Caso especial para nomes como:
        # pda-043-rpc-201505
        # pda-043-rpc-201506
        # Ambos devem cair no mesmo grupo: 043_RPC_2015
        if (
            len(partes_nome) >= 4
            and partes_nome[0].lower() == "pda"
            and partes_nome[-1].isdigit()
            and len(partes_nome[-1]) == 6
        ):
            ano: str = partes_nome[-1][:4]
            codigo: str = partes_nome[1].upper()
            tipo: str = partes_nome[2].upper()
            return f"{codigo}_{tipo}_{ano}"

        return partes_nome[-1].upper()

    def normalizar_nome_arquivo_saida(grupo_mescla: str) -> str:
        grupo_normalizado: str = unicodedata.normalize("NFKD", grupo_mescla)
        grupo_sem_acento: str = "".join(
            caractere
            for caractere in grupo_normalizado
            if not unicodedata.combining(caractere)
        ).upper()
        caracteres_processados: list[str] = []
        for caractere in grupo_sem_acento:
            caracteres_processados.append(caractere if caractere.isalnum() else "_")
        nome_normalizado: str = "".join(caracteres_processados).strip("_")
        while "__" in nome_normalizado:
            nome_normalizado = nome_normalizado.replace("__", "_")
        return nome_normalizado or "SEM_GRUPO"

    def detectar_tipo_origem(url: str) -> str:
        url_parseada = urlparse(url)
        esquema: str = url_parseada.scheme.lower()
        extensao: str = Path(url_parseada.path).suffix.lower()
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
        raise ValueError(
            "URL não suportada. Informe um arquivo .zip, arquivo tabular, "
            "pasta FTP ou página HTTP/HTTPS com listagem."
        )

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
        host: str = url_parseada.hostname or ""
        caminho_remoto: str = unquote(url_parseada.path)
        if not host or not caminho_remoto:
            raise ValueError(f"URL FTP inválida: {url}")

        diretorio_remoto: str = str(PurePosixPath(caminho_remoto).parent)
        nome_arquivo: str = PurePosixPath(caminho_remoto).name

        with ftplib.FTP(host, timeout=300) as ftp:
            ftp.login()
            if diretorio_remoto not in {"", ".", "/"}:
                ftp.cwd(diretorio_remoto)
            with destino.open("wb") as arquivo_saida:
                ftp.retrbinary(f"RETR {nome_arquivo}", arquivo_saida.write)

    def baixar_arquivo_remoto_para_disco(url: str, destino: Path) -> None:
        esquema: str = urlparse(url).scheme.lower()
        if esquema in {"http", "https"}:
            baixar_http_para_arquivo(url, destino)
            return
        if esquema == "ftp":
            baixar_ftp_para_arquivo(url, destino)
            return
        raise ValueError(f"Esquema de URL não suportado: {esquema}")

    def listar_arquivos_ftp(url_ftp: str) -> list[ArquivoDescoberto]:
        url_parseada = urlparse(url_ftp)
        host: str = url_parseada.hostname or ""
        caminho_pasta: str = unquote(url_parseada.path).rstrip("/")
        if not host:
            raise ValueError(f"URL FTP inválida: {url_ftp}")

        resultados: list[ArquivoDescoberto] = []

        def montar_url_ftp(caminho_relativo: str) -> str:
            caminho_limpo: str = caminho_relativo.lstrip("/")
            return f"ftp://{host}/{caminho_limpo}"

        def listar_recursivamente(ftp: ftplib.FTP, caminho_atual: str) -> list[str]:
            arquivos_encontrados_local: list[str] = []
            try:
                for nome, fatos in ftp.mlsd(caminho_atual or "."):
                    tipo_item: str = fatos.get("type", "")
                    if nome in {".", ".."}:
                        continue
                    caminho_item: str = f"{caminho_atual}/{nome}" if caminho_atual else nome
                    if tipo_item == "file":
                        arquivos_encontrados_local.append(caminho_item)
                    elif tipo_item == "dir":
                        arquivos_encontrados_local.extend(listar_recursivamente(ftp, caminho_item))
                return arquivos_encontrados_local
            except (ftplib.error_perm, AttributeError):
                diretorio_original: str = ftp.pwd()
                try:
                    if caminho_atual:
                        ftp.cwd(caminho_atual)
                    for nome in ftp.nlst():
                        nome_limpo: str = PurePosixPath(nome).name
                        if nome_limpo in {".", ".."}:
                            continue
                        try:
                            ftp.cwd(nome_limpo)
                            subdiretorio: str = f"{caminho_atual}/{nome_limpo}" if caminho_atual else nome_limpo
                            arquivos_encontrados_local.extend(listar_recursivamente(ftp, subdiretorio))
                            ftp.cwd("..")
                        except ftplib.error_perm:
                            caminho_arquivo: str = f"{caminho_atual}/{nome_limpo}" if caminho_atual else nome_limpo
                            arquivos_encontrados_local.append(caminho_arquivo)
                finally:
                    ftp.cwd(diretorio_original)
                return arquivos_encontrados_local

        with ftplib.FTP(host, timeout=300) as ftp:
            ftp.login()
            caminhos_brutos: list[str] = list(dict.fromkeys(listar_recursivamente(ftp, caminho_pasta)))

        raiz = PurePosixPath(caminho_pasta)
        for caminho_remoto in caminhos_brutos:
            nome_arquivo: str = PurePosixPath(caminho_remoto).name
            extensao: str = Path(nome_arquivo).suffix.lower()
            if extensao not in extensoes_processaveis:
                continue
            rel: str = str(PurePosixPath(caminho_remoto).relative_to(raiz))
            resultados.append({"url": montar_url_ftp(caminho_remoto), "caminho_relativo": rel})
        return resultados

    def listar_arquivos_http(
        url_pasta: str,
        urls_visitadas: Optional[set[str]] = None,
        caminho_base_raiz: Optional[PurePosixPath] = None,
    ) -> list[ArquivoDescoberto]:
        """
        Lista recursivamente arquivos em uma página HTTP/HTTPS, seguindo apenas
        subpastas contidas no diretório raiz originalmente informado.

        Parameters
        ----------
        url_pasta : str
            URL da página/listagem.
        urls_visitadas : Optional[set[str]], default None
            Controle interno de recursão.
        caminho_base_raiz : Optional[PurePosixPath], default None
            Caminho raiz originalmente informado na primeira chamada. Usado para
            impedir que a recursão suba para níveis superiores.

        Returns
        -------
        list[ArquivoDescoberto]
            Arquivos descobertos.
        """
        if urls_visitadas is None:
            urls_visitadas = set()

        base_url: str = url_pasta if url_pasta.endswith("/") else f"{url_pasta}/"
        if base_url in urls_visitadas:
            return []
        urls_visitadas.add(base_url)

        resposta: requests.Response = requests.get(base_url, timeout=300)
        resposta.raise_for_status()

        soup = BeautifulSoup(resposta.text, "html.parser")
        url_base_parseada = urlparse(base_url)
        host_base: str = url_base_parseada.netloc
        caminho_base_atual = PurePosixPath(url_base_parseada.path.rstrip("/"))
        if caminho_base_raiz is None:
            caminho_base_raiz = caminho_base_atual

        arquivos_encontrados: list[ArquivoDescoberto] = []

        for link in soup.find_all("a", href=True):
            href: str = link["href"].strip()
            if not href or href in {"../", "./", "/"}:
                continue
            if href.startswith("#") or href.startswith("?"):
                continue

            url_absoluta: str = urljoin(base_url, href)
            url_absoluta_parseada = urlparse(url_absoluta)
            if url_absoluta_parseada.netloc != host_base:
                continue

            caminho_absoluto = PurePosixPath(url_absoluta_parseada.path.rstrip("/"))

            try:
                caminho_relativo: str = str(caminho_absoluto.relative_to(caminho_base_raiz))
            except Exception:
                # Ignora qualquer link que aponte para fora do diretório raiz
                # originalmente informado (por exemplo, ../ ou caminhos irmãos).
                continue

            nome_arquivo: str = caminho_absoluto.name
            if not nome_arquivo:
                continue

            eh_subpasta: bool = href.endswith("/")
            if eh_subpasta:
                arquivos_encontrados.extend(
                    listar_arquivos_http(
                        url_absoluta,
                        urls_visitadas,
                        caminho_base_raiz,
                    )
                )
                continue

            extensao: str = Path(nome_arquivo).suffix.lower()
            if extensao not in extensoes_processaveis:
                continue

            arquivos_encontrados.append(
                {
                    "url": url_absoluta,
                    "caminho_relativo": caminho_relativo,
                }
            )

        vistos: set[tuple[str, str]] = set()
        saida: list[ArquivoDescoberto] = []
        for item in arquivos_encontrados:
            chave: tuple[str, str] = (item["url"], item["caminho_relativo"])
            if chave not in vistos:
                vistos.add(chave)
                saida.append(item)
        return saida

    def inferir_encoding_e_separador_csv(caminho_csv: Path) -> tuple[str, str]:
        amostra_bytes: bytes = caminho_csv.read_bytes()[:100_000]
        for encoding in encodings_csv_teste:
            try:
                texto: str = amostra_bytes.decode(encoding)
                try:
                    dialeto = csv.Sniffer().sniff(texto, delimiters=[",", ";", "\t", "|"])
                    separador: str = dialeto.delimiter
                except Exception:
                    separador = ";"
                return encoding, separador
            except Exception:
                continue
        raise ValueError(f"Falha ao inferir encoding do CSV: {caminho_csv}")

    def iterar_lotes_csv_latin1(caminho_arquivo: Path, separador: str, encoding: str) -> Iterator[pl.DataFrame]:
        colunas: list[str] | None = None
        linhas_lote: list[dict[str, str]] = []

        with caminho_arquivo.open("r", encoding=encoding, newline="") as arquivo_csv:
            leitor = csv.DictReader(arquivo_csv, delimiter=separador)
            for linha in leitor:
                if colunas is None:
                    colunas = list(leitor.fieldnames or linha.keys())
                linhas_lote.append({coluna: linha.get(coluna, "") for coluna in colunas})
                if len(linhas_lote) >= chunk_size_csv:
                    yield pl.DataFrame(linhas_lote)
                    linhas_lote = []

        if linhas_lote:
            yield pl.DataFrame(linhas_lote)

    def iterar_lotes_tabulados(caminho_arquivo: Path) -> Iterator[pl.DataFrame]:
        extensao: str = caminho_arquivo.suffix.lower()

        if extensao == ".csv":
            encoding, separador = inferir_encoding_e_separador_csv(caminho_arquivo)
            if encoding == "latin1":
                yield from iterar_lotes_csv_latin1(caminho_arquivo, separador, encoding)
                return

            leitor = pl.scan_csv(
                caminho_arquivo,
                separator=separador,
                encoding="utf8-lossy" if encoding == "utf-8-sig" else "utf8",
                low_memory=True,
                ignore_errors=True,
            )

            for lote in leitor.collect_batches(chunk_size=chunk_size_csv):
                yield lote
            return

        if extensao == ".xlsx":
            yield pl.read_excel(caminho_arquivo, engine="openpyxl")
            return
        if extensao == ".xls":
            yield pl.read_excel(caminho_arquivo, engine="calamine")
            return
        if extensao == ".ods":
            yield pl.read_excel(caminho_arquivo, engine="calamine")
            return

        raise ValueError(f"Extensão não suportada: {caminho_arquivo.suffix}")

    def escrever_df_csv(df: pl.DataFrame, caminho_saida: Path, *, append: bool) -> None:
        caminho_saida.parent.mkdir(parents=True, exist_ok=True)
        modo: str = "a" if append else "w"

        with caminho_saida.open(modo, encoding="utf-8", newline="") as arquivo_saida:
            df.write_csv(arquivo_saida, include_header=not append)

    def converter_arquivo_tabular_para_csv(caminho_entrada: Path, caminho_saida_csv: Path) -> None:
        caminho_saida_csv.parent.mkdir(parents=True, exist_ok=True)
        primeiro_lote: bool = True
        for lote in iterar_lotes_tabulados(caminho_entrada):
            escrever_df_csv(lote, caminho_saida_csv, append=not primeiro_lote)
            primeiro_lote = False

    def extrair_zip_recursivamente(
        *,
        caminho_zip: Path,
        origem_url: str,
        grupo_mescla_padrao: Optional[str],
        prefixo_origem_zip: Optional[str] = None,
        profundidade_atual: int = 0,
    ) -> None:
        if profundidade_atual > profundidade_maxima_zip:
            raise ValueError(
                f"Profundidade máxima de ZIP excedida ({profundidade_maxima_zip}): {caminho_zip}"
            )

        nome_zip_atual: str = caminho_zip.name
        cadeia_origem_zip: str = (
            f"{prefixo_origem_zip}::{nome_zip_atual}" if prefixo_origem_zip else nome_zip_atual
        )

        pasta_destino_zip: Path = contexto.pasta_extraidos / caminho_zip.stem
        if profundidade_atual > 0:
            pasta_destino_zip = contexto.pasta_extraidos / f"{caminho_zip.stem}_nivel_{profundidade_atual}"
        pasta_destino_zip.mkdir(parents=True, exist_ok=True)

        imprimir_log(
            fase="ingestão",
            funcao="extrair_zip_recursivamente",
            etapa=f"Extraindo ZIP (nível {profundidade_atual})",
            caminho=str(pasta_destino_zip),
            arquivo=nome_zip_atual,
            processados=contexto.processados,
            total=contexto.total_planejado,
            icone="🗜️",
        )

        with ZipFile(caminho_zip) as arquivo_zip:
            infos_arquivos = [info for info in arquivo_zip.infolist() if not info.is_dir()]
            total_interno: int = len(infos_arquivos)
            processados_interno: int = 0

            for info in infos_arquivos:
                nome_interno: str = info.filename
                caminho_interno = Path(nome_interno)
                nome_item: str = caminho_interno.name
                extensao_item: str = caminho_interno.suffix.lower()

                imprimir_log(
                    fase="ingestão",
                    funcao="extrair_zip_recursivamente",
                    etapa=f"Processando item interno do ZIP (nível {profundidade_atual})",
                    caminho=str(caminho_interno.parent),
                    arquivo=nome_interno,
                    processados=processados_interno,
                    total=total_interno,
                    icone="📦",
                )

                if nome_deve_ser_ignorado(nome_item):
                    arquivos_ignorados.append(f"{cadeia_origem_zip}::{nome_interno}")
                    processados_interno += 1
                    continue

                destino_item: Path = pasta_destino_zip / caminho_interno
                destino_item.parent.mkdir(parents=True, exist_ok=True)

                try:
                    with arquivo_zip.open(info) as origem, destino_item.open("wb") as destino:
                        shutil.copyfileobj(origem, destino)

                    if extensao_item == ".zip":
                        grupo_zip_interno: str = grupo_mescla_padrao or obter_grupo_mescla(nome_item)
                        extrair_zip_recursivamente(
                            caminho_zip=destino_item,
                            origem_url=origem_url,
                            grupo_mescla_padrao=grupo_zip_interno,
                            prefixo_origem_zip=cadeia_origem_zip,
                            profundidade_atual=profundidade_atual + 1,
                        )
                        processados_interno += 1
                        continue

                    if extensao_item in extensoes_tabulares_suportadas:
                        escrever_manifesto(
                            {
                                "caminho_local": str(destino_item),
                                "caminho_relativo": str(
                                    (Path("extraidos") / pasta_destino_zip.name / caminho_interno).as_posix()
                                ),
                                "nome_arquivo": destino_item.name,
                                "origem_url": origem_url,
                                "arquivo_origem": f"{cadeia_origem_zip}::{nome_interno}",
                                "grupo_mescla_padrao": grupo_mescla_padrao or "",
                                "origem_tipo": "zip_extraido",
                            }
                        )
                    else:
                        arquivos_ignorados.append(f"{cadeia_origem_zip}::{nome_interno}")
                    processados_interno += 1
                except Exception as erro:
                    erros.append(
                        {
                            "arquivo": f"{cadeia_origem_zip}::{nome_interno}",
                            "erro": f"Falha ao processar item interno do ZIP: {erro}",
                        }
                    )
                    processados_interno += 1

    def ingerir_arquivo_unico(url: str) -> None:
        nome_arquivo: str = Path(urlparse(url).path).name
        extensao: str = Path(nome_arquivo).suffix.lower()

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

        destino_download: Path = contexto.pasta_downloads / nome_arquivo
        imprimir_log(
            fase="ingestão",
            funcao="ingerir_arquivo_unico",
            etapa="Baixando arquivo único",
            caminho=url,
            arquivo=nome_arquivo,
            processados=contexto.processados,
            total=contexto.total_planejado,
            icone="🌐",
        )
        baixar_arquivo_remoto_para_disco(url, destino_download)

        if extensao == ".zip":
            grupo_zip: str = obter_grupo_mescla(nome_arquivo)
            extrair_zip_recursivamente(
                caminho_zip=destino_download,
                origem_url=url,
                grupo_mescla_padrao=grupo_zip,
                prefixo_origem_zip=None,
                profundidade_atual=0,
            )
            atualizar_progresso_global(
                fase="ingestão",
                funcao="ingerir_arquivo_unico",
                etapa="ZIP baixado e extraído recursivamente para disco",
                caminho=str(destino_download),
                arquivo=nome_arquivo,
                icone="✅",
            )
            return

        if extensao in extensoes_tabulares_suportadas:
            if extensao == ".csv":
                escrever_manifesto(
                    {
                        "caminho_local": str(destino_download),
                        "caminho_relativo": destino_download.name,
                        "nome_arquivo": destino_download.name,
                        "origem_url": url,
                        "arquivo_origem": destino_download.name,
                        "grupo_mescla_padrao": "",
                        "origem_tipo": "arquivo_tabular",
                    }
                )
            else:
                nome_csv_saida: str = f"{destino_download.stem}.csv"
                caminho_csv_saida: Path = destino_download.with_name(nome_csv_saida)
                imprimir_log(
                    fase="ingestão",
                    funcao="ingerir_arquivo_unico",
                    etapa="Convertendo arquivo tabular para CSV de mesmo nome-base",
                    caminho=str(destino_download.parent),
                    arquivo=caminho_csv_saida.name,
                    processados=contexto.processados,
                    total=contexto.total_planejado,
                    icone="📝",
                )
                converter_arquivo_tabular_para_csv(destino_download, caminho_csv_saida)
                escrever_manifesto(
                    {
                        "caminho_local": str(caminho_csv_saida),
                        "caminho_relativo": caminho_csv_saida.name,
                        "nome_arquivo": caminho_csv_saida.name,
                        "origem_url": url,
                        "arquivo_origem": destino_download.name,
                        "grupo_mescla_padrao": "",
                        "origem_tipo": "arquivo_tabular_convertido",
                    }
                )

            atualizar_progresso_global(
                fase="ingestão",
                funcao="ingerir_arquivo_unico",
                etapa="Arquivo tabular ingerido com sucesso",
                caminho=str(destino_download.parent),
                arquivo=nome_arquivo,
                icone="✅",
            )
            return

        arquivos_ignorados.append(nome_arquivo)
        atualizar_progresso_global(
            fase="ingestão",
            funcao="ingerir_arquivo_unico",
            etapa="Arquivo ignorado por extensão incompatível",
            caminho=str(destino_download.parent),
            arquivo=nome_arquivo,
            icone="⚠️",
        )

    def ingerir_listagem_remota(arquivos_descobertos: list[ArquivoDescoberto]) -> None:
        for item in arquivos_descobertos:
            url_arquivo: str = item["url"]
            caminho_relativo: str = item["caminho_relativo"]
            nome_arquivo: str = Path(caminho_relativo).name
            extensao: str = Path(nome_arquivo).suffix.lower()

            imprimir_log(
                fase="ingestão",
                funcao="ingerir_listagem_remota",
                etapa="Processando item da estrutura remota",
                caminho=str(Path(caminho_relativo).parent),
                arquivo=nome_arquivo,
                processados=contexto.processados,
                total=contexto.total_planejado,
                icone="📡",
            )

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

            destino_download: Path = contexto.pasta_downloads / Path(caminho_relativo)
            destino_download.parent.mkdir(parents=True, exist_ok=True)

            try:
                baixar_arquivo_remoto_para_disco(url_arquivo, destino_download)

                if extensao == ".zip":
                    grupo_zip: str = obter_grupo_mescla(nome_arquivo)
                    extrair_zip_recursivamente(
                        caminho_zip=destino_download,
                        origem_url=url_arquivo,
                        grupo_mescla_padrao=grupo_zip,
                        prefixo_origem_zip=None,
                        profundidade_atual=0,
                    )
                    atualizar_progresso_global(
                        fase="ingestão",
                        funcao="ingerir_listagem_remota",
                        etapa=f"ZIP baixado e extraído recursivamente (grupo={grupo_zip})",
                        caminho=str(destino_download.parent),
                        arquivo=nome_arquivo,
                        icone="✅",
                    )
                    continue

                if extensao in extensoes_tabulares_suportadas:
                    escrever_manifesto(
                        {
                            "caminho_local": str(destino_download),
                            "caminho_relativo": caminho_relativo,
                            "nome_arquivo": nome_arquivo,
                            "origem_url": url_arquivo,
                            "arquivo_origem": nome_arquivo,
                            "grupo_mescla_padrao": "",
                            "origem_tipo": "arquivo_remoto",
                        }
                    )
                    atualizar_progresso_global(
                        fase="ingestão",
                        funcao="ingerir_listagem_remota",
                        etapa="Arquivo tabular baixado para staging",
                        caminho=str(destino_download.parent),
                        arquivo=nome_arquivo,
                        icone="✅",
                    )
                    continue

                arquivos_ignorados.append(nome_arquivo)
                atualizar_progresso_global(
                    fase="ingestão",
                    funcao="ingerir_listagem_remota",
                    etapa="Arquivo ignorado por extensão incompatível",
                    caminho=str(destino_download.parent),
                    arquivo=nome_arquivo,
                    icone="⚠️",
                )
            except Exception as erro:
                erros.append({"arquivo": nome_arquivo, "erro": str(erro)})
                atualizar_progresso_global(
                    fase="ingestão",
                    funcao="ingerir_listagem_remota",
                    etapa="Erro ao baixar/processar item remoto",
                    caminho=str(destino_download.parent),
                    arquivo=nome_arquivo,
                    icone="❌",
                )

    def ingerir_origem() -> None:
        tipo_origem: str = detectar_tipo_origem(url_origem)
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
            imprimir_log(
                fase="descoberta",
                funcao="listar_arquivos_ftp",
                etapa="Listando estrutura FTP recursivamente",
                caminho=url_origem,
                processados=0,
                total=0,
                icone="🗂️",
            )
            arquivos_descobertos: list[ArquivoDescoberto] = listar_arquivos_ftp(url_origem)
            definir_total_arquivos(len(arquivos_descobertos), url_origem)
            ingerir_listagem_remota(arquivos_descobertos)
            return

        if tipo_origem == "http_pagina":
            imprimir_log(
                fase="descoberta",
                funcao="listar_arquivos_http",
                etapa="Lendo página HTTP/HTTPS e percorrendo subpastas",
                caminho=url_origem,
                processados=0,
                total=0,
                icone="🌍",
            )
            arquivos_descobertos = listar_arquivos_http(url_origem)
            definir_total_arquivos(len(arquivos_descobertos), url_origem)
            ingerir_listagem_remota(arquivos_descobertos)
            return

    def append_df_em_csv(df: pl.DataFrame, caminho_saida: Path) -> None:
        escrever_cabecalho: bool = not caminho_saida.exists()
        escrever_df_csv(df, caminho_saida, append=not escrever_cabecalho)

    def ler_manifesto() -> pl.DataFrame:
        if not contexto.manifesto_arquivos.exists() or contexto.manifesto_arquivos.stat().st_size == 0:
            return pl.DataFrame()
        return pl.read_csv(contexto.manifesto_arquivos, encoding="utf8-lossy")

    def imprimir_chamados_leitura_polars(caminhos_arquivos: dict[str, Path]) -> None:
        manifesto_df: pl.DataFrame = ler_manifesto()

        def montar_comando_leitura(caminho_arquivo: Path, nome_variavel: str) -> Optional[str]:
            extensao: str = caminho_arquivo.suffix.lower()
            caminho_str: str = str(caminho_arquivo)
            if extensao == ".csv":
                return f'{nome_variavel} = pl.read_csv(r"{caminho_str}")'
            if extensao == ".xlsx":
                return f'{nome_variavel} = pl.read_excel(r"{caminho_str}", engine="openpyxl")'
            if extensao == ".xls":
                return f'{nome_variavel} = pl.read_excel(r"{caminho_str}", engine="calamine")'
            if extensao == ".ods":
                return f'{nome_variavel} = pl.read_excel(r"{caminho_str}", engine="calamine")'
            return None

        def obter_caminho_original_para_leitura(registro: dict[str, object]) -> Path:
            origem_tipo: str = str(registro.get("origem_tipo") or "").strip()
            caminho_local: Path = Path(str(registro.get("caminho_local") or "").strip())
            arquivo_origem_registro: str = str(registro.get("arquivo_origem") or "").strip()
            if origem_tipo == "arquivo_tabular_convertido" and arquivo_origem_registro:
                return contexto.pasta_downloads / Path(arquivo_origem_registro).name
            return caminho_local

        if manifesto_df.is_empty() and not caminhos_arquivos:
            return

        print("\n" + "═" * 108)
        print("📌 COMANDOS PRONTOS PARA LEITURA DOS ARQUIVOS NO POLARS")
        print("Cole os comandos abaixo no notebook:\n")
        print("import polars as pl\n")

        if not manifesto_df.is_empty():
            print("# Arquivos originais baixados/gerados")
            caminhos_originais_ja_impressos: set[str] = set()
            for registro in manifesto_df.iter_rows(named=True):
                caminho_original: Path = obter_caminho_original_para_leitura(registro)
                caminho_original_str: str = str(caminho_original).strip()
                if not caminho_original_str or caminho_original_str in caminhos_originais_ja_impressos:
                    continue
                caminhos_originais_ja_impressos.add(caminho_original_str)
                nome_variavel_base: str = normalizar_nome_pasta_usuario(caminho_original.stem).lower()
                nome_variavel: str = f"df_origem_{nome_variavel_base}"
                comando_origem: Optional[str] = montar_comando_leitura(caminho_original, nome_variavel)
                if comando_origem:
                    print(comando_origem)
            if caminhos_arquivos:
                print()

        if caminhos_arquivos:
            print("# Arquivos mesclados por grupo")
            for grupo in sorted(caminhos_arquivos):
                caminho_arquivo: Path = caminhos_arquivos[grupo]
                nome_variavel: str = f"df_mescla_{normalizar_nome_arquivo_saida(grupo).lower()}"
                print(f'{nome_variavel} = pl.read_csv(r"{str(caminho_arquivo)}")')
        print("═" * 108)

    def mesclar_arquivos_ingeridos() -> Optional[pl.DataFrame]:
        pasta_saida = Path(pasta_saida_mesclas) if pasta_saida_mesclas is not None else Path.cwd()
        manifesto_df: pl.DataFrame = ler_manifesto()
        total_registros: int = manifesto_df.height
        definir_total_arquivos(total_registros, "mesclagem incremental")

        if mesclar == True:
        
            if total_registros == 0:
                raise ValueError("Nenhum arquivo compatível foi ingerido para processamento.")

            if total_registros == 1:
                registro_unico = manifesto_df.row(0, named=True)
                caminho_local: Path = Path(str(registro_unico.get("caminho_local") or ""))
                nome_arquivo: str = str(registro_unico.get("nome_arquivo") or "")
                arquivo_origem: str = str(registro_unico.get("arquivo_origem") or "")
                grupo_padrao_bruto: str = str(registro_unico.get("grupo_mescla_padrao") or "").strip()
                grupo_padrao: Optional[str] = grupo_padrao_bruto if grupo_padrao_bruto else None

                imprimir_log(
                    fase="saída",
                    funcao="mesclar_arquivos_ingeridos",
                    etapa="Mesclagem não executada: apenas um arquivo compatível foi ingerido",
                    caminho=str(caminho_local.parent),
                    arquivo=nome_arquivo,
                    processados=0,
                    total=1,
                    icone="ℹ️",
                )

                if arquivo_origem not in arquivos_lidos:
                    arquivos_lidos.append(arquivo_origem)

                imprimir_chamados_leitura_polars({})
                if not retornar_df_unificado:
                    return None

                lotes_unicos: list[pl.DataFrame] = []
                grupo_mescla_unico: str = obter_grupo_mescla(nome_arquivo, grupo_padrao)
                for lote in iterar_lotes_tabulados(caminho_local):
                    lote = lote.with_columns(
                        pl.lit(arquivo_origem).alias("arquivo_origem"),
                        pl.lit(grupo_mescla_unico).alias("grupo_mescla"),
                    )
                    lotes_unicos.append(lote)

                if not lotes_unicos:
                    return None
                return pl.concat(lotes_unicos, how="diagonal_relaxed")

            grupos_gerados: set[str] = set()
            for registro in manifesto_df.iter_rows(named=True):
                caminho_local: Path = Path(str(registro.get("caminho_local") or ""))
                nome_arquivo: str = str(registro.get("nome_arquivo") or "")
                arquivo_origem: str = str(registro.get("arquivo_origem") or "")
                grupo_padrao_bruto: str = str(registro.get("grupo_mescla_padrao") or "").strip()
                grupo_padrao: Optional[str] = grupo_padrao_bruto if grupo_padrao_bruto else None

                imprimir_log(
                    fase="mesclagem",
                    funcao="mesclar_arquivos_ingeridos",
                    etapa="Processando arquivo ingerido",
                    caminho=str(caminho_local.parent),
                    arquivo=nome_arquivo,
                    processados=contexto.processados,
                    total=contexto.total_planejado,
                    icone="🧩",
                )

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

                try:
                    grupo_mescla: str = obter_grupo_mescla(nome_arquivo, grupo_padrao)
                    nome_saida: str = f"mescla_{normalizar_nome_arquivo_saida(grupo_mescla)}.csv"
                    caminho_saida: Path = pasta_saida / nome_saida

                    for lote in iterar_lotes_tabulados(caminho_local):
                        lote = lote.with_columns(
                            pl.lit(arquivo_origem).alias("arquivo_origem"),
                            pl.lit(grupo_mescla).alias("grupo_mescla"),
                        )
                        append_df_em_csv(lote, caminho_saida)

                    grupos_gerados.add(grupo_mescla)
                    arquivos_lidos.append(arquivo_origem)
                    caminhos_mesclas_geradas[grupo_mescla] = caminho_saida
                    atualizar_progresso_global(
                        fase="mesclagem",
                        funcao="mesclar_arquivos_ingeridos",
                        etapa=f"Mesclagem concluída para o grupo {grupo_mescla}",
                        caminho=str(caminho_saida.parent),
                        arquivo=nome_saida,
                        icone="✅",
                    )
                except Exception as erro:
                    erros.append(
                        {
                            "arquivo": arquivo_origem,
                            "erro": f"Falha na mesclagem incremental: {erro}",
                        }
                    )
                    atualizar_progresso_global(
                        fase="mesclagem",
                        funcao="mesclar_arquivos_ingeridos",
                        etapa="Erro ao mesclar arquivo",
                        caminho=str(caminho_local.parent),
                        arquivo=nome_arquivo,
                        icone="❌",
                    )

            if not arquivos_lidos:
                raise ValueError("Nenhum arquivo compatível foi encontrado ou lido com sucesso.")

            imprimir_log(
                fase="saída",
                funcao="mesclar_arquivos_ingeridos",
                etapa="Arquivos CSV por grupo gerados com sucesso",
                caminho=str(pasta_saida),
                arquivo=f"{len(grupos_gerados)} grupo(s)",
                processados=len(grupos_gerados),
                total=len(grupos_gerados),
                icone="💾",
            )

            imprimir_chamados_leitura_polars(caminhos_mesclas_geradas)
            if not retornar_df_unificado:
                return None

            imprimir_log(
                fase="saída",
                funcao="mesclar_arquivos_ingeridos",
                etapa="Montando df_unificado a partir dos CSVs por grupo",
                caminho=str(pasta_saida),
                arquivo="df_unificado",
                processados=0,
                total=len(grupos_gerados),
                icone="📚",
            )

            dfs_finais: list[pl.DataFrame] = []
            grupos_processados: int = 0
            for grupo in sorted(grupos_gerados):
                caminho_grupo: Path = pasta_saida / f"mescla_{normalizar_nome_arquivo_saida(grupo)}.csv"
                if not caminho_grupo.exists():
                    continue

                df_grupo: pl.DataFrame = pl.read_csv(caminho_grupo)
                dfs_finais.append(df_grupo)
                grupos_processados += 1
                imprimir_log(
                    fase="saída",
                    funcao="mesclar_arquivos_ingeridos",
                    etapa=f"Lendo CSV consolidado do grupo {grupo}",
                    caminho=str(caminho_grupo.parent),
                    arquivo=caminho_grupo.name,
                    processados=grupos_processados,
                    total=len(grupos_gerados),
                    icone="📥",
                )

            if not dfs_finais:
                return None
            return pl.concat(dfs_finais, how="diagonal_relaxed")

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
    df_unificado: Optional[pl.DataFrame] = mesclar_arquivos_ingeridos()

    conteudo_zip: Optional[bytes] = None
    if reter_conteudo_zip_em_memoria:
        tipo_origem: str = detectar_tipo_origem(url_origem)
        if tipo_origem == "arquivo_zip":
            nome_zip: str = Path(urlparse(url_origem).path).name
            caminho_zip: Path = contexto.pasta_downloads / nome_zip
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

def baixar_arquivo(url: str, nome_arquivo: Optional[str] = None) -> Path:
    """
    Baixa um arquivo a partir de uma URL e o salva na pasta atual de execução.

    Se `nome_arquivo` não for informado, o nome do arquivo será extraído
    automaticamente a partir do final da URL.

    Parâmetros
    ----------
    url : str
        URL do arquivo que será baixado.
    nome_arquivo : str | None, opcional
        Nome que será usado para salvar o arquivo localmente.
        Se não for informado, o nome será obtido a partir da URL.

    Retorno
    -------
    Path
        Caminho completo do arquivo salvo.

    Exceções
    --------
    requests.HTTPError
        Lançada se a requisição HTTP retornar erro.
    ValueError
        Lançada se não for possível determinar o nome do arquivo.
    """
    if nome_arquivo is None:
        nome_arquivo = url.rstrip("/").split("/")[-1]

    if not nome_arquivo:
        raise ValueError("Não foi possível determinar o nome do arquivo a partir da URL.")

    caminho_saida = Path.cwd() / nome_arquivo

    resposta = requests.get(url)
    resposta.raise_for_status()

    with open(caminho_saida, "wb") as arquivo:
        arquivo.write(resposta.content)

    return caminho_saida

def mesclar_arquivos_do_manifesto(
    manifesto_arquivos: Path | str,
    *,
    pasta_saida_mesclas: Optional[Path | str] = None,
    chunk_size_csv: int = 100_000,
    retornar_df_unificado: bool = False,
    verbose: bool = True,
) -> ResultadoMesclagem:
    """
    Executa somente a etapa de mesclagem a partir de um manifesto já existente.

    Parâmetros
    ----------
    manifesto_arquivos : Path | str
        Caminho do `manifesto_arquivos.csv` gerado na ingestão anterior.
    pasta_saida_mesclas : Optional[Path | str], default None
        Pasta onde os arquivos `mescla_<GRUPO>.csv` serão salvos.
        Se None, usa a mesma pasta do manifesto.
    chunk_size_csv : int, default 100_000
        Quantidade de linhas por lote para leitura de CSVs.
    retornar_df_unificado : bool, default False
        Se True, retorna um `pl.DataFrame` unificado ao final.
    verbose : bool, default True
        Se True, imprime logs simples no console.

    Retorno
    -------
    ResultadoMesclagem
        Dicionário com arquivos lidos, ignorados, erros, caminhos das mesclas
        e `df_unificado` (quando solicitado).
    """

    manifesto_path: Path = Path(manifesto_arquivos)
    if not manifesto_path.exists():
        raise FileNotFoundError(f"Manifesto não encontrado: {manifesto_path}")

    pasta_saida: Path = (
        Path(pasta_saida_mesclas)
        if pasta_saida_mesclas is not None
        else manifesto_path.parent
    )
    pasta_saida.mkdir(parents=True, exist_ok=True)

    arquivos_lidos: list[str] = []
    arquivos_ignorados: list[str] = []
    erros: list[ErroLeitura] = []
    caminhos_mesclas: dict[str, str] = {}

    def log(msg: str) -> None:
        if verbose:
            print(msg)

    def nome_deve_ser_ignorado(nome_arquivo: str) -> bool:
        nome_base: str = Path(nome_arquivo).stem
        nome_normalizado: str = unicodedata.normalize("NFKD", nome_base)
        nome_sem_acento: str = "".join(
            caractere
            for caractere in nome_normalizado
            if not unicodedata.combining(caractere)
        ).lower()
        return "dicionario" in nome_sem_acento

    def obter_grupo_mescla(
        nome_arquivo: str,
        grupo_padrao: Optional[str] = None,
    ) -> str:
        if grupo_padrao:
            return grupo_padrao

        nome_base: str = Path(nome_arquivo).stem
        nome_normalizado: str = unicodedata.normalize("NFKD", nome_base)
        nome_sem_acento: str = "".join(
            caractere
            for caractere in nome_normalizado
            if not unicodedata.combining(caractere)
        )

        nome_sem_acento = nome_sem_acento.replace("-", "_")
        partes_nome: list[str] = [parte for parte in nome_sem_acento.split("_") if parte]

        if not partes_nome:
            return "SEM_GRUPO"

        # PATCH MÍNIMO:
        # Caso especial para nomes como:
        # pda-043-rpc-201505
        # pda-043-rpc-201506
        # Ambos devem cair no mesmo grupo: 043_RPC_2015
        if (
            len(partes_nome) >= 4
            and partes_nome[0].lower() == "pda"
            and partes_nome[-1].isdigit()
            and len(partes_nome[-1]) == 6
        ):
            ano: str = partes_nome[-1][:4]
            codigo: str = partes_nome[1].upper()
            tipo: str = partes_nome[2].upper()
            return f"{codigo}_{tipo}_{ano}"

        return partes_nome[-1].upper()

    def normalizar_nome_arquivo_saida(grupo_mescla: str) -> str:
        grupo_normalizado: str = unicodedata.normalize("NFKD", grupo_mescla)
        grupo_sem_acento: str = "".join(
            caractere
            for caractere in grupo_normalizado
            if not unicodedata.combining(caractere)
        ).upper()

        caracteres_processados: list[str] = []
        for caractere in grupo_sem_acento:
            caracteres_processados.append(
                caractere if caractere.isalnum() else "_"
            )

        nome_normalizado: str = "".join(caracteres_processados).strip("_")
        while "__" in nome_normalizado:
            nome_normalizado = nome_normalizado.replace("__", "_")

        return nome_normalizado or "SEM_GRUPO"

    def inferir_encoding_e_separador_csv(caminho_csv: Path) -> tuple[str, str]:
        encodings_csv_teste: tuple[str, ...] = ("utf-8", "utf-8-sig", "latin1")
        amostra_bytes: bytes = caminho_csv.read_bytes()[:100_000]

        for encoding in encodings_csv_teste:
            try:
                texto: str = amostra_bytes.decode(encoding)
                try:
                    dialeto = csv.Sniffer().sniff(
                        texto,
                        delimiters=[",", ";", "\t", "|"],
                    )
                    separador: str = dialeto.delimiter
                except Exception:
                    separador = ";"
                return encoding, separador
            except Exception:
                continue

        raise ValueError(f"Falha ao inferir encoding do CSV: {caminho_csv}")

    def iterar_lotes_csv_latin1(
        caminho_arquivo: Path,
        separador: str,
        encoding: str,
    ) -> Iterator[pl.DataFrame]:
        colunas: list[str] | None = None
        linhas_lote: list[dict[str, str]] = []

        with caminho_arquivo.open("r", encoding=encoding, newline="") as arquivo_csv:
            leitor = csv.DictReader(arquivo_csv, delimiter=separador)
            for linha in leitor:
                if colunas is None:
                    colunas = list(leitor.fieldnames or linha.keys())

                linhas_lote.append(
                    {coluna: linha.get(coluna, "") for coluna in colunas}
                )

                if len(linhas_lote) >= chunk_size_csv:
                    yield pl.DataFrame(linhas_lote)
                    linhas_lote = []

        if linhas_lote:
            yield pl.DataFrame(linhas_lote)

    def iterar_lotes_tabulados(caminho_arquivo: Path) -> Iterator[pl.DataFrame]:
        extensao: str = caminho_arquivo.suffix.lower()

        if extensao == ".csv":
            encoding, separador = inferir_encoding_e_separador_csv(caminho_arquivo)

            if encoding == "latin1":
                yield from iterar_lotes_csv_latin1(
                    caminho_arquivo,
                    separador,
                    encoding,
                )
                return

            leitor = pl.scan_csv(
                caminho_arquivo,
                separator=separador,
                encoding="utf8-lossy" if encoding == "utf-8-sig" else "utf8",
                low_memory=True,
                ignore_errors=True,
            )

            for lote in leitor.collect_batches(chunk_size=chunk_size_csv):
                yield lote
            return

        if extensao == ".xlsx":
            yield pl.read_excel(caminho_arquivo, engine="openpyxl")
            return

        if extensao == ".xls":
            yield pl.read_excel(caminho_arquivo, engine="calamine")
            return

        if extensao == ".ods":
            yield pl.read_excel(caminho_arquivo, engine="calamine")
            return

        raise ValueError(f"Extensão não suportada: {caminho_arquivo.suffix}")

    def escrever_df_csv(df: pl.DataFrame, caminho_saida: Path, *, append: bool) -> None:
        caminho_saida.parent.mkdir(parents=True, exist_ok=True)
        modo: str = "a" if append else "w"

        with caminho_saida.open(modo, encoding="utf-8", newline="") as arquivo_saida:
            df.write_csv(arquivo_saida, include_header=not append)

    def append_df_em_csv(df: pl.DataFrame, caminho_saida: Path) -> None:
        escrever_cabecalho: bool = not caminho_saida.exists()
        escrever_df_csv(df, caminho_saida, append=not escrever_cabecalho)

    # Lê o manifesto com csv puro para evitar surpresas com BOM/encoding
    with manifesto_path.open("r", encoding="utf-8-sig", newline="") as arquivo_manifesto:
        registros_manifesto: list[dict[str, str]] = list(csv.DictReader(arquivo_manifesto))

    total_registros: int = len(registros_manifesto)
    if total_registros == 0:
        raise ValueError("Nenhum arquivo compatível foi encontrado no manifesto.")

    log(f"📘 Registros no manifesto: {total_registros}")

    # Se houver somente 1 arquivo, mantém a mesma lógica da função original:
    # não gera mescla_<GRUPO>.csv
    if total_registros == 1:
        registro_unico = registros_manifesto[0]
        caminho_local: Path = Path(registro_unico["caminho_local"])
        nome_arquivo: str = registro_unico["nome_arquivo"]
        arquivo_origem: str = registro_unico["arquivo_origem"]
        grupo_padrao_bruto: str = (registro_unico.get("grupo_mescla_padrao") or "").strip()
        grupo_padrao: Optional[str] = grupo_padrao_bruto if grupo_padrao_bruto else None

        if not caminho_local.exists():
            raise FileNotFoundError(f"Arquivo do manifesto não encontrado: {caminho_local}")

        log("ℹ️ Apenas 1 arquivo compatível no manifesto; mesclagem não será gerada.")

        if arquivo_origem not in arquivos_lidos:
            arquivos_lidos.append(arquivo_origem)

        df_unificado: Optional[pl.DataFrame] = None
        if retornar_df_unificado:
            lotes_unicos: list[pl.DataFrame] = []
            grupo_mescla_unico: str = obter_grupo_mescla(nome_arquivo, grupo_padrao)

            for lote in iterar_lotes_tabulados(caminho_local):
                lote = lote.with_columns(
                    pl.lit(arquivo_origem).alias("arquivo_origem"),
                    pl.lit(grupo_mescla_unico).alias("grupo_mescla"),
                )
                lotes_unicos.append(lote)

            if lotes_unicos:
                df_unificado = pl.concat(lotes_unicos, how="diagonal_relaxed")

        return {
            "manifesto_arquivos": str(manifesto_path),
            "arquivos_lidos": arquivos_lidos,
            "arquivos_ignorados": arquivos_ignorados,
            "erros": erros,
            "caminhos_mesclas": caminhos_mesclas,
            "df_unificado": df_unificado,
        }

    grupos_gerados: set[str] = set()

    for i, registro in enumerate(registros_manifesto, start=1):
        caminho_local: Path = Path(registro["caminho_local"])
        nome_arquivo: str = registro["nome_arquivo"]
        arquivo_origem: str = registro["arquivo_origem"]
        grupo_padrao_bruto: str = (registro.get("grupo_mescla_padrao") or "").strip()
        grupo_padrao: Optional[str] = grupo_padrao_bruto if grupo_padrao_bruto else None

        log(f"🧩 [{i}/{total_registros}] Processando: {nome_arquivo}")

        if nome_deve_ser_ignorado(nome_arquivo):
            arquivos_ignorados.append(nome_arquivo)
            log(f"🚫 Ignorado por regra de nome: {nome_arquivo}")
            continue

        if not caminho_local.exists():
            erros.append(
                {
                    "arquivo": arquivo_origem,
                    "erro": f"Arquivo do manifesto não encontrado: {caminho_local}",
                }
            )
            log(f"❌ Arquivo não encontrado: {caminho_local}")
            continue

        try:
            grupo_mescla: str = obter_grupo_mescla(nome_arquivo, grupo_padrao)
            nome_saida: str = f"mescla_{normalizar_nome_arquivo_saida(grupo_mescla)}.csv"
            caminho_saida: Path = pasta_saida / nome_saida

            for lote in iterar_lotes_tabulados(caminho_local):
                lote = lote.with_columns(
                    pl.lit(arquivo_origem).alias("arquivo_origem"),
                    pl.lit(grupo_mescla).alias("grupo_mescla"),
                )
                append_df_em_csv(lote, caminho_saida)

            grupos_gerados.add(grupo_mescla)
            arquivos_lidos.append(arquivo_origem)
            caminhos_mesclas[grupo_mescla] = str(caminho_saida)
            log(f"✅ Mesclado grupo {grupo_mescla} -> {caminho_saida.name}")

        except Exception as erro:
            erros.append(
                {
                    "arquivo": arquivo_origem,
                    "erro": f"Falha na mesclagem incremental: {erro}",
                }
            )
            log(f"❌ Erro ao mesclar {nome_arquivo}: {erro}")

    if not arquivos_lidos:
        raise ValueError("Nenhum arquivo compatível foi encontrado ou lido com sucesso.")

    df_unificado: Optional[pl.DataFrame] = None
    if retornar_df_unificado and caminhos_mesclas:
        dfs_finais: list[pl.DataFrame] = []
        for grupo in sorted(caminhos_mesclas):
            caminho_grupo = Path(caminhos_mesclas[grupo])
            if caminho_grupo.exists():
                dfs_finais.append(pl.read_csv(caminho_grupo))

        if dfs_finais:
            df_unificado = pl.concat(dfs_finais, how="diagonal_relaxed")

    return {
        "manifesto_arquivos": str(manifesto_path),
        "arquivos_lidos": arquivos_lidos,
        "arquivos_ignorados": arquivos_ignorados,
        "erros": erros,
        "caminhos_mesclas": caminhos_mesclas,
        "df_unificado": df_unificado,
    }