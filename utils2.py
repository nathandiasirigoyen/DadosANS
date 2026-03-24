from __future__ import annotations

import csv
import ftplib
import shutil
import unicodedata
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path, PurePosixPath
from typing import Iterator, Optional, TypedDict
from urllib.parse import unquote, urljoin, urlparse
from zipfile import ZipFile

import pandas as pd
import requests
from bs4 import BeautifulSoup


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
    df_unificado: Optional[pd.DataFrame]


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
    - Se for arquivo `.zip`, cria uma pasta com timestamp, baixa o ZIP para
      esta pasta, extrai os arquivos compatíveis nela e trata recursivamente
      ZIPs internos.
    - Se for arquivo tabular compatível, baixa o arquivo e o registra para
      mesclagem. Se necessário, pode convertê-lo para CSV.
    - Se for pasta FTP, lista recursivamente os arquivos da estrutura e baixa
      os arquivos compatíveis mantendo a mesma estrutura do diretório remoto.
    - Se for página HTTP/HTTPS, faz o parsing da listagem, identifica links
      para arquivos compatíveis e, se necessário, percorre subpastas
      automaticamente, baixando os arquivos compatíveis e mantendo a estrutura.
    - Se qualquer uma das opções anteriores contiver arquivos `.zip`, a função
      baixa, extrai e processa recursivamente esses arquivos `.zip`.

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
    - `AC_202401_HOSP_DET.zip`  -> grupo `DET`
    - `AC_202401_HOSP_REM.zip`  -> grupo `REM`

    Arquivos do mesmo grupo são concatenados entre si. Ao final, a função:
    - salva um arquivo `.csv` por grupo na pasta atual de execução
      (`Path.cwd()` ou `pasta_saida_mesclas`, se informada), com nomes
      no padrão `mescla_<GRUPO>.csv`;
    - opcionalmente retorna um `DataFrame` unificado contendo todos os grupos;
    - adiciona as colunas `arquivo_origem` e `grupo_mescla`.

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
    base_dir_execucao : Optional[Path | str], default None
        Diretório base onde será criada a pasta timestamp da execução.
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
        - `pasta_execucao`: pasta timestamp da execução
        - `pasta_ingestao`: pasta de staging/ingestão
        - `manifesto_arquivos`: caminho do manifesto CSV
        - `arquivos_lidos`: lista de arquivos lidos com sucesso
        - `arquivos_ignorados`: lista de arquivos ignorados
        - `erros`: lista de erros ocorridos
        - `df_unificado`: DataFrame unificado, quando solicitado

    Efeitos colaterais
    ------------------
    Para cada grupo de mesclagem encontrado, a função salva um arquivo `.csv`
    na pasta de saída, no formato:
    - `mescla_CONS.csv`
    - `mescla_DET.csv`
    - `mescla_REM.csv`
    - etc.

    Se ocorrer erro ao salvar algum arquivo de saída, o erro será registrado na
    lista `erros`, sem interromper o processamento dos demais grupos.

    Raises
    ------
    requests.HTTPError
        Se ocorrer erro ao baixar um arquivo via HTTP/HTTPS.
    ValueError
        Se a URL não for suportada ou se nenhum arquivo compatível for lido.
    """
    extensoes_tabulares_suportadas: set[str] = {".csv", ".xlsx", ".xls", ".ods"}
    extensoes_processaveis: set[str] = extensoes_tabulares_suportadas | {".zip"}
    encodings_csv_teste: tuple[str, ...] = ("utf-8", "utf-8-sig", "latin1")
    chunk_size_download: int = 1024 * 1024

    arquivos_lidos: list[str] = []
    arquivos_ignorados: list[str] = []
    erros: list[ErroLeitura] = []

    def criar_barra_progresso(processados: int, total: int, largura: int = 26) -> str:
        """
        Cria uma barra visual de progresso.

        Parameters
        ----------
        processados : int
            Quantidade já processada.
        total : int
            Quantidade total planejada.
        largura : int, default 26
            Largura da barra em caracteres.

        Returns
        -------
        str
            Barra de progresso formatada.
        """
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
        """
        Imprime um log visual padronizado.

        Parameters
        ----------
        fase : str
            Nome da fase macro da execução.
        funcao : str
            Nome da subfunção em execução.
        etapa : str
            Texto curto descritivo da etapa atual.
        caminho : Optional[str], default None
            Diretório, pasta, subpasta ou URL atual.
        arquivo : Optional[str], default None
            Arquivo atual.
        processados : Optional[int], default None
            Quantidade processada.
        total : Optional[int], default None
            Quantidade total planejada.
        icone : str, default "ℹ️"
            Ícone visual do log.
        """
        print("\n" + "═" * 108)
        print(f"{icone} FASE   : {fase}")
        print(f"🔧 FUNÇÃO : {funcao}")
        print(f"🧭 ETAPA  : {etapa}")

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

    def criar_pasta_timestamp(base_dir: Optional[Path | str] = None) -> ContextoExecucao:
        """
        Cria a estrutura de pastas da execução com timestamp.

        Parameters
        ----------
        base_dir : Optional[Path | str], default None
            Diretório base onde a pasta da execução será criada.
            Se None, usa `Path.cwd()`.

        Returns
        -------
        ContextoExecucao
            Contexto contendo os caminhos de trabalho.
        """
        base: Path = Path(base_dir) if base_dir is not None else Path.cwd()
        timestamp: str = datetime.now().strftime("%Y%m%d_%H%M%S")
        pasta_execucao: Path = base / f"processamento_{timestamp}"
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
        """
        Define o total planejado para o progresso global.

        Parameters
        ----------
        total : int
            Quantidade total planejada.
        descricao : str
            Contexto textual da contagem.
        """
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
        """
        Atualiza o progresso global da execução.

        Parameters
        ----------
        fase : str
            Fase macro atual.
        funcao : str
            Nome da subfunção.
        etapa : str
            Texto da etapa concluída.
        caminho : Optional[str], default None
            Diretório, pasta ou URL.
        arquivo : Optional[str], default None
            Arquivo atual.
        icone : str, default "✅"
            Ícone visual.
        """
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
        """
        Verifica se o nome do arquivo deve ser ignorado com base na presença
        da palavra 'dicionario', desconsiderando acentuação e caixa.

        Parameters
        ----------
        nome_arquivo : str
            Nome do arquivo ou caminho completo.

        Returns
        -------
        bool
            True se o arquivo deve ser ignorado; caso contrário, False.
        """
        nome_base: str = Path(nome_arquivo).stem
        nome_normalizado: str = unicodedata.normalize("NFKD", nome_base)
        nome_sem_acento: str = "".join(
            caractere
            for caractere in nome_normalizado
            if not unicodedata.combining(caractere)
        ).lower()
        return "dicionario" in nome_sem_acento

    def obter_grupo_mescla(nome_arquivo: str, grupo_padrao: Optional[str] = None) -> str:
        """
        Identifica o grupo de mesclagem de um arquivo com base no último bloco
        do nome, separado por underscore.

        Examples
        --------
        - AC_202401_HOSP_CONS.zip -> CONS
        - AC_202401_HOSP_DET.csv -> DET
        - AC_202401_HOSP_REM.xls -> REM

        Parameters
        ----------
        nome_arquivo : str
            Nome do arquivo ou caminho.
        grupo_padrao : Optional[str], default None
            Grupo a ser usado diretamente, quando necessário.

        Returns
        -------
        str
            Grupo de mesclagem identificado.
        """
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

        return partes_nome[-1].upper()

    def normalizar_nome_arquivo_saida(grupo_mescla: str) -> str:
        """
        Normaliza o nome do grupo para uso seguro como nome de arquivo.

        Parameters
        ----------
        grupo_mescla : str
            Nome do grupo de mesclagem.

        Returns
        -------
        str
            Nome normalizado para uso em arquivo.
        """
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
        """
        Detecta o tipo da origem informada.

        Parameters
        ----------
        url : str
            URL de origem.

        Returns
        -------
        str
            Tipo detectado.

        Raises
        ------
        ValueError
            Se o esquema ou tipo não for suportado.
        """
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
        """
        Adiciona uma linha ao manifesto de ingestão.

        Parameters
        ----------
        registro : RegistroManifesto
            Registro a ser persistido.
        """
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
        """
        Baixa um arquivo HTTP/HTTPS para disco, em streaming.

        Parameters
        ----------
        url : str
            URL do arquivo.
        destino : Path
            Caminho local de saída.

        Raises
        ------
        requests.HTTPError
            Se ocorrer erro HTTP.
        """
        destino.parent.mkdir(parents=True, exist_ok=True)

        with requests.get(url, stream=True, timeout=300) as resposta:
            resposta.raise_for_status()
            with destino.open("wb") as arquivo_saida:
                for bloco in resposta.iter_content(chunk_size=chunk_size_download):
                    if bloco:
                        arquivo_saida.write(bloco)

    def baixar_ftp_para_arquivo(url: str, destino: Path) -> None:
        """
        Baixa um arquivo FTP para disco.

        Parameters
        ----------
        url : str
            URL FTP do arquivo.
        destino : Path
            Caminho local de saída.

        Raises
        ------
        ValueError
            Se a URL FTP não for válida.
        """
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
        """
        Baixa um arquivo remoto para disco, suportando HTTP/HTTPS/FTP.

        Parameters
        ----------
        url : str
            URL do arquivo remoto.
        destino : Path
            Caminho local de saída.

        Raises
        ------
        ValueError
            Se o esquema não for suportado.
        """
        esquema: str = urlparse(url).scheme.lower()

        if esquema in {"http", "https"}:
            baixar_http_para_arquivo(url, destino)
            return

        if esquema == "ftp":
            baixar_ftp_para_arquivo(url, destino)
            return

        raise ValueError(f"Esquema de URL não suportado: {esquema}")

    def listar_arquivos_ftp(url_ftp: str) -> list[ArquivoDescoberto]:
        """
        Lista recursivamente arquivos em uma pasta FTP, retornando apenas
        arquivos com extensões processáveis.

        Parameters
        ----------
        url_ftp : str
            URL FTP da pasta.

        Returns
        -------
        list[ArquivoDescoberto]
            Arquivos descobertos, com URL e caminho relativo.
        """
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
            """
            Lista recursivamente arquivos em um diretório FTP já conectado.

            Parameters
            ----------
            ftp : ftplib.FTP
                Conexão FTP já autenticada.
            caminho_atual : str
                Caminho atual a ser percorrido.

            Returns
            -------
            list[str]
                Lista de caminhos remotos encontrados.
            """
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
            resultados.append(
                {
                    "url": montar_url_ftp(caminho_remoto),
                    "caminho_relativo": rel,
                }
            )

        return resultados

    def listar_arquivos_http(
        url_pasta: str,
        urls_visitadas: Optional[set[str]] = None
    ) -> list[ArquivoDescoberto]:
        """
        Lista recursivamente arquivos em uma página HTTP/HTTPS, seguindo subpastas.

        Parameters
        ----------
        url_pasta : str
            URL da página/listagem.
        urls_visitadas : Optional[set[str]], default None
            Controle interno de recursão.

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
        caminho_base = PurePosixPath(url_base_parseada.path.rstrip("/"))

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

            caminho_absoluto = PurePosixPath(url_absoluta_parseada.path)
            nome_arquivo: str = caminho_absoluto.name

            if not nome_arquivo:
                continue

            eh_subpasta: bool = href.endswith("/") or str(caminho_absoluto).endswith("/")

            if eh_subpasta:
                arquivos_encontrados.extend(listar_arquivos_http(url_absoluta, urls_visitadas))
                continue

            extensao: str = Path(nome_arquivo).suffix.lower()
            if extensao not in extensoes_processaveis:
                continue

            try:
                caminho_relativo: str = str(caminho_absoluto.relative_to(caminho_base))
            except Exception:
                caminho_relativo = nome_arquivo

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
        """
        Infere encoding e separador de um CSV.

        Regras:
        - tenta encodings: utf-8, utf-8-sig, latin1
        - tenta inferência com csv.Sniffer
        - fallback do separador: ';'

        Parameters
        ----------
        caminho_csv : Path
            Caminho do arquivo CSV.

        Returns
        -------
        tuple[str, str]
            Tupla `(encoding, separador)`.

        Raises
        ------
        ValueError
            Se nenhum encoding funcionar.
        """
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

    def iterar_lotes_tabulados(caminho_arquivo: Path) -> Iterator[pd.DataFrame]:
        """
        Itera lotes de um arquivo tabular.

        Estratégia:
        - CSV: leitura em lotes (`chunksize`)
        - XLSX/XLS/ODS: leitura integral do arquivo

        Parameters
        ----------
        caminho_arquivo : Path
            Caminho do arquivo tabular.

        Yields
        ------
        Iterator[pd.DataFrame]
            Lotes do arquivo.
        """
        extensao: str = caminho_arquivo.suffix.lower()

        if extensao == ".csv":
            encoding, separador = inferir_encoding_e_separador_csv(caminho_arquivo)

            leitor = pd.read_csv(
                caminho_arquivo,
                sep=separador,
                encoding=encoding,
                low_memory=False,
                chunksize=chunk_size_csv,
            )

            for lote in leitor:
                yield lote

            return

        if extensao == ".xlsx":
            yield pd.read_excel(caminho_arquivo, engine="openpyxl")
            return

        if extensao == ".xls":
            yield pd.read_excel(caminho_arquivo, engine="xlrd")
            return

        if extensao == ".ods":
            yield pd.read_excel(caminho_arquivo, engine="odf")
            return

        raise ValueError(f"Extensão não suportada: {caminho_arquivo.suffix}")

    def converter_arquivo_tabular_para_csv(caminho_entrada: Path, caminho_saida_csv: Path) -> None:
        """
        Converte um arquivo tabular suportado para CSV.

        Parameters
        ----------
        caminho_entrada : Path
            Caminho do arquivo de entrada.
        caminho_saida_csv : Path
            Caminho do CSV de saída.
        """
        caminho_saida_csv.parent.mkdir(parents=True, exist_ok=True)

        primeiro_lote: bool = True
        for lote in iterar_lotes_tabulados(caminho_entrada):
            lote.to_csv(
                caminho_saida_csv,
                mode="w" if primeiro_lote else "a",
                index=False,
                encoding="utf-8-sig",
                header=primeiro_lote,
            )
            primeiro_lote = False

    def extrair_zip_recursivamente(
        *,
        caminho_zip: Path,
        origem_url: str,
        grupo_mescla_padrao: Optional[str],
        prefixo_origem_zip: Optional[str] = None,
        profundidade_atual: int = 0,
    ) -> None:
        """
        Extrai um arquivo ZIP para disco e processa recursivamente ZIPs internos.

        Regras:
        - arquivos tabulares compatíveis são extraídos e registrados no manifesto;
        - arquivos ZIP internos são extraídos para disco e processados recursivamente;
        - arquivos com nome contendo `dicionario` são ignorados;
        - a cadeia de origem é preservada em `arquivo_origem`, por exemplo:
          `externo.zip::interno.zip::arquivo.csv`.

        Parameters
        ----------
        caminho_zip : Path
            Caminho local do arquivo ZIP.
        origem_url : str
            URL original associada ao ZIP raiz.
        grupo_mescla_padrao : Optional[str]
            Grupo herdado do nome do ZIP externo.
        prefixo_origem_zip : Optional[str], default None
            Prefixo da cadeia de origem dos ZIPs, usado na recursão.
        profundidade_atual : int, default 0
            Profundidade atual da recursão.

        Raises
        ------
        ValueError
            Se a profundidade máxima de recursão for excedida.
        """
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
        """
        Ingesta uma URL que aponta diretamente para um arquivo único.

        Regras:
        - se ZIP: baixa para disco e extrai compatíveis para disco,
          incluindo ZIPs internos recursivamente;
        - se tabular: baixa para disco e o registra para mesclagem.

        Parameters
        ----------
        url : str
            URL do arquivo.
        """
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
        """
        Ingesta uma lista de arquivos remotos já descoberta previamente.

        Parameters
        ----------
        arquivos_descobertos : list[ArquivoDescoberto]
            Arquivos remotos encontrados.
        """
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
                erros.append(
                    {
                        "arquivo": nome_arquivo,
                        "erro": str(erro),
                    }
                )
                atualizar_progresso_global(
                    fase="ingestão",
                    funcao="ingerir_listagem_remota",
                    etapa="Erro ao baixar/processar item remoto",
                    caminho=str(destino_download.parent),
                    arquivo=nome_arquivo,
                    icone="❌",
                )

    def ingerir_origem() -> None:
        """
        Executa a fase de ingestão para a URL informada.
        """
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

    def append_df_em_csv(df: pd.DataFrame, caminho_saida: Path) -> None:
        """
        Apende um DataFrame em um CSV de saída.

        Parameters
        ----------
        df : pd.DataFrame
            DataFrame a ser persistido.
        caminho_saida : Path
            CSV de saída.
        """
        caminho_saida.parent.mkdir(parents=True, exist_ok=True)
        escrever_cabecalho: bool = not caminho_saida.exists()

        df.to_csv(
            caminho_saida,
            mode="w" if escrever_cabecalho else "a",
            index=False,
            encoding="utf-8-sig",
            header=escrever_cabecalho,
        )

    def mesclar_arquivos_ingeridos() -> Optional[pd.DataFrame]:
        """
        Executa a mesclagem incremental a partir do manifesto de ingestão.

        Estratégia:
        - lê arquivo por arquivo;
        - identifica o grupo automaticamente;
        - grava diretamente no `mescla_<GRUPO>.csv`;
        - evita manter todos os DataFrames em memória;
        - opcionalmente reconstrói `df_unificado` no final.

        Returns
        -------
        Optional[pd.DataFrame]
            DataFrame unificado, se solicitado; caso contrário, None.

        Raises
        ------
        ValueError
            Se nenhum arquivo compatível tiver sido lido.
        """
        pasta_saida = Path(pasta_saida_mesclas) if pasta_saida_mesclas is not None else Path.cwd()

        manifesto_df: pd.DataFrame = pd.read_csv(contexto.manifesto_arquivos, encoding="utf-8-sig")
        total_registros: int = len(manifesto_df)

        definir_total_arquivos(total_registros, "mesclagem incremental")

        if total_registros == 0:
            raise ValueError("Nenhum arquivo compatível foi ingerido para mesclagem.")

        grupos_gerados: set[str] = set()

        for _, registro in manifesto_df.iterrows():
            caminho_local: Path = Path(str(registro["caminho_local"]))
            nome_arquivo: str = str(registro["nome_arquivo"])
            arquivo_origem: str = str(registro["arquivo_origem"])
            grupo_padrao: str = str(registro["grupo_mescla_padrao"]).strip()

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
                grupo_mescla: str = obter_grupo_mescla(
                    nome_arquivo,
                    grupo_padrao if grupo_padrao else None
                )
                nome_saida: str = f"mescla_{normalizar_nome_arquivo_saida(grupo_mescla)}.csv"
                caminho_saida: Path = pasta_saida / nome_saida

                for lote in iterar_lotes_tabulados(caminho_local):
                    lote["arquivo_origem"] = arquivo_origem
                    lote["grupo_mescla"] = grupo_mescla
                    append_df_em_csv(lote, caminho_saida)

                grupos_gerados.add(grupo_mescla)
                arquivos_lidos.append(arquivo_origem)

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

        dfs_finais: list[pd.DataFrame] = []
        grupos_processados: int = 0

        for grupo in sorted(grupos_gerados):
            caminho_grupo: Path = pasta_saida / f"mescla_{normalizar_nome_arquivo_saida(grupo)}.csv"
            if not caminho_grupo.exists():
                continue

            df_grupo: pd.DataFrame = pd.read_csv(
                caminho_grupo,
                encoding="utf-8-sig",
                low_memory=False
            )
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

        return pd.concat(dfs_finais, ignore_index=True)

    imprimir_log(
        fase="inicialização",
        funcao="baixar_extrair_mesclar_url_v2",
        etapa="Início do processamento",
        caminho=url_origem,
        arquivo=Path(urlparse(url_origem).path).name or url_origem,
        processados=0,
        total=0,
        icone="🚀",
    )

    ingerir_origem()
    df_unificado: Optional[pd.DataFrame] = mesclar_arquivos_ingeridos()

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
        funcao="baixar_extrair_mesclar_url_v2",
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