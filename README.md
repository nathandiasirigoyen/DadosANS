# Projeto Comunitário de Análise de Dados do Saúde Caixa

## Visão geral

Este repositório reúne um projeto comunitário de análise de dados com foco no **Saúde Caixa**, plano de saúde de autogestão dos empregados da Caixa Econômica Federal, utilizando **bases públicas disponibilizadas pela ANS (Agência Nacional de Saúde Suplementar)**.

A proposta central é ampliar a **transparência**, facilitar o **acesso organizado aos dados públicos** e criar uma base técnica para análises **quantitativas** e **qualitativas** sobre o plano, sua rede assistencial, seu atendimento, seu perfil de beneficiários e seus aspectos econômico-financeiros.

O projeto parte do princípio de que dados públicos, quando organizados e analisados de forma responsável, podem apoiar:

- o entendimento da estrutura do plano;
- a avaliação da qualidade da rede de atendimento;
- a análise da relação entre beneficiários e rede credenciada;
- a observação de padrões demográficos;
- a leitura de indicadores financeiros e assistenciais;
- a produção de evidências para debates mais qualificados.

---

## Objetivos

### Objetivo geral

Construir uma base analítica aberta e reproduzível sobre o Saúde Caixa a partir de dados públicos da ANS.

### Objetivos específicos

- Mapear e catalogar as bases públicas relevantes da ANS.
- Automatizar o processo de obtenção, organização e consolidação dos arquivos.
- Estruturar um repositório que permita reprodutibilidade e evolução incremental.
- Gerar análises sobre:
  - rede credenciada;
  - beneficiários;
  - demografia;
  - utilização e cobertura assistencial;
  - indicadores financeiros;
  - qualidade e disponibilidade do atendimento.
- Disponibilizar documentação clara para facilitar colaboração da comunidade.

---

## Escopo atual do projeto

Até o momento, o projeto já conta com uma **base inicial de engenharia de dados** para viabilizar a ingestão e organização das bases públicas.

### O que já foi desenvolvido

#### 1. Catálogo de dados

Foi iniciado um notebook Jupyter com a proposta de funcionar como **catálogo de dados da ANS**, contendo:

- a listagem das bases públicas relevantes ao projeto;
- a identificação das fontes;
- instruções e código em Python para obtenção dos arquivos;
- organização inicial da documentação de entrada de dados.

#### 2. Função geral de download e mesclagem

Foi desenvolvida uma função principal para:

- baixar arquivos a partir de URLs;
- armazenar os arquivos localmente;
- identificar arquivos relacionados;
- mesclar arquivos compatíveis em bases consolidadas.

Essa função foi pensada como base para automação da ingestão de dados públicos em larga escala.

#### 3. Funções separadas por responsabilidade

Além da função geral, o projeto passou a contar também com funções separadas para melhorar reutilização e manutenção:

- **função de download**: responsável apenas por obter os arquivos;
- **função de mesclagem**: responsável apenas por consolidar arquivos compatíveis.

Essa separação favorece:

- testes isolados;
- reaproveitamento em diferentes fluxos;
- manutenção mais simples;
- possibilidade futura de adaptação para ambientes distribuídos.

---

#### 4. Decisões técnicas tomadas

##### 4.1 Utilização da biblioteca Polars

- optou-se pela utilização da Polars em detrimento da Pandas em razão da sua maior eficiência ao lidar com o tratamento das bases;
- além de um ganho de eficiência geral, o projeto trabalhará com datasets grandes, os quais são especialmente custosos para o Pandas;
- sabe-se que a melhor escolha seria utilizar Spark, no entanto, o projeto não conta com recursos financeiros que permitam a utilização de processamento distribuído;
- dessa forma, a Polars, por conseguir distribuir a carga entre os processadores de forma inteligente, acaba sendo a melhor escolha;
- outras ferramentas e tecnologias poderão ser incorporadas no futuro.

##### 4.2 Utilização do GitHub e disponibilização dos dados

- a solução ideal para disponibilização dos dados seria utilizar serviços de nuvem, não só para o armazenamento dos datasets como objetos, mas, preferencialmente, como bancos de dados baseados em SQL, no entanto, não há disponibilidade financeira para tal empreitada;
- em razão disso, optou-se por disponibilizar os notebooks com o catálogo, os códigos de tratamento e de análise, objetivando que o usuário mantenha os dados em sua própria estação de forma organizada para que possa contribuir com o projeto;
- quanto ao HitHub, sua função principal será o versionamento dos arquivos do projeto, bem como a disponibilização facilitada da colaboração pela comunidade; não há intenção de utilizá-lo como banco de dados.

## Problema que o projeto busca enfrentar

Embora os dados sejam públicos, sua utilização prática costuma ser dificultada por fatores como:

- dispersão das bases em diferentes páginas, diretórios e formatos;
- necessidade de tratamento prévio antes da análise;
- ausência de um catálogo consolidado e orientado ao uso analítico;
- barreiras técnicas para usuários não especializados;
- dificuldade de conectar diferentes bases para análises integradas.

Este projeto busca reduzir essas barreiras.

---

## Perguntas analíticas de interesse

O projeto pretende responder, entre outras, perguntas como:

### Rede credenciada e acesso

- Como evolui a rede credenciada ao longo do tempo?
- Existe compatibilidade entre o volume de beneficiários e a estrutura de atendimento disponível?
- Há sinais de concentração, expansão ou retração da rede em determinadas localidades?
- Como avaliar a suficiência da rede sob diferentes recortes?

### Qualidade do atendimento

- Há indicadores públicos que permitam aproximar a percepção de qualidade ou desempenho assistencial?
- É possível identificar gargalos ou assimetrias de cobertura por tipo de prestador?

### Perfil dos beneficiários

- Como o perfil demográfico dos beneficiários evolui ao longo do tempo?
- Existem mudanças relevantes em composição etária, distribuição geográfica ou outros recortes disponíveis?

### Sustentabilidade econômico-financeira

- Como evoluem indicadores financeiros e assistenciais relevantes?
- Que relações podem ser observadas entre despesas, beneficiários, rede e estrutura operacional?

### Transparência pública

- Como transformar dados públicos dispersos em evidências acessíveis, reproduzíveis e auditáveis?

---

## Metodologia proposta

O fluxo de trabalho do projeto tende a seguir as etapas abaixo:

1. **Levantamento das fontes públicas** da ANS.
2. **Catalogação das bases** em notebook e documentação auxiliar.
3. **Automação da obtenção dos arquivos**.
4. **Padronização e mesclagem** dos dados.
5. **Validação da qualidade dos dados**.
6. **Integração entre bases**.
7. **Análises exploratórias e descritivas**.
8. **Construção de indicadores**.
9. **Produção de visualizações, relatórios e documentação pública**.

---

## Tecnologias utilizadas até aqui

- **Python**
- **Jupyter Notebook**
- Manipulação de arquivos tabulares
- Funções customizadas para:
  - download;
  - extração;
  - organização;
  - mesclagem de bases.

### Tecnologias com potencial de adoção futura

- PySpark / Spark DataFrame
- Parquet
- DuckDB
- Power BI ou ferramentas equivalentes para visualização
- GitHub Actions para automação

---

## Estado atual do projeto

**Fase atual:** estruturação inicial da camada de ingestão e catalogação.

### Entregas já iniciadas

- [x] Definição do propósito do projeto
- [x] Início do catálogo de dados da ANS em notebook
- [x] Criação de função geral para baixar e mesclar arquivos
- [x] Criação de função isolada de download
- [x] Criação de função isolada de mesclagem
- [ ] Padronização definitiva da estrutura de pastas
- [ ] Formalização do dicionário de dados por base
- [ ] Consolidação da camada de tratamento
- [ ] Construção dos primeiros indicadores analíticos

---

## Próximos passos imediatos

- Consolidar o catálogo de bases da ANS.
- Identificar quais bases se relacionam diretamente com o Saúde Caixa.
- Organizar uma camada de dados brutos, intermediários e tratados.
- Documentar cada função já criada.
- Criar notebooks de exploração inicial por tema.
- Definir indicadores prioritários de transparência e qualidade assistencial.

---

## Princípios do projeto

- **Transparência**: tornar rastreável a origem de cada dado.
- **Reprodutibilidade**: permitir que terceiros repitam as etapas.
- **Documentação**: registrar premissas, limitações e decisões.
- **Modularidade**: separar ingestão, tratamento, validação e análise.
- **Responsabilidade analítica**: evitar conclusões indevidas e explicitar limitações dos dados públicos.

---

## Limitações esperadas

Como o projeto depende de dados públicos, algumas limitações devem ser sempre consideradas:

- possíveis atrasos de publicação;
- diferenças de granularidade entre bases;
- mudanças de layout ao longo do tempo;
- campos ausentes ou inconsistentes;
- limitações para inferência causal;
- necessidade de cautela ao interpretar proxies de qualidade.

---

## Público-alvo

Este repositório pode ser útil para:

- beneficiários e interessados em transparência do Saúde Caixa;
- pesquisadores de saúde suplementar;
- jornalistas de dados;
- analistas e cientistas de dados;
- comunidade técnica interessada em dados públicos da ANS.

---

## Como contribuir

Sugestões de contribuição:

- aprimorar documentação das bases;
- revisar dicionários de dados;
- propor novos indicadores;
- desenvolver notebooks temáticos;
- validar qualidade e consistência dos dados;
- sugerir melhorias na arquitetura do projeto.

### Boas práticas para contribuição

- abrir issue para discutir mudanças relevantes;
- documentar premissas analíticas;
- manter funções modulares e bem descritas;
- preservar rastreabilidade entre dado bruto e resultado analítico.

---

## Licença

O projeto utiliza a licença GPL 3.0.

---

## Status

🚧 **Projeto em construção** — foco atual em catalogação, ingestão e preparação da base analítica inicial.

---

## Contato / manutenção

- mantenedor: Nathan Dias Irigoyen
- canal para sugestões e colaboração: [Canal de discussões](https://github.com/nathandiasirigoyen-hub/DadosANS/discussions)
