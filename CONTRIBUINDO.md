
# Contribuindo para o Projeto Comunitário de Análise de Dados do Saúde Caixa

Muito obrigado pelo interesse em contribuir com este projeto.

A proposta deste repositório é organizar, documentar e analisar **dados públicos da ANS** com foco em ampliar a **transparência** e a **capacidade analítica** sobre o **Saúde Caixa**, sempre com cuidado metodológico, rastreabilidade e responsabilidade no uso dos dados.

Este documento reúne orientações para quem deseja colaborar de forma técnica, analítica, documental ou metodológica.

---

## 1. Princípios de contribuição

As contribuições devem respeitar os princípios centrais do projeto:

- **Transparência**: toda transformação deve ser compreensível e rastreável.
- **Reprodutibilidade**: os passos devem poder ser repetidos por terceiros.
- **Documentação**: decisões, premissas e limitações devem ser registradas.
- **Responsabilidade analítica**: evitar conclusões não sustentadas pelos dados.
- **Modularidade**: sempre que possível, separar ingestão, tratamento, validação e análise.
- **Respeito ao escopo público**: o projeto usa dados públicos e deve manter esse compromisso de forma clara e responsável.

---

## 2. Formas de contribuir

Você pode contribuir de diferentes maneiras:

### 2.1 Dados e fontes

- identificar novas bases públicas relevantes da ANS;
- revisar links, diretórios, arquivos e periodicidades;
- documentar metadados das fontes;
- identificar mudanças de layout ou schema.

### 2.2 Engenharia de dados

- melhorar rotinas de download, extração e mesclagem;
- adicionar validações de qualidade;
- melhorar desempenho e robustez do pipeline;
- sugerir organização de pastas, manifests e logs.

### 2.3 Análise de dados

- propor indicadores;
- desenvolver notebooks temáticos;
- revisar interpretações;
- sugerir recortes analíticos relevantes.

### 2.4 Documentação

- melhorar README e planejamento;
- documentar bases e dicionários de dados;
- revisar clareza metodológica;
- criar glossários e notas explicativas.

### 2.5 Visualização e comunicação

- propor gráficos, tabelas e painéis;
- melhorar apresentação dos resultados;
- sugerir formas de comunicação mais acessíveis para o público.

---

## 3. Antes de começar

Antes de implementar uma mudança relevante, recomenda-se:

1. verificar se já existe uma **issue** relacionada;
2. abrir uma issue quando a mudança afetar:
   - arquitetura do projeto;
   - lógica de ingestão;
   - regras de transformação;
   - metodologia analítica;
   - nomenclatura de estruturas importantes;
3. explicar claramente:
   - o problema identificado;
   - a proposta de solução;
   - os impactos esperados;
   - eventuais limitações.

Isso ajuda a reduzir retrabalho e mantém o alinhamento do projeto.

---

## 4. Fluxo recomendado de contribuição

### Passo 1 — Fork do repositório

Faça um fork do projeto para sua conta.

### Passo 2 — Crie uma branch descritiva

Use nomes claros de branch, por exemplo:

```text
feature/catalogo-beneficiarios
fix/mesclagem-encoding
docs/readme-metodologia
refactor/download-funcao
analysis/rede-credenciada-inicial
```

### Passo 3 — Faça alterações pequenas e focadas

Prefira contribuições com escopo bem definido.

Evite misturar em um mesmo pull request:

- refatoração estrutural;
- correção de bug;
- nova análise;
- alteração de documentação;
- mudança metodológica grande.

### Passo 4 — Documente o que foi alterado

Sempre que possível, descreva:

- o que mudou;
- por que mudou;
- quais arquivos foram impactados;
- se há mudança de comportamento;
- se existe algum risco ou limitação residual.

### Passo 5 — Abra um Pull Request

Ao abrir o PR, inclua:

- resumo objetivo;
- motivação da mudança;
- impactos esperados;
- exemplos ou evidências, quando aplicável;
- referência à issue relacionada, se existir.

---

## 5. Tipos de contribuição mais valorizados

As contribuições mais úteis nesta fase do projeto são:

### Alta prioridade

- melhoria do catálogo de dados da ANS;
- documentação das bases;
- identificação de chaves de integração entre datasets;
- validação de qualidade de dados;
- melhoria da robustez das funções de download e mesclagem;
- organização do pipeline analítico inicial.

### Média prioridade

- notebooks de análise exploratória;
- gráficos e tabelas padronizados;
- melhorias de performance;
- propostas de indicadores.

### Prioridade contínua

- revisão de texto;
- clareza documental;
- padronização de nomenclaturas;
- correção de pequenos erros.

---

## 6. Padrões gerais para contribuições técnicas

### 6.1 Mudanças pequenas e rastreáveis

Sempre que possível:

- altere o mínimo necessário para resolver o problema;
- evite mudanças paralelas não solicitadas;
- preserve compatibilidade quando isso fizer sentido;
- explique claramente qualquer quebra de comportamento anterior.

### 6.2 Clareza do código

Contribuições de código devem priorizar:

- legibilidade;
- nomes descritivos;
- modularidade;
- baixo acoplamento;
- comentários apenas quando realmente necessários.

### 6.3 Documentação mínima

Funções novas ou alteradas devem, idealmente, incluir:

- docstring;
- descrição dos parâmetros;
- descrição do retorno;
- premissas importantes;
- observações sobre comportamento em casos limite.

### 6.4 Tratamento de erros

Sempre que possível:

- falhe de forma clara;
- use mensagens compreensíveis;
- preserve contexto suficiente para depuração;
- não silencie erros importantes sem justificativa.

---

## 7. Convenções recomendadas para estrutura do projeto

A estrutura exata pode evoluir, mas a contribuição deve respeitar a separação lógica entre camadas.

### Exemplo de organização

```text
notebooks/   -> exploração, catálogo e análises
src/         -> funções e módulos reutilizáveis
data/raw/    -> arquivos brutos
data/interim/ -> dados intermediários
data/processed/ -> saídas tratadas para análise
outputs/     -> tabelas, gráficos e relatórios
docs/        -> documentação metodológica e dicionários
```

> Observação: caso a estrutura real do repositório seja atualizada, siga a convenção mais recente adotada pelos mantenedores.

---

## 8. Orientações para notebooks

Notebooks são parte importante do projeto, especialmente para:

- catálogo de dados;
- exploração inicial;
- análises temáticas;
- demonstrações reprodutíveis.

### Boas práticas para notebooks

- manter células em ordem lógica;
- evitar outputs gigantes sem necessidade;
- incluir títulos e contexto nas seções;
- explicar premissas e limitações analíticas;
- preferir notebooks reproduzíveis do início ao fim;
- evitar misturar funções utilitárias complexas diretamente no notebook quando elas puderem ir para `src/`.

### Quando levar código do notebook para `src/`

Se um bloco de código:

- for reutilizável;
- estiver ficando longo;
- fizer parte do pipeline principal;
- exigir manutenção recorrente;

então ele provavelmente deve virar função ou módulo em `src/`.

---

## 9. Orientações para dados

### 9.1 Não versionar dados brutos pesados no Git

Sempre que possível:

- não subir arquivos grandes diretamente no repositório;
- usar `.gitignore` para diretórios locais de dados;
- documentar a fonte e o processo de obtenção.

### 9.2 Preservar rastreabilidade

Toda base tratada deve poder ser conectada à sua origem por meio de:

- documentação;
- nome padronizado;
- logs;
- manifestos de processamento;
- notebook ou script de geração.

### 9.3 Registrar limitações

Ao trabalhar com uma base, documente quando houver:

- campos ausentes;
- mudança de layout entre períodos;
- inconsistências conhecidas;
- suposições adotadas no tratamento;
- restrições de granularidade.

---

## 10. Orientações para análises e indicadores

Este projeto trata de um tema sensível e relevante. Por isso, contribuições analíticas devem observar cuidado especial.

### Recomendado

- deixar claro o recorte temporal;
- explicitar a base utilizada;
- descrever a metodologia do indicador;
- separar fato observado de interpretação;
- mencionar limitações da análise;
- usar nomes de métricas de forma consistente.

### Evitar

- conclusões fortes sem suporte suficiente;
- comparação indevida entre bases incompatíveis;
- inferência causal quando os dados não suportam isso;
- uso de proxy sem explicar suas limitações.

---

## 11. Checklist para Pull Request

Antes de abrir um PR, revise os itens abaixo.

### Checklist geral

- [ ] A mudança tem objetivo claro.
- [ ] O escopo do PR está focado.
- [ ] A descrição do PR explica o que mudou.
- [ ] A motivação da mudança está clara.
- [ ] A documentação foi atualizada, se necessário.
- [ ] Não incluí arquivos desnecessários.
- [ ] Não incluí dados pesados indevidamente.
- [ ] Mantive rastreabilidade da transformação.

### Se houver código

- [ ] O código está legível.
- [ ] Os nomes estão claros.
- [ ] Há docstrings ou explicações mínimas quando necessário.
- [ ] Casos de erro importantes foram considerados.
- [ ] Mudanças estruturais foram justificadas.

### Se houver análise

- [ ] A base utilizada foi identificada.
- [ ] O recorte temporal está claro.
- [ ] A metodologia foi explicada.
- [ ] Limitações e premissas foram registradas.

---

## 12. Modelo sugerido para descrição de Pull Request

Você pode usar o modelo abaixo:

```md
## Resumo
Descreva em poucas linhas o que este PR faz.

## Motivação
Explique o problema ou oportunidade que motivou a mudança.

## O que foi alterado
- item 1
- item 2
- item 3

## Impactos esperados
Explique o efeito da mudança no projeto.

## Limitações / pontos de atenção
Descreva riscos, restrições ou pendências.

## Checklist
- [ ] documentação atualizada
- [ ] mudança testada
- [ ] sem arquivos desnecessários
- [ ] issue relacionada referenciada
```

---

## 13. Modelo sugerido para abertura de issue

```md
## Problema
Descreva o problema identificado.

## Contexto
Explique em que parte do projeto isso ocorre.

## Comportamento atual
Descreva o comportamento observado.

## Comportamento esperado
Descreva o que deveria acontecer.

## Proposta inicial
Se quiser, sugira uma solução.

## Observações adicionais
Inclua qualquer detalhe útil.
```

---

## 14. Boas práticas de comunicação

Para manter um ambiente de colaboração saudável:

- seja respeitoso nas discussões;
- critique ideias, não pessoas;
- descreva problemas de forma objetiva;
- explique decisões técnicas com clareza;
- aceite revisões como parte do processo de melhoria.

---

## 15. Critérios de revisão dos mantenedores

As contribuições tendem a ser avaliadas com base em:

- aderência ao escopo do projeto;
- clareza da proposta;
- impacto prático da mudança;
- qualidade técnica;
- qualidade documental;
- coerência metodológica;
- facilidade de manutenção futura.

Pull requests podem receber pedidos de ajuste antes da aprovação.

---

## 16. O que evitar neste repositório

Evite, sempre que possível:

- subir dados brutos pesados sem necessidade;
- alterar muitas coisas não relacionadas em um único PR;
- mudar nomenclaturas centrais sem discussão prévia;
- introduzir dependências novas sem justificativa;
- publicar análises sem documentação mínima;
- remover rastreabilidade entre dado de origem e resultado final.

---

## 17. Sugestões de primeiras contribuições

Se você quer ajudar mas não sabe por onde começar, algumas boas primeiras contribuições são:

- revisar o README;
- melhorar descrições do catálogo de dados;
- documentar uma base da ANS;
- revisar links quebrados;
- sugerir um dicionário de dados resumido;
- organizar um notebook exploratório;
- propor um indicador com metodologia clara;
- revisar nomenclaturas e padronização textual.

---

## 18. Dúvidas e alinhamento

Se a contribuição envolver uma alteração importante, o ideal é abrir uma issue antes do PR para alinhar:

- objetivo;
- abordagem;
- impactos;
- compatibilidade com a visão do projeto.

---

## 19. Agradecimento

Toda contribuição — seja técnica, documental, metodológica ou analítica — ajuda a fortalecer a utilidade pública e a qualidade deste projeto.

Obrigado por colaborar.
