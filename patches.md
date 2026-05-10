# Versão 1.1 - 10/05/2026

1. Refatoração completa da função `baixar_extrair_mesclar()` para lógica "lazy";
2. Nova lógica da função:
    2.1. Baixa o arquivo original;
    2.2. Armazena em uma pasta temporária;
    2.3. Transforma em `.parquet`;
    2.4. Faz as consultas solicitadas nos parâmetros utilizando o `lazyframe`;
    2.5. Salva apenas a mescla e o arquivo bruto da consulta no formato `.parquet`.
3. A nova lógica consegue reduzir o espaço de armazenamento necessário em até 99%, pois é possível eleminir todos os registros indesejados, como, por exemplo, operadoras de saúde que não serão analizadas, bem como colunas sem utilizade.
4. A nova função ficará disponível no arquivo utils_teste.py por 30 dias, momento em que será incorporada às utilidades permanentes.

____