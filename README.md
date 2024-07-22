# Pizzas-Extraction

Este projeto utiliza os dados disponibilizados em [Pizza Place Sales](https://www.kaggle.com/datasets/mysarahmadbhat/pizza-place-sales?select=pizza_types.csv) no Kaggle.

Consistem em quatro arquivos csv:
1. orders.csv: dados do cabeçalho da venda (ID, data e hora)
2. order_details.csv: detalhes da venda (código do produto e quantidade do item vendido)
3. pizza_types.csv: lista de pizzas disponíveis. Contém o nome, categoria e ingredientes de cada pizza
4. pizza.csv: opções de tamanhos de cada pizza e seu respectivo preço

Segue o relacionamento das tabelas:

![image](https://github.com/user-attachments/assets/e391cb79-b56f-4a37-a049-bb0a77c52e91)

----
----

## Ingestão

Foi utilizado a arquitetura medalhão onde cada camada é reponsável por:
1. Bronze: replicar dados da raw mantendo todo o histórico das ingestões
2. Silver: limpar, organizar e criar recursos que serão utilizados pelo negócio;
3. Gold: organizar os dados de forma eficiente e consistente para consumo das áreas. Muitas veses se faz o uso do modelo dimensional (star-schema)

----

### Bronze

Nessa camada cada uma das quatro fontes de dados foram:
1. Lidas da camada raw
2. Nome das colunas foram renomeados seguindo o padrão de desenvolvimento do time
3. Criação de coluna "TT_INGESTION" que armazena a data e hora da ingestão. Será usado para particionamento, deduplicação e SCD (se necessário)
4. Persistidas na camada bronze no formato delta

Para os dados de "orders" e "order_details" realizei o particionamento por ano e mês prevendo o alto volume de dados

Arquivos da bronze disponíveis em

![image](https://github.com/user-attachments/assets/e69b9de3-8f99-43b2-9329-06ea89b2e828)

----

### Silver

Para cada uma das fontes foi feito:
1. Leitura da bronze de forma incremental, ou seja, buscando somente os novos registros com base em "TT_INGESTION"
2. Tratamento e limpeza: correção de tipagem, formatação de strings, ...
3. Persistência na silver

Para os dados de "orders" e "order_details" foi replicado o particionamento da bronze

Arquivos da silver disponíveis em 

![image](https://github.com/user-attachments/assets/fd531c78-2ffc-4cf6-aa8c-c6018d946a38)

----

### Gold
