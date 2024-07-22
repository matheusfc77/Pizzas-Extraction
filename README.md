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
2. Silver: limpar, organizar e criar recursos que serão utilizados pelo negócio
3. Gold: organizar os dados de forma eficiente e consistente para consumo das áreas. Muitas veses faz-se o uso do modelo dimensional (star-schema)

----

### Bronze

Nessa camada cada uma das quatro fontes de dados foram:
1. Lidas da camada raw
2. Nome das colunas foram renomeados seguindo o padrão de desenvolvimento do time
3. Criação de coluna "TT_INGESTION" que armazena a data e hora da ingestão. Será usado para particionamento, deduplicação e SCD (quando necessário)
4. Persistidas na camada bronze no formato delta

Para os dados de "orders" e "order_details" realizei o particionamento por ano e mês prevendo o alto volume de dados

Arquivos da bronze disponíveis em https://github.com/matheusfc77/Pizzas-Extraction/tree/main/raw_to_bronze

![image](https://github.com/user-attachments/assets/e69b9de3-8f99-43b2-9329-06ea89b2e828)

----

### Silver

Para cada uma das fontes foi feito:
1. Leitura da bronze de forma incremental, ou seja, buscando somente os novos registros com base em "TT_INGESTION"
2. Tratamento e limpeza: correção de tipagem, formatação de strings, ...
3. Persistência na silver em formato delta

Para os dados de "orders" e "order_details" foi replicado o particionamento da bronze

Arquivos da silver disponíveis em https://github.com/matheusfc77/Pizzas-Extraction/tree/main/bronze_to_silver

![image](https://github.com/user-attachments/assets/fd531c78-2ffc-4cf6-aa8c-c6018d946a38)

----

### Gold

Para esta camada foi visto com a área de negócio que:
1. O principal objetivo é acompanhar o volume de venda das pizzas ao longo do tempo e comparar com os anos anteriores
2. Algumas pizzas são alteradas buscando aprimoramento da receita e, em alguns casos, a categoria da pizza é modificada. Essa alteração precisa ser capturada sem comprometer o histórico das vendas, sendo possível comparar as vendas das diferentes versões
3. Na fonte "pizza_types" é listado os ingredientes de cada pizza. Utilizar essa informação para mensurar a matéria prima usada diariamente

Segue os processos e dimensões identificados (bus matrix):
![image](https://github.com/user-attachments/assets/a77cc952-2d18-4b30-867d-87916dee4f25)

Com base nos requisitos levantados desenhou-se a seguinte solução:
![image](https://github.com/user-attachments/assets/51b81efe-78be-4353-9322-aafd42d58c12)

1. DM_DATA: dimensão de tempo que possibilitará as análises temporais das vendas e consumo de ingredientes
2. DM_PIZZAS: dados das pizzas (nome, categoria, tamanhos, ...). Implementa a [técnica 2 de SCD](https://www.sqlshack.com/implementing-slowly-changing-dimensions-scds-in-data-warehouses/) para manter o histórico das diferentes categorias (atende ao reuisito 2)
3. DM_INGREDIENTES: lista de ingredientes das pizzas
4. BD_PIZZAS_INGREDIENTES: tabela bridge entre ingredientes e pizzas (usa CD_DURABLE_PIZZAS ao invés de CD_PIZZAS pois esta última sofre alterações devido a SCD)
5. FT_VENDAS: transações de vendas ao nível de item
6. FT_ESTOQUE: contém fotos do volume dos ingredientes gastos a cada dia

Arquivos da gold que implementam a lógica acima disponíveis em https://github.com/matheusfc77/Pizzas-Extraction/tree/main/silver_to_gold

----
----

## Orquestração dos jobs

Os notebooks foram orquestrados via Databricks na seguinte estrutura:
![image](https://github.com/user-attachments/assets/fe46acce-c6fa-450d-828b-54d8679224fd)

1. A primeira parte (quadrante cinza) realiza a carga da raw até a silver
2. Na parte dois (quadrante amarelo) é onde as fatos e dimensões são alimentadas
3. Na parte final (quadrante azul) é executado algumas consultas de validação (preço <= 0, quantidade <= 0, mais de uma versão ativa em dm_pizza, ...) visando garantir a integridade dos dados

----

Link com o vídeo explicando o projeto: https://drive.google.com/file/d/1gt-HxZMsJ-YeFlXBXFFzFU376JDuxUt7/view?usp=sharing
