# PlataformaDeVideosBD

## Integrantes

- Gabriel Lovato Camilo de Campos - 22.123.004-8
- Theo Zago Zimmermann - 22.123.035-2

## 1. Tema do Projeto

O projeto Plataforma de Vídeos simula o funcionamento de um sistema de streaming, inspirado em plataformas como Netflix ou YouTube.
O objetivo é demonstrar a integração entre diferentes tipos de bancos de dados — relacionais e não relacionais — aplicados a um mesmo cenário realista.

A aplicação permite:

Inserir dados fictícios de usuários e planos de assinatura;

Cadastrar vídeos e metadados;

Registrar históricos de visualização (quem assistiu o quê, quando e por quanto tempo);

Consultar todas essas informações integradas por meio de um menu interativo.

## 2. Justificativa e Definição dos Bancos Usados (S2)

A escolha dos bancos foi feita considerando as características e vantagens de cada tipo de armazenamento:

### Bancos de Dados Utilizados
PostgreSQL — Banco Relacional

Finalidade: armazenar usuários e planos de assinatura.
Justificativa: o PostgreSQL garante integridade referencial, uso de chaves primárias e estrangeiras. Ideal para cadastros estruturados e consistentes, como usuários e suas assinaturas.

MongoDB — Banco NoSQL 

Finalidade: armazenar vídeos e metadados, como descrição, duração, tags e data de upload. Justificativa: o MongoDB oferece flexibilidade de schema, permitindo armazenar documentos com diferentes formatos e tamanhos. O uso de documentos JSON facilita o registro de vídeos com campos variáveis e dados multimídia.

Astra DB (Cassandra) — Banco NoSQL 

Finalidade: armazenar o histórico de visualizações (ligações entre usuários e vídeos). Justificativa: o Cassandra, utilizado via Astra DB, é otimizado para gravações e consultas em larga escala. Sua estrutura colunar distribuída oferece alto desempenho para consultas rápidas baseadas em chave e grande volume de dados.

### Como o S2 (Service 2) foi implementado

O S2 é responsável por popular os bancos de dados e integrar as operações entre eles:

O módulo Service2 contém três conexões independentes (postgres_connection.py, mongo_connection.py e cassandra_connection.py);

Cada função de povoamento (populate) gera dados falsos com a biblioteca faker;

Após inserir os dados em cada banco, o menu principal permite escolher:

Inserir novos dados aleatórios;

Consultar dados de um usuário específico (exibindo seu cadastro, plano e histórico de visualizações completo).

## Como excecutar o Projeto

O projeto segue a arquitetura Polyglot Persistence, composta por três tipos de bancos de dados e dois serviços principais:

S1 (main.py) → Serviço principal que simula requisições e exibe o menu interativo.

S2 (Service2) → Contém as funções de conexão e manipulação dos bancos de dados (PostgreSQL, MongoDB e Astra DB).

A aplicação realiza operações de inserção e consulta em três bancos distintos, cada um responsável por um tipo de dado.

### 1. Instalação das Dependências

pip install -r requirements.txt (arquivo do projeto)

### 2. Configuração de variáveis de ambiente

Crie um arquivo chamado .env na raiz do projeto com o seguinte formato:

ASTRA_DB_TOKEN=seu_token_do_astra_aqui

ASTRA_DB_API_ENDPOINT=https://seu-endpoint-do-astra.apps.astra.datastax.com

Essas variáveis são lidas automaticamente pelo python-dotenv no código Service2/cassandra_connection.py

### 3. Configuração dos bancos de dados

#### PostgreSQL (Supabase)

Crie no Supabase as tabelas abaixo:

CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    email VARCHAR(100) UNIQUE
);

CREATE TABLE subscriptions (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(id),
    plan VARCHAR(50),
    status VARCHAR(50)
);

Atualize o arquivo Service2/postgres_connection.py com suas credenciais.

#### MongoDB (Atlas / AWS)

Crie um cluster e uma database chamada PlataformadeVideos, com uma coleção chamada videos.
No código Service2/mongo_connection.py, configure a URI de conexão.

#### Astra DB (Cassandra)

Crie uma collection chamada historico_visualizacoes dentro do keyspace streaming.
As credenciais do token e endpoint são lidas automaticamente do arquivo .env.

### 4. Executando o projeto

Execute a main.py e assim rodara um menu intuitivo para você selecionar se deseja inserir dados nos bancos ou retornar os dados dos bancos

### 5. Serviços Utilizados

| Serviço                        | Função                               | Banco de Dados        |
| ------------------------------ | ------------------------------------ | --------------------- |
| **S1 (main.py)**               | Gera e consulta dados via terminal   | Todos                 |
| **S2/postgres_connection.py**  | Conecta ao banco relacional          | PostgreSQL (Supabase) |
| **S2/mongo_connection.py**     | Conecta ao banco de documentos       | MongoDB (Atlas/AWS)   |
| **S2/cassandra_connection.py** | Conecta ao banco colunar distribuído | Astra DB (Cassandra)  |

### 6. Sistema de Logs

Durante a execução, o sistema gera automaticamente um arquivo arquivoLog.txt na raiz do projeto. Esse arquivo registra todas as interações entre os serviços S1 e S2, incluindo horário, ação e banco envolvido.

#### Exemplo: 

[2025-11-12 00:09:37] [S1 → S2] Enviando 1 usuários para o PostgreSQL...

[2025-11-12 00:09:37] [S2] 1 usuários inseridos com sucesso no PostgreSQL.

[2025-11-12 00:09:41] [S1 → S2] Enviando vídeos, filmes e séries para o MongoDB...

[2025-11-12 00:09:41] [S2] Inserção concluída no MongoDB (1 registros).

[2025-11-12 00:09:46] [S1 → S2] Enviando vídeos, filmes e séries para o MongoDB...

[2025-11-12 00:09:46] [S2] Inserção concluída no MongoDB (1 registros).

[2025-11-12 00:09:48] [S1 → S2] Solicitando todos os usuários ao PostgreSQL...

[2025-11-12 00:09:48] [S2 → S1] 162 usuários retornados do PostgreSQL.

[2025-11-12 00:09:48] [S1 → S2] Solicitando vídeos, filmes e séries ao MongoDB...

[2025-11-12 00:09:48] [S2 → S1] 398 registros retornados do MongoDB.

[2025-11-12 00:09:55] [S1 → S2] Enviando histórico de visualizações para o AstraDB...

[2025-11-12 00:09:55] [S2] 1 registros inseridos no AstraDB.

[2025-11-12 00:09:55] [S1 → S2] Enviando histórico de visualizações para o AstraDB...

[2025-11-12 00:09:55] [S2] 2 registros inseridos no AstraDB.

## Conclusão

O projeto demonstrou a integração entre diferentes tipos de bancos de dados dentro de um mesmo sistema, aplicando o conceito de persistência poliglota.
Cada tecnologia foi escolhida de acordo com suas características e pontos fortes, garantindo desempenho, consistência e flexibilidade.

O resultado é uma aplicação capaz de simular uma plataforma de vídeos com armazenamento eficiente e distribuído, mostrando na prática como soluções híbridas podem ser aplicadas em cenários reais de ecologia de dados e sustentabilidade tecnológica.

