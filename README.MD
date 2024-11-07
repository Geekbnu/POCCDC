# Instalação
Siga os passos abaixo para executar a solução. Baseado na solução de #https://medium.com/@kayvan.sol2/debezium-source-connector-from-sql-server-to-apache-kafka-7d59d56f5cc7


## Requisitos
- Docker
- Visual Studio Code ou Visual Studio 2022
- .NET 8

## Docker
Para rodar subir a estutura do sistema rode o seguinte comando.

```bash
docker-compose up -d
```

## Configuração do SqlServer

```bash
# subir os containers
docker-compose up -d
# acessar o container do sql para habilitar o sql client
docker exec -it --user root sqlserver "bash"
#habilitar o sql client
/opt/mssql/bin/mssql-conf set sqlagent.enabled true
#sair do container
exit
#restart o container
docker-compose restart sql-server
```

## Adicionando as tabelas no banco de dados
No SQL SERVER, acesse via localhost:1433, com as credenciais: *sa* e a senha *!sqlServerDev##*

```sql
--1 criar tabase
CREATE DATABASE pocconnect;

--2 Usar database
USE pocconnect;

--3 Criar tabelas de teste
CREATE TABLE dbo.customer (
  id INT PRIMARY KEY,
  name VARCHAR(50) NOT NULL,
  source VARCHAR(20) NOT NULL
);

CREATE TABLE dbo.addresses (
  id INT PRIMARY KEY,
  street VARCHAR(250) NOT NULL,
  source VARCHAR(20) NOT NULL,
  customerId INT NOT NULL,
  FOREIGN KEY (customerId) REFERENCES customer(id)
);

CREATE TABLE dbo.composta (
    ID INT IDENTITY(1,1) NOT NULL,
    IDEMPRESA INT NOT NULL,
    SOURCE VARCHAR(50) NOT NULL,
    Nome VARCHAR(100),
    DataCriacao DATETIME DEFAULT GETDATE(),
    PRIMARY KEY (ID, IDEMPRESA)
);

-- ative o CDC
EXEC sys.sp_cdc_enable_db
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'customer', @role_name = NULL, @supports_net_changes = 0;
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'addresses', @role_name = NULL, @supports_net_changes = 0;
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'composta', @role_name = NULL, @supports_net_changes = 0;

```

No Postgres (Apenas iremos salvar no Postgres)

```sql
create schema poc;

--1 Criar tabela de teste
CREATE TABLE poc.customer (
  id INT NOT NULL,
  name VARCHAR(255) NOT NULL,
  source VARCHAR(255) NOT NULL,
  source_pk VARCHAR(255)  NULL,
  PRIMARY KEY (id)
);


CREATE TABLE poc.addresses (
  id SERIAL PRIMARY KEY,
  street VARCHAR(250) NOT NULL,
  source VARCHAR(20) NOT NULL,
  customerid INT NOT NULL,
  FOREIGN KEY (customerid) REFERENCES poc.customer(id)
);


CREATE TABLE poc.composta (
    ID INT not null,
    IDEMPRESA INT NOT NULL,
    SOURCE VARCHAR(50) NOT NULL,
    Nome VARCHAR(100),
    DataCriacao TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (ID, IDEMPRESA)
);
```

## Adicionando o conector no SQLServer
Abaixo você estará adicionando um conector responsável por buscar os dados do SQLServer.

```bash
docker exec -it connect1 bash

curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" localhost:8083/connectors -d '{ "name": "source-connector", 
"config": { 
    "connector.class": "io.debezium.connector.sqlserver.SqlServerConnector",
    "database.hostname": "sql-server", 
    "database.port": "1433", 
    "database.user": "sa",
    "database.password": "!sqlServerDev##", 
    "database.dbname": "pocconnect", 
    "database.names":"pocconnect",
    "database.server.name": "sql-server", 
    "table.whitelist": "dbo.composta, dbo.addresses, dbo.customer, dbo.referenciacomposta", 
    "topic.prefix": "fullfillment",
    "database.history.kafka.bootstrap.servers": "kafka1:29092", 
    "database.history.kafka.topic": "schema-changes-topic",
    "errors.log.enable": "true",
    "schema.history.internal.kafka.bootstrap.servers": "kafka1:29092",  
    "schema.history.internal.kafka.topic": "schema-changes.inventory",
    "database.trustServerCertificate": true,
    "tombstones.on.delete": "false"  } 
}';
```

## Execute a solução

Entre na pasta do projeto POC onde estão os arquivos do programa. Execute o seguinte comando abaixo:

```bash
dotnet run
```

A tela irá ficar preta, com o cursor piscando, esperando você fazer alguma interação no banco de dados. 

