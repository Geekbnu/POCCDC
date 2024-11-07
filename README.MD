
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