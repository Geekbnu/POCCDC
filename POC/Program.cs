using Confluent.Kafka;
using Dapper;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Npgsql;
using POC;
using POC.Enum;
using StackExchange.Redis;
using System.Data.SqlClient;
using System.Diagnostics;

namespace ForeignKeysQuery
{
    class Program
    {
        static string connectionString = "Server=localhost;Database=pocconnect;User Id=sa;Password=!sqlServerDev##;";
        static string connectionStringPostgres = "Server=127.0.0.1:5432;Database=postgres;User Id=postgres;Password=postgres;";
        static string connectionStringRedis = "localhost:6379,password=Abc.2024";
        static ConnectionMultiplexer redis;
        static string schemaPostgres = "poc";
        static List<string> topics = new List<string> { "fullfillment.pocconnect.dbo.addresses", "fullfillment.pocconnect.dbo.customer",
            "fullfillment.pocconnect.dbo.composta", "fullfillment.pocconnect.dbo.referenciacomposta" };

        private static void StartupCache()
        {
            ConfigurationOptions configOptions = ConfigurationOptions.Parse(connectionStringRedis);
            configOptions.AbortOnConnectFail = false;
            configOptions.SyncTimeout = 5000;
            configOptions.ConnectRetry = 5;
            redis = ConnectionMultiplexer.Connect(configOptions);
        }
        static async Task Main(string[] args)
        {
            StartupCache();

            var config = new ConsumerConfig
            {
                GroupId = "connect",
                BootstrapServers = "localhost:9092",
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = false
            };

            var configProducer = new ConsumerConfig
            {
                BootstrapServers = "localhost:9092",
            };

            using var producer = new ProducerBuilder<Null, string>(configProducer).Build();

            using (var consumer = new ConsumerBuilder<Ignore, string>(config).Build())
            {
                consumer.Subscribe(topics);

                CancellationTokenSource cts = new CancellationTokenSource();
                Console.CancelKeyPress += (_, e) =>
                {
                    e.Cancel = true;
                    cts.Cancel();
                };

                while (true)
                {
                    ConsumeResult<Ignore, string> consumeResult = new ConsumeResult<Ignore, string>();

                    try
                    {
                        consumeResult = consumer.Consume(cts.Token);
                    }
                    catch (OperationCanceledException)
                    {
                        Console.WriteLine("Operação Cancelada pelo usuário");
                        break;
                    }

                    Stopwatch stopWatch = Stopwatch.StartNew();

                    if (consumeResult != null)
                    {
                        try
                        {
                            if (consumeResult.Message.Value != null)
                            {
                                var data = JsonConvert.DeserializeObject<dynamic>(consumeResult.Message.Value);

                                if (data != null)
                                {
                                    var table = (string)data["source"]["table"];
                                    var connector = (string)data["source"]["connector"];
                                    var op = (string)data["op"];

                                    var foreignKeys = await GetReferencesTables();

                                    if (op == "u" || op == "c")
                                    {
                                        var detail = data["after"];
                                        var DependencyCheck = await DependencyEngineSync(schemaPostgres, table, connector, detail, foreignKeys);

                                        if (DependencyCheck == true)
                                        {
                                            await ExecuteOperationDatabaseAsync(table, detail, op);
                                        }
                                        else
                                        {
                                            var retryMessage = new Message<Null, string> { Value = consumeResult.Message.Value };
                                            producer.Produce(consumeResult.Topic, retryMessage, (deliveryReport) =>
                                            {
                                                if (deliveryReport.Error.Code != ErrorCode.NoError)
                                                {
                                                    Console.WriteLine($"Erro ao reenviar mensagem: {deliveryReport.Error.Reason}");
                                                }
                                                else
                                                {
                                                    Console.WriteLine("Mensagem reenviada para o final da fila.");
                                                }
                                            });
                                        }
                                    }
                                    else if (op == "d")
                                    {
                                        var detail = data["before"];
                                        await ExecuteOperationDatabaseAsync(table, detail, op);
                                    }
                                }
                            }
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"Erro ao processar a mensagem: {ex.Message}");

                            var retryMessage = new Message<Null, string> { Value = consumeResult.Message.Value };
                            Thread.Sleep(1000);

                            producer.Produce(consumeResult.Topic, retryMessage, (deliveryReport) =>
                            {
                                if (deliveryReport.Error.Code != ErrorCode.NoError)
                                {
                                    Console.WriteLine($"Erro ao reenviar mensagem: {deliveryReport.Error.Reason}");
                                }
                                else
                                {
                                    Console.WriteLine("Mensagem reenviada para o final da fila.");
                                }
                            });
                        }
                        finally
                        {
                            consumer.Commit(consumeResult);
                            stopWatch.Stop();
                            Console.WriteLine(@$"RunTime {stopWatch.Elapsed:hh\:mm\:ss\.fff}");
                        }
                    }
                }
            }
        }

        /// <summary>
        /// Executa a operação no banco de dados
        /// </summary>
        /// <param name="tableName"></param>
        /// <param name="data"></param>
        /// <param name="operation"></param>
        /// <param name="newFkReferences"></param>
        /// <returns></returns>
        /// <exception cref="ArgumentException"></exception>
        private static async Task ExecuteOperationDatabaseAsync(string tableName, dynamic data, string operation)
        {
            List<TableProperty> tableProperties = await GetPropertiesOriginTable(tableName, Database.SQLSERVER);

            using (var conn = new NpgsqlConnection(connectionStringPostgres))
            {
                await conn.OpenAsync();

                JObject dataJson = data as JObject;

                if (dataJson == null)
                {
                    throw new ArgumentException("O parâmetro data deve ser um objeto JSON válido.");
                }

                var columnNames = new List<string>();
                var valuePlaceholders = new List<string>();
                var setClauses = new List<string>();
                var whereClauses = new List<string>();

                int index = 0;

                // Varre as propriedades do registro para popular as clausulas SQL (update e insert).
                foreach (var property in dataJson.Properties())
                {
                    columnNames.Add(property.Name.ToLower());
                    setClauses.Add($"{property.Name} = @p{index}");
                    valuePlaceholders.Add($"@p{index}");

                    if (tableProperties.Where(x => x.IsPrimaryKey).Any(x => x.Name == property.Name))
                    {
                        whereClauses.Add($"{property.Name.ToLower()} = @p{index}");
                    }

                    index++;
                }

                if (operation == "c") // Insert
                {
                    var columns = string.Join(", ", columnNames);
                    var values = string.Join(", ", valuePlaceholders);

                    var insertSql = $"INSERT INTO {schemaPostgres}.{tableName} ({columns}) VALUES ({values})";

                    var listaIdValues = new List<object>();
                    using (var cmd = new NpgsqlCommand(insertSql, conn))
                    {
                        index = 0;

                        foreach (var property in dataJson.Properties())
                        {
                            if (tableProperties.Where(x => x.DataType.Equals("datetime")).Any(x => x.Name == property.Name))
                            {
                                long timestamp = (long)property.Value.ToObject<object>();
                                DateTime dateTime = DateTimeOffset.FromUnixTimeMilliseconds(timestamp).UtcDateTime;

                                DateTime? value = property.Value.Type == JTokenType.Null ? null : dateTime;
                                cmd.Parameters.AddWithValue($"@p{index}", value);
                            }
                            else
                            {
                                var value = property.Value.Type == JTokenType.Null ? DBNull.Value : property.Value.ToObject<object>();
                                cmd.Parameters.AddWithValue($"@p{index}", value);
                            }

                            // Adicionando os ID para usar no cache
                            if (tableProperties.Where(p => p.IsPrimaryKey).Any(x => x.Name == property.Name))
                            {
                                listaIdValues.Add(property.Value.ToObject<object>());
                            }

                            index++;
                        }

                        await cmd.ExecuteNonQueryAsync();

                        var ids = string.Join("-", tableProperties.Where(x => x.IsPrimaryKey).Select(x => x.Name));
                        var valuesId = string.Join("-", listaIdValues);
                        await redis.GetDatabase().StringSetAsync($"{tableName}.{ids}.{valuesId}", true);
                    }
                }
                else if (operation == "u") // Update
                {
                    var setClause = string.Join(", ", setClauses);
                    var setWhereClause = string.Join(" AND ", whereClauses);

                    // TODO: Ajustar o query do UPDATE AQUI
                    var updateSql = $"UPDATE {schemaPostgres}.{tableName} SET {setClause} WHERE {setWhereClause}";

                    var listaIdValues = new List<object>();

                    using (var cmd = new NpgsqlCommand(updateSql, conn))
                    {
                        index = 0;
                        foreach (var property in dataJson.Properties())
                        {
                            // Tratamento especial para datetime do SQL SERVER para POSTGRES
                            if (tableProperties.Where(x => x.DataType.Equals("datetime")).Any(x => x.Name == property.Name))
                            {
                                long timestamp = (long)property.Value.ToObject<object>();
                                DateTime dateTime = DateTimeOffset.FromUnixTimeMilliseconds(timestamp).UtcDateTime;
                                DateTime? value = property.Value.Type == JTokenType.Null ? null : dateTime;
                                cmd.Parameters.AddWithValue($"@p{index}", value);
                            }
                            else
                            {
                                var value = property.Value.Type == JTokenType.Null ? DBNull.Value : property.Value.ToObject<object>();
                                cmd.Parameters.AddWithValue($"@p{index}", value);
                            }

                            // Adicionando os ID para usar no cache
                            if (tableProperties.Where(p => p.IsPrimaryKey).Any(x => x.Name == property.Name))
                            {
                                listaIdValues.Add(property.Value.ToObject<object>());
                            }

                            index++;
                        }

                        await cmd.ExecuteNonQueryAsync();

                        var ids = string.Join("-", tableProperties.Where(x => x.IsPrimaryKey).Select(x => x.Name));
                        var valuesId = string.Join("-", listaIdValues);
                        await redis.GetDatabase().StringSetAsync($"{tableName}.{ids}.{valuesId}", true);
                    }
                }
                else if (operation == "d") // Delete
                {
                    var setWhereClause = string.Join(" AND ", whereClauses);

                    var deleteSql = $"DELETE FROM {schemaPostgres}.{tableName} WHERE {setWhereClause}";

                    var listaIdValues = new List<object>();
                    using (var cmd = new NpgsqlCommand(deleteSql, conn))
                    {
                        index = 0;
                        foreach (var property in dataJson.Properties())
                        {
                            if (tableProperties.Where(p => p.IsPrimaryKey).Any(x => x.Name == property.Name))
                            {
                                var value = property.Value.Type == JTokenType.Null ? DBNull.Value : property.Value.ToObject<object>();
                                cmd.Parameters.AddWithValue($"@p{index}", value);
                                listaIdValues.Add(value);
                            }

                            index++;
                        }

                        await cmd.ExecuteNonQueryAsync();
                        var ids = string.Join("-", tableProperties.Where(x => x.IsPrimaryKey).Select(x => x.Name));
                        var valuesId = string.Join("-", listaIdValues);
                        await redis.GetDatabase().KeyDeleteAsync($"{tableName}.{ids}.{valuesId}");
                    }
                }
            }
        }

        /// <summary>
        /// Serviço capaz de verificar a dependência de uma tabela em relação a outra (FK)
        /// Serviço verifica se há registro no banco de dados (FK)
        /// </summary>
        /// <param name="schema"></param>
        /// <param name="table"></param>
        /// <param name="connector"></param>
        /// <param name="data"></param>
        /// <param name="foreignKeys"></param>
        /// <returns>Caso haja dependência, o sistema retornará informações destas FKS</returns>
        private static async Task<bool> DependencyEngineSync(string schema, string table, string connector, dynamic data, IEnumerable<ForeignKeyInfo?> foreignKeys)
        {
            var keyPar = new List<KeyPar>();

            // Busca nos registros de foreignKeys especificamente a tabela que está sendo processada.
            var fks = foreignKeys.Where(k => k.ParentTable.Equals(table));

            // Faz o agrupamento das referências para garantir funcionamento para chaves compostas.
            var refTable = fks.GroupBy(k => k.ReferencedTable);

            if (refTable.Any())
            {
                foreach (var key in refTable)
                {
                    // A tabela em questão possui referência a outras tabelas (FK)
                    // Busque e traga somente as informações desta tabela que estamos iterando
                    IEnumerable<ForeignKeyInfo?> fksReferenceTable = fks.Where(r => r.ReferencedTable.Equals(key.FirstOrDefault()?.ReferencedTable, StringComparison.Ordinal));

                    var fkItem = new KeyPar
                    {
                        Table = key.FirstOrDefault().ReferencedTable,
                        Schema = key.FirstOrDefault().ParentSchema
                    };

                    foreach (var fk in fksReferenceTable)
                    {
                        var value = data[fk?.ParentColumn];
                        if (value != null)
                        {
                            fkItem.Keys.Add(new Key
                            {
                                Name = $"{fk?.ReferencedColumn}",
                                Value = value.ToString(),
                            });
                        }
                    }

                    keyPar.Add(fkItem);
                }

                if (!keyPar.Any())
                {
                    return true;
                }
                else
                {
                    // Sendo a tabela dependente
                    // 1. verifica se os pais existem no banco de dados.
                    return await ExistsInDatabase(keyPar);
                }
            }
            else
            {
                return true;
            }
        }

        private static async Task<List<TableProperty>> GetPropertiesOriginTable(string table, Database database)
        {
            var keyCache = await redis.GetDatabase().StringGetAsync($"Property.{database}.{table}");
            if (keyCache.HasValue)
            {
                return JsonConvert.DeserializeObject<List<TableProperty>>(keyCache);
            }

            var primaryKeys = new List<TableProperty>();

            string query = @$"SELECT 
                            c.COLUMN_NAME AS Name,
                            c.DATA_TYPE AS DataType,
                            CASE 
                                WHEN kcu.COLUMN_NAME IS NOT NULL THEN 1 
                                ELSE 0 
                            END AS isPrimaryKey
                        FROM 
                            INFORMATION_SCHEMA.COLUMNS AS c
                        LEFT JOIN 
                            INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS tc 
                            ON c.TABLE_NAME = tc.TABLE_NAME 
                            AND tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
                        LEFT JOIN 
                            INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS kcu 
                            ON c.TABLE_NAME = kcu.TABLE_NAME 
                            AND c.COLUMN_NAME = kcu.COLUMN_NAME 
                            AND tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
                        WHERE 
                            c.TABLE_NAME = '{table}'
                        ORDER BY 
                            c.ORDINAL_POSITION;";

            using (var connection = new SqlConnection(connectionString))
            {
                await connection.OpenAsync();
                var operation = await connection.QueryAsync<TableProperty>(query);

                foreach (var item in operation)
                {
                    primaryKeys.Add(item);
                }
            }

            await redis.GetDatabase().StringSetAsync($"Property.{database}.{table}", JsonConvert.SerializeObject(primaryKeys));

            return primaryKeys;
        }


        /// <summary>
        /// Serviço que verifica se um determinado registro existe no banco dados.
        /// Usado principalmente para consultar a existência de um pai em uma relação de FK
        /// </summary>
        /// <param name="keyPars"></param>
        /// <returns></returns>
        private static async Task<bool> ExistsInDatabase(IEnumerable<KeyPar> keyPars)
        {
            bool exists = false;
            var list = new List<string>();

            var whereClause = string.Join(" AND ", keyPars.SelectMany(x => x.Keys.Select(x => $" {x.Name} = {x.Value} ")));

            // Varre cada um dos pais desse registros para verificar a existência
            foreach (var keyPar in keyPars)
            {
                // Verifica primeiro no Redis se o registro existe
                var ids = string.Join("-", keyPar.Keys.Select(x => x.Name));
                var valuesId = string.Join("-", keyPar.Keys.Select(x => x.Value));

                var keyCache = await redis.GetDatabase().StringGetAsync($"{keyPar.Table}.{ids}.{valuesId}");

                if (keyCache.HasValue)
                {
                    exists = true;
                }
                else
                {
                    // Caso contrário, vai até o banco de dados para fazer a verificação
                    string query = @$"SELECT 1 FROM {schemaPostgres}.{keyPar.Table} WHERE {whereClause} LIMIT 1";

                    try
                    {
                        using (var connection = new NpgsqlConnection(connectionStringPostgres))
                        {
                            await connection.OpenAsync();
                            var operation = await connection.QueryFirstOrDefaultAsync<int?>(query);

                            if (operation.HasValue == false)
                            {
                                exists = false;
                            }
                            else
                            {
                                // E salva o registro no redis para eventuais consultas posteriores.
                                await redis.GetDatabase().StringSetAsync($"{keyPar.Table}.{ids}.{valuesId}", true);
                            }
                        }
                    }
                    catch (SqlException ex)
                    {
                        // Em caso de falha, log o erro e sinalize que não conseguiu encontrar o pai retornando false
                        Console.WriteLine($"An error occurred: {ex.Message}");
                        return false;
                    }

                }
            }

            return exists;
        }

        /// <summary>
        /// Serviço que busca referência das relaçãos PK x FK das tabelas no banco
        /// </summary>
        /// <returns>Lista de relações de chave PK x FK em todas as tabelas do banco</returns>
        private static async Task<IEnumerable<ForeignKeyInfo>> GetReferencesTables(string tabela = "")
        {
            string query = @"
               SELECT 
                fk.name AS ForeignKeyName,
                s1.name AS ParentSchema,
                tp.name AS ParentTable,
                cp.name AS ParentColumn,
                s2.name AS ReferencedSchema,
                tr.name AS ReferencedTable,
                cr.name AS ReferencedColumn
            FROM 
                sys.foreign_keys AS fk
            INNER JOIN 
                sys.foreign_key_columns AS fkc ON fk.object_id = fkc.constraint_object_id
            INNER JOIN 
                sys.tables AS tp ON fkc.parent_object_id = tp.object_id
            INNER JOIN 
                sys.schemas AS s1 ON tp.schema_id = s1.schema_id
            INNER JOIN 
                sys.columns AS cp ON fkc.parent_object_id = cp.object_id AND fkc.parent_column_id = cp.column_id
            INNER JOIN 
                sys.tables AS tr ON fkc.referenced_object_id = tr.object_id
            INNER JOIN 
                sys.schemas AS s2 ON tr.schema_id = s2.schema_id
            INNER JOIN 
                sys.columns AS cr ON fkc.referenced_object_id = cr.object_id AND fkc.referenced_column_id = cr.column_id";

            if (!string.IsNullOrEmpty(tabela))
            {
                query += $" WHERE tp.name LIKE '{tabela}' ";
            }

            query += @" ORDER BY s1.name, tp.name, fk.name;";

            try
            {
                using (var connection = new SqlConnection(connectionString))
                {
                    await connection.OpenAsync();

                    return connection.QueryAsync<ForeignKeyInfo>(query).Result;
                }
            }
            catch (SqlException ex)
            {
                Console.WriteLine($"An error occurred: {ex.Message}");
                throw;
            }
        }
    }
}