using Confluent.Kafka;
using Dapper;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Npgsql;
using POC;
using StackExchange.Redis;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;

namespace ForeignKeysQuery
{
    class Program
    {
        static string connectionString = "Server=localhost;Database=pocconnect;User Id=sa;Password=!sqlServerDev##;";
        static string connectionStringPostgres = "Server=127.0.0.1:5432;Database=postgres;User Id=postgres;Password=postgres;";
        static string connectionStringRedis = "localhost:6379,password=Abc.2024";
        static ConnectionMultiplexer redis;
        static string schemaPostgres = "poc";
        static List<string> topics = new List<string> { "fullfillment.pocconnect.dbo.addresses", "fullfillment.pocconnect.dbo.customer", "fullfillment.pocconnect.dbo.composta" };

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
                                            var newFkReferences = await GetNewReferencePKFromDatabaseAsync(detail, table, foreignKeys);
                                            await ExecuteOperationDatabaseAsync(table, detail, op, newFkReferences);
                                        }

                                        consumer.Commit(consumeResult);
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
                            stopWatch.Stop();
                            Console.WriteLine(@$"RunTime {stopWatch.Elapsed:hh\:mm\:ss\.fff}");
                        }
                    }
                }
            }
        }

        /// <summary>
        /// Busca referência das FK no banco destino para garantir teremos os ID corretos para relacionar.
        /// </summary>
        /// <param name="data"></param>
        /// <param name="fks"></param>
        /// <returns>Relação de IDs atualizados</returns>
        private static async Task<Dictionary<string, object>> GetNewReferencePKFromDatabaseAsync(dynamic data, string table, IEnumerable<ForeignKeyInfo?> fks)
        {
            var dictionary = new Dictionary<string, object>();

            using (var connection = new NpgsqlConnection(connectionStringPostgres))
            {
                await connection.OpenAsync();

                var tableFks = fks.Where(c => c.ParentTable.Equals(table));

                foreach (var fk in tableFks)
                {
                    // Busque pela ReferencedColumn, ou seja, para a coluna na tabela pai que é ID do filho.
                    var query = @$"SELECT {fk?.ReferencedColumn} FROM {schemaPostgres}.{fk?.ReferencedTable} WHERE s_{fk?.ReferencedColumn} = '{data[fk?.ParentColumn]}' LIMIT 1";
                    var value = await connection.QueryFirstOrDefaultAsync<int?>(query, new { value = (string)data[fk?.ParentColumn] });

                    if (value.HasValue)
                    {
                        dictionary.Add(fk.ParentColumn, value.Value);
                    }
                }
            }

            return dictionary;
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
        private static async Task ExecuteOperationDatabaseAsync(string tableName, dynamic data, string operation, Dictionary<string, object> newFkReferences)
        {
            var tablePK = await GetPrimaryKeys(schemaPostgres,tableName);

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

                int index = 0;

                // Varre as propriedades do registro para popular as clausulas SQL (update e insert).
                foreach (var property in dataJson.Properties())
                {
                    // A referência da tabela origem, virará destino s_{key} na base destino.
                    if (tablePK.Any(x => x.Name.Equals(property.Name.ToLower())))
                    {
                        var key = tablePK.Find(x => x.Name.Equals(property.Name.ToLower()));

                        if (key.IsAutoIncrement == false)
                        {
                            columnNames.Add($"s_{property.Name.ToLower()}");
                            setClauses.Add($"s_{property.Name} = @p{index}");
                            valuePlaceholders.Add($"@p{index}");
                            index++;
                        }
                        else
                        {
                            columnNames.Add($"s_{property.Name.ToLower()}");
                            setClauses.Add($"s_{property.Name} = @p{index}");
                            valuePlaceholders.Add($"@p{index}");
                            index++;
                            continue;
                        }

                    }

                    columnNames.Add(property.Name.ToLower());
                    setClauses.Add($"{property.Name} = @p{index}");
                    valuePlaceholders.Add($"@p{index}");
                    index++;
                }

                if (operation == "c") // Insert
                {
                    var columns = string.Join(", ", columnNames);
                    var values = string.Join(", ", valuePlaceholders);

                    var insertSql = $"INSERT INTO {schemaPostgres}.{tableName} ({columns}) VALUES ({values})";

                    using (var cmd = new NpgsqlCommand(insertSql, conn))
                    {
                        index = 0;

                        foreach (var property in dataJson.Properties())
                        {
                            // Verificar se há registro na tabela de PK
                            if (tablePK.Any(x => x.Name.Equals(property.Name.ToLower())))
                            {
                                // Busca o registro especifico na coleção
                                var key = tablePK.Find(x => x.Name.Equals(property.Name.ToLower()));

                                if (key.IsAutoIncrement == false)
                                {
                                    cmd.Parameters.AddWithValue($"@p{index}",
                                        property.Value.Type == JTokenType.Null ? DBNull.Value : property.Value.ToObject<object>());

                                    index++;
                                }
                                else
                                {
                                    cmd.Parameters.AddWithValue($"@p{index}",
                                        property.Value.Type == JTokenType.Null ? DBNull.Value : property.Value.ToObject<object>());

                                    index++;
                                    continue;
                                }
                            }

                            if (newFkReferences.ContainsKey(property.Name))
                            {
                                cmd.Parameters.AddWithValue($"@p{index}",
                                    property.Value.Type == JTokenType.Null ? DBNull.Value : newFkReferences[property.Name]);

                                index++;
                            }

                            var value = property.Value.Type == JTokenType.Null ? DBNull.Value : property.Value.ToObject<object>();
                            cmd.Parameters.AddWithValue($"@p{index}", value);
                            index++;
                        }

                        await cmd.ExecuteNonQueryAsync();
                    }
                }
                else if (operation == "u") // Update
                {
                    var setClause = string.Join(", ", setClauses);

                    // Supondo que você tenha uma chave primária chamada "id" para usar no where
                    var updateSql = $"UPDATE {schemaPostgres}.{tableName} SET {setClause} WHERE s_id = @s_id";

                    using (var cmd = new NpgsqlCommand(updateSql, conn))
                    {
                        index = 0;
                        foreach (var property in dataJson.Properties())
                        {
                            if (tablePK.Any(x => x.Name.Equals(property.Name.ToLower())))
                            {
                                // Busca o registro especifico na coleção
                                var key = tablePK.Find(x => x.Name.Equals(property.Name.ToLower()));

                                if (key.IsAutoIncrement == false)
                                {
                                    cmd.Parameters.AddWithValue($"@p{index}",
                                        property.Value.Type == JTokenType.Null ? DBNull.Value : property.Value.ToObject<object>());

                                    index++;
                                }
                                else
                                {
                                    cmd.Parameters.AddWithValue($"@p{index}",
                                        property.Value.Type == JTokenType.Null ? DBNull.Value : property.Value.ToObject<object>());

                                    index++;
                                    continue;
                                }
                            }

                            if (newFkReferences.ContainsKey(property.Name))
                            {
                                cmd.Parameters.AddWithValue($"@p{index}",
                                    property.Value.Type == JTokenType.Null ? DBNull.Value : newFkReferences[property.Name]);

                                index++;
                            }

                            var value = property.Value.Type == JTokenType.Null ? DBNull.Value : property.Value.ToObject<object>();
                            cmd.Parameters.AddWithValue($"@p{index}", value);
                            index++;
                        }
                       
                        await cmd.ExecuteNonQueryAsync();
                    }
                }
                else if (operation == "d") // Delete
                {
                    var deleteSql = $"DELETE FROM {schemaPostgres}.{tableName} WHERE id = @id";

                    using (var cmd = new NpgsqlCommand(deleteSql, conn))
                    {
                        // Aqui você precisa garantir que o campo 'id' está presente no data
                        if (dataJson["id"] == null)
                        {
                            throw new ArgumentException("O campo 'id' é obrigatório para operações de exclusão.");
                        }
                        cmd.Parameters.AddWithValue("@id", dataJson["id"].ToObject<object>());
                        await cmd.ExecuteNonQueryAsync();
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

                    foreach (var fk in fksReferenceTable)
                    {
                        var value = data[fk?.ParentColumn];
                        if (value != null)
                        {
                            // Adicione no keypar as referências da chave estrangeira
                            keyPar.Add(new KeyPar
                            {
                                Key = $"s_{fk?.ReferencedColumn}",
                                Value = value.ToString(),
                                Table = fk.ReferencedTable,
                                Schema = schema
                            });
                        }
                    }
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

        private static async Task<List<PrimaryKey>> GetPrimaryKeys(string schema, string table)
        {
            var primaryKeys = new List<PrimaryKey>();

            string query = @$"SELECT 
                            a.attname AS Name,
                            CASE 
                                WHEN pg_get_serial_sequence(i.indrelid::regclass::text, a.attname) IS NOT NULL THEN 1
                                ELSE 0
                            END AS IsAutoIncrement
                        FROM 
                            pg_index i
                            JOIN pg_attribute a ON a.attnum = ANY(i.indkey)
                            AND a.attrelid = i.indrelid
                        WHERE 
                            i.indrelid = '{schema}.{table}'::regclass
                            AND i.indisprimary;";

            using (var connection = new NpgsqlConnection(connectionStringPostgres))
            {
                await connection.OpenAsync();
                var operation = await connection.QueryAsync<PrimaryKey>(query);

                foreach (var item in operation)
                {
                    primaryKeys.Add(item);
                }
            }

            return primaryKeys;
        }

        private struct PrimaryKey
        {
            public string Name;
            public bool IsAutoIncrement;
        }

        /// <summary>
        /// Serviço que verifica se um determinado registro existe no banco dados.
        /// Usado principalmente para consultar a existência de um pai em uma relação de FK
        /// </summary>
        /// <param name="keyPars"></param>
        /// <returns></returns>
        private static async Task<bool> ExistsInDatabase(IEnumerable<KeyPar> keyPars)
        {

            // Varre cada um dos pais desse registros para verificar a existência
            foreach (var keyPar in keyPars)
            {
                // Verifica primeiro no Redis se o registro existe
                var keyCache = await redis.GetDatabase().StringGetAsync($"{keyPar.Connector}.{keyPar.Table}.{keyPar.Key}.{keyPar.Value}");

                if (keyCache.HasValue)
                {
                    return true;
                }

                // Caso contrário, vai até o banco de dados para fazer a verificação
                string query = @$"SELECT 1 FROM {keyPar.Schema}.{keyPar.Table} WHERE {keyPar.Key} = @value LIMIT 1";

                try
                {
                    using (var connection = new NpgsqlConnection(connectionStringPostgres))
                    {
                        await connection.OpenAsync();
                        var operation = await connection.QueryFirstOrDefaultAsync<int?>(query, new { value = keyPar.Value });

                        if (operation.HasValue == false)
                        {
                            return false;
                        }

                        // E salva o registro no redis para eventuais consultas posteriores.
                        await redis.GetDatabase().StringSetAsync($"{keyPar.Connector}.{keyPar.Table}.{keyPar.Key}.{keyPar.Value}", true);
                    }
                }
                catch (SqlException ex)
                {
                    // Em caso de falha, log o erro e sinalize que não conseguiu encontrar o pai retornando false
                    Console.WriteLine($"An error occurred: {ex.Message}");
                    return false;
                }
            }

            return true;
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