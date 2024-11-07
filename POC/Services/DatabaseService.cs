using Dapper;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Npgsql;
using POC.Domain;
using POC.Enum;
using POC.Interfaces;
using System.Data.SqlClient;

namespace POC.Services
{
    public class DatabaseServiceSQLServerToPostgres : IDatabaseService
    {
        private readonly IRedisCacheService _redisCache;
        private readonly IPropertyService _property;
        private readonly string _schemaPostgres;
        public DatabaseServiceSQLServerToPostgres(IRedisCacheService redisCache, IPropertyService property)
        {
            _redisCache = redisCache;
            _property = property;
            _schemaPostgres = _property.GetProperty(Property.SCHEMAPOSTGRES);
        }

        public async Task ExecuteOperationAsync(string table, dynamic data, DatabaseOperation? operation)
        {
            List<TableProperty> tableProperties = await GetPropertiesOriginTable(table, Database.SQLSERVER);

            using (var conn = new NpgsqlConnection(_property.GetProperty(Property.CONNECTIONSTRINGPOSTGRES)))
            {
                await conn.OpenAsync();

                var dataJson = data as JObject;

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

                if (operation == DatabaseOperation.CREATE)
                {
                    var columns = string.Join(", ", columnNames);
                    var values = string.Join(", ", valuePlaceholders);

                    var insertSql = $"INSERT INTO {_schemaPostgres}.{table} ({columns}) VALUES ({values})";

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
                        await _redisCache.SetAsync($"{table}.{ids}.{valuesId}", true);
                    }
                }
                else if (operation == DatabaseOperation.UPDATE)
                {
                    var setClause = string.Join(", ", setClauses);
                    var setWhereClause = string.Join(" AND ", whereClauses);

                    // TODO: Ajustar o query do UPDATE AQUI
                    var updateSql = $"UPDATE {_schemaPostgres}.{table} SET {setClause} WHERE {setWhereClause}";

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
                        await _redisCache.SetAsync($"{table}.{ids}.{valuesId}", true);
                    }
                }
                else if (operation == DatabaseOperation.DELETE)
                {
                    var setWhereClause = string.Join(" AND ", whereClauses);

                    var deleteSql = $"DELETE FROM {_schemaPostgres}.{table} WHERE {setWhereClause}";

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
                        await _redisCache.DeleteAsync($"{table}.{ids}.{valuesId}");
                    }
                }
            }
        }


        public DatabaseOperation? GetDatabaseOperation(char operationChar)
        {
            // Itera sobre os valores do enum
            foreach (DatabaseOperation operation in System.Enum.GetValues(typeof(DatabaseOperation)))
            {
                if ((char)operation == operationChar)
                {
                    return operation;
                }
            }

            // Retorna null se não encontrar uma correspondência
            return null;
        }

        private async Task<List<TableProperty>> GetPropertiesOriginTable(string table, Database database)
        {
            var keyCache = await _redisCache.GetAsync($"Property.{database}.{table}");

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

            using (var connection = new SqlConnection(_property.GetProperty(Property.CONNECTIONSTRINGSQLSERVER)))
            {
                await connection.OpenAsync();
                var operation = await connection.QueryAsync<TableProperty>(query);

                foreach (var item in operation)
                {
                    primaryKeys.Add(item);
                }
            }

            await _redisCache.SetAsync($"Property.{database}.{table}", JsonConvert.SerializeObject(primaryKeys));

            return primaryKeys;
        }

        public async Task<IEnumerable<ForeignKeyInfo>> GetReferencesTables(string table = "")
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
                sys.columns AS cr ON fkc.referenced_object_id = cr.object_id AND fkc.referenced_column_id = cr.column_id"
            ;

            if (!string.IsNullOrEmpty(table))
            {
                query += $" WHERE tp.name LIKE '{table}' ";
            }

            query += @" ORDER BY s1.name, tp.name, fk.name;";

            try
            {
                using (var connection = new SqlConnection(_property.GetProperty(Property.CONNECTIONSTRINGSQLSERVER)))
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

        public async Task<bool> ExistsInDatabase(IEnumerable<KeyPar> keyPars)
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

                var keyCache = await _redisCache.GetAsync($"{keyPar.Table}.{ids}.{valuesId}");

                if (keyCache.HasValue)
                {
                    exists = true;
                }
                else
                {
                    // Caso contrário, vai até o banco de dados para fazer a verificação
                    string query = @$"SELECT 1 FROM {_schemaPostgres}.{keyPar.Table} WHERE {whereClause} LIMIT 1";

                    try
                    {
                        using (var connection = new NpgsqlConnection(_property.GetProperty(Property.CONNECTIONSTRINGPOSTGRES)))
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
                                await _redisCache.SetAsync($"{keyPar.Table}.{ids}.{valuesId}", true);
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

    }
}
