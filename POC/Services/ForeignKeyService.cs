using POC.Interfaces;

namespace POC.Domain
{
    public class ForeignKeyService : IForeignKeyService
    {
        private IDatabaseService _databaseService;

        public ForeignKeyService(IDatabaseService databaseService)
        {
            _databaseService = databaseService;
        }

        public async Task<bool> DependencyEngineSync(string table, dynamic data, IEnumerable<ForeignKeyInfo> foreignKeys)
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
                    return await _databaseService.ExistsInDatabase(keyPar);
                }
            }
            else
            {
                return true;
            }
        }
    }
}
