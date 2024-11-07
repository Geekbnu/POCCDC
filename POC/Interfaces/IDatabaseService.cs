using POC.Domain;
using POC.Enum;

namespace POC.Interfaces
{
    public interface IDatabaseService
    {
        Task ExecuteOperationAsync(string table, dynamic data, DatabaseOperation? operation);

        DatabaseOperation? GetDatabaseOperation(char operationChar);

        Task<bool> ExistsInDatabase(IEnumerable<KeyPar> keyPars);

        Task<IEnumerable<ForeignKeyInfo>> GetReferencesTables(string table = "");
    }
}
