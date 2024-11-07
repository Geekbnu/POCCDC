using POC.Domain;

namespace POC.Interfaces
{
    public interface IForeignKeyService
    {
        Task<bool> DependencyEngineSync(string table, dynamic data, IEnumerable<ForeignKeyInfo> foreignKeys);
    }
}
