namespace POC.Interfaces
{
    public interface IPropertyService
    {
        string GetProperty(string propertyName);
        List<string> GetListProperty(string propertyName);
    }
}