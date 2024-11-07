using Microsoft.Extensions.Configuration;
using POC.Interfaces;

namespace POC.Services
{
    public class PropertyService : IPropertyService
    {
        private IConfigurationRoot _config;

        public PropertyService()
        {
            var builder = new ConfigurationBuilder();
            builder.SetBasePath(Directory.GetCurrentDirectory())
               .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true);

            _config = builder.Build();
        }

        public string GetProperty(string propertyName)
        {
            if (!string.IsNullOrEmpty(propertyName))
            {
                return _config[propertyName];
            }

            return string.Empty;
        }

        public List<string> GetListProperty(string propertyName)
        {
            if (!string.IsNullOrEmpty(propertyName))
            {
                var section = _config.GetSection(propertyName);
                if (section.Exists())
                {
                    return section.GetChildren().Select(x => x.Value).ToList();
                }
            }

            return new List<string>();
        }
    }
}
