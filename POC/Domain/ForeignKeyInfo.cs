namespace POC.Domain
{
    public class ForeignKeyInfo
    {
        public string ForeignKeyName { get; set; }
        public string ParentTable { get; set; }
        public string ParentColumn { get; set; }
        public string ReferencedTable { get; set; }
        public string ReferencedColumn { get; set; }
        public string ParentSchema { get; set; }
        public string ReferencedSchema { get; set; }
    }
}
