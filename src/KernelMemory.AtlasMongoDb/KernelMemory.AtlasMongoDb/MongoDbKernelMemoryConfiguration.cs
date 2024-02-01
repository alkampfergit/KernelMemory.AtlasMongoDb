using MongoDB.Driver;

namespace KernelMemory.AtlasMongoDb
{
    public class MongoDbKernelMemoryConfiguration
    {
        public string ConnectionString { get; set; }

        public string DatabaseName { get; set; }

        public MongoDbKernelMemoryConfiguration()
        {
            DatabaseName = "KernelMemory";
        }

        public MongoDbKernelMemoryConfiguration WithConnection(string mongoConnection)
        {
            ConnectionString = mongoConnection;
            return this;
        }

        public MongoDbKernelMemoryConfiguration WithDatabaseName(string databaseName)
        {
            DatabaseName = databaseName;
            return this;
        }

        private IMongoClient _client;

        internal IMongoDatabase GetDatabase()
        {
            if (_client == null)
            {
                var builder = new MongoUrlBuilder(ConnectionString);
                if (!string.IsNullOrEmpty(DatabaseName))
                {
                    builder.DatabaseName = DatabaseName;
                }
                _client = new MongoClient(builder.ToMongoUrl());
            }

            return _client.GetDatabase(DatabaseName);
        }
    }
}
