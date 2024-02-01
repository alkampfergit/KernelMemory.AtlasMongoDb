using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Driver.GridFS;
using System.Collections.Generic;

namespace KernelMemory.AtlasMongoDb
{
    public class MongoDbKernelMemoryBaseStorage
    {
        protected readonly IMongoDatabase _db;
        protected readonly GridFSBucket _bucket;
        protected readonly MongoDbKernelMemoryConfiguration _config;

        /// <summary>
        /// Keys are mongo collection of T but since we do not know T we cache them
        /// as simple object then cast to the correct value.
        /// </summary>
        protected Dictionary<string, object> _collections = new();

        public MongoDbKernelMemoryBaseStorage(MongoDbKernelMemoryConfiguration config)
        {
            _db = config.GetDatabase();
            _bucket = new GridFSBucket(_db);
            _config = config;
        }

        protected IMongoCollection<BsonDocument> GetCollection(string collectionName)
        {
            return GetCollection<BsonDocument>(collectionName);
        }

        protected IMongoCollection<T> GetCollection<T>(string collectionName)
        {
            if (!_collections.ContainsKey(collectionName))
            {
                _collections[collectionName] = _db.GetCollection<T>(collectionName);
            }

            return (IMongoCollection<T>)_collections[collectionName];
        }
    }
}