//using System.Collections.Immutable;
//using System.Runtime.CompilerServices;
//using System.Text.RegularExpressions;
//using Microsoft.KernelMemory;
//using Microsoft.KernelMemory.MemoryStorage;
//using MongoDB.Bson;
//using MongoDB.Driver;

//namespace KernelMemory.AtlasMongoDb.Tests;

//public class AtlasMemory : IMemoryDb
//{
//    private readonly MongoClient _client;
//    private readonly IMongoDatabase _db;
//    private readonly AtlasSearchUtils _utils;

//    private const string ConnectionNamePrefix = "_ix_";

//    public AtlasMemory(string connectionString, string databaseName)
//    {
//        _client = new MongoClient(connectionString);
//        _db = _client.GetDatabase(databaseName);
//        _utils = new AtlasSearchUtils(connectionString, databaseName);
//    }

//    private IMongoCollection<BsonDocument> GetCollection(string indexName)
//    {
//        var collectionName = GetCollectionName(indexName);
//        return _db.GetCollection<BsonDocument>(collectionName);
//    }

//    private static string GetCollectionName(string indexName)
//    {
//        return $"{ConnectionNamePrefix}{indexName}";
//    }

//    public async Task CreateIndexAsync(string index, int vectorSize, CancellationToken cancellationToken = default)
//    {
//        //Index name is the name of the collection, so we need to understand if the collection exists
//        var collectionName = GetCollectionName(index);
//        await _utils.CreateIndexAsync(collectionName, vectorSize);
//        await _utils.WaitForIndexToBeReady(collectionName, 60);
//    }

//    public async Task DeleteIndexAsync(string index, CancellationToken cancellationToken = default)
//    {
//        var collectionName = GetCollectionName(index);
//        await _utils.DeletedIndicesAsync(collectionName);
//        await _db.DropCollectionAsync(collectionName);
//    }

//    public async Task<IEnumerable<string>> GetIndexesAsync(CancellationToken cancellationToken = default)
//    {
//        var cursor = await _db.ListCollectionNamesAsync();

//        return cursor.ToEnumerable()
//            .Where(x => x.StartsWith(ConnectionNamePrefix))
//            .Select(x => x.Replace(ConnectionNamePrefix, ""))
//            .ToImmutableArray();
//    }

//    public Task DeleteAsync(string index, MemoryRecord record, CancellationToken cancellationToken = default)
//    {
//        var collection = GetCollection(index);
//        return collection.DeleteOneAsync(x => x["_id"] == record.Id);
//    }

//    public async IAsyncEnumerable<MemoryRecord> GetListAsync(
//        string index,
//        ICollection<MemoryFilter>? filters = null,
//        int limit = 1,
//        bool withEmbeddings = false,
//        [EnumeratorCancellation] CancellationToken cancellationToken = default)
//    {
//        //need to create a search query and execute it
//        var compound = new BsonDocument();
//        var conditions = new BsonArray();
//        foreach (var filter in filters ?? Array.Empty<MemoryFilter>())
//        {
//            var thisFilter = filter.GetFilters();
//            foreach (var (key, value) in thisFilter)
//            {
//                var condition = new BsonDocument();
//                condition["text"] = new BsonDocument
//                {
//                    { "query", value},
//                    { "path", $"tg_{key}" }
//                };
//                conditions.Add(condition);
//            }
//        }
//        compound["must"] = conditions;
//        var pipeline = new BsonDocument[]
//        {
//            new BsonDocument
//            {
//                {
//                    "$search", new BsonDocument
//                    {
//                        { "index", "searchix__ix_default" },
//                        { "compound", compound }
//                    }
//                }
//            },
//        };

//        var collection = GetCollection(index);

//        var cursor = await collection.AggregateAsync<BsonDocument>(pipeline);
//        foreach (var document in cursor.ToEnumerable())
//        {      
//            yield return FromBsonDocument(document);
//        }
//    }

//    private MemoryRecord FromBsonDocument(BsonDocument doc)
//    {
//        var record = new MemoryRecord();
//        record.Id = doc["_id"].AsString;
//        record.Vector = doc["embedding"].AsBsonArray.Select(x => (float)x.AsDouble).ToArray();
//        foreach (var element in doc.Elements)
//        {
//            if (element.Name.StartsWith("pl_"))
//            {
//                var key = element.Name.Replace("pl_", "");
//                record.Payload[key] = element.Value.AsString;
//            }
//            else if (element.Name.StartsWith("tg_"))
//            {
//                var key = element.Name.Replace("tg_", "");
//                record.Tags[key] = element.Value.AsBsonArray.Select(x => (string?) x.AsString).ToList();
//            }
//        }
//        return record;
//    }

//    public IAsyncEnumerable<(MemoryRecord, double)> GetSimilarListAsync(string index, string text, ICollection<MemoryFilter>? filters = null, double minRelevance = 0, int limit = 1, bool withEmbeddings = false, CancellationToken cancellationToken = default)
//    {
//        throw new NotImplementedException();
//    }

//    public Task<string> UpsertAsync(string index, MemoryRecord record, CancellationToken cancellationToken = default)
//    {
//        var collection = GetCollection(index);
//        var bsonDocument = new BsonDocument();
//        bsonDocument["_id"] = record.Id;
//        bsonDocument["embedding"] = new BsonArray(record.Vector.Data.Span.ToArray());
//        foreach (var (key, value) in record.Payload)
//        {
//            bsonDocument[$"pl_{key}"] = value?.ToString();
//        }

//        foreach (var (key, value) in record.Tags)
//        {
//            bsonDocument[$"tg_{key}"] = new BsonArray(value);
//        }

//        collection.InsertOne(bsonDocument);
//        return Task.FromResult(bsonDocument["_id"].AsString);
//    }
//}
