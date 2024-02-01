using KernelMemory.AtlasMongoDb.Helpers;
using Microsoft.Extensions.Logging;
using Microsoft.KernelMemory;
using Microsoft.KernelMemory.AI;
using Microsoft.KernelMemory.Diagnostics;
using Microsoft.KernelMemory.MemoryStorage;
using MongoDB.Bson;
using MongoDB.Driver;
using System.Collections.Immutable;
using System.Runtime.CompilerServices;

namespace KernelMemory.AtlasMongoDb
{
    public class MongoDbVectorMemory : MongoDbKernelMemoryBaseStorage, IMemoryDb
    {
        private readonly ITextEmbeddingGenerator _embeddingGenerator;
        private readonly ILogger<MongoDbVectorMemory> _log;
        private readonly AtlasSearchHelper _utils;
        private const string ConnectionNamePrefix = "_ix_";

        /// <summary>
        /// Create a new instance of MongoDbVectorMemory from configuration
        /// </summary>
        /// <param name="config">Cnofiguration</param>
        /// <param name="embeddingGenerator">Embedding generator</param>
        /// <param name="log">Application logger</param>
        public MongoDbVectorMemory(
            MongoDbKernelMemoryConfiguration config,
            ITextEmbeddingGenerator embeddingGenerator,
            ILogger<MongoDbVectorMemory>? log = null) : base(config)
        {
            _embeddingGenerator = embeddingGenerator ?? throw new ArgumentNullException(nameof(embeddingGenerator));
            _log = log ?? DefaultLogger<MongoDbVectorMemory>.Instance;

            _utils = new AtlasSearchHelper(_config.ConnectionString, _config.DatabaseName);
        }

        private IMongoCollection<BsonDocument> GetCollectionFromIndexName(string indexName)
        {
            var collectionName = GetCollectionName(indexName);
            return GetCollection(collectionName);
        }

        private static string GetCollectionName(string indexName)
        {
            return $"{ConnectionNamePrefix}{indexName}";
        }

        public async Task CreateIndexAsync(string index, int vectorSize, CancellationToken cancellationToken = default)
        {
            //Index name is the name of the collection, so we need to understand if the collection exists
            var collectionName = GetCollectionName(index);
            await _utils.CreateIndexAsync(collectionName, vectorSize);
            await _utils.WaitForIndexToBeReady(collectionName, 60);
        }

        public async Task DeleteIndexAsync(string index, CancellationToken cancellationToken = default)
        {
            var collectionName = GetCollectionName(index);
            await _utils.DeletedIndicesAsync(collectionName);
            await _db.DropCollectionAsync(collectionName, cancellationToken);
        }

        public async Task<IEnumerable<string>> GetIndexesAsync(CancellationToken cancellationToken = default)
        {
            var cursor = await _db.ListCollectionNamesAsync(cancellationToken: cancellationToken);

            return cursor.ToEnumerable(cancellationToken: cancellationToken)
                .Where(x => x.StartsWith(ConnectionNamePrefix))
                .Select(x => x.Replace(ConnectionNamePrefix, ""))
                .ToImmutableArray();
        }

        public Task DeleteAsync(string index, MemoryRecord record, CancellationToken cancellationToken = default)
        {
            var collection = GetCollectionFromIndexName(index);
            return collection.DeleteOneAsync(x => x["_id"] == record.Id, cancellationToken: cancellationToken);
        }

        public async IAsyncEnumerable<MemoryRecord> GetListAsync(
            string index,
            ICollection<MemoryFilter>? filters = null,
            int limit = 1,
            bool withEmbeddings = false,
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            //need to create a search query and execute it
            var compound = new BsonDocument();
            var conditions = new BsonArray();
            foreach (var filter in filters ?? Array.Empty<MemoryFilter>())
            {
                var thisFilter = filter.GetFilters();
                foreach (var (key, value) in thisFilter)
                {
                    var condition = new BsonDocument
                    {
                        ["text"] = new BsonDocument
                            {
                                { "query", value},
                                { "path", $"tg_{key}" }
                            }
                    };
                    conditions.Add(condition);
                }
            }
            compound["must"] = conditions;
            var pipeline = new BsonDocument[]
            {
            new() {
                {
                    "$search", new BsonDocument
                    {
                        { "index", "searchix__ix_default" },
                        { "compound", compound }
                    }
                }
            },
            };

            var collection = GetCollectionFromIndexName(index);

            var cursor = await collection.AggregateAsync<BsonDocument>(pipeline, cancellationToken: cancellationToken);
            foreach (var document in cursor.ToEnumerable(cancellationToken: cancellationToken))
            {
                yield return FromBsonDocument(document);
            }
        }

        private static MemoryRecord FromBsonDocument(BsonDocument doc)
        {
            var record = new MemoryRecord
            {
                Id = doc["_id"].AsString,
                Vector = doc["embedding"].AsBsonArray.Select(x => (float)x.AsDouble).ToArray()
            };
            foreach (var element in doc.Elements)
            {
                if (element.Name.StartsWith("pl_"))
                {
                    var key = element.Name.Replace("pl_", "");
                    record.Payload[key] = element.Value.AsString;
                }
                else if (element.Name.StartsWith("tg_"))
                {
                    var key = element.Name.Replace("tg_", "");
                    record.Tags[key] = element.Value.AsBsonArray.Select(x => (string?)x.AsString).ToList();
                }
            }
            return record;
        }

        public IAsyncEnumerable<(MemoryRecord, double)> GetSimilarListAsync(string index, string text, ICollection<MemoryFilter>? filters = null, double minRelevance = 0, int limit = 1, bool withEmbeddings = false, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public Task<string> UpsertAsync(string index, MemoryRecord record, CancellationToken cancellationToken = default)
        {
            var collection = GetCollectionFromIndexName(index);
            BsonDocument bsonDocument = new()
            {
                ["_id"] = record.Id,
                ["embedding"] = new BsonArray(record.Vector.Data.Span.ToArray())
            };
            foreach (var (key, value) in record.Payload)
            {
                bsonDocument[$"pl_{key}"] = value?.ToString();
            }

            foreach (var (key, value) in record.Tags)
            {
                bsonDocument[$"tg_{key}"] = new BsonArray(value);
            }

            collection.InsertOne(bsonDocument, cancellationToken: cancellationToken);
            return Task.FromResult(bsonDocument["_id"].AsString);
        }
    }
}
