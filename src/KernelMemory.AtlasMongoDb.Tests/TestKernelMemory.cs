using Microsoft.KernelMemory;
using Microsoft.KernelMemory.AI;
using Microsoft.KernelMemory.MemoryStorage;
using MongoDB.Bson;
using MongoDB.Driver;
using Moq;

namespace KernelMemory.AtlasMongoDb.Tests;

public sealed class Experiments : IDisposable
{
    private const string ConnectionString = "";

    void IDisposable.Dispose()
    {
        var client = new MongoClient(ConnectionString);
        var utils = new AtlasSearchUtils(ConnectionString, "testgm");
        utils.DeletedIndicesAsync("default").Wait();
        client.DropDatabase("testgm");
    }

    [Fact]
    public async Task GetIndex()
    {
        var client = new MongoClient(ConnectionString);
        var database = client.GetDatabase("testgm");

        var atlasUtils = new AtlasSearchUtils(ConnectionString, "testgm");

        var collectionName = "default";
        var collectionExists = database
            .ListCollectionNames()
            .ToEnumerable()
            .ToHashSet()
            .Contains(collectionName);

        if (!collectionExists)
        {
            database.CreateCollection(collectionName);
        }

        var collection = database.GetCollection<BsonDocument>("default");

        collection.InsertOne(new BsonDocument("name", "test"));
        var searchIndex = await atlasUtils.GetIndexInfoAsync(collectionName);
        Assert.False(searchIndex.Exists);
        var indexStatus  = await atlasUtils.CreateIndexAsync(collectionName, 725);

        while (indexStatus.Status != "ready")
        {
            await Task.Delay(1000);
            indexStatus = await atlasUtils.GetIndexInfoAsync(collectionName);
        }
    }

    [Fact]
    public async Task Verify_init_index_smoke() 
    {
        MongoDbKernelMemoryConfiguration mongoDbKernelMemoryConfiguration = new();
        mongoDbKernelMemoryConfiguration.WithConnection(ConnectionString);
        mongoDbKernelMemoryConfiguration.WithDatabaseName("testgm");

        //create an instance of ITextEmbeddingGenerator with moq
        var mock = new Mock<ITextEmbeddingGenerator>();

        MongoDbVectorMemory sut = new(mongoDbKernelMemoryConfiguration, mock.Object);
        await sut.CreateIndexAsync("default", 725, CancellationToken.None);

        //verify that the index is created
        var indexes = await sut.GetIndexesAsync(CancellationToken.None);
        Assert.Contains("default", indexes);

        var record = new MemoryRecord();
        record.Id = "TEST";
        record.Tags = new TagCollection
        {
            { "category", new List<string?> { "Fantasy", "ScienceFiction" } },
            { "owner", new List<string?> { "alkampfer" } }
        };

        record.Payload = new Dictionary<string, object>()
        {
            ["citations"] = "bla bla bla bla",
            ["title"] = "This is a nice test",
        };

        //generate a random array of 725 element of type float
        var vector = new float[725];
        for (int i = 0; i < 725; i++)
        {
            vector[i] = (float) new Random().NextDouble();
        }

        record.Vector = vector;

        await sut.UpsertAsync("default", record, CancellationToken.None);

        var filters = new List<MemoryFilter>();
        var filter = new MemoryFilter();
        filter.ByTag("category", "Fantasy");
        filters.Add(filter);

        var l = new List<MemoryRecord>();

        await Task.Delay(2000);

        await foreach (var item in sut.GetListAsync("default", filters))
        {
            l.Add(item);
        }
        Assert.Single(l);
        Assert.Equal("TEST", l[0].Id);
        Assert.Equal("Fantasy", l[0].Tags["category"][0]);

        await sut.DeleteAsync("default", record, CancellationToken.None);

        l = new List<MemoryRecord>();
        await foreach (var item in sut.GetListAsync("default", filters))
        {
            l.Add(item);
        }
        Assert.Empty(l);

        await sut.DeleteIndexAsync("default", CancellationToken.None);
    }
}