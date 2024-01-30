using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Driver;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace KernelMemory.AtlasMongoDb.Tests;

/// <summary>
/// <para>Wrapper for ATLAS search indexes stuff</para>
/// <para>
/// <ul>
/// <li>https://www.mongodb.com/docs/v7.0/reference/method/db.collection.createSearchIndex/</li>
/// <li>Normalizer (end of the page) https://www.mongodb.com/docs/atlas/atlas-search/analyzers/</li>
/// </ul>
/// </para>
/// </summary>
public class AtlasSearchUtils
{
    private IMongoDatabase _db;

    public AtlasSearchUtils(string connection, string dbName)
    {
        var client = new MongoClient(connection);
        _db = client.GetDatabase(dbName);
    }

    private string GetIndexName(string collectionName)  => $"searchix_{collectionName}";

    public async Task<IndexInfo> GetIndexInfoAsync(string collectionName)
    {
        var collection = _db.GetCollection<BsonDocument>(collectionName);
        var pipeline = new BsonDocument[]
        {
            new BsonDocument
            {
                {
                    "$listSearchIndexes",
                    new BsonDocument
                    {
                        { "name", GetIndexName(collectionName) }
                    }
                }
            }
        };

        //if collection does not exists, index does not exists
        if (!await CollectionExists(collectionName))
        {
            //index does not exists because collection does not exists
            return _falseIndexInfo;
        }

        var result = await collection.AggregateAsync<BsonDocument>(pipeline);
        var allIndexInfo = await result.ToListAsync();

        //Verify if we have information about the index.
        if (allIndexInfo.Count == 0)
        {
            return _falseIndexInfo;
        }

        if (allIndexInfo.Count > 1)
        {
            throw new Exception("We have too many atlas search index for the collection: " + string.Join(",", allIndexInfo.Select(i => i["name"].AsString)));
        }

        var indexInfo = allIndexInfo[0];

        var latestDefinition = indexInfo["latestDefinition"] as BsonDocument;
        var mapping = latestDefinition["mappings"] as BsonDocument;

        var deserialziedMapping = BsonSerializer.Deserialize<AtlasMapping>(mapping);

        return new IndexInfo(true, indexInfo["status"].AsString.ToLower(), deserialziedMapping);
    }

    /// <summary>
    /// Create an ATLAS index and return the id of the index. It also wait for the index to be
    /// ready, and create the collection if needed.
    /// </summary>
    /// <param name="collectionName"></param>
    /// <returns></returns>
    public async Task<IndexInfo> CreateIndexAsync(string collectionName, int embeddingDimension)
    {
        //I need to be able to create index even if collection does not exists
        //if collection does not exists, create collection and index
        //if collection does not exists, index does not exists
        if (!await CollectionExists(collectionName))
        {
            //index does not exists because collection does not exists
            await _db.CreateCollectionAsync(collectionName);
        }

        var status = await GetIndexInfoAsync(collectionName);
        if (status.Exists)
        {
            return status;
        }

        //now I can create the index.
        var command = CreateCreationCommand(collectionName, embeddingDimension);
        var result = await _db.RunCommandAsync<BsonDocument>(command);
        var creationResult = result["indexesCreated"] as BsonArray;
        if (creationResult.Count == 0)
        {
            return _falseIndexInfo;
        }

        return await GetIndexInfoAsync(collectionName);
    }

    public async Task DeletedIndicesAsync(string collectionName)
    {
        var pipeline = new BsonDocument[]
        {
            new BsonDocument
            {
                {
                    "$listSearchIndexes",
                    new BsonDocument
                    {

                    }
                }
            }
        };

        var collection = _db.GetCollection<BsonDocument>(collectionName);
        var result = await collection.AggregateAsync<BsonDocument>(pipeline);
        var allIndexInfo = await result.ToListAsync();

        //for each index we need to delete the indices.
        foreach (var index in allIndexInfo)
        {
            var id = index["id"].AsString;

            var command = new BsonDocument
            {
                { "dropSearchIndex", collectionName },
                { "id", id }
            };
            await _db.RunCommandAsync<BsonDocument>(command);
        }
    }

    public async Task WaitForIndexToBeReady(string collectionName, int secondsToWait)
    {
        //cycle for max 10 seconds to wait for index to be ready
        var maxWait = DateTime.UtcNow.AddSeconds(secondsToWait);
        while (DateTime.UtcNow < maxWait)
        {
            var indexInfo = await GetIndexInfoAsync(collectionName);
            if (indexInfo.Exists && indexInfo.Status != "pending")
            {
                return;
            }

            await Task.Delay(100);
        }
    }

    /// <summary>
    /// https://www.mongodb.com/docs/upcoming/reference/command/createSearchIndexes/
    /// </summary>
    /// <param name="collectionName"></param>
    /// <returns></returns>
    public BsonDocument CreateCreationCommand(string collectionName, int embeddingDimension)
    {
        return new BsonDocument
        {
            { "createSearchIndexes", collectionName },
                {
                    "indexes", new BsonArray
                    {
                        new BsonDocument
                        {
                            { "name", GetIndexName(collectionName) },
                            { "definition", new BsonDocument
                            {
                                { "mappings", GetMappings(Array.Empty<string>(), embeddingDimension) },
                                { "analyzers" , GetAnalyzersList() },
                            }
                        }
                    }
                }
            }
        };
    }

    /// <summary>
    /// https://www.mongodb.com/docs/manual/reference/command/updateSearchIndex/
    /// </summary>
    /// <param name="collectionName"></param>
    /// <param name="stringProperties"></param>
    /// <returns></returns>
    public BsonDocument UpdateIndexCommand(string collectionName, IEnumerable<string> stringProperties, int numberOfDimensions)
    {
        return new BsonDocument
        {
            { "updateSearchIndex", collectionName },
            { "name", GetIndexName(collectionName) },
            { "definition", new BsonDocument
                {
                    { "mappings", GetMappings(stringProperties, numberOfDimensions) },
                    { "analyzers" , GetAnalyzersList() },
                }
            }
        };
    }

    /// <summary>
    /// https://www.mongodb.com/docs/atlas/atlas-search/define-field-mappings/#std-label-fts-field-mappings
    ///
    /// Copy from src\Intranet\Jarvis.Common.Shared\Search\Es\E7\OmniSearchIndexMapper7.cs <see cref="OmniSearchIndexMapper7"/>
    /// </summary>
    /// <param name="stringProperties">Since we do not have a real dynamic mapping we need to update the mapping when
    /// needed.</param>
    /// <returns></returns>
    internal BsonDocument GetMappings(IEnumerable<string> stringProperties, int numberOfDimensions)
    {
        var mappings = new BsonDocument();

        mappings["dynamic"] = true;

        var fields = new BsonDocument();
        mappings["fields"] = fields;

        fields["embedding"] =  new BsonDocument
        {
            { "type", "knnVector" },
            { "dimensions", numberOfDimensions},
            { "similarity", "dotProduct"}
        };

        // fields["tg_*"] = new BsonDocument()
        // {
        //     { "type", "string" },
        // };
 

        // fields[OmniSearchItem.TitleFieldName] = new BsonDocument()
        //     {
        //         { "type", "string" },
        //         { "analyzer", OmniSearchIndexMapper7.MainSearchAnalyzer },
        //     };

        // fields[OmniSearchItem.FullTextFieldName] = new BsonDocument()
        //     {
        //         { "type", "string" },
        //         { "analyzer", "lucene.keyword" },
        //     };

        // fields[OmniSearchItem.ElasticTypeFieldName] = new BsonDocument()
        // {
        //     { "type", "string" },
        //     { "analyzer", OmniSearchIndexMapper7.MainSearchAnalyzer },
        // };

        // fields[OmniSearchItem.SecurityTokensFieldName] = new BsonDocument()
        // {
        //     { "type", "string" },
        //     { "analyzer", OmniSearchIndexMapper7.MainSearchAnalyzer },
        // };

        // fields[OmniSearchItem.DeletedFieldName] = new BsonDocument()
        // {
        //     { "type", "boolean" },
        // };

        // fields[OmniSearchItem.SecondarySecurityTokensFieldName] = new BsonDocument()
        // {
        //     { "type", "string" },
        //     { "analyzer", OmniSearchIndexMapper7.MainSearchAnalyzer },
        // };

        // fields[OmniSearchItem.MainSearchFieldName] = new BsonDocument()
        //         {
        //             { "type", "string" },
        //             { "analyzer", OmniSearchIndexMapper7.MainSearchAnalyzer },
        //             { "multi" , new BsonDocument()
        //                 {
        //                     //multi is the equivalent of Elasticsearch fields.
        //                     //https://www.mongodb.com/docs/atlas/atlas-search/path-construction/#std-label-ref-path
        //                     {
        //                         OmniSearchIndexMapper7.EdgeNgramFieldSuffix, new BsonDocument()
        //                         {
        //                             { "type", "string" },
        //                             { "analyzer", OmniSearchIndexMapper7.AnalyzerNgramStandard },
        //                         }
        //                     },
        //                     {
        //                         //https://www.mongodb.com/developer/products/atlas/atlas-search-exact-match/
        //                         //This link for exact query in atlas mongodb.
        //                         OmniSearchConstants.AnalyzerFieldRawSuffix, new BsonDocument()
        //                         {
        //                             { "type", "string" },
        //                             { "analyzer", "lucene.keyword" },
        //                         }
        //                     },
        //                     {
        //                         OmniSearchConstants.AnalyzerFieldNotAnalyzedLowercase, new BsonDocument()
        //                         {
        //                             { "type", "string" },
        //                             { "analyzer", OmniSearchIndexMapper7.NotAnalyzedLowercase },
        //                         }
        //                     },
        //                     {
        //                         OmniSearchConstants.AnalyzerFieldWhitespaceSuffix, new BsonDocument()
        //                         {
        //                             { "type", "string" },
        //                             { "analyzer", OmniSearchIndexMapper7.NonAsciiAndSpaceSplittedLowerCaseAnalyzerName },
        //                         }
        //                     },
        //                     {
        //                         "std", new BsonDocument()
        //                         {
        //                             { "type", "string" },
        //                             { "analyzer", "lucene.standard" },
        //                         }
        //                     }
        //                 }
        //             }
        //         };

        // foreach (var stringProperty in stringProperties)
        // {
        //     fields[stringProperty] = new BsonDocument()
        //     {
        //         { "type", "string" },
        //         { "analyzer", OmniSearchIndexMapper7.StringPropertiesAnalyzer },
        //         { "multi" , new BsonDocument()
        //                 {
        //                     {
        //                         OmniSearchConstants.AnalyzerFieldNotAnalyzedLowercase, new BsonDocument()
        //                         {
        //                             { "type", "string" },
        //                             { "analyzer", OmniSearchIndexMapper7.NotAnalyzedLowercase },
        //                         }
        //                     },
        //                      {
        //                         OmniSearchConstants.AnalyzerFieldRawSuffix, new BsonDocument()
        //                         {
        //                             { "type", "string" },
        //                             { "analyzer", "lucene.keyword"},
        //                         }
        //                     },
        //                     {
        //                         OmniSearchConstants.AnalyzerFieldWhitespaceSuffix, new BsonDocument()
        //                         {
        //                             { "type", "string" },
        //                             { "analyzer", OmniSearchIndexMapper7.NonAsciiAndSpaceSplittedLowerCaseAnalyzerName},
        //                         }
        //                     },
        //             }
        //         }
        //     };
        // }

        return mappings;
    }

    /// <summary>
    /// Verify CreateIndexSettingsAnalysisDescriptor for mapper 7
    /// </summary>
    /// <exception cref="NotImplementedException"></exception>
    internal BsonArray GetAnalyzersList()
    {
        var analyzers = new BsonArray();

        //https://www.mongodb.com/docs/atlas/atlas-search/analyzers/custom/#std-label-custom-analyzers
        //https://www.mongodb.com/docs/atlas/atlas-search/analyzers/token-filters/
        //[0]
        // analyzers.Add(new BsonDocument
        // {
        //     { "name", OmniSearchIndexMapper7.NotAnalyzedLowercase },
        //     { "tokenizer", new BsonDocument
        //     {
        //             { "type", "keyword" },
        //         }
        //     },
        //     {
        //         "tokenFilters", new BsonArray
        //         {
        //             new BsonDocument
        //             {
        //                 { "type", "lowercase" },
        //             },
        //             new BsonDocument
        //             {
        //                 { "type", "asciiFolding" },
        //             }
        //         }
        //     }
        // });

        // //[1]
        // analyzers.Add(new BsonDocument
        // {
        //     { "name", OmniSearchIndexMapper7.MainSearchAnalyzer },
        //     { "tokenizer", new BsonDocument
        //         {
        //             { "type", "standard" },
        //         }
        //     },
        //     {
        //         "tokenFilters", new BsonArray
        //         {
        //             new BsonDocument
        //             {
        //                 { "type", "lowercase" },
        //             },
        //             new BsonDocument
        //             {
        //                 { "type", "asciiFolding" },
        //             }
        //         }
        //     }
        // });

        // //[2]
        // analyzers.Add(new BsonDocument
        // {
        //     { "name", OmniSearchIndexMapper7.StringPropertiesAnalyzer },
        //     { "tokenizer", new BsonDocument
        //     {
        //             { "type", "standard" },
        //         }
        //     },
        //     {
        //         "tokenFilters", new BsonArray
        //         {
        //             new BsonDocument
        //             {
        //                 { "type", "lowercase" },
        //             },
        //             new BsonDocument
        //             {
        //                 { "type", "asciiFolding" },
        //             }
        //         }
        //     }
        // });

        // //[3]
        // analyzers.Add(new BsonDocument
        // {
        //     { "name", OmniSearchIndexMapper7.AnalyzerNgramStandard },
        //     { "tokenizer", new BsonDocument
        //     {
        //             { "type", "standard" },
        //         }
        //     },
        //     {
        //         "tokenFilters", new BsonArray
        //         {
        //             new BsonDocument
        //             {
        //                 { "type", "lowercase" },
        //             },
        //             new BsonDocument
        //             {
        //                 { "type", "asciiFolding" },
        //             }
        //             ,
        //             new BsonDocument
        //             {
        //                 { "type", "edgeGram" },
        //                 { "minGram", 3 },
        //                 { "maxGram", 15 },
        //             }
        //         }
        //     }
        // });

        // //[4]
        // analyzers.Add(new BsonDocument
        // {
        //     { "name", OmniSearchIndexMapper7.AnalyzerTrigramStandard },
        //     { "tokenizer", new BsonDocument
        //     {
        //             { "type", "standard" },
        //         }
        //     },
        //     {
        //         "tokenFilters", new BsonArray
        //         {
        //             new BsonDocument
        //             {
        //                 { "type", "lowercase" },
        //             },
        //             new BsonDocument
        //             {
        //                 { "type", "nGram" },
        //                 { "minGram", 3 },
        //                 { "maxGram", 3 },
        //             }
        //         }
        //     }
        // });

        // analyzers.Add(new BsonDocument
        // {
        //     { "name", OmniSearchIndexMapper7.AnalyzerPropertiesIndexTime },
        //     { "tokenizer", new BsonDocument
        //     {
        //             { "type", "standard" },
        //         }
        //     },
        //     {
        //         "tokenFilters", new BsonArray
        //         {
        //             new BsonDocument
        //             {
        //                 { "type", "lowercase" },
        //             }
        //         }
        //     }
        // });

        // analyzers.Add(new BsonDocument
        // {
        //     { "name", OmniSearchIndexMapper7.NonAsciiAndSpaceSplittedLowerCaseAnalyzerName },
        //     { "tokenizer", new BsonDocument
        //         {
        //             { "type", "regexSplit" },
        //             { "pattern" , "(?<=[^\\p{ASCII}]|\\s)"},
        //             //case insensitive and multiline seems not to be presetn , also group(-1) is not possible to specify
        //         }
        //     },
        //     {
        //         "tokenFilters", new BsonArray
        //         {
        //             new BsonDocument
        //             {
        //                 { "type", "lowercase" },
        //             }
        //         }
        //     }
        // });

        return analyzers;
    }

    public async Task<bool> CollectionExists(string connectionName)
    {
        var filter = new BsonDocument("name", connectionName);
        var collections = await _db.ListCollectionsAsync(new ListCollectionsOptions { Filter = filter });
        return collections.Any();
    }

    private ConcurrentDictionary<string, HashSet<string>> _mappings = new ConcurrentDictionary<string, HashSet<string>>();

    public async Task EnsureStringMappingAsync(string collectionName, HashSet<string> properties, int numberOfDimensions)
    {
        //we need to check if we have a cached mapping, we do not want to query the index each time this operation
        //must be super fast.
        if (!_mappings.TryGetValue(collectionName, out var cachedProperties))
        {
            var status = await GetIndexInfoAsync(collectionName);
            cachedProperties = status.Mapping.Fields
                .Where(m => m.Key.StartsWith("s_"))
                .Select(k => k.Key)
                .ToHashSet();
            _mappings.TryAdd(collectionName, cachedProperties);
        }

        //now we simple check if we really have to update the mapping.
        if (properties.Count > cachedProperties.Count)
        {
            //enter lock, we need only one thread at a time trying to modify the collection.
            lock (cachedProperties)
            {
                foreach (var property in properties)
                {
                    cachedProperties.Add(property.ToLower());
                }

                var updateIndexCommand = UpdateIndexCommand(collectionName, cachedProperties, numberOfDimensions);
                _db.RunCommand<BsonDocument>(updateIndexCommand);
            }
        }
    }

    private static IndexInfo _falseIndexInfo = new IndexInfo(false, "", null);

    public record IndexInfo(bool Exists, string Status, AtlasMapping Mapping);
}

public class AtlasMapping
{
    [BsonElement("dynamic")]
    public bool Dynamic { get; set; }

    [BsonElement("fields")]
    public Dictionary<string, FieldProperties> Fields { get; set; }
}

public class FieldProperties
{
    [BsonElement("type")]
    public string Type { get; set; }

    [BsonElement("analyzer")]
    public string Analyzer { get; set; }

    [BsonElement("store")]
    public bool Store { get; set; }

    [BsonElement("vector")]
    public VectorProperties Vector { get; set; }

    [BsonElement("multi")]
    public Dictionary<string, MultiProperties> Multi { get; set; }

    [BsonElement("dimensions")]
    public int Dimensions { get; set; }

    [BsonElement("similarity")]
    public string Similarity { get; set; }
}

public class VectorProperties
{
    [BsonElement("dimensions")]
    public int Dimensions { get; set; }

    [BsonElement("method")]
    public string Method { get; set; }

    [BsonElement("distance")]
    public string Distance { get; set; }

    [BsonElement("sparse")]
    public bool Sparse { get; set; }
}

public class MultiProperties
{
    [BsonElement("analyzer")]
    public string Analyzer { get; set; }

    [BsonElement("type")]
    public string Type { get; set; }
}
