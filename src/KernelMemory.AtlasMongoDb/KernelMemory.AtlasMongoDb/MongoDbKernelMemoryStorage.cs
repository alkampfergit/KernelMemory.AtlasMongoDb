using Microsoft.KernelMemory.ContentStorage;
using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Driver.GridFS;
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace KernelMemory.AtlasMongoDb;

public class MongoDbKernelMemoryStorage : MongoDbKernelMemoryBaseStorage, IContentStorage
{
    public MongoDbKernelMemoryStorage(MongoDbKernelMemoryConfiguration config) : base(config)
    {
    }



    public Task CreateIndexDirectoryAsync(string index, CancellationToken cancellationToken = new CancellationToken())
    {
        return Task.CompletedTask;
    }

    public async Task DeleteIndexDirectoryAsync(string index, CancellationToken cancellationToken = new CancellationToken())
    {
        // delete all document in gridfs that have index as metadata
        var bucket = new GridFSBucket(_db);
        var filter = Builders<GridFSFileInfo>.Filter.Eq("metadata.index", index);
        // load all id then delete all id
        var files = await bucket.FindAsync(filter, cancellationToken: cancellationToken);
        var ids = await files.ToListAsync(cancellationToken);
        foreach (var id in ids)
        {
            await bucket.DeleteAsync(id.Id, cancellationToken);
        }

        await _db.DropCollectionAsync(index, cancellationToken);
    }

    public Task CreateDocumentDirectoryAsync(string index, string documentId,
        CancellationToken cancellationToken = new CancellationToken())
    {
        //no need to create anything for the document
        return Task.CompletedTask;
    }

    public async Task EmptyDocumentDirectoryAsync(string index, string documentId,
        CancellationToken cancellationToken = new CancellationToken())
    {
        // delete all document in gridfs that have index as metadata
        var bucket = new GridFSBucket(_db);
        var filter = Builders<GridFSFileInfo>.Filter.And(
            Builders<GridFSFileInfo>.Filter.Eq("metadata.index", index),
            Builders<GridFSFileInfo>.Filter.Eq("metadata.documentId", documentId)
        );
        // load all id then delete all id
        var files = await bucket.FindAsync(filter, cancellationToken: cancellationToken);
        var ids = await files.ToListAsync(cancellationToken);
        foreach (var id in ids)
        {
            await bucket.DeleteAsync(id.Id, cancellationToken);
        }

        // delete all document in mongodb that have index as metadata
        var collection = GetCollection(index);
        var filter2 = Builders<BsonDocument>.Filter.Eq("documentId", documentId);
        await collection.DeleteManyAsync(filter2, cancellationToken);
    }

    public Task DeleteDocumentDirectoryAsync(string index, string documentId,
        CancellationToken cancellationToken = new CancellationToken())
    {
        return EmptyDocumentDirectoryAsync(index, documentId, cancellationToken);
    }

    public async Task WriteFileAsync(string index, string documentId, string fileName, Stream streamContent,
        CancellationToken cancellationToken = new CancellationToken())
    {
        // txt files are extracted text, and are stored in mongodb in the collection
        var extension = Path.GetExtension(fileName);
        if (extension == ".txt")
        {
            var id = $"{documentId}/{fileName}";
            using var reader = new StreamReader(streamContent);
            var doc = new BsonDocument()
            {
                {"_id", id},
                {"documentId", documentId},
                {"fileName", fileName},
                {"content", new BsonString(await reader.ReadToEndAsync())}
            };
            await SaveDocumentAsync(index, cancellationToken, id, doc);
        }
        else if (extension == ".text_embedding")
        {
            //ok the file is a text embedding formatted as json 
            var id = $"{documentId}/{fileName}";
            using var reader = new StreamReader(streamContent);
            var content = await reader.ReadToEndAsync();
            // now deserialize the json
            var doc = BsonDocument.Parse(content);
            doc["_id"] = id;
            doc["documentId"] = documentId;
            doc["fileName"] = fileName;
            doc["content"] = content;
            await SaveDocumentAsync(index, cancellationToken, id, doc);
        }
        else
        {
            var bucket = new GridFSBucket(_db);
            var options = new GridFSUploadOptions
            {
                Metadata = new BsonDocument
                {
                    { "index", index },
                    { "documentId", documentId },
                    { "fileName", fileName }
                }
            };
            await bucket.UploadFromStreamAsync(fileName, streamContent, options, cancellationToken);
        }
    }

    private async Task SaveDocumentAsync(string index, CancellationToken cancellationToken, string id, BsonDocument doc)
    {
        var collection = GetCollection(index);

        //upsert the doc based on the id
        await collection.ReplaceOneAsync(
            Builders<BsonDocument>.Filter.Eq("_id", id),
            doc,
            new ReplaceOptions { IsUpsert = true },
            cancellationToken
        );
    }

    public async Task<BinaryData> ReadFileAsync(string index, string documentId, string fileName, bool logErrIfNotFound = true,
        CancellationToken cancellationToken = new CancellationToken())
    {
        // read from mongodb but you need to check extension to load correctly
        var extension = Path.GetExtension(fileName);
        if (extension == ".txt")
        {
            var id = $"{documentId}/{fileName}";
            var collection = GetCollection(index);
            var filterById = Builders<BsonDocument>.Filter.Eq("_id", id);
            var doc = await collection.Find(filterById).FirstOrDefaultAsync(cancellationToken);
            if (doc == null)
            {
                if (logErrIfNotFound)
                {
                    Console.WriteLine($"File {fileName} not found in index {index} and document {documentId}");
                }
                throw new ContentStorageFileNotFoundException("File not found");
            }
            return new BinaryData(doc["content"].AsString);
        }
        else if (extension == ".text_embedding")
        {
            var id = $"{documentId}/{fileName}";
            var collection = GetCollection(index);
            var filterById = Builders<BsonDocument>.Filter.Eq("_id", id);
            var doc = await collection.Find(filterById).FirstOrDefaultAsync(cancellationToken);
            if (doc == null)
            {
                if (logErrIfNotFound)
                {
                    Console.WriteLine($"File {fileName} not found in index {index} and document {documentId}");
                }
                throw new ContentStorageFileNotFoundException("File not found");
            }
            return new BinaryData(doc["content"].AsString);
        }
        else
        {
            var bucket = new GridFSBucket(_db);
            var filter = Builders<GridFSFileInfo>.Filter.And(
                Builders<GridFSFileInfo>.Filter.Eq(x => x.Filename, fileName),
                Builders<GridFSFileInfo>.Filter.Eq("metadata.index", index),
                Builders<GridFSFileInfo>.Filter.Eq("metadata.documentId", documentId)
            );

            var files = await bucket.FindAsync(filter, cancellationToken: cancellationToken);
            var file = await files.FirstOrDefaultAsync(cancellationToken);
            if (file == null)
            {
                if (logErrIfNotFound)
                {
                    Console.WriteLine($"File {fileName} not found in index {index} and document {documentId}");
                }

                throw new ContentStorageFileNotFoundException("File not found");
            }

            using var stream = await bucket.OpenDownloadStreamAsync(file.Id, cancellationToken: cancellationToken);
            using var memoryStream = new MemoryStream();
            await stream.CopyToAsync(memoryStream);
            memoryStream.Position = 0;
            return new BinaryData(memoryStream.ToArray());
        }
    }
}