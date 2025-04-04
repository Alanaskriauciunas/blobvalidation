using Microsoft.EntityFrameworkCore;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using System;
using System.IO;
using Microsoft.EntityFrameworkCore.ValueGeneration;
using System.Runtime.CompilerServices;
using System.Reflection.Metadata;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion.Internal;
using Microsoft.Extensions.Options;
using Azure.Core;

public class Program
{
    public static void Main(string[] args)
    {
        using(var db = new DatabaseContext())
        {
            db.Database.EnsureCreated();
        }
        while(true)
        {
            Start();
        }
    }
    static void Start()
    {
        Console.WriteLine("Select the operation you want to do");
        Console.WriteLine("A - Fetch document names from blob storage");
        Console.WriteLine("B - Fetch document names from database");
        Console.WriteLine("C - Delete blobsItems that are in the database but not in the blob storage");
        Console.WriteLine("D - Delete blobs that are in the blob storage but not in the database");
        Console.WriteLine("E - Populate both blob storage and the database");

        string? answer = Console.ReadLine();

        if(answer?.Length != 1 )
        {
            Console.WriteLine("Invalid input");
            return;
        }
        char operation = answer!.ToLower()[0];
        if(operation != 'a' && operation != 'b' && operation !='c' && operation != 'd' && operation != 'e')
        {
            Console.WriteLine("Invalid input");
        }
        else
        {
            switch (operation)
            {
                case 'a':
                    Console.WriteLine("Fetching documents from blob storage");
                    FetchFromBlobStorage();
                    Console.WriteLine();
                    break;
                case 'b':
                    Console.WriteLine("Fetching documents from database");
                    FetchFromDatabase();
                    Console.WriteLine();
                    break;
                case 'c':
                    Console.WriteLine("Deleting extra BlobItems from database");
                    DeleteFromDatabase();
                    Console.WriteLine();
                    break;
                case 'd':
                    Console.WriteLine("Deleting extra blobs from blob storage");
                    DeleteFromBlobStorage();
                    Console.WriteLine();
                    break;
                case 'e':
                    Console.WriteLine("How many documents do you want to create?");
                    int.TryParse(Console.ReadLine(), out int documentCount);
                    Console.WriteLine("How many of these documents should be saved in the database?");
                    int.TryParse(Console.ReadLine(), out int inDb);
                        if (documentCount <= 0 && inDb <= 0 && inDb > documentCount)
                        {
                            Console.WriteLine("Invalid input");
                            return;
                        }
                        else
                        {
                            GenerateData(documentCount, inDb);
                        }
                    break;
            }
        }
    }
    public class BlobItem
    {
        public Guid Id { get; set; }
        public required string BlobName { get; set; }
    }
    public class BlobServiceContext
    {
        private static string connectionString = "DefaultEndpointsProtocol=https;AccountName=krstorage3;AccountKey=mCI5YFJvObL3Vn5BIFKvz3Vv9p8lmsK4hS74/qhlU/GOEpelMLBG0+6sH++kxPH9Smp/np+JZviv+ASt58Jemg==;EndpointSuffix=core.windows.net";
        public BlobServiceClient blobServiceClient = new BlobServiceClient(connectionString);
        public BlobContainerClient blobContainerClient;
        public BlobServiceContext()
        {
            blobContainerClient = blobServiceClient.GetBlobContainerClient("krblobstorage");
        }
    }
    public class DatabaseContext() : DbContext
    { 
        public DbSet<BlobItem> BlobItems { get; set; }
        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        {
            
            optionsBuilder.UseSqlServer("Server=tcp:krsqlserver3.database.windows.net,1433;Initial Catalog=krsqldatabase;Encrypt=True;TrustServerCertificate=False;Connection Timeout=30;Authentication=\"Active Directory Default\";");
        }
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<BlobItem>(entity =>
            {
                entity.HasKey(x => x.Id);
                entity.HasIndex(x => x.Id);
                entity.Property(x => x.Id).HasValueGenerator<SequentialGuidValueGenerator>();
            });
        }
    }
    static async void DeleteFromBlobStorage()
    {
        System.Diagnostics.Stopwatch stopwatch = new System.Diagnostics.Stopwatch();
        stopwatch.Start();
        BlobServiceContext _blobContext = new BlobServiceContext();
        DatabaseContext _dbContext = new DatabaseContext();
        HashSet<string> filenamesInDatabase = await _dbContext.BlobItems.Select(x => x.BlobName).ToHashSetAsync();

        int deletedCount = 0;
        await foreach (var page in _blobContext.blobContainerClient.GetBlobsAsync().AsPages(pageSizeHint: 100))
        {
            foreach (var blobItem in page.Values)
            {
                if (!filenamesInDatabase.Contains(blobItem.Name))
                {
                    await _blobContext.blobContainerClient.DeleteBlobAsync(blobItem.Name);
                    deletedCount++;
                }
            }
        }
        Console.WriteLine("Total blobs deleted: " + deletedCount);
        Console.WriteLine(stopwatch.ElapsedMilliseconds);
        stopwatch.Stop();
    }
    static async void DeleteFromDatabase()
    {
        DatabaseContext _dbContext = new DatabaseContext();
        //if an item is in the database but not in the blob storage delete it
        BlobServiceContext _blobContext = new BlobServiceContext();

        string[] blobs = _blobContext.blobContainerClient.GetBlobsAsync().ToBlockingEnumerable().Select(x => x.Name).ToArray();
        string[] filenamesInDatabase = await _dbContext.BlobItems.Select(x => x.BlobName).ToArrayAsync();
        int deletedCount = 0;

        foreach (string filename in filenamesInDatabase)
        {
            if (!blobs.Contains(filename))
            {
                //BlobItem? blobItem = await _dbContext.BlobItems.Where(x => x.BlobName == filename).FirstOrDefaultAsync();
                await _dbContext.BlobItems.Where(x => x.BlobName == filename).ExecuteDeleteAsync();
                deletedCount++;
            }
        }
        Console.WriteLine("Total rows deleted: " + deletedCount);
        await _dbContext.SaveChangesAsync();
    }

    static async void FetchFromBlobStorage()
    {
       
        BlobServiceContext _blobContext = new BlobServiceContext();

        var blobPage = _blobContext.blobContainerClient.GetBlobsAsync().AsPages(continuationToken: null, pageSizeHint: 100);
        var blobList = blobPage.GetAsyncEnumerator();

        Dictionary<int, Azure.Page<Azure.Storage.Blobs.Models.BlobItem>> pagesDictionary = new Dictionary<int, Azure.Page<Azure.Storage.Blobs.Models.BlobItem>>();
        
        int pageId = 0;
        bool exists = true;

        while (exists)
        {
            pageId++;
            pagesDictionary.Add(pageId, blobPage.ToBlockingEnumerable().First());
            exists = await blobList.MoveNextAsync();
        }
        Console.WriteLine($"Total page count: {pageId}");

    }
    static async void FetchFromDatabase()
    {
        DatabaseContext _dbContext = new DatabaseContext();
        List<BlobItem> blobItems =  await _dbContext.BlobItems.ToListAsync();
        foreach (BlobItem blobItem in blobItems)
        {
            Console.WriteLine(blobItem.BlobName);
        }
    }
    static void GenerateData(int count, int inDb)
    {
        var db = new DatabaseContext();

        for (int i = 0; i < count; i++)
        {
            string filename = "file-" + Guid.NewGuid().ToString() + ".txt";
            if (!Directory.Exists("Data"))
            {
                Directory.CreateDirectory("Data");//bin>debug>.NET9>
            }
            string localFilePath = Path.Combine("Data", filename);

            File.WriteAllText(localFilePath, "Hello there");

            BlobServiceContext _blobContext = new BlobServiceContext();

            BlobClient blobClient = _blobContext.blobContainerClient.GetBlobClient(filename);

            blobClient.Upload(localFilePath);

            if (i < inDb)
            {
                db.BlobItems.Add(new BlobItem() { BlobName = filename });
                db.SaveChanges();
            }
        }
    }
}