using System;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Documents.Linq;
using Newtonsoft.Json;
using System.Configuration;

namespace RequestUnits
{

    class TestDocument
    {
        [JsonProperty("pkid")]
        public string pkid;
        [JsonProperty("value")]
        public string value;
        [JsonProperty("id")]
        public string id;
        [JsonProperty("city")]
        public string city;


    }

    /*
     * To have this sample working, please make a Cosmos DB account on portal and database named: "db" and create a partitioned collection called "demo"
     * define the partitionkey: /pkid
     * 
     */
    class Program
    {

        static string _endpoint = ConfigurationManager.AppSettings["endpoint"];
        static string _authkey = ConfigurationManager.AppSettings["authKey"];
        static string _database = ConfigurationManager.AppSettings["database"];
        static string _collection = ConfigurationManager.AppSettings["collection"];
        static string []_city = new string []{"DC", "Seattle", "LA", "NY", "Chicago", "London", "Portland","Vancouver", "Tacoma","Redmond" };

        static void Main(string[] args)
        {

            using (DocumentClient client = new DocumentClient(new Uri(_endpoint),  _authkey,
                new ConnectionPolicy {  ConnectionMode = ConnectionMode.Direct, ConnectionProtocol = Protocol.Tcp, MaxConnectionLimit = 1000 }))
            {
                RunAsync(client).Wait();
            }
        }

        private static async Task RunAsync(DocumentClient client)
        {
            var collectionUri = UriFactory.CreateDocumentCollectionUri(_database, _collection);
            ResourceResponse<Document> kbReadResponse = null;
            FeedResponse<TestDocument> _queryResponse = null;
            IDocumentQuery<TestDocument> query = null;

            try
            {
                string _id = await CreateDocument(client, collectionUri, 1024);
                // Get a 1KB document by ID
                kbReadResponse = await client.ReadDocumentAsync(UriFactory.CreateDocumentUri("db", "demo", _id), new RequestOptions { PartitionKey = new PartitionKey(_id) });
                Console.WriteLine("Read document completed with {0} RUs", kbReadResponse.RequestCharge);
                Document document = kbReadResponse.Resource;
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }

            //Create 100 documents
            await Create_N_Documents(client, collectionUri, 1024,100);

            // Query for 100 1KB documents
            query = client.CreateDocumentQuery<TestDocument>(
                UriFactory.CreateDocumentCollectionUri("db", "demo"), 
                new FeedOptions { MaxItemCount = -1 , EnableCrossPartitionQuery = true}).Take(100).AsDocumentQuery();  //Or you can do .ToList()

            while (query.HasMoreResults){
                _queryResponse = await query.ExecuteNextAsync<TestDocument>();
                Console.ForegroundColor = ConsoleColor.Green;
                Console.WriteLine($"\nQuery TOP 100 documents completed with {_queryResponse.Count} results and {_queryResponse.RequestCharge} RUs");
            }

            #region ToList
            //var list = client.CreateDocumentQuery(
            //   UriFactory.CreateDocumentCollectionUri("db", "demo"),
            //   new FeedOptions { MaxItemCount = -1, EnableCrossPartitionQuery = true }).Take(100).ToList<Document>();  //Or you can do .ToList()
            //   Console.ForegroundColor = ConsoleColor.Green;
            //   Console.WriteLine($"Query TOP 100 documents completed with {list.Count}");
            #endregion


            // Query by filter (from index)
            query = client.CreateDocumentQuery<TestDocument>(UriFactory.CreateDocumentCollectionUri("db", "demo")
                , new FeedOptions { MaxItemCount = -1, EnableCrossPartitionQuery = true, PopulateQueryMetrics = true })
                .Where(f => f.city == "Seattle").AsDocumentQuery();

            FeedResponse<TestDocument> secondQueryResponse = await query.ExecuteNextAsync<TestDocument>();
            Double TotalRu = 0;
            int TotalDocument = 0;
            Console.ForegroundColor = ConsoleColor.Yellow;
            while (query.HasMoreResults)
            {
                _queryResponse = await query.ExecuteNextAsync<TestDocument>();
                TotalRu += _queryResponse.RequestCharge;
                TotalDocument += _queryResponse.Count;
                Console.Write("+");
            }
            Console.WriteLine($"\nQuery with filter but cross partition completed with {TotalDocument} results and {TotalRu} RUs");

            // Query by filter (from index)
            query = client.CreateDocumentQuery<TestDocument>(
                UriFactory.CreateDocumentCollectionUri("db", "demo")
                ,new FeedOptions { MaxItemCount = -1,  PopulateQueryMetrics = true }
                )
                .Where(f => (f.pkid == "Seattle" && f.city == "Seattle")).AsDocumentQuery();

            TotalRu = 0;
            TotalDocument = 0;
            Console.ForegroundColor = ConsoleColor.Green;
            while (query.HasMoreResults)
            {
                _queryResponse = await query.ExecuteNextAsync<TestDocument>();
                TotalRu += _queryResponse.RequestCharge;
                TotalDocument += _queryResponse.Count;
                Console.Write("X");
            }
            Console.WriteLine($"\nQuery with filter and partitionkey defined completed with {TotalDocument} results and {TotalRu} RUs");
            Console.ReadKey();

        }

        static Random _random = new Random (DateTime.Now.Second);
        private static async Task<bool> Create_N_Documents(DocumentClient client, Uri collectionUri, int size, int noOfDocuments)
        {
            for (int i = 0; i < noOfDocuments; i++)
            {
                string _id = DateTime.Now.Ticks.ToString() + "-" + i;
                TestDocument tDoc = new TestDocument
                {
                    pkid = _city[_random.Next(_city.Length - 1)],
                    value = new string('x', size),
                    id = _id,
                    city = _city[_random.Next(_city.Length - 1)]
                };

                //Create the document
                ResourceResponse<Document> kbWriteResponse = await client.CreateDocumentAsync(collectionUri, tDoc);
                Console.Write(".");
              
            }
            return true;
        }

        private static async Task<string> CreateDocument(DocumentClient client, Uri collectionUri, int size)
        {
            string _id = DateTime.Now.Ticks.ToString();
            TestDocument tDoc = new TestDocument
            {
                pkid = _id,
                value = new string('x', size),
                id = _id,
                city = _city[_random.Next(_city.Length - 1)]
            };

            //Create the document
            ResourceResponse<Document> kbWriteResponse = await client.CreateDocumentAsync(collectionUri, tDoc);
            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine($"{size} Bytes document created for { kbWriteResponse.RequestCharge} RUs");
            Console.ForegroundColor = ConsoleColor.White;
            return _id;
        }

        public class Family
        {
            [JsonProperty("id")]
            public string Id { get; set; }

            [JsonProperty("lastName")]
            public string LastName { get; set; }

            [JsonProperty("address")]
            public Address Address { get; set; }
        }

        public class Address
        {
            [JsonProperty("state")]
            public string State { get; set; }

            [JsonProperty("county")]
            public string County { get; set; }

            [JsonProperty("city")]
            public string City { get; set; }
        }
    }
}
