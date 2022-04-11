// See https://aka.ms/new-console-template for more information
using Dapper;
using Microsoft.Extensions.Configuration;
using MongoDB.Bson;
using MongoDB.Driver;
using MySql.Data.MySqlClient;
using Newtonsoft.Json;
using System.Diagnostics;

try
{
    var builder = new ConfigurationBuilder()
    .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true);

    IConfiguration config = builder.Build();
    const string _mySql = "MySql";
    const string _mongoDb = "MySql";

    var t = new TransactionConverter(config, _mySql, _mongoDb);
    DateTime fromDate = new DateTime(2020, 04, 11);
    DateTime toDate = new DateTime(2020, 04, 12);
    Stopwatch timer = Stopwatch.StartNew();
    while (toDate < new DateTime(2022, 04, 01))
    {
        Console.WriteLine($"{timer.Elapsed} : Getting transactions from {fromDate.ToShortDateString()} to {toDate.ToShortDateString()}");
        var tx = await t.GetTransactions(fromDate, toDate);
        string json = JsonConvert.SerializeObject(tx);

        //await File.WriteAllTextAsync("./output.json", json);
        //Console.WriteLine("File Created");
        Console.WriteLine($"{timer.Elapsed} : --> Inserting Transactions");
        t.InsertTransactions(tx);
        Console.WriteLine($"{timer.Elapsed} : --> Inserted Transactions");
        fromDate = fromDate.AddDays(1);
        toDate = toDate.AddDays(1);
    }SW
}
catch (Exception ex)
{
    Console.WriteLine(ex.ToString());   
}


public class Transaction 
{
    public ulong ID { get; set; }
    public string? EventName { get; set; }
    public DateTime DateCreated { get; set; }
    public Dictionary<string, object> Attributes { get; set; } = new Dictionary<string, object>();
}

public class TransactionConverter
{
    private readonly IConfiguration _config;
    private readonly string _mySqlKey;
    private readonly string _mongoDbKey;
    private Dictionary<uint, string> _events { get; set; }
    private Dictionary<uint, string> _attributes { get; set; }

    public TransactionConverter(IConfiguration config, string dbKey, string mongoDbKey)
    {
        _config = config;
        _mySqlKey = dbKey;
        _mongoDbKey = mongoDbKey;
        GetEvents();
        GetAttributes();
    }

    public async void GetEvents()
    {
        _events = new Dictionary<uint, string>();
        using var conn = new MySqlConnection(_config.GetConnectionString(_mySqlKey));
        conn.Open();

        using (var results = await conn.ExecuteReaderAsync($@"
                SELECT * FROM `global`.`tx_event_types`"))
        {
            while (results.Read())
            {
                _events.Add((uint)results["event_type_id"], (string)results["event_type"]);
            }

        }
    }

    public async void GetAttributes()
    {
        _attributes = new Dictionary<uint, string>();
        using var conn = new MySqlConnection(_config.GetConnectionString(_mySqlKey));
        conn.Open();

        using (var results = await conn.ExecuteReaderAsync($@"
                SELECT * FROM `global`.`tx_attributes`"))
        {
            while (results.Read())
            {
                _attributes.Add((uint)results["attribute_id"], (string)results["attribute"]);
            }

        }
    }

    public async Task<List<Transaction>> GetTransactions(DateTime fromDate, DateTime toDate)
    {
        List<Transaction> transactions = new List<Transaction>();
        using var conn = new MySqlConnection(_config.GetConnectionString(_mySqlKey));
        conn.Open();

        object args = new { from_date = fromDate, to_date = toDate };
        using (var results = conn.ExecuteReader($@"
                SELECT * FROM `tx_transactions` t
                    WHERE date_time BETWEEN @from_date AND @to_date", args, commandTimeout: 9999999))
        {
            while (results.Read())
            {
                if (_events.ContainsKey((uint)results["event_type_id"]))
                {
                    var transactionID = (ulong)results["transaction_id"];
                    Transaction t = new Transaction();
                    t.ID = (ulong)results["transaction_id"];
                    t.EventName = _events[(uint)results["event_type_id"]];
                    t.DateCreated = (DateTime)results["date_time"];
                    var success = await GetTransactionData(t);
                    transactions.Add(t);
                }
            }
        }

        return transactions;
    }

    // Gets the transaction data for all listed transactions
    public async Task<bool> GetTransactionData(Transaction transaction)
    {
        bool success = false;

        using var conn = new MySqlConnection(_config.GetConnectionString(_mySqlKey));
        conn.Open();

        var args = new { id = transaction.ID };
        using (var results = await conn.ExecuteReaderAsync($@"
            SELECT a.`attribute`, td.`value` FROM `tx_transaction_data` td
			LEFT JOIN `global`.`tx_attributes` a ON td.attribute_id = a.attribute_id
                WHERE transaction_id = @id", args, commandTimeout: 9999999))
        {
            while (results.Read())
            {
                transaction.Attributes.Add((string)results["attribute"], (string)results["value"]);
            }

            success = true;
        }

        return success;
    }

    public async Task<bool> InsertTransactions(List<Transaction> t)
    {
        bool success = false;

        if(t.Count <= 0)
        {
            return success;
        }

        try
        {
            var mongo = new MongoClient(_mongoDbKey);

            var db = mongo.GetDatabase("vertexone");
            var txs = db.GetCollection<Transaction>("transactions");

            List<BsonDocument> batch = new List<BsonDocument>();
            await txs.InsertManyAsync(t);
        }
        catch (MongoBulkWriteException<Transaction> ex)
        {
            // Fetch the write errors and match them to what we sent in to be inserted
            var failedDocumets = ex.WriteErrors.Select(x => ex.Result.ProcessedRequests[x.Index] as InsertOneModel<Transaction>)
                .Select(x => x.Document).ToArray();

            Console.WriteLine("Failed Inserts");
            Console.WriteLine(failedDocumets.ToJson());
        }

        return success;
    }
}
