using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using CommandLine;
using StackExchange.Redis;

class Options
{
    /// <summary>
    ///     Имя сервиса сентинел
    /// </summary>
    [Option('s', "service", Required = false, HelpText = "Имя сервиса сентинел")]
    public string? ServiceName { get; set; }

    /// <summary>
    ///     Строка для подключения
    /// </summary>
    [Option('h', "host", Required = true, HelpText = "Хост для подключения")]
    public string? Host { get; set; }

    /// <summary>
    ///     Номер базы данных
    /// </summary>
    [Option('d', "db", Required = false, HelpText = "Номер базы данных", Default = 0)]
    public int DatabaseIndex { get; set; }

    /// <summary>
    ///     Пароль
    /// </summary>
    [Option('p', "password", Required = false, HelpText = "Пароль")]
    public string? Password { get; set; }

    /// <summary>
    ///     Путь к файлу
    /// </summary>
    [Option('f', "file", Required = true, HelpText = "Путь к файлу")]
    public string? FilePath { get; set; }
    
    public void Validate()
    {
        if (string.IsNullOrWhiteSpace(Host))
            throw new Exception("Host is not set");
        
        if (string.IsNullOrWhiteSpace(FilePath))
            throw new Exception("File path is not set");
    }
}

class Program
{
    private static IConnectionMultiplexer? _redis;
    private static Options? _options;

    static async Task Main(string[] args)
    {
        Parser.Default.ParseArguments<Options>(args)
            .WithParsed(RunWithOptions)
            .WithNotParsed(errors =>
            {
                Console.WriteLine("Errors:");
                foreach (var error in errors)
                {
                    Console.WriteLine(error);
                }
            });

        await ExportAllKeysToJson();
    }

    private static async Task ExportAllKeysToJson()
    {
        if (_redis == null)
            throw new Exception("Redis connection is not established");
        
        if (_options == null)
            throw new Exception("Options are not set");
        
        var db = _redis.GetDatabase(_options.DatabaseIndex);
        var server = _redis.GetServer(_redis.GetEndPoints().First());
        var keys = server.KeysAsync(_options.DatabaseIndex);
        var json = new Dictionary<string, object>();
        var count = 0;

        await foreach (var key in keys)
        {
            //Get key type
            var keyType = db.KeyType(key);

            switch (keyType)
            {
                case RedisType.None:
                    continue;
                case RedisType.String:
                    await ExportStringKeyToJson(db, key, json);
                    break;
                case RedisType.List:
                    await ExportListKeyToJson(db, key, json);
                    break;
                case RedisType.Set:
                    await ExportSetKeyToJson(db, key, json);
                    break;
                case RedisType.SortedSet:
                    await ExportSortedSetKeyToJson(db, key, json);
                    break;
                case RedisType.Hash:
                    await ExportHashKeyToJson(db, key, json);
                    break;
                default:
                    throw new ArgumentOutOfRangeException();
            }
            
            count++;
        }
        
        var jsonStr = JsonSerializer.Serialize(json, new JsonSerializerOptions {WriteIndented = true});
        
        await File.WriteAllTextAsync(_options.FilePath!, jsonStr);
        
        Console.WriteLine($"Exported {count} keys to {_options.FilePath}");
    }

    private static async Task ExportHashKeyToJson(IDatabase db, RedisKey key, Dictionary<string,object> json)
    {
        var values = await db.HashGetAllAsync(key);
        json.Add(key.ToString(), values.ToDictionary(x => x.Name.ToString(), x => x.Value.ToString()));
    }

    private static async Task ExportSortedSetKeyToJson(IDatabase db, RedisKey key, Dictionary<string,object> json)
    {
        var values = await db.SortedSetRangeByRankWithScoresAsync(key);
        json.Add(key.ToString(), values.Select(x => new {x.Score, Value = x.Element.ToString()}).ToList());
    }

    private static async Task ExportSetKeyToJson(IDatabase db, RedisKey key, Dictionary<string,object> json)
    {
        var values = await db.SetMembersAsync(key);
        json.Add(key.ToString(), values.Select(x => x.ToString()).ToList());
    }

    private static async Task ExportListKeyToJson(IDatabase db, RedisKey key, Dictionary<string,object> json)
    {
        var values = await db.ListRangeAsync(key);
        json.Add(key.ToString(), values.Select(x => x.ToString()).ToList());
    }

    private static async Task ExportStringKeyToJson(IDatabase db, RedisKey key, Dictionary<string, object> json)
    {
        var value = await db.StringGetAsync(key);
        json.Add(key.ToString(), value.ToString());
    }

    private static void RunWithOptions(Options? options)
    {
        if (options == null)
            throw new Exception("Options are not set");
        
        options.Validate();
        
        try
        {
            var redisConfig = ConfigurationOptions.Parse(options.Host, true);
            redisConfig.Password = options.Password;
            redisConfig.DefaultDatabase = options.DatabaseIndex;
            redisConfig.ServiceName = options.ServiceName;
            redisConfig.SyncTimeout = 3000;

            _redis = ConnectionMultiplexer.Connect(redisConfig);
            _options = options;
        }
        catch (Exception e)
        {
            Console.WriteLine(e);
            throw;
        }
    }
}