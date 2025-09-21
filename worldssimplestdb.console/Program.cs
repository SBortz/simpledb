using System.Diagnostics;
using System.Text;
using worldssimplestdb;
using worldssimplestdb.v1;
using worldssimplestdb.v2;
using worldssimplestdb.v3;

await MainAsync(args);
async Task MainAsync(string[] args)
{
    if (args.Length == 0)
    {
        await RunInteractiveMode();
        return;
    }
    await RunCommandLineMode(args);
}

async Task RunInteractiveMode()
{
    Console.WriteLine("=== Simple Database Interactive Mode ===");
    Console.WriteLine();
    
    // Database version selection and run
    await SelectAndRunDatabaseVersion();
}

async Task RunCommandLineMode(string[] args)
{
    // Check for version parameter
    string version = "v3"; // default
    if (args.Length > 0 && (args[0] == "--version" || args[0] == "-v"))
    {
        if (args.Length < 2)
        {
            Console.WriteLine("Usage: --version <v1|v2|v3>");
            return;
        }
        
        version = args[1].ToLowerInvariant();
        args = args.Skip(2).ToArray(); // Remove version arguments
    }
    
    if (args.Length == 0)
    {
        Console.WriteLine("No command provided.");
        return;
    }
    
    await RunWithDatabase(version, args);
}

async Task SelectAndRunDatabaseVersion()
{
    Console.WriteLine("Select Database Version:");
    Console.WriteLine("1. V1 - Basic append-only database (no index)");
    Console.WriteLine("2. V2 - Basic append-only database (no index) [same as V1]");
    Console.WriteLine("3. V3 - Advanced database with index cache for fast lookups");
    Console.WriteLine();
    
    while (true)
    {
        Console.Write("Choose version (1-3) or 'exit': ");
        var input = Console.ReadLine()?.Trim();
        
        if (string.IsNullOrEmpty(input)) continue;
        
        string version = input.ToLowerInvariant() switch
        {
            "1" => "v1",
            "2" => "v2", 
            "3" => "v3",
            "exit" or "quit" => "exit",
            _ => ""
        };
        
        if (version == "exit") return;
        if (version != "")
        {
            Console.WriteLine($"Using Database {version.ToUpper()} ({GetVersionDescription(version)})");
            await RunInteractiveWithDatabase(version);
            return;
        }
        
        Console.WriteLine("Invalid choice. Please enter 1, 2, 3, or 'exit'.");
    }
}

// Unified database methods
async Task RunWithDatabase(string version, string[] args)
{
    var db = CreateDatabase(version);
    if (db is null)
    {
        Console.WriteLine("Invalid version. Use v1, v2, or v3.");
        return;
    }
    
    using (db)
    {
        await ExecuteCommand(db, args);
    }
}

async Task RunInteractiveWithDatabase(string version)
{
    var db = CreateDatabase(version);
    if (db is null)
    {
        Console.WriteLine("Invalid version. Use v1, v2, or v3.");
        return;
    }
    
    Console.WriteLine("Commands: set <key> <value> | get <key> | help | exit");
    Console.WriteLine();

    using (db)
    {
        while (true)
        {
            Console.Write("db> ");
            var input = Console.ReadLine();
            
            if (string.IsNullOrWhiteSpace(input))
                continue;

            var parts = ParseCommandLine(input);
            if (parts.Length == 0)
                continue;

            try
            {
                await ExecuteCommand(db, parts);
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine($"Error: {ex.Message}");
            }
        }
    }
}

IDatabase? CreateDatabase(string version)
{
    return version.ToLowerInvariant() switch
    {
        "v1" or "1" => new WorldsSimplestDbV1(),
        "v2" or "2" => new WorldsSimplestDbV2(),
        "v3" or "3" => CreateV3Database(),
        _ => null
    };
}

IDatabase CreateV3Database()
{
    var indexStore = new IndexStore();
    var db = new WorldsSimplestDbV3(indexStore);
    LoadIndexWithFeedback(indexStore);
    return db;
}

void LoadIndexWithFeedback(IIndexStore indexStore)
{
    Console.Write("Building database index");
    var sw = System.Diagnostics.Stopwatch.StartNew();
    
    indexStore.Load((message) => {
        Console.Write(".");
    });
    
    sw.Stop();
    Console.WriteLine($" done ({sw.ElapsedMilliseconds}ms)");
}

string GetVersionDescription(string version)
{
    return version.ToLowerInvariant() switch
    {
        "v1" => "Basic append-only",
        "v2" => "Basic append-only", 
        "v3" => "With index cache",
        _ => "Unknown"
    };
}

async Task ExecuteCommand(IDatabase database, string[] args)
{
    switch (args[0].ToLowerInvariant())
    {
        case "set":
            Utils.Require(args.Length >= 3, "set <key> <value>");
            await database.SetAsync(args[1], args[2]);
            Console.WriteLine("OK");
            break;

        case "get":
            Utils.Require(args.Length >= 2, "get <key>");
            var val = await database.GetAsync(args[1]);
            if (val is null) Console.WriteLine("Key not found.");
            else Console.WriteLine(val);
            break;
        case "help":
            Console.WriteLine("""
            Available commands:
              set <key> <value>         - Store a key-value pair
              get <key>                 - Get value by key
              help                      - Show this help
              exit/quit                 - Exit the program
            """);
            break;
        case "exit":
        case "quit":
            Console.WriteLine("Goodbye!");
            Environment.Exit(0);
            break;

        default:
            Console.WriteLine($"Unknown command: {args[0]}. Type 'help' for available commands.");
            break;
    }
}

string[] ParseCommandLine(string input)
{
    var parts = new List<string>();
    var current = new StringBuilder();
    bool inQuotes = false;
    char quoteChar = '"';

    for (int i = 0; i < input.Length; i++)
    {
        char c = input[i];

        if (c == '"' || c == '\'')
        {
            if (!inQuotes)
            {
                inQuotes = true;
                quoteChar = c;
            }
            else if (c == quoteChar)
            {
                inQuotes = false;
            }
            else
            {
                current.Append(c);
            }
        }
        else if (char.IsWhiteSpace(c) && !inQuotes)
        {
            if (current.Length > 0)
            {
                parts.Add(current.ToString());
                current.Clear();
            }
        }
        else
        {
            current.Append(c);
        }
    }

    if (current.Length > 0)
    {
        parts.Add(current.ToString());
    }

    return parts.ToArray();
}