using System.Diagnostics;
using System.Text;
using worldssimplestdb.dbv1;
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
    if (args.Length > 0 && (args[0] == "--version" || args[0] == "-v"))
    {
        if (args.Length < 2)
        {
            Console.WriteLine("Usage: --version <v1|v2|v3>");
            return;
        }
        
        var version = args[1].ToLowerInvariant();
        args = args.Skip(2).ToArray(); // Remove version arguments
        
        if (args.Length == 0)
        {
            Console.WriteLine("No command provided.");
            return;
        }
        
        switch (version)
        {
            case "v1" or "1":
                await RunWithV1(args);
                break;
            case "v2" or "2":
                await RunWithV2(args);
                break;
            case "v3" or "3":
                await RunWithV3(args);
                break;
            default:
                Console.WriteLine("Invalid version. Use v1, v2, or v3.");
                break;
        }
    }
    else
    {
        // Default to V3 for command line mode
        if (args.Length == 0)
        {
            Console.WriteLine("No command provided.");
            return;
        }
        await RunWithV3(args);
    }
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
        
        switch (input.ToLowerInvariant())
        {
            case "1":
                Console.WriteLine("Using Database V1 (Basic append-only)");
                await RunInteractiveWithV1();
                return;
            case "2":
                Console.WriteLine("Using Database V2 (Basic append-only)");
                await RunInteractiveWithV2();
                return;
            case "3":
                Console.WriteLine("Using Database V3 (With index cache)");
                await RunInteractiveWithV3();
                return;
            case "exit":
            case "quit":
                return;
            default:
                Console.WriteLine("Invalid choice. Please enter 1, 2, 3, or 'exit'.");
                break;
        }
    }
}

// V1 specific methods
async Task RunWithV1(string[] args)
{
    using var db = new WorldsSimplestDbV1();
    await ExecuteCommandV1(db, args);
}

async Task RunInteractiveWithV1()
{
    using var db = new WorldsSimplestDbV1();
    Console.WriteLine("Commands: set <key> <value> | get <key> | fill [size] [minLen] [maxLen] | help | exit");
    Console.WriteLine();

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
            await ExecuteCommandV1(db, parts);
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"Error: {ex.Message}");
        }
    }
}

// V2 specific methods
async Task RunWithV2(string[] args)
{
    using var db = new WorldsSimplestDbV2();
    await ExecuteCommandV2(db, args);
}

async Task RunInteractiveWithV2()
{
    using var db = new WorldsSimplestDbV2();
    Console.WriteLine("Commands: set <key> <value> | get <key> | fill [size] [minLen] [maxLen] | help | exit");
    Console.WriteLine();

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
            await ExecuteCommandV2(db, parts);
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"Error: {ex.Message}");
        }
    }
}

// V3 specific methods
async Task RunWithV3(string[] args)
{
    var indexCache = new IndexCache();
    using var db = new WorldsSimplestDbV3(indexCache);
    LoadIndexWithFeedback(indexCache);
    await ExecuteCommandV3(db, args);
}

async Task RunInteractiveWithV3()
{
    var indexCache = new IndexCache();
    using var db = new WorldsSimplestDbV3(indexCache);
    LoadIndexWithFeedback(indexCache);
    
    Console.WriteLine("Commands: set <key> <value> | get <key> | fill [size] [minLen] [maxLen] | help | exit");
    Console.WriteLine();

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
            await ExecuteCommandV3(db, parts);
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"Error: {ex.Message}");
        }
    }
}

async Task ExecuteCommandV1(WorldsSimplestDbV1 db, string[] args)
{
    switch (args[0].ToLowerInvariant())
    {
        case "set":
            Utils.Require(args.Length >= 3, "set <key> <value>");
            await db.SetAsync(args[1], args[2]);
            Console.WriteLine("OK");
            break;

        case "get":
            Utils.Require(args.Length >= 2, "get <key>");
            var val = await db.GetAsync(args[1]);
            if (val is null) Console.WriteLine("Key not found.");
            else Console.WriteLine(val);
            break;

        case "fill":
        {
            long size = args.Length >= 2 ? Utils.ParseSize(args[1]) : (2L << 30);
            int minLen = args.Length >= 3 ? int.Parse(args[2]) : 16;
            int maxLen = args.Length >= 4 ? int.Parse(args[3]) : 64;
            
            Console.WriteLine("Filling database...");
            var sw = System.Diagnostics.Stopwatch.StartNew();
            
            var rnd = Random.Shared;
            var sb = new StringBuilder(maxLen);
            int keyNum = 1;
            long entries = 0;
            long currentSize = 0;
            
            // Get current database size
            if (File.Exists("database.bin"))
            {
                currentSize = new FileInfo("database.bin").Length;
            }
            
            while (currentSize < size)
            {
                int len = rnd.Next(minLen, maxLen + 1);
                sb.Clear();
                sb.Append('v').Append(keyNum).Append('-');
                while (sb.Length < len) sb.Append('x');
                
                string key = $"key{keyNum}";
                string value = sb.ToString();
                
                await db.SetAsync(key, value);
                
                entries++;
                currentSize += key.Length + value.Length + 16; // Rough estimate
                
                // Progress feedback every 10000 entries
                if (entries % 10000 == 0)
                {
                    Console.WriteLine($"Filled {entries:N0} entries, current size: {Utils.FormatSize(currentSize)}");
                }
                
                keyNum++;
            }
            
            sw.Stop();
            Console.WriteLine($"Filled {entries:N0} entries, lastKey=key{keyNum - 1}, data={Utils.FormatSize(currentSize)} in {sw.Elapsed.TotalSeconds:F2}s");
            break;
        }


        case "help":
            Console.WriteLine("""
            Available commands:
              set <key> <value>         - Store a key-value pair
              get <key>                 - Get value by key (scan only)
              fill [size] [minLen] [maxLen] - Fill database with test data
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

// V2 ExecuteCommand (same as V1)
async Task ExecuteCommandV2(WorldsSimplestDbV2 db, string[] args)
{
    switch (args[0].ToLowerInvariant())
    {
        case "set":
            Utils.Require(args.Length >= 3, "set <key> <value>");
            await db.SetAsync(args[1], args[2]);
            Console.WriteLine("OK");
            break;

        case "get":
            Utils.Require(args.Length >= 2, "get <key>");
            var val = await db.GetAsync(args[1]);
            if (val is null) Console.WriteLine("Key not found.");
            else Console.WriteLine(val);
            break;

        case "fill":
        {
            long size = args.Length >= 2 ? Utils.ParseSize(args[1]) : (2L << 30);
            int minLen = args.Length >= 3 ? int.Parse(args[2]) : 16;
            int maxLen = args.Length >= 4 ? int.Parse(args[3]) : 64;
            
            Console.WriteLine("Filling database...");
            var sw = System.Diagnostics.Stopwatch.StartNew();
            
            var rnd = Random.Shared;
            var sb = new StringBuilder(maxLen);
            int keyNum = 1;
            long entries = 0;
            long currentSize = 0;
            
            // Get current database size
            if (File.Exists("database.bin"))
            {
                currentSize = new FileInfo("database.bin").Length;
            }
            
            while (currentSize < size)
            {
                int len = rnd.Next(minLen, maxLen + 1);
                sb.Clear();
                sb.Append('v').Append(keyNum).Append('-');
                while (sb.Length < len) sb.Append('x');
                
                string key = $"key{keyNum}";
                string value = sb.ToString();
                
                await db.SetAsync(key, value);
                
                entries++;
                currentSize += key.Length + value.Length + 16; // Rough estimate
                
                // Progress feedback every 10000 entries
                if (entries % 10000 == 0)
                {
                    Console.WriteLine($"Filled {entries:N0} entries, current size: {Utils.FormatSize(currentSize)}");
                }
                
                keyNum++;
            }
            
            sw.Stop();
            Console.WriteLine($"Filled {entries:N0} entries, lastKey=key{keyNum - 1}, data={Utils.FormatSize(currentSize)} in {sw.Elapsed.TotalSeconds:F2}s");
            break;
        }

        case "help":
            Console.WriteLine("""
            Available commands:
              set <key> <value>         - Store a key-value pair
              get <key>                 - Get value by key (scan only)
              fill [size] [minLen] [maxLen] - Fill database with test data
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

// V3 ExecuteCommand (with index and scan)
async Task ExecuteCommandV3(WorldsSimplestDbV3 db, string[] args)
{
    switch (args[0].ToLowerInvariant())
    {
        case "set":
            Utils.Require(args.Length >= 3, "set <key> <value>");
            await db.SetAsync(args[1], args[2]);
            Console.WriteLine("OK");
            break;

        case "get":
            Utils.Require(args.Length >= 2, "get <key>");
            var val = await db.GetAsync(args[1]);
            if (val is null) Console.WriteLine("Key not found.");
            else Console.WriteLine(val);
            break;

        case "fill":
        {
            long size = args.Length >= 2 ? Utils.ParseSize(args[1]) : (2L << 30);
            int minLen = args.Length >= 3 ? int.Parse(args[2]) : 16;
            int maxLen = args.Length >= 4 ? int.Parse(args[3]) : 64;
            
            Console.WriteLine("Filling database...");
            var sw = System.Diagnostics.Stopwatch.StartNew();
            
            var rnd = Random.Shared;
            var sb = new StringBuilder(maxLen);
            int keyNum = 1;
            long entries = 0;
            long currentSize = 0;
            
            // Get current database size
            if (File.Exists("database.bin"))
            {
                currentSize = new FileInfo("database.bin").Length;
            }
            
            while (currentSize < size)
            {
                int len = rnd.Next(minLen, maxLen + 1);
                sb.Clear();
                sb.Append('v').Append(keyNum).Append('-');
                while (sb.Length < len) sb.Append('x');
                
                string key = $"key{keyNum}";
                string value = sb.ToString();
                
                await db.SetAsync(key, value);
                
                entries++;
                currentSize += key.Length + value.Length + 16; // Rough estimate
                
                // Progress feedback every 10000 entries
                if (entries % 10000 == 0)
                {
                    Console.WriteLine($"Filled {entries:N0} entries, current size: {Utils.FormatSize(currentSize)}");
                }
                
                keyNum++;
            }
            
            sw.Stop();
            Console.WriteLine($"Filled {entries:N0} entries, lastKey=key{keyNum - 1}, data={Utils.FormatSize(currentSize)} in {sw.Elapsed.TotalSeconds:F2}s");
            break;
        }

        case "help":
            Console.WriteLine("""
            Available commands:
              set <key> <value>         - Store a key-value pair
              get <key>                 - Get value by key (indexed)
              fill [size] [minLen] [maxLen] - Fill database with test data
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

void LoadIndexWithFeedback(IIndexCache indexCache)
{
    if (File.Exists("database.idx"))
    {
        Console.Write("Loading database index");
        var sw = System.Diagnostics.Stopwatch.StartNew();
        
        indexCache.GetWithFeedback("database.idx", (message) => {
            Console.Write(".");
        });
        
        sw.Stop();
        Console.WriteLine($" done ({sw.ElapsedMilliseconds}ms)");
    }
}


