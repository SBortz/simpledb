using System.Diagnostics;
using System.Text;
using MicroDb;

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
    Console.WriteLine("Commands: set <key> <value> | get <key> | get-scan <key> | fill [size] [minLen] [maxLen] | bench [size] [minLen] [maxLen] [key] | clear | help | exit");
    Console.WriteLine();

    var indexCache = new IndexCache();
    using var db = new SimpleDatabase(indexCache);
    
    // Load index with feedback
    LoadIndexWithFeedback(indexCache);

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

async Task RunCommandLineMode(string[] args)
{
    var indexCache = new IndexCache();
    using var db = new SimpleDatabase(indexCache);
    
    // Load index with feedback
    LoadIndexWithFeedback(indexCache);
    
    await ExecuteCommand(db, args);
}

async Task ExecuteCommand(SimpleDatabase db, string[] args)
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
            var val = await db.GetIndexedAsync(args[1]);
            if (val is null) Console.WriteLine("Key not found (index).");
            else Console.WriteLine(val);
        break;

        case "get-scan":
            Utils.Require(args.Length >= 2, "get-scan <key>");
            var valScan = await db.GetScanAsync(args[1]);
            if (valScan is null) Console.WriteLine("Key not found (scan).");
            else Console.WriteLine(valScan);
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

        case "bench":
        {
            long size = args.Length >= 2 ? Utils.ParseSize(args[1]) : (2L << 30);
            int minLen = args.Length >= 3 ? int.Parse(args[2]) : 16;
            int maxLen = args.Length >= 4 ? int.Parse(args[3]) : 64;

            // fresh start
            await db.ClearAsync();

            Console.WriteLine("Filling database for benchmark...");
            var sw = Stopwatch.StartNew();
            
            var rnd = Random.Shared;
            var sb = new StringBuilder(maxLen);
            int keyNum = 1;
            long entries = 0;
            long currentSize = 0;
            
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
                keyNum++;
            }
            
            sw.Stop();
            Console.WriteLine($"fill:     {sw.Elapsed.TotalSeconds:F2}s  ({entries:N0} entries, {Utils.FormatSize(currentSize)})");

            string benchKey = args.Length >= 5 ? args[4] : $"key{keyNum - 1}";

            // Warmup (JIT)
            _ = db.GetIndexed(benchKey);

            sw.Restart();
            var v1 = db.GetIndexed(benchKey);
            sw.Stop();
            Console.WriteLine($"get:      {sw.Elapsed.TotalMilliseconds:F1} ms  -> {Utils.Preview(v1)}");

            sw.Restart();
            var v2 = db.GetScan(benchKey);
            sw.Stop();
            Console.WriteLine($"get-scan: {sw.Elapsed.TotalSeconds:F2} s   -> {Utils.Preview(v2)}");
            break;
        }

        case "clear":
            await db.ClearAsync();
            Console.WriteLine("Database cleared.");
            break;

        case "help":
            Console.WriteLine("""
            Available commands:
              set <key> <value>         - Store a key-value pair
              get <key>                 - Get value by key (indexed)
              get-scan <key>            - Get value by key (scan)
              fill [size] [minLen] [maxLen] - Fill database with test data
              bench [size] [minLen] [maxLen] [key] - Benchmark database
              clear                     - Clear the database
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

