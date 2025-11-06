using System.Diagnostics;
using System.Text;
using worldssimplestdb.v1;
using worldssimplestdb.v2;
using worldssimplestdb.v3;
using worldssimplestdb.v4;

// Check if benchmark mode is requested
if (args.Length > 0 && args[0].ToLowerInvariant() == "benchmark")
{
    await RunBenchmarkAsync(args.Skip(1).ToArray());
    return;
}

// Original fill mode
if (args.Length < 4)
{
    Console.WriteLine("Usage:");
    Console.WriteLine("  Fill data: FillData <version> <size> <minValueLen> <maxValueLen>");
    Console.WriteLine("  Benchmark: FillData benchmark [fillSize] [testKey] [iterations]");
    Console.WriteLine();
    Console.WriteLine("Fill data examples:");
    Console.WriteLine("  FillData v4 10mb 50 200");
    Console.WriteLine("  FillData v1 200mb 50 200");
    Console.WriteLine();
    Console.WriteLine("Benchmark examples:");
    Console.WriteLine("  FillData benchmark                    # Fill with 200mb, test with default key, 10 iterations");
    Console.WriteLine("  FillData benchmark 10mb               # Fill with 10mb");
    Console.WriteLine("  FillData benchmark 10mb key_00050000  # Fill with 10mb, test specific key");
    Console.WriteLine("  FillData benchmark 10mb key_00050000 20  # Fill with 10mb, 20 iterations");
    return;
}

string version = args[0].ToLowerInvariant();
string sizeStr = args[1].ToLowerInvariant();
if (!int.TryParse(args[2], out int minValueLen) || minValueLen <= 0)
{
    Console.WriteLine("minValueLen must be a positive number");
    return;
}
if (!int.TryParse(args[3], out int maxValueLen) || maxValueLen <= minValueLen)
{
    Console.WriteLine("maxValueLen must be greater than minValueLen");
    return;
}

long targetSizeBytes = ParseSize(sizeStr);
if (targetSizeBytes <= 0)
{
    Console.WriteLine($"Invalid size format: {sizeStr}");
    return;
}

Console.WriteLine($"Filling database to ~{sizeStr} using version {version}");
Console.WriteLine($"Value length range: {minValueLen}-{maxValueLen} characters");

var sw = System.Diagnostics.Stopwatch.StartNew();
(long writtenBytes, int entryCount) result;

switch (version)
{
    case "v1":
        result = await FillV1(targetSizeBytes, minValueLen, maxValueLen);
        break;
    case "v2":
        result = await FillV2(targetSizeBytes, minValueLen, maxValueLen);
        break;
    case "v3":
        result = await FillV3(targetSizeBytes, minValueLen, maxValueLen);
        break;
    case "v4":
        result = await FillV4(targetSizeBytes, minValueLen, maxValueLen);
        break;
    default:
        Console.WriteLine($"Unknown version: {version}");
        return;
}

sw.Stop();
Console.WriteLine($"Done! Wrote {result.entryCount:N0} entries ({result.writtenBytes / (1024.0 * 1024.0):F2} MB) in {sw.ElapsedMilliseconds}ms");

long ParseSize(string sizeStr)
{
    if (sizeStr.EndsWith("kb"))
        return long.Parse(sizeStr[..^2]) * 1024;
    if (sizeStr.EndsWith("mb"))
        return long.Parse(sizeStr[..^2]) * 1024 * 1024;
    if (sizeStr.EndsWith("gb"))
        return long.Parse(sizeStr[..^2]) * 1024 * 1024 * 1024;
    return long.Parse(sizeStr);
}

async Task<(long writtenBytes, int entryCount)> FillV1(long targetSize, int minLen, int maxLen)
{
    var random = new Random(42);
    
    // Always create database in the solution directory
    string solutionDir = Path.GetFullPath(Path.Combine(Directory.GetCurrentDirectory(), ".."));
    string dbPath = Path.Combine(solutionDir, "database.txt");
    await using var fs = new FileStream(dbPath, FileMode.Create, FileAccess.Write, FileShare.None, bufferSize: 65536);
    await using var writer = new StreamWriter(fs);
    
    long writtenBytes = 0;
    int entryCount = 0;
    
    while (writtenBytes < targetSize)
    {
        string key = $"key_{entryCount:D8}";
        string value = GenerateRandomValue(random, minLen, maxLen);
        
        await writer.WriteLineAsync($"{key};{value}");
        writtenBytes += key.Length + value.Length + 2; // +2 for semicolon and newline
        entryCount++;
        
        if (entryCount % 10000 == 0)
        {
            Console.WriteLine($"V1: {entryCount:N0} entries, {writtenBytes / (1024.0 * 1024.0):F2} MB");
        }
    }
    
    return (writtenBytes, entryCount);
}

async Task<(long writtenBytes, int entryCount)> FillV2(long targetSize, int minLen, int maxLen)
{
    var random = new Random(42);
    
    // Always create database in the solution directory
    string solutionDir = Path.GetFullPath(Path.Combine(Directory.GetCurrentDirectory(), ".."));
    string dbPath = Path.Combine(solutionDir, "database.bin");
    await using var fs = new FileStream(dbPath, FileMode.Create, FileAccess.Write, FileShare.None, bufferSize: 65536);
    await using var writer = new BinaryWriter(fs);
    
    long writtenBytes = 0;
    int entryCount = 0;
    
    while (writtenBytes < targetSize)
    {
        string key = $"key_{entryCount:D8}";
        string value = GenerateRandomValue(random, minLen, maxLen);
        
        byte[] keyData = Encoding.UTF8.GetBytes(key);
        byte[] valueData = Encoding.UTF8.GetBytes(value);
        
        writer.Write(keyData.Length);
        writer.Write(keyData);
        writer.Write(valueData.Length);
        writer.Write(valueData);
        
        writtenBytes += 4 + keyData.Length + 4 + valueData.Length;
        entryCount++;
        
        if (entryCount % 10000 == 0)
        {
            Console.WriteLine($"V2: {entryCount:N0} entries, {writtenBytes / (1024.0 * 1024.0):F2} MB");
        }
    }
    
    return (writtenBytes, entryCount);
}

async Task<(long writtenBytes, int entryCount)> FillV3(long targetSize, int minLen, int maxLen)
{
    var random = new Random(42);
    var indexDict = new Dictionary<string, long>();
    
    // Always create database in the solution directory
    string solutionDir = Path.GetFullPath(Path.Combine(Directory.GetCurrentDirectory(), ".."));
    string dbPath = Path.Combine(solutionDir, "database.bin");
    await using var fs = new FileStream(dbPath, FileMode.Create, FileAccess.Write, FileShare.None, bufferSize: 65536);
    await using var writer = new BinaryWriter(fs);
    
    long writtenBytes = 0;
    int entryCount = 0;
    
    while (writtenBytes < targetSize)
    {
        string key = $"key_{entryCount:D8}";
        string value = GenerateRandomValue(random, minLen, maxLen);
        
        long position = fs.Position;
        byte[] keyData = Encoding.UTF8.GetBytes(key);
        byte[] valueData = Encoding.UTF8.GetBytes(value);
        
        writer.Write(keyData.Length);
        writer.Write(keyData);
        writer.Write(valueData.Length);
        writer.Write(valueData);
        
        // Update in-memory index (for demonstration)
        indexDict[key] = position;
        
        writtenBytes += 4 + keyData.Length + 4 + valueData.Length;
        entryCount++;
        
        if (entryCount % 10000 == 0)
        {
            Console.WriteLine($"V3: {entryCount:N0} entries, {writtenBytes / (1024.0 * 1024.0):F2} MB");
        }
    }
    
    return (writtenBytes, entryCount);
}

async Task<(long writtenBytes, int entryCount)> FillV4(long targetSize, int minLen, int maxLen)
{
    var random = new Random(42);
    
    // Always create database in the solution directory
    string solutionDir = Path.GetFullPath(Path.Combine(Directory.GetCurrentDirectory(), ".."));
    string dataDir = Path.Combine(solutionDir, "sstables");
    
    await using var db = new WorldsSimplestDbV4(
        dataDirectory: dataDir,
        memtableFlushSize: 10000
    );
    
    // Initialisiere asynchron (WAL-Recovery, etc.)
    await db.InitializeAsync();
    
    long writtenBytes = 0;
    int entryCount = 0;
    
    while (writtenBytes < targetSize)
    {
        string key = $"key_{entryCount:D8}";
        string value = GenerateRandomValue(random, minLen, maxLen);
        
        await db.SetAsync(key, value);
        
        // Schätzung der geschriebenen Bytes (inkl. WAL + SSTable Overhead)
        // WAL: 4+keyLen+4+valueLen Bytes pro Entry
        // SSTable: Wird später geflusht, aber wir zählen es schon
        // Zähle WAL-Format (wird sofort geschrieben) + SSTable-Format (wird später geflusht)
        long estimatedBytes = 4 + Encoding.UTF8.GetByteCount(key) + 4 + Encoding.UTF8.GetByteCount(value); // WAL
        estimatedBytes += 4 + Encoding.UTF8.GetByteCount(key) + 4 + Encoding.UTF8.GetByteCount(value); // SSTable Data
        estimatedBytes += 4 + Encoding.UTF8.GetByteCount(key) + 8; // SSTable Index Entry
        // SSTable Header (20 Bytes pro Datei, ~10000 Einträge pro SSTable) = ~0.002 Bytes pro Entry
        // Wir ignorieren diesen kleinen Overhead für die Schätzung
        
        writtenBytes += estimatedBytes;
        entryCount++;
        
        // Progress basierend auf tatsächlich geschriebenen Bytes (näherungsweise)
        if (entryCount % 10000 == 0)
        {
            Console.WriteLine($"V4: {entryCount:N0} entries, ~{writtenBytes / (1024.0 * 1024.0):F2} MB " +
                            $"({db.GetStats().Replace("\n", " | ")})");
        }
    }
    
    // Final flush
    await db.FlushAsync();
    Console.WriteLine($"V4: Final flush completed. {db.GetStats()}");
    
    return (writtenBytes, entryCount);
}

string GenerateRandomValue(Random random, int minLen, int maxLen)
{
    int length = random.Next(minLen, maxLen + 1);
    const string chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789 ";
    return new string(Enumerable.Repeat(chars, length)
        .Select(s => s[random.Next(s.Length)]).ToArray());
}

async Task RunBenchmarkAsync(string[] args)
{
    // Parse arguments: [fillSize] [testKey] [iterations]
    string fillSize = "200mb";
    string testKey = "key_00050000";
    int iterations = 10;
    
    foreach (var arg in args)
    {
        if (arg.StartsWith("key_"))
        {
            testKey = arg;
        }
        else if (arg.EndsWith("mb") || arg.EndsWith("kb") || arg.EndsWith("gb") || arg.EndsWith("b"))
        {
            fillSize = arg;
        }
        else if (int.TryParse(arg, out int iter))
        {
            iterations = iter;
        }
    }
    
    long targetSizeBytes = ParseSize(fillSize);
    if (targetSizeBytes <= 0)
    {
        Console.WriteLine($"Invalid size format: {fillSize}");
        return;
    }
    
    Console.WriteLine("=== Database Performance Benchmark ===");
    Console.WriteLine($"Fill size: {fillSize}");
    Console.WriteLine($"Test Key: {testKey}");
    Console.WriteLine($"Read iterations per version: {iterations}");
    Console.WriteLine();
    
    // Benchmark Write Performance (Fill)
    Console.WriteLine("=== WRITE BENCHMARK (Filling Database) ===");
    await BenchmarkWrite("V1", targetSizeBytes, 50, 200);
    await BenchmarkWrite("V2", targetSizeBytes, 50, 200);
    await BenchmarkWrite("V3", targetSizeBytes, 50, 200);
    await BenchmarkWrite("V4", targetSizeBytes, 50, 200);
    
    Console.WriteLine();
    Console.WriteLine("=== READ BENCHMARK ===");
    
    // Benchmark Read Performance
    await BenchmarkRead("V1", testKey, iterations);
    await BenchmarkRead("V2", testKey, iterations);
    
    // V3 needs index building
    Console.WriteLine("\n=== Benchmarking V3 ===");
    Console.Write("  Building index");
    var indexStore = new IndexStore();
    indexStore.Load((msg) => Console.Write("."));
    var swIndex = Stopwatch.StartNew();
    swIndex.Stop();
    Console.WriteLine($" done ({swIndex.ElapsedMilliseconds}ms)");
    
    await BenchmarkRead("V3", testKey, iterations, indexStore);
    
    await BenchmarkRead("V4", testKey, iterations);
    
    Console.WriteLine();
    Console.WriteLine("=== Benchmark Complete ===");
}

async Task BenchmarkWrite(string version, long targetSizeBytes, int minValueLen, int maxValueLen)
{
    Console.WriteLine($"\n=== Benchmarking {version} Write ===");
    
    try
    {
        var sw = Stopwatch.StartNew();
        (long writtenBytes, int entryCount) result;
        
        switch (version)
        {
            case "V1":
                result = await FillV1(targetSizeBytes, minValueLen, maxValueLen);
                break;
            case "V2":
                result = await FillV2(targetSizeBytes, minValueLen, maxValueLen);
                break;
            case "V3":
                result = await FillV3(targetSizeBytes, minValueLen, maxValueLen);
                break;
            case "V4":
                result = await FillV4(targetSizeBytes, minValueLen, maxValueLen);
                break;
            default:
                throw new ArgumentException($"Unknown version: {version}");
        }
        
        sw.Stop();
        
        if (result.entryCount == 0)
        {
            Console.WriteLine("  Error: No entries written");
            return;
        }
        
        double mbWritten = result.writtenBytes / (1024.0 * 1024.0);
        double mbPerSecond = mbWritten / (sw.ElapsedMilliseconds / 1000.0);
        
        Console.WriteLine($"  Entries written: {result.entryCount:N0}");
        Console.WriteLine($"  Size: {mbWritten:F2} MB");
        Console.WriteLine($"  Time: {sw.ElapsedMilliseconds}ms ({sw.ElapsedMilliseconds / 1000.0:F2}s)");
        Console.WriteLine($"  Throughput: {mbPerSecond:F2} MB/s");
        Console.WriteLine($"  Avg time per entry: {(sw.ElapsedMilliseconds / (double)result.entryCount):F3}ms");
    }
    catch (Exception ex)
    {
        Console.WriteLine($"  Error: {ex.Message}");
    }
}

async Task BenchmarkRead(string version, string testKey, int iterations, IIndexStore? indexStore = null)
{
    Console.WriteLine($"\n=== Benchmarking {version} Read ===");
    
    try
    {
        Func<Task<string?>> getOperation = version switch
        {
            "V1" => async () => {
                using var db = new WorldsSimplestDbV1();
                return await db.GetAsync(testKey);
            },
            "V2" => async () => {
                using var db = new WorldsSimplestDbV2();
                return await db.GetAsync(testKey);
            },
            "V3" => async () => {
                using var db = new WorldsSimplestDbV3(indexStore ?? new IndexStore());
                return await db.GetAsync(testKey);
            },
            "V4" => async () => {
                await using var db = new WorldsSimplestDbV4();
                await db.InitializeAsync();
                return await db.GetAsync(testKey);
            },
            _ => throw new ArgumentException($"Unknown version: {version}")
        };
        
        // Warmup
        await getOperation();
        
        // Multiple runs for average
        var times = new List<long>();
        string? result = null;
        
        for (int i = 0; i < iterations; i++)
        {
            var sw = Stopwatch.StartNew();
            result = await getOperation();
            sw.Stop();
            times.Add(sw.ElapsedMilliseconds);
        }
        
        var avg = times.Average();
        var min = times.Min();
        var max = times.Max();
        var median = times.OrderBy(t => t).Skip(times.Count / 2).First();
        
        Console.WriteLine($"  Result: {(result != null ? "Found" : "Not found")}");
        Console.WriteLine($"  Average: {avg:F2}ms");
        Console.WriteLine($"  Median:  {median}ms");
        Console.WriteLine($"  Minimum: {min}ms");
        Console.WriteLine($"  Maximum: {max}ms");
    }
    catch (Exception ex)
    {
        Console.WriteLine($"  Error: {ex.Message}");
    }
}