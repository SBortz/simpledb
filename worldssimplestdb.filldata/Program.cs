using System.Text;

if (args.Length < 4)
{
    Console.WriteLine("Usage: FillData <version> <size> <minValueLen> <maxValueLen>");
    Console.WriteLine("Versions: v1, v2, v3");
    Console.WriteLine("Size: 10mb, 100mb, 1gb, etc.");
    Console.WriteLine("Example: FillData v3 10mb 50 200");
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
long writtenBytes = 0;
int entryCount = 0;

switch (version)
{
    case "v1":
        await FillV1(targetSizeBytes, minValueLen, maxValueLen, () => writtenBytes, () => entryCount);
        break;
    case "v2":
        await FillV2(targetSizeBytes, minValueLen, maxValueLen, () => writtenBytes, () => entryCount);
        break;
    case "v3":
        await FillV3(targetSizeBytes, minValueLen, maxValueLen, () => writtenBytes, () => entryCount);
        break;
    default:
        Console.WriteLine($"Unknown version: {version}");
        return;
}

sw.Stop();
Console.WriteLine($"Done! Wrote {entryCount:N0} entries ({writtenBytes / (1024.0 * 1024.0):F2} MB) in {sw.ElapsedMilliseconds}ms");

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

async Task FillV1(long targetSize, int minLen, int maxLen, Func<long> getWrittenBytes, Func<int> getEntryCount)
{
    var random = new Random(42);
    
    string dbPath = Path.GetFullPath(Path.Combine(Directory.GetCurrentDirectory(), "database.txt"));
    await using var fs = new FileStream(dbPath, FileMode.Create, FileAccess.Write, FileShare.None, bufferSize: 65536);
    await using var writer = new StreamWriter(fs);
    
    while (getWrittenBytes() < targetSize)
    {
        string key = $"key_{getEntryCount():D8}";
        string value = GenerateRandomValue(random, minLen, maxLen);
        
        await writer.WriteLineAsync($"{key};{value}");
        writtenBytes += key.Length + value.Length + 2; // +2 for semicolon and newline
        entryCount++;
        
        if (entryCount % 10000 == 0)
        {
            Console.WriteLine($"V1: {entryCount:N0} entries, {getWrittenBytes() / (1024.0 * 1024.0):F2} MB");
        }
    }
}

async Task FillV2(long targetSize, int minLen, int maxLen, Func<long> getWrittenBytes, Func<int> getEntryCount)
{
    var random = new Random(42);
    
    string dbPath = Path.GetFullPath(Path.Combine(Directory.GetCurrentDirectory(), "database.bin"));
    await using var fs = new FileStream(dbPath, FileMode.Create, FileAccess.Write, FileShare.None, bufferSize: 65536);
    await using var writer = new BinaryWriter(fs);
    
    while (getWrittenBytes() < targetSize)
    {
        string key = $"key_{getEntryCount():D8}";
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
            Console.WriteLine($"V2: {entryCount:N0} entries, {getWrittenBytes() / (1024.0 * 1024.0):F2} MB");
        }
    }
}

async Task FillV3(long targetSize, int minLen, int maxLen, Func<long> getWrittenBytes, Func<int> getEntryCount)
{
    var random = new Random(42);
    var indexDict = new Dictionary<string, long>();
    
    string dbPath = Path.GetFullPath(Path.Combine(Directory.GetCurrentDirectory(), "database.bin"));
    await using var fs = new FileStream(dbPath, FileMode.Create, FileAccess.Write, FileShare.None, bufferSize: 65536);
    await using var writer = new BinaryWriter(fs);
    
    while (getWrittenBytes() < targetSize)
    {
        string key = $"key_{getEntryCount():D8}";
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
            Console.WriteLine($"V3: {entryCount:N0} entries, {getWrittenBytes() / (1024.0 * 1024.0):F2} MB");
        }
    }
}

string GenerateRandomValue(Random random, int minLen, int maxLen)
{
    int length = random.Next(minLen, maxLen + 1);
    const string chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789 ";
    return new string(Enumerable.Repeat(chars, length)
        .Select(s => s[random.Next(s.Length)]).ToArray());
}