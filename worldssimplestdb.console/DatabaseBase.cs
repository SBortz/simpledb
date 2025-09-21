using System.Diagnostics;
using System.Text;
using worldssimplestdb.dbv1;
using worldssimplestdb.v2;
using worldssimplestdb.v3;

namespace worldssimplestdb.console;

// Abstract base class for all database versions
public abstract class DatabaseBase : IDisposable
{
    public abstract Task SetAsync(string key, string value);
    public abstract Task<string?> GetAsync(string key);
    public abstract void Dispose();
    
    // Optional methods for versions that support them
    public virtual Task<string?> GetIndexedAsync(string key) => GetAsync(key);
    public virtual Task<string?> GetScanAsync(string key) => GetAsync(key);
}

// V1 Implementation
public class DatabaseV1 : DatabaseBase
{
    private readonly WorldsSimplestDbV1 db;
    
    public DatabaseV1() => db = new WorldsSimplestDbV1();
    
    public override async Task SetAsync(string key, string value) => await db.SetAsync(key, value);
    public override async Task<string?> GetAsync(string key) => await db.GetAsync(key);
    public override void Dispose() => db.Dispose();
}

// V2 Implementation (same as V1)
public class DatabaseV2 : DatabaseBase
{
    private readonly WorldsSimplestDbV2 db;
    
    public DatabaseV2() => db = new WorldsSimplestDbV2();
    
    public override async Task SetAsync(string key, string value) => await db.SetAsync(key, value);
    public override async Task<string?> GetAsync(string key) => await db.GetAsync(key);
    public override void Dispose() => db.Dispose();
}

// V3 Implementation
public class DatabaseV3 : DatabaseBase
{
    private readonly WorldsSimplestDbV3 db;
    private readonly IndexCache indexCache;
    
    public DatabaseV3()
    {
        indexCache = new IndexCache();
        db = new WorldsSimplestDbV3(indexCache);
        LoadIndexWithFeedback(indexCache);
    }
    
    public override async Task SetAsync(string key, string value) => await db.SetAsync(key, value);
    public override async Task<string?> GetAsync(string key) => await db.GetIndexedAsync(key);
    public override async Task<string?> GetIndexedAsync(string key) => await db.GetIndexedAsync(key);
    public override async Task<string?> GetScanAsync(string key) => await db.GetScanAsync(key);
    public override void Dispose() => db.Dispose();
    
    private void LoadIndexWithFeedback(IIndexCache indexCache)
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
}