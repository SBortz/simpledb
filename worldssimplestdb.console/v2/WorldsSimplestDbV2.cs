using System.Text;

namespace worldssimplestdb.v2;

// Data format
// ┌─────────┬─────────┬─────────┬─────────┐
// │ keyLen  │ keyData │valueLen │valueData│
// │ (4B)    │ (N B)   │ (4B)    │ (M B)   │
// └─────────┴─────────┴─────────┴─────────┘

public class WorldsSimplestDbV2(string? dataFile = null) : IDatabase
{
    private readonly string _dataFile = dataFile ?? GetSolutionDatabasePath("database.bin");
    private readonly SemaphoreSlim writeSemaphore = new(1, 1);
    private bool disposed = false;
    
    private static string GetSolutionDatabasePath(string fileName)
    {
        // Find the solution directory by looking for .sln file
        var currentDir = Directory.GetCurrentDirectory();
        var dir = new DirectoryInfo(currentDir);
        
        while (dir != null && !dir.GetFiles("*.sln").Any())
        {
            dir = dir.Parent;
        }
        
        return dir != null ? Path.Combine(dir.FullName, fileName) : fileName;
    }
    
    public async Task SetAsync(string key, string value)
    {
        await writeSemaphore.WaitAsync();
        try
        {
            byte[] keyData = Encoding.UTF8.GetBytes(key);
            byte[] valueData = Encoding.UTF8.GetBytes(value);

            await using var fs = new FileStream(_dataFile, FileMode.Append, FileAccess.Write, FileShare.Read, bufferSize: 4096, useAsync: true);
            await using var bw = new BinaryWriter(fs, Encoding.UTF8, leaveOpen: true);
            bw.Write(keyData.Length);
            bw.Write(keyData);
            bw.Write(valueData.Length);
            bw.Write(valueData);
            await fs.FlushAsync();
        }
        finally
        {
            writeSemaphore.Release();
        }
    }

    public async Task<string?> GetAsync(string searchKey)
    {
        string? last = null;
        await using var fs = new FileStream(_dataFile, FileMode.OpenOrCreate, FileAccess.Read, FileShare.ReadWrite, bufferSize: 4096, useAsync: true);
        using var br = new BinaryReader(fs);

        while (fs.Position < fs.Length)
        {
            int keyLen = br.ReadInt32();
            byte[] keyBuf = br.ReadBytes(keyLen);
            int valueLen = br.ReadInt32();
            byte[] valueBuf = br.ReadBytes(valueLen);
            
            string key = Encoding.UTF8.GetString(keyBuf);
            if (key == searchKey)
                last = Encoding.UTF8.GetString(valueBuf);
        }
        return last;
    }

    public void Dispose()
    {
        if (disposed) return;
        writeSemaphore?.Dispose();
        disposed = true;
    }
}