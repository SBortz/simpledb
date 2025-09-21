using System.Text;

namespace worldssimplestdb.v1;

public class WorldsSimplestDbV1(string dataFile = "database.txt") : IDatabase
{
    private readonly SemaphoreSlim writeSemaphore = new(1, 1);
    private bool disposed;
    
    public async Task SetAsync(string key, string value)
    {
        await writeSemaphore.WaitAsync();
        try
        {
            await File.AppendAllTextAsync(dataFile, $"{key};{value}{Environment.NewLine}", Encoding.UTF8);
        }
        finally
        {
            writeSemaphore.Release();
        }
    }

    public async Task<string?> GetAsync(string searchKey) =>
        (await File.ReadAllLinesAsync(dataFile, Encoding.UTF8))
            .Select(line => line.Split(';', 2))
            .Where(parts => parts[0] == searchKey)
            .Select(parts => parts[1])
            .LastOrDefault();

    public void Dispose()
    {
        if (disposed) return;
        writeSemaphore?.Dispose();
        disposed = true;
    }
}