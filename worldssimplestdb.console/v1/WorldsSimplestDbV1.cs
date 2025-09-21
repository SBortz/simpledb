using System.Text;

namespace worldssimplestdb.v1;

public class WorldsSimplestDbV1(string? dataFile = null) : IDatabase
{
    private readonly string _dataFile = dataFile ?? GetSolutionDatabasePath("database.txt");
    private readonly SemaphoreSlim writeSemaphore = new(1, 1);
    private bool disposed;
    
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
            await File.AppendAllTextAsync(_dataFile, $"{key};{value}{Environment.NewLine}", Encoding.UTF8);
        }
        finally
        {
            writeSemaphore.Release();
        }
    }

    public async Task<string?> GetAsync(string searchKey) =>
        (await File.ReadAllLinesAsync(_dataFile, Encoding.UTF8))
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