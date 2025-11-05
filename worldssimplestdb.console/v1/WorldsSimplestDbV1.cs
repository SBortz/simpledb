using System.Text;

namespace worldssimplestdb.v1;

/// <summary>
/// Version 1: Einfachste Implementation mit Text-Format
/// 
/// Speicherformat: Jeder Eintrag ist eine Textzeile im Format "key;value\n"
/// 
/// Eigenschaften:
/// - Write: O(1) - Neue Einträge werden einfach an die Datei angehängt
/// - Read: O(n) - Die gesamte Datei wird bei jedem Lesevorgang durchsucht
/// - Speicher: Text-Format, einfach zu debuggen aber nicht platzsparend
/// 
/// Vorteile:
/// + Sehr einfach zu verstehen und zu debuggen
/// + Datei ist human-readable
/// + Minimaler Code-Aufwand
/// 
/// Nachteile:
/// - Sehr langsam bei vielen Einträgen (vollständiger Scan bei jedem Get)
/// - Ineffizientes Speicherformat (Text-Encoding Overhead)
/// - Keine Optimierung möglich
/// 
/// Use Case: Prototyping, sehr kleine Datenmengen (&lt;1000 Einträge)
/// </summary>
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