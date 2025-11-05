using System.Text;

namespace worldssimplestdb.v3;

/// <summary>
/// In-Memory Index für WorldsSimplestDbV3
/// 
/// Verwaltet eine Dictionary&lt;string, long&gt; die jeden Key auf seine Position
/// in der Datenbankdatei mappt. Ermöglicht O(1) Lookups ohne vollständigen Datei-Scan.
/// 
/// Der Index wird beim Start durch Scannen der gesamten Datei aufgebaut (Load-Methode)
/// und dann bei jedem Write-Vorgang im Speicher aktualisiert.
/// </summary>
public class IndexStore(string? dataFile = null) : IIndexStore
{
    private readonly string _dataFile = dataFile ?? GetSolutionDatabasePath("database.bin");
    private Dictionary<string, long> indexDict = new();
    
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

    public void Load(Action<string>? feedback = null)
    {
        if (!File.Exists(_dataFile)) 
        {
            feedback?.Invoke("No database file found, starting with empty index");
            return;
        }
        
        feedback?.Invoke($"Building index from database file...");
        var sw = System.Diagnostics.Stopwatch.StartNew();
        
        indexDict.Clear();
        
        using var fs = new FileStream(_dataFile, FileMode.Open, FileAccess.Read, FileShare.ReadWrite, bufferSize: 1 << 20);
        using var br = new BinaryReader(fs);
        
        long entries = 0;

        while (fs.Position < fs.Length)
        {
            long position = fs.Position;
            
            // Read: keyLen + keyData + valueLen
            int keyLen = br.ReadInt32();
            byte[] keyBuf = br.ReadBytes(keyLen);
            int valueLen = br.ReadInt32();
            
            // Skip over valueData, we only need the key
            fs.Seek(valueLen, SeekOrigin.Current);
            
            string key = Encoding.UTF8.GetString(keyBuf);
            indexDict[key] = position;
            entries++;
            
            // Progress feedback alle 10000 Einträge
            if (entries % 10000 == 0)
            {
                feedback?.Invoke($"Building index... {entries:N0} entries processed");
            }
        }
        
        sw.Stop();
        feedback?.Invoke($"Index built: {entries:N0} entries in {sw.ElapsedMilliseconds}ms");
    }

    public bool TryGetValue(string key, out long offset)
    {
        return this.indexDict.TryGetValue(key, out offset);
    }

    public void WriteEntry(string key, long offset)
    {
        // Update in-memory index only
        indexDict[key] = offset;
    }
}