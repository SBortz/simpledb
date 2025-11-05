using System.Text;

namespace worldssimplestdb.v4;

/// <summary>
/// Write-Ahead Log (WAL) für Crash-Recovery
/// 
/// Jeder Write wird VOR dem Memtable-Insert ins WAL geschrieben.
/// Bei einem Crash kann das WAL replayed werden, um verlorene Daten wiederherzustellen.
/// 
/// Format: Append-only Log mit Entries im Format:
/// ┌─────────┬─────────┬─────────┬─────────┐
/// │ KeyLen  │ KeyData │ValueLen │ValueData│
/// │  (4B)   │  (N B)  │  (4B)   │  (M B)  │
/// └─────────┴─────────┴─────────┴─────────┘
/// </summary>
public class WriteAheadLog : IDisposable
{
    private readonly string _walFile;
    private readonly SemaphoreSlim _writeLock = new(1, 1);
    private FileStream? _walStream;
    private BinaryWriter? _walWriter;
    private bool _disposed = false;
    
    /// <summary>
    /// Gibt den Pfad zur WAL-Datei zurück (für externe Checks)
    /// </summary>
    public string WalFilePath => _walFile;
    
    public WriteAheadLog(string? walFile = null)
    {
        _walFile = walFile ?? GetSolutionDatabasePath("wal.log");
    }
    
    private static string GetSolutionDatabasePath(string fileName)
    {
        var currentDir = Directory.GetCurrentDirectory();
        var dir = new DirectoryInfo(currentDir);
        
        while (dir != null && !dir.GetFiles("*.sln").Any())
        {
            dir = dir.Parent;
        }
        
        return dir != null ? Path.Combine(dir.FullName, fileName) : fileName;
    }
    
    /// <summary>
    /// Öffnet das WAL für Schreibzugriffe (thread-safe)
    /// </summary>
    public void Open()
    {
        _writeLock.Wait();
        try
        {
            if (_walStream != null)
                return;
            
            // Append-Modus: Fügt an bestehende Datei an (für Recovery)
            _walStream = new FileStream(_walFile, FileMode.Append, FileAccess.Write, FileShare.Read, bufferSize: 64 * 1024);
            _walWriter = new BinaryWriter(_walStream, Encoding.UTF8, leaveOpen: true);
        }
        finally
        {
            _writeLock.Release();
        }
    }
    
    /// <summary>
    /// Schreibt einen Entry ins WAL (synchron, muss persistiert sein!)
    /// </summary>
    public async Task AppendAsync(string key, string value)
    {
        await _writeLock.WaitAsync();
        try
        {
            // Prüfe ob WAL noch geöffnet ist (könnte durch Clear() geschlossen worden sein)
            if (_walWriter == null || _walStream == null)
            {
                // WAL wurde geschlossen, öffne es wieder
                _walStream = new FileStream(_walFile, FileMode.Append, FileAccess.Write, FileShare.Read, bufferSize: 64 * 1024);
                _walWriter = new BinaryWriter(_walStream, Encoding.UTF8, leaveOpen: true);
            }
            
            byte[] keyBytes = Encoding.UTF8.GetBytes(key);
            byte[] valueBytes = Encoding.UTF8.GetBytes(value);
            
            _walWriter.Write(keyBytes.Length);
            _walWriter.Write(keyBytes);
            _walWriter.Write(valueBytes.Length);
            _walWriter.Write(valueBytes);
            
            // WICHTIG: Flush sofort auf Disk (für Crash-Safety)
            await _walStream.FlushAsync();
        }
        finally
        {
            _writeLock.Release();
        }
    }
    
    /// <summary>
    /// Replayt alle Entries aus dem WAL
    /// </summary>
    public static Task<IEnumerable<(string key, string value)>> ReplayAsync(string? walFile = null)
    {
        walFile ??= GetSolutionDatabasePath("wal.log");
        
        if (!File.Exists(walFile))
            return Task.FromResult(Enumerable.Empty<(string, string)>());
        
        var entries = new List<(string, string)>();
        
        using var fs = new FileStream(walFile, FileMode.Open, FileAccess.Read, FileShare.ReadWrite, bufferSize: 64 * 1024);
        using var br = new BinaryReader(fs, Encoding.UTF8);
        
        while (fs.Position < fs.Length)
        {
            try
            {
                int keyLen = br.ReadInt32();
                byte[] keyBytes = br.ReadBytes(keyLen);
                int valueLen = br.ReadInt32();
                byte[] valueBytes = br.ReadBytes(valueLen);
                
                string key = Encoding.UTF8.GetString(keyBytes);
                string value = Encoding.UTF8.GetString(valueBytes);
                
                entries.Add((key, value));
            }
            catch (EndOfStreamException)
            {
                // Datei ist möglicherweise unvollständig (Crash während Write)
                // Ignoriere diesen Entry und breche ab
                break;
            }
            catch (Exception ex)
            {
                // Logging könnte hier hinzugefügt werden
                Console.WriteLine($"Warning: Error reading WAL entry at position {fs.Position}: {ex.Message}");
                break;
            }
        }
        
        return Task.FromResult<IEnumerable<(string, string)>>(entries);
    }
    
    /// <summary>
    /// Markiert das WAL als "aufgearbeitet" - kann nach erfolgreichem Flush gelöscht werden (thread-safe)
    /// </summary>
    public void Clear()
    {
        if (_disposed) return;
        
        // Warte auf alle laufenden Writes
        _writeLock.Wait();
        try
        {
            _walWriter?.Dispose();
            _walStream?.Dispose();
            _walWriter = null;
            _walStream = null;
            
            // Lösche alte WAL-Datei
            if (File.Exists(_walFile))
            {
                File.Delete(_walFile);
            }
            
            // Öffne neue WAL für nächste Memtable (ohne Lock, da bereits gelockt)
            if (!_disposed)
            {
                _walStream = new FileStream(_walFile, FileMode.Append, FileAccess.Write, FileShare.Read, bufferSize: 64 * 1024);
                _walWriter = new BinaryWriter(_walStream, Encoding.UTF8, leaveOpen: true);
            }
        }
        finally
        {
            _writeLock.Release();
        }
    }
    
    public void Dispose()
    {
        if (_disposed) return;
        
        _walWriter?.Dispose();
        _walStream?.Dispose();
        _writeLock?.Dispose();
        _disposed = true;
    }
}

