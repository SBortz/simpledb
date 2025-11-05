namespace worldssimplestdb.v4;

/// <summary>
/// Version 4: SSTable-basierte Implementation mit Log-Structured Merge Tree Prinzipien
/// 
/// Architektur:
/// ┌──────────────┐
/// │  Memtable    │ ← Neue Writes (in-memory, sortiert)
/// └──────┬───────┘
///        │ Flush bei Überlauf
///        ▼
/// ┌──────────────┐
/// │ SSTable 1    │ ← Neueste (immutable, sortiert)
/// ├──────────────┤
/// │ SSTable 2    │
/// ├──────────────┤
/// │ SSTable 3    │ ← Älteste
/// └──────────────┘
/// 
/// Eigenschaften:
/// - Write: O(log n) - Insert in SortedDictionary (Memtable)
/// - Read: O(log n * m) - Memtable + binäre Suche in m SSTables
/// - Speicher: Sortierte SSTables mit Index für binäre Suche
/// 
/// Vorteile:
/// + Sehr schnelle Writes (nur in-memory bis Flush)
/// + Effiziente Reads durch binäre Suche in sortierten SSTables
/// + Immutable SSTables (keine Korruptionsgefahr)
/// + Sortierung ermöglicht Range-Queries (nicht implementiert, aber möglich)
/// + Gut skalierbar
/// + Compaction möglich (nicht implementiert, aber vorbereitet)
/// 
/// Nachteile:
/// - Komplexere Architektur
/// - Read-Amplification (mehrere Dateien müssen durchsucht werden)
/// - Memtable-Daten gehen bei Crash verloren (WAL nicht implementiert)
/// - SSTables können fragmentiert werden (Compaction nötig)
/// 
/// Unterschiede zu V3:
/// + Keys sind sortiert (ermöglicht binäre Suche und Range-Queries)
/// + Write-Buffer in Memory (bessere Write-Performance)
/// + Mehrere immutable Dateien statt einer großen
/// + Erweiterbar mit Compaction, Bloom-Filters, WAL
/// 
/// Use Case: Moderne Anwendungen mit hohem Write-Throughput,
///           wo sortierte Daten und Range-Queries nützlich sind
/// </summary>
public class WorldsSimplestDbV4 : IDatabase
{
    private readonly string _dataDirectory;
    private readonly int _memtableFlushSize;
    private readonly SemaphoreSlim _writeLock = new(1, 1);
    
    private Memtable _memtable;
    private readonly List<SSTableReader> _sstables = new();
    private bool _disposed = false;
    
    public WorldsSimplestDbV4(string? dataDirectory = null, int memtableFlushSize = SSTableFormat.DefaultMemtableFlushSize)
    {
        _dataDirectory = dataDirectory ?? GetSolutionDatabasePath("sstables");
        _memtableFlushSize = memtableFlushSize;
        _memtable = new Memtable(_memtableFlushSize);
        
        // Lade existierende SSTables
        LoadExistingSSTables();
    }
    
    private static string GetSolutionDatabasePath(string directoryName)
    {
        var currentDir = Directory.GetCurrentDirectory();
        var dir = new DirectoryInfo(currentDir);
        
        while (dir != null && !dir.GetFiles("*.sln").Any())
        {
            dir = dir.Parent;
        }
        
        return dir != null ? Path.Combine(dir.FullName, directoryName) : directoryName;
    }
    
    private void LoadExistingSSTables()
    {
        if (!Directory.Exists(_dataDirectory))
            return;
        
        var sstableFiles = Directory.GetFiles(_dataDirectory, "sstable_*.sst")
            .OrderByDescending(f => File.GetCreationTimeUtc(f)) // Neueste zuerst
            .ToList();
        
        foreach (var file in sstableFiles)
        {
            try
            {
                _sstables.Add(new SSTableReader(file));
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Warning: Could not load SSTable {file}: {ex.Message}");
            }
        }
    }
    
    public async Task SetAsync(string key, string value)
    {
        await _writeLock.WaitAsync();
        try
        {
            _memtable.Set(key, value);
            
            // Flush wenn Memtable voll ist
            if (_memtable.IsFull)
            {
                await FlushMemtableAsync();
            }
        }
        finally
        {
            _writeLock.Release();
        }
    }
    
    private async Task FlushMemtableAsync()
    {
        if (_memtable.Count == 0)
            return;
        
        // Schreibe aktuelle Memtable als SSTable
        string sstableFile = await SSTableWriter.FlushAsync(_memtable, _dataDirectory);
        
        // Öffne neue SSTable und füge sie am Anfang ein (neueste zuerst)
        var reader = new SSTableReader(sstableFile);
        _sstables.Insert(0, reader);
        
        // Erstelle neue leere Memtable
        _memtable.Clear();
    }
    
    public async Task<string?> GetAsync(string searchKey)
    {
        // 1. Prüfe zuerst Memtable (neueste Daten)
        if (_memtable.TryGet(searchKey, out var value))
            return value;
        
        // 2. Durchsuche SSTables von neuesten zu ältesten
        foreach (var sstable in _sstables)
        {
            var result = await sstable.GetAsync(searchKey);
            if (result != null)
                return result;
        }
        
        return null; // Key nicht gefunden
    }
    
    /// <summary>
    /// Force-Flush der aktuellen Memtable (für Tests oder Shutdown)
    /// </summary>
    public async Task FlushAsync()
    {
        await _writeLock.WaitAsync();
        try
        {
            await FlushMemtableAsync();
        }
        finally
        {
            _writeLock.Release();
        }
    }
    
    /// <summary>
    /// Gibt Statistiken über die Datenbank zurück
    /// </summary>
    public string GetStats()
    {
        return $"Memtable: {_memtable.Count} entries\n" +
               $"SSTables: {_sstables.Count} files\n" +
               $"Total SSTable entries: {_sstables.Sum(s => s.EntryCount)}";
    }
    
    public void Dispose()
    {
        if (_disposed) return;
        
        // Flush Memtable vor dem Beenden
        FlushAsync().GetAwaiter().GetResult();
        
        // Dispose alle SSTableReader
        foreach (var sstable in _sstables)
        {
            sstable.Dispose();
        }
        
        _writeLock?.Dispose();
        _disposed = true;
    }
}

