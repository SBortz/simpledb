namespace worldssimplestdb.v4;

/// <summary>
/// Version 4: SSTable-basierte Implementation mit Log-Structured Merge Tree Prinzipien
/// 
/// Architektur mit Double-Buffering:
/// 
/// Write-Flow (Non-Blocking):
/// ┌──────────────┐
/// │  Memtable A  │ ← Aktive Memtable (neue Writes)
/// └──────┬───────┘
///        │ Bei Überlauf: Switch zu B (sehr schnell, nur Lock)
///        ▼
/// ┌──────────────┐
/// │  Memtable B  │ ← Neue aktive Memtable (Writes können sofort weiter)
/// └──────────────┘
/// 
/// Flush-Flow (Background, ohne Lock):
/// ┌──────────────┐
/// │  Memtable A  │ ← Geflusht im Background (immutable während Flush)
/// └──────┬───────┘
///        │ Async Flush (kann dauern, blockiert keine Writes!)
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
/// + Non-blocking Flush: Neue Writes können während Flush in neue Memtable
/// + Effiziente Reads durch binäre Suche in sortierten SSTables
/// + Immutable SSTables (keine Korruptionsgefahr)
/// + Sortierung ermöglicht Range-Queries (nicht implementiert, aber möglich)
/// + Gut skalierbar
/// + Compaction möglich (nicht implementiert, aber vorbereitet)
/// 
/// Nachteile:
/// - Komplexere Architektur
/// - Read-Amplification (mehrere Dateien müssen durchsucht werden)
/// - WAL-Overhead (jeder Write wird doppelt geschrieben: WAL + Memtable)
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
public class WorldsSimplestDbV4 : IDatabase, IAsyncDisposable
{
    private readonly string _dataDirectory;
    private readonly long _memtableFlushSizeBytes;
    private readonly SemaphoreSlim _writeLock = new(1, 1);
    private readonly SemaphoreSlim _readLock = new(1, 1); // Für Thread-sichere Reads
    
    private volatile Memtable _memtable; // volatile für Thread-Safety
    private readonly List<SSTableReader> _sstables = new();
    private bool _disposed = false;
    private Task? _currentFlushTask = null; // Track laufende Flush-Operation
    private readonly WriteAheadLog _wal; // Write-Ahead Log für Crash-Recovery
    
    public WorldsSimplestDbV4(string? dataDirectory = null, long memtableFlushSizeBytes = SSTableFormat.DefaultMemtableFlushSizeBytes, bool enableWAL = true)
    {
        _dataDirectory = dataDirectory ?? GetSolutionDatabasePath("sstables");
        _memtableFlushSizeBytes = memtableFlushSizeBytes;
        _memtable = new Memtable(_memtableFlushSizeBytes);
        
        // WAL initialisieren
        _wal = new WriteAheadLog();
        if (enableWAL)
        {
            _wal.Open();
        }
        
        // Lade existierende SSTables (synchron)
        LoadExistingSSTables();
    }
    
    /// <summary>
    /// Initialisiert die Datenbank asynchron (WAL-Recovery, etc.)
    /// Muss nach dem Konstruktor aufgerufen werden, bevor die Datenbank verwendet wird.
    /// </summary>
    public async Task InitializeAsync()
    {
        // Recovery: WAL replayen (falls vorhanden)
        await RecoverFromWALAsync();
        
        // Stelle sicher, dass WAL nach Recovery geöffnet ist (für neue Writes)
        if (_wal != null && !_disposed)
        {
            _wal.Open();
        }
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
    
    /// <summary>
    /// Recovered verlorene Daten aus dem WAL nach einem Crash
    /// </summary>
    private async Task RecoverFromWALAsync()
    {
        try
        {
            var walEntries = await WriteAheadLog.ReplayAsync();
            
            if (!walEntries.Any())
                return; // Kein WAL vorhanden oder leer
            
            Console.WriteLine($"Recovery: Found {walEntries.Count()} entries in WAL, replaying...");
            
            // Replay alle WAL-Entries in die Memtable
            foreach (var (key, value) in walEntries)
            {
                _memtable.Set(key, value);
            }
            
            Console.WriteLine($"Recovery: Replayed {walEntries.Count()} entries into memtable");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Warning: Error during WAL recovery: {ex.Message}");
        }
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
        // WICHTIG: WAL zuerst (Crash-Safety: "Write-Ahead")
        // Falls Crash nach WAL aber vor Memtable: Daten können replayed werden
        await _wal.AppendAsync(key, value);
        
        // Kurzes Lock nur für Write + ggf. Memtable-Switch
        await _writeLock.WaitAsync();
        try
        {
            var currentMemtable = _memtable;
            currentMemtable.Set(key, value);
            
            // Prüfe ob Flush nötig ist (aber noch nicht voll, um Race Conditions zu vermeiden)
            if (currentMemtable.IsFull)
            {
                // Switch zu neuer Memtable (sehr schnell, keine I/O)
                _memtable = new Memtable(_memtableFlushSizeBytes);
                
                // Starte Flush asynchron im Background (ohne Lock!)
                // Warte nicht darauf - neue Writes können sofort weiter
                _currentFlushTask = FlushMemtableInBackgroundAsync(currentMemtable);
            }
        }
        finally
        {
            _writeLock.Release();
        }
    }
    
    /// <summary>
    /// Flusht eine Memtable im Background (ohne Write-Lock zu halten)
    /// </summary>
    private async Task FlushMemtableInBackgroundAsync(Memtable memtableToFlush)
    {
        // Warte auf vorherigen Flush falls noch laufend
        if (_currentFlushTask != null && !_currentFlushTask.IsCompleted)
        {
            await _currentFlushTask;
        }
        
        if (memtableToFlush.Count == 0)
            return;
        
        try
        {
            // I/O-Operationen (können lang dauern, aber ohne Lock!)
            string sstableFile = await SSTableWriter.FlushAsync(memtableToFlush, _dataDirectory);
            
            // Kurzer Lock nur für SSTable-Liste Update
            await _writeLock.WaitAsync();
            try
            {
                var reader = new SSTableReader(sstableFile);
                _sstables.Insert(0, reader);
            }
            finally
            {
                _writeLock.Release();
            }
            
            // WAL löschen nach erfolgreichem Flush (Daten sind jetzt persistent in SSTable)
            // Öffnet automatisch neue WAL für nächste Memtable
            _wal.Clear();
        }
        catch (Exception ex)
        {
            // Logging könnte hier hinzugefügt werden
            Console.WriteLine($"Error flushing memtable: {ex.Message}");
            // WAL wird NICHT gelöscht - kann bei nächstem Start replayed werden
            throw;
        }
    }
    
    /// <summary>
    /// Flusht die aktuelle Memtable synchron (für Force-Flush)
    /// </summary>
    private async Task FlushCurrentMemtableAsync()
    {
        Memtable memtableToFlush;
        
        await _writeLock.WaitAsync();
        try
        {
            memtableToFlush = _memtable;
            _memtable = new Memtable(_memtableFlushSizeBytes); // Switch zu neuer Memtable
        }
        finally
        {
            _writeLock.Release();
        }
        
        // Flush ohne Lock
        await FlushMemtableInBackgroundAsync(memtableToFlush);
    }
    
    public async Task<string?> GetAsync(string searchKey)
    {
        // Snapshot der Memtable für Thread-Safety (keine Blockierung während Flush)
        Memtable memtableSnapshot;
        await _readLock.WaitAsync();
        try
        {
            memtableSnapshot = _memtable;
        }
        finally
        {
            _readLock.Release();
        }
        
        // 1. Prüfe zuerst Memtable (neueste Daten)
        if (memtableSnapshot.TryGet(searchKey, out var value))
            return value;
        
        // 2. Durchsuche SSTables von neuesten zu ältesten
        // Snapshot für Thread-Safety
        List<SSTableReader> sstablesSnapshot;
        await _readLock.WaitAsync();
        try
        {
            sstablesSnapshot = new List<SSTableReader>(_sstables);
        }
        finally
        {
            _readLock.Release();
        }
        
        foreach (var sstable in sstablesSnapshot)
        {
            var result = await sstable.GetAsync(searchKey);
            if (result != null)
                return result;
        }
        
        return null; // Key nicht gefunden
    }
    
    /// <summary>
    /// Force-Flush der aktuellen Memtable (für Tests oder Shutdown)
    /// Wartet auf alle laufenden Flush-Operationen
    /// </summary>
    public async Task FlushAsync()
    {
        // Warte auf laufenden Flush falls vorhanden
        if (_currentFlushTask != null && !_currentFlushTask.IsCompleted)
        {
            await _currentFlushTask;
        }
        
        // Flush aktuelle Memtable
        await FlushCurrentMemtableAsync();
        
        // Warte auf diesen Flush
        if (_currentFlushTask != null && !_currentFlushTask.IsCompleted)
        {
            await _currentFlushTask;
        }
    }

    /// <summary>
    /// Führt eine vollständige SSTable-Compaction durch (alle Tabellen werden zusammengeführt).
    /// </summary>
    public async Task CompactAsync()
    {
        // Stelle sicher, dass keine Daten mehr in der Memtable liegen.
        await FlushAsync();

        var mergedEntries = new Dictionary<string, string>(StringComparer.Ordinal);
        var oldReaders = new List<SSTableReader>();
        var oldFiles = new List<string>();
        string? newSstableFile = null;
        SSTableReader? newReader = null;
        bool compactionPerformed = false;

        await _writeLock.WaitAsync();
        await _readLock.WaitAsync();
        try
        {
            if (_sstables.Count <= 1)
            {
                return;
            }

            compactionPerformed = true;
            oldReaders = _sstables.ToList();
            oldFiles = oldReaders.Select(r => r.Filename).ToList();

            // Älteste zuerst, damit neuere Werte ältere überschreiben.
            for (int i = oldReaders.Count - 1; i >= 0; i--)
            {
                foreach (var entry in oldReaders[i].ReadAllEntries())
                {
                    mergedEntries[entry.Key] = entry.Value;
                }
            }

            _sstables.Clear();

            if (mergedEntries.Count > 0)
            {
                var compactedMemtable = new Memtable(long.MaxValue);
                foreach (var kvp in mergedEntries.OrderBy(kvp => kvp.Key, StringComparer.Ordinal))
                {
                    compactedMemtable.Set(kvp.Key, kvp.Value);
                }

                newSstableFile = await SSTableWriter.FlushAsync(compactedMemtable, _dataDirectory);
                newReader = new SSTableReader(newSstableFile);
                _sstables.Add(newReader);
            }
        }
        finally
        {
            _readLock.Release();
            _writeLock.Release();
        }

        if (!compactionPerformed)
            return;

        // Aufräumen außerhalb der Locks, um Blockierungen zu vermeiden.
        foreach (var reader in oldReaders)
        {
            reader.Dispose();
        }

        foreach (var file in oldFiles)
        {
            if (newSstableFile != null && string.Equals(file, newSstableFile, StringComparison.OrdinalIgnoreCase))
                continue;

            try
            {
                if (File.Exists(file))
                {
                    File.Delete(file);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Warning: Could not delete SSTable {file}: {ex.Message}");
            }
        }
    }
    
    /// <summary>
    /// Gibt Statistiken über die Datenbank zurück
    /// </summary>
    public string GetStats()
    {
        return $"Memtable: {_memtable.Count} entries (~{_memtable.ApproximateSizeBytes / (1024.0 * 1024.0):F1} MB)\n" +
               $"SSTables: {_sstables.Count} files\n" +
               $"Total SSTable entries: {_sstables.Sum(s => s.EntryCount)}";
    }
    
    public void Dispose()
    {
        DisposeAsync().AsTask().GetAwaiter().GetResult();
    }
    
    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        
        // Flush Memtable vor dem Beenden (mit WAL-Clear)
        // WICHTIG: FlushAsync() ruft FlushMemtableInBackgroundAsync auf,
        // welches _wal.Clear() aufruft, wenn der Flush erfolgreich ist
        await FlushAsync();
        
        // Stelle sicher, dass alle Memtable-Einträge geflusht sind
        // Falls noch Einträge in der Memtable sind, flushen wir nochmal
        while (_memtable.Count > 0)
        {
            await FlushAsync();
        }
        
        // Leere WAL final (falls noch vorhanden - für den Fall, dass Flush fehlgeschlagen ist)
        // Normalerweise wird WAL durch erfolgreichen Flush bereits geleert
        if (_wal != null && File.Exists(_wal.WalFilePath))
        {
            try
            {
                _wal.Clear();
            }
            catch
            {
                // Ignoriere Fehler beim finalen Clear (wird beim Dispose aufgeräumt)
            }
        }
        
        // Dispose alle SSTableReader
        foreach (var sstable in _sstables)
        {
            sstable.Dispose();
        }
        
        _wal?.Dispose();
        _writeLock?.Dispose();
        _readLock?.Dispose();
        _disposed = true;
    }
}

