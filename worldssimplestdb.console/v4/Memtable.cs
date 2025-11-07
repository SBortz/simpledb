namespace worldssimplestdb.v4;

/// <summary>
/// In-Memory sortierte Tabelle für neue Schreibzugriffe
/// 
/// Verwendet SortedDictionary für automatische Sortierung der Keys.
/// Intern implementiert als Red-Black Tree (selbstbalancierender binärer Suchbaum).
/// 
/// Performance-Charakteristiken:
/// - Insert (Set): O(log n) - Red-Black Tree Insert mit Balancing
/// - Lookup (TryGet): O(log n) - Binäre Suche im Baum
/// - Iteration (GetSortedEntries): O(n) - In-Order Traversal
/// - Speicher: O(n) - Linear mit Anzahl der Einträge
/// 
/// Die Red-Black Tree Implementierung garantiert:
/// - Automatische Sortierung der Keys
/// - O(log n) Höhe des Baums (garantiert balanciert)
/// - Effiziente Writes auch bei vielen Einträgen
/// 
/// Wenn die Memtable voll ist, wird sie als SSTable auf Disk geflusht.
/// </summary>
public class Memtable
{
    private readonly SortedDictionary<string, string> _data = new();
    private readonly long _maxBytes;
    private long _currentBytes;
    
    public Memtable(long maxBytes = SSTableFormat.DefaultMemtableFlushSizeBytes)
    {
        _maxBytes = maxBytes;
    }
    
    public int Count => _data.Count;
    
    public long ApproximateSizeBytes => _currentBytes;
    
    public bool IsFull => _currentBytes >= _maxBytes;
    
    /// <summary>
    /// Fügt einen Key-Value Eintrag zur Memtable hinzu
    /// </summary>
    public void Set(string key, string value)
    {
        if (_data.TryGetValue(key, out var existingValue))
        {
            _currentBytes -= EstimateBytes(key, existingValue);
        }
        
        _data[key] = value;
        _currentBytes += EstimateBytes(key, value);
    }
    
    /// <summary>
    /// Sucht einen Key in der Memtable
    /// </summary>
    public bool TryGet(string key, out string? value)
    {
        return _data.TryGetValue(key, out value);
    }
    
    /// <summary>
    /// Gibt alle Einträge in sortierter Reihenfolge zurück
    /// </summary>
    public IEnumerable<KeyValuePair<string, string>> GetSortedEntries()
    {
        return _data;
    }
    
    /// <summary>
    /// Löscht alle Einträge (nach Flush)
    /// </summary>
    public void Clear()
    {
        _data.Clear();
        _currentBytes = 0;
    }

    private static long EstimateBytes(string key, string value)
    {
        // Schätzung auf Basis UTF-8 Encoding + 8 Byte Length Prefix Overhead (Key + Value)
        int keyBytes = System.Text.Encoding.UTF8.GetByteCount(key);
        int valueBytes = System.Text.Encoding.UTF8.GetByteCount(value);
        return keyBytes + valueBytes + sizeof(int) * 2;
    }
}

