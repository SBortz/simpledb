namespace worldssimplestdb.v4;

/// <summary>
/// In-Memory sortierte Tabelle für neue Schreibzugriffe
/// 
/// Verwendet SortedDictionary für automatische Sortierung der Keys.
/// Wenn die Memtable voll ist, wird sie als SSTable auf Disk geflusht.
/// </summary>
public class Memtable
{
    private readonly SortedDictionary<string, string> _data = new();
    private readonly int _maxSize;
    
    public Memtable(int maxSize = SSTableFormat.DefaultMemtableFlushSize)
    {
        _maxSize = maxSize;
    }
    
    public int Count => _data.Count;
    
    public bool IsFull => _data.Count >= _maxSize;
    
    /// <summary>
    /// Fügt einen Key-Value Eintrag zur Memtable hinzu
    /// </summary>
    public void Set(string key, string value)
    {
        _data[key] = value;
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
    }
}

