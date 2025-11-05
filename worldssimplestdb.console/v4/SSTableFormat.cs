namespace worldssimplestdb.v4;

/// <summary>
/// Konstanten und Format-Definitionen für SSTable-Dateien
/// </summary>
public static class SSTableFormat
{
    /// <summary>
    /// Magic Number am Anfang jeder SSTable-Datei zur Validierung
    /// "SSTB" = 0x53535442
    /// </summary>
    public const int MagicNumber = 0x53535442;
    
    /// <summary>
    /// Aktuelle Format-Version
    /// </summary>
    public const int Version = 1;
    
    /// <summary>
    /// Standard-Größe der Memtable bevor Flush (in Einträgen)
    /// </summary>
    public const int DefaultMemtableFlushSize = 10000;
    
    /// <summary>
    /// Dateiformat:
    /// 
    /// Header (16 Bytes):
    /// ┌─────────────┬─────────┬─────────────┬─────────────┐
    /// │ MagicNumber │ Version │ EntryCount  │ IndexOffset │
    /// │   (4B)      │  (4B)   │    (4B)     │    (8B)     │
    /// └─────────────┴─────────┴─────────────┴─────────────┘
    /// 
    /// Data Section (variable):
    /// ┌─────────┬─────────┬─────────┬─────────┐ (wiederholt für jeden Entry)
    /// │ KeyLen  │ KeyData │ValueLen │ValueData│
    /// │  (4B)   │  (N B)  │  (4B)   │  (M B)  │
    /// └─────────┴─────────┴─────────┴─────────┘
    /// 
    /// Index Section (am Ende):
    /// ┌─────────┬─────────┬─────────┐ (wiederholt für jeden Entry)
    /// │ KeyLen  │ KeyData │ Offset  │
    /// │  (4B)   │  (N B)  │  (8B)   │
    /// └─────────┴─────────┴─────────┘
    /// 
    /// Der Index ermöglicht binäre Suche, da Keys sortiert sind!
    /// </summary>
    public const int HeaderSize = 20;
}

