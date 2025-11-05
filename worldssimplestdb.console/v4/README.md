# WorldsSimplestDB V4 - SSTable Implementation

## Überblick

Version 4 implementiert **SSTables (Sorted String Tables)**, ein fundamentales Konzept aus modernen NoSQL-Datenbanken wie LevelDB, RocksDB, Cassandra und Apache HBase.

## Architektur

```
┌─────────────────────────────────────────────────┐
│              WorldsSimplestDbV4                 │
└───────────┬─────────────────────────────────────┘
            │
            ├─► Memtable (In-Memory)
            │   └─ SortedDictionary<string, string>
            │   └─ Neue Writes landen hier
            │
            └─► SSTable Files (On-Disk)
                ├─ sstable_20241105_120000.sst  ← Neueste
                ├─ sstable_20241105_110000.sst
                └─ sstable_20241105_100000.sst  ← Älteste
```

## Komponenten

### 1. **Memtable** (In-Memory Schreibpuffer)
- Verwendet `SortedDictionary` für automatische Key-Sortierung
- Neue Schreibzugriffe landen zuerst hier
- Wenn voll (Standard: 10.000 Einträge), wird sie als SSTable geflusht

### 2. **SSTableWriter** (Flush-Mechanismus)
- Schreibt die sortierte Memtable als immutable Datei auf Disk
- Generiert eindeutigen Dateinamen mit Timestamp
- Erstellt Index am Ende der Datei für schnelle Lookups

### 3. **SSTableReader** (Lesezugriffe)
- Lädt Index in den Speicher beim Öffnen
- Nutzt **binäre Suche** im Index (Keys sind sortiert!)
- Direkter Seek zur Datenposition

### 4. **WorldsSimplestDbV4** (Koordinator)
- Verwaltet Memtable und alle SSTable-Dateien
- Read-Path: Memtable → SSTable 1 → SSTable 2 → ...
- Write-Path: Memtable → (Flush bei Bedarf)

## Dateiformat

```
SSTable-Datei (.sst):

┌─────────────────────────────────────────────┐
│ HEADER (20 Bytes)                           │
├─────────────────────────────────────────────┤
│ - MagicNumber: 0x53535442 ("SSTB")         │
│ - Version: 1                                │
│ - EntryCount: Anzahl der Einträge           │
│ - IndexOffset: Position des Index           │
└─────────────────────────────────────────────┘

┌─────────────────────────────────────────────┐
│ DATA SECTION (variable)                     │
├─────────────────────────────────────────────┤
│ Entry 1: KeyLen|KeyData|ValueLen|ValueData  │
│ Entry 2: KeyLen|KeyData|ValueLen|ValueData  │
│ ...                                         │
│ Entry N: KeyLen|KeyData|ValueLen|ValueData  │
└─────────────────────────────────────────────┘

┌─────────────────────────────────────────────┐
│ INDEX SECTION                               │
├─────────────────────────────────────────────┤
│ Entry 1: KeyLen|KeyData|Offset              │
│ Entry 2: KeyLen|KeyData|Offset              │
│ ...                                         │
│ Entry N: KeyLen|KeyData|Offset              │
└─────────────────────────────────────────────┘
```

## Performance-Charakteristiken

### Komplexität
- **Write**: O(log n) - Insert in SortedDictionary
- **Read**: O(log n × m) - Binäre Suche in m SSTables
- **Space**: O(n) - Sortierte Daten mit Index

### Vergleich mit vorherigen Versionen

| Aspekt | V1 | V2 | V3 | V4 |
|--------|----|----|----|----|
| Write Performance | O(1) | O(1) | O(1) | O(log n)* |
| Read Performance | O(n) | O(n) | O(1) | O(log n × m) |
| Sortierung | ❌ | ❌ | ❌ | ✅ |
| Binäre Suche | ❌ | ❌ | ❌ | ✅ |
| Write Buffer | ❌ | ❌ | ❌ | ✅ |

\* In-Memory Writes sind schnell, periodischer Flush auf Disk  
** Konzeptuell möglich, aber nicht implementiert

## Vorteile

### ✅ Sehr schnelle Writes
- Writes gehen nur in Memory (SortedDictionary)
- Asynchroner Flush auf Disk nur bei Überlauf

### ✅ Effiziente Reads
- Binäre Suche in sortierten SSTables
- Jede SSTable hat eigenen Index im RAM

### ✅ Sortierte Daten
- Ermöglicht Range-Queries (von Key A bis Key B)
- Prefix-Scans möglich

### ✅ Immutable SSTables
- Keine Korruptionsgefahr durch konkurrierende Writes
- Alte SSTables können parallel gelesen werden
- Einfaches Backup/Restore

### ✅ Erweiterbar
- Compaction: Mehrere SSTables zusammenführen
- Bloom-Filters: Schnelle negative Lookups
- WAL (Write-Ahead Log): Crash-Recovery
- Multi-Level SSTables (wie LevelDB)

## Nachteile

### ⚠️ Read Amplification
- Bei vielen SSTables müssen mehrere Dateien durchsucht werden
- Lösung: Compaction (Zusammenführen alter SSTables)

### ⚠️ Komplexere Architektur
- Mehr Code und Komponenten als V1-V3
- Mehr Debugging-Aufwand

### ⚠️ Memtable-Verlust bei Crash
- Daten in Memtable gehen verloren wenn Prozess abstürzt
- Lösung: WAL implementieren (nicht enthalten)

### ⚠️ Fragmentierung
- Viele kleine SSTable-Dateien können entstehen
- Lösung: Background-Compaction (nicht implementiert)

## Mögliche Erweiterungen

### 1. **Compaction**
Mehrere alte SSTables zu einer großen zusammenführen:
```csharp
public async Task CompactAsync()
{
    // Merge alte SSTables, lösche Duplikate, sortiere neu
    // Reduziert Anzahl der Dateien und verbessert Read-Performance
}
```

### 2. **Bloom-Filter**
Pro SSTable ein Bloom-Filter für schnelle negative Lookups:
```csharp
// Vor binärer Suche: Prüfe ob Key wahrscheinlich existiert
if (!bloomFilter.MightContain(key))
    return null; // Definitiv nicht vorhanden
```

### 3. **Write-Ahead Log (WAL)**
Für Crash-Recovery:
```csharp
// Jeder Write geht zuerst ins WAL
await wal.AppendAsync(key, value);
memtable.Set(key, value);
```

### 4. **Range Queries**
Nutze die Sortierung für Range-Scans:
```csharp
public async Task<List<KeyValuePair<string, string>>> RangeAsync(
    string startKey, string endKey)
{
    // Scan über alle SSTables, merge sortiert
}
```

### 5. **Snapshots**
Immutable SSTables ermöglichen Point-in-Time Snapshots:
```csharp
public Snapshot CreateSnapshot()
{
    // Liefert konsistente Sicht auf Daten zu einem Zeitpunkt
}
```

## Verwendung

```csharp
// Erstelle Datenbank mit Custom Memtable-Größe
await using var db = new WorldsSimplestDbV4(
    dataDirectory: "./mydata", 
    memtableFlushSize: 5000
);

// Schreibe Daten (gehen in Memtable)
await db.SetAsync("user:1", "Alice");
await db.SetAsync("user:2", "Bob");

// Lese Daten (prüft Memtable, dann SSTables)
var value = await db.GetAsync("user:1");

// Force-Flush (für Tests oder Shutdown)
await db.FlushAsync();

// Statistiken
Console.WriteLine(db.GetStats());
```

## Inspirationen

Diese Implementation ist inspiriert von:
- **LevelDB** (Google) - LSM-Tree Design
- **RocksDB** (Meta) - LevelDB Fork mit mehr Features
- **Cassandra** (Apache) - Distributed SSTable Design
- **HBase** (Apache) - Hadoop Database mit LSM-Trees

## Weitere Ressourcen

- [LSM-Trees Paper (O'Neil et al.)](https://www.cs.umb.edu/~poneil/lsmtree.pdf)
- [LevelDB Documentation](https://github.com/google/leveldb/blob/main/doc/index.md)
- [Designing Data-Intensive Applications (Kapitel 3)](https://www.oreilly.com/library/view/designing-data-intensive-applications/9781491903063/)

