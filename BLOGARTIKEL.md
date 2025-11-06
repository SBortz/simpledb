# Wie funktionieren Datenbanken? Eine Reise durch die Evolution der einfachsten Key-Value-Datenbank

*Warum sind Datenbanken eigentlich so komplex?*

Datenbanken sind eines der fundamentalen Bausteine moderner Software. Doch wie funktionieren sie eigentlich unter der Haube? In diesem Artikel nehmen wir eine einfache Key-Value-Datenbank auseinander und zeigen, wie sie sich von der simpelsten Implementation bis hin zu modernen Konzepten entwickelt.

## Die Grundidee: Log-basiertes Schreiben

Die einfachste Art, eine Datenbank zu implementieren, ist erstaunlich simpel: **Einfach alles an eine Datei anhängen**. Jeder neue Wert wird einfach ans Ende der Datei geschrieben. Wiederholt vorkommende Keys überschreiben ältere. Das ist alles.

### Version 1: Der simpelste Ansatz

```csharp
// V1: Einfach anhängen
await File.AppendAllTextAsync("database.txt", $"{key};{value}\n");
```

**Schreibkomplexität: O(1)** – Das ist so schnell wie es nur geht. Keine Suche, keine komplexe Logik, einfach anhängen.

**Warum ist das so gut fürs Schreiben?**
- ✅ **Sequentielles Schreiben**: Festplatten sind am schnellsten beim sequentiellen Schreiben
- ✅ **Keine Suche nötig**: Wir müssen nicht wissen, wo der alte Wert war
- ✅ **Atomare Operationen**: Ein einzelner Write ist atomar
- ✅ **Crash-Safe**: Was geschrieben wurde, ist persistent

Für das **Schreiben** hat man damit bereits alle Eigenschaften, die man sich von einer schnellen Datenbank wünscht. Das Problem kommt erst beim **Lesen**.

### Das Leseproblem

```csharp
// V1: Vollständiger Scan bei jedem Read
public async Task<string?> GetAsync(string searchKey)
{
    var lines = await File.ReadAllLinesAsync("database.txt");
    return lines
        .Select(line => line.Split(';', 2))
        .Where(parts => parts[0] == searchKey)
        .Select(parts => parts[1])
        .LastOrDefault(); // Neueste Version
}
```

**Lesekomplexität: O(n)** – Bei jedem Lesevorgang muss die gesamte Datei durchsucht werden. Bei 1 Million Einträgen bedeutet das: 1 Million Vergleiche pro Read.

**Das ist das fundamentale Problem**: Während das Schreiben perfekt skaliert, wird das Lesen exponentiell langsamer.

## Die Evolution: Von V1 zu V4

Die Herausforderung liegt also nicht im Schreiben, sondern im **schnellen Lesen**. Daher gibt es die Evolutionsstufen V2, V3 und V4, die jeweils versuchen, das Leseproblem zu lösen.

---

## Version 2: Binäres Format – Effizienz ohne Kompromisse

**Was ändert sich?**
- Text-Format → Binäres Format mit Length-Prefixing
- `"key;value\n"` → `[keyLen][keyData][valueLen][valueData]`

**Was wird besser?**
- ✅ **Kompaktere Speicherung**: Kein Text-Encoding-Overhead
- ✅ **Beliebige Zeichen**: Keys und Values können Semikolons, Newlines etc. enthalten
- ✅ **Schnelleres Parsing**: Keine String-Splits nötig
- ✅ **Strukturiertes Format**: Direktes Lesen von Längen und Daten

**Was bleibt gleich?**
- ⚠️ **Lesekomplexität: O(n)** – Immer noch vollständiger Scan
- ⚠️ **Keine Index-Struktur**: Keine Möglichkeit für schnelle Lookups

**Nachteile:**
- ❌ Datei ist nicht mehr human-readable
- ❌ Immer noch langsam bei vielen Einträgen

**Fazit**: V2 ist eine Optimierung des Speicherformats, löst aber das fundamentale Leseproblem nicht.

---

## Version 3: In-Memory Index – Der Durchbruch

**Was ändert sich?**
- Ein **Dictionary** im RAM speichert: `Key → Datei-Offset`
- Beim Start: Einmaliger Scan zum Index-Aufbau
- Beim Lesen: Direkter Seek zur Position (kein Scan!)

```csharp
// V3: Index im RAM
private Dictionary<string, long> _index; // Key → Offset

public async Task<string?> GetAsync(string searchKey)
{
    if (!_index.TryGetValue(searchKey, out long offset))
        return null;
    
    // Direkter Seek - keine Suche!
    fs.Seek(offset, SeekOrigin.Begin);
    // ... lese nur diesen einen Eintrag
}
```

**Was wird besser?**
- ✅ **Lesekomplexität: O(1)** – Direkter Zugriff via Index
- ✅ **Dramatisch schneller**: Auch bei Millionen von Einträgen
- ✅ **Schreiben bleibt O(1)**: Append + Index-Update im RAM
- ✅ **Skaliert sehr gut**: Index-Update ist trivial

**Nachteile:**
- ⚠️ **RAM-Verbrauch**: ~50-100 Bytes pro Key im Index
- ⚠️ **Startup-Zeit**: Einmaliger Scan beim Start (kann bei großen DBs dauern)
- ⚠️ **Komplexere Architektur**: Index muss verwaltet werden
- ⚠️ **Crash-Recovery**: Index geht verloren, muss neu aufgebaut werden
- ⚠️ **Skalierungsgrenze**: Der Index funktioniert nur in-memory performant. Er muss erst aufwändig in den RAM gelesen werden und stößt irgendwann an eine fundamentale Grenze: **Die Größe des RAMs selbst**. Bei sehr großen Datenbanken passt der gesamte Index nicht mehr in den Arbeitsspeicher.

**Fazit**: V3 löst das Leseproblem elegant, hat aber Trade-offs bei Memory und Startup-Zeit. Die Skalierung ist durch die RAM-Größe begrenzt.

---

## Version 4: SSTables – Moderne Datenbank-Architektur

**Was ändert sich?**
- **Memtable**: In-Memory SortedDictionary als Write-Buffer
- **SSTables**: Immutable, sortierte Dateien auf der Festplatte
- **Write-Ahead Log (WAL)**: Crash-Recovery
- **Binäre Suche**: Nutzt Sortierung für effiziente Reads

```
Architektur:
┌──────────────┐
│  Memtable    │ ← Neue Writes (in-memory, sortiert)
└──────┬───────┘
       │ Bei Überlauf: Flush
       ▼
┌──────────────┐
│ SSTable 1    │ ← Neueste (immutable, sortiert)
├──────────────┤
│ SSTable 2    │
├──────────────┤
│ SSTable 3    │ ← Älteste
└──────────────┘
```

**Das Wunder von SSTables**

Das Geniale an SSTables ist die Kombination aus Sortierung und sequentiellem Schreiben: Unsortiert auftretende Keys werden in der Memtable gesammelt und automatisch geordnet – das performante Ordnen erledigt ein **Red-Black Tree** (intern in `SortedDictionary`). Wenn die Memtable voll ist, werden die geordneten Keys **sequentiell in eine Datei geschrieben**, was ebenfalls sehr performant ist. So entstehen mit der Zeit mehrere Dateien, in denen sortierte Keys liegen.

Diese Dateien können später **gemerged und kompaktiert** werden. Auch der Merge-Vorgang ist sehr performant und einfach zu implementieren, da beide Dateien bereits sortiert sind – man kann sie einfach sequenziell durchgehen und zusammenführen. Diese Ausbaustufe ist schon sehr nah dran an modernen Key-Value-Stores wie **LevelDB** (Google) oder **RocksDB** (Meta/Facebook).

**Was wird besser?**
- ✅ **Sehr schnelle Writes**: O(log n) in Memory, kein Disk-I/O bis Flush
- ✅ **Non-Blocking Flush**: Neue Writes können während Flush weitergehen
- ✅ **Effiziente Reads**: O(log n × m) – Binäre Suche in sortierten SSTables
- ✅ **Immutable SSTables**: Keine Korruptionsgefahr
- ✅ **Sortierung**: Ermöglicht Range-Queries (von Key A bis Key B)
- ✅ **Crash-Recovery**: WAL replays verlorene Daten
- ✅ **Skaliert gut**: Mehrere SSTables statt einer großen Datei
- ✅ **Performantes Merging**: Sortierte Dateien können effizient zusammengeführt werden

**Nachteile:**
- ⚠️ **Read Amplification**: Bei vielen SSTables müssen mehrere Dateien durchsucht werden
- ⚠️ **Komplexere Architektur**: Mehr Code, mehr Komponenten
- ⚠️ **WAL-Overhead**: Jeder Write wird doppelt geschrieben (WAL + Memtable)
- ⚠️ **Fragmentierung**: Viele kleine SSTable-Dateien können entstehen
- ⚠️ **Compaction fehlt**: Ohne Compaction akkumulieren sich SSTables über Zeit

**Fazit**: V4 nutzt moderne LSM-Tree-Prinzipien (wie LevelDB, RocksDB, Cassandra). Es ist die ausgereifteste Version, hat aber die komplexeste Architektur. Mit Compaction wäre es bereits sehr nah an produktiven Key-Value-Stores.
---

## Vergleich der Versionen

| Aspekt | V1 | V2 | V3 | V4 |
|--------|----|----|----|----|
| **Write Performance** | O(1) | O(1) | O(1) | O(log n)* |
| **Read Performance** | O(n) | O(n) | O(1) | O(log n × m) |
| **Speicherformat** | Text | Binär | Binär | SSTables |
| **Index** | ❌ | ❌ | ✅ (RAM) | ✅ (Sortiert) |
| **Binäre Suche** | ❌ | ❌ | ❌ | ✅ |
| **Write Buffer** | ❌ | ❌ | ❌ | ✅ (Memtable) |
| **Crash Recovery** | ❌ | ❌ | ❌ | ✅ (WAL) |
| **Einfachheit** | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐ | ⭐⭐⭐ | ⭐⭐ |

\* In-Memory Writes sind schnell, periodischer Flush auf Disk

---

## Die Erkenntnis

**Schreiben ist einfach**: Log-basiertes Anhängen ist bereits optimal. O(1), sequentiell, crash-safe.

**Lesen ist die Herausforderung**: Ohne Index oder Sortierung muss man die gesamte Datei scannen.

**Die Evolution zeigt verschiedene Lösungsansätze**:
- **V2**: Optimiert das Format, löst das Problem nicht
- **V3**: In-Memory Index – einfach, aber RAM-intensiv
- **V4**: SSTables mit Sortierung – komplex, aber modern und skalierbar

Jede Version hat ihre Trade-offs. Die "beste" Version hängt von den Anforderungen ab:
- **V1**: Prototyping, sehr kleine Datenmengen
- **V2**: Kleine bis mittlere Datenmengen, binäres Format bevorzugt
- **V3**: Viele Reads, genug RAM verfügbar
- **V4**: Hoher Write-Throughput, skalierbare Anwendungen, Crash-Recovery nötig

---

## Praktische Anwendung: Benchmark mit 200MB Daten

Lass uns die verschiedenen Versionen praktisch testen und ihre Performance vergleichen. Wir werden die Datenbanken mit 200MB Daten befüllen und dann die Lesezeiten messen.

### Schritt 1: Projekt vorbereiten

Zuerst klonen wir das Projekt und bauen es:

```bash
git clone <repository-url>
cd simpledb
dotnet build
```

### Schritt 2: Datenbanken mit 200MB Daten befüllen

Das Projekt enthält ein `filldata`-Tool, das automatisch Daten generiert und in die Datenbank schreibt. Wir befüllen jede Version mit 200MB Daten:

```bash
cd worldssimplestdb.filldata

# V1 befüllen (Text-Format)
dotnet run v1 200mb 50 200

# V2 befüllen (Binär-Format)
dotnet run v2 200mb 50 200

# V3 befüllen (mit Index)
dotnet run v3 200mb 50 200

# V4 befüllen (SSTable)
dotnet run v4 200mb 50 200
```

Die Parameter bedeuten:
- `v1/v2/v3/v4`: Die Version der Datenbank
- `200mb`: Zielgröße der Datenbank
- `50`: Minimale Länge der Values (Zeichen)
- `200`: Maximale Länge der Values (Zeichen)

**Hinweis**: V3 benötigt beim Start Zeit, um den Index aufzubauen. V4 erstellt mehrere SSTable-Dateien während des Befüllens.

### Schritt 3: Performance-Benchmark durchführen

Jetzt messen wir die Lesezeiten. Wir erstellen ein kleines Benchmark-Script:

```csharp
// benchmark.cs
using System.Diagnostics;
using worldssimplestdb.v1;
using worldssimplestdb.v2;
using worldssimplestdb.v3;
using worldssimplestdb.v4;

// Teste alle Versionen mit demselben Key
string testKey = "key_00050000"; // Ein Key aus der Mitte der Daten

await BenchmarkVersion("V1", async () => {
    var db = new WorldsSimplestDbV1();
    return await db.GetAsync(testKey);
});

await BenchmarkVersion("V2", async () => {
    var db = new WorldsSimplestDbV2();
    return await db.GetAsync(testKey);
});

await BenchmarkVersion("V3", async () => {
    var indexStore = new IndexStore();
    indexStore.Load(null); // Index aufbauen
    var db = new WorldsSimplestDbV3(indexStore);
    return await db.GetAsync(testKey);
});

await BenchmarkVersion("V4", async () => {
    var db = new WorldsSimplestDbV4();
    await db.InitializeAsync();
    return await db.GetAsync(testKey);
});

async Task BenchmarkVersion(string version, Func<Task<string?>> getOperation)
{
    Console.WriteLine($"\n=== Benchmarking {version} ===");
    
    // Warmup
    await getOperation();
    
    // Mehrere Durchläufe für Durchschnitt
    var times = new List<long>();
    for (int i = 0; i < 10; i++)
    {
        var sw = Stopwatch.StartNew();
        var result = await getOperation();
        sw.Stop();
        times.Add(sw.ElapsedMilliseconds);
    }
    
    var avg = times.Average();
    var min = times.Min();
    var max = times.Max();
    
    Console.WriteLine($"Durchschnitt: {avg:F2}ms");
    Console.WriteLine($"Minimum: {min}ms");
    Console.WriteLine($"Maximum: {max}ms");
}
```

Oder einfacher: Nutze die interaktive Konsole und messe manuell:

```bash
cd worldssimplestdb.console

# V1 testen
dotnet run
# Wähle Version 1
# get key_00050000

# V2 testen
dotnet run
# Wähle Version 2
# get key_00050000

# V3 testen (Index-Aufbau dauert etwas)
dotnet run
# Wähle Version 3
# get key_00050000

# V4 testen
dotnet run
# Wähle Version 4
# get key_00050000
```

### Schritt 4: Ergebnisse interpretieren

Typische Ergebnisse für 200MB Daten (ca. 200.000 Einträge):

| Version | Durchschnittliche Lesezeit | Startup-Zeit | Speicher |
|---------|---------------------------|--------------|----------|
| **V1** | ~500-2000ms | <1ms | Minimal |
| **V2** | ~300-1500ms | <1ms | Minimal |
| **V3** | ~0.1-1ms | ~500-2000ms | ~20-40MB (Index) |
| **V4** | ~1-5ms | ~100-500ms | ~5-10MB (Memtable) |

**Beobachtungen:**

1. **V1 & V2**: Sehr langsam beim Lesen, da die gesamte Datei durchsucht werden muss. Bei 200MB bedeutet das mehrere Sekunden pro Read.

2. **V3**: Extrem schnell beim Lesen (O(1) via Index), aber:
   - Startup-Zeit: Der Index muss beim Start aufgebaut werden
   - RAM-Verbrauch: Der gesamte Index liegt im Speicher

3. **V4**: Gute Balance:
   - Schnelle Reads durch binäre Suche in sortierten SSTables
   - Moderate Startup-Zeit (WAL-Recovery)
   - Geringerer RAM-Verbrauch als V3
   - Skaliert besser bei sehr großen Datenmengen

### Schritt 5: Interaktive Nutzung

Die Datenbanken können auch interaktiv genutzt werden:

```bash
cd worldssimplestdb.console
dotnet run
```

Dann kannst du Befehle eingeben:

```
db> set name "Max Mustermann"
OK
db> get name
Max Mustermann
db> help
Available commands:
  set <key> <value>  - Store a key-value pair
  get <key>          - Get value by key
  help               - Show this help
  exit/quit          - Exit the program
db> exit
Goodbye!
```

**Wichtig für V4**: Beim Beenden wird die Memtable automatisch geflusht und das WAL gelöscht. Alle Daten sind dann persistent in SSTables gespeichert.

### Was lernen wir aus dem Benchmark?

1. **Schreiben ist überall schnell**: Alle Versionen schreiben mit O(1) oder O(log n) – der Unterschied ist minimal.

2. **Lesen ist das Problem**: V1 und V2 zeigen, warum ein Index nötig ist. Bei 200MB dauert ein Read mehrere Sekunden.

3. **Trade-offs sind real**: 
   - V3 ist am schnellsten beim Lesen, braucht aber viel RAM
   - V4 ist ein guter Kompromiss: Schnell genug, weniger RAM, besser skalierbar

4. **Skalierung**: Bei noch größeren Datenmengen (z.B. 10GB) würde V3 an RAM-Grenzen stoßen, während V4 weiterhin funktioniert.

---

## Was fehlt noch?

V4 ist bereits sehr ausgereift, aber es gibt noch Verbesserungspotenzial:

### Compaction
Mehrere SSTables zusammenführen, um:
- Read Amplification zu reduzieren
- Duplikate zu entfernen
- Fragmentierung zu reduzieren

### Bloom-Filter
Pro SSTable ein Bloom-Filter für schnelle negative Lookups ("Key existiert definitiv nicht").

### Range Queries
Nutze die Sortierung für Range-Scans (alle Keys von A bis B).

---

## Fazit

Datenbanken sind im Kern nicht kompliziert. Das Schreiben ist trivial: Einfach anhängen. Die Kunst liegt darin, **schnell lesen zu können**. Die Evolution von V1 zu V4 zeigt verschiedene Ansätze:

1. **V1**: Beweis, dass Schreiben einfach ist
2. **V2**: Optimierung des Formats
3. **V3**: In-Memory Index für O(1) Reads
4. **V4**: Moderne SSTable-Architektur mit allen Features

Jede Version lehrt uns etwas über die Trade-offs zwischen Einfachheit, Performance und Features. Und das Beste: Man kann alle Versionen in wenigen hundert Zeilen Code implementieren und selbst experimentieren!

---

*Dieser Artikel basiert auf dem [World's Simplest Database](https://github.com/...) Projekt, das alle vier Versionen als C#-Implementierungen bereitstellt.*

