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
- ⚠️ **Langsameres Schreiben**: Warum ist V4 beim Schreiben langsamer als V1-V3?

**Warum ist V4 beim Schreiben langsamer?**

Obwohl V4 theoretisch schnelle Writes haben sollte (nur O(log n) in Memory), ist es in der Praxis beim Schreiben langsamer als V1-V3. Die Hauptgründe:

1. **WAL-Overhead (Write-Ahead Log)**: Jeder Write wird **zuerst ins WAL geschrieben** und **sofort auf Disk geflusht** (synchrone Disk-I/O). Das bedeutet, dass jeder einzelne Write eine Disk-I/O-Operation hat, bevor er in die Memtable kommt. Bei V1-V3 werden Writes nur gepuffert und später gebündelt geschrieben.

2. **Memtable-Flush**: Wenn die Memtable voll ist (z.B. nach 10.000 Einträgen), wird sie als SSTable geflusht. Das ist eine **große I/O-Operation**, die:
   - Alle Einträge sortiert auf Disk schreibt
   - Einen Sparse-Index erstellt
   - Eine neue Datei erstellt (mit Temp-File + Rename für Atomizität)
   - Alles auf Disk flusht

3. **Doppeltes Schreiben**: Die Daten werden **dreimal geschrieben**:
   - Ins WAL (sofort, synchron)
   - In die Memtable (in-memory)
   - In die SSTable (beim Flush)

4. **Sparse-Index-Erstellung**: Beim Flush der SSTable muss ein Sparse-Index erstellt werden, was zusätzliche Berechnungen und I/O bedeutet.

**Im Vergleich zu V1-V3:**
- **V1-V3**: Einfaches Append-Only-Schreiben, keine WAL, keine Sortierung, keine Index-Erstellung während des Schreibens. Writes werden gepuffert und gebündelt geschrieben.
- **V4**: Jeder Write hat WAL-Overhead, und periodische Memtable-Flushes erzeugen zusätzliche I/O-Last.

**Trade-off**: V4 opfert Write-Performance für:
- **Crash-Safety** (WAL garantiert Datenintegrität)
- **Bessere Read-Performance** (sortierte SSTables mit Index)
- **Skalierbarkeit** (funktioniert auch bei sehr großen Datenmengen)

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

Das Projekt enthält ein integriertes Benchmark-Tool, das automatisch alle Versionen testet. Es misst sowohl **Write-Performance** (Befüllen) als auch **Read-Performance** (Lookup).

#### Benchmark-Kommando

```bash
cd worldssimplestdb.filldata
dotnet run benchmark [fillSize] [testKey] [iterations]
```

**Parameter (alle optional, können in beliebiger Reihenfolge angegeben werden):**

- **`fillSize`** (Standard: `200mb`): Größe der Datenbank zum Befüllen
  - Unterstützte Formate: `10mb`, `100mb`, `1gb`, `500kb`, etc.
  - Beispiel: `200mb`, `10mb`, `1gb`

- **`testKey`** (Standard: `key_00050000`): Der Key, der beim Read-Benchmark gesucht wird
  - Muss mit `key_` beginnen
  - Beispiel: `key_00050000`, `key_00010000`, `key_00100000`

- **`iterations`** (Standard: `10`): Anzahl der Read-Iterationen pro Version
  - Mehr Iterationen = genauere Durchschnittswerte
  - Beispiel: `10`, `20`, `50`

#### Beispiele

```bash
# Standard-Benchmark (200mb, key_00050000, 10 Iterationen)
dotnet run benchmark

# Benchmark mit 10mb Daten
dotnet run benchmark 10mb

# Benchmark mit spezifischem Key
dotnet run benchmark key_00010000

# Benchmark mit 20 Iterationen
dotnet run benchmark 20

# Alle Parameter kombiniert
dotnet run benchmark 100mb key_00050000 20
```

#### Was macht das Benchmark-Tool?

1. **Write-Benchmark**: Befüllt alle vier Versionen mit der angegebenen Datenmenge und misst:
   - Zeit zum Befüllen
   - Throughput (MB/s)
   - Anzahl der geschriebenen Einträge
   - Durchschnittszeit pro Eintrag

2. **Read-Benchmark**: Misst die Lesezeiten für alle Versionen:
   - Durchschnitt, Median, Minimum, Maximum
   - Startup-Zeit (für V3: Index-Aufbau)
   - Mehrere Iterationen für statistische Genauigkeit

**Hinweis**: Das Benchmark-Tool erstellt die Datenbanken neu. Falls bereits Daten vorhanden sind, werden diese überschrieben.

### Schritt 4: Benchmark-Ergebnisse

Das Benchmark-Tool führt automatisch beide Tests durch: Zuerst wird jede Version mit Daten befüllt (Write-Benchmark), dann werden die Lesezeiten gemessen (Read-Benchmark).

**Beispiel-Output:**

```
=== Database Performance Benchmark ===
Fill size: 200mb
Test Key: key_00050000
Read iterations per version: 10

=== WRITE BENCHMARK (Filling Database) ===
[... Write-Statistiken für alle Versionen ...]

=== READ BENCHMARK ===
[... Read-Statistiken für alle Versionen ...]
```

**Tatsächliche Benchmark-Ergebnisse (200MB Daten, ~200.000 Einträge):**

#### Write-Performance (Befüllen)

| Version | Zeit | Throughput | Einträge |
|---------|------|------------|----------|
| **V1** | ~2-3s | ~70-100 MB/s | ~200.000 |
| **V2** | ~2-3s | ~70-100 MB/s | ~200.000 |
| **V3** | ~2-3s | ~70-100 MB/s | ~200.000 |
| **V4** | ~3-4s | ~50-70 MB/s | ~200.000 |

**Erkenntnis**: V1-V3 schreiben ähnlich schnell (einfaches Append-Only). V4 ist etwas langsamer wegen:
- **WAL-Overhead**: Jeder Write wird zuerst ins WAL geschrieben und sofort geflusht (synchrone Disk-I/O)
- **Memtable-Flush**: Periodische Flushes der Memtable als SSTable (große I/O-Operationen)
- **Doppeltes Schreiben**: Daten werden ins WAL, in die Memtable und später in die SSTable geschrieben

Der Trade-off: V4 opfert Write-Performance für Crash-Safety (WAL) und bessere Read-Performance (sortierte SSTables mit Index).

#### Read-Performance (Lookup)

| Version | Durchschnitt | Median | Minimum | Maximum | Startup-Zeit |
|---------|--------------|--------|---------|---------|--------------|
| **V1** | **1.625,9ms** | 1.610ms | 1.530ms | 1.835ms | <1ms |
| **V2** | **162,5ms** | 162ms | 157ms | 169ms | <1ms |
| **V3** | **<0,1ms** | <0,1ms | <0,1ms | <0,1ms | ~500-2000ms* |
| **V4** | **15,8ms** | 15ms | 14ms | 24ms | ~100-500ms |

\* V3 benötigt Zeit zum Index-Aufbau beim ersten Start

**Detaillierte Ergebnisse:**

```
=== READ BENCHMARK (200MB Daten) ===

V1 Read:
  Average: 1.625,90ms  Median: 1.610ms  Min: 1.530ms  Max: 1.835ms

V2 Read:
  Average: 162,50ms   Median: 162ms    Min: 157ms    Max: 169ms

V3 Read (nach Index-Aufbau):
  Average: <0,1ms      Median: <0,1ms   Min: <0,1ms   Max: <0,1ms
  Index-Aufbau: ~500-2000ms (einmalig)

V4 Read:
  Average: 15,80ms     Median: 15ms     Min: 14ms     Max: 24ms
```

**Beobachtungen:**

1. **V1 ist am langsamsten** (1.625,9ms ≈ 1,6 Sekunden): Text-Parsing und vollständiger Scan sind teuer. Bei 200MB muss die gesamte Datei geladen und geparst werden.

2. **V2 ist 10× schneller als V1** (162,5ms): Binäres Format ist effizienter, aber immer noch O(n) Scan. Die Optimierung (Datei auf einmal laden) macht V2 deutlich schneller als die ursprüngliche Implementierung.

3. **V3 ist am schnellsten** (<0,1ms): O(1) Lookup via Index ist unschlagbar. Aber:
   - **Startup-Zeit**: ~500-2000ms für Index-Aufbau (einmalig)
   - **RAM-Verbrauch**: ~20-40MB für den Index bei 200.000 Einträgen
   - **Skalierungsgrenze**: Bei sehr großen Datenmengen passt der Index nicht mehr in den RAM

4. **V4 ist ein guter Kompromiss** (15,8ms): 
   - **10× schneller als V2**, aber langsamer als V3
   - **Sparse-Index**: Nur jeder 16. Key im Index → viel kleinerer RAM-Verbrauch
   - **Skaliert besser**: Funktioniert auch bei sehr großen Datenmengen
   - **Startup-Zeit**: Moderate (~100-500ms für WAL-Recovery und Index-Laden)

**Performance-Vergleich (Read):**

```
V1: ████████████████████████████████████████████████████████ 1.625,9ms
V2: ██████████ 162,5ms
V4: █ 15,8ms
V3: █ <0,1ms (aber: Startup-Zeit + RAM-Overhead)
```

**Fazit**: V2 zeigt, dass Format-Optimierung hilft, aber ein Index (V3/V4) ist für schnelle Reads essentiell. V4 bietet die beste Balance zwischen Performance, RAM-Verbrauch und Skalierbarkeit.

---

### Benchmark-Ergebnisse bei 1GB Daten

Bei größeren Datenmengen werden die Unterschiede noch deutlicher. Hier die Ergebnisse mit **1GB Daten** (~1.000.000 Einträge):

#### Read-Performance (Lookup) - 1GB

| Version | Durchschnitt | Median | Minimum | Maximum | Startup-Zeit |
|---------|--------------|--------|---------|---------|--------------|
| **V1** | **9.043,3ms** | 8.638ms | 8.119ms | 10.818ms | <1ms |
| **V2** | **1.056,4ms** | 959ms | 905ms | 1.316ms | <1ms |
| **V3** | **<0,1ms** | <0,1ms | <0,1ms | <0,1ms | ~2-5s* |
| **V4** | **105,4ms** | 108ms | 92ms | 127ms | ~200-800ms |

\* V3 benötigt Zeit zum Index-Aufbau beim ersten Start (bei 1GB deutlich länger)

**Detaillierte Ergebnisse:**

```
=== READ BENCHMARK (1GB Daten) ===

V1 Read:
  Average: 9.043,30ms  Median: 8.638ms  Min: 8.119ms  Max: 10.818ms

V2 Read:
  Average: 1.056,40ms  Median: 959ms    Min: 905ms    Max: 1.316ms

V3 Read (nach Index-Aufbau):
  Average: <0,1ms      Median: <0,1ms   Min: <0,1ms   Max: <0,1ms
  Index-Aufbau: ~2-5s (einmalig, bei 1GB deutlich länger)

V4 Read:
  Average: 105,40ms    Median: 108ms    Min: 92ms     Max: 127ms
```

**Beobachtungen bei 1GB:**

1. **V1 skaliert linear schlecht** (9.043ms ≈ 9 Sekunden): Bei 5× mehr Daten ist die Lesezeit auch ~5× langsamer. Vollständiger Scan wird bei großen Datenmengen unpraktikabel.

2. **V2 skaliert besser** (1.056ms ≈ 1 Sekunde): Binäres Format hilft, aber O(n) Scan bleibt bei großen Datenmengen ein Problem. Immer noch ~6,5× langsamer als bei 200MB.

3. **V3 bleibt konstant schnell** (<0,1ms): O(1) Lookup ist unabhängig von der Datenbankgröße. **Aber**: 
   - **Startup-Zeit**: ~2-5 Sekunden für Index-Aufbau bei 1GB (vs. ~500-2000ms bei 200MB)
   - **RAM-Verbrauch**: ~100-200MB für den Index bei 1.000.000 Einträgen
   - **Skalierungsgrenze**: Bei noch größeren Datenmengen wird der RAM-Verbrauch problematisch

4. **V4 skaliert gut** (105,4ms): 
   - **Nur ~6,7× langsamer** als bei 200MB, obwohl die Datenbank **5× größer** ist
   - **Sparse-Index**: RAM-Verbrauch bleibt moderat auch bei großen Datenmengen
   - **Startup-Zeit**: Moderate (~200-800ms für WAL-Recovery und Index-Laden)
   - **Skaliert besser als V2**: Bei 5× mehr Daten nur ~6,7× langsamer (vs. V2: ~6,5× langsamer)

**Performance-Vergleich (Read) - 1GB:**

```
V1: ████████████████████████████████████████████████████████ 9.043,3ms
V2: ████████████████████████████████████████████████████████ 1.056,4ms
V4: ████████████████████████████████████████████████████████ 105,4ms
V3: █ <0,1ms (aber: Startup-Zeit + RAM-Overhead)
```

**Skalierungsvergleich (200MB vs. 1GB):**

| Version | 200MB | 1GB | Faktor |
|---------|-------|-----|--------|
| **V1** | 1.625,9ms | 9.043,3ms | **5,6×** |
| **V2** | 162,5ms | 1.056,4ms | **6,5×** |
| **V3** | <0,1ms | <0,1ms | **1×** (konstant) |
| **V4** | 15,8ms | 105,4ms | **6,7×** |

**Fazit bei 1GB**: 
- **V3** ist immer noch am schnellsten, aber der RAM-Overhead und die Startup-Zeit werden bei großen Datenmengen problematisch
- **V4** zeigt die beste Skalierbarkeit: Nur moderat langsamer bei 5× mehr Daten, mit viel geringerem RAM-Verbrauch als V3
- **V1/V2** werden bei großen Datenmengen unpraktikabel (mehrere Sekunden pro Lookup)

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