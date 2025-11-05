namespace worldssimplestdb.v4;

/// <summary>
/// Beispiel-Code für die Verwendung von WorldsSimplestDbV4
/// </summary>
public static class Example
{
    public static async Task RunDemo()
    {
        Console.WriteLine("=== WorldsSimplestDB V4 - SSTable Demo ===\n");
        
        // Erstelle Datenbank mit kleiner Memtable für Demo-Zwecke
        await using var db = new WorldsSimplestDbV4(
            dataDirectory: "./demo_sstables",
            memtableFlushSize: 5  // Klein für Demo (normalerweise 10.000)
        );
        
        Console.WriteLine("1. Schreibe 12 Einträge (Memtable-Größe = 5)");
        Console.WriteLine("   → Erwarte 2 Flushes bei Entry 5 und 10\n");
        
        for (int i = 1; i <= 12; i++)
        {
            string key = $"user:{i:D3}";
            string value = $"User {i}";
            await db.SetAsync(key, value);
            Console.WriteLine($"   Wrote: {key} = {value}");
            
            // Zeige Stats nach jedem Flush
            if (i == 5 || i == 10)
            {
                Console.WriteLine($"\n   📊 Nach Flush:\n   {db.GetStats().Replace("\n", "\n   ")}\n");
            }
        }
        
        Console.WriteLine($"\n2. Aktuelle Statistik:");
        Console.WriteLine($"   {db.GetStats().Replace("\n", "\n   ")}\n");
        
        Console.WriteLine("3. Lese einige Werte:");
        var keysToRead = new[] { "user:001", "user:005", "user:010", "user:012", "user:999" };
        foreach (var key in keysToRead)
        {
            var value = await db.GetAsync(key);
            Console.WriteLine($"   Get({key}) = {value ?? "null"}");
        }
        
        Console.WriteLine("\n4. Flush verbleibende Memtable:");
        await db.FlushAsync();
        Console.WriteLine($"   {db.GetStats().Replace("\n", "\n   ")}\n");
        
        Console.WriteLine("5. Sortierung testen (Keys sind sortiert in SSTables):");
        Console.WriteLine("   Dies ermöglicht effiziente binäre Suche!");
        
        Console.WriteLine("\n✅ Demo abgeschlossen!");
        Console.WriteLine("\nℹ️  SSTable-Dateien wurden erstellt in: ./demo_sstables/");
        Console.WriteLine("   Du kannst die Dateien inspizieren (binäres Format).");
    }
    
    public static async Task RunPerformanceTest()
    {
        Console.WriteLine("=== WorldsSimplestDB V4 - Performance Test ===\n");
        
        const int entryCount = 50_000;
        
        await using var db = new WorldsSimplestDbV4(
            dataDirectory: "./perf_sstables",
            memtableFlushSize: 10_000
        );
        
        // Write Performance
        Console.WriteLine($"1. Schreibe {entryCount:N0} Einträge...");
        var sw = System.Diagnostics.Stopwatch.StartNew();
        
        for (int i = 0; i < entryCount; i++)
        {
            await db.SetAsync($"key:{i:D8}", $"value-{i}");
            
            if ((i + 1) % 10_000 == 0)
            {
                Console.WriteLine($"   {i + 1:N0} Einträge geschrieben...");
            }
        }
        
        sw.Stop();
        Console.WriteLine($"   ✅ {entryCount:N0} Einträge in {sw.ElapsedMilliseconds:N0}ms");
        Console.WriteLine($"   → {entryCount / sw.Elapsed.TotalSeconds:N0} writes/sec\n");
        
        Console.WriteLine($"2. Statistik:");
        Console.WriteLine($"   {db.GetStats().Replace("\n", "\n   ")}\n");
        
        // Read Performance
        Console.WriteLine($"3. Lese {1000:N0} zufällige Einträge...");
        var random = new Random(42);
        sw.Restart();
        
        for (int i = 0; i < 1000; i++)
        {
            int randomKey = random.Next(entryCount);
            var value = await db.GetAsync($"key:{randomKey:D8}");
        }
        
        sw.Stop();
        Console.WriteLine($"   ✅ {1000:N0} Reads in {sw.ElapsedMilliseconds:N0}ms");
        Console.WriteLine($"   → {1000 / sw.Elapsed.TotalSeconds:N0} reads/sec\n");
        
        Console.WriteLine("✅ Performance Test abgeschlossen!");
    }
}

