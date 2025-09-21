namespace worldssimplestdb;

/// <summary>
/// Common interface for all database versions
/// </summary>
public interface IDatabase : IDisposable
{
    /// <summary>
    /// Store a key-value pair in the database
    /// </summary>
    Task SetAsync(string key, string value);
    
    /// <summary>
    /// Retrieve a value by key from the database
    /// </summary>
    Task<string?> GetAsync(string key);
}