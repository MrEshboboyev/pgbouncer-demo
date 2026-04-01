using Npgsql;
using System.Diagnostics;

class Program
{
    static async Task Main()
    {
        int workerCount = 500;       // Number of concurrent background workers
        int queriesPerWorker = 2000; // Number of monitors checked by each worker

        // 1. DIRECT POSTGRESQL CONNECTION (Port: 15432)
        // NoResetOnClose is not needed here.
        string postgresConnectionString = "Host=localhost;Port=15432;Database=falcon;Username=postgres;Password=toorqwe1234r;Pooling=True;Minimum Pool Size=10;Maximum Pool Size=200;Timeout=30;No Reset On Close=true;"; // when No Reset on Close: sorry, too many clients already

        // 2. PGBOUNCER CONNECTION (Port: 6432)
        // NoResetOnClose=true is REQUIRED for PgBouncer transaction mode.
        string pgbouncerConnectionString = "Host=localhost;Port=6432;Database=falcon;Username=postgres;Password=toorqwe1234r;Pooling=True;Minimum Pool Size=10;Maximum Pool Size=200;Timeout=30;No Reset On Close=true;";

        Console.WriteLine("======================================================");
        Console.WriteLine("   FALCON STRESS TEST: Direct Postgres vs PgBouncer   ");
        Console.WriteLine("======================================================\n");

        // --- ROUND 1: Direct PostgreSQL ---
        //await RunStressTest("1. DIRECT POSTGRESQL", postgresConnectionString, workerCount, queriesPerWorker);

        // Give the database a 5-second cooldown to close unused connections
        Console.WriteLine("\n[System] Cooling down for 5 seconds before the next test...\n");
        await Task.Delay(5000);

        // --- ROUND 2: PgBouncer ---
        await RunStressTest("2. PGBOUNCER", pgbouncerConnectionString, workerCount, queriesPerWorker);

        Console.WriteLine("\n[System] All tests completed successfully.");
    }

    /// <summary>
    /// Runs the stress test with the given configuration.
    /// </summary>
    static async Task RunStressTest(string testName, string connectionString, int workerCount, int queriesPerWorker)
    {
        Console.WriteLine($"--- STARTING TEST: {testName} ---");
        Console.WriteLine($"Active Workers: {workerCount}, Total Monitor Checks: {workerCount * queriesPerWorker}");

        int successCount = 0;
        int failCount = 0;

        var stopwatch = Stopwatch.StartNew();

        // Create a dedicated connection pool for this test
        await using var dataSource = NpgsqlDataSource.Create(connectionString);

        await Parallel.ForEachAsync(
            Enumerable.Range(1, workerCount),
            new ParallelOptions { MaxDegreeOfParallelism = workerCount },
            async (workerId, ct) =>
            {
                // Thread-safe random generator to simulate different data points
                var rnd = new Random(Guid.NewGuid().GetHashCode());

                for (int i = 0; i < queriesPerWorker; i++)
                {
                    try
                    {
                        // Lease connection from the pool
                        await using var conn = await dataSource.OpenConnectionAsync(ct);

                        // Lightweight dummy query (Replace with UPDATE/INSERT for real test)
                        string falconQuery = "SELECT 1;";
                        await using var cmd = new NpgsqlCommand(falconQuery, conn);

                        // Execute the write operation
                        await cmd.ExecuteNonQueryAsync(ct);

                        Interlocked.Increment(ref successCount);
                    }
                    catch (Exception ex)
                    {
                        Interlocked.Increment(ref failCount);
                        // Log only the first 3 errors to prevent console spam
                        if (failCount <= 3)
                        {
                            Console.WriteLine($"[Error - {testName}] Worker {workerId}: {ex.Message}");
                        }
                    }
                }
            });

        stopwatch.Stop();

        Console.WriteLine($"\n=== RESULTS: {testName} ===");
        Console.WriteLine($"Elapsed Time:      {stopwatch.ElapsedMilliseconds} ms");
        Console.WriteLine($"Successful Checks: {successCount}");
        Console.WriteLine($"Failed Checks:     {failCount}");

        double totalSeconds = stopwatch.Elapsed.TotalSeconds;
        double throughput = (successCount + failCount) / (totalSeconds > 0 ? totalSeconds : 1);
        Console.WriteLine($"Throughput:        {Math.Round(throughput)} checks/sec\n");
    }
}
