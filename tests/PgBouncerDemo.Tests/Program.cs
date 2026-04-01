using System.Diagnostics;
using Npgsql;

// Connection string with PgBouncer config
string connectionString = "Host=localhost;Port=6432;Database=falcon;Username=postgres;Password=toorqwe1234r;Pooling=True;Minimum Pool Size=10;Maximum Pool Size=200;Timeout=30;No Reset On Close=true;";

int workerCount = 500;       // Number of concurrent background workers
int queriesPerWorker = 2000; // Number of monitors checked by each worker

Console.WriteLine("=== Starting Stress Test for Falcon Monitor Check Simulation ===");
Console.WriteLine($"Active Workers: {workerCount}, Total Monitor Checks: {workerCount * queriesPerWorker}");

int successCount = 0;
int failCount = 0;

var stopwatch = Stopwatch.StartNew();

// Modern, high-performance connection pooling via NpgsqlDataSource
await using var dataSource = NpgsqlDataSource.Create(connectionString);

// Launch parallel workers to simulate concurrent monitor checks
await Parallel.ForEachAsync(
    Enumerable.Range(1, workerCount),
    new ParallelOptions { MaxDegreeOfParallelism = workerCount },
    async (workerId, ct) =>
    {
        // Thread-safe random generator to simulate different monitor IDs and latencies
        var rnd = new Random(Guid.NewGuid().GetHashCode());

        for (int i = 0; i < queriesPerWorker; i++)
        {
            try
            {
                // 1. Lease a connection from PgBouncer pool
                await using var conn = await dataSource.OpenConnectionAsync(ct);

                // =========================================================================
                // FALCON SCENARIO: Simulating a monitor status UPDATE/INSERT
                // =========================================================================
                // Buni loyihangizdagi haqiqiy SQL so'rovga almashtirishingiz tavsiya etiladi!
                string falconQuery = @"
                    SELECT 1; -- HOZIRCHA ODDIY SELECT (Test uchun)
                ";

                await using var cmd = new NpgsqlCommand(falconQuery, conn);

                // cmd.Parameters.AddWithValue("monitorId", rnd.Next(1, 1000000));
                // cmd.Parameters.AddWithValue("status", rnd.Next(0, 100) > 5 ? "UP" : "DOWN"); // 95% UP, 5% DOWN
                // cmd.Parameters.AddWithValue("latency", rnd.Next(10, 1500)); // 10ms dan 1.5s gacha javob vaqti

                // 2. Execute the monitor check write operation
                await cmd.ExecuteNonQueryAsync(ct);

                Interlocked.Increment(ref successCount);
            }
            catch (Exception ex)
            {
                Interlocked.Increment(ref failCount);
                // Log only the first few errors to avoid console spam during extreme load
                if (failCount <= 5)
                    Console.WriteLine($"Error (Worker {workerId}): {ex.Message}");
            }
        }
    });

stopwatch.Stop();

Console.WriteLine("\n=== Stress Test Results ===");
Console.WriteLine($"Elapsed Time: {stopwatch.ElapsedMilliseconds} ms");
Console.WriteLine($"Successful Checks: {successCount}");
Console.WriteLine($"Failed Checks: {failCount}");
Console.WriteLine($"Throughput: {Math.Round((successCount + failCount) / stopwatch.Elapsed.TotalSeconds)} checks/sec");
