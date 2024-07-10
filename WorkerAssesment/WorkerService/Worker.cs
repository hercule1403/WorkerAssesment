using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Data.SqlClient;


namespace Worker
{
    public class WorkerService : BackgroundService
    {
        private readonly ILogger<WorkerService> _logger;
        private string _workerId;
        private string _connectionString;

        public WorkerService(ILogger<WorkerService> logger)
        {
            _logger = logger;
            _workerId = Environment.MachineName;
            _connectionString = "Server=TISED-042;Database=Assesment;Integrated Security=True;";

        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                var items = GetItemsForWorker();
                foreach (var item in items)
                {
                    IncrementValue(item);
                }
                _logger.LogInformation($"Worker {_workerId} processing items: {string.Join(", ", items.Select(i => i.Id))}");
                await Task.Delay(1000, stoppingToken);
            }
        }

        private List<Item> GetItemsForWorker()
        {
            var items = new List<Item>();
            using (var connection = new SqlConnection(_connectionString))
            {
                connection.Open();
                var command = new SqlCommand($"SELECT * FROM worker WHERE CurrentWorker = '{_workerId}'", connection);
                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        items.Add(new Item
                        {
                            Id = reader.GetGuid(0).ToString(),
                            Value = reader.GetInt32(1),
                            CurrentWorker = reader.GetString(2)
                        });
                    }
                }
            }
            return items;
        }

        private void IncrementValue(Item item)
        {
            using (var connection = new SqlConnection(_connectionString))
            {
                connection.Open();
                var command = new SqlCommand($"UPDATE items SET Value = Value + 1 WHERE Id = '{item.Id}'", connection);
                command.ExecuteNonQuery();
            }
        }
    }

    public class Item
    {
        public string Id { get; set; }
        public int Value { get; set; }
        public string CurrentWorker { get; set; }
    }
}
