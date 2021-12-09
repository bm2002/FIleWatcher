using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace WorkerFileWatcher
{
    public class Worker : BackgroundService
    {
        private readonly ILogger<Worker> _logger;
        private static int _sPdfCounter;
        private static int _sCsvCounter;
        private static int _sTxtCounter;
        private static int _sOtherCounter;

        public Worker(ILogger<Worker> logger)
        {
            _logger = logger;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                _logger.LogInformation("Worker running at: {time}", DateTimeOffset.Now);

                var task = Task.Factory.StartNew(() => ReadFiles());
                task.Wait(stoppingToken);

                Thread.Sleep(10000);
            }
        }


        private void ReadFiles()
        {
            var files = Directory.GetFiles(@"C:\_fileWatcher")
                .Select(x => new
                {
                    path = x,
                    expansion = !new string[] {"pdf", "csv", "txt"}.Contains(x.Split('.').Last().ToLower())
                        ? "other"
                        : x.Split('.').Last().ToLower()
                })
                .GroupBy(g => g.expansion)
                .Select(x => Tuple.Create(x.Key, x.Select(_ => _.path).ToList()))
                .ToList();
            
            files.AsParallel().ForAll(x =>
            {
                Processing(x.Item1, x.Item2);
            });

        }

        private void Processing(string expansion, List<string> files)
        {
            switch (expansion)
            {
                case "pdf":
                    foreach (var file in files)
                    {
                        Interlocked.Increment(ref _sPdfCounter);
                        Thread.Sleep(5000);
                        ConsoleWriteLineWithTime(file);
                        File.Delete(file);
                        ConsoleWriteLineWithTime($"Обработано pdf-файлов: {_sPdfCounter}");
                    }
                    break;
                case "csv":
                    foreach (var file in files)
                    {
                        Interlocked.Increment(ref _sCsvCounter);
                        Thread.Sleep(5000);
                        ConsoleWriteLineWithTime(file);
                        File.Delete(file);
                        ConsoleWriteLineWithTime($"Обработано csv-файлов: {_sCsvCounter}");
                    }
                    break;
                case "txt":
                    foreach (var file in files)
                    {
                        Interlocked.Increment(ref _sTxtCounter);
                        Thread.Sleep(5000);
                        ConsoleWriteLineWithTime(file);
                        File.Delete(file);
                        ConsoleWriteLineWithTime($"Обработано txt-файлов: {_sTxtCounter}");
                    }
                    break;
                default:
                    foreach (var file in files)
                    {
                        Thread myNewThread = new Thread(() => MultithreadedProcessing(file));
                        myNewThread.Start();
                    }
                    break;
            }
        }

        private void MultithreadedProcessing(string file)
        {
            if (!File.Exists(file))
            {
                return;
            }
            Interlocked.Increment(ref _sOtherCounter);
            Thread.Sleep(5000);
            ConsoleWriteLineWithTime(file);
            File.Delete(file);
            ConsoleWriteLineWithTime($"Обработано других файлов: {_sOtherCounter}");

        }

        private void ConsoleWriteLineWithTime(string message) =>
            _logger.LogInformation($"{DateTime.Now.ToString("HH:mm:ss.ffff")} - {message}");




    }
}
