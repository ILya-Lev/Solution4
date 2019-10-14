using System;
using System.Linq;
using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace ReceiveLogDirect
{
    class ReceiveLogDirectProgram
    {
        private const string InformationSeverity = "info";
        private const string WarningSeverity = "warning";
        private const string ErrorSeverity = "error";
        private static readonly string[] _allSeverities = new[]
        {
            InformationSeverity,
            WarningSeverity,
            ErrorSeverity
        };

        private const string ExchangeName = "direct_logs";

        static void Main(string[] args)
        {
            var factory = new ConnectionFactory() { HostName = "localhost" };
            using (var connection = factory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                channel.ExchangeDeclare(ExchangeName, ExchangeType.Direct);
                var queueName = channel.QueueDeclare().QueueName;

                if (!CreateBindings(args, channel, queueName))
                    return;

                var consumer = new EventingBasicConsumer(channel);
                consumer.Received += (sender, eventArgs) =>
                {
                    var message = Encoding.UTF8.GetString(eventArgs.Body);

                    Console.WriteLine($"Received {eventArgs.RoutingKey} : {message}");
                };

                channel.BasicConsume(queueName, true, consumer);

                Console.WriteLine("To exit the app press enter");
                Console.ReadLine();
            }
        }

        private static bool CreateBindings(string[] args, IModel channel, string queueName)
        {
            var knownSeverities = args?
                .Where(s => !string.IsNullOrWhiteSpace(s))
                .Where(s => _allSeverities.Contains(s))
                .ToArray();

            if (knownSeverities == null || knownSeverities.Length == 0)
            {
                Console.WriteLine("no known severities has been found");
                Console.WriteLine($"Usage: {Environment.GetCommandLineArgs()[0]} [info] [warning] [error]");
                Environment.ExitCode = 1;
                return false;
            }

            foreach (var severity in knownSeverities)
            {
                channel.QueueBind(queueName, ExchangeName, severity);
            }

            return true;
        }
    }
}
