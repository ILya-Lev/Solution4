using System;
using System.Linq;
using System.Text;
using RabbitMQ.Client;

namespace EmitLogDirect
{
    class EmitLogDirectProgram
    {
        private const string InformationSeverity = "info";
        private const string WarningSeverity = "warning";
        private const string ErrorSeverity = "error";
        private const string ExchangeName = "direct_logs";

        static void Main(string[] args)
        {
            var factory = new ConnectionFactory() { HostName = "localhost" };
            using (var connection = factory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                channel.ExchangeDeclare(ExchangeName, ExchangeType.Direct);

                (string message, string severity) = GetMessage(args);
                var body = Encoding.UTF8.GetBytes(message);

                channel.BasicPublish(ExchangeName, severity, null, body);
                Console.WriteLine($"Sent {severity}: {message}");
            }

            Console.WriteLine("to exit press enter");
            Console.ReadLine();
        }

        private static (string message, string severity) GetMessage(string[] args)
        {
            var message = args?.Length > 1
                ? string.Join(" ",args.Skip(1))
                : "hit the road Jack!";
            
            var severity = args?.Length > 0 ? args[0] : InformationSeverity;
            
            return (message, severity);
        }
    }
}
