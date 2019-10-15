using System;
using System.Linq;
using System.Text;
using RabbitMQ.Client;

namespace EmitLogTopic
{
    class EmitLogTopicProgram
    {
        private const string ExchangeName = "logs_topic";
        static void Main(string[] args)
        {
            var connectionFactory = new ConnectionFactory() { HostName = "localhost" };
            using (var connection = connectionFactory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                channel.ExchangeDeclare(ExchangeName, ExchangeType.Topic);

                (string message, string facility, string severity) = GetMessage(args);
                var body = Encoding.UTF8.GetBytes(message);

                channel.BasicPublish(ExchangeName, $"{facility}.{severity}", body: body);
                Console.WriteLine($"Sent {facility}.{severity}: {message}");
            }

            Console.WriteLine("to exit the application press enter");
            Console.ReadLine();
        }

        private static (string, string, string) GetMessage(string[] args)
        {
            if (args == null || args.Length == 0)
                return ("incubator", "anonymous", "info");
            
            var keyParts = args[0].Split(new[] {'.'}, StringSplitOptions.RemoveEmptyEntries);
            var message = (args.Length == 1)
                ? "obfuscator"
                : string.Join(" ", args.Skip(1));
            
            return (message, keyParts.First(), keyParts.Last());
        }
    }
}
