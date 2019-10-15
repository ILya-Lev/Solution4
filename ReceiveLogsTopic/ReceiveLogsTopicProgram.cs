using System;
using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace ReceiveLogsTopic
{
    class ReceiveLogsTopicProgram
    {
        private const string ExchangeName = "logs_topic";

        static void Main(string[] args)
        {
            var factory = new ConnectionFactory() {HostName = "localhost"};
            using (var connection = factory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                channel.ExchangeDeclare(ExchangeName, ExchangeType.Topic);
                var queueName = channel.QueueDeclare().QueueName;

                if (!CreateBindings(channel, ExchangeName, queueName, args))
                {
                    Environment.ExitCode = 1;
                    return;
                }

                var consumer = new EventingBasicConsumer(channel);
                consumer.Received += OnMessageDelivered;

                channel.BasicConsume(queueName, true, consumer);
                Console.WriteLine("To exit the application press enter");
                Console.ReadLine();
            }
        }

        private static bool CreateBindings(IModel channel, string exchangeName, string queueName, string[] args)
        {
            if (args == null || args.Length == 0)
            {
                Console.WriteLine("Usage is [binding_key...]");
                Console.WriteLine("To exit the application press enter");
                Console.ReadLine();
                return false;
            }

            foreach (var routingKey in args)
            {
                channel.QueueBind(queueName, exchangeName, routingKey);
            }

            return true;
        }

        private static void OnMessageDelivered(object sender, BasicDeliverEventArgs e)
        {
            var message = Encoding.UTF8.GetString(e.Body);
            Console.WriteLine($"Received {e.RoutingKey}: {message}");
        }
    }
}
