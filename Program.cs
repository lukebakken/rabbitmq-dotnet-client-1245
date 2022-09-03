using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace RabbitMQError
{
    internal class Program
    {
        static IConnection connection = null;
        static IModel channel = null;

        static void Main(string[] args)
        {
            if (args.Length == 0)
                throw new ArgumentException("A file path is required as an argument");
            if (!File.Exists(args[0]))
                throw new FileNotFoundException($"File {args[0]} not found");

            var factory = new ConnectionFactory()
            {
                HostName = "localhost",
                ClientProvidedName = "TestClient",
                RequestedHeartbeat = TimeSpan.FromSeconds(10),
                AutomaticRecoveryEnabled = false,
                TopologyRecoveryEnabled = false,
            };

            try
            {
                string exchange = "TestExchange";
                connection = factory.CreateConnection();
                channel = connection.CreateModel();

                channel.ExchangeDeclare(exchange: exchange, type: ExchangeType.Topic, durable: true);

                channel.ModelShutdown += ModelShutdown;
                channel.BasicAcks += BasicAck;
                channel.ConfirmSelect();

                var body = Encoding.UTF8.GetBytes(File.ReadAllText(args[0]));

                Console.WriteLine("Sending message");
                channel.BasicPublish(exchange: exchange,
                     routingKey: "TestRoute",
                     mandatory: true,
                     basicProperties: channel.CreateBasicProperties(),
                     body: body);

                Console.WriteLine("Press enter to exit");
                Console.ReadLine();
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
            }
            finally
            {
                channel?.Close();
                channel?.Dispose();
                connection?.Close();
                connection?.Dispose();
            }

        }

        private static void BasicAck(object sender, BasicAckEventArgs e)
        {
            Console.WriteLine($"Message received {e.DeliveryTag}");
        }

        private static void ModelShutdown(object sender, ShutdownEventArgs e)
        {
            Console.WriteLine("Channel shutdown");
            if (e.ReplyCode == RabbitMQ.Client.Constants.ReplySuccess)
                return;

            try
            {
                Console.WriteLine("Creating new channel");
                channel = connection.CreateModel();
                Console.WriteLine("Channel created");
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
            }
        }
    }
}
