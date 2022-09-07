using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace RabbitMQError
{
    internal class Program
    {
        static IConnection? connection = null;
        static IModel? channel = null;

        static bool isRunning  = true;
        static bool channelShutdown = false;

        /*
         * Note: you would have multiple latches or multiple boolean
         * flags (like channelShutdown) based on the events that
         * could be raised and how your application responds
         */
        static AutoResetEvent latch = new AutoResetEvent(false);

        static void Main(string[] args)
        {
            if (args.Length == 0)
            {
                throw new ArgumentException("A file path is required as an argument");
            }

            if (!File.Exists(args[0]))
            {
                throw new FileNotFoundException($"File {args[0]} not found");
            }

            Console.CancelKeyPress += new ConsoleCancelEventHandler(CancelHandler);

            var factory = new ConnectionFactory()
            {
                HostName = "localhost",
                ClientProvidedName = "TestClient",
                RequestedHeartbeat = TimeSpan.FromSeconds(10),
                AutomaticRecoveryEnabled = false,
                TopologyRecoveryEnabled = false,
            };

            string exchange = "TestExchange";
            connection = factory.CreateConnection();
            channel = connection.CreateModel();

            while (isRunning)
            {
                try
                {
                    channel.ExchangeDeclare(exchange: exchange, type: ExchangeType.Topic, durable: true);

                    channel.ModelShutdown += ModelShutdown;
                    channel.BasicAcks += BasicAck;
                    channel.ConfirmSelect();

                    var body = Encoding.UTF8.GetBytes(File.ReadAllText(args[0]));

                    Console.WriteLine("Sending message");
                    channel.BasicPublish(exchange: exchange, routingKey: "TestRoute", mandatory: true, basicProperties: channel.CreateBasicProperties(), body: body);

                    Console.WriteLine("Running...");
                    latch.WaitOne();

                    if (!isRunning)
                    {
                        Console.WriteLine("Exiting...");
                        isRunning = false;
                        break;
                    }

                    if (channelShutdown)
                    {
                        try
                        {
                            Console.WriteLine("Creating new channel");
                            channel = connection.CreateModel();
                            Console.WriteLine("Channel created");
                            channelShutdown = false;
                            isRunning = true;
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine(ex.ToString());
                        }
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.ToString());
                }
                finally
                {
                    if (!isRunning)
                    {
                        channel?.Close();
                        channel?.Dispose();
                        connection?.Close();
                        connection?.Dispose();
                    }
                }
            }
        }

        private static void CancelHandler(object? sender, ConsoleCancelEventArgs e)
        {
            Console.WriteLine("CTRL-C pressed, exiting!");
            e.Cancel = true;
            isRunning = false;
            latch.Set();
        }

        private static void BasicAck(object? sender, BasicAckEventArgs e)
        {
            Console.WriteLine($"Message received by RabbitMQ: {e.DeliveryTag}");
        }

        private static void ModelShutdown(object? sender, ShutdownEventArgs e)
        {
            Console.WriteLine("Channel shutdown");
            channelShutdown = true;
            latch.Set();
        }
    }
}
