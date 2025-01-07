using Confluent.Kafka;
using Microsoft.AspNetCore.SignalR;
using SignalR_Kafka.SignalR;

namespace SignalR_Kafka.KafkaService
{
    public class KafkaConsumerService
    {
        private readonly IHubContext<ChatHub> _hubContext;
        private readonly ConsumerConfig _consumerConfig;

        public KafkaConsumerService(IHubContext<ChatHub> hubContext)
        {
            _hubContext = hubContext;

            // Configure Kafka consumer
            _consumerConfig = new ConsumerConfig
            {
                BootstrapServers = "localhost:9092",
                GroupId = "signalr-group",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };
        }

        public void StartConsuming(string topic)
        {
            Task.Run(() => ConsumeMessagesAsync(topic));
        }

        private async Task ConsumeMessagesAsync(string topic)
        {
            using (var consumer = new ConsumerBuilder<string, string>(_consumerConfig).Build())
            {
                consumer.Subscribe(topic);
                var cts = new CancellationTokenSource();
                try
                {
                    while (true)
                    {
                        var consumeResult = consumer.Consume(cts.Token);
                        if (consumeResult != null)
                        {
                            await _hubContext.Clients.Group(consumeResult.Message.Key).SendAsync("ReceiveMessage", consumeResult.Message.Value);
                            Console.WriteLine($"{consumeResult.Message.Key}: {consumeResult.Message.Value}");
                        }
                    }
                }
                catch (ConsumeException e)
                {
                    Console.WriteLine($"Error occurred: {e.Error.Reason}");
                }
                finally
                {
                    cts.Cancel();
                    consumer.Close();
                }
            }
        }
    }
}