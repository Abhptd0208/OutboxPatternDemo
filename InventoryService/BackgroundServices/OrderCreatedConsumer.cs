using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using SharedKernel;
using System.Text;
using System.Text.Json;

namespace InventoryService.BackgroundServices
{
    public class OrderCreatedConsumer : BackgroundService
    {
        private readonly ILogger<OrderCreatedConsumer> _logger;
        private IConnection? _connection;
        private IChannel? _channel;
        private string? _queueName;

        public OrderCreatedConsumer(ILogger<OrderCreatedConsumer> logger)
        {
            _logger = logger;
            InitializeRabbitMq();
        }

        private void InitializeRabbitMq()
        {
            var factory = new ConnectionFactory { HostName = "localhost" };

            _connection = factory.CreateConnectionAsync().GetAwaiter().GetResult();
            _channel = _connection.CreateChannelAsync().GetAwaiter().GetResult();

            _channel.ExchangeDeclareAsync(exchange: "order-exchange", type: ExchangeType.Topic, durable: true).GetAwaiter().GetResult();

            _queueName = "inventory-saga-queue";
            _channel.QueueDeclareAsync(queue: _queueName, durable:true, exclusive:false, autoDelete:false).GetAwaiter().GetResult();

            _channel.QueueBindAsync(_queueName, "order-exchange", "PaymentSuccessEvent").GetAwaiter().GetResult();
            _channel.QueueBindAsync(_queueName, "order-exchange", "PaymentFailedEvent").GetAwaiter().GetResult();
            
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _logger.LogInformation("Inventory OrderCreatedConsumer background service started");

            var consumer = new AsyncEventingBasicConsumer(_channel!);

            consumer.ReceivedAsync += async (model, ea) =>
            {
                var body = ea.Body.ToArray();
                var messagePayload = Encoding.UTF8.GetString(body);
                var routingKey = ea.RoutingKey;

                if(routingKey == "PaymentSuccessEvent")
                {
                    var successData = JsonSerializer.Deserialize<PaymentSuccessEvent>(messagePayload);
                    _logger.LogInformation("Payment verified. Deducing stock for Order : {Id}", successData.OrderId);
                }
                else if(routingKey == "PaymentFailedEvent")
                {
                    var failedData = JsonSerializer.Deserialize<PaymentFailedEvent>(messagePayload);
                    _logger.LogWarning("Restoring stock items for Order: {Id} due to: {Reason}",
            failedData.OrderId, failedData.Reason);

                    // DB Work to put stock back or mark inventory allocation as cancelled
                }
            };

            await _channel!.BasicConsumeAsync(queue: _queueName!, autoAck: false, consumer: consumer, cancellationToken:stoppingToken);

            while (!stoppingToken.IsCancellationRequested)
            {
                await Task.Delay(1000, stoppingToken);
            }
        }

        public override void Dispose()
        {
            _channel?.Dispose();
            _connection?.Dispose();
            base.Dispose();
        }
    }
}
