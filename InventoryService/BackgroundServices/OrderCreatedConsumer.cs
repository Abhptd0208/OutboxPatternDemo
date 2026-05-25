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

            _queueName = "inventory-service-queue";
            _channel.QueueDeclareAsync(queue: _queueName, durable:true, exclusive:false, autoDelete:false).GetAwaiter().GetResult();

            _channel.QueueBindAsync(queue: _queueName, exchange: "order-exchange", routingKey: "OrderCreatedEvent")
                .GetAwaiter().GetResult();
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _logger.LogInformation("Inventory OrderCreatedConsumer background service started");

            var consumer = new AsyncEventingBasicConsumer(_channel!);

            consumer.ReceivedAsync += async (model, ea) =>
            {
                try
                {
                    var body = ea.Body.ToArray();
                    var messagePayload = Encoding.UTF8.GetString(body);

                    _logger.LogInformation("Received Raw Message in Inventory: {Message}", messagePayload);

                    var orderEvent = JsonSerializer.Deserialize<OrderCreatedEvent>(messagePayload);

                    if (orderEvent != null)
                    {
                        _logger.LogInformation("Deducting inventory for Order ID: {OrderId} | Total Amount: {Amount}",
                            orderEvent.OrderId, orderEvent.TotalAmount);

                        //  Inject our InventoryDbContext here to decrease stock!
                    }

                    await _channel!.BasicAckAsync(deliveryTag: ea.DeliveryTag, multiple: false);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error processing inventory deduction event.");

                    await _channel!.BasicNackAsync(deliveryTag: ea.DeliveryTag, multiple: false, requeue: true);
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
