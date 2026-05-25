using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using SharedKernel;
using System.Text;
using System.Text.Json;

namespace PaymentService.BackgroundServices
{
    public class OrderPaymentConsumer : BackgroundService
    {
        private readonly ILogger<OrderPaymentConsumer> _logger;
        private IConnection? _connection;
        private IChannel? _channel;
        private string? _queueName;

        public OrderPaymentConsumer(ILogger<OrderPaymentConsumer> logger)
        {
            _logger = logger;
            InitializeRabbitMq();
        }
        private void InitializeRabbitMq()
        {
            var factory = new ConnectionFactory { HostName="localhost" };

            _connection = factory.CreateConnectionAsync().GetAwaiter().GetResult();
            _channel = _connection.CreateChannelAsync().GetAwaiter().GetResult();

            _channel.ExchangeDeclareAsync(exchange: "order-exchange", type: ExchangeType.Topic, durable: true).GetAwaiter().GetResult();

            _queueName = "payment-service-queue";
            _channel.QueueDeclareAsync(queue: _queueName, durable: true, exclusive: false, autoDelete: false).GetAwaiter().GetResult();

            _channel.QueueBindAsync(queue: _queueName, exchange: "order-exchange", routingKey: "OrderCreatedEvent").GetAwaiter().GetResult();
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _logger.LogInformation("Payment OrderPaymentConsumer background service started.");

            var consumer = new AsyncEventingBasicConsumer(_channel!);

            consumer.ReceivedAsync += async (model, ea) =>
            {
                try
                {
                    var body = ea.Body.ToArray();
                    var messagePayload = Encoding.UTF8.GetString(body);

                    var orderEvent = JsonSerializer.Deserialize<OrderCreatedEvent>(messagePayload);

                    if (orderEvent != null)
                    {
                        // DB Simulation This is where we would interact with our PaymentDb
                        _logger.LogInformation("Charging customer card for Order ID: {OrderId} | Amount: ${Amount}",
                            orderEvent.OrderId, orderEvent.TotalAmount);
                    }

                    // Acknowledge receipt
                    await _channel!.BasicAckAsync(deliveryTag: ea.DeliveryTag, multiple: false);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error processing customer payment execution.");
                    await _channel!.BasicNackAsync(deliveryTag: ea.DeliveryTag, multiple: false, requeue: true);
                }
            };

            await _channel!.BasicConsumeAsync(queue: _queueName!, autoAck: false, consumer: consumer, cancellationToken: stoppingToken);

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
