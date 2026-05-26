using PaymentService.Infrastructure;
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
        private readonly IMessageBus _messageBus;

        public OrderPaymentConsumer(ILogger<OrderPaymentConsumer> logger, IMessageBus messageBus)
        {
            _logger = logger;
            _messageBus = messageBus;
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
                        if (orderEvent.TotalAmount > 100)
                        {
                            _logger.LogWarning("Payment Declined! Amount ${Amount} exceeds limit for order {id}", orderEvent.TotalAmount, orderEvent.OrderId);
                            var failedEvent = new PaymentFailedEvent(orderEvent.OrderId, "Card Declined: Insufficient Funds");
                            await _messageBus.PublishAsync("order-exchange", "PaymentFailedEvent", JsonSerializer.Serialize(failedEvent));
                        }
                        else
                        {
                            _logger.LogInformation("Payment successful for Order {Id}", orderEvent.OrderId);
                            var successEvent = new PaymentSuccessEvent(orderEvent.OrderId, orderEvent.CustomerId, orderEvent.TotalAmount);
                            await _messageBus.PublishAsync("order-exchange", "PaymentSuccessEvent", JsonSerializer.Serialize(successEvent));
                        }
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
