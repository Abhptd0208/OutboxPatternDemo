using OrderService.data;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using SharedKernel;
using System.Text;
using System.Text.Json;

namespace OrderService.BackgroundServices
{
    public class OrderSagaResponseConsumer : BackgroundService
    {
        private readonly IServiceProvider _serviceProvider;
        private readonly ILogger<OrderSagaResponseConsumer> _logger;
        private IConnection? _connection;
        private IChannel? _channel;
        private string? _queueName;
        public OrderSagaResponseConsumer(IServiceProvider serviceProvider, ILogger<OrderSagaResponseConsumer> logger)
        {
            _serviceProvider = serviceProvider;
            _logger = logger;
            InitializeRabbitMq();

        }

        private void InitializeRabbitMq()
        {
            var factory = new ConnectionFactory { HostName="localhost" };

            _connection = factory.CreateConnectionAsync().GetAwaiter().GetResult();
            _channel = _connection.CreateChannelAsync().GetAwaiter().GetResult();

            _channel.ExchangeDeclareAsync(exchange:"order-exchange", type: ExchangeType.Topic, durable: true).GetAwaiter().GetResult();

            _queueName = "order-service-saga-queue";
            _channel.QueueDeclareAsync(queue:_queueName, durable: true, exclusive: false, autoDelete:false).GetAwaiter().GetResult();

            _channel.QueueBindAsync(_queueName, "order-exchange", "PaymentSuccessEvent");
            _channel.QueueBindAsync(_queueName, "order-exchange", "PaymentFailedEvent");
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _logger.LogInformation("Order Saga Response Consumer has started.");

            var consumer = new AsyncEventingBasicConsumer(_channel!);

            consumer.ReceivedAsync += async (model, ea) =>
            {
                try
                {
                    var body = ea.Body.ToArray();
                    var messagePayload = Encoding.UTF8.GetString(body);
                    var routingKey = ea.RoutingKey;

                    // Open a safe scope to request our scoped DbContext
                    using var scope = _serviceProvider.CreateScope();
                    var dbContext = scope.ServiceProvider.GetRequiredService<OrderDbContext>();

                    if (routingKey == "PaymentSuccessEvent")
                    {
                        var successEvent = JsonSerializer.Deserialize<PaymentSuccessEvent>(messagePayload);
                        if (successEvent != null)
                        {
                            var order = await dbContext.Orders.FindAsync(successEvent.OrderId);
                            if (order != null)
                            {
                                order.Status = "Confirmed"; //Update Status
                                _logger.LogInformation("Order {Id} status updated to CONFIRMED.", successEvent.OrderId);
                            }
                        }
                    }
                    else if (routingKey == "PaymentFailedEvent")
                    {
                        var failedEvent = JsonSerializer.Deserialize<PaymentFailedEvent>(messagePayload);
                        if (failedEvent != null)
                        {
                            var order = await dbContext.Orders.FindAsync(failedEvent.OrderId);
                            if (order != null)
                            {
                                order.Status = "Cancelled"; // Update Status
                                _logger.LogWarning("❌ Order {Id} status updated to CANCELLED. Reason: {Reason}", failedEvent.OrderId, failedEvent.Reason);
                            }
                        }
                    }

                    // Save changes back to the SQL database atomically
                    await dbContext.SaveChangesAsync(stoppingToken);

                    // Acknowledge the message to remove it from the RabbitMQ queue
                    await _channel!.BasicAckAsync(deliveryTag: ea.DeliveryTag, multiple: false);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error processing Saga response update for OrderService.");
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
