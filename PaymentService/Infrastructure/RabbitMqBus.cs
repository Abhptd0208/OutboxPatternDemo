
using RabbitMQ.Client;
using System.Text;

namespace PaymentService.Infrastructure;

public class RabbitMqBus : IMessageBus, IDisposable
{
    private readonly IConnection _connection;
    private readonly IChannel _channel;

    public RabbitMqBus(IConfiguration configuration)
    {
        var factory = new ConnectionFactory
        {
            HostName = configuration["RabbitMQ:Host"] ?? "localhost"
        };

        _connection = factory.CreateConnectionAsync().GetAwaiter().GetResult();
        _channel = _connection.CreateChannelAsync().GetAwaiter().GetResult();
    }

    public async Task PublishAsync(string exchange, string routingKey, string messagePayload)
    {
        await _channel.ExchangeDeclareAsync(exchange: exchange, type: ExchangeType.Topic, durable: true);

        var body = Encoding.UTF8.GetBytes(messagePayload);

        await _channel.BasicPublishAsync(
            exchange: exchange,
            routingKey: routingKey,
            mandatory: false,
            basicProperties: new BasicProperties
            {
                DeliveryMode = DeliveryModes.Persistent
            },
            body: body);
    }

    public void Dispose()
    {
        _channel?.Dispose();
        _connection?.Dispose();
    }
}