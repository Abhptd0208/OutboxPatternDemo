namespace OrderService.Infrastructure
{
    public interface IMessageBus
    {
        Task PublishAsync(string exchange, string routingKey, string messagePayload);
    }
}
