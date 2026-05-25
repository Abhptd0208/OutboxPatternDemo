namespace SharedKernel
{
    public record OrderCreatedEvent(
        Guid OrderId,
        Guid CustomerId,
        decimal TotalAmount
    );
}