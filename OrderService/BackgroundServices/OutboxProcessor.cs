using Microsoft.EntityFrameworkCore;
using OrderService.data;
using OrderService.Infrastructure;

namespace OrderService.BackgroundServices;

public class OutboxProcessor : BackgroundService
{
    private readonly IServiceProvider _serviceProvider;
    private readonly ILogger<OutboxProcessor> _logger;

    public OutboxProcessor(IServiceProvider serviceProvider, ILogger<OutboxProcessor> logger)
    {
        _serviceProvider = serviceProvider;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Outbox Processor background service is starting.");

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await ProcessOutboxMessagesAsync(stoppingToken);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "An error occurred while executing the outbox background processor.");
            }

            await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken);
        }
    }

    private async Task ProcessOutboxMessagesAsync(CancellationToken stoppingToken)
    {
        using var scope = _serviceProvider.CreateScope();
        var context = scope.ServiceProvider.GetRequiredService<OrderDbContext>();
        var messageBus = scope.ServiceProvider.GetRequiredService<IMessageBus>();

        var messages = await context.OutboxMessages
            .Where(m => m.ProcessedOnUtc == null)
            .OrderBy(m => m.OccurredOnUtc)
            .Take(20)
            .ToListAsync(stoppingToken);

        if (!messages.Any()) return;

        _logger.LogInformation("Found {Count} unprocessed outbox messages.", messages.Count);

        foreach (var message in messages)
        {
            try
            {
                await messageBus.PublishAsync(
                    exchange: "order-exchange",
                    routingKey: message.Type,
                    messagePayload: message.Content);

                message.ProcessedOnUtc = DateTime.UtcNow;
                message.Error = null;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to publish message {MessageId}", message.Id);
                message.Error = ex.Message;
            }
        }

        await context.SaveChangesAsync(stoppingToken);
    }
}