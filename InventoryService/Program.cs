using InventoryService.BackgroundServices; 

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddHostedService<OrderCreatedConsumer>();

var app = builder.Build();
app.Run();