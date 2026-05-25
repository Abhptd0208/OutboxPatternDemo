using Microsoft.AspNetCore.Mvc;
using OrderService.data;
using OrderService.entities;
using SharedKernel;
using System.Text.Json;

namespace OrderService.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class OrdersController : ControllerBase
    {
        private readonly OrderDbContext _context;
        public OrdersController(OrderDbContext context)
        {
            _context = context;
        }

        [HttpPost]
        public async Task<IActionResult> CreateOrder([FromBody] CreateOrderRequest request
            )
        {
            var order = new Order
            {
                Id = Guid.NewGuid(),
                CustomerId = request.CustomerId,
                TotalAmount = request.TotalAmount,
                Status = request.Status,
                CreatedAtUtc = DateTime.UtcNow,
            };

            var orderCreatedEvent = new OrderCreatedEvent(
                order.Id, order.CustomerId, order.TotalAmount
            );

            var outboxMessage = new OutboxMessage
            {
                Id = Guid.NewGuid() ,
                Type = typeof(OrderCreatedEvent).Name,
                Content = JsonSerializer.Serialize(orderCreatedEvent),
                OccurredOnUtc = DateTime.UtcNow,
                ProcessedOnUtc = null
            };

            await _context.Orders.AddAsync(order);
            await _context.OutboxMessages.AddAsync(outboxMessage);

            await _context.SaveChangesAsync();

            return Ok(new { Message = "Order placed successfully!", OrderId = order.Id });
        }
    }

    public class CreateOrderRequest
    {
        public Guid CustomerId { get; set; }
        public decimal TotalAmount { get; set; }
        public string Status { get; set; } = "Pending";
    }

}
