using Microsoft.EntityFrameworkCore;
using OrderService.entities;
using SharedKernel;

namespace OrderService.data
{
    public class OrderDbContext : DbContext
    {
        public OrderDbContext(DbContextOptions<OrderDbContext> options) : base(options) {
        }

        public DbSet<Order> Orders => Set<Order>();
        public DbSet<OutboxMessage> OutboxMessages => Set<OutboxMessage>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            base.OnModelCreating(modelBuilder);

            modelBuilder.Entity<Order>(builder =>
            {
                builder.HasKey(o => o.Id);
                builder.Property(o => o.TotalAmount).HasPrecision(18, 2);
            });

            modelBuilder.Entity<OutboxMessage>(builder =>
            {
                builder.HasKey(m => m.Id);

                builder.HasIndex(m => m.ProcessedOnUtc).HasFilter("[ProcessedOnUtc] IS NULL");
            });
        }

    }
}
