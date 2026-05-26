using System;
using System.Collections.Generic;
using System.Text;

namespace SharedKernel
{
    public record PaymentSuccessEvent(Guid OrderId, Guid CustomerId, decimal Amount);
}
