using System;
using System.Collections.Generic;
using System.Text;

namespace SharedKernel
{
    public record PaymentFailedEvent(Guid OrderId, string Reason);
    
}
