from message_broker.src.core.interfaces import BrokerCapability, EnforcingPublisher, ScheduledEnvelope
from message_broker.src.core.context import BrokerContext
from message_broker.src.adapters.redis import RedisBroker

print('Imports OK')
ctx = BrokerContext('redis://localhost:6379')
redis_broker = RedisBroker(ctx)
print('RedisBroker created; capabilities:', redis_broker.capabilities)
pub = redis_broker.get_publisher()
print('Publisher type:', type(pub))
