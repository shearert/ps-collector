
import sharedrabbitmq

# Shared RabbitMQ
shared_rabbitmq = None

def get_rabbitmq_connection(cp):
    global shared_rabbitmq
    if shared_rabbitmq == None:
        shared_rabbitmq = sharedrabbitmq.SharedRabbitMQ(cp)
    return shared_rabbitmq