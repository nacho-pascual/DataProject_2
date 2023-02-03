import uuid

clients = {}

num_clients = 100
num_devices = 5

for n in range(0, num_clients):
    client_id = str(uuid.uuid4())
    clients[client_id] = []
    for m in range(0, num_devices):
        clients[client_id] << str(uuid.uuid4())

