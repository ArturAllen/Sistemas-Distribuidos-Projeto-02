import hashlib
import grpc
from concurrent import futures

import grpc._channel
import dht_pb2_grpc
import dht_pb2

import threading
import os

import secrets

peers_file = 'peers.txt'

buffer = []
node_pred = None
node_succ = None
node_curr = None
log = []

data = {}
temp = {}

server = None

# A Fila guardará ações a serem executadas em decorrência de uma requisição de outro nó
queue = []

class DHTNode(dht_pb2_grpc.DHTServicer):
    def __init__(self, node_id, ip, port):
        self.node_id = node_id
        self.ip = ip
        self.port = port
        self.successor = None
        self.predecessor = None
        self.data = {}  # chave-valor local do nó

    def Join(self, request, context):
        global node_pred, node_succ, node_curr
        # Lógica para o nó se juntar à rede
        # Atualizar predecessor e sucessor
        # log.append('Received join request')
        log.append(f'Join request received by node with id {request.node_id}')
        
        if node_succ == None and node_pred == None:
            node_succ = {'id': request.node_id, 'ip': request.ip, 'port': request.port}
            node_pred = {'id': request.node_id, 'ip': request.ip, 'port': request.port}
            return dht_pb2.JoinResponse(node_id=node_curr['id'], ip=node_curr['ip'], port=node_curr['port'])

        former_pred = node_pred.copy()
        node_pred = {'id': request.node_id, 'ip': request.ip, 'port': request.port}
        queue.append('transfer')
        return dht_pb2.JoinResponse(node_id=former_pred['id'], ip=former_pred['ip'], port=former_pred['port'])

    def InformNewSucc(self, request, context):
        global node_succ
        log.append(f'New Succ informed: {request.node_id}')
    
        if request.node_id == node_curr['id']:
            node_succ = None
        else:
            node_succ = {'id': request.node_id, 'ip': request.ip, 'port': request.port}
    
        return dht_pb2.InformSuccResponse(success=True)

    def Store(self, request, context):
        # Armazenar o valor no nó correto
        key = request.key
        value = request.value
        queue.append(f'store:{key}:{value}')
        return dht_pb2.StoreResponse(success=True)

    def Retrieve(self, request, context):
        # Buscar o valor da chave solicitada
        queue.append(f'retrieve:{request.key}:{request.node_id}:{request.ip}:{request.port}')
        return dht_pb2.RetrieveResponse(success=True)
        # value = self.data.get(key, None)
        # if value:
        #     return dht_pb2.RetrieveResponse(value=value)
        # else:
        #     return dht_pb2.RetrieveResponse(value=b"")

    def Found(self, request, context):
        if request.found:
            temp[request.key] = request.value
        else:
            log.append(f'Key {request.key} not found')

        return dht_pb2.FoundResponse()

    def Leave(self, request, context):
        global node_pred
        log.append(f'New Pred informed: {request.node_id}')
    
        if request.node_id == node_curr['id']:
            node_pred = None
        else:
            node_pred = {'id': request.node_id, 'ip': request.ip, 'port': request.port}
        
        return dht_pb2.LeaveResponse(success=True)
    
    def Transfer(self, request, context):
        data[request.key] = request.value
        return dht_pb2.TransferResponse()

    def Test(self, request, context):
        message = request.content
        buffer.append(message)
        return dht_pb2.TestResponse(success=True)

def serve(id,ip,port):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    # dht_pb2_grpc.add_DHTServicer_to_server(DHTNode(id, "127.0.0.1", port), server)
    dht_pb2_grpc.add_DHTServicer_to_server(DHTNode(id, ip, port), server)
    server.add_insecure_port(f'[::]:{port}')
    server.start()
    server.wait_for_termination()

# Processador de fila
def job():
    global node_pred, node_succ
    while True:
        if queue != []:
            task = queue.pop(0)
            sep = task.split(':')
            command = sep[0].lower().strip()

            match command:
                case 'test':
                    ip = sep[1]
                    port = int(sep[2])
                    message = ':'.join(sep[3:])

                    with grpc.insecure_channel(f'{ip}:{port}') as channel:
                        stub = dht_pb2_grpc.DHTStub(channel)
                        response = stub.Test(dht_pb2.TestRequest(content=message))
                        if response.success:
                            log.append('Message successfully delivered')

                case 'store':
                    key = int(sep[1])
                    value = sep[2]

                    if should_store_locally(node_curr['id'], node_pred['id'], node_succ['id'], key):
                        data[key] = value
                        log.append('Stored key value pair.')

                    else:
                        with grpc.insecure_channel(f'{node_succ['ip']}:{node_succ['port']}') as channel:
                            stub = dht_pb2_grpc.DHTStub(channel)
                            response = stub.Store(dht_pb2.StoreRequest(key=key, value=value))
                            if response.success:
                                log.append('Store request routed.')

                case 'retrieve':
                    key = int(sep[1])
                    id = int(sep[2])
                    ip = sep[3]
                    port = int(sep[4])

                    log.append(f'Retrieve processing {key} {id} {ip} {port}')

                    # TODO: Handle NULL
                    if should_store_locally(node_curr['id'],node_pred['id'],node_curr['id'],key):
                        if key in data.keys():
                            if node_curr['id'] == id:
                                temp[key] = data[key]
                            else:
                                with grpc.insecure_channel(f'{ip}:{port}') as channel:
                                    stub = dht_pb2_grpc.DHTStub(channel)
                                    log.append(f'Returning value {data[key]} with key {key} to node {id}')
                                    response = stub.Found(dht_pb2.FoundRequest(found=True, key=key, value=data[key]))
                        # Valor não está na DHT
                        else:
                            if node_curr['id'] == id:
                                log.append(f'Key {key} not found')
                            else:
                                with grpc.insecure_channel(f'{ip}:{port}') as channel:
                                    stub = dht_pb2_grpc.DHTStub(channel)
                                    response = stub.Found(dht_pb2.FoundRequest(found=False, key=key, value=''))
                    else:
                        log.append(f'Routing retrieve request {key} to successor {node_succ}')
                        try:
                            with grpc.insecure_channel(f'{node_succ['ip']}:{node_succ['port']}') as channel:
                                stub = dht_pb2_grpc.DHTStub(channel)
                                response = stub.Retrieve(dht_pb2.RetrieveRequest(key=key, node_id=id, ip=ip, port=port))
                        except:
                            log.append('Something went terribly wrong!')
                
                case 'transfer':
                    items_to_transfer = [(k,v) for (k,v) in data.items() if k <= node_pred['id']]

                    with grpc.insecure_channel(f'{node_pred['ip']}:{node_pred['port']}') as channel:
                        stub = dht_pb2_grpc.DHTStub(channel)
                        for (k,v) in items_to_transfer:
                            del data[k]
                            response = stub.Transfer(dht_pb2.TransferRequest(key=k, value=v))

                case 'join':
                    find_neighbors()
                    # write_node_info()

                case 'inform':
                    with grpc.insecure_channel(f'{node_pred['ip']}:{node_pred['port']}') as channel:
                        stub = dht_pb2_grpc.DHTStub(channel)
                        response = stub.InformNewSucc(
                            dht_pb2.InformSuccRequest(node_id=node_curr['id'], ip=node_curr['ip'], port=node_curr['port']))
                        if response.success:
                            log.append('Predecessor node informed')

                case 'leave':
                    if node_succ != None:
                        # Informar saida ao sucessor
                        with grpc.insecure_channel(f'{node_succ['ip']}:{node_succ['port']}') as channel:
                            stub = dht_pb2_grpc.DHTStub(channel)
                            response = stub.Leave(
                                dht_pb2.LeaveRequest(node_id=node_pred['id'], ip=node_pred['ip'], port=node_pred['port']))
                            #TODO: TRANSFERIR DADOS PARA SUCESSOR

                            for (k, v) in data.items():
                                response = stub.Transfer(dht_pb2.TransferRequest(key=k, value=v))

                    if node_pred != None:
                        # Informar novo sucessor ao predecessor
                        with grpc.insecure_channel(f'{node_pred['ip']}:{node_pred['port']}') as channel:
                            stub = dht_pb2_grpc.DHTStub(channel)
                            response = stub.InformNewSucc(
                                dht_pb2.InformSuccRequest(node_id=node_succ['id'], ip=node_succ['ip'], port=node_succ['port']))
                        
                    server.stop(grace=1)

                    exit()


def client():
    global node_succ, node_pred
    while True:
        command = input('>>> ').lower().strip()
        match command:
            case 'leave':
                queue.append('leave')
                exit()

            case 'test':
                
                if node_succ == None:
                    ip = 'localhost'
                    port = int(input('Port: '))
                else:
                    ip = node_succ['ip']
                    port = node_succ['port']
                
                message = input('Message: ').strip()
                
                queue.append(f'test:{ip}:{port}:{message}')

            case 'store':
                raw_key = input('Key: ')
                value = input('Value: ')
                queue.append(f'store:{short_hash(raw_key.encode())}:{value}')

            case 'retrieve':
                raw_key = input('Key: ')
                queue.append(f'retrieve:{short_hash(raw_key.encode())}:{node_curr['id']}:{node_curr['ip']}:{node_curr['port']}')

            case 'view':
                if buffer == []:
                    print('No messages')
                
                while buffer != []:
                    message = buffer.pop(0)
                    print(message)

                print(data)

            case 'temp':
                print(temp)

            case 'stats':
                print(f'Current node: {node_curr}')
                print(f'Previous node: {node_pred}')
                print(f'Next node: {node_succ}')

            case 'log':
                for l in log:
                    print(l)

def find_neighbors():
    global node_pred, node_succ
    if not os.path.exists(peers_file):
        return 

    content = ''
    with open(peers_file, 'r') as f:
        content = f.read()

    peers = []
    for line in content.split('\n'):
        comps = line.split(':')
        peers.append({'id': int(comps[0]), 'ip': comps[1], 'port': int(comps[2])})

    sorted_peers = sorted(peers, key=lambda p: p['id'])
    circular_list = [p for p in sorted_peers if p['id'] > node_curr['id']] + [p for p in sorted_peers if p['id'] < node_curr['id']]

    for peer in circular_list:

        id = peer['id']
        ip = peer['ip']
        port = peer['port']

        # print(f'Trying {id}:{ip}:{port}')
        with grpc.insecure_channel(f'{ip}:{port}') as channel:
            stub = dht_pb2_grpc.DHTStub(channel=channel)
            try:
                response = stub.Join(dht_pb2.JoinRequest(node_id=node_curr['id'], ip=node_curr['ip'], port=node_curr['port']))
                
                # Encontrou sucessor
                succ = {'id': id, 'ip': ip, 'port': port}
                pred = {'id': response.node_id, 'ip': response.ip, 'port': response.port}

                if succ != pred:
                    queue.append('inform')

                node_succ = succ
                node_pred = pred
                log.append(f'Found succ: {succ}')
                log.append(f'Found pred: {pred}')
                return

            except grpc._channel._InactiveRpcError:
                log.append(f'Connection with {id}:{ip}:{port} failed.')
                # Nó não respondeu. procurar o próximo
                continue

    # Todos os nós do arquivo estão inativos
    me = {'id': node_curr['id'], 'ip': node_curr['ip'], 'port': node_curr['port']}
    
    return (me, None, None)

def set_node_params():
    global node_curr
    my_id = secrets.randbits(64)
    my_ip = 'localhost'

    if not os.path.exists(peers_file):
        # Este é o primeiro nó da rede
        my_port = 50051
        
        node_curr = {'id': my_id, 'ip': my_ip, 'port': my_port}
        with open(peers_file, 'a') as f:
            f.write(f"{node_curr['id']}:{node_curr['ip']}:{node_curr['port']}")
        return
    
    content = ''
    with open(peers_file, 'r') as f:
        content = f.read()

    peers = []
    for line in content.split('\n'):
        comps = line.split(':')
        peers.append({'id': int(comps[0]), 'ip': comps[1], 'port': int(comps[2])})

    my_port = max([p['port'] for p in peers]) + 1
    node_curr = {'id': my_id, 'ip': my_ip, 'port': my_port}
    write_node_info()
    return

def write_node_info():
    with open(peers_file, 'a') as f:
        f.write(f"\n{node_curr['id']}:{node_curr['ip']}:{node_curr['port']}")

def short_hash(value):
    """
    value has to be byte array.
    """
    return int.from_bytes(hashlib.sha256(value).digest()[:8], 'little')
    
def should_store_locally(current_node_id, predecessor_id, successor_id, data_id):
    """
    Determines if the data with the given ID should be stored locally or routed to the successor.
    
    :param current_node_id: ID of the current node
    :param predecessor_id: ID of the predecessor node
    :param successor_id: ID of the successor node
    :param data_id: ID of the data to store
    :return: True if the data should be stored locally, False if it should be routed to the successor
    """
    if predecessor_id < current_node_id:
        # No wrap-around case
        return predecessor_id < data_id <= current_node_id
    else:
        # Wrap-around case
        return data_id > predecessor_id or data_id <= current_node_id

#if __name__ == '__main__':
def init():
    global node_pred, node_succ, node_curr, server
    set_node_params()

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    # dht_pb2_grpc.add_DHTServicer_to_server(DHTNode(id, "127.0.0.1", port), server)
    dht_pb2_grpc.add_DHTServicer_to_server(DHTNode(node_curr['id'], node_curr['ip'], node_curr['port']), server)
    server.add_insecure_port(f'[::]:{node_curr['port']}')
    server.start()

    job_thread = threading.Thread(target=job)
    job_thread.start()
    
    client_thread = threading.Thread(target=client)
    client_thread.start()
    
    queue.append('join')

    server.wait_for_termination()
    # serve(node_curr['id'], node_curr['ip'], node_curr['port'])

init()
