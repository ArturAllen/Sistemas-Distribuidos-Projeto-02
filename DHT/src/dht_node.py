import hashlib
import cv2
import grpc
import numpy as np
from concurrent import futures

import grpc._channel
import dht_pb2_grpc
import dht_pb2

import threading
import os

import secrets

peers_file = 'peers.txt'

# Guarda mensagens recebidas de outros nós da DHT
buffer = []

node_pred = None
node_succ = None
node_curr = None

log = []

# Guarda pares chaves valor da DHT
data = {}

server = None

busy = False

# A Fila guardará ações a serem executadas em decorrência de uma requisição de outro nó ou de ação do usuário
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
        if request.node_id == node_curr['id']:
            node_succ = None
        else:
            node_succ = {'id': request.node_id, 'ip': request.ip, 'port': request.port}
    
        return dht_pb2.InformSuccResponse(success=True)

    def Store(self, request, context):
        key = request.key
        value = request.value
        queue.append(f'store')
        queue.append(f'{key}')
        queue.append(value)
        return dht_pb2.StoreResponse(success=True)

    def Retrieve(self, request, context):
        # Buscar o valor da chave solicitada
        queue.append(f'retrieve:{request.key}:{request.node_id}:{request.ip}:{request.port}')
        return dht_pb2.RetrieveResponse(success=True)

    def Found(self, request, context):
        if request.found:
            buffer.append(f'found')
            buffer.append(f'1')
            buffer.append(f'{request.key}')
            buffer.append(request.value)
        else:
            buffer.append(f'found')
            buffer.append(f'0')
            buffer.append(f'{request.key}')

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
    dht_pb2_grpc.add_DHTServicer_to_server(DHTNode(id, ip, port), server)
    server.add_insecure_port(f'[::]:{port}')
    server.start()
    server.wait_for_termination()

# Processador de fila
def queue_processor():
    global node_pred, node_succ, busy
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
                    key = int(queue.pop(0))
                    img_bytes = queue.pop(0)
                    log.append('Stored key value pair.')
                    
                    if node_succ == None or node_pred == None:
                        data[key] = img_bytes

                    elif should_store_locally(node_curr['id'], node_pred['id'], node_succ['id'], key):
                        data[key] = img_bytes
                        log.append('Stored key value pair.')

                    else:
                        with grpc.insecure_channel(f'{node_succ['ip']}:{node_succ['port']}') as channel:
                            stub = dht_pb2_grpc.DHTStub(channel)
                            response = stub.Store(dht_pb2.StoreRequest(key=key, value=img_bytes))
                            if response.success:
                                log.append('Store request routed.')

                case 'retrieve':
                    key = int(sep[1])
                    id = int(sep[2])
                    ip = sep[3]
                    port = int(sep[4])

                    # MELHORAR AQUI
                    if node_pred == None or node_succ == None:
                        if key in data.keys():
                            buffer.append('found')
                            buffer.append('1')
                            buffer.append(f'{key}')
                            buffer.append(data[key])
                        else:
                            buffer.append('found')
                            buffer.append('0')
                            buffer.append(f'{key}')

                    elif should_store_locally(node_curr['id'],node_pred['id'],node_succ['id'],key):
                        # Procura o valor localmente
                        log.append('Looking for value locally')
                        if key in data.keys():
                            if node_curr['id'] == id:
                                buffer.append('found')
                                buffer.append('1')
                                buffer.append(f'{key}')
                                buffer.append(data[key])
                            else:
                                with grpc.insecure_channel(f'{ip}:{port}') as channel:
                                    stub = dht_pb2_grpc.DHTStub(channel)
                                    log.append(f'Returning value {data[key]} with key {key} to node {id}')
                                    response = stub.Found(dht_pb2.FoundRequest(found=True, key=key, value=data[key]))
                        # Valor não está na DHT
                        else:
                            if node_curr['id'] == id:
                                buffer.append('found')
                                buffer.append('0')
                                buffer.append(f'{key}')
                            else:
                                with grpc.insecure_channel(f'{ip}:{port}') as channel:
                                    stub = dht_pb2_grpc.DHTStub(channel)
                                    response = stub.Found(dht_pb2.FoundRequest(found=False, key=key, value=b''))
                    else:
                        # Encaminha a requisição para o sucessor
                        with grpc.insecure_channel(f'{node_succ['ip']}:{node_succ['port']}') as channel:
                            stub = dht_pb2_grpc.DHTStub(channel)
                            response = stub.Retrieve(dht_pb2.RetrieveRequest(key=key, node_id=id, ip=ip, port=port))
                        
                case 'transfer':
                    items_to_transfer = [(k,v) for (k,v) in data.items() if k <= node_pred['id']]

                    with grpc.insecure_channel(f'{node_pred['ip']}:{node_pred['port']}') as channel:
                        stub = dht_pb2_grpc.DHTStub(channel)
                        for (k,v) in items_to_transfer:
                            del data[k]
                            response = stub.Transfer(dht_pb2.TransferRequest(key=k, value=v))

                case 'join':
                    find_neighbors()
                    buffer.append(f'Joined DHT with id {node_curr['id']}. Listening on port {node_curr['port']}.')
                    # busy = False
                    # write_node_info()

                # O novo nó a informa seu predecessor para atualizar o nó sucessor
                case 'inform':
                    with grpc.insecure_channel(f'{node_pred['ip']}:{node_pred['port']}') as channel:
                        stub = dht_pb2_grpc.DHTStub(channel)
                        response = stub.InformNewSucc(
                            dht_pb2.InformSuccRequest(node_id=node_curr['id'], ip=node_curr['ip'], port=node_curr['port']))
                        if response.success:
                            log.append('Predecessor node informed')

                # O nó está saindo da DHT. Informar os vizinhos, transferir os pares, encerrar threads e servidor
                case 'leave':
                    if node_succ != None:
                        # Informar saida ao sucessor
                        with grpc.insecure_channel(f'{node_succ['ip']}:{node_succ['port']}') as channel:
                            stub = dht_pb2_grpc.DHTStub(channel)
                            response = stub.Leave(
                                dht_pb2.LeaveRequest(node_id=node_pred['id'], ip=node_pred['ip'], port=node_pred['port']))
                            
                            # Transfere os pares deste nó ao sucessor
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
    global node_succ, node_pred, busy
    while True:
        if busy:
            while buffer != []:
                action = buffer.pop(0)
                
                if action == 'found':
                    success = buffer.pop(0)

                    if success == '1':
                        key = buffer.pop(0)
                        img_bytes = buffer.pop(0)

                        nparr = np.frombuffer(img_bytes, np.uint8)

                        img = cv2.imdecode(nparr, cv2.IMREAD_COLOR)

                        cv2.imshow(f'{key}', img)
                        cv2.waitKey(0)
                        cv2.destroyAllWindows()

                    else:
                        key = buffer.pop(0)
                        print(f'Key {key} not found')
                else:
                    print(action)
                
                busy = False

            continue


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
                path = input('Path: ')

                img = cv2.imread(path)
                _,encoded = cv2.imencode('.jpg', img)
                img_bytes = encoded.tobytes()

                queue.append(f'store')
                queue.append(f'{short_hash(raw_key.encode())}')
                queue.append(img_bytes)

            case 'retrieve':
                raw_key = input('Key: ')
                queue.append(f'retrieve:{short_hash(raw_key.encode())}:{node_curr['id']}:{node_curr['ip']}:{node_curr['port']}')
                busy = True

            # case 'open':
            #     path = "C:\\Users\\artur\\OneDrive\\Documentos\\Projetos\\Python\\DHT\\imgs\\giraffe-6378717_640.jpg"

            #     # Display the image in a window
            #     cv2.imshow('Image', img)

            #     # Wait for a key press and close the window
            #     cv2.waitKey(0)
            #     cv2.destroyAllWindows()

            case 'messages':
                if buffer == []:
                    print('No messages')
                
                while buffer != []:
                    message = buffer.pop(0)
                    print(message)

            case 'cmd':
                print(eval(input()))

            case 'data':
                print(data)

            case 'neighbors':
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

# Calcula um hash de 64 bits de uma chave
def short_hash(value):
    return int.from_bytes(hashlib.sha256(value).digest()[:8], 'little')
    
def should_store_locally(current_node_id, predecessor_id, successor_id, data_id):
    if predecessor_id < current_node_id:
        return predecessor_id < data_id <= current_node_id
    else:
        return data_id > predecessor_id or data_id <= current_node_id

#if __name__ == '__main__':
def init():
    global node_pred, node_succ, node_curr, server, busy
    set_node_params()

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    dht_pb2_grpc.add_DHTServicer_to_server(DHTNode(node_curr['id'], node_curr['ip'], node_curr['port']), server)
    server.add_insecure_port(f'[::]:{node_curr['port']}')
    server.start()

    job_thread = threading.Thread(target=queue_processor)
    job_thread.start()
    
    busy = True
    client_thread = threading.Thread(target=client)
    client_thread.start()
    
    queue.append('join')

    server.wait_for_termination()

init()
