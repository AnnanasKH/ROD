import pika
import sys
import threading
import time
# from random import randomint

# 1-N 2-rank 3-v 4-ttl
N = int(sys.argv[1])

proc_n = int(sys.argv[2])

v = int(sys.argv[3])

ttl = int(sys.argv[4])

buf = []

V = [0 for k in range(N)]

data_lock = threading.Lock()


def cmpV(V1, V2, i):
    for k in range(N):
        if k != i:
            if V1[k] < V2[k]:
                return False
        elif V1[i] != V2[i] - 1:
            return False
    return True


def notprevV(V1, V2):
    for k in range(N):
        if V1[k] > V2[k]:
            return True
    return False


def split_message(body):
    message, i, RV = body.split(b'@')
    i = int(i)
    RV = eval(RV)
    return [message, i, RV]


def connect():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()
    channel.exchange_declare(exchange='multicast', exchange_type='fanout')
    # random name, after connection is closed the queue is deleted
    result = channel.queue_declare(queue='',
                                   exclusive=True,
                                   arguments={'x-message-ttl': ttl * 1000})
    queue_name = result.method.queue
    channel.queue_bind(exchange='multicast', queue=queue_name)
    return connection, channel, queue_name


counter = 0


def publisher(channel):
    while True:
        time.sleep(v)
        if proc_n == 0:
            global counter
            with data_lock:
                if counter == 0:
                    V[proc_n] = 1
                elif counter % 2:
                    V[proc_n] = counter + 2
                else:
                    V[proc_n] = counter
            counter += 1
        else:
            with data_lock:
                V[proc_n] += 1
        t = int(time.time()) % 100
        print("when send V ", V, t)
        message = str(t)
        message += '@' + str(proc_n) + '@' + str(V)
        channel.basic_publish(exchange='multicast',
                              routing_key='',
                              body=message)


def start_publisher():
    connection, channel, queue_name = connect()
    publisher(channel)
    # connection.close()


def append_sort(body):
    z = 0
    if len(buf) == 0:
        buf.append(body)
    else:
        while not notprevV(split_message(buf[z])[2], split_message(body)[2]):
            z += 1
            if z == len(buf):
                break
        buf.insert(z, body)


def callback(ch, method, properties, body):
    message, i, RV = split_message(body)
    if i != proc_n:
        if len(buf) != 0:
            if notprevV(RV, split_message(buf[0])[2]):
                append_sort(body)
                # print('delay', body)
                if cmpV(V, split_message(buf[0])[2], split_message(buf[0])[1]):
                    message, i, RV = split_message(buf.pop(0))
                else:
                    return
        if not cmpV(V, RV, i):
            # print('delay1', body)
            append_sort(body)
            return
        with data_lock:
            for k in range(N):
                V[k] = max(V[k], RV[k])
        print(" [x] Recieved %r" % message)
        print(RV, V)


def start_subscriber():
    connection, channel, queue_name = connect()
    print("subscriber")
    channel.basic_consume(queue=queue_name,
                          on_message_callback=callback,
                          auto_ack=True)
    channel.start_consuming()


def start():
    publish_thread = threading.Thread(target=start_publisher, args=())
    subscriber_thread = threading.Thread(target=start_subscriber, args=())
    publish_thread.start()
    subscriber_thread.start()


start()
