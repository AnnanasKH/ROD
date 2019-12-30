import pika

global _id
global request

def b_to_str(b):
	return str(b)[2:-1]

def callback(ch, method, properties, body):
	l = b_to_str(body).split(r'\n')
	for w in l:
		print(w)
	request = input()
	channel.basic_publish(exchange='',
						  routing_key='shop',
						  properties=pika.BasicProperties(reply_to = callback_queue,),
                      	  body=request)


connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost'))

channel = connection.channel()

result = channel.queue_declare(queue='', exclusive=True)

callback_queue = result.method.queue

channel.basic_publish(exchange='',
					  routing_key='shop',
					  properties=pika.BasicProperties(reply_to = callback_queue,),
                      body='login')

channel.basic_consume(queue=callback_queue,
                      on_message_callback=callback,
                      auto_ack=True)

channel.start_consuming()