import redis
import random
import json
import pika

r = redis.Redis()

cats = []
stats = {
	'mean_cart':0,
	'visitors': 0,
	'buyers':   0,
	'mean_items':0,
	'total_money_spent':0,
	'purchase': 0,
}
user_id = {}
bought = {}

def b_to_str(b):
	return str(b)[2:-1]

def add_to_cart(user_id, item_id, amount):
	a = r.hget("item{0}".format(item_id),"amount")
	cart_amount = r.hget("cart{0}".format(user_id), item_id)
	name = b_to_str(r.hget("item{0}".format(item_id), "name"))
	if cart_amount:
		if amount + int(cart_amount) > int(a):
	 		return "Sorry, not enough {0} in stock".format(name)
	else:
		if amount > int(a):
			return "Sorry, not enough {0} in stock".format(name)
	r.hincrby("cart{0}".format(user_id), item_id, amount)
	return "item added"

def rem_from_cart(user_id, item_id):
	r.hdel("cart{0}".format(user_id), item_id)
	return "Done"

def init_stock():
	names = ["burrito","tomato","potato"]
	global cats
	cats = ['mexico','italy']
	#{"item12345": 
	#	{
	# 		"name": "potato",
	# 		..., 
	# 		"cat":['mex','ico']
	# 	}
	# }	
	for name in names: 
		r.hmset("item{0}".format(id(name)),{ 
				"name": name,
				"price": random.randint(1,10),
				"amount": 7,
				"cat":json.dumps(['mexico'] if name=='burrito' else ['mexico','italy'])
			})

def discard(user_id):
	for x in r.hkeys("cart{0}".format(user_id)):
		r.hdel("cart{0}".format(user_id),x)
		return "Cart is empty"

def list_categories():
	res = 'Categories:\n'
	for x in cats:
		res += '> '+ x + '\n'
	return res

def show_all():
	k = [x for x in r.keys('item*')]
	res = "All items available:\n"
	for item_id in k:
		item = r.hgetall(item_id)
		_id = b_to_str(item_id)
		name = b_to_str(item[b"name"])
		price = b_to_str(item[b"price"])
		amount = b_to_str(item[b"amount"])
		cat = b_to_str(item[b'cat'])
		res += "> {0}:{1:>10}; price:{2:>3}$; amount:{3:>3}, categories:{4}\n".format(
			_id,name,price,amount,cat
		)
	return res

def show_category(category):
	k = [x for x in r.keys('item*')]
	res = "Items in category <{0}>:\n".format(category)
	for item_id in k:
		cats = json.loads(str(r.hget(item_id,"cat"))[2:-1])
		for cat in cats:
			if cat == category:
				item = r.hgetall(item_id)
				_id = b_to_str(item_id)
				name = b_to_str(item[b"name"])
				price = b_to_str(item[b"price"])
				amount = b_to_str(item[b"amount"])
				res += "> {0}:{1:>10}; price:{2:>3}$; amount:{3:>3}\n".format(
					_id,name,price,amount
				)
				# res += '> ' + b_to_str(item_id) + '\n'
	return res

def buy(user_id):
	ids = r.hgetall("cart{0}".format(user_id)).keys()
	ids = [b_to_str(x) for x in ids]
	cart_price = 0
	# if len(ids) == 0:
	# 	return "Cart is empty"
	for _id in ids: #check if everything in stock
		amount_cart = int(r.hget("cart{0}".format(user_id), _id))
		print(amount_cart)
		amount_stock = int(r.hget("item{0}".format(_id), "amount"))
		if amount_stock:
			if amount_cart == None:
				return "Cart is empty"
			if amount_stock < amount_cart:
				return "Sorry, not enough {0} in stock".format(_id)
		else:
			return "Sorry, not enough {0} in stock".format(_id)
	for _id in ids: #buy
		price = int(r.hget("item{0}".format(_id), "price"))
		amount_cart = int(r.hget("cart{0}".format(user_id), _id))
		cart_price += price * amount_cart
		r.hincrby("item{0}".format(_id), "amount", -amount_cart)
		if int(r.hget("item{0}".format(_id), "amount")) <= 0:
			for x in ['name','price','amount','cat']:
				r.hdel("item{0}".format(_id),x)
		r.hdel("cart{0}".format(user_id),_id)
	stats['total_money_spent'] += cart_price
	if bought.get(user_id) == None:
		stats['buyers'] += 1
		bought[user_id] = 1
		# stats['mean_items'] = (stats['mean_items'] * stats['buyers'] + len(ids)) / stats['buyers']
	# else:
	print(stats['purchase'])
	stats['mean_items'] = (stats['mean_items'] * stats['purchase'] + len(ids)) / (stats['purchase'] + 1)
	stats['purchase'] += 1
	stats['mean_cart'] = stats['total_money_spent'] / stats['purchase'] 
	return "Purchase succesfull!"

def statistics():
	res = "> mean cart price {0:>5}\n> mean items {1:>10}\n> visitors {2}, buyers {3}".format(
			stats["mean_cart"], stats["mean_items"], stats["visitors"], stats["buyers"]
		)
	return res

def name_to_item(n):
	k = [x for x in r.keys('item*')]
	for item_id in k:
		name = b_to_str(r.hget(item_id, 'name'))
		if name == n:
			# print("ITEM_ID IN nametoitem", int(str(item_id)[6:-1]))
			return int(str(item_id)[6:-1])

def show_cart(user_id):
	res = "Cart:\n"
	ids = r.hgetall("cart{0}".format(user_id)).keys()
	print(ids)
	for _id in ids:
		_id = b_to_str(_id)
		name = b_to_str(r.hget("item{0}".format(_id), "name"))
		res += name + ' '
		res += b_to_str(r.hget("cart{0}".format(user_id), _id))
		res += "\n"
	return res

def parse(body, reply):
	w = b_to_str(body).split(' ')
	bs = w[0]
	args = []
	if bs == 'login':
		stats['visitors'] += 1
		user_id[reply] = id(reply)
		return "Login is " + str(user_id[reply])
	if bs == 'add_to_cart':
		if len(w) == 1:
			return "Miss name, amount"
		elif len(w) == 2:
			return "Miss amount"
		return add_to_cart(user_id[reply], name_to_item(w[1]), int(w[2]))
	if bs == 'rem_from_cart':
		if len(w) < 2:
			return "Miss name"
		return rem_from_cart(user_id[reply], name_to_item(w[1])) # !!!!
	if bs == 'buy':
		return buy(user_id[reply])
	if bs == 'discard':
		return discard(user_id[reply])
	if bs == 'show_all':
		return show_all()
	if bs == 'list_categories':
		return list_categories()
	if bs == 'show_category':
		if len(w) < 2:
			return "Miss name"
		return show_category(w[1])
	if bs == 'statistics':
		return statistics()
	if bs == 'show_cart':
		return show_cart(user_id[reply])
	if bs == 'help':
		res = "\nAvailable commands:\n"
		res += "add_to_cart name amount\n"
		res += "rem_from_cart name\n"
		res += "buy\n"
		res += "discard\n"
		res += "show_all\n"
		res += "list_categories\n"
		res += "show_category name\n"
		res += "statistics\n"
		res += "show_cart"
		return res
	else:
		return "Invalid command"

def callback(ch, method, properties, body):
	response = parse(body,properties.reply_to)
	ch.basic_publish(exchange='',
					 routing_key=properties.reply_to,
                     body=str(response))

r.flushall()
init_stock()
connection = pika.BlockingConnection(
pika.ConnectionParameters(host='localhost'))
channel = connection.channel()
result = channel.queue_declare(queue='shop',exclusive=True)
channel.basic_consume(queue='shop',on_message_callback=callback)
channel.start_consuming()
