import falcon
import json
from falconjsonio.schema import request_schema
import falconjsonio.middleware
from kafka import KafkaProducer
from cassandra.cluster import Cluster
from cassandra import ConsistencyLevel

cluster = Cluster(['cassandra'], port=9042)

post_request_schema = {
    'type':       'object',
    'properties': {
        'product':  {'type': 'integer'},
        'product_generic_category':  {'type': 'integer'},
        'product_specific_category':  {'type': 'integer'},
        'site_name':   {'type': 'integer'},
        'user_location_country':    {'type': 'integer'},
        'user_location_region':    {'type': 'integer'},
        'user_location_city':    {'type': 'integer'},
        'user_id':  {'type': 'integer'},
        'date_time':    {'type': 'string'},
        'is_mobile':    {'type': 'integer'},
        'channel':    {'type': 'integer'},
        'is_purchase':    {'type': 'integer'}
    },
    'required': [
	'product', 
	'product_generic_category', 
	'product_specific_category', 
	'site_name', 
	'user_location_country', 
	'user_location_region', 
	'user_location_city',
	'user_id', 
	'date_time', 
	'is_mobile', 
	'channel',
        'is_purchase'
    ],
}

class BdrResource:
    def on_get(self, req, resp):
	session = cluster.connect('bdr')

	input = falcon.uri.parse_query_string(req.query_string)
	print('input = {}'.format(input))
        if 'specific_category' in input:
          specific = int(input['specific_category'])
          specific_lookup = session.prepare("SELECT top_products FROM top_specific WHERE specific_cat=?")
          specific_lookup.consistency_level = ConsistencyLevel.ONE
          specific_rows = session.execute(specific_lookup, [specific])
	  rows = session.execute(specific_lookup, [specific])
          resp.status = falcon.HTTP_200
          result = "[]"
          for row in rows:
            result = row.top_products
            break
          resp.body = json.dumps({"specific_category":specific, "recommendedProducts":result}, encoding='utf-8')
        elif 'generic_category' in input:
          generic = int(input['generic_category'])
          generic_lookup = session.prepare("SELECT top_products FROM top_generic WHERE generic_cat=?")
          generic_lookup.consistency_level = ConsistencyLevel.ONE
          generic_rows = session.execute(generic_lookup, [generic])
	  rows = session.execute(generic_lookup, [generic])
          resp.status = falcon.HTTP_200
          result = "[]"
          for row in rows:
            result = row.top_products
            break
          resp.body = json.dumps({"generic_category":generic, "recommendedProducts":result}, encoding='utf-8')
        elif 'product' in input:
	  product = int(input['product'])
	  product_lookup = session.prepare("SELECT other_products FROM top_other_products WHERE product=?")
	  product_lookup.consistency_level = ConsistencyLevel.ONE
	  rows = session.execute(product_lookup, [product])
	  #TODO: deal appropriately if the product doesn't exist in cassandra
	  resp.status = falcon.HTTP_200
          result = "[]"
          for row in rows:
            result = row.other_products
            break
          resp.body = json.dumps({"product":product, "recommendedProducts":result}, encoding='utf-8')
        elif 'user' in input:
          user = int(input['user'])
	  user_lookup = session.prepare("SELECT recommended_products FROM cf WHERE user_id=?")
	  user_lookup.consistency_level = ConsistencyLevel.ONE
          rows = session.execute(user_lookup, [user])
          resp.status = falcon.HTTP_200
          result = "[]"
          for row in rows:
            result = row.recommended_products
            break
          resp.body = json.dumps({"user":user, "recommendedProducts":result}, encoding='utf-8')
	else:
          resp.status = falcon.HTTP_400
          resp.body = '{"error": "Parameter *product*,  *user*, *specific_category* or *generic_category* must be provided in the query string"}'

    @request_schema(post_request_schema)
    def on_post(self, req, resp):
	msg = json.dumps(req.context['doc'], encoding='utf-8')
        producer = KafkaProducer(bootstrap_servers='kafka:9092')
        producer.send('bdr', msg)
        producer.flush()
        resp.status = falcon.HTTP_201
        resp.body = msg

class HealthCheckResource:
    def on_get(self, req, resp):
        resp.status = falcon.HTTP_200
        resp.body = json.dumps({"healthcheck":"ok"}, encoding='utf-8')

app = falcon.API(
    middleware=[
        falconjsonio.middleware.RequireJSON(),
        falconjsonio.middleware.JSONTranslator(),
    ],
)

bdr = BdrResource()
healthcheck = HealthCheckResource()

app.add_route('/bdr', bdr)
app.add_route('/healthcheck', healthcheck)
