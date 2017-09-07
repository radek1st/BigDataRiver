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
        if 'product-kappa' in input:
	  product = int(input['product-kappa'])
	  product_lookup = session.prepare("SELECT other_products FROM top_other_products_kappa WHERE product=?")
	  product_lookup.consistency_level = ConsistencyLevel.ONE
	  rows = session.execute(product_lookup, [product])
	  resp.status = falcon.HTTP_200
          result = []
          if rows:
            result = rows[0].other_products
          resp.body = json.dumps({"product":product, "recommendedProducts":result}, encoding='utf-8')
        elif 'product-lambda' in input:
	  product = int(input['product-lambda'])
	  stream_lookup = session.prepare("SELECT other_products FROM top_other_products_stream WHERE product=?")
	  stream_lookup.consistency_level = ConsistencyLevel.ONE
	  stream_rows = session.execute(stream_lookup, [product])
	  batch_lookup = session.prepare("SELECT other_products FROM top_other_products_batch WHERE product=?")
	  batch_lookup.consistency_level = ConsistencyLevel.ONE
	  batch_rows = session.execute(batch_lookup, [product])
	  resp.status = falcon.HTTP_200
          result = []
          if batch_rows and stream_rows:
  	    merged = []
            for x in (batch_rows[0].other_products + stream_rows[0].other_products):
              if x not in merged:
                merged.append(x)
            result = merged[:5]
          elif batch_rows:
            result = batch_rows[0].other_products          
          elif stream_rows:
            result = stream_rows[0].other_products          
          resp.body = json.dumps({"product":product, "recommendedProducts":result}, encoding='utf-8')
        elif 'user' in input:
          user = int(input['user'])
	  user_lookup = session.prepare("SELECT recommended_products FROM cf WHERE user_id=?")
	  user_lookup.consistency_level = ConsistencyLevel.ONE
          rows = session.execute(user_lookup, [user])
          resp.status = falcon.HTTP_200
          result = []
          if rows:
            result = rows[0].recommended_products
          resp.body = json.dumps({"user":user, "recommendedProducts":result}, encoding='utf-8')
	else:
          resp.status = falcon.HTTP_400
          resp.body = '{"error": "Parameter *product-lambda*, *product-kappa* or *user* must be provided in the query string"}'

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
