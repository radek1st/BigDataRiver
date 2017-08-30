#!/usr/bin/env python

import requests

url = 'http://localhost:8000/bdr'
lines = open('test-small.json').read().split("\n")
for data in lines:
  response = requests.post(url, data=data, headers={"Content-Type": "application/json"})
  print(response)
