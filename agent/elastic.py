#!/usr/bin/python
import os
import sys
import json
import urllib2
import time
import errno

ttl = 60

stats = {
	'cluster': 'http://{0}:{1}/_cluster/stats',
	'nodes': 'http://{0}:{1}/_nodes/stats',
	'indices': 'http://{0}:{1}/_stats',
	'health': 'http://{0}:{1}/_cluster/health'
}


def created_file(name):
	try:
		fd = os.open(name, os.O_WRONLY | os.O_CREAT | os.O_EXCL)
		os.close(fd)
		return True
	except OSError, e:
		if e.errno == errno.EEXIST:
			return False
		raise


def is_older_then(name, ttl):
	age = time.time() - os.path.getmtime(name)
	return age > ttl

def get_cache(eshost, esport, api):
	cache = '/tmp/elastizabbix-{0}.json'.format(api)
	lock = '/tmp/elastizabbix-{0}.lock'.format(api)
	should_update = (not os.path.exists(cache)) or is_older_then(cache, ttl)
	if should_update and created_file(lock):
		try:
			print stats[api].format(eshost, esport)
			d = urllib2.urlopen(stats[api].format(eshost, esport)).read()
			with open(cache, 'w') as f:
				f.write(d)
		except Exception as e:
			pass
		if os.path.exists(lock):
			os.remove(lock)
	if os.path.exists(lock) and is_older_then(lock, 300):
		os.remove(lock)
	ret_data = {}
	try:
		with open(cache) as data_file:
			ret_data = json.load(data_file)
	except Exception as e:
		ret_data = json.loads(
			urllib2.urlopen(stats[api].format(eshost, esport)).read())
	return ret_data


def get_stat(eshost, esport, api, stat):
	d = get_cache(eshost, esport, api)
	keys = []
	for i in stat.split('.'):
		keys.append(i)
		key = '.'.join(keys)
		if key in d:
			d = d.get(key)
			keys = []
	return d


def discover_nodes(eshost, esport):
	d = {'data': []}
	for k, v in get_stat(eshost, esport, 'nodes', 'nodes').iteritems():
		d['data'].append({'{#NAME}': v['name'], '{#NODE}': k})
	return json.dumps(d)

def discover_indices(eshost, esport):
	d = {'data': []}
	for k, v in get_stat(eshost, esport, 'indices', 'indices').iteritems():
		d['data'].append({'{#NAME}': k})
	return json.dumps(d)


if __name__ == '__main__':
	api = sys.argv[1]
	stat = sys.argv[2]
	eshost = sys.argv[3] if len(sys.argv) > 3 else "localhost"
	esport = sys.argv[4] if len(sys.argv) > 4 else "9200"

	if api == 'discover':
		if stat == 'nodes':
			print discover_nodes(eshost, esport)
		if stat == 'indices':
			print discover_indices(eshost, esport)

	else:
		stat = get_stat(eshost, esport, api, stat)
		if isinstance(stat, dict):
			print ''
		else:
			print stat

