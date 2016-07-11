#!/usr/bin/env python

from datetime import datetime
import json
import os
import time
from threading import Lock
import Queue
import threading

from flask import abort, Flask, jsonify, request

app = Flask(__name__)

JOB_STATUSES = (QUEUED, STARTED, ENDED
            ) = ('QUEUED', 'STARTED', 'ENDED')

ON_DEMAND_RESERVE_SIZE = 1

jobs = {
}

debug_task_id = 1
lock = Lock()
request_queue = Queue.Queue()
nodes = {
    'lcrc-worker-1': {
        'openstack_state': 'unavailable',
        'torque_state': 'free'
    },
    'lcrc-worker-2': {
        'openstack_state': 'unavailable',
        'torque_state': 'free'
    },
    'lcrc-worker-3': {
        'openstack_state': 'unavailable',
        'torque_state': 'free'
    },
    'lcrc-worker-4': {
        'openstack_state': 'unavailable',
        'torque_state': 'free'
    },
    'lcrc-worker-5': {
        'openstack_state': 'unavailable',
        'torque_state': 'free'
    },
    'lcrc-worker-6': {
        'openstack_state': 'unavailable',
        'torque_state': 'free'
    },
    'lcrc-worker-7': {
        'openstack_state': 'unavailable',
        'torque_state': 'free'
    },
    'lcrc-worker-8': {
        'openstack_state': 'unavailable',
        'torque_state': 'free'
    },
    'lcrc-worker-9': {
        'openstack_state': 'unavailable',
        'torque_state': 'free'
    },
    'lcrc-worker-10': {
        'openstack_state': 'unavailable',
        'torque_state': 'free'
    },
    'lcrc-worker-11': {
        'openstack_state': 'unavailable',
        'torque_state': 'free'
    },
    'lcrc-worker-12': {
        'openstack_state': 'unavailable',
        'torque_state': 'free'
    },
    'lcrc-worker-13': {
        'openstack_state': 'unavailable',
        'torque_state': 'free'
    },
    'lcrc-worker-14': {
        'openstack_state': 'unavailable',
        'torque_state': 'free'
    },
    'lcrc-worker-15': {
        'openstack_state': 'unavailable',
        'torque_state': 'free'
    },
    'lcrc-worker-16': {
        'openstack_state': 'unavailable',
        'torque_state': 'free'
    }
}
available_count = len(nodes)

def enable_host(**kwargs):
    host = kwargs.get("host")
    if host is not None:
        print "Enabling host %s" % host
        cmd = "sudo pbsnodes -c %s" % host
        os.system(cmd)
    lock.acquire()
    global available_count
    available_count += 1
    nodes[host]['torque_state'] = 'free'
    nodes[host]['openstack_state'] = 'unavailable'
    print "enable_host available_count =", available_count
    lock.release()

def disable_host(**kwargs):
    host = kwargs.get("host")
    if host is not None:
        print "Disabling host %s" % host
        cmd = "sudo pbsnodes -o %s" % host
        os.system(cmd)
    nodes[host]['torque_state'] = 'offline'
    nodes[host]['openstack_state'] = 'available'

@app.route('/execute', methods=['POST'])
def execute():
    print "request.data = %s" % request.data
    if not request.json:
        abort(400)
    data = request.get_json()
    if "command" in data:
        command = data['command']
    else:
        abort(400)
    if "args" in data:
        args = data['args']
    if command == 'disable_host':
        disable_host(**args)
    elif command == 'enable_host':
        enable_host(**args)
    return jsonify(data)


@app.route('/nodes/request/<count>', methods=['POST'])
def request_nodes(count):
    count = int(count)

    lock.acquire()
    global available_count, debug_task_id
    _debug_task_id = debug_task_id
    debug_task_id += 1
    lock.release()
    new_nodes = {}

    queue_flag = False
    lock.acquire()
    print "request_nodes() available_count =", available_count
    if not request_queue.empty() or available_count == 0:
        print "QUEUE a request", _debug_task_id
        newEvent = threading.Event()
        request_queue.put(newEvent)
        queue_flag = True
    lock.release()

    if queue_flag:
        newEvent.wait(timeout=30)
        lock.acquire()
        if not newEvent.is_set(): # no nodes were released before timeout
            request_queue.get()
            print _debug_task_id, "W TIMEOUT available_count =", available_count
            lock.release()
            # Race condition: new node might suddenly become available
            # after lock is being released. This is not a problem because
            # in this condition, on the NOVA scheduler side, conflicts
            # will be solved eventually when multiple instances try to
            # launch on the same node: only the first request will be
            # successful, all the others will fail due to no enough resource
            # left.
            return jsonify({'nodes': nodes})
        else:
            lock.release()

    lock.acquire()
    print _debug_task_id, "SUCCESS available_count = {}".format(available_count)
    if available_count > 0:
        for node in nodes:
            if len(new_nodes) == count:
                break
            if nodes[node]['torque_state'] == 'free':
                nodes[node]['torque_state'] = 'offline'
                nodes[node]['openstack_state'] = 'available'
                new_nodes[node] = nodes[node]
        available_count -= len(new_nodes)
        print _debug_task_id, "LEFT available_count = {}".format(available_count)
    lock.release()

    # Disable host in Torque
    for node in new_nodes:
        kwargs = {'host': node}
        disable_host(**kwargs)
    if len(new_nodes):
        print _debug_task_id, "FOUND node"
    else:
        print _debug_task_id, "REJECT"
    return jsonify({'nodes': nodes})

@app.route('/nodes', methods=['GET'])
def get_nodes():
    return jsonify({'nodes': nodes})

@app.route('/jobs', methods=['GET'])
def get_jobs():
    return jsonify({'jobs': jobs})

def get_short_hostname(hostname):
    try:
        i = hostname.index(".")
        return hostname[:i]
    except ValueError:
        return hostname

@app.route('/jobs/prologue', methods=['POST'])
def prologue():
    if not request.json:
        abort(400)
    job = request.get_json()
    job_id = job['job_id']
    job['hostname'] = get_short_hostname(job['hostname'])
    jobs[job_id] = job
    print(job)

    # If any of the nodes Torque tries to run on is not free,
    # it must mean that OpenStack got to them in the meantime.
    # Return a 403 which will requeue the job
    for node in job['node_list']:
        if nodes[node]['torque_state'] != 'free':
            print "ABORT 403"
            abort(403)

    lock.acquire()
    taken_node = 0
    for node in job['node_list']:
        nodes[node]['torque_state'] = 'job-exclusive'
        taken_node += 1
    global available_count
    available_count -= taken_node
    print "prologue took", taken_node, "available_count =", available_count
    lock.release()

    #print jobs[job_id]
    #print nodes
    return jsonify(job)

@app.route('/jobs/epilogue', methods=['POST'])
def epilogue():
    if not request.json:
        abort(400)
    job = request.get_json()
    job['hostname'] = get_short_hostname(job['hostname'])
    job_id = job['job_id']
    print(job)

    existing_job = jobs.get(job_id, None)
    if existing_job is None:
        jobs[job_id] = job
    else:
        existing_job.update(job)
        jobs[job_id] = existing_job

    global available_count
    released_node = 0
    for node in job['node_list']:
      if nodes[node]['torque_state'] == 'job-exclusive':
          nodes[node]['torque_state'] = 'free'
          released_node += 1
    print "RELEASE", released_node

    lock.acquire()
    available_count += released_node
    print "epilogue available_count =", available_count
    while released_node > 0:
        if not request_queue.empty():
            request_queue.get().set()
            released_node -= 1
        else:
            break
    lock.release()
    #print jobs[job_id]
    #print nodes
    return jsonify(job)

def _create_job_if_not_exists(job_id):
    if job_id not in jobs:
        jobs[job_id] = { 'id': job_id }

@app.route('/jobs/<job_id>/queue', methods=['POST'])
def queue_job(job_id):
    # store a queue event for this job
    _create_job_if_not_exists(job_id)
    jobs[job_id]['status'] = 'QUEUED'
    jobs[job_id]['queued_time'] = datetime.utcnow()
    return jsonify(jobs[job_id])

@app.route('/jobs/<job_id>/start', methods=['POST'])
def start_job(job_id):
    # store a start event for this job
    _create_job_if_not_exists(job_id)
    jobs[job_id]['status'] = 'STARTED'
    jobs[job_id]['start_time'] = datetime.utcnow()
    return jsonify(jobs[job_id])

@app.route('/jobs/<job_id>/end', methods=['POST'])
def end_job(job_id):
    # store a end event for this job
    _create_job_if_not_exists(job_id)
    jobs[job_id]['status'] = 'ENDED'
    jobs[job_id]['end_time'] = datetime.utcnow()
    return jsonify(jobs[job_id])

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=1234, debug=True, threaded=True)
