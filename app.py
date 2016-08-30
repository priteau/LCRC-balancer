#!/usr/bin/env python

from datetime import datetime
import json
import os
import time
from threading import Lock
import Queue
import threading
import sys

from flask import abort, Flask, jsonify, request

def disable_host(host):
    try:
        print "Disabling host %s" % host
        cmd = "sudo pbsnodes -o %s" % host
        os.system(cmd)
    except Exception as e:
        print e

if len(sys.argv) < 4:
    sys.exit("Usage: {} <number of nodes> <W> <R>")

app = Flask(__name__)

JOB_STATUSES = (QUEUED, STARTED, ENDED
            ) = ('QUEUED', 'STARTED', 'ENDED')

ON_DEMAND_RESERVE_SIZE = 1

jobs = {
}

C = int(sys.argv[1])
W = int(sys.argv[2])
R = int(sys.argv[3])
request_id = 1
lock = Lock()
request_queue = Queue.Queue()
nodes = {}
for i in range(1, C-R+1):
    nodes['lcrc-worker-{}'.format(i)] = {
        'openstack_state': 'unavailable',
        'torque_state': 'free' }
# available_count indicates how many nodes are 'free' and can be 'appropriated' from batch scheduler
available_count = C - R

# reserve high numbered nodes
reserved_nodes = []
for i in range(C-R+1, C+1):
    node = 'lcrc-worker-{}'.format(i)
    disable_host(node)
    nodes[node] = {
        'openstack_state': 'available',
        'torque_state': 'offline' }
    reserved_nodes.append(node)

def is_node_reserved(node):
    return node in reserved_nodes

def current_reserve_size():
    return len(reserved_nodes)

leapfrog_count = 0

def clear_exiting_flag(host):
    global available_count, leapfrog_count
    try:
        if nodes[host]['openstack_state'] == 'exiting':
            lock.acquire()
            if request_queue.empty():
                lock.release()

                # Replenish the reserve if needed
                if current_reserve_size < R:
                    nodes[host]['openstack_state'] = 'available'
                    reserved_nodes.append(node)
                    print "clear_exiting_flag, reserved node {}".format(host)
                    return
                print datetime.utcnow(), "Enabling host %s" % host
                cmd = "sudo pbsnodes -c %s" % host
                os.system(cmd)
                lock.acquire()
                nodes[host]['torque_state'] = 'free'
                nodes[host]['openstack_state'] = 'unavailable'
                available_count += 1
                print "clear_exiting_flag no waiting request, available_count =", available_count
                lock.release()
            else:
                # It is possible to leapfrog a reserved node.
                # The reason for doing this is to wake up a
                # queued request.
                nodes[host]['openstack_state'] = 'leapfrog'
                leapfrog_count += 1
                print "clear_exiting_flag leapfrog", host, "leapfrog_count =", leapfrog_count
                request_queue.get().set()
                lock.release()
    except Exception as e:
        print e

def enable_host(**kwargs):
    # Does NOT return node to batch scheduler instantly
    #
    # 1. give NOVA compute node 10 seconds to update its resource
    #    status to db
    #
    # 2. if next ondemand request is waiting for node in 10 second,
    #    allow it to leapfrog to this node
    #
    # 3. batch jobs cannot use this node within these 10 seconds
    try:
        host = kwargs.get("host")
        print datetime.utcnow(), "enable_host", host
        nodes[host]['openstack_state'] = 'exiting'
        t = threading.Timer(10, clear_exiting_flag, args=[host])
        t.start()
    except Exception as e:
        print e

def unlock_hosts(**kwargs):
    try:
        for host in kwargs.get("hosts"):
            if nodes[host]['openstack_state'] == 'locked':
                print "Unlock", host
                nodes[host]['openstack_state'] = 'available'
            else:
                # Reserved hosts, if they were not being scheduled
                # by calling request_nodes(), they won't be locked.
                # So I did not exclude them from this checking.
                print "ERROR unlock_host nodes[{}]['openstack_state'] = {}".format(
                        host, nodes[host]['openstack_state'])
    except Exception as e:
        print e

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
    if command == 'enable_host':
        enable_host(**args)
    elif command == 'unlock_hosts':
        unlock_hosts(**args)
    return jsonify(data)

@app.route('/nodes/request/<count>', methods=['POST'])
def request_nodes(count):
    # When request_nodes() is invoked, it means all openstack nodes,
    # including reserved nodes are busy.
    global available_count, leapfrog_count, request_id
    count = int(count)
    if count <= 0:
        return jsonify({'nodes': {}})

    new_nodes = {}
    leapfrog_nodes = {}

    lock.acquire()
    _request_id = request_id
    request_id += 1
    print datetime.utcnow(), _request_id, "request_nodes starting available_count =", available_count
    if W > 0 and (not request_queue.empty() or available_count == 0):
        print datetime.utcnow(), "QUEUE request", _request_id
        newEvent = threading.Event()
        request_queue.put(newEvent)
        lock.release()
        newEvent.wait(timeout=W)
        lock.acquire()
        if not newEvent.is_set():
            # W timeout
            try:
                assert request_queue.get() is newEvent
            except Exception as e:
                print "ERROR uNicOrn", e # TODO fix this
            lock.release()
            print datetime.utcnow(), _request_id, "request_nodes W TIMEOUT available_count =", available_count
            return jsonify({'nodes': new_nodes})
        else:
            print datetime.utcnow(), _request_id, \
                    "request_nodes W SUCCESS available_count = {}, leapfrog_count = {}".format(
                    available_count, leapfrog_count)
            if leapfrog_count > 0:
                for node in nodes:
                    if nodes[node]['openstack_state'] == 'leapfrog':
                        nodes[node]['openstack_state'] = 'locked'
                        leapfrog_nodes[node] = nodes[node]
                        if len(leapfrog_nodes) == count:
                            break
                leapfrog_count -= len(leapfrog_nodes)
                print datetime.utcnow(), _request_id, "request_nodes remaining leapfrog_count = ", leapfrog_count

    if available_count > 0 and len(leapfrog_nodes) < count:
        for node in nodes:
            if nodes[node]['torque_state'] == 'free':
                if nodes[node]['openstack_state'] == 'locked':
                    continue
                nodes[node]['torque_state'] = 'offline'
                nodes[node]['openstack_state'] = 'locked'
                new_nodes[node] = nodes[node]
                if len(new_nodes) == count - len(leapfrog_nodes):
                    break
        # TODO what about len(new_nodes) < count
        available_count -= len(new_nodes)
    print datetime.utcnow(), _request_id, "request_nodes remaining available_count = ", available_count
    lock.release()

    # Replenish the reserve if needed
    new_reserved_nodes = []
    if current_reserve_size < R:
        required_nodes_for_reserve = R - current_reserve_size
        for node in nodes:
            if required_nodes_for_reserve > 0:
                if nodes[node]['torque_state'] == 'free':
                    if nodes[node]['openstack_state'] == 'locked':
                        continue
                    nodes[node]['torque_state'] = 'offline'
                    nodes[node]['openstack_state'] = 'locked'
                    new_reserved_nodes.append(node)
                    required_nodes_for_reserve -= 1

    # Disable host in Torque
    for node in new_nodes:
        disable_host(node)
    for node in new_reserved_nodes:
        disable_host(node)
    result = {}
    for node in new_nodes:
        result[node] = new_nodes[node]
    for node in leapfrog_nodes:
        result[node] = leapfrog_nodes[node]
    if len(result) == count:
        print datetime.utcnow(), _request_id, "request_nodes SUCCESS", new_nodes, leapfrog_nodes
    else:
        print datetime.utcnow(), _request_id, "request_nodes REJECT / partially success", new_nodes, leapfrog_nodes
    return jsonify({'nodes': result})

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
    global available_count
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
    available_count -= taken_node
    print "prologue took", taken_node, "available_count =", available_count
    lock.release()

    #print jobs[job_id]
    #print nodes
    return jsonify(job)

@app.route('/jobs/epilogue', methods=['POST'])
def epilogue():
    global available_count
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

    released_node = 0
    lock.acquire()
    for node in job['node_list']:
        if nodes[node]['torque_state'] == 'job-exclusive':
            nodes[node]['torque_state'] = 'free'
            released_node += 1

    # Replenish the reserve if needed
    if current_reserve_size < R:
        for i in range(R - current_reserve_size):
            node = job['node_list'][i]
            nodes[node]['openstack_state'] = 'available'
            nodes[node]['torque_state'] = 'offline'
            reserved_nodes.append(node)
            released_node -= 1
            print "epilogue returned node {} to reserve".format(node)

    available_count += released_node
    print "epilogue released_node =", released_node, "available_count =", available_count
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
