#!/usr/bin/env python

from datetime import datetime
import json
import os

from flask import abort, Flask, jsonify, request

app = Flask(__name__)

JOB_STATUSES = (QUEUED, STARTED, ENDED
            ) = ('QUEUED', 'STARTED', 'ENDED')

ON_DEMAND_RESERVE_SIZE = 1

jobs = {
}

nodes = {
    'lcrc-worker-1': {
        'openstack_state': 'unavailable',
        'torque_state': 'free'
    },
    'lcrc-worker-2': {
        'openstack_state': 'unavailable',
        'torque_state': 'free'
    }
}

def enable_host(**kwargs):
    host = kwargs.get("host")
    if host is not None:
        print "Enabling host %s" % host
        cmd = "sudo pbsnodes -c %s" % host
        os.system(cmd)
    nodes[host]['torque_state'] = 'free'
    nodes[host]['openstack_state'] = 'unavailable'

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
    result = {}
    for node in nodes:
        if len(result) == count:
            break
        if nodes[node]['torque_state'] == 'free':
            nodes[node]['torque_state'] == 'offline'
            nodes[node]['openstack_state'] == 'available'
            result[node] = nodes[node]

    # Disable host in Torque
    for node in result:
        kwargs = {'host': node}
        disable_host(**kwargs)
    return jsonify({'nodes': result})

@app.route('/nodes', methods=['GET'])
def get_nodes():
    return jsonify({'nodes': nodes})

@app.route('/jobs', methods=['GET'])
def get_jobs():
    return jsonify({'jobs': jobs})

@app.route('/jobs/prologue', methods=['POST'])
def prologue():
    if not request.json:
        abort(400)
    job = request.get_json()
    job_id = job['job_id']
    jobs[job_id] = job

    nodes[job['hostname']]['torque_state'] = 'job-exclusive'

    #print jobs[job_id]
    #print nodes
    return jsonify(job)

@app.route('/jobs/epilogue', methods=['POST'])
def epilogue():
    if not request.json:
        abort(400)
    job = request.get_json()
    job_id = job['job_id']

    existing_job = jobs.get(job_id, None)
    if existing_job is None:
        jobs[job_id] = job
    else:
        existing_job.update(job)
        jobs[job_id] = existing_job

    nodes[job['hostname']]['torque_state'] = 'free'

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
    app.run(host='0.0.0.0', port=1234, debug=True)
