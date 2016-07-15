#!/usr/bin/env python

from datetime import datetime
import os

from flask import abort, Flask, jsonify, request
from flask_sqlalchemy import SQLAlchemy

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:////tmp/lcrc.db'
db = SQLAlchemy(app)


class Node(db.Model):
    __tablename__ = 'nodes'
    hostname = db.Column(db.String(255), primary_key=True)
    openstack_state = db.Column(db.String(64))
    torque_state = db.Column(db.String(64))

    def __init__(self, hostname, openstack_state='unavailable',
                 torque_state='free'):
        self.hostname = hostname
        self.openstack_state = openstack_state
        self.torque_state = torque_state

    def __repr__(self):
        return '<Node %r>' % self.hostname

    def to_dict(self):
        return {'hostname': self.hostname,
                'openstack_state': self.openstack_state,
                'torque_state': self.torque_state}

ON_DEMAND_RESERVE_SIZE = 1

jobs = {
}


@app.route('/nodes/reset/<node>', methods=['POST'])
def reset_node(node):
    if not request.json:
        abort(400)
    data = request.get_json()
    node = Node.query.filter_by(hostname=node).first()
    node.openstack_state = data.get('openstack_state', 'unavailable')
    node.torque_state = data.get('torque_state', 'free')
    db.session.commit()
    return jsonify(node.to_dict())


def enable_host(**kwargs):
    host = kwargs.get("host")
    if host is not None:
        print "Enabling host %s" % host
        cmd = "sudo pbsnodes -c %s" % host
        os.system(cmd)
    node = Node.query.filter_by(hostname=host).first()
    node.openstack_state = 'unavailable'
    node.torque_state = 'free'
    db.session.commit()


def disable_host(**kwargs):
    host = kwargs.get("host")
    if host is not None:
        print "Disabling host %s" % host
        cmd = "sudo pbsnodes -o %s" % host
        os.system(cmd)
    node = Node.query.filter_by(hostname=host).first()
    node.openstack_state = 'available'
    node.torque_state = 'offline'
    db.session.commit()


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
    for node in Node.query.all():
        if len(result) == count:
            break
        if node.torque_state == 'free':
            node.torque_state = 'offline'
            node.openstack_state = 'available'
            result[node.hostname] = node.to_dict()

    # Disable host in Torque
    for node in result:
        kwargs = {'host': node}
        disable_host(**kwargs)
    return jsonify({'nodes': result})


@app.route('/nodes', methods=['GET'])
def get_nodes():
    nodes = {}
    for n in Node.query.all():
        nodes[n.hostname] = n.to_dict()
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
        db_node = Node.query.filter_by(hostname=node).first()
        if db_node.torque_state != 'free':
            abort(403)

    for node in job['node_list']:
        db_node = Node.query.filter_by(hostname=node).first()
        db_node.torque_state = 'job-exclusive'
        db.session.commit()

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

    for node in job['node_list']:
        db_node = Node.query.filter_by(hostname=node).first()
        if db_node.torque_state == 'job-exclusive':
            db_node.torque_state = 'free'

    return jsonify(job)


def _create_job_if_not_exists(job_id):
    if job_id not in jobs:
        jobs[job_id] = {'id': job_id}


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
