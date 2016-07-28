import argparse

import requests

COMMANDS = ['node_list', 'reset_node']


def node_list(opts):
    url = 'http://%s:%s/nodes' % (opts.host, opts.port)
    r = requests.get(url)
    print r.text
    return r.json()


def reset_node(opts):
    data = {'command': 'reset'}
    headers = {}
    headers['content-type'] = 'application/json'
    headers['Accept'] = 'application/json'

    if opts.node is None:
        raise Exception("node must be set")

    url = 'http://%s:%s/nodes/reset/%s' % (opts.host, opts.port, opts.node)
    if opts.openstack_state:
        data['openstack_state'] = opts.openstack_state
    if opts.torque_state:
        data['torque_state'] = opts.torque_state
    r = requests.post(url, json=data, headers=headers)
    print r.text
    return r.json()


def main():
    parser = argparse.ArgumentParser(description='Client to control the LCRC balancer')

    parser.add_argument('--balancer', '-b', action='store', dest='host', default='127.0.0.1')
    parser.add_argument('--node', '-n', action='store', dest='node')
    parser.add_argument('--openstack_state', '-o', action='store', dest='openstack_state')
    parser.add_argument('--port', '-p', action='store', dest='port', default='1234')
    parser.add_argument('--torque_state', '-t', action='store', dest='torque_state')
    parser.add_argument('command', metavar='command', help='Command to run')
    opts = parser.parse_args()

    if opts.command not in COMMANDS:
        raise ValueError('Command %s is not supported' % (opts.command))

    if opts.command == 'node_list':
        node_list(opts)
    elif opts.command == 'reset_node':
        reset_node(opts)
