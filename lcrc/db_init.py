from balancer import db
from balancer import Node


def main():
    db.create_all()

    lcrc_worker_1 = Node('lcrc-worker-1')
    lcrc_worker_2 = Node('lcrc-worker-2')

    db.session.add(lcrc_worker_1)
    db.session.add(lcrc_worker_2)
    db.session.commit()
