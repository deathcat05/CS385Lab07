from __future__ import print_function
import time
from kazoo.client import KazooClient
from kazoo.retry import KazooRetry

LOCK = '/dbupdate'


def get_zookeeper():
    """
    Waits and returns a connection to a Zookeeper running in localhost:2181
    """

    hosts = ['localhost']

    # wait forever or until a connection is successful
    while True:
        client = KazooClient(hosts=hosts, connection_retry=KazooRetry(max_tries=3, max_delay=3))
        try:
            client.start()
        except Exception as exc:
            # we should probably handle different types of exceptions with different sleep times.
            print(exc)
            raise
        return client


def update_database():
    """
    Waits until it acquires a lock in Zookeeper and then proceeds to update the database file
    """
    lock = zk.Lock(LOCK)

    try:
        lock.acquire()
        with open('database.txt', 'r+') as database:
            content = database.read()
            if content: 
                value = int(content)
            else:
                value = 0
            value += 1
            database.seek(0)
            database.write(str(value))
    finally:
        lock.release()


if __name__ == '__main__':

    # there is actually a potential race condition in this snippet that creates the file.
    # We avoid it by initializing the file in the calling bash script.
    try:
        open('database.txt', 'r').close()
    except Exception as exc:
        if exc.errno == errno.ENOENT:
            open('database.txt', 'w').close()

    try:
        zk = get_zookeeper()
        for i in range (1000):
            update_database()
    finally:
        zk.stop()
