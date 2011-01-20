import os
import os.path
import sys
import time
import boto
from fabric.api import *

ami = 'ami-9c9f6ef5'
key_name = 'progrium'

conn = boto.connect_ec2()

#if os.path.exists('hosts'):
#    f = open('hosts', 'r')
#    env.roledefs = f.read()

def start():
    reservation = conn.get_image(ami).run(key_name=key_name)
    instance = reservation.instances[0]
    while instance.update() == 'pending':
        sys.stdout.write('.')
        sys.stdout.flush()
        time.sleep(1)
    env.hosts = ['root@%s' % instance.public_dns_name]
    time.sleep(60)

def touch():
    run("touch testfile")
