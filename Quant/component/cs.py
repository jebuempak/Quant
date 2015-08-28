#!/usr/bin/env python

from pika import BlockingConnection, ConnectionParameters, PlainCredentials
import datetime

import ConfigParser
Config = ConfigParser.ConfigParser()
quant_ini = '/home/osci/source/Quant/etc/quant.ini'
Config.read(quant_ini)
hostname = Config.get('Rabbit-1', 'hostname')
port = int(Config.get('Rabbit-1', 'port'))
user = Config.get('Rabbit-1', 'user')
password = Config.get('Rabbit-1', 'pass')

class Service( object ):

    def __init__( self, topics, duration = 360, host = 'localhost', verbose = True, port = 5672, user = '', password = '' ):
        credentials = PlainCredentials(user, password)
        self._connection = BlockingConnection( ConnectionParameters( host,  port, '/', credentials ) )
        self._channel = self._connection.channel()
        self._channel.exchange_declare( exchange = 'topic_logs', type = 'topic' )
        self._queueID = self._channel.queue_declare( exclusive = True ).method.queue

        for topic in topics:
            self._channel.queue_bind( 
                exchange = 'topic_logs', queue = self._queueID, routing_key = topic
            )

        self._duration = duration
        self._verbose = verbose

    def _handle( self, d ):

        v = d.split( ',' )
        code = v[ 0 ]
        src = tuple( [ float( t ) for t in v[ 3: ] ] )
        print code, v[ 2 ], max( src ), min( src )

    def close( self ):

        self._channel.stop_consuming()
        print 'done', datetime.datetime.now()

        self._connection.close()

    def run( self ):

        _callback = lambda c, m, p, d: self._handle( d )
        self._channel.basic_consume( _callback, queue = self._queueID, no_ack = True )
        self._channel.start_consuming()


import sys
from optparse import OptionParser as parser

def main():

    f = parser()
    f.add_option( '-g', '--groups', help = 'group specifiers', dest = 'groups', default = '#' )

    options, args = f.parse_args()

    groups = options.groups.split( ',' )
    topics = [ 'SS.' + g for g in groups ]
    print topics

    s = Service( topics, duration = 10, host=hostname, port=port, user=user, password=password )
    try:
        s.run()
    except KeyboardInterrupt:
        s.close()

if __name__ == '__main__':

    sys.exit( main() )
