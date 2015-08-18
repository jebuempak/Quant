#!/usr/bin/env python

from pika import BlockingConnection, ConnectionParameters
import datetime

class Service( object ):

    def __init__( self, topics, duration = 360, host_ = 'localhost', verbose = True ):

        self._connection = BlockingConnection( ConnectionParameters( host = host_ ) )
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

    s = Service( topics, duration = 10 )
    try:
        s.run()
    except KeyboardInterrupt:
        s.close()

if __name__ == '__main__':

    sys.exit( main() )
