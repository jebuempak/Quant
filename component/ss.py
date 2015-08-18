#!/usr/bin/env python

from pika import BlockingConnection, ConnectionParameters
import datetime

import ConfigParser
Config = ConfigParser.ConfigParser()
quant_ini = '/home/osci/source/Quant/etc/quant.ini'
Config.read(quant_ini)
hostname = Config.get('Rabbit-1', 'hostname')

class Service( object ):

    def __init__( self, name, topics, duration = 360, dimension = 20, host_ = 'localhost', verbose = True ):

        self._connection = BlockingConnection( ConnectionParameters( host = host_ ) )
        self._channel = self._connection.channel()
        self._channel.exchange_declare( exchange = 'topic_logs', type = 'topic' )
        self._queueID = self._channel.queue_declare( exclusive = True ).method.queue

        for topic in topics:
            self._channel.queue_bind( 
                exchange = 'topic_logs', queue = self._queueID, routing_key = topic
            )

        self._name = name
        self._duration = duration
        self._dimension = dimension
        self._deposit = dict()
        self._verbose = verbose

    def _mockupInitialState( self, code ):

        return [ 100. ] * self._dimension

    def _mockupUpdateState( self, state, x ):

        p = tuple( [ 1. / max( 1 + t * t, 1.05 ) for t in range( 0, self._dimension ) ] )
        z = []
        for i in range( 0, self._dimension ):
            z.append( state[ i ] * ( 1 - p[ i ] ) + x * p[ i ] )
        return tuple( z )

    def _handle( self, d ):

        v = d.split( ',' )
        code = v[ 0 ]
        if not self._deposit.has_key( code ):
            self._deposit[ code ] = []
            prev = self._mockupInitialState( code )
        else:
            prev = self._deposit[ code ][ -1 ][ -1 ]
        src = tuple( [ float( t ) for t in v[ 3: ] ] )
        ups = self._mockupUpdateState( prev, sum( src ) / len( src ) )
        toDeposit = v[ 1 ], int( v[ 2 ] ), src, ups
        self._deposit[ code ].append( toDeposit )

        timeStamp = str( datetime.datetime.now().time() )
        message = ','.join( [ code, timeStamp, v[ 2 ] ] + [ str( t ) for t in ups ] )
        routingKey = '.'.join( [ 'SS', self._name ] )
        self._channel.basic_publish( exchange = 'topic_logs', routing_key = routingKey, body = message )
        if self._verbose:
            print message

    def close( self ):

        self._channel.stop_consuming()
        print 'done', datetime.datetime.now()
        for key, val in self._deposit.iteritems():
            print key, len( val )

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
    f.add_option( '-n', '--name', help = 'name', dest = 'name' )

    options, args = f.parse_args()

    groups = options.groups.split( ',' )
    topics = [ 'DS.' + g for g in groups ]
    print topics

    s = Service( options.name, topics, duration = 10, host=hostname )
    try:
        s.run()
    except KeyboardInterrupt:
        s.close()

if __name__ == '__main__':

    sys.exit( main() )
