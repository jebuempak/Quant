#!/usr/bin/env python

from pika import BlockingConnection, ConnectionParameters, PlainCredentials
import datetime 
import time, random, math

import ConfigParser
Config = ConfigParser.ConfigParser()
quant_ini = '/home/osci/source/Quant/etc/quant.ini'
Config.read(quant_ini)
hostname = Config.get('Rabbit-1', 'hostname')
port = Config.get('Rabbit-1', 'port')
user = Config.get('Rabbit-1', 'user')
password = Config.get('Rabbit-1', 'pass')

class Service( object ):

    def __init__( self, partition, interval = 5, duration = 360, seed = 0, host = 'localhost', verbose = True, port = 5672, user = '', password = '' ):
        credentials = PlainCredentials(user, password)
        self._connection = BlockingConnection( ConnectionParameters( host,  port, '/', credentials ) )
        self._channel = self._connection.channel()
        self._channel.exchange_declare( exchange = 'topic_logs', type = 'topic' )
        self._partition = partition
        self._duration = duration
        self._interval = interval
        self._verbose = verbose
        
        random.seed( seed )

    def _generate( self ):

        return tuple( [ int( 10000 * math.exp( random.gauss( 0, 1 ) ) ) * 0.01 for i in range( 0, 5 ) ] )

    def close( self ):

        self._connection.close()

    def run( self ):

        for serial in range( 0, self._duration ):
            current = str( serial )
            for group, codes in self._partition.iteritems():
                routingKey = '.'.join( [ 'DS', group ] )
                for code in codes:
                    timeStamp = str( datetime.datetime.now().time() )
                    message = ','.join( [ code, timeStamp, current ] + [ str( t ) for t in self._generate() ] )
                    self._channel.basic_publish( exchange = 'topic_logs', routing_key = routingKey, body = message )
                    if self._verbose:
                        print message
            time.sleep( self._interval )

        print 'done', datetime.datetime.now()
        self.close()

import sys

def main():

    _formatCode = lambda x: '0' * max( 0, ( 6 - len( str( x ) ) ) ) + str( x )
    partition = dict()
    partition[ 'XKRX-A' ] = tuple( [ 'XKRX-CS-KR-' + _formatCode( i ) for i in range( 100, 105 ) ] )
    partition[ 'XKRX-B' ] = tuple( [ 'XKRX-CS-KR-' + _formatCode( i ) for i in range( 250, 258 ) ] )
    s = Service( partition, duration = 10, host=hostname, port=port, user=user, password=password )

    try:
        s.run()
    except KeyboardInterrupt:
        s.close()

if __name__ == '__main__':

    sys.exit( main() )
