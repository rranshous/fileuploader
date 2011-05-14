#!/usr/bin/python



# we are going to take http(s) connection
# on root, they should be file uploads.
# the path that is attempted during the POST
# should be the file's name. All other info
# is in headers. the body of hte post will be
# the chunk data

import re, os, sys
import socket
from tornado import ioloop, iostream
import tornado.options
from tornado.options import define, options
from copy import copy

import logging
logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger(__name__)

CHUNK_SIZE = 5 * 1024

TEMP_FILE_DIR = './data_cache'

def get_temp_file_path(file_name,file_hash):
    """
    returns back the location on disk of the
    temp file we are accumulating chunks into
    """
    
    # TODO: better
    return os.path.join(TEMP_FILE_DIR,
                        file_name)

class Server():
    def __init__(self):
        self.stream = None

    def get_data_reader(self,headers):
        """
        returns a reader function to be used
        as a callback
        """

        # how much data are we going to receive?
        content_len = int(headers.get('content-length'))

        # pull the hash out of the headers
        file_hash = headers.get('content-md5')

        # figure out what they want to name the file
        file_name = headers.get('PATH','').strip()
        
        # the file name comes w/ the host name, strip it
        file_name = '/'.join(file_name.split('/')[1:])

        # where in the overall file are we?
        offset = int(headers.get('content-offset'))
        cursor = int(headers.get('content-offset'))

        # we may have already started to construct
        # the file, maybe not. figure out where
        temp_file_path = get_temp_file_path(file_name,file_hash)

        log.debug('temp_file_path: %s' % temp_file_path)

        # create our callback function for when we receive
        # data off the wire
        def data_reader(data):
            # woot, we got a piece of a piece of a file!

            log.debug('data reader got data: %s' % len(data))

            try:
                log.debug('writing data: %s' % temp_file_path)

                # make sure the temp file exists
                if not os.path.exists(temp_file_path):
                    # create a file
                    with open(temp_file_path,'w') as fh:
                        pass

                # open our file, and write our piece
                with open(temp_file_path,'r+b') as fh:
                    log.debug('offset: %s' % offset)
                    
                    # find our spot in the file
                    fh.seek(cursor)

                    # dump our shit
                    fh.write(data)

                # we receive the data sequentially!
                cursor += len(data)

            except:
                # TODO: write back error
                raise
            

            log.debug('offset: %s' % offset)

            # if we've received all the data we want to check
            # the md5 of what we received
            if offset == content_len:

                log.debug('got all data! checking chunk')

                # get our full chunks data
                with open(temp_file_path,'r+b') as fh:
                    
                    # seek into the temp file to where we
                    # started writing the chunk
                    fh.seek(offset)

                    # read in the complete chunk
                    data = fh.read(content_len)

                    # get it's hash
                    _hash = get_chunk_hash(data)

                    log.debug('file_hash: %s' % file_hash)
                    log.debug('our_hash: %s' % _hash)

                    # is it the same as the hash we thought we'd get
                    if file_hash != _hash:

                        # woops! file must have currupt en route
                        # TODO: write an error msg back
                        log.debug('CURRUPT!')
                
        return data_reader

    def get_handle_headers(self,stream):

        def handle_headers(data):
            """
            we've received some headers, get
            ready to get some file data
            """

            log.debug('handling headers: %s' % data)
             
            # store thea headers in a dict
            headers = {}

            # split the header by newline
            for line in data.split('\r\n'):

                # the k/v part are split by colons
                parts = line.split(':')

                # check if it's k/v
                if len(parts) == 2:
                    # store the k/v
                    headers[parts[0].strip().lower()] = parts[1].strip()

                # check if it's the first line, including the
                # name of the file
                elif 'post' in line.lower():

                    # pull the path from the line
                    path = line.split()[1]

                    # throw it in the headers
                    headers['PATH'] = path

            # read a chunk off the stream, sending it to
            # our reader callback

            log.debug('got headers: %s' % headers)
            stream.read_bytes(CHUNK_SIZE,
                              self.get_data_reader(headers))

        return handle_headers
    
    def handle_accept(self, fd, events):
        log.debug('accepting')

        conn, addr = self._sock.accept()
        stream = iostream.IOStream(conn)
        stream.read_until('\r\n\r\n',
                          self.get_handle_headers(stream))

    def start(self, host, port):
        # let those listening know we are about to begin

        log.debug('plugin server starting: %s %s'
                  % (host,port))

        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
        self._sock.setblocking(0)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.bind((host,port))
        self._sock.listen(128)
        ioloop.IOLoop.instance().add_handler(self._sock.fileno(),
                                             self.handle_accept,
                                             ioloop.IOLoop.READ)

        self.host = host
        self.port = port



define('host', default="0.0.0.0", help="The binded ip host")
define('port', default=8005, type=int, help='The port to be listened')

if __name__ == '__main__':
    
    log.debug('parsing command line')
    tornado.options.parse_command_line()

    log.debug('creating server')
    server = Server()

    log.debug('starting server')
    server.start(options.host, options.port)

    ioloop.IOLoop.instance().start()
