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
from functools import partial
from hashlib import md5

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
                        file_name,
                        file_hash)

def get_chunk_hash(chunk):
    """ returns the MD5 for the data """
    m = md5()
    m.update(chunk)
    return m.hexdigest()


class Handler():
    def __init__(self,stream):
        self.stream = stream

        # request headers
        self.headers = {}

    def __call__(self,data):
        """
        handle data coming off the socket
        """

        # if we haven't gotten the headers yet,
        # than we should be receiving those now
        if not self.headers:
            self.headers_reader(data)

        else:
            # if we already have the headers this
            # is file data
            self.data_reader(data)

    def headers_reader(self,data):
        log.debug('handling headers: %s' % data)
         
        # split the header by newline
        for line in data.split('\r\n'):

            # the k/v part are split by colons
            parts = line.split(':')

            # check if it's k/v
            if len(parts) == 2:
                # store the k/v
                self.headers[parts[0].strip().lower()] = parts[1].strip()

            # check if it's the first line, including the
            # name of the file
            elif 'post' in line.lower():

                # pull the path from the line
                path = line.split()[1]

                # throw it in the headers
                self.headers['PATH'] = path

        # read a chunk off the stream, sending it to
        # our reader callback

        log.debug('got headers: %s' % self.headers)

        # how much data are we going to receive?
        self.content_len = int(self.headers.get('content-length'))

        # pull the hash out of the headers
        self.file_hash = self.headers.get('content-md5')

        # figure out what they want to name the file
        file_name = self.headers.get('PATH','').strip()
        
        # the file name comes w/ the host name, strip it
        self.file_name = '/'.join(file_name.split('/')[1:])

        # where in the overall file are we?
        self.offset = int(self.headers.get('content-offset'))
        self.cursor = int(self.headers.get('content-offset'))

        # we may have already started to construct
        # the file, maybe not. figure out where
        self.temp_file_path = get_temp_file_path(self.file_name,
                                                 self.file_hash)

        log.debug('temp_file_path: %s' % self.temp_file_path)

        # read the next sub-chunk off the line
        self.stream.read_bytes(CHUNK_SIZE, self.data_reader)
        


    def data_reader(self,data):
        """
        takes in a subchunk of data
        """
        # woot, we got a piece of a piece of a file!

        log.debug('data reader got data: %s' % len(data))

        try:
            log.debug('writing data: %s' % self.temp_file_path)

            # make sure the temp file exists
            if not os.path.exists(self.temp_file_path):

                # what is the dir the file will live in?
                file_dir = os.path.dirname(self.temp_file_path)

                # create the dirs leading to it if it isn't there
                if not os.path.exists(file_dir):
                    os.makedirs(file_dir)

                # create the file
                with open(self.temp_file_path,'w') as fh:
                    pass

            # open our file, and write our piece
            with open(self.temp_file_path,'r+b') as fh:
                log.debug('cursor: %s' % self.cursor)
                
                # find our spot in the file
                fh.seek(self.cursor)

                log.debug('writing data')

                # dump our shit
                fh.write(data)

            # we receive the data sequentially!
            self.cursor += len(data)

            log.debug('new cursor')

        except:
            # TODO: write back error
            raise
        
        # if we've received all the data we want to check
        # the md5 of what we received
        if self.cursor == self.content_len:

            log.debug('got all data! checking chunk')
            log.debug('reading file data')

            # get our full chunks data
            with open(self.temp_file_path,'r+b') as fh:
                
                # seek into the temp file to where we
                # started writing the chunk
                fh.seek(self.offset)

                # read in the complete chunk
                data = fh.read(self.content_len)

            # get it's hash
            _hash = get_chunk_hash(data)

            log.debug('file_hash: %s' % self.file_hash)
            log.debug('our_hash: %s' % _hash)

            # is it the same as the hash we thought we'd get
            if self.file_hash != _hash:

                # woops! file must have currupt en route
                log.debug('CURRUPT!')

                self.stream.write('HTTP/1.1 400 BadDigest\r\n')
            
            else:
                log.debug('200 OK')

                # tell them we're done
                self.stream.write('HTTP/1.1 200 OK\r\n'+ \
                                  'Content-Length: 0\r\n' + \
                                  'Connection: close\r\n\r\n')

            log.debug('done!')

            # kill the connection
            #self.stream.close()

        else:
            log.debug('not end of upload')

            # guess we're not done
            # read the next sub-chunk off the line
            self.stream.read_bytes(min(CHUNK_SIZE,
                                       self.content_len - self.cursor),
                                   self)

class Server():
    
    def handle_accept(self, fd, events):
        log.debug('accepting')

        conn, addr = self._sock.accept()
        stream = iostream.IOStream(conn)
        handler = Handler(stream)
        stream.read_until('\r\n\r\n',handler)

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
