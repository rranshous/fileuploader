#!/usr/bin/python


# the idea is that we want to facilitate
# large file uploads to a server. the client
# is going to send the data in chunks to the server.
# it should be capable of pickup up an interrupted
# upload on next launch, as well as uploading dirs
# instead of files

from urllib2 import Request, HTTPHandler, urlopen
from hashlib import md5
import os.path
from findfiles import find_files_iter as find_files
from threading import Thread
from time import sleep
import json

import logging
logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger(__name__)


CHUNK_SIZE = 5242880 # 5MB

def chunk_file(file_path):
    """ generator: yields up chunks of a bin file """

    global CHUNK_SIZE

    log.debug('chunking file: %s' % file_path)

    with file(file_path,'rb') as fh:
        while True:
            data = fh.read(CHUNK_SIZE)
            if not data:
                break
            yield data

def get_chunk_hash(chunk):
    """ returns the MD5 for the data """
    m = md5()
    m.update(chunk)
    return m.hexdigest()

def get_file_hash(path):
    """ returns the MD5 for the file """
    # we don't want to read in the while file
    # at once so we'll chunk through it
    m = md5()
    for chunk in chunk_file(path):
        m.update(chunk)
    return m.hexdigest()


class Uploader(object):
    def __init__(self,host,port):

        # info about the server
        self.host = host
        self.port = port

        # list of active uploads
        self.uploads = []

        log.debug('uploader init: %s %s' % (
                  self.host,self.port))

    def get_progress(self):
        """
        returns the overall % progress
        """

        log.debug('getting progress')

        # maybe nothing to report
        if not self.uploads:
            log.debug('no active uploads')
            return None

        # collect up the progress %'s
        prog_sum = 0

        # go through our uploads
        for upload in self.uploads:

            # add in their %
            prog_sum += upload.progress

        # avg !
        progress = prog_sum / len(self.uploads)

        log.debug('progress: %s' % progress)

        return progress

 
    def is_active(self):
        """
        True / False are we actively uploading ?
        """
        
        # find the active uploads
        active = [x for x in self.uploads if x.is_alive()]
        
        # if any of them are active, we are active
        uploading = True if active else False

        log.debug('getting is uploading: %s' % uploading)

        return uploading

    def add_upload(self,path):
        """
        adds a file / dir to the list
        of files to be uploaded. if the path
        is to a directory than each file in the
        directory is send (recursively)
        """

        # get our abs path
        path = os.path.abspath(os.path.expanduser(path))

        log.debug('adding upload: %s' % path)

        # if it's a directory than we want to search it recursively
        if os.path.isdir(path):
            
            log.debug('path is dir')

            # find all the files recursively
            files = find_files(path)

        else:
            
            log.debug('path is file')

            # it's a single file, still want a list
            files = [path]

        log.debug('files: %s' % files)

        # go through all our files, starting uploads
        for file_path in files:

            log.debug('creating upload for: %s' % file_path)

            # create our upload thread
            upload = UploadThread(file_path,
                                  self.host,self.port)

            log.debug('starting upload')

            # start it goin
            upload.start()

            # add it to our tracking list
            self.uploads.append(upload)


class UploadThread(Thread):

    def __init__(self,file_path,host,port):
        Thread.__init__(self)

        # bytes sent
        self.data_sent = 0

        # bytes total to send
        self.file_size = None

        # md5 hex digest of the file
        self.file_hash = None
        
        # tracks start of current chunk
        self.cursor = None

        # where are we posting to?
        self.url = None

        # what file are we working on?
        self.file_path = file_path

        # where's the file going?
        self.host = host
        self.port = port


    # how far along are we ?
    progress = property(lambda s: (s.data_sent/s.file_size)*100
                                if s.file_size else None)

    def run(self):
        """
        given a source file's path will (re)start uploading
        a file to the server
        """

        log.debug('starting upload thread: %s %s %s' %
                  (self.file_path,self.host,self.port))

        # get the file's size
        self.file_size = os.path.getsize(self.file_path)

        log.debug('file size: %s' % self.file_size)

        # we track files by their hash, get this files hash
        self.file_hash = get_file_hash(self.file_path)

        log.debug('file hash: %s' % self.file_hash)

        # figure out what the name of our file is
        self.file_name = os.path.basename(self.file_path)

        log.debug('cursor: %s' % self.cursor)

        # set our url from the host and port
        self.url = 'http://%s:%s/%s' % (self.host,
                                        self.port,
                                        self.file_name)

        log.debug('url: %s' % self.url)

        # start uploading the file
        for chunk in chunk_file(self.file_path):

            log.debug('sending new chunk')
            
            # post the data to the server
            self.send_file_data(chunk)

            # update our cursor
            self.cursor += len(chunk)

            log.debug('new cursor pos: %s' % self.cursor)

            # and our data sent
            self.data_sent += len(chunk)

            # update our history
            file_history['cursor'] = self.cursor

            # save down history
            save_files_history(self.files_history)

        # if we're done, than reset the cursor
        # and update the history to show it's been
        # uploaded before

        logging.debug('DONE!')


    def send_file_data(self,chunk,**kwargs):
        """
        sends a chunk of file data to the server
        """

        log.debug('sending file data')

        # we want to get the hash of the chunk, so that
        # the server knows if it's valid on the other end
        chunk_hash = get_chunk_hash(chunk)

        log.debug('chunk hash: %s' % chunk_hash)

        # when we make the request to the server
        # we are going to pass it the cursor location
        # the size of the chunk, and the hash for the chunk
        # as well as any kwargs which we were passed

        headers = {
            'Content-Type':None,
            'Content-Length':len(chunk),
            'Content-MD5':chunk_hash,
            'Content-Offset':self.cursor,
            'User-Agent':'UploadClientAlpha'
        }

        # add in the kwargs to the headers
        for k,v in kwargs.iteritems():
            headers[k] = v

        log.debug('headers: %s' % headers)
        log.debug('creating request')

        # create our request
        request = Request(self.url,None,headers)

        log.debug('adding chunk data to request')

        # add our chunk's data to the request
        request.add_data(chunk)

        log.debug('posting request')

        try:
            # send our request to the server
            urlopen(request)

        except:
            raise

        log.debug('done sending')


if __name__ == '__main__':
    import sys

    log.debug('starting')
    log.debug('creating uploader')
    
    # create an uploader
    uploader = Uploader('localhost',8005)

    # feed it in the files we were passed
    for path in sys.argv[1:]:

        log.debug('adding upload path: %s' % path)

        # feed the path to our uploader
        uploader.add_upload(path)

   
    # sit on the uploader
    while uploader.is_active():
        
        # print out the progress
        print 'progress: %s' % uploader.get_progress()

        sleep(1)

    # done!
