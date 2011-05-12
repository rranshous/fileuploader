#!/usr/bin/python


# the idea is that we want to facilitate
# large file uploads to a server. the client
# is going to send the data in chunks to the server.
# it should be capable of pickup up an interrupted
# upload on next launch, as well as uploading dirs
# instead of files

from urllib2 import Request, HTTPHandler
from hashlib import md5
import os.path
from findfiles import find_files_iter as find_files

import logging
logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger(__name__)


CHUNK_SIZE = 5 * 1024 # 5MB

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
    for chunk in chunk_file(file_path):
        m.update(chunk)
    return m.hexdigest()


HISTORY_PATH = './history.json'

def load_files_history(history):
    
    log.debug('loading history')

    if not os.path.exists(HISTORY_PATH):
        log.debug('no history found')
        return {}

    with file(HISTORY_PATH,'r') as fh:
        return json.loads(fh.read())

def save_files_history(history):
    
    log.debug('saving history')

    with file(HISTORY_PATH,'w') as fh:
        fh.write(json.dumps(history))


class Uploader(object):
    def __init__(self,host,port):

        # info about the server
        self.host = host
        self.port = port

        # list of active uploads
        self.uploads = []

        # history of the files, where
        # we are in the file, it's hash etc
        self.files_history = {}

        # load up our previous history
        self.files_history = load_files_history()

        log.debug('uploader init: %s %s %s' % (
                  self.host,self.port,
                  len(self.files_history)))

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

 
    def is_uploading(self):
        """
        True / False are we actively uploading ?
        """
        
        # find the active uploads
        active = [x for x in self.uploads if x.is_active()]
        
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
            upload = UploadThread(args=(file_path,host,port,
                                        files_history))

            log.debug('starting upload')

            # start it goin
            upload.start()

            # add it to our tracking list
            uploads.append(upload)


class UploadThread(Thread):

    def __init__(self,*args,**kwargs):
        Thread.__init__(self,*args,**kwargs)

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

    # how far along are we ?
    progress = property(lambda: self.data_sent/self.file_size*100
                                if self.file_size else None)

    def run(self,file_path,host,port,files_history,
            **kwargs):
        """
        given a source file's path will (re)start uploading
        a file to the server
        """

        log.debug('starting upload thread: %s %s %s' %
                  file_path,host,port)

        # get the file's size
        self.file_size = os.path.getsize(file_path)

        log.debug('file size: %s' % self.file_size)

        # we track files by their hash, get this files hash
        self.file_hash = get_file_hash(file_path)

        log.debug('file hash: %s' % self.file_hash)

        # see if we've already begun uploading it's data
        file_history = files_history.setdefault(file_hash,{})

        log.debug('file history: %s' % file_history)

        # our curser is going to track the start of the current chunk
        # get our cursor from history, or start @ 0
        self.cursor = file_history.get('cursor',0)

        log.debug('cursor: %s' % self.cursor)

        # set our url from the host and port
        self.url = 'http://%s:%s' % (host,port)

        log.debug('url: %s' % self.url)

        # start uploading the file
        for chunk in chunk_file(file_path):

            log.debug('sending new chunk')
            
            # post the data to the server
            self.send_file_data(chunk, **kwargs)

            # update our cursor
            self.cursor += len(chunk)

            log.debug('new cursor pos: %s' % self.cursor)

            # and our data sent
            self.data_sent += len(chunk)

            # update our history
            file_history['cursor'] = cursor

            # save down history
            save_history(files_history)


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
            'User-Agent':'UploadClientAlpha'
        }

        # add in the kwargs to the headers
        for k,v in kwargs.iteritems():
            headers[k] = v

        log.debug('headers: %s' % headers)
        log.debug('creating request')

        # create our request
        request = Request(url,None,headers)

        log.debug('adding chunk data to request')

        # add our chunk's data to the request
        request.add_data(chunk)

        log.debug('creating handler')

        # create a handler for the request
        # TODO: should use HTTPS ..
        handler = HTTPHandler()

        log.debug('posting request')

        try:
            # send our request to the server
            handler.http_request(request)

        except:
            raise

        log.debug('done sending')


if __name__ == '__main__':

    log.debug('starting')
    log.debug('creating uploader')
    
    # create an uploader
    uploader = Uploader('localhost',80)

    # feed it in the files we were passed
    for path in sys.argv[1:]:

        log.debug('adding upload path: %s' % path)

        # feed the path to our uploader
        uploader.add_upload(path)

   
    # sit on the uploader
    while uploader.is_uploading():
        
        # print out the progress
        print 'progress: %s' % uploader.get_progress()

    # done!
