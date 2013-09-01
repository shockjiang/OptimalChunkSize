import sys
import pyndn
import logging
import datetime
import mydata
#import utils

PAPER="optimal-chunk-size"
ITEM = "mydata"

DEFAULT_BLOCK_SIZE = 4096
DEFAULT_RUN_TIMEOUT = 1000

#ADAPTIVE_MOD = MyFlag.new_flag("ADAPTIVE_MOD", 0x64)
ADAPTIVE_MOD_FLAG = "<adaptive>"

class ClassFilter(logging.Filter):
    """filter the log information by class name
    """
    ALLOWED_CLASS_LI = None
    def filter(self, record):
        if ClassFilter.ALLOWED_CLASS_LI == None:
            return True
        
        if record.name in ALLOWED_CLASS_LI:
            return True
        else:
            return False
log = logging.getLogger() #root logger

format = logging.Formatter('%(levelname)5s:%(filename)12s:%(lineno)3d: %(message)s')
#%(name) logger name %(filename): file name
#formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
#http://docs.python.org/2/library/self.logging.html#self.logrecord-attributes
#logging.basicConfig(format=format, datefmt='%m/%d/%Y %I:%M:%S %p')

fh = logging.FileHandler(PAPER+".log", mode="w")
fh.setFormatter(format)

sh = logging.StreamHandler() #console
sh.setFormatter(format)

f = ClassFilter()
sh.addFilter(f)


log.addHandler(sh)
log.addHandler(fh)

log.setLevel(logging.DEBUG)

class ChunkInfo:
    def __init__(self, seg):
        self.seg = seg
        self.begin_byte = None
        self.content_size = None
        self.expected_block_size = None
        self.beginT = None #time of first interest of that segment sent
        
        self.retxN = 0

        self.endT = None #time of first content of that segment arrived
class MyData:
    def __init__(self):
        self.chunksinfo = []
        self.beginT = datetime.datetime.now()
        self.endT = None
        self.next_seg = 0
        self.next_byte = 0
        self.estimated_optimal_size = None
        self.final_block_id = None
        self.total_size = 0  #include whole size of chunk, including sig, key, etc
        
    def get_latest_chunk_info(self):
        if self.chunksinfo == [] or self.chunksinfo == None:
            return None
        
        return self.chunksinfo[-1]
    
    def add_chunk_info(self, chunkinfo):
        self.chunksinfo.append(chunkinfo)

    def list_info(self, enable_chunks=False):
        if self.endT == None:
            self.endT = datetime.datetime.now()
        retxNC = 0 #C for counter
        for chunkinfo in self.chunksinfo:
            retxNC += chunkinfo.retxN
            
            if (not enable_chunks) and chunkinfo.retxN == 1:
                #pass
                continue
            s = '''
            seg         = %s
            begin       = %s
            expectedS   = %s
            contentS    = %s
            retxN       = %s
            ''' \
            %(chunkinfo.seg, chunkinfo.begin_byte, chunkinfo.expected_block_size, chunkinfo.content_size, chunkinfo.retxN)
            log.debug(s)
            
        
        s = '''
            next_seg     = %s
            next_byte    = %s
            retxNC       = %s
            total_size   = %s
            final_block  = %s
            beginT       = %s
            endT         = %s
            
            
            
            
             
            
            ''' \
            %(str(self.next_seg), self.next_byte,retxNC, self.total_size, self.final_block_id, str(self.beginT), str(self.endT))
        log.info(s)
            
    def egg(self):
        s = '''
            #------------------------------------------#
            WWWWWWWWWWWWWWWWW
                    W
                    W
                   W
                   W
                  W
                  W
            '''