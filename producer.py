import sys
import pyndn
import logging
import datetime
import sys, os, os.path
from mydata import MyData, ChunkInfo, log
from mydata import DEFAULT_BLOCK_SIZE, DEFAULT_RUN_TIMEOUT
from mydata import ADAPTIVE_MOD_FLAG
import signal

PAPER="optimal-chunk-size"
ITEM = "producer"

class Producer(pyndn.Closure):
    def __init__(self, name, mydata, content_path=None, **kwargs):
        self.name = pyndn.Name(name) #not include segment number and version number
        self.handle = pyndn.NDN()
        self.exclusions = pyndn.ExclusionFilter()
        
        self.mod = kwargs.get("mod", "non-adatpive")
        self.default_block_size = kwargs.get("default_block_size", DEFAULT_BLOCK_SIZE)
        
        self.mydata = mydata
        s = content_path
        if s == None:  
            s = os.path.basename(name)    
        
        if os.path.exists(s):
            log.info("file %s will be published with name %s " %(s, name))
        else:
            log.error("%s is not exist under %s directory" %(s, "current" if content_path==None else "the"))
            exit(0)
                            
        self.file_in = open(s, "rb")
        self.file_size =  os.path.getsize(s)
        
    def start(self):
        self.handle.setInterestFilter(self.name, self)
        self.handle.run(-1)

    def prepare(self, upcallInfo):
        ist = upcallInfo.Interest
        ist_name = ist.name
        
        mod = "non-adaptive"
        expected_block_size = self.default_block_size
        chunkid = int(ist_name[-1])
        begin_byte = chunkid * self.default_block_size
        
        
         
        for i in range(len(ist_name)):
            sub = ist_name[i]
            if sub == ADAPTIVE_MOD_FLAG:
                if self.mod != "adaptive":
                    log.error("Interest ask for adptive mod while server does not support")
                    #return None
                mod = "adaptive"
                assert i+1 < len(ist_name), "bad name %s" %(ist_name)
                if self.mod == "adaptive":
                    expected_block_size = int(ist_name[i+1])
                    begin_byte = chunkid
                break
            
        
        if begin_byte > self.file_size:
            log.warn("already reach the final block: begin_byte: %s, file_size: %s" %(begin_byte, self.file_size))
            exit(1)
        
        self.file_in.seek(begin_byte)
        data = self.file_in.read(expected_block_size)
        log.debug("Interest: %s, mod: %s, expected_block_size: %s, begin_byte: %s, chunk_size: %s" \
                  %(ist.name, mod, expected_block_size, begin_byte, len(data)))
        
        final_block_id = None
        
        if self.file_in.tell() == self.file_size:
            final_block_id = chunkid
            
        log.debug("data is %s"%(data))
        
        # create a new data packet
        co = pyndn.ContentObject()

        # since they want us to use versions and segments append those to our name
        #co.name = self.name.appendVersion().appendSegment(0)
        co.name = ist_name
        
        # place the content
        co.content = data

        si = co.signedInfo

        key = self.handle.getDefaultKey()
        # key used to sign data (required by ndnx)
        si.publisherPublicKeyDigest = key.publicKeyID

        # how to obtain the key (required by ndn); here we attach the
        # key to the data (not too secure), we could also provide name
        # of the key under which it is stored in DER format
        si.freshnessSeconds = 0
        
        si.keyLocator = pyndn.KeyLocator(key)

        # data type (not needed, since DATA is the default)
        si.type = pyndn.CONTENT_DATA

        # number of the last segment (0 - i.e. this is the only
        # segment)
        if final_block_id != None:
            si.finalBlockID = pyndn.Name.num2seg(final_block_id)
    
        # signing the packet
        co.sign(key)
        
        chunkinfo = ChunkInfo(chunkid)
        chunkinfo.begin_byte = begin_byte
        chunkinfo.content_size = len(data)
        chunkinfo.expected_block_size = expected_block_size
        chunkinfo.retxN = 1
        self.mydata.add_chunk_info(chunkinfo)
        
        
        return co
    
    def upcall(self, kind, upcallInfo):
        if kind != pyndn.UPCALL_INTEREST:
            log.warn("get kind: %s" %str(kind))
            return pyndn.RESULT_OK
        co = self.prepare(upcallInfo)
        self.handle.put(co)
        log.info("content: %s" %co)
        
        
        
        
        return pyndn.RESULT_INTEREST_CONSUMED

    def stop(self):
        #self.mydata.list_info()
        self.handle.setRunTimeout(DEFAULT_RUN_TIMEOUT/100)
        return
    
mydata = MyData()

def signal_handler(signal, frame):
        print 'You pressed Ctrl+C!'
        
        mydata.list_info()
        sys.exit(0)
        
def usage():
    s = '''
        python consumer <NAME> [INTEREST_SCHEMA:-i|s|e] [MOD:-a|-na] [DEFAULT_BLOCK_SIZE: -b DEFAULT_BLOCK_SIZE]
        e.g.: python consumer.py /swordguy/chunksize/a -i -na -b 4096
            ------------------------------------------------------------
            Usage:
            -h:    show help information
            
            Name:  make sure NAME is routable to the producer
            
            Interest Schema: default -i
            -i:    use id directly in the Interest, e.g.: <name>/0, <name>/0
            -e:    use exclusion in the Interest, e.g.: <name> with exclusion <0,1,2>
            -s:    use segment in the Interest, e.g.: <name>/%FD00
            
            Mod: default -na
            -a:    adapt expected optimal block size according to loss rate
            -na:   non-adaptive, use static block size
            
            DEFAULT_BLOCK_SIZE: default 4096
            -b DEFAULT_BLOCK_SIZE: set the default block size to DEFAULT_BLOCK_SIZE
           ----------------------------------------------------------------- 
        '''
    print s
  
if __name__ == "__main__":
    if len(sys.argv) < 2 or sys.argv[1].startswith("-"):
        usage()
        exit(0)
    signal.signal(signal.SIGINT, signal_handler)
    
    
    name = sys.argv[1]
    content_path = None
    begin_index = 2
    if len(sys.argv) >=3 and (not sys.argv[2].startswith("-")):
        content_path = sys.argv[2]
        begin_index += 1
        
    interest_schema = "id"
    mod = "adaptive"
    default_block_size = DEFAULT_BLOCK_SIZE
    
    for i in range(begin_index, len(sys.argv)):
        av = sys.argv[i]
        if av == "-e":#use exclusion
            interest_schema = "exclusion"
        elif av == "-s": #use segment
            interest_schema = "segment"
        elif av == "-i":#use id directly in the name
            interest_schema = "id"
        elif av == "-a": #adaptive mode, which adapt chunk size according to the packet loss rate
            mod = "adaptive"
        elif av == "-na": #adaptive mode, which adapt chunk size according to the packet loss rate
            mod = "non-adaptive"
        elif av == "-b": #block size
            assert i <= len(sys.argv), "-b should be followed by chunk size"
            i += 1
            default_block_size = int(sys.argv[i])
        elif av == "-h" or av == "--help":
            usage()
            #exit(0)
    #global mydata
    
    #mydata = MyData()
    consumer = Producer(name=name, mydata=mydata, content_path=content_path,\
                        mod=mod, default_block_size=default_block_size)
    consumer.start()
    mydata.list_info(enable_chunks=True)
    log.info("program %s:%s end" %(PAPER, ITEM))
    

    