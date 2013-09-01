import sys
#from pyndn import Name, Key
import pyndn
import logging
import datetime
from mydata import MyData, ChunkInfo, log
from mydata import DEFAULT_BLOCK_SIZE, DEFAULT_RUN_TIMEOUT
from mydata import ADAPTIVE_MOD_FLAG
import os.path
import signal

PAPER="optimal-chunk-size"
ITEM = "consumer"

class Consumer(pyndn.Closure):
    def __init__(self, name, mydata, **kwargs):
        self.name = pyndn.Name(name) #not include segment number and version number
        self.handle = pyndn.NDN()
        self.exclusions = pyndn.ExclusionFilter()
        self.interest_schema = kwargs.get("interest_schema", "id")
        self.mod = kwargs.get("mod", "non-adatpive")
        self.default_block_size = kwargs.get("default_block_size", DEFAULT_BLOCK_SIZE)
        
        self.mydata = mydata
        s = os.path.basename(name)
        if os.path.exists(s):
            log.info("file %s will be replaced " %(s))
        self.file_out = open(s, "w")
        
        self.chunkinfo = None
        
    def start(self):
        log.info("consumer start, name: "+ str(self.name))
        self.express_interest()

        self.handle.run(-1)
    
    def express_interest(self):
        if self.chunkinfo == None:#a new chunk requested
            self.chunkinfo = ChunkInfo(self.mydata.next_seg)
            self.chunkinfo.begin_byte = self.mydata.next_byte
            self.chunkinfo.expected_chunk_size = self.mydata.estimated_optimal_size or self.default_block_size
            self.chunkinfo.beginT = datetime.datetime.now()
            self.mydata.add_chunk_info(self.chunkinfo)
                
        self.chunkinfo.retxN += 1
        
        selector = pyndn.Interest()
        selector.answerOriginKind = True
        name = self.name
        chunkid = self.mydata.next_seg
        
        if self.mod == "adaptive": #byte index as chunkid
            name = self.name.append(ADAPTIVE_MOD_FLAG).append(str(self.chunkinfo.expected_chunk_size))
            chunkid = self.mydata.next_byte
            
        if self.interest_schema == "exclusion": #do not use this
            selector = pyndn.Interest(exclude = self.exclusions)
            selector.answerOriginKind = True
        elif self.interest_schema == "id":#the default one
            name = pyndn.Name(name.components)
            name = name.append(chunkid)
            
        elif self.interest_schema == "segment":
            name = pyndn.Name(name.components)
            name = name.appendSegment(chunkid)
            
        self.handle.expressInterest(name, self, selector)
        
        #log.info("express interest %s, exclusions: %s"  %(str(self.name), self.exclusions));
        log.debug("interest=%s " %name)
        
    def do_meet_accident(self, kind, upcallInfo):
        log.warn("accident happen: kind=%s, upcallInfo=%s" %(kind, upcallInfo))
        self.handle.setRunTimeout(DEFAULT_RUN_TIMEOUT)
        self.express_interest()
        
    def do_receive_content(self, kind, upcallInfo):
        self.handle.setRunTimeout(DEFAULT_RUN_TIMEOUT)
        self.chunkinfo.endT = datetime.datetime.now()
        self.chunkinfo.content_size = len(upcallInfo.ContentObject.content)
        
        
        self.chunkinfo = None
        log.info("receive valid content: %s" %upcallInfo.ContentObject.name)
        log.debug("receive valid content: %s" %upcallInfo.ContentObject)
        
        self.file_out.write(upcallInfo.ContentObject.content)
        fbi = upcallInfo.ContentObject.signedInfo.finalBlockID 
        if  fbi != None:
            if isinstance(fbi, str):
                fbi = pyndn.Name.seg2num(fbi)
                
            log.info("***************final block id: %s, or %s" %(upcallInfo.ContentObject.signedInfo.finalBlockID, fbi))
            #log.debug("final_block_id: %s" %self.mydata.final_block_id)
            #log.debug("final content: %s" %upcallInfo.ContentObject)
            if self.mydata.final_block_id == None:
                self.mydata.final_block_id = int(fbi)
            else:
                assert self.mydata.final_block_id == fbi, \
                    "self.mydata.final_block_id: %s != upcallInfo.ContentObject.signedInfo.finalBlockID: %s" \
                        %(self.mydata.final_block_id, upcallInfo.ContentObject.signedInfo.finalBlockID)
            log.info("mydata.final_block_id: %s, next_seg: %s, next_byte: %s" \
                      %(self.mydata.final_block_id, self.mydata.next_seg, self.mydata.next_byte))
            
            
        self.mydata.next_seg += 1
        self.mydata.next_byte += len(upcallInfo.ContentObject.content)
        self.mydata.total_size += len(upcallInfo.ContentObject.content)
        
        if self.mydata.final_block_id !=None:
            
            if (self.mod == "adaptive" and self.mydata.next_byte > self.mydata.final_block_id) or \
                    (self.mod == "non-adaptive" and  self.mydata.next_seg > self.mydata.final_block_id):
                log.info("**********aready get the final block")
                self.stop()
                return
        else:
            log.debug("final_block_id: %s, next_seg: %s" %(self.mydata.final_block_id, self.mydata.next_seg))
            self.express_interest()
        
        
    def upcall(self, kind, upcallInfo):
        if kind == pyndn.UPCALL_FINAL:#handler is about to be deregistered
            log.info("handler is about to be deregistered")
            if self.mydata.final_block_id == None:
                self.handle.setRunTimeout(DEFAULT_RUN_TIMEOUT*10)

            return pyndn.RESULT_OK
        
        if kind == pyndn.UPCALL_INTEREST_TIMED_OUT:
            
            self.do_meet_accident(kind, upcallInfo)
            return pyndn.RESULT_OK
        
        if kind in [pyndn.UPCALL_CONTENT_UNVERIFIED, pyndn.UPCALL_CONTENT_BAD]:
            
            self.do_meet_accident(kind, upcallInfo)
            return pyndn.RESULT_OK
        
        if kind in [pyndn.UPCALL_INTEREST, pyndn.UPCALL_CONSUMED_INTEREST]:
            log.warn("unexpected kind: %s" %kind)
            
            
        assert kind == pyndn.UPCALL_CONTENT, "kind: "+str(kind)
        if self.interest_schema == "exclusion":
            matched_comps = upcallInfo.matchedComps
            response_name = upcallInfo.ContentObject.name
            org_prefix = response_name[:matched_comps]
            
            assert org_prefix == self.name, "org_prefix: "+ str(org_prefix)+" , self.name: "+str(self.name)
            
            if matched_comps == len(response_name):
                comp = pyndn.Name([upcallInfo.ContentObject.digest()])
                disp_name = pyndn.Name(response_name)
            else:
                comp = response_name[matched_comps:matched_comps + 1]
                disp_name = response_name[:matched_comps + 1]
            
            log.info("disp_name: %s, comp: %s" %(disp_name, comp))
            
            self.exclusions.add_name(comp)
            
        self.do_receive_content(kind, upcallInfo)
        
        
        #if matched_comps + 1 < len(response_name):
        #    new = Consumer(response_name[:matched_comps+1])
        #    new.express_interest()
        
        
        return pyndn.RESULT_OK
    
    def stop(self):
        #self.mydata.list_info()
        self.handle.setRunTimeout(DEFAULT_RUN_TIMEOUT/100)
        return
        


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

mydata = MyData()

def signal_handler(signal, frame):
        print 'You pressed Ctrl+C!'
        global mydata
        mydata.list_info()
        sys.exit(0)
        
if __name__ == "__main__":
    if len(sys.argv) < 2 or sys.argv[1].startswith("-"):
        usage()
        exit(0)
    
    
    name = sys.argv[1]
    interest_schema = "id"
    mod = "non-adaptive"
    default_block_size = DEFAULT_BLOCK_SIZE
    
    for i in range(2, len(sys.argv)):
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
    #timeout = sys.argv[2]
    #global mydata
    consumer = Consumer(name=name, mydata=mydata, \
                        interest_schema=interest_schema, mod=mod, default_block_size=default_block_size)
    
    signal.signal(signal.SIGINT, signal_handler)
    consumer.start()
    mydata.list_info()
    log.info("program %s:%s end" %(PAPER, ITEM))    
    
    