#!python
"""
ZEEX:
Zeex is both an experimental container file format and a corresponding python class for chunked lzma compression that allows to seek quickly in the compressed file.
The file format is Write Once Read Many. The compressor takes the given input data, breaks it into blocks(or chunks) of known size, compresses each chunk seperately, and writes out the position index of each block in the data (after header).
The compression is not streamable, as the header is re-written at the end of compression. Decompression however is streamable.


The following classes help in writing and reading the compressed files. They roughly follow the pattern of a file object (read, write and close methods).This is only for conveinience and the object must not be treated as a file object. 

ZeexHeader is used to represent the header of the file. It is used internally, but may also be useful when needing to know the uncompressed size and other details of the file.

ZeexFileWriter is a class that can be instantiated and fed data to, which will be compressed and written to the compressed file.

ZeexFileReader is a class that can be instantiated with an already compressed file 

"""

try:
    from backports import lzma
except ImportError:
    import lzma
import ctypes
import sys

#FILE:

"""
Header
    MagicCode   (32 Bytes = 4 bytes)   Must be ZEEX
    Version Code    (16 Bytes = 2 Bytes)  uint32   Currently 1
    Block size (64 bit = 8 bytes) uint64
    Data_Length (64 bit = 8 bytes) in no of bytes

Compressed(DATA)

Index  (Comes After Data
    Byte Index
        Index size (32 bit = 4 bytes)  Array Len in num of items  
        Index Data[Array]
            pointer to next block (64bit int)

"""
ZEEXMAGIC = 'ZEEX'
ZEEXVERSION = 1
'Format version. (Not necessarily the version of code.)'

class ZeexHeader(ctypes.Structure):
    'Represents the structure of the header of the file. See also: the _fields_ member'
    _fields_ = (('magic', ctypes.c_char * 4),
                ('version', ctypes.c_uint16),
                ('block_size', ctypes.c_uint64),
                ('data_length', ctypes.c_uint64),   #length before compression
                ('cdata_length', ctypes.c_uint64)   #length after compression
                )

class ZeexOutOfBoundBlockExceptions(Exception):
    'Exception when some code requests for a non-exsitant data block'
    def __init__(self, message, blocknum):
        super(ZeexOutOfBoundExceptions,self).__init__(message)
        self.blocknum = blocknum

class ZeexOutOfBoundExceptions(Exception):
    'Exception when some code requests for the data out of the file\'s size'
    def __init__(self, message, bounds):
        super(ZeexOutOfBoundExceptions,self).__init__(message)
        self.bounds = bounds

class ZeexFileWriter(object):
    """A class that helps in creating a zeex file. See also: the initializer documentation.
    
    Don't forget to call "close()" object at the end!
    
    WARNING: These methods are made to resemble File class methods only for convenience. THey may not behave intricately like one.
    """
    def __init__(self, outfilename, in_block_size=int(20.0 *1024 * 1024)):
        """ZeexFileWriter has to be initialized with an outfilename, which will be unconditionally overwritten.
        
        @param outfilename
        The filename to which the compressed data is to be written

        @param in_block_size
        The size of the data block to accumulate and compress in one go.

        TL;DR:
        The way to use this class is to initialize it with a filename, which will be opened in write mode. 
        Then keep adding any amount of data using the "write" method, just like a file object.
        When everything is done, call the "close()" method on this object. WARNING: Do not forget this.

        What really happens is : every time you write data, it is collected in a buffer till it reaches at least 
        the block size. 
        Then exactly the blocksize sized block is compressed and the size is remembered in the index attirubute.
        Finally, when the "close()" method is called, the remianing data is compressed and the index written. Hence, it is very important to call the "close" method.

        """

        self._index = [0]
        self._outfilename = outfilename
        self.in_block_size = in_block_size
        self._header = ZeexHeader()
        self._header.magic = 'ZEEX'
        self._header.version = 1

        self._outfile = open(self._outfilename, 'w')
        self._outfile.write(buffer(self._header)[:]) # Placeholder .. we will overwrite it in the "finish" function
        
        self._last_out_pos = 0
        self._queue = ''
    
    def write(self,data):
        """Feeds Data to the compressor. The data buffer will not be compressed till either it reaches the block size or the "close" funciton is called on the writer object.
        
        @param data
        Data to be fed to the writer

        WARNING: These methods are made to resemble File class methods only for convenience. They may not behave intricately like one.
        """
        self._queue += data
        while (len(self._queue) >= self.in_block_size):
            data = self._queue[0:self.in_block_size]
            self._queue = self._queue[self.in_block_size:]
            compressed = lzma.compress(data)
            self._outfile.write(compressed)
            cur_out_pos = self._last_out_pos + len(compressed)
            self._index.append(cur_out_pos)
            self._last_out_pos = cur_out_pos

    def close(self):
        """Compress and write remaining data, and also write the index."""
        compressed = lzma.compress(self._queue)
        self._outfile.write(compressed) 
        
        size = ((len(self._index)-1) * self.in_block_size ) + len(self._queue)
        self._header.block_size = self.in_block_size
        self._header.data_length = size
        self._header.cdata_length = len(compressed) + self._last_out_pos

        self._outfile.write(buffer(ctypes.c_uint32(len(self._index)))[:])
        #sys.stderr.write("Index: \n")
        for idx, i in enumerate(self._index):
            #sys.stderr.write("\t %d: %d\n"% (idx,i))
            self._outfile.write(buffer(ctypes.c_uint64(i))[:])
        self._outfile.seek(0)
        self._outfile.write(buffer(self._header)[:])
        self._outfile.close()


class ZeexFileReader(object):
    """A class that helps in reading a zeex file. See also: the initializer documentation.
    
    WARNING: These methods are made to resemble File class methods only for convenience. THey may not behave intricately like one.
    """
    def __init__(self,filename):
        self._infile = open(filename)
        self.header = ZeexHeader()
        self.headersize = ctypes.sizeof(self.header)
        self._infile.readinto(self.header)
        #sys.stderr.write("Reading File, Header:\nMagic:%s\nversion: %d\nBlock_size: %d\nData Length: %d\nCompressed_Data_Length:%d\n" % (
        #                 self.header.magic,
        #                 self.header.version,
        #                 self.header.block_size,
        #                 self.header.data_length,
        #                 self.header.cdata_length))


        if self.header.magic != ZEEXMAGIC:
            raise Exception("Unknown file type")

        if self.header.version > ZEEXVERSION:
            raise Exception('Incompatible version')
        index_offset = ctypes.sizeof(self.header) + (self.header.cdata_length)
        #sys.stderr.write("Index offset at %d\n" % index_offset)
        self._infile.seek(index_offset)
        self._index_size = ctypes.c_uint32()
        self._infile.readinto(self._index_size)
        #sys.stderr.write("Index size: %d\n" % self._index_size.value)
        self._index = (ctypes.c_uint64 * self._index_size.value)()
        self._infile.readinto(self._index)
        #for i in xrange(self._index_size.value):
            #sys.stderr.write("\t%d: %d\n"%(i,self._index[i]))
        self._pos = 0
        self.cur_block = 0
        self._max_cached_blocks = 5
        self._blocks_cache_queue=[]
        self._blocks_cache={}
        
    def read(self,size=None):
        """ Returns size bytes of data from the original data or throws an exception. 

        @param size
        number of bytes from the current position. Unlike a normal file object, this parameter is not optional.
        Why?: This library is supposed to help with huge files. read()ing a whole huge file would be problematic in most circumstances, and usually a mistake. 
        To avoid programming error, this change has been made.
        
        WARNING: These methods are made to resemble File class methods only for convenience. THey may not behave intricately like one.
        """
        if size is None:
            raise Exception("unknown size")
            #pos_begin = self._pos
            #pos_end = self.header.data_length
        data_to_ret=''
        sections = self._get_sections(self._pos, size)
        #sys.stderr.write("in read() _pos %d, size: %d\n" % (self._pos, size))
        #
        #sys.stderr.write("Sections: %s\n" % str(sections))
        for block, offset, section_size in sections:
            try:
                block_data = self._get_block_data( block )
            except ZeexOutOfBoundBlockExceptions as be:
                raise  ZeexOutOfBoundExceptions("Requested range out of file bounds", {'offset':offset, 'section_size':section_size})

            data_to_ret = data_to_ret + block_data[offset:offset+section_size]
        if len(data_to_ret) != size:
            raise  ZeexOutOfBoundExceptions("Requested offset out of file bounds", {'offset':offset+section_size, 'section_size':1})
                
        self._pos += size
        #sys.stderr.write("Pos after read %d\n" % self._pos)
        return data_to_ret
            
    def _get_sections(self, pos, size):
        #sys.stderr.write("Getting sections for Pos: %d, Size: %d\n" % (pos,size))
        tpos = pos
        sections=[]
        while tpos < (pos+size):
            #sys.stderr.write("tpos at %d\n" % tpos)
            block = tpos / self.header.block_size
            offset = tpos % self.header.block_size
            section_size = self.header.block_size - offset
            if section_size > size:
                section_size = size
            tpos = (block* self.header.block_size) + offset + section_size + 1
            sections.append( (block, offset, section_size,) )
        return sections

    def _get_block_data(self,block):
        if block in self._blocks_cache_queue:
            return self._blocks_cache[block]
        
        if not (block < len(self._index)):
            raise ZeexOutOfBoundExceptions("Requested block not in file", block)

        offset = self._index[block] + self.headersize
        
        if (block + 1) < len(self._index):
            csize = self._index[block+1] + self.headersize - offset
        else:
            csize = self.header.cdata_length + self.headersize - offset
            
        self._infile.seek(offset)
        compressed = self._infile.read(csize)
        data = lzma.decompress(compressed)

        if len(self._blocks_cache_queue) > self._max_cached_blocks:
            del self._blocks_cache[self._blocks_cache_queue[0]]
            del self._blocks_cache_queue[0]
            self._blocks_cache_queue.append(block)
            self._blocks_cache[block] = data

        return data


    def tell(self):
        """Return current offset in data"""
        return self._pos

    def seek(self,pos):
        """Set current offset in data"""
        if (pos >= self.header.data_length):
            raise IOError("Illegal Seek to pos: " + str(pos))
        self._pos = pos

    def close(self):
        """close the file"""
        self._infile.close()


def print_usage():
    sys.stderr.write(
"""
Usage:
  buzz action args
  
Actions
-------

  c infile outfile
    compresses data from standard input and spits out compressed data into file provided. File and filesystem must be seekable.
    outile - the file to put compressed data
    example usage:
        buzz compress somefile.xml.buzz < somefile.xml
  
  d infile [outfile]
    decompresses data from infile and places the output in outfile if specified, or routes it to standard output if not.
    infile - filename representing an existing file in the buzz format, must be seekable.
    outfile - filename to which the extracted data is to be written. DANGER: if the file exists, it will be overwritten. Filesystem must be seekable.

  x infile start_offset end_offset

"""
)
        
if __name__ == '__main__':

    if len(sys.argv) < 2 :
        sys.stderr.write("Error: No action specified\n")
        print_usage()
        sys.exit(1)
    
    if sys.argv[1] == 'c':
        #sys.stderr.write("Compressing File")
        f = open(sys.argv[2])
        bw = ZeexFileWriter(sys.argv[3])
        while True:
            data = f.read(bw.in_block_size)
            bw.write(data)
            if len(data) < bw.in_block_size:
                break
        bw.close()
        f.close()
        sys.exit(0)

    if sys.argv[1] == 'd' or sys.argv[1] == 'x':
            
        br = ZeexFileReader(sys.argv[2])
        outfile = sys.stdout
        if sys.argv[1] == 'x':
            if len(sys.argv) < 5:
                print_usage()
                sys.exit(1)
            
            start = int(sys.argv[3])
            end = int(sys.argv[4])
        else:
            start = 0
            end = br.header.data_length
            
            if len(sys.argv) < 3:
                print_usage()
                sys.exit(1)
            if len(sys.argv) == 4:
                outfile = open(sys.argv[3],'w')

        br.seek(start)
        total = 0
        size = end - start
        #sys.stderr.write("size: %d\n" % size)
        done = False
        while not done:
            #sys.stderr.write("Total: %d\n" % total)
            #sys.stderr.write("Size - Total: %d\n" % (size - total))
            if (size - total) > br.header.block_size:
                bsize = br.header.block_size
            else:
                bsize = size - total
                done = True
            #sys.stderr.write("bsize: %d\n" % bsize)
            data = br.read(bsize)
            outfile.write(data)
            total = total + bsize
            if not done:
                br.seek(start + total)
            #sys.stderr.write("seeking to %d\n" % (start + total))
            #sys.stderr.write("seeked to %d\n" % br._pos)
