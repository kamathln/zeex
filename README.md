# zeex

## Description
*Chunked lzma compression, with offset index. Quickly seekable. WORM. Non-streamable. Python Implementation*

**Alpha quality DO NOT use in production yet** This has been published more to present the idea. It must be somewhat usable now, but edge cases might not be posished off.

Zeex is both an experimental container file format and a corresponding python class for chunked lzma compression that allows to seek quickly in the compressed file.

The file format is *Write Once Read Many*. The compressor takes the given input data, breaks it into blocks(or chunks) of known size, compresses each chunk seperately, and writes out the position index of each block in the data (after header).
The compression is not streamable, as the header is re-written at the end of compression. Decompression however is streamable.

The classes help in writing and reading the compressed files. They roughly follow the pattern of a file object (read, write, tell, seek and close methods). This is only for convenience and the object must not be treated as a file object, as the behaviour can vary from that of a file object. 


The library can also be called directly 

## Usage on command line:
  python3 zeex.py action args
  python3 -m zeex action args
  
**Actions**

 *c infile outfile*
 
- compresses data from standard input and spits out compressed data into file provided. File and filesystem must be seekable.
   * outile - the file to put compressed data
   
   * example usage:
      *  buzz compress somefile.xml.buzz < somefile.xml
  
- d infile [outfile]
    * decompresses data from infile and places the output in outfile if specified, or routes it to standard output if not.
    * infile - filename representing an existing file in the buzz format, must be seekable.
    * outfile - filename to which the extracted data is to be written. DANGER: if the file exists, it will be overwritten. Filesystem must be seekable.

-  x infile start_offset end_offset

## Usage in a program

### Writer example
```python
import zeex
....somecode....
zwriter = zeex.ZeexFileWriter("largefile.zeex")
....somecode....
data = somecodethatmakesdata()
zwriter.write(data)  # pushes the data into the compression queue.
while somecond:
  ....somecode that results in gbs of data in total....
  data = gensomedata(x,z,y) 
  zwriter.write(data)   # pushes some more data into the compression queue

#extremely important:
zwriter.close()
```

### Reader example
```python
import zeex
....somecode....
zreader = zeex.ZeexFileReader("largefile.zeex")
print("Compressed file size {}".format(zreader.header.cdata_length))  #size of the data after compression excluding header and index
print("Original file size {}".format(zreader.header.data_length)) # The size of the original file

zreader.seek(100 * 1024 * 1024)   # move to 100 mega byte'th byte 
print(zreader.tell())  #ensure in current position
data = zreader.read(1024 * 1024)  # get 1 MB of data  
```
