# zeex

## Description
*Chunked lzma compression, with offset index. Quickly seekable. WORM. Non-streamable. Python Implementation*

Zeex is both an experimental container file format and a corresponding python class for chunked lzma compression that allows to seek quickly in the compressed file.

The file format is *Write Once Read Many*. The compressor takes the given input data, breaks it into blocks(or chunks) of known size, compresses each chunk seperately, and writes out the position index of each block in the data (after header).
The compression is not streamable, as the header is re-written at the end of compression. Decompression however is streamable.

The classes help in writing and reading the compressed files. They roughly follow the pattern of a file object (read, write, tell, seek and close methods). This is only for convenience and the object must not be treated as a file object, as the behaviour can vary from that of a file object. 


The library can also be called directly 

## Usage:
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


