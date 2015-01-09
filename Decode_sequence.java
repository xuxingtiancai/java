package local_parse;


import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;

public class Decode_sequence {
	public static void main(String[] args) throws IOException {
		if(args.length == 0) {
			System.err.println("filepath arg missing");
			System.exit(1);
		}
		
		//fin
		SequenceFile.Reader reader = null;
		
		//fout
		BufferedOutputStream bos = new BufferedOutputStream(System.out);
		DataOutputStream fout = new DataOutputStream(bos);
		
	    try {  
	        String uri = args[0];
	        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();  
	        FileSystem fs = FileSystem.getLocal(conf);  
	        Path path = new Path(uri);  
	        reader = new SequenceFile.Reader(fs, path, conf);  
	        //Writable key = (Writable) org.apache.hadoop.util.ReflectionUtils.newInstance(reader.getKeyClass(), conf);  
	        //Writable value = (Writable) org.apache.hadoop.util.ReflectionUtils.newInstance(reader.getValueClass(), conf);
	        BytesWritable key = new BytesWritable();
	        BytesWritable value = new BytesWritable();
	        DataOutputBuffer buffer = new DataOutputBuffer();
	        SequenceFile.ValueBytes vbytes = reader.createValueBytes();
	        
	        byte[] bytes_type = new byte[1];
	        bytes_type[0] = (byte) 0;
	        
	        while(true){ 
	        	long pos = reader.getPosition();
	  	      	boolean eof = -1 == reader.nextRawKey(buffer);
	  	      	if(eof) {
	  	      		break;
	  	      	}
	  	        key.set(buffer.getData(), 0, buffer.getLength());
	  	        buffer.reset();
	  	        reader.nextRawValue(vbytes);
	  	        vbytes.writeUncompressedBytes(buffer);
	  	        value.set(buffer.getData(), 0, buffer.getLength());
	  	        buffer.reset();
	  	        
	        	int length = value.getLength();
	        	int offset = 4;
	        	byte[] streamBytes = new byte[length - offset];
				for (int i = offset; i < length; i++) {
					streamBytes[i - offset] = value.getBytes()[i];
				}
				
				fout.write(bytes_type);
				key.write(fout);
				fout.write(bytes_type);
				value.write(fout);
	        }  
	    } catch (Exception e) {  
	        e.printStackTrace();  
	    } finally {  
	        IOUtils.closeStream(reader);  
	        fout.flush();
	        fout.close();
	    }  
	}  
}
