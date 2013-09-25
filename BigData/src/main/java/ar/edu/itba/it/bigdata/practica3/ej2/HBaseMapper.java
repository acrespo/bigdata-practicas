package ar.edu.itba.it.bigdata.practica3.ej2;

import java.io.IOException;
import java.util.NavigableMap;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class HBaseMapper extends TableMapper<Text, IntWritable> {
	public static final byte[] CF = "info".getBytes();
	public static final byte[] COLUMN = "categories".getBytes();
	
	private final IntWritable ONE = new IntWritable(1);
	
	public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {
		
		byte[] val = value.getValue(CF,COLUMN);
		
		String[] strs = new String(val).split(",");
		
		for (String s: strs) {
			context.write(new Text(s.trim()), ONE);
		}
		
	}
}