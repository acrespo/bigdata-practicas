package ar.edu.itba.it.bigdata.practica3;

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
	public static final byte[] CF = "emails".getBytes();

	private final IntWritable ONE = new IntWritable(1);
	
	public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {
		
		NavigableMap<byte[], byte[]> familyMap = value.getFamilyMap(CF);
		
		
		Set<byte[]> keySet = familyMap.keySet();
		for (byte[] barray: keySet) {

			byte[] val = value.getValue(CF, barray);
			String str = new String(val);
			String[] strs = Pattern.compile("[: ]").split(str);
			Text text = new Text(strs[1]);

			if (str.contains("spam")) {
				context.write(text, ONE);
			}
		}
		
	}
}