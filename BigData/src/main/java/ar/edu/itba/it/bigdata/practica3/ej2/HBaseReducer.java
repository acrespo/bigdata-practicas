package ar.edu.itba.it.bigdata.practica3.ej2;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class HBaseReducer extends TableReducer<Text, IntWritable, ImmutableBytesWritable> {
	public static final byte[] CF = "category".getBytes();
	public static final byte[] COLUMN1 = "name".getBytes();
	public static final byte[] COLUMN2 = "count".getBytes();

	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		int i = 0;
		for (IntWritable val : values) {
			i += val.get();
		}
		
		Put put = new Put(Bytes.toBytes(key.toString()));
		put.add(CF, COLUMN1, Bytes.toBytes(key.toString()));
		put.add(CF, COLUMN2, Bytes.toBytes(String.valueOf(i)));

		context.write(null, put);
	}
}
