package ar.edu.itba.it.bigdata.practica3;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

public class HBaseMapReducer {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

		Configuration config = HBaseConfiguration.create();
		Job job = new Job(config, "ExampleSummary");
		job.setJarByClass(HBaseMapReducer.class); // class that contains mapper
													// and reducer

		Scan scan = new Scan();
		scan.setCaching(500); // 1 is the default in Scan, which will be bad for
								// MapReduce jobs
		scan.setCacheBlocks(false); // don't set to true for MR jobs
		// set other scan attrs
		scan.addFamily("emails".getBytes());

		String sourceTable = "email";
		String targetTable = "spammers";

		TableMapReduceUtil.initTableMapperJob(sourceTable, // input table
				scan, // Scan instance to control CF and attribute selection
				HBaseMapper.class, // mapper class
				Text.class, // mapper output key
				IntWritable.class, // mapper output value
				job);
		TableMapReduceUtil.initTableReducerJob(targetTable, // output table
				HBaseReducer.class, // reducer class
				job);

		boolean b = job.waitForCompletion(true);
		if (!b) {
			throw new IOException("error with job!");
		}

	}
}
