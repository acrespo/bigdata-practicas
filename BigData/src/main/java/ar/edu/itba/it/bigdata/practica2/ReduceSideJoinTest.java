package ar.edu.itba.it.bigdata.practica2;


import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ReduceSideJoinTest {

	static class CustomerMapper extends
			Mapper<LongWritable, Text, Text, TextPair> {

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			String line = value.toString();
			String[] fields = line.split(",");
			System.out.println("customer-order-reducer");
			for (String s: fields) {
				System.out.print(s);
				System.out.print(" - ");
			}
			System.out.println(" ");
			String id = fields[0];
			String name = fields[1];
			String phone = fields[2];

			context.write(new Text(id), new TextPair("Customer", name));
		}
	}

	static class CustomerOrderMapper extends
			Mapper<LongWritable, Text, Text, TextPair> {

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			String line = value.toString();
			String[] fields = line.split(",");
			System.out.println("customer-order-mapper");
			for (String s: fields) {
				System.out.print(s);
				System.out.print(" - ");
			}
			System.out.println(" ");
			String orderId = fields[0];
			String customerId = fields[1];
			String price = fields[2];
			String date = fields[3]; // NOT INTERESTED

			context.write(new Text(customerId), new TextPair("Customer-Orders",
					orderId + "-" + price));
		}
	}

	static class MyReducer extends Reducer<Text, TextPair, NullWritable, Text> {


		
		public void reduce(Text key, Iterable<TextPair> values, Context context)
				throws IOException, InterruptedException {

			List<Text> customer = new LinkedList<Text>();
			List<Text> customerOrders = new LinkedList<Text>();
			
			System.out.println("Reducer for id:" + key);
			
			for (TextPair p : values) {
				if (p.getFirst().toString().equals("Customer")) {
					customer.add(p.getSecond());
					System.out.println("Adding customers:" +  p.getSecond());
					
				} else {
					customerOrders.add(p.getSecond());
					System.out.println("Adding customerOrders:" +  p.getSecond());
				}
			}
			System.out.println(customer.size());
			System.out.println(customerOrders.size());
			for (Text c : customer) {
				for (Text co : customerOrders) {
					System.out.println(new Text(co + "-" + c));
					context.write(null, new Text(co + "-" + c));
				}
			}
		}
	}

	public static void main(String[] args) throws Exception {

		if (args.length != 2) {
			System.err.println("Usage: ReduceSideJoinTest <input path> <outputpath>");
			System.exit(-1);
		}

		Job job = new Job();
		job.setJarByClass(ReduceSideJoinTest.class);

		FileUtils.deleteDirectory(new File(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		MultipleInputs.addInputPath(job, new Path("/home/acrespo/customer"), TextInputFormat.class, CustomerMapper.class);
		MultipleInputs.addInputPath(job, new Path("/home/acrespo/customer-orders"), TextInputFormat.class, CustomerOrderMapper.class);

//		job.setCombinerClass(MyReducer.class);
		job.setReducerClass(MyReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(TextPair.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
