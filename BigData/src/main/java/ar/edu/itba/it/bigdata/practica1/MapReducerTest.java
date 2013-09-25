package ar.edu.itba.it.bigdata.practica1;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MapReducerTest {

	static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

		private static int counter = 0;
		
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			String line = value.toString();
			counter++;
			int words = line.split("").length;

			context.write(new Text(String.valueOf(counter)), new IntWritable(words));
			}
		}

		static class MyReducer extends
				Reducer<Text, IntWritable, Text, IntWritable> {

			public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

			    int sum = 0; 

			    for (IntWritable value : values) {
			      sum += value.get(); 
			    }
			    
			    context.write(key, new IntWritable(sum)); 
			  }

		}

		public static void main(String[] args) throws Exception {

				  if (args.length != 2) {
				    System.err.println("Usage: MapReduceExample <input path> <outputpath>");
				    System.exit(-1); 
				  }
				  
				  Job job = new Job(); 
				  job.setJarByClass(MapReducerTest.class);
									
				  FileInputFormat.addInputPath(job, new Path(args[0]));
				  FileOutputFormat.setOutputPath(job, new Path(args[1]));
				  
				  job.setMapperClass(MyMapper.class);
				  job.setReducerClass(MyReducer.class);
				  job.setOutputKeyClass(Text.class);
				  job.setOutputValueClass(IntWritable.class);

				  System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
