

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;


public class SearchZipCode {
	

	public static class Map extends Mapper<LongWritable, Text, Text, Text>{
		//private final static IntWritable one = new IntWritable(1);
		//private Text word = new Text();  // type of output key 
		
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			String str = value.toString();
	    	String [] arr = str.split("::");
	    	String zipCode = arr[4].trim();
            String usedId = arr[0].trim();
            
	    	if(arr != null && !arr[0].isEmpty() && !arr[4].isEmpty() )
	    	{  
	            	context.write(new Text(zipCode), new Text(str));
	    	}
			
			
		}
	}

	public static class Reduce extends Reducer<Text,Text,Text,Text> {
		//private Text result = new Text();
		public void reduce(Text key, Iterable<Text> values,Context context ) throws IOException, InterruptedException {
			//int sum = 0; // initialize the sum for each keyword
			//String result;
			Configuration conf = context.getConfiguration();
	        String InputZip = conf.get("inputZipCode").toString();	
			
			for (Text val : values) {
				String []splitArray = val.toString().split("::");
	    		if(splitArray[4].equals(InputZip))
	    		{
	    			context.write(new Text(key),new Text(splitArray[0]));
	    		}
			}
			//result.set(sum);
			 // create a pair <keyword, number of occurences>
		}
	}

// Driver program
	public static void main(String[] args) throws Exception {
		 Configuration conf = new Configuration();
		    conf.set("inputZipCode", args[2]);
		 
		    Job job = new Job(conf, "ZipCode");
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(Text.class);
		    job.setJarByClass(SearchZipCode.class);
		    job.setMapperClass(Map.class);
		    job.setReducerClass(Reduce.class);
	  
		    job.setInputFormatClass(TextInputFormat.class);
		    job.setOutputFormatClass(TextOutputFormat.class);
		        
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    job.waitForCompletion(true);
	}
}