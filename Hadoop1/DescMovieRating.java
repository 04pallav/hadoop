import java.io.IOException;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Stack;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import java.util.*;
        
public class DescMovieRating {
        
 public static class Map extends Mapper<LongWritable, Text, Text, Text> {
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    	String str = value.toString();
    	String []arr = str.split("::");
    	if(arr != null && !arr[1].isEmpty() && !arr[2].isEmpty())
    	{
            String movieId = arr[1].trim();
            String rating = arr[2].trim();
            context.write(new Text(movieId), new Text(rating));
    	}
    }
 }
        
 public static class Reduce extends Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterable<Text> values, Context context) 
      throws IOException, InterruptedException {
        double sum = 0;
        double count = 0;
        for (Text val : values) {
            sum += Double.parseDouble(val.toString());
            count ++;
        }
        String average = Double.toString(sum / count);
        context.write(key, new Text(average));
    }
 }
 
 public static class MapForMovieRatings extends Mapper<LongWritable, Text, Text, Text> {
	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	    	//for (IntWritable val : value) {
	    	String line = value.toString();
	    	String movieId =  line.split("\t")[0].trim();
	    	String AverageRating = "_" + line.split("\t")[1].trim();
	    	String IdAndAvgRating = movieId + AverageRating;
	    	System.out.println(IdAndAvgRating);
	        context.write(new Text("red"), new Text(IdAndAvgRating));
	    	
	    }
	 }
 
 public static class Movie{
	 public String movieId;
	 public Double movieRatingAverage;
	 public Movie(){
		 
	 }
	 public Movie(Double movieRatingAverage,String movieId){
		 this.movieId= movieId;
		 this.movieRatingAverage= movieRatingAverage;
	 }
	
 }
 
 public static class ReduceMovieRatings extends Reducer<Text, Text, Text, Text> {
	    public void reduce(Text key, Iterable<Text> values, Context context)
	      throws IOException, InterruptedException {
	    	int TopSize = 10;
	    	int count = 0;
	    	Queue<Movie> myQueue = new PriorityQueue<Movie>(10,new Comparator<Movie>() {
	            public int compare(Movie patient1, Movie patient2) {
	                return ((patient1.movieRatingAverage > patient2.movieRatingAverage) ? -1: 1);
	            }
	        });
	    	Stack<Movie> myStack = new Stack<Movie>();
	    	for (Text val : values) 
	    	{
	    		Movie myObj = new Movie(Double.parseDouble(val.toString().split("_")[1]), val.toString().split("_")[0]);
	    		System.out.println("PEEKING : "+myObj.movieRatingAverage);
	    		myQueue.add(myObj);
	        	//context.write(key, new Text("TP"));
		    	
	        }
	    	
	    	
	    	while(count < TopSize) {
	    		Movie currentObj = myQueue.poll();
	    		System.out.println("PEEKING : "+currentObj.movieRatingAverage);
	            if(currentObj == null) {
	                break;
	            }
	            context.write(new Text(currentObj.movieId.toString()), new Text(currentObj.movieRatingAverage.toString()));
		        count++;
	        	  }
	    	
	    	
	    }
	 }
 
 public static void main(String[] args) throws Exception {
	 
    Configuration conf = new Configuration();        
    Job job = new Job(conf, "partcA");
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setJarByClass(DescMovieRating.class);
    job.setMapperClass(Map.class);
    job.setCombinerClass(Reduce.class);
    job.setReducerClass(Reduce.class);
        
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
        
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
    job.waitForCompletion(true);
    
    if(job.isSuccessful())
    {
	    Configuration conf2 = new Configuration();        
	    Job job2 = new Job(conf2, "partcB");
	    
	    job2.setOutputKeyClass(Text.class);
	    job2.setOutputValueClass(Text.class);
	    job2.setJarByClass(DescMovieRating.class);

	    job2.setMapperClass(MapForMovieRatings.class);
	   
	    job2.setReducerClass(ReduceMovieRatings.class);
	        
	    job2.setInputFormatClass(TextInputFormat.class);
	    job2.setOutputFormatClass(TextOutputFormat.class);
	        
	    FileInputFormat.addInputPath(job2, new Path(args[1]));
	    FileOutputFormat.setOutputPath(job2, new Path(args[2]));
	
	    job2.waitForCompletion(true);
    }  
 }      
}