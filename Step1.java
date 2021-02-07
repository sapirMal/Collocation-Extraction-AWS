package testing2;

import java.io.IOException;
 
 
 
import java.util.StringTokenizer;
 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Step1 {
	 //static final String path = "s3://"
	//static final String path = "s3://dsps192ass2-sy/input/";
	public static class MapperClass extends Mapper<LongWritable, Text, Text, IntWritable> {
	    //private final static IntWritable one = new IntWritable(1);
	    private Text _key = new Text(); // <(Year/10) <w1 w2>>
	    private IntWritable val = new IntWritable();
	    @Override
	    public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
	      StringTokenizer itr = new StringTokenizer(value.toString(), "\n"); 
	      
	      while (itr.hasMoreTokens()) {

	        String[] parts = itr.nextToken().split("\t"); 
	        _key.set(String.valueOf(Integer.valueOf(parts[1]) / 10) + " "+parts[0]);
	        val.set(Integer.valueOf(parts[2]));

	        context.write(_key, val);
	        
	        
	      }
	    }
	  }
 
  public static class ReducerClass extends Reducer<Text,IntWritable,Text,IntWritable> {
    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,  InterruptedException {
      int sum = 0;
      for (IntWritable value : values) {
        sum += value.get();
      }
      context.write(key, new IntWritable(sum)); 
    }
  }

  //step2
  public static class PartitionerClass extends Partitioner<Text, IntWritable> {
  	private Text _key = new Text();
    @Override
    public int getPartition(Text key, IntWritable value, int numPartitions) {
  	String[] parts = key.toString().split(" "); // (Y w1 w2)
  	_key.set(parts[0]);
      return _key.hashCode() % numPartitions; // Partition by Y
    }    
  }
 public static void main(String[] args) throws Exception { // args[4] = {NULL, inputPath, outputFolder,minPmi, relMinPmi}
	 
	 
	 System.out.println(">>>>> PRINTS:");
	 
	 
	 System.out.println(">>>>> args[1] - Input Path = "+args[1]);
	 System.out.println(">>>>> args[2] - Output Path = "+args[2]);
	 System.out.println(">>>>> args[3] - min PMI = "+args[3]);
	 System.out.println(">>>>> args[4] - relative min PMI = "+args[4]);
	 
	 
    Configuration conf = new Configuration();
    Job job = new Job(conf, " Step 1");
    job.setJarByClass(Step1.class);
    job.setMapperClass(MapperClass.class);
    job.setPartitionerClass(PartitionerClass.class);
    job.setCombinerClass(ReducerClass.class);
    job.setReducerClass(ReducerClass.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    
    FileInputFormat.addInputPath(job, new Path(args[1]));
    //job.setInputFormatClass(SequenceFileInputFormat.class);
    FileOutputFormat.setOutputPath(job, new Path(args[2]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}