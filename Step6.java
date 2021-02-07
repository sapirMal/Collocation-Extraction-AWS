	package testing2;

import java.io.IOException;
import java.util.Arrays;
import java.util.StringTokenizer;
 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Step6 { 
	public static class MapperClass extends Mapper<LongWritable, Text, Text,  Text> {
		private Text _key = new Text();
		private Text _val = new Text();
		
	    public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
	      StringTokenizer itr = new StringTokenizer(value.toString(), "\n"); 
	      while (itr.hasMoreTokens()) {
	        String[] parts = itr.nextToken().split("\t");  // <Y w1 w2 0/1> \t npmi -> npmi \t key
	        String[] key_parts = parts[0].split(" ");
	        
	        if(key_parts[3].equals("1")&& !parts[1].equals("NaN")) { // remove NaN values caused by division by 0
	        	
	        	_key.set(key_parts[0]+" "+parts[1]);
	        	_val.set(key_parts[1]+" "+key_parts[2]);
	        	
	        	context.write(_key, _val);
	        }
	      }
	    }
	  }
 
  public static class ReducerClass extends Reducer<Text, Text, Text, DoubleWritable> {
	  private Text _key = new Text();
	  private DoubleWritable _val = new DoubleWritable();
	  private String space = " ";
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,  InterruptedException {
    	for(Text val: values) {
    		String[] parts_oldK = key.toString().split(" ");
    		
    		_key.set(parts_oldK[0]+space+val.toString());
    		_val.set(Double.valueOf(parts_oldK[1]));
    		context.write(_key, _val);
    	}
    	
    }
 }
 

 
    public static class PartitionerClass extends Partitioner<Text, Text> {
    	private Text to_hash = new Text();
      @Override
      public int getPartition(Text key, Text value, int numPartitions) {
    	String[] parts = key.toString().split(" "); // (Y npmi) , (w1 w2)
    	to_hash.set(parts[0]);
        return to_hash.hashCode() % numPartitions; 
      }    
    }
 
    
    public static class TextComparator2 extends WritableComparator{
    	protected TextComparator2() {
    		super(Text.class, true);
    	}
    	
    	public int compare(WritableComparable w1, WritableComparable w2) {
    		Text k1 = (Text)w1;
    		Text k2 = (Text)w2;
    		
    		String[] w1_parts = k1.toString().split(" ");
    		String[] w2_parts = k2.toString().split(" ");
    		
    		int w1_y = Integer.valueOf(w1_parts[0]);
    		double w1_npmi = Double.valueOf(w1_parts[1]);
    		int w2_y = Integer.valueOf(w2_parts[0]);
    		double w2_npmi = Double.valueOf(w2_parts[1]);
    		// if w1 < w2 : -1
    		// if w1 = w2 : 0
    		// if w1 > w2 : 1
    		if(w1_y > w2_y) {
    			return 1;
    		}
    		else if(w1_y < w2_y) {
    			return -1;
    		}
    		else { // compare to enable descending npmi order
    			if(w1_npmi > w2_npmi) {
    				return -1;
    			}
    			else {// if(w1_npmi =< w2_npmi) {
    				return 1;
    			}
    		}
    	}
    		
    		
    		
    }
    
    
    
    public static void main(String[] args) throws Exception {// args[4] = {inputPath, outputFolder,minPmi, relMinPmi}
        Configuration conf = new Configuration();
        Job job = new Job(conf, " Step 6");
        job.setJarByClass(Step6.class);
        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(PartitionerClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setSortComparatorClass(TextComparator2.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
      }
}