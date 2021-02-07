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
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

  
public class Step5 { 
    static final String omega = "\uFFFF";
    static final String nil = "\u0000";
    static final String space = " ";
    static final String first = space+nil+space+nil;
	
	public static class MapperClass extends Mapper<LongWritable, Text, Text, DoubleWritable> {
	    public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
	      StringTokenizer itr = new StringTokenizer(value.toString(), "\n"); 
	      while (itr.hasMoreTokens()) {
	        String[] parts = itr.nextToken().split("\t");  // <Y w1 w2 0/1> \t npmi
	        context.write(new Text(parts[0]), new DoubleWritable(Double.valueOf(parts[1])));
	      }
	    }
	  }
 
  public static class ReducerClass extends Reducer<Text,DoubleWritable,Text,DoubleWritable> {
	  private double sum = 0;
	  private DoubleWritable _val = new DoubleWritable();
	  private Text _key = new Text();
	  
    public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException,  InterruptedException {
    	double relMinPmi = Double.valueOf(context.getConfiguration().get("minRelPmi", "-1"));
    	System.out.println("GOT K: "+key.toString()+" V: "+values.toString());
    	String[] parts = key.toString().split(" ");  // <Y w1 w2 0/1> 
    	for(DoubleWritable val: values) {
    		if(parts[1].equals(nil) && parts[2].equals(nil)) {
    			sum = val.get();
    			return;
    		}
    		else if(parts[3].equals("1")) {
    			//context.write(key, new DoubleWritable(0));
    			//context.write(new Text("SUM IS "), new DoubleWritable(sum));
    			context.write(key, val);
        	}
        	else {
        		// To check: relminpmi <= npmi / sum ? 1 : 0
        		DoubleWritable div = new DoubleWritable(val.get() / sum);
        		if (sum == 0) {
        			//context.write(key, new DoubleWritable(1));
        			context.write(key, val);
        		}
        		else if(relMinPmi > (val.get() / sum)) {
        			//context.write(key, new DoubleWritable(2));
            		context.write(key, val);
        		}
        		else {
        			_key.set(parts[0]+" "+parts[1]+" "+parts[2]+" 1");
        			context.write(_key, val);
        			//context.write(key, val);
        			//context.write(key, div);
        			//context.write(key, new DoubleWritable(3));
        			//context.write(_key, new DoubleWritable(relMinPmi));
        		}
        	}
    	}
    	}
   }
 

 
    public static class PartitionerClass extends Partitioner<Text, DoubleWritable> {
    	private Text to_hash = new Text();
      @Override
      public int getPartition(Text key, DoubleWritable value, int numPartitions) {
    	String[] parts = key.toString().split(" "); // (Y w1 w2)
    	to_hash.set(parts[0]);
        return to_hash.hashCode() % numPartitions; // Partition by Y
      }    
    }
 
    public static void main(String[] args) throws Exception {// args[4] = {inputPath, outputFolder,minPmi, relMinPmi}
        Configuration conf = new Configuration();
        conf.set("minPmi", args[3]);
        conf.set("minRelPmi",  args[4]);
        
   	 
   	 
   	 System.out.println(">>>>> args[1] - Input Path = "+args[1]);
   	 System.out.println(">>>>> args[2] - Output Path = "+args[2]);
   	 System.out.println(">>>>> args[3] - min PMI = "+args[3]);
   	 System.out.println(">>>>> args[4] - relative min PMI = "+args[4]);
   	 
   	 
   	 
        Job job = new Job(conf, " Step 5");
        job.setJarByClass(Step5.class);
        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(PartitionerClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
      }
}