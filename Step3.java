	package testing2;

import java.io.IOException;
import java.util.Arrays;
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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

  
public class Step3 { 
    static final String omega = "\uFFFF";
	public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {
		private Text _key = new Text();
	    public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
	      StringTokenizer itr = new StringTokenizer(value.toString(), "\n"); 
	      while (itr.hasMoreTokens()) {
	        String[] parts = itr.nextToken().split("\t");
	        if(parts[1].indexOf(" ") > 0) {
	        	_key.set(parts[0]+" a");
	        }
	        else {
	        	_key.set(parts[0]+" 0");
	        }
	        context.write(_key, new Text(parts[1]));
	      }
	    }
	  }
 
  public static class ReducerClass extends Reducer<Text,Text,Text,Text> {
	  private String decade = "";
	  private Text _key = new Text();
	  private Text x1 = new Text();
	  private Text x2 = new Text();
	  
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,  InterruptedException {
    	String[] parts = key.toString().split(" ");  // <Y w1 w2 tag trash>
    	if(!decade.equals(parts[0])){
		  decade = parts[0];
		  context.write(new Text(decade+" "+omega+" "+omega), new Text("dontcare"));
    	}

    	for(Text val: values) {
        	if(parts[1].equals(".") && parts[2].equals(".")&&parts[3].equals(".")) {
        		_key.set(parts[0]+" . .");
        		context.write(_key, val);
        	}
        	else if (parts[3].equals("1")) {
        		if(val.toString().indexOf(" ") < 0) { // val = #
        			x1.set("1 "+val.toString());
        		}
        		else { // val = w1 w2
        			_key.set(decade+" "+val.toString());
        			context.write(_key, x1); // < Y w1 w2 >, 1 #
        		}
        	}
        	else if(parts[3].equals("2")) {
        		if(val.toString().indexOf(" ") < 0) { // val = #
        			x2.set("2 "+val.toString());
        		}
        		else { // val = w1 w2
        			_key.set(decade+" "+val.toString());
        			context.write(_key, x2); // < Y w1 w2 >, 2 #
        		}
        	}
        	else { // <Y w1 w2 0>, #
        		_key.set(parts[0]+" "+parts[1]+" "+parts[2]);
        		context.write(_key, new Text("0 "+val.toString())); // <Y w1 w2>, 0 #
        	}
        	
    	}
    }
  }
 
    public static class PartitionerClass extends Partitioner<Text, Text> {
    	private Text to_hash = new Text();
      @Override
      public int getPartition(Text key, Text value, int numPartitions) {
    	String[] parts = key.toString().split(" "); // (Y w1 w2)
    	to_hash.set(parts[0]);
        return to_hash.hashCode() % numPartitions; // Partition by Y
      }    
    }
 
    public static void main(String[] args) throws Exception {// args[4] = {inputPath, outputFolder,minPmi, relMinPmi}
        Configuration conf = new Configuration();
        Job job = new Job(conf, " Step 3");
        job.setJarByClass(Step3.class);
        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(PartitionerClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
      }
}