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

  
public class Step4 { 
    static final String omega = "\uFFFF";
    static final String nil = "\u0000";
    static final String space = " ";
    static final String first = space+nil+space+nil;
    
	public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {
	    public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
	      StringTokenizer itr = new StringTokenizer(value.toString(), "\n"); 
	      while (itr.hasMoreTokens()) {
	        String[] parts = itr.nextToken().split("\t");
	        //System.out.println(">>>>>>>>>>>>>>"+parts[1]);
	        context.write(new Text(parts[0]), new Text(parts[1]));
	      }
	    }
	  }
 
  public static class ReducerClass extends Reducer<Text,Text,Text,DoubleWritable> {
	  private double N = -1;
	  private double cw1 = -1;
	  private double cw2 = -1;
	  private double cw1w2 = -1;
	  private double pw1w2, pmi, npmi, sum = 0;
	  private Text _key = new Text();
	  private DoubleWritable _val = new DoubleWritable();
	  
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,  InterruptedException {
    	String[] parts = key.toString().split(" ");  // <Y w1 w2>
    	double minPmi = Double.valueOf(context.getConfiguration().get("minPmi", "-1"));
    	if(parts[1].equals(omega)) { // sends sum npmi 
    		context.write(new Text(parts[0]+first), new DoubleWritable(sum));
    		sum = 0;
    			
    	}
    	else {
	    	for(Text val: values) {
    			String[] val_parts = val.toString().split(" ");
	    		//if(parts[1].equals(".") && parts[2].equals(".")) { 
    			if(val_parts.length == 1) {
	    			N = Double.valueOf(val.toString());
	    			return;
	    		}
		    		else {
		    			//val_parts[0] = tag, val_parts[1] = #
		    			switch(Integer.valueOf(val_parts[0])) {
			    			case 0:
			    				cw1w2 = Double.valueOf(val_parts[1]);
			    				break;
			    			case 1:
			    				cw1 = Double.valueOf(val_parts[1]);
			    				break;
			    			case 2:
			    				cw2 = Double.valueOf(val_parts[1]);
			    				break;
		    			}
		    		}
		    	}
    			
		    	pw1w2 = cw1w2 / N;
		    	if(cw1w2 + N == cw1 + cw2) {
		    		pmi = 0;
		    	}
		    	else{
		    		pmi = Math.log(cw1w2) + Math.log(N) - Math.log(cw1) - Math.log(cw2);
		    	}
		    	if((-1 * Math.log(pw1w2) == 0)) {
		    		npmi = 0;
		    	}
		    	else {
		    		npmi = pmi / (-1 * Math.log(pw1w2));
		    	}
		    	sum += npmi;
	    		_val.set(npmi);
		    	if(npmi < minPmi) {
		    		_key.set(key.toString() + " 0");
		    	}
		    	else {
		    		_key.set(key.toString() + " 1");
		    	}
		    	context.write(_key, _val);
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
        conf.set("minPmi", args[3]);
        conf.set("minRelPmi",  args[4]);
        
        Job job = new Job(conf, " Step 4");
        job.setJarByClass(Step4.class);	
        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(PartitionerClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
      }
}