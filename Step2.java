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
  
public class Step2 { 
    static final String omega = "\uFFFF";
	public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {
	    private Text _key = new Text(); 
	    private Text _2gram = new Text();
	    private Text val = new Text();
	    private String w1, w2, Y, space = " ";

	    
	    @Override
	    public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
	    	System.out.println("MAPPER TEXT IS: ");
	    	System.out.println(value.toString());
	      StringTokenizer itr = new StringTokenizer(value.toString(), "\n"); 
	      while (itr.hasMoreTokens()) {

	        String[] parts = itr.nextToken().split("\t");
	        String[] key_parts = parts[0].split(" ");
	        if(key_parts.length == 3) {
		        val.set(parts[1]);
		        Y = key_parts[0];
		        w1 = key_parts[1];
		        w2 = key_parts[2];
		        _2gram.set(w1+space+w2);
		       
		        // (<Y w1 1 1>, #)
		        _key.set(Y+space+w1+space+"1 1");
		        context.write(_key, val);
		        // (<Y w1 1 1>, w1 w2)
		        context.write(_key, _2gram);
		        // (<Y w2 2 2>, #)
		        _key.set(Y+space+w2+space+"2 2");
		        context.write(_key, val);
		        // (<Y w2 2 2>, w1 w2)
		        context.write(_key, _2gram);
		        // (<Y w1 w2 0>, #)
		        _key.set(Y+space+w1+space+w2+" 0");
		        context.write(_key, val);
		        // (<Y end end>, #)
		        _key.set(Y+space+omega+space+omega+space+omega);
		        //val.set(omega+space+val.toString());
		        context.write(_key, val);
	        }
	      }
	    }
	  }
 
  public static class ReducerClass extends Reducer<Text,Text,Text,Text> {
	private Text decade = new Text("");
	  
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,  InterruptedException {
    	
    	int sum = 0;
    	String[] parts = key.toString().split(" ");
    	if(!decade.toString().equals(parts[0])){
		  decade.set(parts[0]);
	  }
	  
	  for (Text value : values) {
		  if(value.toString().indexOf(" ") < 0) {
			  sum += Integer.valueOf(value.toString());
		  }
		  else {
			  context.write(key, value);
		  }
	 	}
	  if(key.toString().indexOf(omega) > 0) {
		  key.set(decade.toString()+" . . .");
	  }
	 context.write(key, new Text(String.valueOf(sum)));
    }
    
  }
 
    public static class PartitionerClass extends Partitioner<Text, Text> {
    	private Text _key = new Text();
      @Override
      public int getPartition(Text key, Text value, int numPartitions) {
    	String[] parts = key.toString().split(" "); // (Y w1 w2)
    	_key.set(parts[0]);
        return _key.hashCode() % numPartitions; // Partition by Y
      }    
    }
 
 public static void main(String[] args) throws Exception {// args[4] = {inputPath, outputFolder,minPmi, relMinPmi}
    Configuration conf = new Configuration();
    Job job = new Job(conf, " Step 2");
    job.setJarByClass(Step2.class);
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