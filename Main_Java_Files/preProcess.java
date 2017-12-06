import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class preProcess {

	public static class Map extends Mapper<Object, Text, Text, NullWritable>{
	  
		private Text row = new Text();
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
     
			String[] features = value.toString().split("\t");
			String rowVal = "";
		 
			for(int i = 19;i<28;i++){
				try{
					rowVal = rowVal + features[i] + '\t';
				}
				catch(Exception e){}
			}
			row.set(rowVal);
			context.write(row,NullWritable.get());
		}
	}
	public static class Reduce extends Reducer<IntWritable,Text,Text,NullWritable> {
		public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    	
		}
    }
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "preprocess");
		job.setJarByClass(preprocess.class);
		job.setMapperClass(Map.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		job.setNumReduceTasks(0);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}


