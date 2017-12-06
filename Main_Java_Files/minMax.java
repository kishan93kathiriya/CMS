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

public class minMax {

	public static class Map extends Mapper<Object, Text, IntWritable, Text>{
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			context.write(new IntWritable(1), value);
		}
	}
	public static class Reduce extends Reducer<IntWritable,Text,Text,NullWritable> {	  
		public void reduce(IntWritable key, Iterable<Text> values, Context context ) throws IOException, InterruptedException {
    		//find min and max for each column
    		ArrayList<Double> minValues = new ArrayList<Double>();
    		ArrayList<Double> maxValues = new ArrayList<Double>();
    		for (Text val : values) {
    			try{
					String[] columns = val.toString().split("\t");
					//iterate through columns
					for(int i =0;i<columns.length;i++){
						double value = Double.parseDouble(columns[i]);
						if(minValues.size()==columns.length){
							if(value < minValues.get(i)){
								minValues.set(i, value);
							}
							if(value > maxValues.get(i)){
								maxValues.set(i, value);
							}
						}
						else{
							minValues.add(value);
							maxValues.add(value);
						}
					}
         	  	}
				catch(Exception e){
					System.out.println("Warning: exception "+e.getStackTrace().toString());
				}
    		}
    			
    		//normalize values
    		//iterate through rows
    		String minValue = "";
    		String maxValue = "";
    		for(int i=0;i<minValues.size();i++){
    			minValue = minValue + minValues.get(i) + ",";
    			maxValue = maxValue + maxValues.get(i) + ",";
    		}
    		minValue = minValue.substring(0,minValue.length()-1);
    		maxValue = maxValue.substring(0,maxValue.length()-1);
			context.write(new Text(minValue),NullWritable.get());
			context.write(new Text(maxValue),NullWritable.get());
		}
	}
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "minmax");
		job.setJarByClass(minmax.class);
		job.setMapperClass(Map.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		job.setReducerClass(Reduce.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}

