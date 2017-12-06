import java.io.*;
import java.util.*;
import java.lang.Math.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.filecache.*;


public class kMeansMedicare extends Configured implements Tool {
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> 
    {
		private	ArrayList<ArrayList<Double>> clusters = new ArrayList<ArrayList<Double>>();		
    	private ArrayList<Double> min_values = new ArrayList<Double>();
    	private ArrayList<Double> max_values = new ArrayList<Double>();
    	
		@Override
		public void configure(JobConf job)
		{			
			Path[] localFiles = new Path[0];
			try{
			localFiles = DistributedCache.getLocalCacheFiles(job);
			BufferedReader fileIn = new BufferedReader (new FileReader(localFiles[0].toString()));
			String line;
			while((line = fileIn.readLine()) != null)
				{
					ArrayList<Double> cluster = new ArrayList<Double>();
					String[] tokens = line.split("\\s+");
					for (int i = 0; i<tokens.length -1 ; i++)
					{
						cluster.add(Double.parseDouble(tokens[i].trim()));
					}
					clusters.add(cluster);
				}				
			fileIn.close();	
			}
			catch(IOException ioe) {
				System.err.println("Caught exception");
			}
		}
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException 
		{	    
	    	String[] columns = value.toString().trim().split("\\s+");
	    	ArrayList<Double> values = new ArrayList<Double>();
	    	int[] col_req = {1,3};
	    	for(int i = 0; i < col_req.length;i++)
	    	{
	    		values.add(Double.parseDouble(columns[col_req[i]]));
	    	}
	    	double least_distance = 0;
	    	int best_cluster = 0;
			for(int i=0; i<clusters.size(); i++)
			{
				double distance=0;
				ArrayList<Double> centroid = new ArrayList<Double>(clusters.get(i));
				for(int j=0; j<values.size(); j++)
				{	    						
					double dist_centroid = centroid.get(j) - values.get(j);
					double sqr_dist = dist_centroid*dist_centroid;
					distance = distance + sqr_dist;
				}
				distance = Math.sqrt(distance);
				if(i == 0)
				{
					least_distance = distance;
					best_cluster = 1;
				}
				else
				{
					if(distance < least_distance)
					{
						least_distance = distance;
						best_cluster = i+1;
					}
				}
			}
			String cluster = Integer.toString(best_cluster);
			String stringValue = Double.toString(values.get(0));
			for (int i=1; i<values.size(); i++)
			{
				stringValue = stringValue+ "	"+ Double.toString(values.get(i)).trim();	    			
			}				
			output.collect(new Text(cluster), new Text(stringValue));
		}
	}
	public static class Reduce extends MapReduceBase implements Reducer<Text, Text, NullWritable, Text> 
	{
		public void reduce(Text key, Iterator<Text> values, OutputCollector<NullWritable, Text> output, Reporter reporter) throws IOException 
		{
			ArrayList<Double> sum = new ArrayList<Double>();
	    	double count = 0;
			while (values.hasNext()) 
	    	{
	    		ArrayList<Double> row_values = new ArrayList<Double>();
				String line = values.next().toString();
	    		String [] token = line.split("\\,|\\s+|\\t+");
				for (int i=0; i<token.length; i++)
				{
					row_values.add(Double.parseDouble(token[i].trim()));
				}
	    		if (count == 0)
	    		{	
					for(int i=0; i<row_values.size(); i++)
					{
						sum.add(row_values.get(i));
					}
	    		}
	    		else
	    		{
					for(int i=0; i<row_values.size(); i++)
					{
						double newSum = sum.get(i)+row_values.get(i);
						sum.set(i, newSum);
					}	
				}
	    		count++;
			}
	    	ArrayList<Double> centroid_updated = new ArrayList<Double>();
			for(int i=0; i<sum.size(); i++)
			{
				Double newValue = (sum.get(i))/count;
				centroid_updated.add(newValue);
			}
			String centroid_string = centroid_updated.get(0).toString();
			for(int i=1; i<centroid_updated.size(); i++)
			{
				centroid_string= centroid_string+"	"+centroid_updated.get(i).toString();
			}
			centroid_string = centroid_string + "	" + count;
			NullWritable key_null = NullWritable.get();
	    	output.collect(key_null, new Text(centroid_string));
		}
 	}
	public int run(String[] args) throws Exception  
    {
		JobConf conf = new JobConf(getConf(), k_means_medicare.class); ///
		conf.setJobName("k_means_medicare");
		DistributedCache.addCacheFile(new Path("med_centroids.txt").toUri(), conf);
		conf.setMapOutputKeyClass(Text.class);
	    conf.setMapOutputValueClass(Text.class);
		conf.setOutputKeyClass(NullWritable.class);
		conf.setOutputValueClass(Text.class); ///
		conf.setMapperClass(Map.class);
		conf.setReducerClass(Reduce.class);
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		JobClient.runJob(conf);
		return 0;
    }
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new k_means_medicare(), args);
		System.exit(res);
	}
}
