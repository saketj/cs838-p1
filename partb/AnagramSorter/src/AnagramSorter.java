import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
/**
 * 
 * @author Deepanker Aggarwal, Saket Saurabh
 * This job is done in 2 map-reduce phases. First phase involves TokenizerMapper and TokenizerReducer. Second phase is map only which involves SortMap. 
 * TokenizerMapper: creates <key, value> pairs according to input line where key is sorted representation of line and value is line itself. eg line = "hello" then key = "ehllo" and value ="hello"
 * TokenizerReducer: takes input from TokenizerMapper and creates an intermediate output of the form <-#ofwords> <word_1> <word_2> ....<word_#ofWords>
 * SortMap: takes reads the intermediate input and creates key value pairs of the form key = <-#ofwords> and value = <word_1> <word_2> ... <word_#ofWords>
 * There is no reducer for SortMap, output is sorted on the value of key by default which in our case is negative of number of words so output is generated in decreasing order of sizes.
 * As required output doesn't have key therefore we remove key using a custom output format class "ValueOutputFormat" which just outputs the value and skips the key.   
 */
public class AnagramSorter {
	
	public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {
		private Text word = new Text();
		private Text sortedWord = new Text();
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			char charWords[] = line.toCharArray();
			Arrays.sort(charWords);
			word.set(line);
			sortedWord.set(new String(charWords));
			context.write(sortedWord, word);
		}	
	}
	
	public static class TokenizerReducer extends Reducer<Text,Text,LongWritable,Text> {
		private Text resultValue = new Text();
		private LongWritable resultKey = new LongWritable();
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			StringBuffer sum = new StringBuffer();
			int ctr = 0;
			for (Text val : values) {
				sum.append(" ");
				sum.append(val.toString());
				ctr++;
			}
			resultKey.set(-1*ctr);
			resultValue.set(sum.toString());
			context.write(resultKey, resultValue);
		}
	}
	
	public static class SortMap extends Mapper<LongWritable,Text,LongWritable,Text>{
		LongWritable newKey = new LongWritable();
		Text newValue = new Text();
	   public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		   String line = value.toString();
		   String words[] = line.split(" ", 2);
		   newKey.set(Long.parseLong(words[0].trim()));
		   newValue.set(words[1]);
		   context.write(newKey, newValue);
	   }
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "AnagramSorter");
		job.setJarByClass(AnagramSorter.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setReducerClass(TokenizerReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		String outputTempDir = "Temp_Output";
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(outputTempDir));
		boolean success = job.waitForCompletion(true);
		if (success) {
		    Job job2 = Job.getInstance(conf, "JOB_2");
			job2.setJarByClass(AnagramSorter.class);
		    job2.setMapperClass(SortMap.class);
		    job2.setMapOutputKeyClass(LongWritable.class);
		    job2.setMapOutputValueClass(Text.class);
		    job2.setOutputFormatClass(ValueOutputFormat.class);
		    FileInputFormat.addInputPath(job2, new Path(outputTempDir));
		    FileOutputFormat.setOutputPath(job2, new Path(args[1]));
		    success = job2.waitForCompletion(true);
		}
		FileUtil.fullyDelete(new File(outputTempDir));
		System.exit(success ? 0 : 1);
	}
}