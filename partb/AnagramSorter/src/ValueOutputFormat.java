import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ValueOutputFormat extends FileOutputFormat<LongWritable, Text> {
	protected static class ValueRecordWriter extends RecordWriter<LongWritable, Text> {
		private DataOutputStream out;
		
		public ValueRecordWriter(DataOutputStream out) throws IOException {
			this.out = out;
		}
		
		@Override
		public void close(TaskAttemptContext arg0) throws IOException, InterruptedException {
			out.close();
		}

		@Override
		public void write(LongWritable arg0, Text arg1) throws IOException, InterruptedException {
			out.writeBytes(arg1.toString()+"\n");
		}
	}
	@Override
	public RecordWriter<LongWritable, Text> getRecordWriter(TaskAttemptContext job)
			throws IOException, InterruptedException {
		String file_extension = ".txt";
		Path file = getDefaultWorkFile(job, file_extension);
		FileSystem fs = file.getFileSystem(job.getConfiguration());
		FSDataOutputStream fileOut = fs.create(file, false);
		return new ValueRecordWriter(fileOut);
	}

}