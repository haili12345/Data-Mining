import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Haili_Wang_Average {

	public static class MyMapper extends Mapper<Object, Text, Text, Text>{

		public String change(String event) {
			event = event.replaceAll("[-\']","").replaceAll("[^\\w]"," ").replaceAll("\\s+"," ").replaceAll("^\\s","").replaceAll("\\s$","").toLowerCase();
			return event.length()==0?null:event;
		}

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] l = value.toString().split(",");
			if(l[3].equals("event") || change(l[3])==null || l.length<19 || l[18].length()<=0) {
				return;
			}
			String result = "1    ".concat(new String(l[18]));
			context.write(new Text(change(l[3])),new Text(result));
		}
	}

	public static class MyReducer extends Reducer<Text,Text,Text,Text> {
		public void reduce(Text key, Iterable<Text> values, Context context ) throws IOException, InterruptedException {
			float s = 0;
			int c = 0;
			for (Text te : values) {
				String[] num = te.toString().split("    ");
				c += Integer.valueOf(num[0]);
				s += Math.round(Float.parseFloat(num[1]) * Integer.valueOf(num[0]));
			}
			context.write(key,new Text(Integer.toString(c) + "    " + Float.toString(s/c)));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("Usage: Haili_Wang_Average <in> [<in>...] <out>");
			System.exit(2);
		}
		Job job = Job.getInstance(conf, "Haili_Wang_Average");
		job.setJarByClass(Haili_Wang_Average.class);
		job.setMapperClass(MyMapper.class);
		job.setCombinerClass(MyReducer.class);
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		for (int i = 0; i < otherArgs.length - 1; ++i) {
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}
		FileOutputFormat.setOutputPath(job,new Path(otherArgs[otherArgs.length - 1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}