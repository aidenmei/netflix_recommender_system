import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;


public class Normalize {

    public static class NormalizeMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context) 
                throws IOException, InterruptedException {

            // input_format: movieA:movieB \t relation
            String[] movies_relation = value.toString().trim().split("\t");

            String[] movies = movies_relation[0].split(":");
            String relation = movies_relation[1];

            context.write(new Text(movies[0]), new Text(movies[1] + '=' + relation));
        }
    }

    public static class NormalizeReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) 
                throws IOException, InterruptedException {

            int sum = 0;
            Map<String, Integer> map = new HashMap<>();

            for (Text value : values) {
                String[] movie_relation = value.toString().trim().split("=");
                int relation = Integer.parseInt(movie_relation[1]);

                sum += relation;
                map.put(movie_relation[0], relation);
            }

            for (String movie : map.keySet()) {
                double relative_relation = ((double) map.get(movie)) / sum;
                String outputValue = key.toString() + '=' + relative_relation;

                context.write(new Text(movie), new Text(outputValue));
            }
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setMapperClass(NormalizeMapper.class);
        job.setReducerClass(NormalizeReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setJarByClass(Normalize.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        TextInputFormat.setInputPaths(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }

}