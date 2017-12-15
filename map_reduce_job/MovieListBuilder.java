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
import java.util.HashSet;
import java.util.Set;


public class MovieListBuilder {

    public static class MovieListMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context) 
                throws IOException, InterruptedException {

            // input_format: user,movie,rating
            String[] user_movie_rating = value.toString().trim().split(",");

            if (user_movie_rating.length < 3) {
                String info = String.format(
                    "Warning: The length of user_movie_rating array is %d. Requires at least 3. Origin line: %s", 
                    user_movie_rating.length, value.toString());
                context.getCounter("Warnings", info);
                return;
            }

            String movieID = user_movie_rating[1];

            context.write(new IntWritable(123), new Text(movieID));
        }
    }

    public static class MovieListReducer extends Reducer<IntWritable, Text, Text, Text> {

        @Override
        public void reduce(IntWritable key, Iterable<Text> values, Context context) 
                throws IOException, InterruptedException {

            Set<String> set = new HashSet<>();

            for (Text movie : values) {
                set.add(movie.toString());
            }

            for (String movieID : set) {
                context.write(new Text(movieID), new Text("<END>"));
            }
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setMapperClass(MovieListMapper.class);
        job.setReducerClass(MovieListReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setJarByClass(MovieListBuilder.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        TextInputFormat.setInputPaths(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }

}