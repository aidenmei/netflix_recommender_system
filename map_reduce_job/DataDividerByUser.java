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


public class DataDividerByUser {

    public static class DataDividerMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

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

            int userID = Integer.parseInt(user_movie_rating[0]);
            String movieID = user_movie_rating[1];
            String rating = user_movie_rating[2];

            context.write(new IntWritable(userID), new Text(movieID + ':' + rating));
        }
    }

    public static class DataDividerReducer extends Reducer<IntWritable, Text, IntWritable, Text> {

        @Override
        public void reduce(IntWritable key, Iterable<Text> values, Context context) 
                throws IOException, InterruptedException {

            StringBuilder sb = new StringBuilder();

            for (Text value : values) {
                sb.append(value.toString() + ',');
            }
            sb.deleteCharAt(sb.length() - 1);

            context.write(key, new Text(sb.toString()));
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setMapperClass(DataDividerMapper.class);
        job.setReducerClass(DataDividerReducer.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        job.setJarByClass(DataDividerByUser.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        TextInputFormat.setInputPaths(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }

}