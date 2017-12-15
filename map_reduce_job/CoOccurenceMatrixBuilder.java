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


public class CoOccurenceMatrixBuilder {

    public static class MatrixBuidlerMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        @Override
        public void map(LongWritable key, Text value, Context context) 
                throws IOException, InterruptedException {

            // input_format: userID \t movie1:rating,movie2:rating,...
            String line = value.toString().trim();
            String[] user_movieRatings = line.split("\t");
            String[] movie_ratings = user_movieRatings[1].split(",");

            for (int i = 0; i < movie_ratings.length; ++i) {
                String movie1 = movie_ratings[i].trim().split(":")[0];

                for (int j = 0; j < movie_ratings.length; ++j) {
                    String movie2 = movie_ratings[j].trim().split(":")[0];
                    context.write(new Text(movie1 + ':' + movie2), new IntWritable(1));
                }
            }
        }
    }

    public static class MatrixBuilderReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) 
                throws IOException, InterruptedException {

            // key_format:  movie1:movie2
            // value_format:  Iterable<1, 1, 1, ...>
            int count = 0;
            for (IntWritable value : values) {
                count += value.get();
            }
            context.write(key, new IntWritable(count));
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setMapperClass(MatrixBuidlerMapper.class);
        job.setReducerClass(MatrixBuilderReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setJarByClass(CoOccurenceMatrixBuilder.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        TextInputFormat.setInputPaths(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }

}