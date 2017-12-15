import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;


public class MatrixMultiplication {

    public static class CoocurrenceMatrixMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context) 
                throws IOException, InterruptedException {

            // input_format: movieB \t movieA=relation
            String[] key_value = value.toString().trim().split("\t");
            context.write(new Text(key_value[0]), new Text(key_value[1]));
        }
    }

    public static class RatingMatrixMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context) 
                throws IOException, InterruptedException {

            // input_format: userID \t movieID:rating
            String[] user_movieRating = value.toString().trim().split("\t");

            if (user_movieRating.length < 2) {
                String info = String.format(
                    "Warning: The length of user_movieRating array is %d. Requires at least 2. Origin line: %s", 
                    user_movieRating.length, value.toString());
                context.getCounter("Warnings", info);
                return;
            }

            String userID = user_movieRating[0];
            String movieID = user_movieRating[1].split(":")[0];
            String rating = user_movieRating[1].split(":")[1];

            context.write(new Text(movieID), new Text(userID + ':' + rating));
        }
    }

    public static class MultiplicationReducer extends Reducer<Text, Text, Text, DoubleWritable> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) 
                throws IOException, InterruptedException {

            Map<String, Double> relationMap = new HashMap<>();
            Map<String, Double> ratingMap = new HashMap<>();

            for (Text value : values) {
                if (value.toString().contains("=")) {
                    String[] movie_ralation = value.toString().trim().split("=");
                    relationMap.put(movie_ralation[0], Double.parseDouble(movie_ralation[1]));
                } else {
                    String[] user_rating = value.toString().trim().split(":");
                    ratingMap.put(user_rating[0], Double.parseDouble(user_rating[1]));
                }
            }

            for (String movie : relationMap.keySet()) {
                double relation = relationMap.get(movie);

                for (String user : ratingMap.keySet()) {
                    double rating = ratingMap.get(user);

                    String outputKey = user + ':' + movie;
                    double outputVal = relation * rating;

                    context.write(new Text(outputKey), new DoubleWritable(outputVal));
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setJarByClass(MatrixMultiplication.class);

        ChainMapper.addMapper(job, CoocurrenceMatrixMapper.class, LongWritable.class, Text.class, Text.class, Text.class, conf);
        ChainMapper.addMapper(job, RatingMatrixMapper.class, Text.class, Text.class, Text.class, Text.class, conf);

        job.setMapperClass(CoocurrenceMatrixMapper.class);
        job.setMapperClass(RatingMatrixMapper.class);

        job.setReducerClass(MultiplicationReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, CoocurrenceMatrixMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, RatingMatrixMapper.class);

        TextOutputFormat.setOutputPath(job, new Path(args[2]));

        job.waitForCompletion(true);
    }

}