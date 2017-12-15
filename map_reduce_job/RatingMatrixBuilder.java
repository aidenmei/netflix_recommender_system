import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;


public class RatingMatrixBuilder {

    public static class UserMovieRatingMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context) 
                throws IOException, InterruptedException {

            // input_format: userID \t movie1:rating,movie2:rating,...,avgRating
            context.write(new IntWritable(123), value);
        }
    }

    public static class MovieListMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context) 
                throws IOException, InterruptedException {

            // input_format: movieID \t <END>
            String movieID = value.toString().trim().split("\t")[0];
            context.write(new IntWritable(123), new Text(movieID));
        }
    }

    public static class MatrixBuilderReducer extends Reducer<IntWritable, Text, Text, Text> {

        @Override
        public void reduce(IntWritable key, Iterable<Text> values, Context context) 
                throws IOException, InterruptedException {

            Map<String, String> map = new HashMap<>();
            List<String> movieList = new ArrayList<>();

            for (Text value : values) {
                if (value.toString().contains("\t")) {
                    String[] user_movieRating = value.toString().trim().split("\t");
                    map.put(user_movieRating[0], user_movieRating[1]);
                } else {
                    movieList.add(value.toString().trim());
                }
            }

            for (String userID : map.keySet()) {
                String[] movieRatings = map.get(userID).split(",");
                double avgRating = Double.parseDouble(movieRatings[movieRatings.length - 1]);

                Set<String> set = new HashSet<>();
                for (int i = 0; i < movieRatings.length - 1; ++i) {
                    set.add(movieRatings[i].split(":")[0]);
                    context.write(new Text(userID), new Text(movieRatings[i]));
                }

                for (String movie : movieList) {
                    if (!set.contains(movie)) {
                        context.write(new Text(userID), new Text(movie + ':' + avgRating));
                    }
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setJarByClass(RatingMatrixBuilder.class);

        ChainMapper.addMapper(job, UserMovieRatingMapper.class, LongWritable.class, Text.class, IntWritable.class, Text.class, conf);
        ChainMapper.addMapper(job, MovieListMapper.class, IntWritable.class, Text.class, IntWritable.class, Text.class, conf);

        job.setMapperClass(UserMovieRatingMapper.class);
        job.setMapperClass(MovieListMapper.class);

        job.setReducerClass(MatrixBuilderReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, UserMovieRatingMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, MovieListMapper.class);

        TextOutputFormat.setOutputPath(job, new Path(args[2]));

        job.waitForCompletion(true);
    }

}