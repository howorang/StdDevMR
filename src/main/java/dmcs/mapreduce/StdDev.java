package dmcs.mapreduce;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class StdDev {

        public static class JsonMapper extends Mapper<Object, Text, Text, DoubleWritable> {

            private static JsonParser parser = new JsonParser();

            @Override
            public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
                JsonObject json = parser.parse(value.toString()).getAsJsonObject();
                String side = json.get("side").getAsString();
                int series = json.get("series").getAsInt();
                for (Map.Entry<String, JsonElement> field : json.get("features2D").getAsJsonObject().entrySet()) {
                    String finger = field.getKey();
                    double featureValue = field.getValue().getAsDouble();
                    context.write(new Text(side + "-" + series + "-" + finger), new DoubleWritable(featureValue));
                }
            }
        }

        public static class StdDevReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

            @Override
            public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
                double avg = 0;
                int count = 0;
                List<Double> values_ = new ArrayList<>();
                values.forEach(p -> values_.add(p.get()));
                for (Double val : values_) {
                    avg += val;
                    count++;
                }
                avg = avg / count;


                double sum = 0;
                for (Double val : values_) {
                    sum += Math.pow((val - avg), 2);
                }
                double stdDev = Math.sqrt(sum / count);
                context.write(new Text(key.toString() + " stdDev(1," + count + ")"), new DoubleWritable(stdDev));
            }

        }

        public static void main(String[] args) throws Exception {
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "stdDev");
            job.setJarByClass(StdDev.class);
            job.setMapperClass(JsonMapper.class);
            job.setReducerClass(StdDevReducer.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(DoubleWritable.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(DoubleWritable.class);

            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        }
}

