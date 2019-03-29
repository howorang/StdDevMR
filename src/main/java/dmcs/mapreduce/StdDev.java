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
import java.util.Map;
import java.util.Objects;

public class StdDev {

    public static class Key {
        private final String side;
        private final int serie;
        private final String finger;

        public Key(String side, int serie, String finger) {
            this.side = side;
            this.serie = serie;
            this.finger = finger;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Key key = (Key) o;
            return serie == key.serie &&
                    Objects.equals(side, key.side) &&
                    Objects.equals(finger, key.finger);
        }

        @Override
        public int hashCode() {

            return Objects.hash(side, serie, finger);
        }
    }

    public static class JsonMapper extends Mapper<Object, Text, Key, DoubleWritable> {

        private static JsonParser parser = new JsonParser();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            JsonObject json = parser.parse(value.toString()).getAsJsonObject();
            String side = json.get("side").getAsString();
            int series = json.get("series").getAsInt();
            for (Map.Entry<String, JsonElement> field : json.get("features2D").getAsJsonObject().entrySet()) {
                String finger = field.getKey();
                double featureValue = field.getValue().getAsDouble();
                context.write(new Key(side, series, finger), new DoubleWritable(featureValue));
            }
        }
    }

    public static class StdDevReducer extends Reducer<Key, DoubleWritable, Text, DoubleWritable> {

        @Override
        public void reduce(Key key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double avg = 0;
            int count = 0 ;
            for (DoubleWritable val : values) {
                avg += val.get();
                count++;
            }
            avg = avg/count;

            double sum = 0;
            for (DoubleWritable val : values) {
                sum += Math.pow((val.get() - avg), 2);
            }
            context.write(new Text(key.side + "-" + key.serie + "-" + key.finger + " stdDev(1," + count + ")"), new DoubleWritable(Math.sqrt(sum/count)));
        }

    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "stdDev");
        job.setJarByClass(StdDev.class);
        job.setMapperClass(JsonMapper.class);
        job.setCombinerClass(StdDevReducer.class);
        job.setReducerClass(StdDevReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

