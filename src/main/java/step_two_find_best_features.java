import java.io.IOException;
import java.util.HashSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;



import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class step_two_find_best_features {




    public static class MapperClass extends Mapper<LongWritable, Text, LongWritable, Text> {


        @Override
        protected void setup(Mapper<LongWritable, Text, LongWritable, Text>.Context context) throws IOException {
        }

        @Override
        public void map(LongWritable lineId, Text line, Context context) throws IOException,  InterruptedException {
            String [] feature_prob = line.toString().split("\\s+");
            long prob = Long.parseLong(feature_prob[1]);
            prob = prob * (-1);
            context.write(new LongWritable(prob), new Text(feature_prob[0]));
        }
    }



    public static class ReducerClass extends Reducer<LongWritable,Text,Text,Text> {
        int counter = 0;
        int index = 0;



        @Override
        protected void setup(Reducer<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        }

        @Override
        protected void reduce(LongWritable key, Iterable<Text> values, Reducer<LongWritable,Text,Text,Text>.Context context) throws IOException, InterruptedException {
//            String key_as_string = key.toString();
//            long prob = Long.parseLong(key_as_string);
//            prob = prob * (-1);
//            String prob_updated = String.valueOf(prob);


            String key_string = String.valueOf( key.get() * (-1)) ;

            for (Text value : values) {
                counter++;
                if(counter<101){
                    continue;
                }
                if(counter>1100){
                    break;
                }

                context.write(value, new Text(index + " " + key_string));
                index++;

            }

        }

    }

}



//    public static void main(String[] args) throws Exception {
//        Configuration conf = new Configuration();
//        Job job = Job.getInstance(conf, "step1_count_N_and_split_corpus");
//        job.setJarByClass(step1_count_N_and_split_corpus.class);
//        job.setMapperClass(step1_count_N_and_split_corpus.MapperClass.class);
//        job.setReducerClass(step1_count_N_and_split_corpus.ReducerClass.class);
//        job.setMapOutputKeyClass(Text.class);
//        job.setMapOutputValueClass(Text.class);
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(Text.class);
//        FileInputFormat.addInputPath(job, new Path(args[0]));
//        FileOutputFormat.setOutputPath(job, new Path(args[1]));
//        job.setInputFormatClass(TextInputFormat.class);
//        job.setOutputFormatClass(TextOutputFormat.class);
//        System.exit(job.waitForCompletion(true) ? 0 : 1);
//    }

