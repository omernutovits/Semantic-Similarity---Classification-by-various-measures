import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;


import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

public class step_three_make_vector_4000 {
    public static Counter count_big_F;

    public static class DoubleArrayWritable extends ArrayWritable {
        public DoubleArrayWritable() {
            super(DoubleWritable.class);
        }

        public DoubleArrayWritable(Double[] doubles) {
            super(DoubleWritable.class);
            DoubleWritable[] doubleWritables = new DoubleWritable[doubles.length];
            for (int i = 0; i < doubles.length; i++) {
                doubleWritables[i] = new DoubleWritable(doubles[i]);
            }
            set(doubleWritables);
        }
    }





    public static class MapperClass_count_of_l extends Mapper<LongWritable, Text, Text, Text> {


        @Override
        protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException {
        }

        @Override
        public void map(LongWritable lineId, Text line, Context context) throws IOException,  InterruptedException {
            String [] lexema_count = line.toString().split("\\s+");
            context.write(new Text(lexema_count[0] + " xxxxxx "), new Text(lexema_count[1]));
        }
    }



    public static class MapperClass_count_of_l_and_f extends Mapper<LongWritable, Text, Text, Text> {


        @Override
        protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException {
        }

        @Override
        public void map(LongWritable lineId, Text line, Context context) throws IOException,  InterruptedException {
            String [] lexema_count = line.toString().split("\\s+");
            context.write(new Text(lexema_count[0] + " wwwwww " + lexema_count[1] ), new Text(lexema_count[2]));
        }
    }

    public static class ReducerClass extends Reducer<Text,Text,Text,Text> {
        Map <String,String> best_features_index_count_f = new HashMap<>();
        Double [] vector;
        Integer count_of_l = null;
        String last_round = null;



        @Override
        protected void setup(Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
//            TODO upload best feature
            vector = new Double[4000];
            best_features_index_count_f = gold_standard_maker.create_feature_map();
//            Arrays.fill(vector,	Double.NEGATIVE_INFINITY);
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text,Text,Text,Text>.Context context) throws IOException, InterruptedException {
            count_big_F = context.getCounter(steps.NCounter.N_COUNTER);
            double count_big_F_as_double = count_big_F.getValue();



            String [] key_as_string = key.toString().split("\\s+");

            last_round = key_as_string[0];
//            update lexema, and write
            if(key_as_string[1].equals("xxxxxx")){
                if(count_of_l != null){
                    StringBuilder stringBuilder = new StringBuilder();
                    for (double d : vector){
                        stringBuilder.append(d);
                        stringBuilder.append(" ");
                    }
                    String to_write = stringBuilder.toString();

                    context.write(new Text(last_round), new Text(to_write.substring(0,to_write.length()-1))  );
                    vector = new Double[4000];
//                    Arrays.fill(vector,	Double.NEGATIVE_INFINITY);
                }
                for (Text value : values) {
                    count_of_l = Integer.parseInt(value.toString());
                }

            }
//            update vector accordingly
            else {

                for (Text value : values) {
                    String feature_update = best_features_index_count_f.get(key_as_string[2]);
                    if (feature_update != null) {
                        String[] index_count_f = feature_update.split("\\s+");
                        int index = Integer.parseInt(index_count_f[0]);
                        long count_f = Long.parseLong(index_count_f[1]);

                        double count_f_l = Double.parseDouble(value.toString());


//                    eq five = count(f,l)
                        vector[index * 4] = count_f_l;
//                    double eq_five = count_of_f_comma_l;

//                    eq six = P(f|l) = count(f,l) / count(l)
                        vector[index * 4 + 1] = count_f_l / count_of_l;


//                    P(f) = count(f) / count (F)
                        double prob_of_f = count_f / count_big_F_as_double;


                        double prob_of_l = count_of_l / count_big_F_as_double;


//                    P(f,l) = count(f,l) / count(F)
                        double prob_of_f_comma_l = count_f_l / count_big_F_as_double;


//                    P(l) * P(f)

                        double prob_of_f_mult_prob_of_l = prob_of_l * prob_of_f;

//                    eq seven = log2 (P(l,f) / (P(l) * P(f)) )
                        double eq_seven_before_log = prob_of_f_comma_l / prob_of_f_mult_prob_of_l;

                        vector[index * 4 + 2] = Math.log(eq_seven_before_log) / Math.log(2.0);

//                    eq eight = ( P(l,f) - (P(l) * P(f)) ) / sqrt ( (P(l) * P(f) )
                        double sqrt_P_l_P_f = Math.sqrt(prob_of_f_mult_prob_of_l);

                        double numerator = prob_of_f_comma_l - prob_of_f_mult_prob_of_l;

                        vector[index * 4 + 3] = numerator / sqrt_P_l_P_f;

                    }
                }
            }
        }

        @Override
        protected void cleanup(Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
//            if(last_round!=null) {
//                StringBuilder stringBuilder = new StringBuilder();
//                for (double d : vector){
//                    stringBuilder.append(d);
//                    stringBuilder.append(" ");
//                }
//                String to_write = stringBuilder.toString();
//
//                context.write(new Text(last_round), new Text(to_write.substring(0,to_write.length()-1))  );
//
//            }
        }
    }

    //    Partitioner
    public static class PartitionerClass extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            String part = key.toString().split("\\s+")[0];
            Text newKey = new Text(part);
            return Math.abs(newKey.hashCode()) % numPartitions;
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

