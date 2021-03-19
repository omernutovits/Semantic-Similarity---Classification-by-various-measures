import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;


import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class step_one_make_vectors {
    public static Counter bigF;
    public static AmazonS3 s3 = AmazonS3ClientBuilder.defaultClient();



    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {
        HashSet <String> gold_standard;


        @Override
        protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException {

            gold_standard = gold_standard_maker.create_golden_set();

        }

        @Override
        public void map(LongWritable lineId, Text line, Context context) throws IOException,  InterruptedException {

            String line_as_string = line.toString();
            String [] head_body_rest = line_as_string.split("\t");
            String occurrences = head_body_rest[2];

//            String head = head_body_rest[0];
//            int index_of_head = -1;
            String [] splitted_body = head_body_rest[1].split(" ") ;
            for (int i=0; i<splitted_body.length; i++){

                String current_str = splitted_body[i];
                String who_is_my_daddy = current_str.substring(current_str.lastIndexOf("/")+1);

                int index_of_daddy = Integer.parseInt(who_is_my_daddy);
                if(index_of_daddy != 0){


                    String daddy_word = splitted_body[index_of_daddy - 1].substring(0,splitted_body[index_of_daddy - 1].indexOf("/"));
                    if(i%50==0){
                        System.out.println(daddy_word);
                    }
                    String feature = current_str.substring(0, current_str.indexOf("/"));
                    String feature_type = current_str.substring(current_str.indexOf("/",feature.length() + 1));
                    feature_type = feature_type.substring(1,feature_type.length()-2);


//                    TODO do i need only the golden standard lexemas?
                    if(gold_standard.contains(daddy_word)) {
//                    count(l,f)
                        context.write(new Text(daddy_word + "\t" + feature + "-" + feature_type), new Text(occurrences));
                        System.out.println("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx");
                        System.out.println(daddy_word);

//                    count(l)
                        context.write(new Text(daddy_word), new Text(occurrences));

                    }
//                    count(L) = count(F)
                    context.write(new Text("*******"), new Text(occurrences));
//                    count(f)
                    context.write(new Text(feature + "-" + feature_type), new Text(occurrences));



                }

            }



        }
    }



    public static class ReducerClass extends Reducer<Text,Text,Text,Text> {
        private MultipleOutputs mos;



        @Override
        protected void setup(Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            mos = new MultipleOutputs(context);
            bigF = context.getCounter(steps.NCounter.N_COUNTER);

        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text,Text,Text,Text>.Context context) throws IOException, InterruptedException {
            long counter = 0;
            String key_as_string = key.toString();
            for (Text value : values) {
                counter += Long.parseLong(value.toString());
            }

            if(key_as_string.equals("*******")){
                bigF.setValue(counter);
            }
            else if(key_as_string.contains("\t")){
                mos.write(key, new Text(Long.toString(counter)),"countlf/countlf");
            }
            else if(key_as_string.contains("-")){
                mos.write(key, new Text(Long.toString(counter)),"countf/countf");
            }
            else{
                mos.write(key, new Text(Long.toString(counter)),"countl/countl");
            }

        }

        @Override
        protected void cleanup(Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            mos.close();
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

