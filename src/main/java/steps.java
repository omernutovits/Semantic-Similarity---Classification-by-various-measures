import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class steps {

    public enum NCounter
    {
        N_COUNTER
    }

    private static String bucketOutputPath;

    private static String setPaths (Job job, String inputPath) throws  IOException {
        FileInputFormat.addInputPath(job, new Path(inputPath));
        String path = bucketOutputPath + job.getJobName();
        FileOutputFormat.setOutputPath(job, new Path (path));
        return path;
    }



    private static void step1 (Job job, String filePath) throws IOException {

        job.setJarByClass(step_one_make_vectors.class);
        job.setMapperClass(step_one_make_vectors.MapperClass.class);
        job.setReducerClass(step_one_make_vectors.ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(filePath));
        FileOutputFormat.setOutputPath(job, new Path(filePath));
        MultipleOutputs.addNamedOutput(job,"countlf",TextOutputFormat.class,Text.class,Text.class);
        MultipleOutputs.addNamedOutput(job,"countf",TextOutputFormat.class,Text.class,Text.class);
        MultipleOutputs.addNamedOutput(job,"countl",TextOutputFormat.class,Text.class,Text.class);
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(Text.class);
//        job.setOutputFormatClass(TextOutputFormat.class);
//        if(use_combiner_or_not){
//            job.setCombinerClass(step1_count_N_and_split_corpus.ReducerClass.class);
//        }
    }
    private static void step2 (Job job, String filePath) throws IOException {
        job.setJarByClass(step_two_find_best_features.class);
        job.setMapperClass(step_two_find_best_features.MapperClass.class);
        job.setReducerClass(step_two_find_best_features.ReducerClass.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1);
//        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job, new Path(filePath+"/countf"));
//        MultipleInputs.addInputPath(job, new Path(filePath  + "/countlf"), TextInputFormat.class);
//        MultipleInputs.addInputPath(job, new Path(filePath + "/countf"), TextInputFormat.class);
//        MultipleInputs.addInputPath(job, new Path(filePath + "/countl"), TextInputFormat.class);
    }
    private static String step3 (Job job, String filePath, String step1Path) throws IOException {
        job.setJarByClass(step_three_make_vector_4000.class);
//        job.setMapperClass(step_three_make_vector_4000.MapperClass_count_of_l.class);
        job.setReducerClass(step_three_make_vector_4000.ReducerClass.class);
        job.setPartitionerClass(step_three_make_vector_4000.PartitionerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(step_three_make_vector_4000.DoubleArrayWritable.class);
//        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        MultipleInputs.addInputPath(job, new Path(step1Path + "/countlf"), TextInputFormat.class, step_three_make_vector_4000.MapperClass_count_of_l_and_f.class);
        MultipleInputs.addInputPath(job, new Path(step1Path + "/countl"), TextInputFormat.class, step_three_make_vector_4000.MapperClass_count_of_l.class);
        return setPaths(job, filePath);
    }

        private static String step3Debug (Job job, String filePath, String step1Path) throws IOException {
            job.setJarByClass(step_three_debug.class);
//        job.setMapperClass(step_three_make_vector_4000.MapperClass_count_of_l.class);
            job.setReducerClass(step_three_debug.ReducerClass.class);
            job.setPartitionerClass(step_three_debug.PartitionerClass.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
//        job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);
            MultipleInputs.addInputPath(job, new Path(step1Path  + "/countlf"), TextInputFormat.class, step_three_debug.MapperClass_count_of_l_and_f.class);
            MultipleInputs.addInputPath(job, new Path(step1Path + "/countl"), TextInputFormat.class, step_three_debug.MapperClass_count_of_l.class);
            return setPaths(job, filePath);
    }
    private static String step4 (Job job, String filePath) throws IOException {
        job.setJarByClass(step_four_make_gold_standard_pairs_vectors.class);
        job.setMapperClass(step_four_make_gold_standard_pairs_vectors.MapperClass.class);
        job.setReducerClass(step_four_make_gold_standard_pairs_vectors.ReducerClass.class);
        job.setNumReduceTasks(1);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        return setPaths(job, filePath);
    }



    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        // First stage - split the corpus into 2 parts
//        if (args.length<2){
//            System.out.println("please provide input path and output path");
//            System.exit(1);
//        }

        String input = args[0];
        bucketOutputPath = args[1];
        String isDebug = args.length>2 ? args[2] : "no";
//        boolean combiner_or_not = args.length > 2 && args[3].equals("yes");

        Configuration job1conf = new Configuration();
        Job job1 = Job.getInstance(job1conf, "step1");
        step1(job1, input);
        String step1Path = bucketOutputPath + job1.getJobName();
        FileOutputFormat.setOutputPath(job1, new Path (step1Path));
        if (job1.waitForCompletion(true)){
            System.out.println("Job 1 Completed");
        }
        else{
            System.out.println("Job 1 Failed");
            System.exit(1);
        }
        Counters counters = job1.getCounters();
        Counter counter = counters.findCounter(NCounter.N_COUNTER);
        long N = counter.getValue();
//        System.out.println("===================================================" + N);
        job1conf.setLong("counter", N);


        Configuration job2conf = new Configuration();
        job2conf.setLong("counter", N);
        Job job2 = Job.getInstance(job2conf, "step2");
        step2(job2, step1Path);
        String path = bucketOutputPath + job2.getJobName();
        FileOutputFormat.setOutputPath(job2, new Path (path));


        if (job2.waitForCompletion(true)){
            System.out.println("Job 2 Completed");
            System.out.println("===================================================" + N);

        }
        else{
            System.out.println("Job 2 Failed");
            System.exit(1);
        }
        String job3Path = "";
        Configuration job3conf = new Configuration();
        job3conf.setLong("counter", N);
        final Job job3 = Job.getInstance(job3conf, "step3");
        if(isDebug.equalsIgnoreCase("yes")){
            job3Path = step3Debug(job3, path,step1Path);
        }
        else {
            job3Path = step3(job3, path, step1Path);
        }
        if (job3.waitForCompletion(true)){
            System.out.println("Job 3 Completed");
        }
        else{
            System.out.println("Job 3 Failed");
            System.exit(1);
        }


        Configuration job4conf = new Configuration();
        final Job job4 = Job.getInstance(job4conf, "step4");
        String job4Path = step4(job4, job3Path);
        if (job4.waitForCompletion(true)){
            System.out.println("Job 4 Completed");
        }
        else{
            System.out.println("Job 4 Failed");
            System.exit(1);
        }
    }
}
