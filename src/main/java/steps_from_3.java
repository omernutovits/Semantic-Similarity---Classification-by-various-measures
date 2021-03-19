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

public class steps_from_3 {

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
    private static String step4Debug (Job job, String filePath) throws IOException {
        job.setJarByClass(step_four_debug.class);
        job.setMapperClass(step_four_debug.MapperClass.class);
        job.setReducerClass(step_four_debug.ReducerClass.class);
        job.setNumReduceTasks(1);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
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

       long N = 1654449915;
        String job3Path = "";
        Configuration job3conf = new Configuration();
        job3conf.setLong("counter", N);
        final Job job3 = Job.getInstance(job3conf, "step3");
        if(isDebug.equalsIgnoreCase("yes")){
            job3Path = step3Debug(job3, "s3://testing-bucket-omer-tzuki2/","s3://testing-bucket-omer-tzuki2/step1/");
        }
        else {
            job3Path = step3(job3, "s3://testing-bucket-omer-tzuki2/","s3://testing-bucket-omer-tzuki2/step1/");
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
        if(isDebug.equalsIgnoreCase("yes")){
            String job4Path = step4Debug(job4, job3Path);
        }
        else {
            String job4Path = step4(job4, job3Path);
        }


        if (job4.waitForCompletion(true)){
            System.out.println("Job 4 Completed");
        }
        else{
            System.out.println("Job 4 Failed");
            System.exit(1);
        }
    }
}
