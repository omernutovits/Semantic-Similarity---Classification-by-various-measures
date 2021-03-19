import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;


import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class step_four_debug {

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

        @Override
        public DoubleWritable[] get() {
            return (DoubleWritable[]) super.get();
        }
    }





    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {
        Map<String, ArrayList<String>> gold_map_one;
        Map<String, ArrayList<String>> gold_map_two;


        @Override
        protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException {
            gold_map_one = gold_standard_maker.create_golden_map(true);
            gold_map_two = gold_standard_maker.create_golden_map(false);

        }

        @Override
        public void map(LongWritable lineId, Text vector_with_key, Context context) throws IOException,  InterruptedException {
            String vector_with_key_as_string = vector_with_key.toString();
            String key_as_string = vector_with_key_as_string.substring(0,vector_with_key_as_string.indexOf("\t"));
            String vector = vector_with_key_as_string.substring(vector_with_key_as_string.indexOf("\t")+1);


            ArrayList<String> second_words = gold_map_one.get(key_as_string);
            if(second_words!= null) {
                for (String word : second_words) {
                    context.write(new Text(key_as_string + "\t" + word), new Text(vector));


                }
            }
            ArrayList<String> first_words = gold_map_two.get(key_as_string);
            if(first_words!= null) {
                for (String word : first_words) {
                    String first_word = word.substring(0,word.indexOf(" "));
                    String bool_word = word.substring(word.indexOf(" ") + 1);
                    context.write(new Text(first_word + "\t" + key_as_string + " " + bool_word), new Text(vector));


                }
            }

        }
    }



    public static class ReducerClass extends Reducer<Text,Text,Text,Text> {




        @Override
        protected void setup(Reducer<Text, Text, Text, Text>.Context context) {
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text,Text,Text,Text>.Context context) throws IOException, InterruptedException {

//            String last_round = null;
            ArrayList<Text> vectors_of_line = new ArrayList<>();

            for (Text value : values) {
                vectors_of_line.add(value);
                System.out.println(key.toString());
            }
            System.out.println();
            System.out.println();

            Double[] vector_of_twenty_four = new Double[24];
            Arrays.fill(vector_of_twenty_four, 0.0);

            if (vectors_of_line.size() > 1) {

                String[] vector_one_as_string = vectors_of_line.remove(0).toString().split(" ");
                String[] vector_two_as_string = vectors_of_line.remove(0).toString().split(" ");


                double[] vector_one = new double[vector_one_as_string.length];
                double[] vector_two = new double[vector_one_as_string.length];


                for (int i = 0; i < vector_one.length; i++) {
                    vector_one[i] = Double.parseDouble(vector_one_as_string[i]);
                    vector_two[i] = Double.parseDouble(vector_two_as_string[i]);

                }


//          equations 9,10,11,13,15,17

//            eq nine = SIGMA ( |l_1i - l_2i | )

                double[] eq_nine = new double[4];
                double tmp_five;


                for (int i = 0; i < 1000; i++) {
                    for (int j = 0; j < 4; j++) {
                        tmp_five = vector_one[4 * i + j] - vector_two[4 * i + j];
                        tmp_five = Math.abs(tmp_five);

                        eq_nine[j] += tmp_five;
                    }
                }

//              eq_ten = sqrt (SIGMA (l_1i - l_2i) ^ 2)

                double[] eq_ten = new double[4];


                for (int i = 0; i < 1000; i++) {
                    for (int j = 0; j < 4; j++) {
                        tmp_five = vector_one[4 * i + j] - vector_two[4 * i + j];
//                square
                        tmp_five *= tmp_five;

                        eq_ten[j] += tmp_five;
                    }
                }

                for (int j = 0; j < 4; j++) {
                    eq_ten[j] = Math.sqrt(eq_ten[j]);
                }


                double[] eleven_one_mult_two = new double[4];
                double[] eleven_square_one = new double[4];
                double[] eleven_square_two = new double[4];


                for (int i = 0; i < 1000; i++) {

                    for (int j = 0; j < 4; j++) {
                        tmp_five = vector_one[4 * i + j] * vector_two[4 * i + j];
//                one_mult_two
                        eleven_one_mult_two[j] += tmp_five;


//                    square_one
                        tmp_five = vector_one[4 * i + j];
                        tmp_five *= tmp_five;

                        eleven_square_one[j] += tmp_five;

//                    square_two
                        tmp_five = vector_two[4 * i + j];
                        tmp_five *= tmp_five;

                        eleven_square_two[j] += tmp_five;

                    }


                }

                double[] eq_eleven_final = new double[4];

                for (int i = 0; i < 4; i++) {
                    eleven_square_one[i] = Math.sqrt(eleven_square_one[i]);
                    eleven_square_two[i] = Math.sqrt(eleven_square_two[i]);
                }


                for (int i = 0; i < 4; i++) {
                    eq_eleven_final[i] = eleven_square_one[i] * eleven_square_two[i];

                    eq_eleven_final[i] = eleven_one_mult_two[i] / eq_eleven_final[i];

                }


//            =============================================
//            eq_thirteen

                double[] thirteen_max = new double[4];
                double[] thirteen_min = new double[4];


                double[] sum_one_and_two = new double[4];

                double[] thirteen_final = new double[4];
                double[] fifteen_final = new double[4];


                for (int i = 0; i < 1000; i++) {

                    for (int j = 0; j < 4; j++) {
                        tmp_five = Math.max(vector_one[4 * i + j], vector_two[4 * i + j]);
//                max
                        thirteen_max[j] += tmp_five;


                        tmp_five = Math.min(vector_one[4 * i + j], vector_two[4 * i + j]);
//                min
                        thirteen_min[j] += tmp_five;

                        tmp_five = vector_one[4 * i + j] + vector_two[4 * i + j];

                        sum_one_and_two[j] += tmp_five;

                    }

                }

                for (int i = 0; i < 4; i++) {

                    fifteen_final[i] = thirteen_min[i] * 2;

                    fifteen_final[i] = fifteen_final[i] / sum_one_and_two[i];


                    thirteen_final[i] = thirteen_min[i] / thirteen_max[i];
                }

//            =====================================
//            seventeen


                double[] seventeen_one = new double[4];
                double[] seventeen_two = new double[4];
                double[] seventeen_avg = new double[4];
                double[] seventeen_final = new double[4];


//            D(P||Q) = SIGMA (P(x) * log (P(x) / Q(x) ))

//            17 - D(l1 || avg) +
//                 D(l2 || avg)

                for (int i = 0; i < 1000; i++) {
                    for (int j = 0; j < 4; j++) {
                        seventeen_one[j] = vector_one[4 * i + j];
                        seventeen_two[j] = vector_two[4 * i + j];
                        seventeen_avg[j] = seventeen_one[j] + seventeen_two[j];
                        seventeen_avg[j] = seventeen_avg[j] / 2;

                        tmp_five = seventeen_one[j] / seventeen_avg[j];
                        tmp_five = Math.log10(tmp_five);

                        tmp_five *= seventeen_one[j];

                        seventeen_final[j] += tmp_five;

                        tmp_five = seventeen_two[j] / seventeen_avg[j];
                        tmp_five = Math.log10(tmp_five);

                        tmp_five *= seventeen_two[j];


                        seventeen_final[j] += tmp_five;


                    }
                }


                for (int i = 0; i < 4; i++) {

                    vector_of_twenty_four[i] = eq_nine[i];
                    vector_of_twenty_four[4 + i] = eq_ten[i];
                    vector_of_twenty_four[8 + i] = eq_eleven_final[i];
                    vector_of_twenty_four[12 + i] = thirteen_final[i];
                    vector_of_twenty_four[16 + i] = fifteen_final[i];
                    vector_of_twenty_four[20 + i] = seventeen_final[i];
                }


                StringBuilder output_of_vector = new StringBuilder();
                for (double num : vector_of_twenty_four) {
                    if (Double.isNaN(num) || Double.isInfinite(num)) {
                        output_of_vector.append("?");
                    } else {
                        output_of_vector.append(String.valueOf(num));
                    }
                    output_of_vector.append(",");
                }

                String to_write = output_of_vector.substring(0, output_of_vector.length() - 1);

                String [] key_array = key.toString().split("\\s+");

                context.write(new Text(to_write),new Text( key_array[2]));


//                context.write(new Text( key_array[2]), new Text(output_of_vector.toString()));



            }
        }
        @Override
        protected void cleanup(Reducer<Text, Text, Text, Text>.Context context)  {
//            context.write(new Text(last_round), new DoubleArrayWritable(vector)  );

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

