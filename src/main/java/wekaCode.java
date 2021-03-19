import java.io.*;
import java.util.Random;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import weka.classifiers.Classifier;
import weka.classifiers.Evaluation;
import weka.classifiers.bayes.NaiveBayes;
import weka.core.Instances;

public class wekaCode {
    public static BufferedReader readDataFile(String filename) {
        BufferedReader inputReader = null;

        try {
            inputReader = new BufferedReader(new FileReader(filename));
        } catch (FileNotFoundException ex) {
            System.err.println("File not found: " + filename);
        }

        return inputReader;
    }


    public static void main(String[] args) throws Exception {

        String input ="@relation input \n " +
                "@attribute fiveNine \n" +
                "@attribute sixNine \n" +
                "@attribute sevenNine \n" +
                "@attribute eightNine \n" +
                "@attribute fiveTen \n" +
                "@attribute sixTen \n" +
                "@attribute sevenTen \n" +
                "@attribute eightTen \n" +
                "@attribute fiveEleven \n" +
                "@attribute sixEleven \n" +
                "@attribute sevenEleven \n" +
                "@attribute eightEleven \n" +
                "@attribute fiveThirteen \n" +
                "@attribute sixThirteen \n" +
                "@attribute sevenThirteen \n" +
                "@attribute eightThirteen \n" +
                "@attribute fiveFifteen \n" +
                "@attribute sixFifteen \n" +
                "@attribute sevenFifteen \n" +
                "@attribute eightFifteen \n" +
                "@attribute fiveSeventeen \n" +
                "@attribute sixSeventeen \n" +
                "@attribute sevenSeventeen \n" +
                "@attribute eightSeventeen \n" +
                "@data \n";
        AmazonS3 s3 = AmazonS3ClientBuilder.defaultClient();
        S3Object o = s3.getObject("testing-bucket-omer-tzuki2", "finalObject");

        S3ObjectInputStream object_content = o.getObjectContent();
        System.out.println("Object Received");

        BufferedReader reader = new BufferedReader(new InputStreamReader(object_content));

        String line;
        String output;

        File input_file = new File("input.arff");
        FileWriter fileWriter = new FileWriter("input.arff");
        fileWriter.write(input);
        while ((line = reader.readLine()) != null) {
            output = line.replaceAll("", "\\,");
            fileWriter.write(output);
        }



        BufferedReader datafile = readDataFile("input.arff");
        Instances data = new Instances(datafile);
        Evaluation evaluation = new Evaluation(data);
        Classifier cModel = new NaiveBayes();
        evaluation.crossValidateModel(cModel, data, 10, new Random(1));
        // Do 10-split cross validation
        File output_weka = new File("output.txt");
        FileWriter wekaWriter = new FileWriter("output.txt");
        wekaWriter.write(evaluation.toSummaryString());
        wekaWriter.write(evaluation.toClassDetailsString());
        try {
            s3.putObject("bucket-similar-words-2502", "omer_and_tzuki", output_weka);
        }
        catch (AmazonServiceException e){
            System.out.println(e.getErrorMessage());
        }



    }
}