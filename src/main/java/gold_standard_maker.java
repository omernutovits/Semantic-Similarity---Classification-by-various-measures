import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;

import java.io.*;
import java.util.*;

public class gold_standard_maker {

    public static HashSet<String> create_golden_set() throws IOException {
        HashSet<String> gold_map = new HashSet<>();
        AmazonS3 s3 = AmazonS3ClientBuilder.defaultClient();
        S3Object o = s3.getObject("testing-bucket-omer-tzuki2", "word-relatedness.txt");
        S3ObjectInputStream object_content = o.getObjectContent();
        BufferedReader reader = new BufferedReader(new InputStreamReader(object_content));

        String line;

        while ((line = reader.readLine()) != null) {
            String[] two_words = line.split("\\s+");
            if (two_words.length < 2) {
                continue;
            }
            gold_map.add(two_words[0]);
            gold_map.add(two_words[1]);
        }
//        String [] arr_set = (String[]) gold_map.toArray();
        int counter = 1;
        for (String str : gold_map) {
            System.out.println(counter+ ". " +str);
            counter++;
        }


        return gold_map;
    }

    public static Map<String, ArrayList<String>> create_golden_map (boolean bool) throws IOException {
        Map <String,ArrayList<String>>gold_map = new HashMap<>();

        int first_index = (bool) ? 0 : 1;
        int second_index = 1- first_index;


        AmazonS3 s3 = AmazonS3ClientBuilder.defaultClient();
        S3Object o = s3.getObject("testing-bucket-omer-tzuki2", "word-relatedness.txt");
        S3ObjectInputStream object_content = o.getObjectContent();
        BufferedReader reader = new BufferedReader(new InputStreamReader(object_content));

        String line;

        while ((line = reader.readLine()) != null) {
            String[] two_words = line.split("\\s+");
            if (two_words.length < 2) {
                continue;
            }
            if(gold_map.containsKey(two_words[first_index])){
                ArrayList<String> to_add = gold_map.get(two_words[first_index]);


                to_add.add( two_words[second_index] + " " + two_words[2]);
                gold_map.put(two_words[first_index] , to_add);
            }
            else{
                ArrayList<String> to_add = new ArrayList<String>();
                to_add.add( two_words[second_index] + " " + two_words[2]);
                gold_map.put(two_words[first_index] , to_add);
            }

        }

        return gold_map;
    }


    public static Map<String, String> create_feature_map () throws IOException {
        Map<String, String> feature_map = new HashMap<>();

        AmazonS3 s3 = AmazonS3ClientBuilder.defaultClient();
        S3Object o = s3.getObject("testing-bucket-omer-tzuki2", "step2/part-r-00000");
        S3ObjectInputStream object_content = o.getObjectContent();
        BufferedReader reader = new BufferedReader(new InputStreamReader(object_content));

        String line;

        while ((line = reader.readLine()) != null) {
            String key = line.substring(0, line.indexOf("\t"));
            String val = line.substring(line.indexOf("\t") + 1);
            feature_map.put(key, val);
            System.out.println("key: " + key);
        }




        return feature_map;


    }
}
