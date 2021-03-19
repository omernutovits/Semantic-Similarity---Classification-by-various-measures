import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.*;


public class runner {
    public static AWSCredentialsProvider credentialsProvider = new AWSStaticCredentialsProvider(new ProfileCredentialsProvider().getCredentials());
    public static AmazonElasticMapReduce emr;

    public static void main(String[] args) {

        System.out.println("Creating EMR instance");

        emr = AmazonElasticMapReduceClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion("us-east-1")
                .build();


		/*
        Step No 1: Get N and split corpus
		 */
        HadoopJarStepConfig only_step = new HadoopJarStepConfig()
                .withJar("s3://testing-bucket-omer-tzuki2/steps3.jar")
                .withArgs("s3://assignment3dsp/biarcs/", "s3://testing-bucket-omer-tzuki2/", "yes");
        StepConfig onlyStep = new StepConfig()
                .withName("all_steps")
                .withHadoopJarStep(only_step)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
                .withInstanceCount(8)
                .withMasterInstanceType(InstanceType.M4Large.toString())
                .withSlaveInstanceType(InstanceType.M4Large.toString())
                .withHadoopVersion("2.6.0")
                .withEc2KeyName("omer_and_tzuki")
                .withPlacement(new PlacementType("us-east-1a"))
                .withKeepJobFlowAliveWhenNoSteps(false);

		/*
        Run all jobs
		 */
        RunJobFlowRequest request = new RunJobFlowRequest()
                .withName("omer_and_tzuki")
                .withInstances(instances)
                .withSteps(onlyStep)
                .withLogUri("s3n://testing-bucket-omer-tzuki2/logs/")
                .withServiceRole("EMR_DefaultRole")
                .withJobFlowRole("EMR_EC2_DefaultRole")
                .withReleaseLabel("emr-5.11.0");
        RunJobFlowResult result = emr.runJobFlow(request);
        String id = result.getJobFlowId();
        System.out.println("Id: " + id);
    }
}