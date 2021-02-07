package com.amazonaws.samples;


import java.util.UUID;

import com.amazonaws.services.codedeploy.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowRequest;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowResult;
import com.amazonaws.services.elasticmapreduce.model.Application;
import com.amazonaws.services.elasticmapreduce.model.HadoopJarStepConfig;
import com.amazonaws.services.elasticmapreduce.model.JobFlowInstancesConfig;
import com.amazonaws.services.elasticmapreduce.model.PlacementType;
import com.amazonaws.services.elasticmapreduce.model.StepConfig;
import com.amazonaws.services.elasticmapreduce.util.StepFactory;

public class Main {

	public static void main(String[] args) {
		AmazonElasticMapReduce emr = AmazonElasticMapReduceClientBuilder.defaultClient();
		// initialize minPmi and relMinPmi inside the configuration
		String rand = UUID.randomUUID().toString();
		String minPmi = args[0];
		String relMinPmi = args[1];
		
		/**
		 * Step 1
		 */
        HadoopJarStepConfig step1cfg = new HadoopJarStepConfig()
        	.withJar("s3://dsps192ass2-sy/jars/step1.jar")
        	//.withJar("s3://dsps192ass2-sy/jars/step1test.jar")
        	// for debugging and testing purposes change to jar to step1test.jar
        	.withMainClass("Step1")
        	.withArgs("s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-us-all/2gram/") 
        	.withArgs("s3://dsps192ass2-sy/output/"+rand+"-1/")
        	.withArgs(minPmi)
        	.withArgs(relMinPmi);
		
	        StepConfig step1 = new StepConfig()
	        	.withName("Step 1")
	        	.withActionOnFailure("TERMINATE_JOB_FLOW")
	        	.withHadoopJarStep(step1cfg);
		/**
		 * Step 2
		 */
	        HadoopJarStepConfig step2cfg = new HadoopJarStepConfig()
	            	.withJar("s3://dsps192ass2-sy/jars/step2a.jar")
	            	.withMainClass("Step2")
	            	.withArgs("s3://dsps192ass2-sy/output/"+rand+"-1/") 
	            	.withArgs("s3://dsps192ass2-sy/output/"+rand+"-2/")
	            	.withArgs(minPmi)
	            	.withArgs(relMinPmi);
	    		
	    	        StepConfig step2 = new StepConfig()
	    	        	.withName("Step 2")
	    	        	.withActionOnFailure("TERMINATE_JOB_FLOW")
	    	        	.withHadoopJarStep(step2cfg);
		/**
		 * Step 3
		 */
	        HadoopJarStepConfig step3cfg = new HadoopJarStepConfig()
	            	.withJar("s3://dsps192ass2-sy/jars/step3a.jar")
	            	.withMainClass("Step3")
	            	.withArgs("s3://dsps192ass2-sy/output/"+rand+"-2/") 
	            	.withArgs("s3://dsps192ass2-sy/output/"+rand+"-3/")
	            	.withArgs(minPmi)
	            	.withArgs(relMinPmi);
	    		
	    	        StepConfig step3 = new StepConfig()
	    	        	.withName("Step 3")
	    	        	.withActionOnFailure("TERMINATE_JOB_FLOW")
	    	        	.withHadoopJarStep(step3cfg);
		/** 
		 * Step 4
		 */	
	        HadoopJarStepConfig step4cfg = new HadoopJarStepConfig()
	            	.withJar("s3://dsps192ass2-sy/jars/step4b.jar")
	            	.withMainClass("Step4")
	            	.withArgs("s3://dsps192ass2-sy/output/"+rand+"-3/")
	            	.withArgs("s3://dsps192ass2-sy/output/"+rand+"-4/")
	            	.withArgs(minPmi)
	            	.withArgs(relMinPmi);
	    		
	    	        StepConfig step4 = new StepConfig()
	    	        	.withName("Step 4")
	    	        	.withActionOnFailure("TERMINATE_JOB_FLOW")
	    	        	.withHadoopJarStep(step4cfg);
		/**
		 * Step 5
		 */
	        HadoopJarStepConfig step5cfg = new HadoopJarStepConfig()
	            	.withJar("s3://dsps192ass2-sy/jars/step5.jar")
	            	.withMainClass("Step5")
	            	.withArgs("s3://dsps192ass2-sy/output/"+rand+"-4/")
	            	.withArgs("s3://dsps192ass2-sy/output/"+rand+"-5/")
	            	.withArgs(minPmi)
	            	.withArgs(relMinPmi);
	    		
	    	        StepConfig step5 = new StepConfig()
	    	        	.withName("Step 5")
	    	        	.withActionOnFailure("TERMINATE_JOB_FLOW")
	    	        	.withHadoopJarStep(step5cfg);
		/**
		 * Step 6
		 */
	        HadoopJarStepConfig step6cfg = new HadoopJarStepConfig()
	            	.withJar("s3://dsps192ass2-sy/jars/step6.jar")
	            	.withMainClass("Step6")
	            	.withArgs("s3://dsps192ass2-sy/output/"+rand+"-5/")
	            	.withArgs("s3://dsps192ass2-sy/output/"+rand+"-6/")
	            	.withArgs(minPmi)
	            	.withArgs(relMinPmi);
	    		
	    	        StepConfig step6 = new StepConfig()
	    	        	.withName("Step 6")
	    	        	.withActionOnFailure("TERMINATE_JOB_FLOW")
	    	        	.withHadoopJarStep(step6cfg);

		RunJobFlowRequest request = new RunJobFlowRequest()
	       		.withName("Distributed-Ass2")
	       		.withReleaseLabel("emr-5.3.1")
	       		.withSteps(step1, step2, step3, step4, step5, step6)
	       		.withLogUri("s3://dsps192ass2-sy/logs/")
	       		.withServiceRole("EMR_DefaultRole")
	       		.withJobFlowRole("EMR_EC2_DefaultRole")
	       		.withInstances(new JobFlowInstancesConfig()
	           		.withEc2KeyName("ssh_login")
	           		.withInstanceCount(20) // CLUSTER SIZE
	           		.withKeepJobFlowAliveWhenNoSteps(false)    
	           		.withMasterInstanceType("m3.xlarge")
	           		.withSlaveInstanceType("m3.xlarge"));
			
		

	   RunJobFlowResult result = emr.runJobFlow(request);  
	   System.out.println("JobFlow id: "+result.getJobFlowId());
	   
	   System.out.println("The results will be stored in address:");
	   System.out.println("s3://dsps192ass2-sy/output/"+rand+"-6/");
	   System.out.println("(Access via aws s3 services)");
	   System.out.println("Notice: the backend will run in the background,\nyou might want to order a pizza and watch a movie...:)");
	   
	}
}