package com.demo;

import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.*;

import java.util.ArrayList;
import java.util.List;

public class SdkSubmitJob {

    public static void main(String[] args) {

        AmazonElasticMapReduce emr = AmazonElasticMapReduceClientBuilder.standard().build();

        AddJobFlowStepsRequest req = new AddJobFlowStepsRequest();
        req.withJobFlowId("j-3A2S4TE316NPL");

        List<StepConfig> stepConfigs = new ArrayList<StepConfig>();

        HadoopJarStepConfig sparkStepConf = new HadoopJarStepConfig()
                .withJar("command-runner.jar")
                .withArgs("spark-submit", "--executor-memory", "8g", "--class", "org.apache.spark.examples.SparkPi", "/usr/lib/spark/examples/jars/spark-examples.jar", "10");

        StepConfig sparkStep = new StepConfig()
                .withName("Spark Step")
                .withActionOnFailure("CONTINUE")
                .withHadoopJarStep(sparkStepConf);

        stepConfigs.add(sparkStep);
        req.withSteps(stepConfigs);
        emr.addJobFlowSteps(req);

        StepSummary stepSummary = emr.listSteps(new ListStepsRequest().withClusterId("j-3A2S4TE316NPL")).getSteps().get(0);
        StepStatus stepSummaryStatus = stepSummary.getStatus();
        String stepStatus = stepSummaryStatus.getState();
        StepExecutionState.valueOf(stepStatus);
    }
}
