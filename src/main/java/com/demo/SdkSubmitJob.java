package com.demo;

import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.*;

import java.util.ArrayList;
import java.util.List;

public class SdkSubmitJob {

    public static void main(String[] args) {
        long startTime = System.nanoTime();

        AmazonElasticMapReduce emr = AmazonElasticMapReduceClientBuilder.standard().build();


        AddJobFlowStepsRequest req = new AddJobFlowStepsRequest();
        req.withJobFlowId("j-3A2S4TE316NPL");

        List<StepConfig> stepConfigs = new ArrayList<StepConfig>();

        HadoopJarStepConfig sparkStepConf = new HadoopJarStepConfig()
                .withJar("command-runner.jar")
                .withArgs("spark-submit", "--deploy-mode", "cluster", "--class", "com.caseware.txn2csv2parquet.ExportPgToParquet", "s3a://export-tp-to-parquet/txn-to-csv-to-parquet-with-aws.jar", "A8A127FB-39F8-4BFB-A2C9-BC7E331DA796", "7M0z7UwcSkim5b6uWkaY9A");

        StepConfig sparkStep = new StepConfig()
                .withName("Spark Step")
                .withActionOnFailure("CONTINUE")
                .withHadoopJarStep(sparkStepConf);

        stepConfigs.add(sparkStep);
        req.withSteps(stepConfigs);
        AddJobFlowStepsResult result = emr.addJobFlowSteps(req);

        result.getStepIds().forEach(stepId -> {
            emr.listSteps(new ListStepsRequest().withClusterId("j-3A2S4TE316NPL")).getSteps().forEach(steps -> {
                if (steps.getId().equalsIgnoreCase(stepId)) {
                    System.out.println(steps.getStatus().getState());
                }
            });

            // Use stepstatus to get the execution status - value could be PENDING | CANCEL_PENDING | RUNNING | COMPLETED | CANCELLED | FAILED | INTERRUPTED
//            StepSummary stepSummary = emr.listSteps(new ListStepsRequest().withClusterId("j-3A2S4TE316NPL")).getSteps().get(0);
//            StepStatus stepSummaryStatus = stepSummary.getStatus();
//            String stepStatus = stepSummaryStatus.getState();
//            StepExecutionState.valueOf(stepStatus);
        });

//

        long endTime = System.nanoTime();
        long duration = (endTime - startTime);

        System.out.println("***Total process time****" + duration + "\n");
    }
}
//1889649505
//114971744089