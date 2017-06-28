# Dataflow Timeseries Examples

These samples are designed to be used as demonstrators and are not intended for production usage. Timeseries data has relevance in many sectors and industries with the problems being solved having relevance to financial data to the internet of things and instrumentation data from virtual machines.

Please note this is not an official Google product

### SETUP

Ensure you have a working Dataflow environment by following the instructions at:

https://github.com/GoogleCloudPlatform/DataflowJavaSDK

Java 1.8 is required for these samples.

#### NOTE: 

This sample is based on Dataflow version 1.9 which does not directly support keyed state. The workaround used here with accumulating panes will no longer be needed with version 2.0 which will make use of the BEAM SDK. This sample will be updated soon to make use of BEAM 2.0.

For a production pipeline the main pipelines various stages should be written in a series of Transforms rather than DoFn's, this sample makes use of DoFn to more easily illustrate the concepts.


#### Cost of Running the samples
These samples have been setup to run locally and not require any dependencies to external resources such as Pub/Sub, BigTable or BigQuery. In a real world example you of course will be making use of these resources but before you make changes to incorporate these services please take time to understand the costs associated with this by looking at the pricing information at http://cloud.google.com.

### Detailed Pipeline Description
Available pipelines will be located at:

com.google.cloud.solutions.samples.timeseries.pipelines

## Example 1 : Financial instruments timeseries
com.google.cloud.solutions.samples.timeseries.dataflow.application.pipelines.fx.FXTimeSeriesPipelineDemo

### Running the sample
After downloading this sample you can run the example by changing to the directory created and run the example using Maven by using the following command:

mvn install

mvn exec:java Dexec.args="--runner=InProcessPipelineRunner"

### The FXTimeSeriesPipelineDemo pipeline in detail
In this example we will use Dataflow to take multiple streams of data and compute the correlation between these streams. Each stream will be aggregated into a time window which will contain the open, close, min and max positions for that time window. We will then take a sliding window to these aggregation points and compute the correlation of all the values against each other. This sliding window will compute the correlations for all values against all other values. The number of calculations per sliding window will be (n^2-n)/2 so for 1000 assets this will be 499,500 computations.


The example has 5 time series values TS-1 through to TS-5. The data that is provided for them is of the format:

 * Generate 5 timeseries values across a 10 min window TS1, TS2, TS3, TS4, TS5   
 * TS1 / TS2 will contain a single value per 1 min and will increase by a value of 1 till min 5 and then decrease by a value of 1 until 10 mins
 * TS3 will have missing values at 2, 3, 7 and 8 mins and will decrease by a value of 1 till min 5 and then increase by a value of 1 until 10 mins
 * TS4 will have missing values at 2, 3, 7 and 8 mins and will decrease by a value of 1 till min 5 and then increase by a value of 1 until 10 mins
 * TS5 will have random values assigned to it throughout

The Table below displays the values created by the Data generator (you will need to be using a mark down viewer for it to render)

| Time Window |TS-1|TS-2|TS-3|TS-4|TS-5|
| --- | --- | --- | --- | --- | --- |
| Window-00 | 1 | 1 | 10 | 10 | RND() |
| Window-01 | 2 | 2 | - | - | RND() |
| Window-02 | 3 | 3 | - | - | RND() |
| Window-03 | 4 | 4 | 7 | 7 | RND() |
| Window-04 | 5 | 5 | 6 | 6 | RND() |
| Window-05 | 5 | 5 | 6 | 6 | RND() |
| Window-06 | 4 | 4 | - | - | RND() |
| Window-07 | 3 | 3 | - | - | RND() |
| Window-08 | 2 | 2 | 9 | 9 | RND() |
| Window-09 | 1 | 1 | 10 | 10 | RND() |

The high level overview of the various stages is described below:

### STEP 1: Setup
Setup of all the configuration and variables. Please refer to FXTimeSeriesPipelineOptions for default options.

### STEP 2 : Read data
Here the values are generated to avoid having any dependencies on external sources such as Pub/Sub. Switching the sample to use Pub/Sub is a straight forward task. The work will be to convert the source to a TSProto object, the rest of the pipeline will not need any changes. It is important to note that values that are being read must have the isLive property set to TRUE. A live value is one that had been read in from an external source rather than generated internally by this pipeline to fill in gaps in the data.

At this stage we will have a PCollection with 44 elements.

### STEP 3 : Create aggregates
In this step we will fill in any missing values and aggregate the data ready for processing in the next stage. The sub steps are:

* Create a Window based on the resolution of the aggregation needed
* Create aggregations per window which calculates min/max and close for the period
* Generate values for any timeseries that does not have a value in this time window
* Complete the aggregation by bringing forward the previous windows close value to the current window

### STEP 4 : Distribute work packets
* Collapse the different streams within a time window back together and distribute across workers
* Create pairwise arrays for all of the timeseries

### STEP 5 : Process pairwise arrays with Pearson's correlation
* Compute the correlation between the values
