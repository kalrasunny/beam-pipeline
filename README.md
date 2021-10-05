
|  Tested Runners|
|--|
|  DatflowRunner|
|  SparkRunner|
|  DirectRunner|

## Using DataflowRunner

**Day 1**

    mvn compile -e exec:java \
     -Dexec.mainClass=com.raj.processor.SCD \
          -Dexec.args="--project=<YOUR_GCS_BUCKET> \
          --stagingLocation=gs://<YOUR_GCS_BUCKET>/staging/ \
          --tempLocation=gs://<YOUR_GCS_BUCKET>/staging/ \
          --input=gs://<YOUR_GCS_BUCKET>/day_1.csv \
          --existing="" \
      	  --output=gs://<YOUR_GCS_BUCKET>/dataflowrunner/complete_output/  \
          --inputType=csv \
          --outputType=csv  \
          --cdcType="scdtype2" \
          --goodRecordPath=gs://<YOUR_GCS_BUCKET>/dataflowrunner/staging/ \
          --errorPath=gs://<YOUR_GCS_BUCKET>/dataflowrunner/badrecords/ \
          --configName=gs://<YOUR_GCS_BUCKET>/dataflowrunner/original_config.properties \
          --dqOutputType=csv \
          --runner=DataflowRunner"

**Day 2**
 
 

    mvn compile -e exec:java \
     -Dexec.mainClass=com.raj.processor.SCD \
          -Dexec.args="--project=<YOUR_GCS_BUCKET> \
          --stagingLocation=gs://<YOUR_GCS_BUCKET>/staging/ \
          --tempLocation=gs://<YOUR_GCS_BUCKET>/staging/ \
          --input=gs://<YOUR_GCS_BUCKET>/day_2.csv \
          --existing=gs://<YOUR_GCS_BUCKET>/<DAY1_PROCESSED_GCS_CSV_LOCATION>
      	  --output=gs://<YOUR_GCS_BUCKET>/dataflowrunner/complete_output/  \
          --inputType=csv \
          --outputType=csv  \
          --cdcType="scdtype2" \
          --goodRecordPath=gs://<YOUR_GCS_BUCKET>/dataflowrunner/staging/ \
          --errorPath=gs://<YOUR_GCS_BUCKET>/dataflowrunner/badrecords/ \
          --configName=gs://<YOUR_GCS_BUCKET>/dataflowrunner/original_config.properties \
          --dqOutputType=csv \
          --runner=DataflowRunner"

**---------------------------------------------------------------------------**

## Using SparkRunner
     
**Day 1**

    spark-submit --class com.raj.processor.SCD gs://<YOUR_GCS_BUCKET>/b-for-beam-new-1.0-SNAPSHOT.jar  \
    --runner=SparkRunner --input=gs://<YOUR_GCS_BUCKET>/day_1.csv \
     --existing="" --output=gs://<YOUR_GCS_BUCKET>/sparkrunner/complete_output/ \
     --inputType=csv --outputType=csv --cdcType="scdtype2" \
     --goodRecordPath=gs://<YOUR_GCS_BUCKET>/sparkrunner/staging/ \
     --errorPath=gs://<YOUR_GCS_BUCKET>/sparkrunner/badrecords/ \
     --configName=gs://<YOUR_GCS_BUCKET>/sparkrunner/original_config.properties \
     --dqOutputType=csv --project=<YOUR_GCS_BUCKET>

**Day 2**

    spark-submit --class com.raj.processor.SCD gs://<YOUR_GCS_BUCKET>/b-for-beam-new-1.0-SNAPSHOT.jar \
    --runner=SparkRunner --input=gs://<YOUR_GCS_BUCKET>/day_2.csv \
    --existing=gs://<YOUR_GCS_BUCKET>/sparkrunner/complete_output/source/<DAY1_PROCESSED_GCS_CSV_LOCATION> \
    --output=gs://<YOUR_GCS_BUCKET>/sparkrunner/complete_output/ --inputType=csv \
    --outputType=csv --cdcType="scdtype2" \
    --goodRecordPath=gs://<YOUR_GCS_BUCKET>/sparkrunner/staging/ \
    --errorPath=gs://<YOUR_GCS_BUCKET>/sparkrunner/badrecords/ \
    --configName=gs://<YOUR_GCS_BUCKET>/sparkrunner/original_config.properties \
    --dqOutputType=csv --project=<YOUR_GCS_BUCKET>

**---------------------------------------------------------------------------**

## Using DirectRunner

**Day 1**

    mvn compile -e exec:java \
     -Dexec.mainClass=com.raj.processor.SCD \
          -Dexec.args="--project=<YOUR_GCS_BUCKET> \
          --stagingLocation=gs://<YOUR_GCS_BUCKET>/staging/ \
          --tempLocation=gs://<YOUR_GCS_BUCKET>/staging/ \
          --input=gs://<YOUR_GCS_BUCKET>/day_1.csv \
          --existing="" \
      	  --output=gs://<YOUR_GCS_BUCKET>/dataflowrunner/complete_output/  \
          --inputType=csv \
          --outputType=csv  \
          --cdcType="scdtype2" \
          --goodRecordPath=gs://<YOUR_GCS_BUCKET>/dataflowrunner/staging/ \
          --errorPath=gs://<YOUR_GCS_BUCKET>/dataflowrunner/badrecords/ \
          --configName=gs://<YOUR_GCS_BUCKET>/dataflowrunner/original_config.properties \
          --dqOutputType=csv \
          --runner=DirectRunner"

**Day 2**
 
 

    mvn compile -e exec:java \
     -Dexec.mainClass=com.raj.processor.SCD \
          -Dexec.args="--project=<YOUR_GCS_BUCKET> \
          --stagingLocation=gs://<YOUR_GCS_BUCKET>/staging/ \
          --tempLocation=gs://<YOUR_GCS_BUCKET>/staging/ \
          --input=gs://<YOUR_GCS_BUCKET>/day_2.csv \
          --existing=gs://<YOUR_GCS_BUCKET>/<DAY1_PROCESSED_GCS_CSV_LOCATION>
      	  --output=gs://<YOUR_GCS_BUCKET>/dataflowrunner/complete_output/  \
          --inputType=csv \
          --outputType=csv  \
          --cdcType="scdtype2" \
          --goodRecordPath=gs://<YOUR_GCS_BUCKET>/dataflowrunner/staging/ \
          --errorPath=gs://<YOUR_GCS_BUCKET>/dataflowrunner/badrecords/ \
          --configName=gs://<YOUR_GCS_BUCKET>/dataflowrunner/original_config.properties \
          --dqOutputType=csv \
          --runner=DirectRunner"

**---------------------------------------------------------------------------**
