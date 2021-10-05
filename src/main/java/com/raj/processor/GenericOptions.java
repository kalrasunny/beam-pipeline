package com.raj.processor;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.io.hdfs.HadoopFileSystemOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

public interface GenericOptions  extends PipelineOptions, HadoopFileSystemOptions,DataflowPipelineOptions  {


	String getInput();

	void setInput(String value);
	
	String getInputType();

	void setInputType(String value);
	
	String getOutputType();

	void setOutputType(String value);

	String getOutput();

	void setOutput(String value);

	String getErrorPath();

	void setErrorPath(String value);
	
	String getCdcType();

	void setCdcType(String value);
	
	String getGoodRecordPath();
	void setGoodRecordPath(String value);
	@Description("Existing Path")
    String getExisting();

    void setExisting(String value);
	
     
     @Description("Config Path")
     String getConfigName();

     void setConfigName(String value);
     
     @Description("DQ Output type")
     String getDqOutputType();

     void setDqOutputType(String value);

     @Description("DQ error batch size")
     @Default.String("1")
     String getErrorCommitBatchSize();

     void setErrorCommitBatchSize(String value);

}