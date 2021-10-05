package com.raj.processor;


import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.math.BigDecimal;
import java.nio.channels.Channels;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.extensions.joinlibrary.Join;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileIO.ReadableFile;
import org.apache.beam.sdk.io.FileSystem;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sample;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//import GenericOptions;
//import com.raj.utils.Utils;

public class SCD {
	private static final String YYYY_MM_DD_HH_MM_SS3 = "yyyy_MM_dd_HH_mm_ss";
	private static final String YYYY_MM_DD_HH_MM_SS2 = "yyyy-MM-dd_HH:mm:ss";
	private static final String INT = "int";
	private static final String DECIMAL = "decimal";
	private static final String LONG = "long";
	private static final String BOOLEAN = "boolean";
	private static final String FLOAT = "float";
	private static final String DOUBLE = "double";
	private static final String LEFT_NULLVALUE = "LEFT_NULLVALUE";
	private static final String RIGHT_NULLVALUE = "RIGHT_NULLVALUE";
	private static final String MAX_TIMESTAMP_VALUE = "9999-12-31 23:59:59";
	private static final Logger LOG = LoggerFactory.getLogger(SCD.class);

	private static Schema SCHEMA = null;
	private static Integer batchSize = null;
	private static Map<String, String> newColumnDetails = null;
  
	public static void main(String[] args) throws InterruptedException, IOException {
		
		GenericOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(GenericOptions.class);
		Pipeline dataQualityPipeline = Pipeline.create(options);
		System.out.println("Options are: " + options.toString());
		newColumnDetails = Utils.getColumnDetails();
		batchSize = Integer.valueOf(options.getErrorCommitBatchSize());
		
		PCollection<String> inputRead = null; 
		PCollection<String> finalResult =null;
		PCollection<String> goodRecords = null;
		String inputType = options.getInputType();
		
			inputRead = reader(options, dataQualityPipeline, inputRead, inputType);
			String format = LocalDateTime.now().format(DateTimeFormatter.ofPattern(YYYY_MM_DD_HH_MM_SS3)).toString();
			dqChecks(inputRead, options,format);
			dataQualityPipeline.run().waitUntilFinish();
			
			/**
			 * Creating new Pipeline object, this is to separate the dq and processing stages. If clubbed, error pops-up
			 */
			Pipeline dataProcessingPipeline = Pipeline.create(options);
			//processing
			finalResult = cdcProcessing(options, goodRecords, format, dataProcessingPipeline);
			print(finalResult,"FinalBeforeSave");
			
			//Export the result
			outExporter(options, finalResult, format);
			dataProcessingPipeline.run().waitUntilFinish();

	}

	private static void outExporter(GenericOptions options, PCollection<String> finalResult, String format) {
		String outputType = options.getOutputType();
		switch (outputType) {
		case "parquet":
			writeAsParquet(options, finalResult,false,format);
			break;
			
		case "csv":
			System.out.println("Writing final Result into: " + options.getOutput()+"source/"+format+"/");
			finalResult.apply("",FileIO.<String>write().to(options.getOutput()+"source/"+format+"/").withSuffix(".csv").via(TextIO.sink()).withNumShards(1));
			break;
	
		default:
			break;
		}
	}

	private static PCollection<String> cdcProcessing(GenericOptions options,
			PCollection<String> goodRecords, String format, Pipeline pipeline_new) {
		PCollection<String> finalResult = pipeline_new.apply(Create.empty(StringUtf8Coder.of()));
		String newFormat;
		PCollection<String> existing;
		String cdcType = options.getCdcType();
		switch (cdcType) {
		case "scdtype2":
			if(options.getDqOutputType().equals("csv")){
				goodRecords = pipeline_new.apply("Find files", FileIO.match().filepattern(options.getGoodRecordPath()+format+"/*"))
						.apply("Read matched files", FileIO.readMatches())
						.apply("ReadFiles", TextIO.readFiles());
			}
			//print(goodRecords,"FromStagingLocation");
			if(null != options.getExisting() && !"".equals(options.getExisting())) {
				existing = pipeline_new.apply("Find files", FileIO.match().filepattern(options.getExisting()))
				.apply("Read matched files", FileIO.readMatches())
				.apply("ReadFiles_", TextIO.readFiles());
				PCollection<String> existingWithoutAudit = existing.apply("FilteroutAuditColumns",ParDo.of(new DoFn<String, String>(){
					@ProcessElement
					public void processElement(ProcessContext pc){
						String row = pc.element();
						String[] rowSplit = row.split(",");
						StringBuffer sb = new StringBuffer();
						for(int i=0; i<newColumnDetails.keySet().size(); i++) {
							sb.append(rowSplit[i]+",");
						}
						pc.output(sb.toString().substring(0, sb.toString().length()-1));
					}
				}));
				print(existingWithoutAudit,"without Audit columns");
				finalResult = joinDataSets(goodRecords, existingWithoutAudit);
			} else {
				finalResult =  goodRecords.apply("FirstDayLoad",ParDo.of(new DoFn<String,String>(){
					@ProcessElement
					public void processElement(ProcessContext pc){
						pc.output(pc.element()+","+LocalDateTime.now().format(DateTimeFormatter.ofPattern(YYYY_MM_DD_HH_MM_SS2))+","+MAX_TIMESTAMP_VALUE+",I");
					}
				}));
			}
			
			
			break;
	
		default:
			break;
		}
		return finalResult;
	}

	private static PCollection<String> reader(GenericOptions options, Pipeline pipeline, PCollection<String> inputRead,
			String inputType) {
		switch (inputType) {
		case "parquet":
			//Read 
		    createAvroSchema();
		    PCollection<GenericRecord> parquetRead = pipeline.apply(ParquetIO.read(SCHEMA).from(options.getInput()));
		    inputRead =parquetRead.apply(ParDo.of(new DoFn<GenericRecord, String>() {
		    	@ProcessElement
		    	public void processElement(ProcessContext pc) {
		    		pc.output(pc.element().toString());
		    	}
		    }));
			break;
		case "csv":
		    inputRead = pipeline.apply(TextIO.read().from(options.getInput()));
			break;
		}
		return inputRead;
	}

private static void writeAsParquet(GenericOptions options, PCollection<String> inputRead, Boolean isIntermediate, String format) {
	createAvroSchema();
	PCollection<GenericRecord> abc = inputRead.apply(ParDo.of(new DoFn<String, GenericRecord>(){
		@ProcessElement
		public void processElement(ProcessContext pc){
			GenericRecordBuilder genericRecordBuilder = new GenericRecordBuilder(SCHEMA);
			int counter = 0;
			String[] split = pc.element().split(",");
			for (String field : newColumnDetails.keySet()) {
				System.out.println("element is: "+pc.element() + "|||| field: "+ field +" - value: "+split[counter]);
				String datatype = newColumnDetails.get(field);
				if(datatype.equals(INT)){
					genericRecordBuilder.set(field, Integer.parseInt(split[counter]));	
				} 
				else if(datatype.equals(DOUBLE)){
					genericRecordBuilder.set(field, Double.parseDouble(split[counter]));	
				} else {
					genericRecordBuilder.set(field, split[counter]);
				}
		        counter++;
		      }
			System.out.println("after : "+pc.element() + "This is the genericRecordBuilder.build(): " +  genericRecordBuilder.build());
			 pc.output(genericRecordBuilder.build());
		}
	}));
	String writePath =  "";
	if(isIntermediate){
		writePath =  options.getGoodRecordPath()+format;
	}
	else {
		writePath =  options.getOutput()+"abc";
	}
	System.out.println("THis is the path constructed: "+ writePath + " flag is: "+isIntermediate+" and the format is: " + format);
	abc.setCoder(AvroCoder.of(SCHEMA)).apply(
	        "Write Parquet files",
	        FileIO.<GenericRecord>write().via(ParquetIO.sink(SCHEMA)).to(writePath));
}

private static void createAvroSchema() {
	Map<String, String> columns = Utils.getColumnDetails();
	StringBuffer sb = new StringBuffer("{\n"
    	      + " \"namespace\": \"ioitavro\",\n"
    	      + " \"type\": \"record\",\n"
    	      + " \"name\": \"TestAvroLine\",\n"
    	      + " \"fields\": [\n");
    for (String col : newColumnDetails.keySet()) {
    	sb.append("{ \"name\" : \""+col+"\", \"type\":\""+newColumnDetails.get(col)+"\"},\n");
	}
    String string = sb.toString().substring(0,sb.toString().length()-2);
    string=string+" ]\n"
      + "}";
    System.out.println("----- ---Constructed schema: "+string);
    SCHEMA  = new Schema.Parser().parse(string);
}

	private static void dqChecks(PCollection<String> incoming, GenericOptions options, String format) {
		Map<String, String> primaryColumns = Utils.getPrimaryColumns();
		//List<Integer> pkIndex = Utils.getPkIndex(primaryColumns, new ArrayList<>(columns.keySet()));
		final TupleTag<String> goodRecords = new TupleTag<String>(){};
		final TupleTag<String> badRecords = new TupleTag<String>(){};

		PCollectionTuple mixedCollection =
				incoming.apply(ParDo
		        .of(dqCheck(newColumnDetails, badRecords))
		        .withOutputTags(goodRecords,
		                        TupleTagList.of(badRecords)));
		PCollection<String> goodRecordsPColl = mixedCollection.get(goodRecords);
		print(goodRecordsPColl," Good Records ");
		switch (options.getDqOutputType()) {
		case "csv":
			//goodRecordsPColl.apply("WriteGoodRecords",TextIO.write().to(options.getGoodRecordPath()+"/"+format+"/").withoutSharding());
			goodRecordsPColl.apply("",FileIO.<String>write().to(options.getGoodRecordPath()+format+"/").withNumShards(1).withSuffix(".csv")
			           .via(TextIO.sink()));
			break;
		case "parquet":
			writeAsParquet(options, goodRecordsPColl, true, format);
			break;
		default:
			break;
		}
		
		PCollection<String> badRecordsPColl = mixedCollection.get(badRecords);
		print(badRecordsPColl,"  Bad Records ");
		badRecordsPColl.apply("WriteBadRecordsToAFile",FileIO.<String>write().to(options.getErrorPath()+format).withSuffix(".txt")
		           .via(TextIO.sink()));
		
		
	}

	private static DoFn<String, String> dqCheck(Map<String, String> columns, final TupleTag<String> badRecords) {
		return new DoFn<String, String>() {
		  @ProcessElement
		  public void processElement(ProcessContext pc) {
				String[] split = pc.element().split(",");
				List<String> errorList = new ArrayList<>();
				if(split.length != columns.size()) {
					pc.output(badRecords,pc.element()+",error,NO_COLUMN-column count doesnt match");
				} else {
					Set<String> colKeySet = columns.keySet();
					for(int i=0; i< colKeySet.size(); i++){
						
						String colValue = split[i];
						String columnName = new ArrayList<>(colKeySet).get(i);
						String columndDataType = columns.get(columnName);
							switch (columndDataType) {
							case INT:
								try {
									Integer.parseInt(colValue);
								} catch (Exception e) {	errorOut(errorList, columnName, INT);	}
								break;
							case DECIMAL:
									if(!decimalCheck(colValue))
										errorOut(errorList, columnName, DECIMAL);
								break;
							case BOOLEAN:
									if(!Boolean.parseBoolean(colValue))
										errorOut(errorList, columnName, BOOLEAN);
								break;
							case LONG:
								try {
									Long.parseLong(colValue);
								} catch (Exception e) {	errorOut(errorList, columnName, LONG);	}
								break;
							case FLOAT:
								if(!decimalCheck(colValue))
									errorOut(errorList, columnName, FLOAT);
								break;
							case DOUBLE:
								if(!decimalCheck(colValue))
									errorOut(errorList, columnName, DOUBLE);
								break;
							}
							if(columndDataType.startsWith("date<") || columndDataType.startsWith("timestamp<")){
								Matcher m = Pattern.compile("\\<(.*?)\\>").matcher(columndDataType);
								while(m.find()) {
									try{
								    	LocalDate.parse(colValue, DateTimeFormatter.ofPattern(m.group(1)));
								    } catch(Exception e) {
								    	e.printStackTrace();
								    	errorOut(errorList, columnName, "date");
								    }
								}
							}
							
						//System.out.println("Column Name: "+columnName+" | Column DataType: "+columndDataType+ " | ColumnVal: "+ colValue +" errrorlist size: "+ errorList.size()+" colKeySet.size(): "+ colKeySet.size() + " --> i value is: " + i);
						if(colKeySet.size()-1 == i) {
							if(!errorList.isEmpty()) {
								pc.output(badRecords,pc.element() +",error,"+ String.join("|", errorList));
							} else {
								pc.output(pc.element());
							}
						}
					}
				}
		  }

		private Boolean errorOut(List<String> errorList, String columnName, String datatype) {
			Boolean dirtyDataFlag;
			dirtyDataFlag = Boolean.TRUE;
			errorList.add(columnName+"-"+datatype+" DataTypeError");
			return dirtyDataFlag;
		}

		private Boolean decimalCheck(String colValue) {
			int precision = 2;
			int scale = 2;
			try {
				BigDecimal decimal = new  BigDecimal(colValue);
				if(decimal.precision() > precision && decimal.scale() > scale) {
					return Boolean.FALSE;
				} else 
					return Boolean.TRUE;
			} catch (Exception e) {
				return Boolean.FALSE;
			}
		}
		};
	}
	
	private static void print(PCollection<String> pcollection, final String forwhat) {
		pcollection.apply(forwhat,ParDo.of(new DoFn<String, String>(){
			@ProcessElement
			public void processElement(ProcessContext pc ) {
				System.out.println(forwhat + " ---- " + pc.element());
			}
		}));
	}
	
	private static PCollection<String> joinDataSets(PCollection<String> incoming, PCollection<String> existing) {
		Map<String, String> primaryColumns = Utils.getPrimaryColumns();
		List<Integer> pkIndex = Utils.getPkIndex(primaryColumns, new ArrayList<>(newColumnDetails.keySet()));
		
		//Read content in KV form, key being primarykey(s) & value entire row
		ReadFileInKVForm fileConntet = new ReadFileInKVForm();
		fileConntet.setPkIndex(pkIndex);
		 
		PCollection<KV<String, String>> incomingInput = incoming.apply(ParDo.of(fileConntet));
		//print(incomingInput);
		//Remove duplicates
		PCollection<KV<String, String>> incomingDeDup = deDup(incomingInput);
		incomingDeDup.apply(Sample.any(20));
		
		//Existing content read:
		PCollection<KV<String, String>> existingContent = existing.apply(ParDo.of(fileConntet));
		
		
		PCollection<KV<String, KV<String, String>>> fullOuterJoin = Join.fullOuterJoin(existingContent, incomingDeDup, LEFT_NULLVALUE, RIGHT_NULLVALUE);
		
		PCollection<KV<String, String>> finalSCDTypeResult = newFilter(fullOuterJoin);
		
		PCollection<String> finalResult = print(finalSCDTypeResult);
		return finalResult;
	}
	
	private static PCollection<String> print(PCollection<KV<String, String>> finalSCDTypeResult) {
		return finalSCDTypeResult.apply("PrintSCDTypeResult",ParDo.of(new DoFn<KV<String, String>, String>(){
			@ProcessElement
			public void processElement(ProcessContext pc ) {
				System.out.println("SCDTYpe Result: " +  pc.element().getValue());
				pc.output(pc.element().getValue());
			}
		}));
	}

	private static PCollection<KV<String, String>> newFilter(PCollection<KV<String, KV<String, String>>> fullOuterJoin) {
		LocalDateTime now = LocalDateTime.now();
		String startTime = now.format(DateTimeFormatter.ofPattern(YYYY_MM_DD_HH_MM_SS2));
		final String currEndDate = startTime;
		
		PCollection<KV<String, String>> output = fullOuterJoin.apply("LabelSCDType2Records",ParDo.of(new DoFn<KV<String, KV<String, String>>, KV<String, String>>(){
			@ProcessElement
			public void processElement(ProcessContext pc) {
				//Its new Record
				if(LEFT_NULLVALUE.equals(pc.element().getValue().getKey())){
					pc.output(KV.of(pc.element().getKey(),pc.element().getValue().getValue()+","+startTime+","+MAX_TIMESTAMP_VALUE+",I"));
				}
				if(RIGHT_NULLVALUE.equals(pc.element().getValue().getValue())){
					pc.output(KV.of(pc.element().getKey(),pc.element().getValue().getKey()+","+startTime+","+MAX_TIMESTAMP_VALUE+",D"));
					pc.output(KV.of(pc.element().getKey(),pc.element().getValue().getKey()+","+startTime+","+currEndDate+",U"));
				}
				//its updated Record
				if(!LEFT_NULLVALUE.equals(pc.element().getValue().getKey()) && !RIGHT_NULLVALUE.equals(pc.element().getValue().getValue())){
					pc.output(KV.of(pc.element().getKey(),pc.element().getValue().getValue()+","+startTime+","+MAX_TIMESTAMP_VALUE+",U"));
					pc.output(KV.of(pc.element().getKey(),pc.element().getValue().getKey()+","+startTime+","+currEndDate+",U"));
				}
			}
		}));
		 return output;
	}

	private static PCollection<KV<String, String>> deDup(PCollection<KV<String, String>> incomingInput) {
		final TupleTag<String> incomingTupleTag = new TupleTag<String>();

        PCollection<KV<String, CoGbkResult>> kvpCollection = KeyedPCollectionTuple
                .of(incomingTupleTag, incomingInput)
                .apply(CoGroupByKey.<String>create());

        PCollection<KV<String, String>> finalResultCollection =
                kvpCollection.apply(ParDo.of(
                        new DoFn<KV<String, CoGbkResult>, KV<String, String>>() {
                            @ProcessElement
                            public void processElement(ProcessContext c) {
                                KV<String, CoGbkResult> e = c.element();
                                Iterator<String> iterator = e.getValue().getAll(incomingTupleTag).iterator();
                                if(iterator.hasNext()) {
                                	c.output(KV.of(e.getKey(), iterator.next()));
                                }
                            }
                        }));
		return finalResultCollection;
	}

	static class ReadFileInKVForm extends DoFn<String, KV<String, String>> {
		private List<Integer> pkIndex;
		public List<Integer> getPkIndex() {
			return pkIndex;
		}
		public void setPkIndex(List<Integer> pkIndex) {
			this.pkIndex = pkIndex;
		}
		
		@ProcessElement
		public void processElement(ProcessContext pc){
			String key = "";
			String[] row = pc.element().split(",");
			for (Integer integer : pkIndex) {
				//System.out.println("row:"+row);
				if(key.isEmpty()){
					key=row[integer];
					if(pkIndex.size() == 1) {
						pc.output(KV.of(key, pc.element()));
					}
				} else {
					key = key +"-"+row[integer];
					pc.output(KV.of(key, pc.element()));
				}
			}
		}
	}
	
	private static Date getMaxDaate(GenericOptions options) {
		File source = new File(options.getOutput()+"source/"); 
		File[] list = source.listFiles();
		System.out.println("list: " + list);
		List<Date> listDate = new ArrayList<>();
		for (File eachFile : list) {
			//System.out.println("Name: " + eachFile.getName() + " AbsolutePath: " + eachFile.getAbsolutePath() + " directory: " + eachFile.isDirectory() + " isHidden: " + eachFile.isHidden());
			if(eachFile.isDirectory() && !eachFile.isHidden()) {
				LocalDateTime dateTime = LocalDateTime.parse(eachFile.getName(), DateTimeFormatter.ofPattern(YYYY_MM_DD_HH_MM_SS3));
				System.out.println(":::::: LocalDate: " + dateTime);
				
				Date from = java.util.Date
				      .from(dateTime.atZone(ZoneId.systemDefault())
				      .toInstant());
				listDate.add(from);
			}
		} 
		if(!listDate.isEmpty()) {
			Date maxDate = listDate.stream().max(Date::compareTo).get();
			System.out.println("Max Date is: " + maxDate);
			return maxDate;
		} else
			return null;
		
	}
}
