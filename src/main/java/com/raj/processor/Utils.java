package com.raj.processor;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Utils {
	
	public static Map<String, String> getColumnDetails() {
		Map<String, String> columns = new LinkedHashMap<>();
		columns.put("id", "int");
		columns.put("prod", "string");
		columns.put("desc", "string");
		columns.put("price", "double");
		return columns;
	}
	
	public static Map<String, String> getPrimaryColumns() {
		Map<String, String> primCols = new LinkedHashMap<>();
		primCols.put("id", "INT");
		//primCols.put("prod", "string");
		return primCols;
	}
	
	public static List<Integer> getPkIndex(Map<String, String> primCols, List<String> list) {
		
		List<Integer> indexList = primCols.keySet().stream().map(i -> {
			if(list.contains(i)) {
				return list.indexOf(i);
			}
			return -1;
			}).collect(Collectors.toList());
		return indexList;
	}
	
	
	public static Map<String, String> getNewColumnDetails(String path) {
		Map<String, String> columns = new LinkedHashMap<>();
	        try {
	            Stream<String> lines = Files.lines(Paths.get(path));
	            lines.forEach(i -> {
	            	System.out.println("String value is: "+i);
	            	String [] split = i.split("=");
	            	columns.put(split[0], split[1]);
	            	});
	            lines.close();
	        } catch(IOException io) {
	            io.printStackTrace();
	        }
	        return columns;
	    }

}
