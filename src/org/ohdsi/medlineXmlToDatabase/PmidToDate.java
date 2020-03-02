/*******************************************************************************
 * Copyright 2014 Observational Health Data Sciences and Informatics
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package org.ohdsi.medlineXmlToDatabase;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.ohdsi.databases.ConnectionWrapper;
import org.ohdsi.utilities.XmlTools;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * This class is used to create and populate a derived table with publication dates. In the original Medline XML the publication needs to be constructed by
 * combining several fields.
 * 
 * @author mschuemi
 */
public class PmidToDate {
	
	private List<String>		months		= getMonths();
	private Pattern				yearPattern	= Pattern.compile("(19|20)[0-9][0-9]");
	public static int			BATCH_SIZE	= 1000;
	private static String		tableName	= "pmid_to_date";
	private ConnectionWrapper	connectionWrapper;
	
	public PmidToDate(ConnectionWrapper connectionWrapper) {
		this.connectionWrapper = connectionWrapper;
		connectionWrapper.setDateFormat();
	}
	
	public static void createTable(ConnectionWrapper connectionWrapper) {
		List<String> fields = new ArrayList<String>();
		List<String> types = new ArrayList<String>();
		fields.add("pmid");
		types.add("int");
		
		fields.add("pmid_version");
		types.add("int");
		
		fields.add("date");
		types.add("date");
		
		List<String> primaryKey = new ArrayList<String>();
		primaryKey.add("PMID");
		primaryKey.add("PMID_Version");
		
		connectionWrapper.createTable(tableName, fields, types, primaryKey);
	}
	
	private DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH);
	
	public void insertDates(Document document) {
		connectionWrapper.setBatchMode(true);
		NodeList citationNodes = document.getElementsByTagName("MedlineCitation");
		for (int i = 0; i < citationNodes.getLength(); i++) {
			Node citation = citationNodes.item(i);
			Node pmidNode = XmlTools.getChildByName(citation, "PMID");
			String pmid = XmlTools.getValue(pmidNode);
			String pmid_version = XmlTools.getAttributeValue(pmidNode, "Version");
			
			// Could be an update, so delete old record just to be sure:
			connectionWrapper.execute("DELETE FROM pmid_to_date WHERE pmid = " + pmid + " AND pmid_version = " + pmid_version);
			
			Node articleDateNode = XmlTools.getChildByName(XmlTools.getChildByName(citation, "Article"), "ArticleDate");
			String articleYearString; 
			String articleMonthString; 
			String articleDayString; 
			if (articleDateNode == null) {
				articleYearString = null;
				articleMonthString = null;
				articleDayString = null;
			} else {
				articleYearString = XmlTools.getChildByNameValue(articleDateNode, "Year");
				articleMonthString = XmlTools.getChildByNameValue(articleDateNode, "Month");
				articleDayString = XmlTools.getChildByNameValue(articleDateNode, "Day");
			}
			Node pubDateNode = XmlTools.getChildByName(
					XmlTools.getChildByName(XmlTools.getChildByName(XmlTools.getChildByName(citation, "Article"), "Journal"), "JournalIssue"), "PubDate");
			String pubYearString = XmlTools.getChildByNameValue(pubDateNode, "Year");
			String pubMonthString = XmlTools.getChildByNameValue(pubDateNode, "Month");
			String pubDayString = XmlTools.getChildByNameValue(pubDateNode, "Day");
			String medlineString = XmlTools.getChildByNameValue(pubDateNode, "MedlineDate");
			String date = parseDate(articleYearString, articleMonthString, articleDayString, pubYearString, pubMonthString, pubDayString, medlineString);
			// if (pmid.equals("27904912"))
			// System.out.println("asdfsadf");
			try {
				date = dateFormat.format(dateFormat.parse(date));
			} catch (ParseException e) {
				System.err.println("Error parsing date with\n" + "article year = '" + articleYearString + "', month = '" + articleMonthString + "', day = '"
						+ articleDayString + "\n" + "pub year = '" + pubYearString + "', month = '" + pubMonthString + "', day = '" + pubDayString + "\n"
						+ "', medline date = '" + medlineString + "'");
				date = null;
			}
			if (date == null) {
				System.err.println("No valid date found for PMID " + pmid);
			} else {
				Map<String, String> field2Value = new HashMap<String, String>();
				field2Value.put("pmid", pmid);
				field2Value.put("pmid_version", pmid_version);
				field2Value.put("date", date);
				connectionWrapper.insertIntoTable(tableName, field2Value);
			}
		}
		connectionWrapper.setBatchMode(false);
	}
	
	private static List<String> getMonths() {
		List<String> result = new ArrayList<String>(12);
		result.add("Jan");
		result.add("Feb");
		result.add("Mar");
		result.add("Apr");
		result.add("May");
		result.add("Jun");
		result.add("Jul");
		result.add("Aug");
		result.add("Sep");
		result.add("Oct");
		result.add("Nov");
		result.add("Dec");
		return result;
	}
	
	private String parseDate(String articleYearString, String articleMonthString, String articleDayString, String pubYearString, String pubMonthString,
			String pubDayString, String medlineString) {
		String year = null;
		if (articleYearString == null) {
			if (pubYearString == null) {
				if (medlineString == null) {
					return null;
				} else {
					Matcher matcher = yearPattern.matcher(medlineString);
					if (matcher.find())
						year = matcher.group();
				}
			} else {
				year = pubYearString;
			}
		} else {
			year = articleYearString;
		}
		String month = null;
		if (articleMonthString == null) {
			if (pubMonthString == null) {
				month = "1";
				if (medlineString != null) {
					for (int i = 0; i < months.size(); i++) {
						if (medlineString.contains(months.get(i))) {
							month = Integer.toString(i + 1);
							break;
						}
					}
				}
			} else {
				try {
					month = Integer.toString(Integer.parseInt(pubMonthString));
				} catch (NumberFormatException e) {
					month = Integer.toString(months.indexOf(pubMonthString) + 1).toString();
				}
			}
			
		} else {
			try {
				month = Integer.toString(Integer.parseInt(articleMonthString));
			} catch (NumberFormatException e) {
				month = Integer.toString(months.indexOf(articleMonthString) + 1).toString();
			}
		}
		String day;
		if (articleDayString == null) {
			if (pubDayString == null) {
				day = "1";
			} else {
				day = pubDayString;
			}
		} else {
			day = articleDayString;
		}
		return year + "-" + month + "-" + day;
	}
}
