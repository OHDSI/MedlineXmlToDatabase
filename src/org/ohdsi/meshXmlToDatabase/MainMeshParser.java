package org.ohdsi.meshXmlToDatabase;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.ohdsi.databases.InsertableDbTable;
import org.ohdsi.utilities.StringUtilities;
import org.ohdsi.utilities.files.Row;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

public class MainMeshParser extends DefaultHandler {

	private InsertableDbTable	outTerms;
	private InsertableDbTable	outRelationship;
	private Map<String, String>	treeNumberToUi;
	private Row row;
	private Trace trace = new Trace();
	private String ui;
	
	public MainMeshParser(InsertableDbTable outTerms, InsertableDbTable outRelationship, Map<String, String>	treeNumberToUi) {
		super();
		this.outRelationship = outRelationship;
		this.outTerms = outTerms;
		this.treeNumberToUi = treeNumberToUi;
	}

	public static void parse(String fileName, InsertableDbTable outTerms, InsertableDbTable outRelationship, Map<String, String>	treeNumberToUi) {
		StringUtilities.outputWithTime("Parsing main file");
		try {
			FileInputStream fileInputStream = new FileInputStream(fileName);
			GZIPInputStream gzipInputStream = new GZIPInputStream(fileInputStream);
			MainMeshParser mainMeshParser = new MainMeshParser(outTerms, outRelationship, treeNumberToUi);
			SAXParserFactory factory = SAXParserFactory.newInstance();
			SAXParser saxParser = factory.newSAXParser();
			saxParser.parse(gzipInputStream, mainMeshParser);
		} catch (org.xml.sax.SAXException e) {
			e.printStackTrace();
		} catch (ParserConfigurationException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void startElement(String uri, String localName, String name, Attributes a) {
		trace.push(name);
		if (name.equalsIgnoreCase("DescriptorRecord")) 
			row = new Row();
	}
	

	public void characters(char ch[], int start, int length) throws SAXException {
		String traceString = trace.toString();
		if (traceString.equalsIgnoreCase("DescriptorRecordSet.DescriptorRecord.DescriptorUI")) {
			ui = new String(ch, start, length);
			row.add("ui", ui);
		} else if (traceString.equalsIgnoreCase("DescriptorRecordSet.DescriptorRecord.DescriptorName.String")) {
			row.add("name", new String(ch, start, length));
		} else if (traceString.equalsIgnoreCase("DescriptorRecordSet.DescriptorRecord.TreeNumberList.TreeNumber")) {
			treeNumberToUi.put(new String(ch, start, length), ui);
		} else if (traceString.equalsIgnoreCase("DescriptorRecordSet.DescriptorRecord.PharmacologicalActionList.PharmacologicalAction.DescriptorReferredTo.DescriptorUI")) {
			Row rowPa = new Row();
			rowPa.add("ui_1", ui);
			rowPa.add("ui_2", new String(ch, start, length));
			rowPa.add("relationship_id", "Pharmacological action");
			outRelationship.write(rowPa);
		}
	}

	public void endElement(String uri, String localName, String name) {
		trace.pop();
		if (name.equalsIgnoreCase("DescriptorRecord")) {
			row.add("supplement", "0");
			outTerms.write(row);
		}
	}
	
	private class Trace {
		private List<String> tags = new ArrayList<String>();
		
		public void push(String tag) {
			tags.add(tag);
		}
		
		public void pop() {
			tags.remove(tags.size()-1);
		}
		
		public String toString() {
			return StringUtilities.join(tags, ".");
		}
	}

}
