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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.zip.GZIPInputStream;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.ohdsi.utilities.RandomUtilities;
import org.ohdsi.utilities.concurrency.BatchProcessingThread;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

/**
 * Iterates over all xml.gz files in a specified folder, decompressing and parsing them in a separate thread.
 * 
 * @author Schuemie
 * 
 */
public class XMLFileIterator implements Iterator<Document> {
	
	private Iterator<File>				fileIterator;
	private DecompressAndParseThread	decompressAndParseThread	= new DecompressAndParseThread();
	private boolean						hasNext						= true;
	
	/**
	 * @param folder
	 *            Specifies the absolute path to the folder containing the xml files
	 */
	public XMLFileIterator(String folder) {
		this(folder, Integer.MAX_VALUE);
	}
	
	/**
	 * 
	 * @param folder
	 *            Specifies the absolute path to the folder containing the xml files
	 * @param sampleSize
	 *            Specifies the maximum number of files that is randomly sampled
	 */
	public XMLFileIterator(String folder, int sampleSize) {
		List<File> files = new ArrayList<File>();
		for (File file : new File(folder).listFiles())
			if (file.getAbsolutePath().endsWith("xml.gz"))
				files.add(file);
		files = RandomUtilities.sampleWithoutReplacement(files, sampleSize);
		Collections.sort(files, new Comparator<File>() {
			@Override
			public int compare(File o1, File o2) {
				return o1.getName().compareTo(o2.getName());
			}
		});
		fileIterator = files.iterator();
		if (fileIterator.hasNext())
			decompressAndParseThread.startProcessing(fileIterator.next());
		else {
			hasNext = false;
			decompressAndParseThread.terminate();
		}
	}
	
	@Override
	public boolean hasNext() {
		return hasNext;
	}
	
	@Override
	public Document next() {
		decompressAndParseThread.waitUntilFinished();
		Document document = decompressAndParseThread.getDocument();
		if (fileIterator.hasNext())
			decompressAndParseThread.startProcessing(fileIterator.next());
		else {
			hasNext = false;
			decompressAndParseThread.terminate();
		}
		return document;
	}
	
	@Override
	public void remove() {
		throw new RuntimeException("Calling unimplemented method remove in " + this.getClass().getName());
	}
	
	private class DecompressAndParseThread extends BatchProcessingThread {
		
		private File		file;
		private Document	document;
		
		public void startProcessing(File file) {
			this.file = file;
			proceed();
		}
		
		public Document getDocument() {
			return document;
		}
		
		@Override
		protected void process() {
			System.out.println("Processing " + file.getName());
			try {
				FileInputStream fileInputStream = new FileInputStream(file);
				GZIPInputStream gzipInputStream = new GZIPInputStream(fileInputStream);
				DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
				document = builder.parse(gzipInputStream);
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			} catch (ParserConfigurationException e) {
				e.printStackTrace();
			} catch (SAXException e) {
				e.printStackTrace();
			}
		}
	}
}
