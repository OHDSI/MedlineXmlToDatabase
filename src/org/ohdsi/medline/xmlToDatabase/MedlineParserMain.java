/*******************************************************************************
 * Copyright 2014 Observational Health Data Sciences and Informatics
 * 
 * This file is part of MedlineXmlToDatabase
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * 
 * @author Observational Health Data Sciences and Informatics
 * @author Martijn Schuemie
 ******************************************************************************/
package org.ohdsi.medline.xmlToDatabase;

import org.ohdsi.databases.DbType;
import org.ohdsi.utilities.files.IniFile;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * Main parser class. Here's where we iterate over all xml.gz files
 * 
 * @author MSCHUEMI
 *
 */
public class MedlineParserMain {

	private MedlineCitationParser	medlineCitationParser;
	private PmidToDate				pmidToDate;

	public static void main(String[] args) {
		IniFile iniFile = new IniFile(args[0]);

		MedlineParserMain main = new MedlineParserMain();
		main.parseFolder(iniFile.get("XML_FOLDER"), iniFile.get("SERVER"), iniFile.get("SCHEMA"), iniFile.get("DOMAIN"), iniFile.get("USER"),
				iniFile.get("PASSWORD"), iniFile.get("DATA_SOURCE_TYPE"));
	}

	private void parseFolder(String folder, String server, String schema, String domain, String user, String password, String dateSourceType) {
		ConnectionWrapper connectionWrapper = new ConnectionWrapper(server, domain, user, password, new DbType(dateSourceType));
		connectionWrapper.use(schema);

		medlineCitationParser = new MedlineCitationParser(connectionWrapper, schema);
		pmidToDate = new PmidToDate(connectionWrapper);

		XMLFileIterator iterator = new XMLFileIterator(folder);
		while (iterator.hasNext()) {
			Document document = iterator.next();
			analyse(document);
			pmidToDate.insertDates(document);
		}
	}

	private void analyse(Document document) {
		NodeList citationNodes = document.getElementsByTagName("MedlineCitation");
		long start = System.currentTimeMillis();
		int total = citationNodes.getLength();
		for (int i = 0; i < total; i++) {
			Node citation = citationNodes.item(i);
			medlineCitationParser.parseAndInjectIntoDB(citation);
			if (i % 10000 == 0 && i != 0)
				System.out.println(i + " of " + total + " (" + (System.currentTimeMillis() - start) + "ms since start)");
		}
		System.out.println(total + " of " + total + " (" + (System.currentTimeMillis() - start) + "ms since start)");

		NodeList deleteNodes = document.getElementsByTagName("DeleteCitation");
		if (deleteNodes.getLength() != 0) {
			System.out.println("Deleting obsolete records");
			for (int i = 0; i < deleteNodes.getLength(); i++) {
				Node delete = deleteNodes.item(i);
				medlineCitationParser.delete(delete);
			}
		}
	}
}
