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

import org.ohdsi.databases.ConnectionWrapper;
import org.ohdsi.databases.DbType;
import org.ohdsi.utilities.files.IniFile;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * This class analyzes the XML files and creates the appropriate database structure
 * 
 * @author Schuemie
 * 
 */
public class MedlineAnalyserMain {

	/**
	 * Specifies the maximum number of files to randomly sample.
	 */
	public static int				MAX_FILES_TO_ANALYSE	= 1000;

	private MedlineCitationAnalyser	medlineCitationAnalyser;

	public static void main(String[] args) {
		IniFile iniFile = new IniFile(args[0]);

		MedlineAnalyserMain main = new MedlineAnalyserMain();
		main.analyseFolder(iniFile.get("XML_FOLDER"));
		main.createDatabase(iniFile.get("SERVER"), iniFile.get("SCHEMA"), iniFile.get("DOMAIN"), iniFile.get("USER"), iniFile.get("PASSWORD"),
				iniFile.get("DATA_SOURCE_TYPE"));
	}

	private void analyseFolder(String folderName) {
		medlineCitationAnalyser = new MedlineCitationAnalyser();

		XMLFileIterator iterator = new XMLFileIterator(folderName, MAX_FILES_TO_ANALYSE);
		while (iterator.hasNext())
			analyse(iterator.next());

		medlineCitationAnalyser.finish();
	}

	private void analyse(Document document) {
		NodeList citationNodes = document.getElementsByTagName("MedlineCitation");
		for (int i = 0; i < citationNodes.getLength(); i++) {
			Node citation = citationNodes.item(i);
			medlineCitationAnalyser.analyse(citation);
		}
	}

	private void createDatabase(String server, String schema, String domain, String user, String password, String dateSourceType) {
		ConnectionWrapper connectionWrapper = new ConnectionWrapper(server, domain, user, password, new DbType(dateSourceType));
		connectionWrapper.use(schema);
		System.out.println("Creating tables");
		medlineCitationAnalyser.createTables(connectionWrapper);
		PmidToDate.createTable(connectionWrapper);
		connectionWrapper.close();
		System.out.println("Finished creating table structure");
	}
}
