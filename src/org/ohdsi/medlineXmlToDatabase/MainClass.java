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

import org.ohdsi.meshXmlToDatabase.MeshParserMain;

/**
 * The main class that parses the command line arguments, and calls either MedlineAnalyserMain or MedlineParserMain
 * @author MSCHUEMI
 *
 */
public class MainClass {

	private static enum Action {
		ANALYSE, PARSE, PARSE_MESH
	};

	private static Action	action;
	private static String	pathToIniFile;

	public static void main(String[] args) {
		parseParameters(args);
		if (action == Action.ANALYSE)
			MedlineAnalyserMain.main(new String[] { pathToIniFile });
		else if (action == Action.PARSE)
			MedlineParserMain.main(new String[] { pathToIniFile });
		if (action == Action.PARSE_MESH)
			MeshParserMain.main(new String[] { pathToIniFile });
	}

	private static void parseParameters(String[] args) {
		String mode = null;
		for (String arg : args) {
			if (arg.startsWith("-")) {
				if (arg.toLowerCase().equals("-analyse"))
					action = Action.ANALYSE;
				else if (arg.toLowerCase().equals("-parse"))
					action = Action.PARSE;
				else if (arg.toLowerCase().equals("-parse_mesh"))
					action = Action.PARSE_MESH;
				else
					mode = arg.toLowerCase();
			} else {
				if (mode.equals("-ini"))
					pathToIniFile = arg;
				mode = null;
			}
		}
	}
}
