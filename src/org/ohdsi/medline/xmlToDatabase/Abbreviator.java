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

import java.util.ArrayList;
import java.util.List;

import org.ohdsi.utilities.collections.Pair;
import org.ohdsi.utilities.files.ReadCSVFileWithHeader;
import org.ohdsi.utilities.files.Row;

/**
 * Abbreviates some table names so they can fit in the DB
 * @author MSCHUEMI
 *
 */
public class Abbreviator {
	public static List<Pair<String, String>>	termToAbbr	= loadTermToAbbr();

	public static String abbreviate(String name) {
		name = name.toLowerCase();
		for (Pair<String, String> pair : termToAbbr)
			name = name.replaceAll(pair.getItem1(), pair.getItem2());
		return name;
	}

	private static List<Pair<String, String>> loadTermToAbbr() {
		List<Pair<String, String>> termToAbbr = new ArrayList<Pair<String, String>>();
		for (Row row : new ReadCSVFileWithHeader(Abbreviator.class.getResourceAsStream("abbreviations.csv")))
			termToAbbr.add(new Pair<String, String>(row.get("term"), row.get("abbreviation")));
		return termToAbbr;
	}

	public static String unAbbreviate(String name) {
		name = name.toLowerCase();
		for (Pair<String, String> pair : termToAbbr)
			name = name.replaceAll(pair.getItem2(), pair.getItem1());
		return name;
	}
}
