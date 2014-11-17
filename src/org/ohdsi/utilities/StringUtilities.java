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
package org.ohdsi.utilities;

import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

public class StringUtilities {

	public static String join(Collection<?> s, String delimiter) {
		StringBuffer buffer = new StringBuffer();
		Iterator<?> iter = s.iterator();
		if (iter.hasNext()) {
			buffer.append(iter.next().toString());
		}
		while (iter.hasNext()) {
			buffer.append(delimiter);
			buffer.append(iter.next().toString());
		}
		return buffer.toString();
	}

	public static String join(Object[] objects, String delimiter) {
		StringBuffer buffer = new StringBuffer();
		if (objects.length != 0)
			buffer.append(objects[0].toString());
		for (int i = 1; i < objects.length; i++) {
			buffer.append(delimiter);
			buffer.append(objects[i].toString());
		}
		return buffer.toString();
	}

	public static String now() {
		Date d = new Date();
		DateFormat df = DateFormat.getDateTimeInstance(DateFormat.MEDIUM, DateFormat.MEDIUM);
		return df.format(d);
	}

	public static void outputWithTime(String message) {
		System.out.println(now() + "\t" + message);
	}

	public static String findBetween(String source, String pre, String post) {
		int start = source.indexOf(pre);
		if (start == -1)
			return "";
		int end = source.indexOf(post, start + pre.length());
		if (end == -1)
			return "";
		return source.substring(start + pre.length(), end);
	}

	public static List<String> multiFindBetween(String source, String pre, String post) {
		List<String> result = new ArrayList<String>();
		int start = 0;
		int end = 0;
		while (start != -1 && end != -1) {
			start = source.indexOf(pre, end);
			if (start != -1) {
				end = source.indexOf(post, start + pre.length());
				if (end != -1)
					result.add(source.substring(start + pre.length(), end));
			}
		}
		return result;
	}

	public static boolean isInteger(String string) {
		try {
			Integer.parseInt(string);
		} catch (NumberFormatException e) {
			return false;
		}
		return true;
	}

	public static List<String> safeSplit(String string, char delimiter) {
		List<String> result = new ArrayList<String>();
		if (string.length() == 0) {
			result.add("");
			return result;
		}
		boolean literal = false;
		boolean escape = false;
		int startpos = 0;
		int i = 0;
		char currentchar;
		while (i < string.length()) {
			currentchar = string.charAt(i);
			if (currentchar == '"' && !escape) {
				literal = !literal;
			}
			if (!literal && (currentchar == delimiter && !escape)) {
				result.add(string.substring(startpos, i));
				startpos = i + 1;
			}
			if (currentchar == '\\') {
				escape = !escape;
			} else {
				escape = false;
			}
			i++;
		}
		result.add(string.substring(startpos, i));
		return result;
	}

}
