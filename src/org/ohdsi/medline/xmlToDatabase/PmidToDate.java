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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.ohdsi.databases.DbType;
import org.ohdsi.utilities.StringUtilities;
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

	private List<String>	months		= getMonths();
	private Pattern			yearPattern	= Pattern.compile("(19|20)[0-9][0-9]");
	public static int		BATCH_SIZE	= 1000;
	private Connection		connection;
	private DbType			dataSourceType;

	public PmidToDate(Connection connection, String dateSourceType) {
		this.connection = connection;
		this.dataSourceType = new DbType(dateSourceType);
	}

	public static void createTable(Connection connection, DbType dbType) {
		String sql = "";
		try {
			Statement statement = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
			sql = "CREATE TABLE pmid_to_date (pmid int,pmid_version int,date date NOT NULL, CONSTRAINT pmid_pmid_version PRIMARY KEY(pmid,pmid_version));";
			statement.execute(sql);
			sql = "CREATE INDEX pmidDate_idx ON pmid_to_date (date);";
			statement.execute(sql);
			statement.close();
		} catch (SQLException e) {
			System.err.println(sql);
			e.printStackTrace();
		}
	}

	/**
	 * This method is used only to repopulate the entire pmidToDate table in one go, using the existing xml to database dump.
	 */
	public void bulkPopulateTable() {
		StringBuilder sql = new StringBuilder();
		try {
			Statement retrieveStatement = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			sql.append("SELECT pmid,pmid_version,art_journal_journalissue_pubdate_year,art_journal_journalissue_pubdate_month,art_journal_journalissue_pubdate_day,art_journal_journalissue_pubdate_medlinedate FROM medcit");
			ResultSet resultSet = retrieveStatement.executeQuery(sql.toString());
			Statement insertStatement = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
			sql = new StringBuilder();
			sql.append("SET DateFormat MDY;");
			insertStatement.execute(sql.toString());
			sql.append("INSERT INTO pmid_to_date (pmid,pmid_version,date) VALUES ");
			int count = 0;

			while (resultSet.next()) {
				String yearString = resultSet.getString("art_journal_journalissue_pubdate_year");
				String monthString = resultSet.getString("art_journal_journalissue_pubdate_month");
				String dayString = resultSet.getString("art_journal_journalissue_pubdate_day");
				String medlineString = resultSet.getString("art_journal_journalissue_pubdate_medlinedate");
				String date = parseDate(yearString, monthString, dayString, medlineString);
				if (date == null) {
					System.err.println("No valid date found for PMID " + resultSet.getString("pmid"));
				} else {
					String pmid = resultSet.getString("pmid");
					String pmid_version = resultSet.getString("pmid_version");
					if (count != 0)
						sql.append(",");
					if (dataSourceType.equals(DbType.MSSQL))
						sql.append("(" + pmid + "," + pmid_version + ",'" + date + "')");
					else if (dataSourceType.equals(DbType.MYSQL))
						sql.append("(" + pmid + "," + pmid_version + ",\"" + date + "\")");
					count++;
					if (count == BATCH_SIZE) {
						insertStatement.execute(sql.toString());
						sql = new StringBuilder();
						sql.append("INSERT INTO pmid_to_date (pmid,pmid_version,date) VALUES ");
						count = 0;
						System.out.print("*");
					}
				}
			}
			insertStatement.execute(sql.toString());
			insertStatement.close();
			retrieveStatement.close();
		} catch (SQLException e) {
			System.err.println(sql);
			e.printStackTrace();
		}
	}

	public void insertDates(Document document) {
		setDateFormat();
		List<String> sqls = new ArrayList<String>();
		StringBuilder sql = new StringBuilder();
		NodeList citationNodes = document.getElementsByTagName("MedlineCitation");
		sql.append("INSERT INTO pmid_to_date (pmid,pmid_version,date) VALUES ");
		int count = 0;
		for (int i = 0; i < citationNodes.getLength(); i++) {
			Node citation = citationNodes.item(i);
			Node pmidNode = XmlTools.getChildByName(citation, "PMID");
			String pmid = XmlTools.getValue(pmidNode);
			String pmid_version = XmlTools.getAttributeValue(pmidNode, "Version");

			// Could be an update, so delete old record just to be sure:
			sqls.add("DELETE FROM pmid_to_date WHERE pmid = " + pmid + " AND pmid_version = " + pmid_version + ";");

			Node pubDateNode = XmlTools.getChildByName(
					XmlTools.getChildByName(XmlTools.getChildByName(XmlTools.getChildByName(citation, "Article"), "Journal"), "JournalIssue"), "PubDate");
			String yearString = XmlTools.getChildByNameValue(pubDateNode, "Year");
			String monthString = XmlTools.getChildByNameValue(pubDateNode, "Month");
			String dayString = XmlTools.getChildByNameValue(pubDateNode, "Day");
			String medlineString = XmlTools.getChildByNameValue(pubDateNode, "MedlineDate");
			String date = parseDate(yearString, monthString, dayString, medlineString);

			if (date == null) {
				System.err.println("No valid date found for PMID " + pmid);
			} else {
				if (count != 0)
					sql.append(",");
				if (dataSourceType.equals(DbType.MSSQL) || dataSourceType.equals(DbType.POSTGRESQL))
					sql.append("(" + pmid + "," + pmid_version + ",'" + date + "')");
				else if (dataSourceType.equals(DbType.MYSQL))
					sql.append("(" + pmid + "," + pmid_version + ",\"" + date + "\")");
				count++;
				if (count == BATCH_SIZE) {
					sqls.add(sql.toString());
					sql = new StringBuilder();
					sql.append("INSERT INTO pmid_to_date (pmid,pmid_version,date) VALUES ");
					count = 0;
				}
			}
		}
		if (count != 0)
			sqls.add(sql.toString());
		execute(sqls);
	}

	private void setDateFormat() {
		try {
			Statement statement = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
			if (dataSourceType.equals(DbType.POSTGRESQL))
				statement.execute("SET datestyle = \"ISO, DMY\"");
			else
				statement.execute("SET DateFormat MDY;");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	private void execute(List<String> sqls) {
		try {
			connection.setAutoCommit(false);
			Statement statement = connection.createStatement();
			for (String sql : sqls)
				statement.addBatch(sql);
			statement.executeBatch();
			statement.close();
			connection.setAutoCommit(true);
		} catch (SQLException e) {
			System.err.println(StringUtilities.join(sqls, "\n"));
			e.printStackTrace();
		}
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

	private String parseDate(String yearString, String monthString, String dayString, String medlineString) {
		String year = null;
		if (yearString == null) {
			if (medlineString == null)
				return null;

			Matcher matcher = yearPattern.matcher(medlineString);
			if (matcher.find())
				year = matcher.group();
		} else {
			year = yearString;
		}
		String month = null;
		if (monthString == null) {
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
			month = Integer.toString(months.indexOf(monthString) + 1).toString();
		}
		String day = dayString == null ? "1" : dayString;
		return year + "-" + month + "-" + day;
	}
}
