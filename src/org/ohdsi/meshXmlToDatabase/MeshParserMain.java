package org.ohdsi.meshXmlToDatabase;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.ohdsi.databases.ConnectionWrapper;
import org.ohdsi.databases.DbType;
import org.ohdsi.databases.InsertableDbTable;
import org.ohdsi.utilities.StringUtilities;
import org.ohdsi.utilities.files.IniFile;
import org.ohdsi.utilities.files.Row;

public class MeshParserMain {

	private Map<String, String>	treeNumberToUi;

	public static void main(String[] args) {
		IniFile iniFile = new IniFile(args[0]);
		MeshParserMain meshParser = new MeshParserMain();
		meshParser.parseMesh(iniFile.get("MESH_XML_FOLDER"), iniFile.get("SERVER"), iniFile.get("SCHEMA"), iniFile.get("DOMAIN"), iniFile.get("USER"),
				iniFile.get("PASSWORD"), iniFile.get("DATA_SOURCE_TYPE"));

	}

	private void parseMesh(String folder, String server, String schema, String domain, String user, String password, String dateSourceType) {
		String meshFile = null;
		String meshSupplementFile = null;
		for (File file : new File(folder).listFiles()) {
			String fileName = file.getName();
			if (fileName.startsWith("desc") && fileName.endsWith(".gz")) {
				if (meshFile != null)
					throw new RuntimeException("Error: Multiple main MeSH files found. Please have only 1 in the folder");
				else
					meshFile = file.getAbsolutePath();
			}
			if (fileName.startsWith("supp") && fileName.endsWith(".gz")) {
				if (meshSupplementFile != null)
					throw new RuntimeException("Error: Multiple supplementary MeSH files found. Please have only 1 in the folder");
				else
					meshSupplementFile = file.getAbsolutePath();
			}
		}
		if (meshFile == null)
			throw new RuntimeException("Error: No main MeSH file found.");
		if (meshSupplementFile == null)
			throw new RuntimeException("Error: No supplementary MeSH file found.");
		ConnectionWrapper connectionWrapper = new ConnectionWrapper(server, domain, user, password, new DbType(dateSourceType));
		connectionWrapper.use(schema);
		
		treeNumberToUi = new HashMap<String, String>();
		InsertableDbTable outTerms = new InsertableDbTable(connectionWrapper, "mesh_term");
		InsertableDbTable outRelationship = new InsertableDbTable(connectionWrapper, "mesh_relationship");
		
		MainMeshParser.parse(meshFile, outTerms, outRelationship, treeNumberToUi);
		SupplementaryMeshParser.parse(meshSupplementFile, outTerms, outRelationship);
		outTerms.close();
		outRelationship.close();

		InsertableDbTable outAncestor = new InsertableDbTable(connectionWrapper, "mesh_ancestor");
		generateAncestorTable(outAncestor);
		outAncestor.close();
	}

	private void generateAncestorTable(InsertableDbTable outAncestor) {
		Map<String, PairInfo> pairToInfo = new HashMap<String, PairInfo>();
		for (String treeNumber1 : treeNumberToUi.keySet()) {
			String parts[] = treeNumber1.split("\\.");
			String ui1 = treeNumberToUi.get(treeNumber1);
			for (int i = 0; i < parts.length; i++) {
				int distance = parts.length - i - 1;
				String treeNumber2 = StringUtilities.join(Arrays.copyOfRange(parts, 0, i + 1), ".");
				String ui2 = treeNumberToUi.get(treeNumber2);
				PairInfo pairInfo = pairToInfo.get(ui1 + "_" + ui2);
				if (pairInfo == null) {
					pairInfo = new PairInfo(distance, distance);
					pairToInfo.put(ui1 + "_" + ui2, pairInfo);
				} else {
					if (pairInfo.maxDistance < distance)
						pairInfo.maxDistance = distance;
					if (pairInfo.minDistance > distance)
						pairInfo.minDistance = distance;
				}
			}
		}

		for (String pair : pairToInfo.keySet()) {
			PairInfo pairInfo = pairToInfo.get(pair);
			Row row = new Row();
			row.add("ancestor_ui", pair.split("_")[1]);
			row.add("descendant_ui", pair.split("_")[0]);
			row.add("max_distance", pairInfo.maxDistance);
			row.add("min_distance", pairInfo.minDistance);
			outAncestor.write(row);
		}
	}

	private class PairInfo {
		public int	minDistance;
		public int	maxDistance;

		public PairInfo(int minDistance, int maxDistance) {
			this.minDistance = minDistance;
			this.maxDistance = maxDistance;
		}
	}
}
