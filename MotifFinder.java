package motiffinder;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MotifFinder {

	public static void main(String[] args) throws SQLException,
			NumberFormatException, IOException {
		// TODO Auto-generated method stub

		final String[] chrString = { "", "CHR1", "CHR2", "CHR3", "CHR4",
				"CHR5", "CHR6", "CHR7", "CHR8", "CHR9", "CHR10", "CHR11",
				"CHR12", "CHR13", "CHR14", "CHR15", "CHR16", "CHR17", "CHR18",
				"CHR19", "CHR20", "CHR21", "CHR22", "CHRX", "CHRY" };
		final String[] stateString = { "", "1_Active_Promoter",
				"2_Weak_Promoter", "3_Poised_Promoter", "4_Strong_Enhancer",
				"5_Strong_Enhancer", "6_Weak_Enhancer", "7_Weak_Enhancer",
				"8_Insulator", "9_Txn_Transition", "10_Txn_Elongation",
				"11_Weak_Txn", "12_Repressed", "13_Heterochrom/lo",
				"14_Repetitive/CNV", "15_Repetitive/CNV" };

		Connection conn = null;
		final String url = "jdbc:oracle:thin:@127.0.0.1:1521:orcl";
		final String userID = "SYSTEM";
		final String pass = "4Kyungeun";
		final String[] celllineString = { "", "K562", "GM12878", "H1HESC",
				"HEPG2", "HMEC", "HSMM", "HUVEC", "NHEK", "NHLF" };// 9∞≥ ºø∂Û¿Œ.
		final String[] celllineString2 = { "", "GM12878", "HEPG2", "HMEC",
				"HSMM", "NHLF", "HUVEC", "H1HESC", "NHEK" };
		// ///////////////////////////////////////
		int ngram = 6;
		String cellLine = "K562";
		// ///////////////////////////////////////
		StringBuffer stateSequence = new StringBuffer();
		PreparedStatement psmt = null;
		Clob myClob = null;
		int hmm_index = 0;
		Statement stmt = null;
		Statement stmt1 = null;
		ResultSet rs1 = null;
		Statement stmt2 = null;
		ResultSet rs2 = null;
		String query = "";
		for (int i = 1; i <= 9; i++) {
			query += "\"" + celllineString[i] + "\" VARCHAR2(20), ";
		}

		String tata = "ta[ta][ta][tag][ta]";
		String ini = "[ctg][ctg]a[atgc][at][ct][ct]";
		String bre = "[gc][gc][ga]cgcc";
		String dpe = "[ag]g[at][ct][cag]";

		try {
			conn = DriverManager.getConnection(url, userID, pass);
			stmt = conn.createStatement();
			stmt1 = conn.createStatement();
			stmt2 = conn.createStatement();
			for (int chrNum = 1; chrNum <= 22; chrNum++) {

				int cnt = 0;
				BufferedReader br1 = new BufferedReader(new FileReader(
						"E://data_kyungeun//fasta//chr" + chrNum + ".fa"));

				StringBuffer sb = new StringBuffer();
				String line1 = "";
				while ((line1 = br1.readLine()) != null) {
					if (line1.charAt(0) != '>')
						sb.append(line1);
				}
				System.out
						.println("CREATE TABLE \""
								+ cellLine
								+ "_MOTIF_"
								+ chrString[chrNum]
								+ "\" (\"INDEX_MOTIF\" NUMBER NOT NULL, "
								+ "\"START\" NUMBER, \"END\" NUMBER, \"TATA\" NUMBER, \"INI\" NUMBER, \"BRE\" NUMBER, \"DPE\" NUMBER,"
								+ query + " PRIMARY KEY (\"INDEX_MOTIF\"))");

				stmt.execute("CREATE TABLE \""
						+ cellLine
						+ "_MOTIF_"
						+ chrString[chrNum]
						+ "\" (\"INDEX_MOTIF\" NUMBER NOT NULL, "
						+ "\"START\" NUMBER, \"END\" NUMBER, \"TATA\" NUMBER, \"INI\" NUMBER, \"BRE\" NUMBER, \"DPE\" NUMBER,"
						+ query + " PRIMARY KEY (\"INDEX_MOTIF\"))");
				for (int stateNum = 1; stateNum <= 3; stateNum++) {
					rs1 = stmt1.executeQuery("select * from NINE_CELLLINE_"
							+ chrString[chrNum] + " where \"K562\"='"
							+ stateString[stateNum] + "'");

					while (rs1.next()) {
						int start = rs1.getInt(3);
						int end = rs1.getInt(4);
						String seq = sb.toString().substring(start, end);

						Pattern pattern_tata = Pattern.compile(tata);
						Matcher matcher_tata = pattern_tata.matcher(seq
								.toLowerCase());
						int cnt_tata = 0;
						while (matcher_tata.find()) {
							++cnt_tata;
						}

						Pattern pattern_ini = Pattern.compile(ini);
						Matcher matcher_ini = pattern_ini.matcher(seq
								.toLowerCase());
						int cnt_ini = 0;
						while (matcher_ini.find()) {
							++cnt_ini;
						}

						Pattern pattern_bre = Pattern.compile(bre);
						Matcher matcher_bre = pattern_bre.matcher(seq
								.toLowerCase());
						int cnt_bre = 0;
						while (matcher_bre.find()) {
							++cnt_bre;
						}

						Pattern pattern_dpe = Pattern.compile(dpe);
						Matcher matcher_dpe = pattern_dpe.matcher(seq
								.toLowerCase());
						int cnt_dpe = 0;
						while (matcher_dpe.find()) {
							++cnt_dpe;
						}
						String query1 = "";
						for (int i = 5; i <= 12; i++)
							query1 += "'" + rs1.getString(i) + "',";
						query1 += "'" + rs1.getString(13) + "'";

						stmt2.execute("insert into " + cellLine + "_MOTIF_"
								+ chrString[chrNum] + " values('" + ++cnt
								+ "','" + start + "','" + end + "','"
								+ cnt_tata + "','" + cnt_ini + "','" + cnt_bre
								+ "','" + cnt_dpe + "'," + query1 + ")");

					}
					br1.close();
				}
			}

		} catch (SQLException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			stmt.close();
			rs1.close();
			stmt1.close();
			stmt2.close();
			conn.close();

		}

	}
}
