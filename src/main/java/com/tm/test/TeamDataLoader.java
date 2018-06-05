package com.tm.test;

import java.io.File;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

public class TeamDataLoader implements Runnable {
	Connection con = null;
	String TEAM_FILE_PATH = null;
	TeamDataLoader(Connection conn, String filepath){
		con = conn;
		TEAM_FILE_PATH = filepath;
	}

	@Override
	public void run() {
		PreparedStatement pstm = null;
		try (
				Reader reader = Files.newBufferedReader(Paths.get(TEAM_FILE_PATH));
				CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT);
				) {			
			for (CSVRecord csvRecord : csvParser) {
				if(!csvRecord.get(0).equals("TEAM_ID")){
					// Accessing Values by Column Index
					String teamid = csvRecord.get(0);
					System.out.println("Record No - " + csvRecord.getRecordNumber());
					System.out.println("---------------");
					System.out.println("teamid : " + teamid);
					System.out.println("---------------\n\n");
					con.setAutoCommit(false);
					System.out.println("add statement");
					String sql = "INSERT INTO team (TEAM_ID) VALUES('" + teamid + "')";
					pstm = (PreparedStatement) con.prepareStatement(sql);
					pstm.execute();
				}
			}
			con.commit();
			File taskFile = new File(TEAM_FILE_PATH);
			taskFile.delete();
		}catch (Exception e) {
			e.printStackTrace();
			if (con != null) {
				try {
					System.out.println("Transaction is being rolled back.");
					con.rollback();
				} catch (Exception ex) {
					ex.printStackTrace();
				}
			}
		}

	}

}
