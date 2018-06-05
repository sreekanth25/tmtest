package com.tm.test;

import java.io.File;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

public class TeamSkillDataLoader implements Runnable {
	Connection con = null;
	String TEAM_SKILL_FILE_PATH = null;
	TeamSkillDataLoader(Connection conn, String filepath){
		con = conn;
		TEAM_SKILL_FILE_PATH = filepath;
	}
	@Override
	public void run() {
		// TODO Auto-generated method stub
		PreparedStatement pstm = null;
		try (
				Reader reader = Files.newBufferedReader(Paths.get(TEAM_SKILL_FILE_PATH));
				CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT);
				) {			
			for (CSVRecord csvRecord : csvParser) {
				if(!csvRecord.get(0).equals("TASK_ID")){
					// Accessing Values by Column Index
					String teamid = csvRecord.get(0);
					String skill = csvRecord.get(1);
					System.out.println("Record No - " + csvRecord.getRecordNumber());
					System.out.println("---------------");
					System.out.println("teamid : " + teamid);
					System.out.println("skill : " + skill);
					System.out.println("---------------\n\n");
					con.setAutoCommit(false);
					System.out.println("add statement");
					String sql = "INSERT INTO team_skill (TEAM_ID, SKILL) VALUES('" + teamid + "','" + skill + "')";
					pstm = (PreparedStatement) con.prepareStatement(sql);
					pstm.execute();
				}
			}
			con.commit();
			File taskFile = new File(TEAM_SKILL_FILE_PATH);
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
