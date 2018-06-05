package com.tm.test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;

public class CSVDataLoader {

	private static Connection con = null;
	private static String CSV_FILE_PATH = "E:\\TM_test\\csv_files\\";
	private static String TASK_FILE_PATH = null;
	private static String TEAM_FILE_PATH = null;
	private static String TEAM_SKILL_FILE_PATH = null;

	public static void main(String[] args) throws IOException {
		 Path dir = Paths.get(CSV_FILE_PATH);
		new FolderMonitorService(dir).processEvents();
		processFiles();
	}
	
	public static void processFiles(){
		listFilesForFolder(new File(CSV_FILE_PATH));
		dbConnect();
		for(String filename:filenames){
			if(filename.equals("task.csv"))
				TASK_FILE_PATH = CSV_FILE_PATH+filename;
			else if(filename.equals("team.csv"))
				TEAM_FILE_PATH = CSV_FILE_PATH+filename;
			else
				TEAM_SKILL_FILE_PATH = CSV_FILE_PATH+filename;
		}
		TaskDataLoader taskdl = new TaskDataLoader(con, TASK_FILE_PATH);
		taskdl.run();
		TeamDataLoader teaml = new TeamDataLoader(con, TEAM_FILE_PATH);
		teaml.run();
		TeamSkillDataLoader teamSkilldl = new TeamSkillDataLoader(con, TEAM_SKILL_FILE_PATH);
		teamSkilldl.run();
	}

	public static void dbConnect(){
		try {
			con = DriverManager.getConnection("jdbc:mysql://localhost:3306/tm_test?useSSL=false&serverTimezone=UTC", "root", "root");
			System.out.println("connection successful");
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	static List<String> filenames = new LinkedList<String>();
	public static void listFilesForFolder(final File folder) {
		for (final File fileEntry : folder.listFiles()) {
			if (fileEntry.isDirectory()) {
				listFilesForFolder(fileEntry);
			} else {
				if(fileEntry.getName().contains(".csv"))
					filenames.add(fileEntry.getName());
			}
		}
	}
}


