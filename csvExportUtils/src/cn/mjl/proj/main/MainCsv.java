package cn.mjl.proj.main;

import java.io.File;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.csvreader.CsvWriter;

public class MainCsv {
	private static final Logger log = LogManager.getLogger(MainCsv.class);
	static String directorysString = null;

	public static void main(String[] args) throws ParseException {
		try {
			System.out.println("11111");
			initialize();
			System.out.println("12111");

			exportCsv();
			System.out.println("12611");
			logCycle();
		} catch (SQLException e) {
			log.error(e);
		}
	}

	/***
	 * 初始化，新建文件夹
	 ***/
	public static void initialize() {
		File directory = new File("");	
		try {
			directorysString = directory.getAbsolutePath();
			log.info("getAbsolutePath");
		} catch (Exception e) {
			log.error(e);
		}

		String pathFloder = directorysString + "/CSVTest";
		String pathFile = directorysString + "/CSVTest/create.csv";

		File file1 = new File(pathFloder);
		if (!file1.exists()) {
			file1.mkdir();
			log.info("创建文件夹成功");
		}
		File file2 = new File(pathFile);
		if (file2.exists()) {
			file2.delete();
			log.info("旧文件删除");
		}
	}

	/***
	 * csv导出
	 ***/
	public static void exportCsv() throws SQLException {
		String login_id;
		String user_last_name;
		String user_first_name;
		String email;
		Statement stmt = null;
		ResultSet result = null;
		Connection conn = null;
		String sql = getProperty("contact.url.mysql.sql");
System.out.println(sql);
		String outFile = directorysString + "/CSVTest/create.csv";
		CsvWriter writer = new CsvWriter(outFile, ',', Charset.forName("utf-8"));
		try {
			conn = getcon();
			stmt = conn.createStatement();

			result = stmt.executeQuery(sql);
			System.out.println("test");

			log.info("连接数据成功");
			while (result.next()) {
				login_id = result.getString("login_id");
				user_last_name = result.getString("user_last_name");
				user_first_name = result.getString("user_first_name");
				email = result.getString("email");
				String[] contents = { login_id, user_last_name,
						user_first_name, email };
				writer.writeRecord(contents);
			}
		} catch (Exception e) {
			log.error(e);
		} finally {
			try {
				if (conn != null) {
					conn.close();
				}
			} catch (Exception e2) {
				log.error(e2);
			}

			try {
				writer.close();
			} catch (Exception e2) {
				log.error(e2);
			}
		}
	}

	/**
	 * jdbc
	 * 
	 * ***/
	public static Connection getcon() throws SQLException {
		String jdbcName = "com.mysql.jdbc.Driver";
		Connection conn = null;
		String url = getProperty("contact.url.mysql.wifi");
		try {
			Class.forName(jdbcName);
			log.info("成功加载MySQL驱动程序");
			conn = DriverManager.getConnection(url);
		} catch (Exception e) {
			log.error(e);
		}
		return conn;
	}

	/***
	 * 删除超过7天的log
	 * 
	 * ***/
	public static void logCycle() {
		String logPath = directorysString+"/logs";
		Date nowDate = new Date();
		File file = new File(logPath);
		File[] filesList = file.listFiles();
		log.info("length" + "=" + filesList.length);
		try {
			for (File f : filesList) {
				String f1 = String.valueOf(f);
				File tempFile = new File(f1.trim());
				String fileName = tempFile.getName();
				if (fileName.length() > 20) {
					String[] str = fileName.split("-");
					String yearsString = str[1];
					String mounthString = str[2];
					String dayString = str[3];
					StringBuffer a = new StringBuffer();
					a.append(yearsString).append("-").append(mounthString)
							.append("-").append(dayString);
					String strTemp = a.toString();
					SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
					Date date = sdf.parse(strTemp);

					long day = (nowDate.getTime() - date.getTime())
							/ (24 * 60 * 60 * 1000);
					if (day > 6) {
						String bTempString = "csvExportUtils-" + strTemp
								+ "-1.log.gz";
						String bPathString = directorysString + "/logs/" + bTempString;
						f = new File(bPathString);
						if (f.exists()) {
							f.delete();
							log.info("log删除成功。");
						}
					}
				}
			}
		} catch (Exception e) {
			log.error(e);
		}

	}

	/***
	 * properties
	 * 
	 * ***/

	private static Properties properties;

	public static String getProperty(String key) {
		if (properties == null) {
			properties = new Properties();
			try {
				properties.load(MainCsv.class.getClassLoader()
						.getResourceAsStream("system.properties"));
			} catch (Exception e) {
				log.error(e);
				try {
					throw new Exception("system.properties not found");
				} catch (Exception e1) {
					log.error(e);
				}
			}
		}
		return (String) properties.get(key);
	}
}
