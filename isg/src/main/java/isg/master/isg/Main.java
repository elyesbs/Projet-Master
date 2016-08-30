package isg.master.isg;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Scanner;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

public class Main {
	public static void main(String[] args) {

		String logfile = "C:\\Users\\Ben-Sliman®\\Desktop\\ProjetMaster\\DataFile.txt";

		String logfile2 = "C:\\Users\\Ben-Sliman®\\Desktop\\ProjetMaster\\InputFile.txt";
		String logfile3 = "C:\\Users\\Ben-Sliman®\\Desktop\\ProjetMaster\\ResultFile.txt";
		String x="";
		try {
			BufferedReader   reader  = new BufferedReader(
					new FileReader(new File(logfile2)));
			// normalement si le fichier n'existe pas, il est crée à la racine
			// du projet
			 x = reader.readLine();

			reader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

			
		
		final String  x1=x;
// SPARK MAP REDUCE 
		SparkConf conf = new SparkConf().setMaster("local[4]").setAppName("My Main");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> logData = sc.textFile(logfile).cache();
		long numAs = logData.filter(new Function<String, Boolean>() {
			public Boolean call(String s) {
				return s.contains(x1);
			}
		}).count();

		
		System.out.println("Number of line containing "+x+": " + numAs);
		try {
			BufferedWriter writer = new BufferedWriter(
					new FileWriter(new File(logfile3)));
			// normalement si le fichier n'existe pas, il est crée à la racine
			// du projet
			writer.write(numAs + "");
			

			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

		  System.out.println("Press enter to exit...");
	        Scanner scan=new Scanner(System.in);
	        scan.nextLine();
	}
}
