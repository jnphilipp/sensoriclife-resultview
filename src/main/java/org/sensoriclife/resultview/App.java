package org.sensoriclife.resultview;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.sensoriclife.Config;
import org.sensoriclife.Logger;
import org.sensoriclife.db.Accumulo;
import org.sensoriclife.util.Helpers;

/**
 *
 * @author jnphilipp
 * @version 0.2.1
 */
public class App {
	public static void main(String[] args) throws AccumuloException, AccumuloSecurityException, MutationsRejectedException, TableNotFoundException, TableExistsException, IOException {
		Logger.getInstance();

		String confFile = "", table = "";
		if ( args.length != 0 ) {
			List<String> l = Arrays.asList(args);
			Iterator<String> it = l.iterator();

			while ( it.hasNext() ) {
				switch ( it.next() ) {
					case "-conf":
						confFile = it.next();
						break;
					case "-t":
						table = it.next();
						break;
				}
			}
		}

		if ( table.isEmpty() )
			System.out.println("resultView (-conf <configuration file>) -t <table>");

		Config.getInstance();
		if ( confFile.isEmpty() )
			Config.load();
		else
			Config.load(confFile);

		try {
			if ( Config.getProperty("accumulo.name").isEmpty() && Config.getProperty("accumulo.zooServers").isEmpty() && Config.getProperty("accumulo.user").isEmpty() && Config.getProperty("accumulo.password").isEmpty() ){
				Accumulo.getInstance().connect();
			}
			else {
				Accumulo.getInstance().connect(Config.getProperty("accumulo.name"), Config.getProperty("accumulo.zooServers"), Config.getProperty("accumulo.user"), Config.getProperty("accumulo.password"));
			}
		}
		catch ( AccumuloException | AccumuloSecurityException e ) {
			Logger.error("Error while connecting to accumulo.", e.toString());
		}

		boolean quit = false;
		do {
			System.out.print("row ids (range): ");
			String[] rowIds = System.console().readLine().trim().split(" ");
			System.out.print("family: ");
			String family = System.console().readLine().trim();
			String qualifier = "";
			if ( !family.isEmpty() ) {
				System.out.print("qualifier: ");
				qualifier = System.console().readLine().trim();
			}

			Iterator<Entry<Key, Value>> iterator = null;
			Scanner scanner = Accumulo.getInstance().getScanner(table);

			if ( rowIds.length != 0 && !rowIds[0].isEmpty() ) {
				if ( rowIds.length == 1 )
					scanner.setRange(new Range(new Text(Helpers.toByteArray(rowIds[0].trim()))));
				else
					scanner.setRange(new Range(new Text(Helpers.toByteArray(rowIds[0].trim())), new Text(Helpers.toByteArray(rowIds[1].trim()))));
			}

			if ( !family.isEmpty() && qualifier.isEmpty() )
				scanner.fetchColumnFamily(new Text(Helpers.toByteArray(family)));
			else if ( !family.isEmpty() && !qualifier.isEmpty() )
				scanner.fetchColumn(new Text(Helpers.toByteArray(family)), new Text(Helpers.toByteArray(qualifier)));

			iterator = scanner.iterator();
			try {
				printIterator(iterator);
			}
			catch ( ClassNotFoundException e ) {
				Logger.error(App.class, "Error while converting byte arrays to Object.", e.toString());
			}
			finally {
				scanner.close();
			}

			System.out.print("quit? ");
			switch ( System.console().readLine().trim() ) {
				case "q":
				case "y":
				case "quit":
				case "yes":
				case "exit":
					quit = true;
			}
		} while ( !quit );
	}

	private static void printIterator(Iterator<Entry<Key, Value>> iterator) throws ClassNotFoundException, IOException {
		if ( iterator == null )
			System.out.println("Nothing found.");
		else {
			System.out.println("            row id            |        family        |       qualifier       |    timestamp    |                           value");
			System.out.println("------------------------------+----------------------+-----------------------+-----------------+------------------------------------------------------------");
			while ( iterator.hasNext() ) {
				Entry<Key, Value> entry = iterator.next();

				String key = Helpers.toObject(entry.getKey().getRow().getBytes()).toString();
				int spaces = (30 - key.length()) / 2;
				System.out.print(Helpers.repeat(" ", spaces, "") + key + Helpers.repeat(" ", ((30 - key.length()) % 2 == 0 ? spaces : spaces + 1), ""));

				String family = Helpers.toObject(entry.getKey().getColumnFamily().getBytes()).toString();
				spaces = (22 - family.length()) / 2;
				System.out.print("|" + Helpers.repeat(" ", spaces, "") + family + Helpers.repeat(" ", ((30 - family.length()) % 2 == 0 ? spaces : spaces + 1), ""));

				String qualifier = Helpers.toObject(entry.getKey().getColumnQualifier().getBytes()).toString();
				spaces = (23 - qualifier.length()) / 2;
				System.out.print("|" + Helpers.repeat(" ", spaces, "") + "" + qualifier + Helpers.repeat(" ", ((23 - qualifier.length()) % 2 == 0 ? spaces : spaces + 1), ""));

				spaces = (17 - Long.toString(entry.getKey().getTimestamp()).length()) / 2;
				System.out.print("|" + Helpers.repeat(" ", spaces, "") + "" + entry.getKey().getTimestamp() + Helpers.repeat(" ", ((17 - Long.toString(entry.getKey().getTimestamp()).length()) % 2 == 0 ? spaces : spaces + 1), ""));

				String value = "";
				if ( family.equals("user") && qualifier.equals("id") ) {
					byte[] values = entry.getValue().get();
					String id = Helpers.toObject(Arrays.copyOfRange(values, 0, 82)).toString();
					String user = Helpers.toObject(Arrays.copyOfRange(values, 82, values.length)).toString();
					value = id + ";" + user;
				}
				else
					value = Helpers.toObject(entry.getValue().get()).toString();

				spaces = (60 - value.length()) / 2;
				System.out.println("|" + Helpers.repeat(" ", spaces, "") + value + Helpers.repeat(" ", ((60 - value.length()) % 2 == 0 ? spaces : spaces + 1), ""));
			}
		}
	}
}