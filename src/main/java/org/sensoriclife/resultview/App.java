package org.sensoriclife.resultview;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Random;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.sensoriclife.Config;
import org.sensoriclife.Logger;
import org.sensoriclife.db.Accumulo;
import org.sensoriclife.util.Helpers;

/**
 *
 * @author jnphilipp
 * @version 0.0.1
 */
public class App {
	public static void main(String[] args) throws AccumuloException, AccumuloSecurityException, MutationsRejectedException, TableNotFoundException, TableExistsException {
		Logger.getInstance();
		Config.getInstance();

		Random r = new Random(System.currentTimeMillis());

		Accumulo.getInstance().connect();
		Accumulo.getInstance().createTable("test", false);
		Accumulo.getInstance().addMutation("test", Math.abs(r.nextLong()) + "_el", "device", "amount", "5".getBytes());
		Accumulo.getInstance().addMutation("test", Math.abs(r.nextLong()) + "_el", "residential", "id", "6".getBytes());
		Accumulo.getInstance().addMutation("test", Math.abs(r.nextLong()) + "_el", "user", "id", "6".getBytes());
		Accumulo.getInstance().addMutation("test", Math.abs(r.nextLong()) + "_el", "user", "residential", "6".getBytes());
		Accumulo.getInstance().addMutation("test", Math.abs(r.nextLong()) + "_el", "moehre", "blub", "6".getBytes());
		Accumulo.getInstance().addMutation("test", Math.abs(r.nextLong()) + "_el", "moehre", "blub", "6".getBytes());
		Accumulo.getInstance().flushBashWriter("test");

		boolean quit = false;
		do {
			System.out.print("table: ");
			String table = System.console().readLine().trim();
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

			if ( rowIds.length != 0 && !rowIds[0].isEmpty() ) {
				if ( rowIds.length == 1 )
					iterator = Accumulo.getInstance().scanByKey(table, new Range(rowIds[0].trim(), rowIds[0].trim()));
				else
					iterator = Accumulo.getInstance().scanByKey(table, new Range(rowIds[0].trim(), rowIds[1].trim()));
			}

			if ( !family.isEmpty() && qualifier.isEmpty() )
				iterator = Accumulo.getInstance().scanByFamily(table, family);
			else if ( !family.isEmpty() && !qualifier.isEmpty() )
				iterator = Accumulo.getInstance().scanColumns(table, family, qualifier);

			if ( (rowIds.length == 0 || rowIds[0].isEmpty()) && family.isEmpty() )
				iterator = Accumulo.getInstance().scanAll(table);

			printIterator(iterator);

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

	private static void printIterator(Iterator<Entry<Key, Value>> iterator) {
		if ( iterator == null )
			System.out.println("Nothing found.");
		else {
			System.out.println("            row id            |        family        |       qualifier       |    timestamp    |                           value");
			System.out.println("------------------------------+----------------------+-----------------------+-----------------+------------------------------------------------------------");
			while ( iterator.hasNext() ) {
				Entry<Key, Value> entry = iterator.next();

				int spaces = (30 - entry.getKey().getRow().toString().length()) / 2;
				System.out.print(Helpers.repeat(" ", spaces, "") + "" + entry.getKey().getRow() + Helpers.repeat(" ", ((30 - entry.getKey().getRow().toString().length()) % 2 == 0 ? spaces : spaces + 1), ""));

				spaces = (22 - entry.getKey().getColumnFamily().toString().length()) / 2;
				System.out.print("|" + Helpers.repeat(" ", spaces, "") + "" + entry.getKey().getColumnFamily() + Helpers.repeat(" ", ((30 - entry.getKey().getColumnFamily().toString().length()) % 2 == 0 ? spaces : spaces + 1), ""));

				spaces = (23 - entry.getKey().getColumnQualifier().toString().length()) / 2;
				System.out.print("|" + Helpers.repeat(" ", spaces, "") + "" + entry.getKey().getColumnQualifier() + Helpers.repeat(" ", ((23 - entry.getKey().getColumnQualifier().toString().length()) % 2 == 0 ? spaces : spaces + 1), ""));

				spaces = (17 - Long.toString(entry.getKey().getTimestamp()).length()) / 2;
				System.out.print("|" + Helpers.repeat(" ", spaces, "") + "" + entry.getKey().getTimestamp() + Helpers.repeat(" ", ((17 - Long.toString(entry.getKey().getTimestamp()).length()) % 2 == 0 ? spaces : spaces + 1), ""));

				spaces = (60 - entry.getValue().toString().length()) / 2;
				System.out.println("|" + Helpers.repeat(" ", spaces, "") + "" + entry.getValue() + Helpers.repeat(" ", ((60 - entry.getValue().toString().length()) % 2 == 0 ? spaces : spaces + 1), ""));
			}
		}
	}
}