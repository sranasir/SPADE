/*
 --------------------------------------------------------------------------------
 SPADE - Support for Provenance Auditing in Distributed Environments.
 Copyright (C) 2012 SRI International

 This program is free software: you can redistribute it and/or
 modify it under the terms of the GNU General Public License as
 published by the Free Software Foundation, either version 3 of the
 License, or (at your option) any later version.

 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 General Public License for more details.

 You should have received a copy of the GNU General Public License
 along with this program. If not, see <http://www.gnu.org/licenses/>.
 --------------------------------------------------------------------------------

 */
package spade.utility;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;

/**
 * This class implements the ExternalStore interface and is used in ExternalMemoryMap class
 *
 * @param <V> Serializable object to save in BerkeleyDB
 */

public class BerkeleyDB<V extends Serializable> implements ExternalStore<V> {
	
	private Environment environment;
	private Database database;
	
	/**
	 * Opens an existing database at the given path or creates a new database at the given path
	 * 
	 * @param dirPath path of the database
	 * @param databaseName name of the database
	 * @param create true if new database. false if opening an existing database
	 */
	public BerkeleyDB(String dirPath, String databaseName, boolean create) {
		EnvironmentConfig envConfig = new EnvironmentConfig();
		DatabaseConfig dbConfig = new DatabaseConfig();
		
		envConfig.setAllowCreate(create);
		dbConfig.setAllowCreate(create);
		
		environment = new Environment(new File(dirPath), envConfig);
		database = environment.openDatabase(null, databaseName, dbConfig);
	}

	@Override
	public boolean init() throws Exception {
		return true;
	}

	@Override
	public boolean shutdown() throws Exception {
		database.close();
		environment.close();
		return true;
	}

	@SuppressWarnings("unchecked")
	@Override
	public V get(String key) throws Exception {
		DatabaseEntry keyEntry = new DatabaseEntry(key.getBytes());
		DatabaseEntry valueEntry = new DatabaseEntry();
		
	    if(database.get(null, keyEntry, valueEntry, LockMode.DEFAULT) == OperationStatus.SUCCESS){
	        byte[] valueBytes = valueEntry.getData();
	        ByteArrayInputStream byteInputStream = new ByteArrayInputStream(valueBytes);
			ObjectInputStream objectInputStream = new ObjectInputStream(byteInputStream);
			return (V)objectInputStream.readObject();
	    }else{
	        return null;
	    }
	}

	@Override
	public void put(String key, V value) throws Exception {
		
		ByteArrayOutputStream byteOutputStream = new ByteArrayOutputStream();
		ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteOutputStream);
		objectOutputStream.writeObject(value);
		byte[] valueBytes = byteOutputStream.toByteArray(); 
		
		DatabaseEntry keyEntry = new DatabaseEntry(key.getBytes());
		DatabaseEntry valueEntry = new DatabaseEntry(valueBytes);
		database.put(null, keyEntry, valueEntry);
	}

	@Override
	public void remove(String key) throws Exception {
	    DatabaseEntry keyEntry = new DatabaseEntry(key.getBytes());
	    database.delete(null, keyEntry);
	}

	/**
	 * NOT IMPLEMENTED YET	
	 */
	@Override
	public void clear() throws Exception {
		//no current implementation
	}

}
