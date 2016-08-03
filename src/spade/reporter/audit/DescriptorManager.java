/*
 --------------------------------------------------------------------------------
 SPADE - Support for Provenance Auditing in Distributed Environments.
 Copyright (C) 2015 SRI International

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
package spade.reporter.audit;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * A class for managing file descriptors for a process
 * 
 */
public class DescriptorManager implements Serializable{

	private static final long serialVersionUID = 5724247327201528391L;

	/**
	 * Internal map to keep track of file descriptors. Map<pid, Map<fd, classes implementing ArtifactIdentity>>.
	 */
	private Map<String, Map<String, ArtifactIdentity>> descriptors = new HashMap<String, Map<String, ArtifactIdentity>>();
	
	/**
	 * Adds a file descriptor for the process
	 * 
	 * @param pid process id
	 * @param fd file descriptor number
	 * @param artifactIdentity artifact identity object like FileIdentity and etc.
	 */
	public void addDescriptor(String pid, String fd, ArtifactIdentity artifactIdentity){
		if(descriptors.get(pid) == null){
			descriptors.put(pid, new HashMap<String, ArtifactIdentity>());
		}
		descriptors.get(pid).put(fd, artifactIdentity);
	}
	
	/**
	 * Copies the given map of descriptors in the existing map
	 * 
	 * @param pid process id
	 * @param newDescriptors map of descriptors to add
	 */
	public void addDescriptors(String pid, Map<String, ArtifactIdentity> newDescriptors){
		if(descriptors.get(pid) == null){
			descriptors.put(pid, new HashMap<String, ArtifactIdentity>());
		}
		descriptors.get(pid).putAll(newDescriptors);
	}
	
	/**
	 * Removes and returns the descriptor if it exists
	 * 
	 * @param pid process id
	 * @param fd file descriptor
	 * @return ArtifactIdentity subclass or null if didn't exist
	 */
	public ArtifactIdentity removeDescriptor(String pid, String fd){
		if(descriptors.get(pid) == null){
			return null;
		}
		return descriptors.get(pid).remove(fd);
	}
	
	/**
	 * Returns the descriptor if it exists
	 * 
	 * @param pid process id
	 * @param fd file descriptor
	 * @return ArtifactIdentity subclass or null if didn't exist
	 */
	public ArtifactIdentity getDescriptor(String pid, String fd){
		if(descriptors.get(pid) == null){
			descriptors.put(pid, new HashMap<String, ArtifactIdentity>());
		}
		//cannot be sure if the file descriptors 0,1,2 are stdout, stderr, stdin, respectively. so just use unknowns
//		if(descriptors.get(pid).get(fd) == null){
//			String path = null;
//			if("0".equals(fd)){
//	    		path = "stdin";
//	    	}else if("1".equals(fd)){
//	    		path = "stdout";
//	    	}else if("2".equals(fd)){
//	    		path = "stderr";
//	    	}
//			if(path != null){
//				descriptors.get(pid).put(fd, new FileIdentity(path));
//			}
//		}
		return descriptors.get(pid).get(fd);
	}
	
	/**
	 * Copies file descriptors from one process to another process
	 * 	
	 * @param fromPid process id to copy descriptors of
	 * @param toPid process id to copy descriptors to
	 */
	public void copyDescriptors(String fromPid, String toPid){
		if(descriptors.get(fromPid) == null){
			return;
		}
		if(descriptors.get(toPid) == null){
			descriptors.put(toPid, new HashMap<String, ArtifactIdentity>());
		}
		descriptors.get(toPid).putAll(descriptors.get(fromPid));
	}
	
	/**
	 * Copies the reference of the file descriptors map from one process to another process
	 * 
	 * @param fromPid process id to get the file descriptors map's reference from
	 * @param toPid process id to assign the file descriptors map's reference to
	 */
	public void linkDescriptors(String fromPid, String toPid){
		if(descriptors.get(fromPid) == null){
			descriptors.put(fromPid, new HashMap<String, ArtifactIdentity>());
		}
		descriptors.put(toPid, descriptors.get(fromPid));
	}
	
	/**
	 * Removes the referenced map of file descriptors and creates a copy of the removed map of file descriptors for the process
	 * @param pid process id
	 */
	public void unlinkDescriptors(String pid){
		if(descriptors.get(pid) == null){
			return;
		}
		descriptors.put(pid, new HashMap<String, ArtifactIdentity>(descriptors.get(pid)));
	}
	
	/**
	 * Add an unknown descriptor object
	 * 
	 * @param pid process id
	 * @param fd file descriptor
	 */
	public void addUnknownDescriptor(String pid, String fd){
		addDescriptor(pid, fd, new UnknownIdentity(pid, fd));
	}
}
