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
import java.util.Map;

/**
 * Interface that needs to be implemented by any class that needs to be treated as a file descriptor by 
 * the spade.reporter.audit.DescriptorManager class
 */
public interface ArtifactIdentity extends Serializable{
	
	/**
	 * Allowed descriptor subtypes for classes implementing this interface
	 */
	public static final String SUBTYPE_FILE = "file",
								SUBTYPE_SOCKET = "network", //for unix and network sockets at the moment
								SUBTYPE_MEMORY = "memory",
								SUBTYPE_PIPE = "pipe", //for named and unnamed pipes at the moment
								SUBTYPE_UNKNOWN = "unknown";
	
	/**
	 * Should returns the OPM annotations map for the artifact 
	 * 
	 * @return the map of annotations
	 */
	public abstract Map<String, String> getAnnotationsMap();
	
	/**
	 * Should return one of the subtype as defined in this class depending on the subclass
	 * 
	 * @return the subtype
	 */
	public abstract String getSubtype();
		
}
