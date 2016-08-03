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

import spade.utility.CommonFunctions;

/**
 * Properties for different kinds of artifacts like version and etc.
 */
public class ArtifactProperties implements Serializable{
	
	private static final long serialVersionUID = -1299250614232336780L;

	/**
	 * User friendly renaming of a value to be used for an uninitialized property
	 */
	public static final long ARTIFACT_PROPERTY_UNINITIALIZED = -1;
	
	/**
	 * Count of the current version
	 */
	private long version = ARTIFACT_PROPERTY_UNINITIALIZED;

	/**
	 * Count of the current epoch
	 */
	private long epoch = ARTIFACT_PROPERTY_UNINITIALIZED;
	
	/**
	 * Flag to indicate if an epoch had happened and hasn't been handled yet
	 */
	private boolean epochPending = true;
	
	/**
	 * Event id when the artifact was created
	 */
	private long creationEventId = ARTIFACT_PROPERTY_UNINITIALIZED;
	
	/**
	 * Function to use set new epoch. 1) Sets creation event id, 2) Sets epoch pending to true, and 3) Sets version to uninitialized
	 * @param creationEventId event id when this happened
	 */
	public void markNewEpoch(long creationEventId){
		this.creationEventId = creationEventId;
		this.epochPending = true;
		this.version = ARTIFACT_PROPERTY_UNINITIALIZED;
	}	
	
	/**
	 * A convenience wrapper for {@link #markNewEpoch(long) markNewEpoch}
	 * @param creationEventIdString event id when this happened
	 */
	public void markNewEpoch(String creationEventIdString){
		long creationEventId = CommonFunctions.parseLong(creationEventIdString, ARTIFACT_PROPERTY_UNINITIALIZED);
		markNewEpoch(creationEventId);
	}
	
	/**
	 * Returns the event id of the artifact when the last epoch was marked
	 * @return the event id of creation
	 */
	public long getCreationEventId(){
		return creationEventId;
	}

	/**
	 * Returns the epoch. If epoch is pending then it is incremented and returned.
	 * If epoch is uninitialized then epoch is incremented and is returned i.e. -1 to 0.
	 * In all cases, is epoch was pending it is set to false before returning.
	 * 
	 * @return current epoch value
	 */
	public long getEpoch(){
		if(epochPending || epoch == ARTIFACT_PROPERTY_UNINITIALIZED){
			epochPending = false;
			epoch++;
		}
		return epoch;
	}
	
	/**
	 * Returns the version. If version is uninitialized or increment argument is true then
	 * version is incremented and returned.
	 * 
	 * @param increment true if needs to be increment, false if not
	 * @return current version value
	 */
	public long getVersion(boolean increment){
		if(increment || version == ARTIFACT_PROPERTY_UNINITIALIZED){
			version++;
		}
		return version;
	}
	
	/**
	 * Returns true if the version is uninitialized else false
	 * 
	 * @return true or false
	 */
	public boolean isVersionUninitialized(){
		return version == ARTIFACT_PROPERTY_UNINITIALIZED;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (creationEventId ^ (creationEventId >>> 32));
		result = prime * result + (int) (epoch ^ (epoch >>> 32));
		result = prime * result + (epochPending ? 1231 : 1237);
		result = prime * result + (int) (version ^ (version >>> 32));
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		ArtifactProperties other = (ArtifactProperties) obj;
		if (creationEventId != other.creationEventId)
			return false;
		if (epoch != other.epoch)
			return false;
		if (epochPending != other.epochPending)
			return false;
		if (version != other.version)
			return false;
		return true;
	}
}