/***********************************************************************
 * BathroomBedroom.java
 *
 * Copyright 2018, Kronos Incorporated. All rights reserved.
 * This software is the confidential and proprietary information of
 * Kronos, Inc. ("Confidential Information").  You shall not
 * disclose such Confidential Information and shall use it only in
 * accordance with the terms of the license agreement you entered into
 * with Kronos.
 *
 * KRONOS MAKES NO REPRESENTATIONS OR WARRANTIES ABOUT THE SUITABILITY OF THE
 * SOFTWARE, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR
 * PURPOSE, OR NON-INFRINGEMENT. KRONOS SHALL NOT BE LIABLE FOR ANY DAMAGES
 * SUFFERED BY LICENSEE AS A RESULT OF USING, MODIFYING OR DISTRIBUTING
 * THIS SOFTWARE OR ITS DERIVATIVES.
 **********************************************************************/
package com.sparkTutorial.practice;

import java.io.Serializable;

/**
 * @author Monika.Arora
 *
 */
public class BathroomBedroom implements Serializable{
	
	private int bathroom;
	private int bedroom;
	/**
	 * @param bathroom
	 * @param bedroom
	 */
	public BathroomBedroom(int bathroom, int bedroom) {
		super();
		this.bathroom = bathroom;
		this.bedroom = bedroom;
	}
	
	public int getBathroom() {
		return bathroom;
	}
	public void setBathroom(int bathroom) {
		this.bathroom = bathroom;
	}
	public int getBedroom() {
		return bedroom;
	}
	public void setBedroom(int bedroom) {
		this.bedroom = bedroom;
	}

}
