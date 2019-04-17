/***********************************************************************
 * AverageCount.java
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
package com.sparkTutorial.pairRdd.aggregation.reducebykey.housePrice;

import java.io.Serializable;

/**
 * @author Monika.Arora
 *
 */
public class AverageCount implements Serializable{
	
	/**
	 * @param count
	 * @param price
	 */
	public AverageCount(int count, double price) {
		super();
		this.count = count;
		this.price = price;
	}
	private int count;
	private double price;
	public int getCount() {
		return count;
	}
	public void setCount(int count) {
		this.count = count;
	}
	public double getPrice() {
		return price;
	}
	public void setPrice(double price) {
		this.price = price;
	}

}
