package com.movie.util;

/**
 * @author mehtab khan
 * @description AgeFactory class to handle the Age data.
 */
public class AgeFactory {

	public static AgeFactory ageFactory = null;

	public static AgeFactory getInstance() {
		if (ageFactory == null) {
			ageFactory = new AgeFactory();

		}
		return ageFactory;

	}

	public String getAgeString(int age) {
		if (age < 18) {
			return null;
		} else if (age >= 18 && age <= 35) {
			return "18-35";
		} else if (age >= 36 && age <= 50) {
			return "36-50";
		} else if (age > 50) {
			return "50+";
		}
		return null;
	}

}
