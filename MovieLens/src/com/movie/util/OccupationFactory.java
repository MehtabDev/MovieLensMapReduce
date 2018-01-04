package com.movie.util;


/**
 * @author mehtab khan
 * @description OccupationFactory class to handle User's profession data.
 */
public class OccupationFactory {

	public static OccupationFactory occupationFactory = null;

	public static OccupationFactory getInstance() {
		if (occupationFactory == null) {
			occupationFactory = new OccupationFactory();

		}
		return occupationFactory;

	}

	public String getOccupation(int code) {
		String occupation = null;
		switch (code) {
		case 0:
			occupation = "other or not specified";
			break;
		case 1:
			occupation = "academic/educator";
			break;

		case 2:
			occupation = "artist";
			break;
		case 3:
			occupation = "clerical/admin";
			break;

		case 4:
			occupation = "college/grad student";
			break;
		case 5:
			occupation = "customer service";
			break;
		case 6:
			occupation = "doctor/health care";
			break;
		case 7:
			occupation = "executive/managerial";
			break;
		case 8:
			occupation = "farmer";
			break;
		case 9:
			occupation = "homemaker";
			break;
		case 10:
			occupation = "K-12 student";
			break;
		case 11:
			occupation = "lawyer";
			break;
		case 12:
			occupation = "programmer";
			break;
		case 13:
			occupation = "retired";
			break;
		case 14:
			occupation = "sales/marketing";
			break;
		case 15:
			occupation = "scientist";
			break;
		case 16:
			occupation = "self-employed";
			break;
		case 17:
			occupation = "technician/engineer";
			break;
		case 18:
			occupation = "tradesman/craftsman";
			break;
		case 19:
			occupation = "unemployed";
			break;
		case 20:
			occupation = "writer";
			break;

		}

		return occupation;
		
	}

}
