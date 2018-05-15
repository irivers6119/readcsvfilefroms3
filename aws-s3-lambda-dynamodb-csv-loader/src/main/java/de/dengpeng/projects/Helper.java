package de.dengpeng.projects;


import java.text.ParseException;

import com.amazonaws.services.dynamodbv2.document.Item;

// TODO: Auto-generated Javadoc
/**
 * The Class Helper.
 * This class helps you parse CSV file.
 * 
 */
public class Helper {
	
	/**
	 * Parses the it.
	 *
	 * @param nextLine the next line
	 * @return the item
	 * @throws ParseException the parse exception
	 */
	
	
	public Item parseIt(String[] nextLine) throws ParseException {
		
				
		Item newItem = new Item();
		
		
	
		
		/*def_spcification table*/
		String  specification_id = nextLine[0];
		String  specification_category;
		String  specification_name;
		String specification_value;
		String is_ancillary;
		
		
		
		
		if (nextLine[1] != null && !nextLine[1].isEmpty()) {
			specification_category = nextLine[1];
		}else {
			specification_category =" ";
		}
		if (nextLine[2] != null && !nextLine[2].isEmpty()) {
			specification_name = nextLine[2];
		}else {
			specification_name =" ";
		}
		if (nextLine[3] != null && !nextLine[3].isEmpty()) {
			specification_value = nextLine[3];
		}else {
			specification_value =" ";
		}
		if (nextLine[4] != null && !nextLine[4].isEmpty()) {
			is_ancillary = nextLine[4];
		}else {
			is_ancillary =" ";
		}
		
		
		newItem.withPrimaryKey("specification_id", specification_id); 
		newItem.withString("specification_category", specification_category);
		newItem.withString("specification_name", specification_name);
		newItem.withString("specification_value", specification_value);
		newItem.withString("is_ancillary", is_ancillary);
		
		
				
		
		
		return newItem;
	}
}
