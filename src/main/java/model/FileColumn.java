package model;

import java.util.Objects;

public class FileColumn {
	private String name;
	private String dataType;
	private int position;

	public FileColumn(String name, String dataType, int position) {
		super();
		this.name = Objects.requireNonNull(name, "name must not be null");
		this.dataType = Objects.requireNonNull(dataType, "dataType must not be null");
		this.position = position;
	}

	public String getName() {
		return this.name;
	}

	public String getDataType() {
		return this.dataType;
	}
	
	public int getPosition() {
		return this.position;
	}

}//end class
