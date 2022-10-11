package model;

public class FileColumn {
	private String name;
	private String dataType;
	private int position;

	public FileColumn(String name, String dataType, int position) {
		super();
		this.name = name;
		this.dataType = dataType;
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
