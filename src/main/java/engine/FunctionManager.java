package engine;

import java.awt.event.ActionListener;

public class FunctionManager {

	public ActionListener createCommand(String type) {
		if(type.equals("Select")) {
			FileSelector test = new FileSelector();
			return  test;
		}
		return null;
	}
}