package engine;

import java.awt.event.ActionListener;

public class FunctionManager {

	public ActionListener createCommand(String type) {
		if(type.equals("Select")) {
			FileSelector selector = new FileSelector();
			return  selector;
		}
		if(type.equals("Run")) {
			FileSelector test = new FileSelector();
			return  test;
		}
		return null;
	}
}