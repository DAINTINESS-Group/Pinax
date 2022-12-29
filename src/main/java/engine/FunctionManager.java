package engine;

import java.awt.event.ActionListener;

public class FunctionManager {

	public ActionListener createCommand(String type) {
		if(type.equals("Select")) {
			FileSelector selector = new FileSelector();
			return  selector;
		}
		return null;
	}
}