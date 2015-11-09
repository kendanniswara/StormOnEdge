package external;

import java.util.ArrayList;

public abstract class CloudsState {
	
	private ArrayList<String> cloudNames = new ArrayList<String>();
	
	public abstract void update();
	public abstract void init();
	
	
}
