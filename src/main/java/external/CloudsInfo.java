package external;

import java.util.ArrayList;

public abstract class CloudsInfo {
	
	protected ArrayList<String> cloudNames = new ArrayList<String>();
	
	
	public abstract float quality(String cloudName1, String cloudName2);
	public abstract String bestCloud();

}
