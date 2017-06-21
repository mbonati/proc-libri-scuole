package com.sbd.procscuola.db;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class ScuolaDef {

	
	private String codiceScuola;
	private Map<String, String> datiScuola;
	List<String> names;
	List<String> values;


	public ScuolaDef(String codiceScuola, Map<String, String> datiScuola, List<String> names,List<String> values) {
		this.codiceScuola = codiceScuola;
		this.datiScuola = datiScuola;
		this.names = names;
		this.values = values;
	}

	public String getCodiceScuola() {
		return codiceScuola;
	}

	public Map<String, String> getDatiScuola(){
		return this.datiScuola;
	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("ScuolaDef [codiceScuola=");
		sb.append(codiceScuola);
		sb.append("");
		Set<Entry<String,String>> entries = datiScuola.entrySet();
		for (Entry<String,String> entry:entries){
			sb.append(", ");
			sb.append(entry.getKey());
			sb.append("=");
			sb.append(entry.getValue());
		}
		sb.append("]");
		return sb.toString();
	}

	public List<String> values() {
		return this.values;
//		String[] ret = new String[datiScuola.size()];
//		int i = 0;
//		Set<Entry<String,String>> entries = datiScuola.entrySet();
//		for (Entry<String,String> entry:entries){
//			ret[i] = entry.getValue();
//			i++;
//		}
//		return ret;
	}

	public List<String> names() {
		return this.names;
	}
	
	
}
