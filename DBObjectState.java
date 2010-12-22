package se.bth.libsla.db;

import java.lang.reflect.Constructor;
import java.sql.PreparedStatement;
import java.util.List;

public class DBObjectState {
	public DataLayer db;
	public String table;
	
	public PreparedStatement all;
	public PreparedStatement by_id;
	public PreparedStatement persist;
	
	public List<DBObject.Field> fields;
	public Constructor<?>[] ctor;
}
