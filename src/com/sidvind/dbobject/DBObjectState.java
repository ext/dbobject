package com.sidvind.dbobject;

import java.lang.reflect.Constructor;
import java.sql.PreparedStatement;
import java.util.List;

public class DBObjectState {
	public DataLayer db;
	public String table;
	public Class<? extends DBObject> cls;
	
	public PreparedStatement all;
	public PreparedStatement by_id;
	public PreparedStatement begin;
	public PreparedStatement commit;
	public PreparedStatement rollback;
	
	public List<DBObject.ColumnData> fields;
	public Constructor<?>[] ctor;
	
	public void commit() {
		db.commit();
	}
	
	public void rollback() {
		db.rollback();
	}
}
