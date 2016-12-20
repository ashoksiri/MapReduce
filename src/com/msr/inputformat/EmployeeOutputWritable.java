package com.msr.inputformat;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
public class EmployeeOutputWritable implements Writable, DBWritable
{
 private int emp_no;
 private String birth_date;
 private String first_name;
 private String last_name;
 private String gender;
 private String hire_date;

 
 
public EmployeeOutputWritable(int emp_no, String birth_date, String first_name, String last_name, String gender,
		String hire_date) {
	super();
	this.emp_no = emp_no;
	this.birth_date = birth_date;
	this.first_name = first_name;
	this.last_name = last_name;
	this.gender = gender;
	this.hire_date = hire_date;
}
public void readFields(DataInput in) throws IOException { }
public void readFields(ResultSet rs) throws SQLException
 {
 }
public void write(DataOutput out) throws IOException { }
public void write(PreparedStatement ps) throws SQLException
 {
	//ps.getConnection().setAutoCommit(false);
 ps.setInt(1, emp_no);
 ps.setString(2, birth_date);
 ps.setString(3, first_name);
 ps.setString(4,last_name);
 ps.setString(5,gender);
 ps.setString(6,hire_date);
 }
}