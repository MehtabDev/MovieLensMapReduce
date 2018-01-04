package com.movie.common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * @author mehtab khan
 * @description JoinWritable class used for custom value.
 */
public class JoinWritable implements Writable {

	private Text mValues;

	private Text mFileName;

	public JoinWritable() {
		set(new Text(), new Text());
	}

	public JoinWritable(String title, String mFileName) {
		set(new Text(title), new Text(mFileName));
	}

	public JoinWritable(Text title, Text mFileName) {
		set(title, mFileName);
	}

	public void set(Text title, Text mFileName) {
		this.mValues = title;
		this.mFileName = mFileName;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		mValues.write(out);

		mFileName.write(out);
	}

	public void readFields(DataInput in) throws IOException {
		mValues.readFields(in);

		mFileName.readFields(in);
	}

	public Text getmFileName() {
		return mFileName;
	}

	public Text getmTitle() {
		return mValues;
	}

}
