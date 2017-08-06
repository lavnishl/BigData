package solution;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class CountDistinctIPKey implements WritableComparable<CountDistinctIPKey> {

	private String ipaddress;
	private String month;
	
	public CountDistinctIPKey(String ipaddress, String month) {
		this.ipaddress = ipaddress;
		this.month = month;
	}
	
	public CountDistinctIPKey() {}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((ipaddress == null) ? 0 : ipaddress.hashCode());
		result = prime * result + ((month == null) ? 0 : month.hashCode());
		return result;
	}
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		CountDistinctIPKey other = (CountDistinctIPKey) obj;
		if (ipaddress == null) {
			if (other.ipaddress != null)
				return false;
		} else if (!ipaddress.equals(other.ipaddress))
			return false;
		if (month == null) {
			if (other.month != null)
				return false;
		} else if (!month.equals(other.month))
			return false;
		return true;
	}
	
	


	@Override
	public void readFields(DataInput in) throws IOException {
		ipaddress = in.readUTF();
		month = in.readUTF();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(ipaddress);
		out.writeUTF(month);
	}

	@Override
	public int compareTo(CountDistinctIPKey arg0) {
		int response = this.ipaddress.compareTo(arg0.ipaddress);
		if(response == 0) {
			response = this.month.compareTo(arg0.month);
		}
		return response;
	}

	public String getipaddress() {
		return ipaddress;
	}

	public void setipaddress(String ipaddress) {
		this.ipaddress = ipaddress;
	}

	public String getmonth() {
		return month;
	}

	public void setmonth(String month) {
		this.month = month;
	}

	@Override
	public String toString() {
		return ipaddress + "," + month;
	}

	

}
