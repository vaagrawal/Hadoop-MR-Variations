import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;

public class EmployeeNoCodeGenerator {

	// public int id ;
	// public String name ;
	// public String designation;
	// public int mgrid ;
	// private String hiredate;
	// private double salary;
	// private double commission;
	// private int deptid ;

	public static void serializeData(Schema sc, String output) {
		DataFileWriter<GenericRecord> df = null;
		DatumWriter<GenericRecord> dw = null;
		try {

			GenericRecord e1 = new GenericData.Record(sc);
			e1.put("id", 100);
			e1.put("name", "Popat kaka");
			e1.put("designation", "CEO");
			e1.put("mgrid", 5);
			e1.put("hiredate", " May18");
			e1.put("salary", (Double) 40000000.0);
			e1.put("commission", (Double) 30.0);
			e1.put("deptid", 2);
			GenericRecord e2 = new GenericData.Record(sc);
			e2.put("id", 101);
			e2.put("name", "Popat chela");
			e2.put("designation", "Peon");
			e2.put("mgrid", 5);
			e2.put("hiredate", " May18");
			e2.put("salary", (Double) 4000000.0);
			e2.put("commission", (Double) 30.0);
			e2.put("deptid", 2);

			File file = new File(output);
			dw = new GenericDatumWriter<GenericRecord>(sc);
			df = new DataFileWriter<GenericRecord>(dw);
			df.create(sc, file);
			df.append(e1);
			df.append(e2);

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			try {
				df.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}

	public static void deserializeData(Schema sc, String input, String output) {
		DatumReader<GenericRecord> dr = new GenericDatumReader<GenericRecord>(
				sc);
		DataFileReader<GenericRecord> df = null;
		FileWriter fo =null;
		try {
			df = new DataFileReader<GenericRecord>(new File(input), dr);
			GenericRecord e = null;
			fo= new FileWriter(new File(output));

			while (df.hasNext()) {
				e = df.next(e);
				
				System.out.println(e);
				fo.write(e.toString());
			}
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}finally{
			try {
				fo.close();
				df.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}

	}

	public static void main(String args[]) throws IOException {
		Schema sc = new Schema.Parser().parse(new File(
				"./src/main/resources/schema/avr.avsc"));
		String output = "emp.avro";
		serializeData(sc, output);
		String textfilesource = "finaltextwithnocode.txt";
		deserializeData(sc, output, textfilesource);

	}
}
