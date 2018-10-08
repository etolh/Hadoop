package com.huliang.avro;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.File;
import java.io.IOException;
import java.net.URL;

/**
 * 测试序列化
 * @author huliang
 * @date 2018/10/8 20:04
 */
public class EmployeeSerTest {

    public void write() throws IOException {

        Employee e1 = new Employee();
        e1.setName("omar");
        e1.setAge(21);
        e1.setSalary(30000);
        e1.setAddress("Hyderabad");
        e1.setId(001);

        Employee e2 = new Employee();
        e2.setName("ram");
        e2.setAge(30);
        e2.setSalary(40000);
        e2.setAddress("Hyderabad");
        e2.setId(002);

        Employee e3 = new Employee();
        e3.setName("robbin");
        e3.setAge(25);
        e3.setSalary(35000);
        e3.setAddress("Hyderabad");
        e3.setId(003);

        // 将对象转化为内存序列格式
        DatumWriter<Employee> empDatuWrite = new SpecificDatumWriter<Employee>(Employee.class);
        // 文件写入类
        DataFileWriter<Employee> empFileWrite = new DataFileWriter<Employee>(empDatuWrite);

        empFileWrite.create(e1.getSchema(), new File("F:\\bigdata\\test\\serialization\\avro\\employee.data"));
        // 添加序列化对象
        empFileWrite.append(e1);
        empFileWrite.append(e2);
        empFileWrite.append(e3);

        empFileWrite.close();
        System.out.println("data successfully serialized");
    }

    public void read() throws IOException {

        DatumReader<Employee> empDatuReader = new SpecificDatumReader<Employee>(Employee.class);
        DataFileReader<Employee> empFileReader = new DataFileReader<Employee>(new File("F:\\bigdata\\test\\serialization\\avro\\employee.data"), empDatuReader);

        Employee emp = null;
        while (empFileReader.hasNext()) {
            emp = empFileReader.next();
            System.out.println(emp.getName()+":"+emp.getAge()+":"+emp.getSalary());
        }
        System.out.println("**********schema*************");
        System.out.println(emp.getSchema());
    }

    // 不预先编译schema，直接利用schema序列化与反序列化

    public void writeWithParser() throws IOException {

        // 初始化Schema类
        URL url = EmployeeSerTest.class.getClassLoader().getResource("employee.avsc");
        Schema schema = new Schema.Parser().parse(new File(url.getFile()));

        // 初始化GenericRecord对象，相当于Employee对象
        GenericRecord e1 = new GenericData.Record(schema);
        GenericRecord e2 = new GenericData.Record(schema);

        e1.put("name", "ramu");
        e1.put("id", 001);
        e1.put("salary",30000);
        e1.put("age", 25);
        e1.put("address", "chenni");
        e2.put("name", "rahman");
        e2.put("id", 002);
        e2.put("salary", 35000);
        e2.put("age", 30);
        e2.put("address", "Delhi");

        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
        DataFileWriter<GenericRecord> fileWriter = new DataFileWriter<GenericRecord>(datumWriter);

        fileWriter.create(schema, new File("F:\\bigdata\\test\\serialization\\avro\\employee2.data"));
        fileWriter.append(e1);
        fileWriter.append(e2);

        fileWriter.close();
        System.out.println("data successfully serialized");
    }

    public void readWithParser() throws IOException {

        URL url = EmployeeSerTest.class.getClassLoader().getResource("employee.avsc");
        Schema schema = new Schema.Parser().parse(new File(url.getFile()));

        DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(schema);
        DataFileReader<GenericRecord> fileReader = new DataFileReader<GenericRecord>(new File("F:\\bigdata\\test\\serialization\\avro\\employee2.data"),datumReader);

        GenericRecord emp = null;

        while (fileReader.hasNext()) {
                emp = fileReader.next();
                System.out.println(emp);
            }
        System.out.println("**********schema*************");
        System.out.println(emp.getSchema());
    }
    public static void main(String[] args) throws IOException {

        EmployeeSerTest test = new EmployeeSerTest();

        //test.write();
        //test.read();
        //test.writeWithParser();
        test.readWithParser();
    }
}