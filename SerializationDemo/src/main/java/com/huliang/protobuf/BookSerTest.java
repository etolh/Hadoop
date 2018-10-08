package com.huliang.protobuf;


import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * 测试序列化和反序列化
 * @author huliang
 * @date 2018/10/8 19:40
 */
public class BookSerTest {

    /**
     * 序列化：将对象保存为磁盘文件  45byte
     * @throws IOException
     */
    public void write() throws IOException {

        AddressBookProtos.Person join = AddressBookProtos.Person.newBuilder()
                .setId(12)
                .setName("join")
                .setEmail("join@gmai.com")
                .addPhone(AddressBookProtos.Person.PhoneNumber.newBuilder()
                        .setNumber("+351 999 999 999")
                        .setType(AddressBookProtos.Person.PhoneType.HOME)
                        .build())
                .build();       // 根据生产类创建对象

        FileOutputStream fos = new FileOutputStream("F:\\bigdata\\test\\serialization\\protobuf\\person.data");
        join.writeTo(fos);
    }

    public void read() throws IOException {

        FileInputStream fis = new FileInputStream("F:\\bigdata\\test\\serialization\\protobuf\\person.data");
        AddressBookProtos.Person join = AddressBookProtos.Person.parseFrom(fis);
        System.out.println(join.getName()+":"+join.getEmail()+":"+join.getPhone(0).getNumber());

    }

    public static void main(String[] args) throws IOException {

        BookSerTest test = new BookSerTest();
        //test.write();
        test.read();
    }
}
