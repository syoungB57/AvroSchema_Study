package com.tistory.devkongb;

import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.File;
import java.io.IOException;

public class AvroSerialize {
    public static void mainS(String[] args) throws IOException {

        // 다양한 방법을 통해 User 클래스로 예시 객체 생성
        User user1 = new User();
        user1.setName("David");
        user1.setFavoriteNumber(99);

        User user2 = new User("Nick", 22, "grey");

        User user3 = User.newBuilder()
                .setName("Molly")
                .setFavoriteColor("orange")
                .setFavoriteNumber(null)
                .build();

        // user1, user2, user3 디스크에 직렬화
        DatumWriter<User> userDatumWriter = new SpecificDatumWriter<User>(User.class);
        DataFileWriter<User> dataFileWriter = new DataFileWriter<User>(userDatumWriter);
        dataFileWriter.create(user1.getSchema(), new File("users.avro"));
        dataFileWriter.append(user1);
        dataFileWriter.append(user2);
        dataFileWriter.append(user3);
        dataFileWriter.close();
    }

}
