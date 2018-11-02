package utils;

import java.io.File;
import java.io.FileWriter;
import java.util.Random;

public class writeJson {
    public static void main(String[] args){
        Random random=new Random();

        if(args.length < 2){
            System.out.println("need parameters: <File> <LineNum>");
            System.exit(1);
        }
        File f= new File(args[0]);
        int ln = Integer.parseInt(args[1]);
        int fln=ln/10;
        try (  FileWriter fw = new FileWriter(f)){
            for(int i=0; i< ln;i++){
                String name="name"+i;
                int age = random.nextInt(100);
                fw.write("{\"name\":\"" +name+
                        "\", \"age\":" +age+
                        "}\n");
                if(i%fln==0)
                    fw.flush();
            }
            fw.flush();
        }catch (Exception e){
            e.printStackTrace();
        }


    }
}
