package fr.eurecom.hybris.test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Map;

import fr.eurecom.hybris.Hybris;
import fr.eurecom.hybris.HybrisException;
import fr.eurecom.hybris.mds.Metadata;

public class HybrisTestCli implements Runnable {
    
    private Hybris hybris;
    private static String HELP_STRING = "Usage:\n" +
                                        "\th - help\n" +
                                        "\tq - quit\n" +
                                        "\tw [key] [value] - write\n" +
                                        "\tr [key] - read\n" +
                                        "\td [key] - delete\n" +
                                        "\tl - list\n" +
                                        "\tla - list all";
    
    public HybrisTestCli() throws HybrisException {
        hybris = new Hybris();
    }
    
    @Override
    public void run() {
        System.out.println("Hybris test console\n===================");
        System.out.println("Type 'h' for help.");

        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        
        boolean quit = false;
        String line;
        while(!quit) {
            
            try {
                System.out.print(">");
                line = br.readLine().trim().toLowerCase();
                
                if (line.equals("q"))
                    quit = true;
                else if (line.equals("h"))
                    System.out.println(HELP_STRING);
                else if (line.startsWith("w")) {
                    String key = line.split(" ")[1];
                    byte[] value = line.split(" ")[2].getBytes();
                    this.write(key, value);
                } else if (line.startsWith("r")) {
                    String key = line.split(" ")[1];
                    byte[] value = this.read(key);
                    if (value == null)
                        System.out.println("No value was found.");
                    else
                        System.out.println("Value retrieved: " + new String(value));
                } else if (line.startsWith("d")) {
                    String key = line.split(" ")[1];
                    this.delete(key);
                } else if (line.equals("l")) {
                    List<String> list = this.list();
                    for (String key : list)
                        System.out.println("\t - " + key);
                } else if (line.equals("la")) {
                    Map<String, Metadata> map = this.getAllMetadata();
                    for(String key : map.keySet()) 
                        System.out.println("\t - " + key + ": " + map.get(key));
                } else
                    System.out.println("* Unknown command.");
            
             } catch (IOException e) {
                e.printStackTrace();
             } catch (ArrayIndexOutOfBoundsException e) {
                 System.out.println("* Unknown command.");
             }
        }
        try {
            hybris.gc();
        } catch (HybrisException e) { e.printStackTrace(); }
    }
    
    private void write(String key, byte[] value) {
        try {
            hybris.write(key, value);
        } catch (HybrisException e) {
            e.printStackTrace();
        }
    }
    
    private byte[] read(String key) {
        try {
            return hybris.read(key);
        } catch (HybrisException e) {
            e.printStackTrace();
            return null;
        }
    }
    
    private void delete(String key) {
        try {
            hybris.delete(key);
        } catch (HybrisException e) {
            e.printStackTrace();
        }
    }
    
    private List<String> list() {
        try {
            return hybris.list();
        } catch (HybrisException e) {
            e.printStackTrace();
            return null;
        }
    }
    
    private Map<String, Metadata> getAllMetadata() {
        try {
            return hybris.getAllMetadata();
        } catch (HybrisException e) {
            e.printStackTrace();
            return null;
        }
    }

    public static void main(String[] args) throws HybrisException {
        HybrisTestCli htc = new HybrisTestCli();
        htc.run();
    }
}