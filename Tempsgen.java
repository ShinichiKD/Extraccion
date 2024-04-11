/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */


 import java.io.File;
 import java.io.FileWriter;
 import java.util.Random;
 
 /**
  *
  * @author renzo
  */
 public class Tempsgen {
 
     /**
      * @param args the command line arguments
      */
     public static void main(String[] args) {
         String words[] = {"Santiago","Curico","Talca","Rancagua","Concepcion","Iquique","Arica","Calama","Ovalle","Antofagasta"};
         if (args.length == 0) {
            System.out.println("Tempsgen permite generar un archivo con ciudades y sus temperaturas mínima y máxima por día.");
            System.out.println("Uso:");
            System.out.println("  java Tempsgen N ");
            System.out.println("N es el número de años de simulación.");

        } else {
            try {
                int N = Integer.parseInt(args[0]);
                long time = System.currentTimeMillis();
                Random rand = new Random(time);
                File file = new File("temps.txt");
                if (file.exists()) {
                    file.delete();
                }
                FileWriter writer = new FileWriter(file, true);
                int c = 0;
                String line = "";
                for (int i = 0; i < N+1; i++) {
                    String word = words[rand.nextInt(words.length)];
                    int anio = 1980+i;
                    
                    
                    for(int j=1; j<13;j++){
                        
                        for(int k=1;k<31;k++){
                            int temp1 = rand.nextInt(45);
                            int temp2 = rand.nextInt(45);
                            int mayor = 0;
                            int menor = 0;
                            if(temp1>=temp2){
                                mayor=temp1;
                                menor=temp2;
                            }else{
                                mayor=temp2;
                                menor=temp1;
                            }
                            
                            writer.write(word+" "+anio+"-"+j+"-"+k+" "+menor+" "+mayor+"\n");
                        }
                    }
                    

                }
                writer.close();
            } catch (Exception ex) {
                System.out.println("Error:" + ex.getMessage());
            }
        }
    }

     
 
 }