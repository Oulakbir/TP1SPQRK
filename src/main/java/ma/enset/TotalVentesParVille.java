package ma.enset;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class TotalVentesParVille {
    public static void main(String[] args) {
        // Initialisation de la configuration Spark
        SparkConf conf = new SparkConf().setAppName("Total Sales by City");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Lecture du fichier texte contenant les ventes
        JavaRDD<String> lines = sc.textFile("ventes.txt");

        // Transformation : extraction des informations pertinentes (ville et prix)
        JavaPairRDD<String, Double> citySales = lines.mapToPair(line -> {
            String[] parts = line.split(" ");
            String city = parts[1];
            double price = Double.parseDouble(parts[3]);
            return new Tuple2<>(city, price);
        });

        // Réduction : calcul du total des ventes par ville
        JavaPairRDD<String, Double> totalSalesByCity = citySales.reduceByKey(Double::sum);

        // Affichage des résultats
        totalSalesByCity.foreach(tuple -> System.out.println(tuple._1() + " : " + tuple._2()));

        // Arrêt du contexte Spark
        sc.stop();
    }
}
