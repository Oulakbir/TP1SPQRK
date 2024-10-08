package ma.enset;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Scanner;

public class VentesParAnnéeDonner {
    public static void main(String[] args) {
        // Demander à l'utilisateur de saisir l'année
        Scanner scanner = new Scanner(System.in);
        System.out.print("Veuillez entrer l'année : ");
        String annee = scanner.nextLine();

        // Initialisation de la configuration Spark
        SparkConf conf = new SparkConf()
                .setAppName("Total des Ventes par Produit et Ville");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Lecture du fichier texte contenant les ventes
        JavaRDD<String> lignes = sc.textFile("ventes.txt");

        // Filtrer les ventes pour l'année spécifiée
        JavaRDD<String> lignesFiltrees = lignes.filter(new org.apache.spark.api.java.function.Function<String, Boolean>() {
            @Override
            public Boolean call(String ligne) throws Exception {
                return ligne.startsWith(annee);
            }
        });

        // Transformation : extraire les informations pertinentes (ville, produit, prix)
        JavaPairRDD<Tuple2<String, String>, Double> ventesProduits = lignesFiltrees.mapToPair(new org.apache.spark.api.java.function.PairFunction<String, Tuple2<String, String>, Double>() {
            @Override
            public Tuple2<Tuple2<String, String>, Double> call(String ligne) throws Exception {
                String[] parties = ligne.split(" ");
                if (parties.length != 4) {
                    // Gestion des lignes mal formatées
                    System.err.println("Ligne mal formatée : " + ligne);
                    return new Tuple2<>(new Tuple2<>("Inconnu", "Inconnu"), 0.0);
                }
                String ville = parties[1];
                String produit = parties[2];
                double prix = 0.0;
                try {
                    prix = Double.parseDouble(parties[3]);
                } catch (NumberFormatException e) {
                    System.err.println("Prix invalide dans la ligne : " + ligne);
                }
                return new Tuple2<>(new Tuple2<>(ville, produit), prix);
            }
        });

        // Réduction : calculer le total des ventes par produit et par ville
        JavaPairRDD<Tuple2<String, String>, Double> totalVentesParProduitEtVille = ventesProduits
                .reduceByKey(new org.apache.spark.api.java.function.Function2<Double, Double, Double>() {
                    @Override
                    public Double call(Double v1, Double v2) throws Exception {
                        return v1 + v2;
                    }
                });

        // Affichage des résultats
        totalVentesParProduitEtVille.foreach(new org.apache.spark.api.java.function.VoidFunction<Tuple2<Tuple2<String, String>, Double>>() {
            @Override
            public void call(Tuple2<Tuple2<String, String>, Double> tuple) throws Exception {
                System.out.println("Ville : " + tuple._1()._1() +
                        ", Produit : " + tuple._1()._2() +
                        ", Total des Ventes : " + tuple._2());
            }
        });

        // Arrêt du contexte Spark
        sc.stop();
    }
}
