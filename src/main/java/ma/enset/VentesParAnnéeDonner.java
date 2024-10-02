package ma.enset;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
public class VentesParAnnéeDonner {
    public static void main(String[] args) {
        // Vérification des arguments passés au programme
        if (args.length < 1) {
            System.err.println("Usage: TotalVentesParProduitEtVille <année>");
            System.exit(1);
        }

        String annee = args[0];

        // Initialisation de la configuration Spark
        SparkConf conf = new SparkConf()
                .setAppName("Total des Ventes par Produit et Ville")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Lecture du fichier texte contenant les ventes
        JavaRDD<String> lignes = sc.textFile("ventes.txt");

        // Filtrer les ventes pour l'année spécifiée
        JavaRDD<String> lignesFiltrees = lignes.filter(ligne -> ligne.startsWith(annee));

        // Transformation : extraire les informations pertinentes (ville, produit, prix)
        JavaPairRDD<Tuple2<String, String>, Double> ventesProduits = lignesFiltrees.mapToPair(ligne -> {
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
        });

        // Réduction : calculer le total des ventes par produit et par ville
        JavaPairRDD<Tuple2<String, String>, Double> totalVentesParProduitEtVille = ventesProduits
                .reduceByKey(Double::sum);

        // Affichage des résultats
        totalVentesParProduitEtVille.foreach(tuple ->
                System.out.println("Ville : " + tuple._1()._1() +
                        ", Produit : " + tuple._1()._2() +
                        ", Total des Ventes : " + tuple._2())
        );

        // Arrêt du contexte Spark
        sc.stop();
    }
}
